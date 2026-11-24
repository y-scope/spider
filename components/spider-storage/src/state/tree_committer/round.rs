//! A round of a tree's committer: its events planned, written, applied, answered and published.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::Arc;

use spider_core::job::JobState;
use spider_core::types::id::JobId;

use crate::cache::error::CacheError;
use crate::cache::error::InternalError;
use crate::cache::job::SharedJobControlBlock;
use crate::cache::task::PreparedChild;
use crate::cache::tree::Pending;
use crate::cache::tree::Plan;
use crate::cache::tree::Publication;
use crate::cache::tree::TreeStructure;
use crate::db::ChildJobInsertion;
use crate::db::DbError;
use crate::db::InternalJobOrchestration;
use crate::db::JobStateUpdate;
use crate::db::TreeTransition;
use crate::inbound_queue::InboundQueueSender;
use crate::state::tree_committer::CommitterEnv;
use crate::state::tree_committer::planned_round::PlannedRound;
use crate::task_instance_pool::TaskInstancePoolConnector;
use crate::test_hooks::HookContext;
use crate::test_hooks::HookPoint;

/// The state that a tree's committer owns without a lock: its structure, which holds every
/// non-terminal job's state, and strong references to the control blocks of exactly those jobs.
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
pub(super) struct Committer<
    InboundQueueSenderType: InboundQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    env: Arc<CommitterEnv<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>>,
    structure: TreeStructure,
    jcbs: HashMap<
        JobId,
        SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    >,
}

impl<
    InboundQueueSenderType: InboundQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> Committer<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The committer of the one-job tree rooted at `root`.
    ///
    /// # Panics
    ///
    /// Panics if `root` is terminal.
    pub(super) async fn new_root(
        env: Arc<
            CommitterEnv<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
        >,
        root: SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    ) -> Self {
        let node = root.tree_node().await;
        assert!(
            !node.state.is_terminal(),
            "a tree's root should not be terminal when its committer starts"
        );
        let root_id = root.id();
        Self {
            env,
            structure: TreeStructure::new_root(root_id, node),
            jcbs: HashMap::from([(root_id, root)]),
        }
    }

    /// Runs one round over the events in `in_flight`:
    ///
    /// 1. Plans the events in their order.
    /// 2. Writes the planned state changes and the registered child jobs to the database, in one
    ///    transaction.
    /// 3. Updates the numbers of active children in the tree's structure, then applies the state
    ///    changes to the cache, deepest job first and root last.
    /// 4. Answers each planned event and publishes the ready work.
    ///
    /// On return, `in_flight` holds the events that the committer puts back at the front of the
    /// pending queue, in their order:
    ///
    /// * Normally, the events that the round deferred.
    /// * If the database rejected a job's outputs or a registered child job as too large, every
    ///   event of the round, with that job's event replaced by the job's failure (see
    ///   [`PlannedRound::fail_job_with_oversized_payload`]). Nothing was written.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TreeStructure::plan_events`]'s return values on failure.
    /// * Forwards [`PlannedRound::new`]'s return values on failure.
    /// * Forwards [`PlannedRound::fail_job_with_oversized_payload`]'s return values on failure.
    /// * Forwards [`TreeStructure::apply_num_active_children`]'s return values on failure.
    /// * Forwards [`Self::apply_to_cache`]'s return values on failure.
    /// * Forwards [`Self::insert_registered_children_into_tree`]'s return values on failure.
    /// * Forwards [`Self::publish`]'s return values on failure.
    pub(super) async fn run_round(
        &mut self,
        in_flight: &mut Vec<Pending>,
    ) -> Result<(), CacheError> {
        let Plan {
            updates,
            mut publications,
            num_active_children,
            outcomes,
            ..
        } = self
            .structure
            .plan_events(in_flight.iter().map(|pending| &pending.event))?;
        let mut round = PlannedRound::new(in_flight, outcomes)?;

        if !updates.is_empty() {
            let mut transitions: Vec<Transition> = updates.iter().map(Transition::from).collect();
            // The cache applies the round's changes one job at a time, so a reader can see some
            // jobs changed and others not yet. Applying each job after its descendants means a
            // reader that sees a job's new state also sees the new states of the jobs below it.
            transitions.sort_by_key(|transition| Reverse(self.jcb(transition.job_id).depth()));
            let child_ids = match self
                .env
                .db
                .apply_tree_transition(TreeTransition {
                    updates,
                    child_insertions: self.child_insertions(&round),
                })
                .await
            {
                Ok(child_ids) => child_ids,
                Err(error) => {
                    return round.fail_job_with_oversized_payload(error);
                }
            };
            self.hit_hook(HookPoint::CommitterRoundAfterDbWrite, &transitions)
                .await;
            for (job_id, new_num_active_children) in num_active_children {
                self.structure
                    .apply_num_active_children(job_id, new_num_active_children)?;
            }
            for transition in transitions {
                self.apply_to_cache(transition).await?;
            }
            if round.has_child_registration() {
                let publications_of_registered_child_jobs = self
                    .insert_registered_children_into_tree(&mut round, &child_ids)
                    .await?;
                publications.extend(publications_of_registered_child_jobs);
            }
        }

        round.answer();

        for publication in publications {
            self.publish(publication).await?;
        }
        #[cfg(any(test, debug_assertions))]
        self.assert_invariants().await;
        Ok(())
    }

    /// # Returns
    ///
    /// Whether the committer's tree has no job left.
    pub(super) fn is_quiescent(&self) -> bool {
        self.structure.is_quiescent()
    }

    /// Applies `transition`, which the round has written to the database, to its job's control
    /// block and to the structure. A job that `transition` makes terminal leaves the committer and
    /// is enqueued for cache GC.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`SharedJobControlBlock::apply_tree_state`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::jcb`] or [`TreeStructure::apply_state`] panics.
    async fn apply_to_cache(&mut self, transition: Transition) -> Result<(), CacheError> {
        let Transition { job_id, from, to } = transition;
        self.jcb(job_id).apply_tree_state(from, to).await?;
        self.structure.apply_state(job_id, to);
        if to.is_terminal() {
            self.jcbs.remove(&job_id);
            self.env.job_cache_gc_handle.enqueue_terminated_job(job_id);
        }
        Ok(())
    }

    /// # Type Parameters
    ///
    /// * `'round_lifetime` - The lifetime of the borrowed round, whose events the insertions
    ///   borrow.
    ///
    /// # Returns
    ///
    /// The database insertions of the children that the registering events of `round` carry, in the
    /// order of [`PlannedRound::registered_child_sets`] and, within each event, in order. Each
    /// child is inserted under its registering job, in the committer's tree and in the registering
    /// job's resource group.
    fn child_insertions<'round_lifetime>(
        &self,
        round: &'round_lifetime PlannedRound<'_>,
    ) -> Vec<ChildJobInsertion<'round_lifetime>> {
        let root_id = self.structure.root_id();
        let mut child_insertions = Vec::with_capacity(round.num_registered_children());
        for (parent_id, children) in round.registered_child_sets() {
            let resource_group_id = self.jcb(parent_id).owner_id();
            child_insertions.extend(children.iter().map(|child| ChildJobInsertion {
                parent_id,
                root_id,
                resource_group_id,
                compressed_job_submission: child.submission(),
            }));
        }
        child_insertions
    }

    /// Inserts the children that the registering events of `round` carry, which the round has
    /// inserted into the database under the IDs `child_ids`, in the order of
    /// [`Self::child_insertions`], into the in-memory tree: takes each registering event's children
    /// out of it, inserts them into the structure (see [`Self::insert_child_set_into_structure`])
    /// and adds them to the committer and the job cache.
    ///
    /// # Returns
    ///
    /// The publication of each child's ready tasks, in the order of `child_ids`, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`CacheError`] converted from [`DbError::CorruptedDbState`] if the number of `child_ids`
    ///   differs from the number of registered children.
    /// * [`CacheError`] converted from [`InternalError::RegistrationMismatch`] if a registering
    ///   event carries more children than `child_ids` has left.
    /// * Forwards [`Self::insert_child_set_into_structure`]'s return values on failure.
    /// * Forwards [`JobCache::insert_many`](crate::state::JobCache::insert_many)'s return values on
    ///   failure.
    async fn insert_registered_children_into_tree(
        &mut self,
        round: &mut PlannedRound<'_>,
        child_ids: &[JobId],
    ) -> Result<Vec<Publication>, CacheError> {
        let num_children = round.num_registered_children();
        if child_ids.len() != num_children {
            return Err(DbError::CorruptedDbState(format!(
                "the database inserted {} children, but the round registers {}",
                child_ids.len(),
                num_children
            ))
            .into());
        }
        let mut jcbs = Vec::with_capacity(num_children);
        let mut publications = Vec::with_capacity(num_children);
        let mut remaining_child_ids = child_ids;
        for (parent_id, prepared_children) in round.take_registered_child_sets() {
            let (child_set_ids, rest) = remaining_child_ids
                .split_at_checked(prepared_children.len())
                .ok_or(InternalError::RegistrationMismatch(parent_id))?;
            remaining_child_ids = rest;
            self.insert_child_set_into_structure(
                parent_id,
                child_set_ids,
                prepared_children,
                &mut jcbs,
                &mut publications,
            )
            .await?;
        }
        self.jcbs
            .extend(jcbs.iter().map(|jcb| (jcb.id(), jcb.clone())));
        self.env.job_cache.insert_many(jcbs).await?;
        Ok(publications)
    }

    /// Inserts `prepared_children`, the children of `parent_id` that the round has inserted into
    /// the database under the IDs `child_ids`, in order, into the structure: builds each child's
    /// control block and appends it to `jcbs`, and appends the publication of each child's ready
    /// tasks to `publications`, in the order of `child_ids`.
    ///
    /// `child_ids` and `prepared_children` must have the same length.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`CacheError`] converted from [`InternalError::TaskGraphCorrupted`] if a registered child
    ///   has no ready task.
    /// * Forwards [`SharedJobControlBlock::get_ready_task_indices`]'s return values on failure.
    /// * Forwards [`TreeStructure::insert_children`]'s return values on failure.
    async fn insert_child_set_into_structure(
        &mut self,
        parent_id: JobId,
        child_ids: &[JobId],
        prepared_children: Vec<PreparedChild>,
        jcbs: &mut Vec<
            SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
        >,
        publications: &mut Vec<Publication>,
    ) -> Result<(), CacheError> {
        let parent = self.jcb(parent_id).clone();
        let mut nodes = Vec::with_capacity(child_ids.len());
        for (&child_id, prepared_child) in child_ids.iter().zip(prepared_children) {
            let jcb =
                SharedJobControlBlock::from_prepared_child(child_id, &parent, prepared_child).await;
            let task_indices = jcb.get_ready_task_indices().await?;
            if task_indices.is_empty() {
                return Err(InternalError::TaskGraphCorrupted(format!(
                    "the registered child {child_id:?} has no ready task"
                ))
                .into());
            }
            nodes.push((child_id, jcb.tree_node().await));
            jcbs.push(jcb);
            publications.push(Publication::Ready {
                job_id: child_id,
                task_indices,
            });
        }
        self.structure.insert_children(parent_id, nodes)?;
        Ok(())
    }

    /// Sends `publication` to its lane of the inbound queue, for the resource group that owns its
    /// job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`InboundQueueSender::send_task_ready`]'s return values on failure.
    /// * Forwards [`InboundQueueSender::send_commit_ready`]'s return values on failure.
    /// * Forwards [`InboundQueueSender::send_cleanup_ready`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::jcb`] panics.
    async fn publish(&self, publication: Publication) -> Result<(), CacheError> {
        let owner_id = self.jcb(publication.job_id()).owner_id();
        let inbound_queue_sender = &self.env.inbound_queue_sender;
        match publication {
            Publication::Ready {
                job_id,
                task_indices,
            } => {
                inbound_queue_sender
                    .send_task_ready(owner_id, job_id, task_indices)
                    .await?;
            }
            Publication::Commit(job_id) => {
                inbound_queue_sender
                    .send_commit_ready(owner_id, job_id)
                    .await?;
            }
            Publication::Cleanup(job_id) => {
                inbound_queue_sender
                    .send_cleanup_ready(owner_id, job_id)
                    .await?;
            }
        }
        Ok(())
    }

    /// Checks that the tree's structure keeps the tree invariants, and that the committer holds the
    /// control blocks of exactly the structure's jobs, each in the job's state in the structure.
    ///
    /// # Panics
    ///
    /// Panics if any check fails.
    #[cfg(any(test, debug_assertions))]
    async fn assert_invariants(&self) {
        if let Err(violation) = self.structure.check_invariants() {
            panic!("the tree should keep its invariants after a round: {violation}");
        }
        let job_ids = self.structure.sorted_job_ids();
        assert_eq!(
            job_ids.len(),
            self.jcbs.len(),
            "the committer should hold the control blocks of exactly its structure's jobs"
        );
        for job_id in job_ids {
            let cached_state = self.jcb(job_id).state().await;
            assert_eq!(
                self.structure.node(job_id).map(|node| node.state),
                Some(cached_state),
                "the structure should hold the cached state of the job {job_id:?}"
            );
        }
    }

    /// # Returns
    ///
    /// The control block of the tree's non-terminal job `job_id`.
    ///
    /// # Panics
    ///
    /// Panics if the committer holds no control block of `job_id`.
    fn jcb(
        &self,
        job_id: JobId,
    ) -> &SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType> {
        self.jcbs
            .get(&job_id)
            .expect("a job of the tree's structure should have its control block in the committer")
    }

    /// Fires the committer's hooks at `point` for the jobs of `transitions`.
    async fn hit_hook(&self, point: HookPoint, transitions: &[Transition]) {
        // A pause that the service's shutdown ends lets the round go on, as the round would without
        // the hook.
        let _ = self
            .env
            .hooks
            .hit(point, || HookContext {
                job_ids: transitions
                    .iter()
                    .map(|transition| transition.job_id)
                    .collect(),
                ..HookContext::default()
            })
            .await;
    }
}

/// The state change of one job that a round applies after its write.
struct Transition {
    job_id: JobId,
    from: JobState,
    to: JobState,
}

impl From<&JobStateUpdate> for Transition {
    fn from(update: &JobStateUpdate) -> Self {
        Self {
            job_id: update.job_id,
            from: update.from,
            to: update.to,
        }
    }
}

#[cfg(test)]
mod tests {
    use spider_utils::shutdown::ShutdownHandle;
    use tokio::sync::mpsc;
    use tokio_util::task::TaskTracker;

    use super::*;
    use crate::cache::error::InternalError;
    use crate::cache::error::StaleStateError;
    use crate::cache::tree::TreeEventKind;
    use crate::cache::tree::TreeEventOutcome;
    use crate::inbound_queue::CleanupTaskMarker;
    use crate::inbound_queue::CommitTaskMarker;
    use crate::inbound_queue::InboundQueueEntry;
    use crate::inbound_queue::create_inbound_queue;
    use crate::state::JobCache;
    use crate::state::JobCacheGcHandle;
    use crate::state::test_utils::MOCK_MAX_OUTPUTS_SIZE;
    use crate::state::test_utils::MockDbConnector;
    use crate::state::test_utils::MockInboundQueueSender;
    use crate::state::test_utils::create_test_jcb;
    use crate::state::tree_committer::spawn_committer;
    use crate::state::tree_committer::test_utils::ROOT_ID;
    use crate::state::tree_committer::test_utils::TIMEOUT;
    use crate::state::tree_committer::test_utils::TestTree;
    use crate::state::tree_committer::test_utils::regular_tasks_done;
    use crate::state::tree_committer::test_utils::reply;
    use crate::state::tree_committer::test_utils::start;
    use crate::test_hooks::TestHooks;

    #[tokio::test]
    async fn outputs_too_large_for_the_database_fail_the_job() -> anyhow::Result<()> {
        let mut tree = TestTree::spawn(false, false, MockInboundQueueSender).await;
        let start = reply(tree.submit(start())).await;
        assert!(
            matches!(start, Ok(TreeEventOutcome::Applied(JobState::Running))),
            "a start gave {start:?}"
        );

        let outputs = vec![vec![0; usize::try_from(MOCK_MAX_OUTPUTS_SIZE)? + 1]];
        let regular_tasks_done = reply(tree.submit(TreeEventKind::RegularTasksDone {
            outputs,
            children: Vec::new(),
        }))
        .await;
        assert!(
            matches!(
                regular_tasks_done,
                Ok(TreeEventOutcome::Applied(JobState::Failed))
            ),
            "outputs too large for the database gave {regular_tasks_done:?}"
        );
        let expected_message = DbError::OutputsTooLarge {
            job_id: ROOT_ID,
            size: MOCK_MAX_OUTPUTS_SIZE + 1,
            limit: MOCK_MAX_OUTPUTS_SIZE,
        }
        .to_string();
        assert_eq!(
            tree.db.errors.get(&ROOT_ID).as_deref(),
            Some(&expected_message)
        );
        assert!(
            !tree.db.outputs.contains_key(&ROOT_ID),
            "outputs too large for the database should not be stored"
        );
        assert_eq!(tree.gc_receiver.try_recv(), Ok(ROOT_ID));
        tree.wait_for_exit().await;
        assert!(!tree.shutdown_handle.is_fatal());
        Ok(())
    }

    #[tokio::test]
    async fn a_round_replies_with_its_plans_outcome_and_publishes_its_ready_work_to_the_jobs_owner()
    -> anyhow::Result<()> {
        let (inbound_queue_sender, inbound_queue_receiver) = create_inbound_queue();
        let mut tree = TestTree::spawn(true, true, inbound_queue_sender).await;

        let start = reply(tree.submit(start())).await;
        assert!(
            matches!(start, Ok(TreeEventOutcome::Applied(JobState::Running))),
            "a start gave {start:?}"
        );
        assert_eq!(
            inbound_queue_receiver.recv_tasks(1, TIMEOUT).await?,
            [InboundQueueEntry {
                resource_group_id: tree.owner_id,
                job_id: ROOT_ID,
                task_kind: 0,
            }]
        );

        let regular_tasks_done = reply(tree.submit(regular_tasks_done())).await;
        assert!(
            matches!(
                regular_tasks_done,
                Ok(TreeEventOutcome::Applied(JobState::CommitReady))
            ),
            "the regular tasks being done gave {regular_tasks_done:?}"
        );
        assert_eq!(
            inbound_queue_receiver.recv_commits(1, TIMEOUT).await?,
            [InboundQueueEntry {
                resource_group_id: tree.owner_id,
                job_id: ROOT_ID,
                task_kind: CommitTaskMarker,
            }]
        );

        let cancel = reply(tree.submit(TreeEventKind::CancelRoot)).await;
        assert!(
            matches!(
                cancel,
                Ok(TreeEventOutcome::Applied(JobState::CleanupReady))
            ),
            "a cancel gave {cancel:?}"
        );
        assert_eq!(
            inbound_queue_receiver.recv_cleanups(1, TIMEOUT).await?,
            [InboundQueueEntry {
                resource_group_id: tree.owner_id,
                job_id: ROOT_ID,
                task_kind: CleanupTaskMarker,
            }]
        );

        let second_cancel = reply(tree.submit(TreeEventKind::CancelRoot)).await;
        assert!(
            matches!(
                second_cancel,
                Ok(TreeEventOutcome::Stale(
                    StaleStateError::JobCancellationAlreadyRequested
                ))
            ),
            "a second cancel gave {second_cancel:?}"
        );

        drop(tree.submit(TreeEventKind::CancelRoot));
        let cleanup_done = reply(tree.submit(TreeEventKind::CleanupDone)).await;
        assert!(
            matches!(
                cleanup_done,
                Ok(TreeEventOutcome::Applied(JobState::Cancelled))
            ),
            "the cleanup task being done gave {cleanup_done:?}"
        );
        assert_eq!(tree.gc_receiver.try_recv(), Ok(ROOT_ID));
        tree.wait_for_exit().await;
        assert!(!tree.shutdown_handle.is_fatal());
        Ok(())
    }

    #[tokio::test]
    async fn a_cache_diverging_from_a_committed_write_poisons_the_service() -> anyhow::Result<()> {
        let tree = TestTree::spawn(false, false, MockInboundQueueSender).await;
        let commit_done = reply(tree.submit(TreeEventKind::CommitDone)).await;
        assert!(
            matches!(commit_done, Ok(TreeEventOutcome::Stale(_))),
            "a commit of a ready job gave {commit_done:?}"
        );

        tree.jcb
            .apply_tree_state(JobState::Ready, JobState::Running)
            .await?;
        let start = reply(tree.submit(start())).await;
        assert!(
            matches!(
                start,
                Err(CacheError::Internal(InternalError::TreePoisoned))
            ),
            "a start whose apply diverges from its write gave {start:?}"
        );
        assert_eq!(
            tree.db.states.get(&ROOT_ID).map(|state| *state),
            Some(JobState::Running)
        );
        tree.wait_for_exit().await;
        assert!(tree.shutdown_handle.is_fatal());
        Ok(())
    }

    #[tokio::test]
    async fn a_cache_diverging_from_the_structure_after_a_round_poisons_the_service()
    -> anyhow::Result<()> {
        let tree = TestTree::spawn(false, false, MockInboundQueueSender).await;
        reply(tree.submit(TreeEventKind::CommitDone)).await?;

        tree.jcb
            .apply_tree_state(JobState::Ready, JobState::Running)
            .await?;
        let commit_done = reply(tree.submit(TreeEventKind::CommitDone)).await;
        assert!(
            matches!(
                commit_done,
                Ok(TreeEventOutcome::Stale(
                    StaleStateError::JobNoLongerCommitReady
                ))
            ),
            "a commit of a job whose cached state diverges gave {commit_done:?}"
        );
        tree.wait_for_exit().await;
        assert!(tree.shutdown_handle.is_fatal());
        Ok(())
    }

    #[tokio::test]
    async fn a_publish_failure_after_the_reply_poisons_the_service() {
        let (inbound_queue_sender, inbound_queue_receiver) = create_inbound_queue();
        let tree = TestTree::spawn(false, false, inbound_queue_sender).await;
        drop(inbound_queue_receiver);

        let start = reply(tree.submit(start())).await;
        assert!(
            matches!(start, Ok(TreeEventOutcome::Applied(JobState::Running))),
            "a start whose publish fails gave {start:?}"
        );
        tree.wait_for_exit().await;
        assert!(tree.shutdown_handle.is_fatal());
    }

    #[tokio::test]
    async fn a_committer_started_with_a_terminal_root_poisons_the_service() -> anyhow::Result<()> {
        let (root, receivers) = create_test_jcb(ROOT_ID).await;
        root.apply_tree_state(JobState::Ready, JobState::Cancelled)
            .await?;
        let shutdown_handle = ShutdownHandle::new();
        let task_tracker = TaskTracker::new();

        spawn_committer(
            &task_tracker,
            Arc::new(CommitterEnv {
                db: MockDbConnector::default(),
                job_cache: JobCache::new(),
                job_cache_gc_handle: JobCacheGcHandle::new(mpsc::unbounded_channel().0),
                inbound_queue_sender: MockInboundQueueSender,
                shutdown_handle: shutdown_handle.clone(),
                hooks: TestHooks::default(),
            }),
            root,
            receivers,
        );
        task_tracker.close();
        tokio::time::timeout(TIMEOUT, task_tracker.wait()).await?;
        assert!(shutdown_handle.is_fatal());
        Ok(())
    }
}
