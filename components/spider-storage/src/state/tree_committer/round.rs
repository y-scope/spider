//! A round of a tree's committer: its events planned, written, applied, answered and published.

use std::collections::HashMap;
use std::sync::Arc;

use spider_core::job::JobState;
use spider_core::types::id::JobId;

use crate::cache::error::CacheError;
use crate::cache::job::SharedJobControlBlock;
use crate::cache::tree::Lane;
use crate::cache::tree::Pending;
use crate::cache::tree::Plan;
use crate::cache::tree::Publication;
use crate::cache::tree::TreeEventKind;
use crate::cache::tree::TreeEventOutcome;
use crate::cache::tree::TreeStructure;
use crate::db::DbError;
use crate::db::InternalJobOrchestration;
use crate::db::JobStateUpdate;
use crate::db::TreeTransition;
use crate::inbound_queue::InboundQueueSender;
use crate::state::tree_committer::CommitterEnv;
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
    env: Arc<CommitterEnv<InboundQueueSenderType, DbConnectorType>>,
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
        env: Arc<CommitterEnv<InboundQueueSenderType, DbConnectorType>>,
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

    /// Runs one round for the events of `in_flight`, in their order, and answers each event that
    /// the round plans. `in_flight`, which [`super::CommitterContextGuard`] owns, keeps the events
    /// to put back at the front of the pending queue, in their order: the events that the round
    /// defers or, if the database rejects the outputs of a job as too large, every event of the
    /// round, with that job's event turned into the job's failure.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`fail_job_with_oversized_outputs`]'s return values on failure.
    /// * Forwards [`Self::apply_to_cache`]'s return values on failure.
    /// * Forwards [`Self::publish`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::apply_to_cache`], [`Self::publish`] or, in debug and test builds,
    /// [`Self::assert_invariants`] panics.
    pub(super) async fn run_round(
        &mut self,
        in_flight: &mut Vec<Pending>,
    ) -> Result<(), CacheError> {
        let Plan {
            updates,
            publications,
            outcomes,
            ..
        } = self
            .structure
            .plan_events(in_flight.iter().map(|pending| &pending.event));

        if !updates.is_empty() {
            let transitions: Vec<Transition> = updates.iter().map(Transition::from).collect();
            if let Err(error) = self
                .env
                .db
                .apply_tree_transition(TreeTransition { updates })
                .await
            {
                return fail_job_with_oversized_outputs(in_flight, &outcomes, error);
            }
            self.hit_hook(HookPoint::CommitterRoundAfterDbWrite, &transitions)
                .await;
            for transition in transitions {
                self.apply_to_cache(transition).await?;
            }
        }

        for (pending, outcome) in std::mem::take(in_flight).into_iter().zip(outcomes) {
            let Some(outcome) = outcome else {
                in_flight.push(pending);
                continue;
            };
            // The event's waiter may have dropped its future, which is not an error of the
            // committer.
            let _ = pending.reply.send(Ok(outcome));
        }

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

    /// Checks that the tree's structure keeps the tree invariants, and that the committer holds
    /// the control blocks of exactly the structure's jobs, each in the job's state in the
    /// structure.
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

/// Turns a [`DbError::OutputsTooLarge`] of a round's write into a failure, on the regular lane, of
/// the job whose outputs the database rejects: the round's planned
/// [`TreeEventKind::RegularTasksDone`] event of that job in `in_flight`, whose outcomes are
/// `outcomes`, becomes that failure.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`CacheError`] converted from `error` if it is not a [`DbError::OutputsTooLarge`] of the job
///   of a planned [`TreeEventKind::RegularTasksDone`] event of the round.
fn fail_job_with_oversized_outputs(
    in_flight: &mut [Pending],
    outcomes: &[Option<TreeEventOutcome>],
    error: DbError,
) -> Result<(), CacheError> {
    let DbError::OutputsTooLarge {
        job_id: culprit_id, ..
    } = &error
    else {
        return Err(error.into());
    };
    let Some(culprit) = in_flight
        .iter_mut()
        .zip(outcomes)
        .find_map(|(pending, outcome)| {
            (outcome.is_some()
                && pending.event.job_id == *culprit_id
                && matches!(pending.event.kind, TreeEventKind::RegularTasksDone { .. }))
            .then_some(pending)
        })
    else {
        return Err(error.into());
    };
    tracing::warn!(
        job_id = ? culprit_id,
        error = % error,
        "The database rejected a job's outputs. Failing the job."
    );
    culprit.event.kind = TreeEventKind::TerminalFailure {
        lane: Lane::Regular,
        message: error.to_string(),
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use spider_utils::shutdown::ShutdownHandle;
    use tokio::sync::mpsc;
    use tokio_util::task::TaskTracker;

    use super::*;
    use crate::cache::error::InternalError;
    use crate::cache::error::StaleStateError;
    use crate::inbound_queue::CleanupTaskMarker;
    use crate::inbound_queue::CommitTaskMarker;
    use crate::inbound_queue::InboundQueueEntry;
    use crate::inbound_queue::create_inbound_queue;
    use crate::state::JobCacheGcHandle;
    use crate::state::test_utils::MOCK_MAX_OUTPUTS_SIZE;
    use crate::state::test_utils::MockDbConnector;
    use crate::state::test_utils::MockInboundQueueSender;
    use crate::state::test_utils::create_test_jcb;
    use crate::state::tree_committer::spawn_committer;
    use crate::state::tree_committer::test_utils::ROOT_ID;
    use crate::state::tree_committer::test_utils::TIMEOUT;
    use crate::state::tree_committer::test_utils::TestTree;
    use crate::state::tree_committer::test_utils::pending;
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
        let regular_tasks_done =
            reply(tree.submit(TreeEventKind::RegularTasksDone { outputs })).await;
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

    #[test]
    fn rejected_outputs_fail_only_the_planned_completion_of_their_job() -> anyhow::Result<()> {
        let mut in_flight = vec![
            pending(ROOT_ID.get() + 1, regular_tasks_done()),
            pending(ROOT_ID.get(), TreeEventKind::CommitDone),
            pending(ROOT_ID.get(), regular_tasks_done()),
        ];
        let outcomes = [
            Some(TreeEventOutcome::Untracked),
            Some(TreeEventOutcome::Stale(
                StaleStateError::JobNoLongerCommitReady,
            )),
            Some(TreeEventOutcome::Applied(JobState::Succeeded)),
        ];
        let error = DbError::OutputsTooLarge {
            job_id: ROOT_ID,
            size: MOCK_MAX_OUTPUTS_SIZE + 1,
            limit: MOCK_MAX_OUTPUTS_SIZE,
        };
        let expected_message = error.to_string();

        fail_job_with_oversized_outputs(&mut in_flight, &outcomes, error)?;
        let [other_job_completion, stale_commit_done, completion] = in_flight.as_slice() else {
            panic!("the round should keep its three events");
        };
        assert!(
            matches!(
                other_job_completion.event.kind,
                TreeEventKind::RegularTasksDone { .. }
            ),
            "the completion of another job should be kept"
        );
        assert!(
            matches!(stale_commit_done.event.kind, TreeEventKind::CommitDone),
            "a stale event of the job should be kept"
        );
        assert!(
            matches!(
                &completion.event.kind,
                TreeEventKind::TerminalFailure {
                    lane: Lane::Regular,
                    message,
                } if *message == expected_message
            ),
            "the job's planned completion should become its failure"
        );
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
