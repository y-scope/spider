//! A round of a tree's committer: its events planned, written, applied, answered and published.

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;

use spider_core::job::JobState;
use spider_core::types::id::JobId;

use crate::cache::error::CacheError;
use crate::cache::error::InternalError;
use crate::cache::job::SharedJobControlBlock;
use crate::cache::task::PreparedChild;
use crate::cache::tree::Lane;
use crate::cache::tree::Pending;
use crate::cache::tree::Plan;
use crate::cache::tree::PlannedRegistration;
use crate::cache::tree::Publication;
use crate::cache::tree::RoundOutcome;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::TreeEventKind;
use crate::cache::tree::TreeStructure;
use crate::db::ChildJobInsertion;
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

    /// Runs one round for the events of `in_flight`, in their order, and answers each event that
    /// the round plans. `in_flight`, which [`super::CommitterContextGuard`] owns, keeps the events
    /// to put back at the front of the pending queue, in their order: the events that the round
    /// defers or, if the database rejects the outputs of a job or the children that the round
    /// registers (see [`fail_culprit`]), every event of the round, with the culprit event turned
    /// into its job's failure.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TreeStructure::plan_events`]'s return values on failure.
    /// * Forwards [`Self::child_insertions`]'s return values on failure.
    /// * Forwards [`fail_culprit`]'s return values on failure.
    /// * Forwards [`Self::apply_to_cache`]'s return values on failure.
    /// * Forwards [`Self::bind_children`]'s return values on failure.
    /// * Forwards [`Self::publish`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * The plan of the round does not have exactly one outcome per event of `in_flight`.
    /// * [`Self::child_insertions`], [`Self::apply_to_cache`], [`Self::bind_children`],
    ///   [`Self::publish`] or, in debug and test builds, [`Self::assert_invariants`] panics.
    pub(super) async fn run_round(
        &mut self,
        in_flight: &mut Vec<Pending>,
    ) -> Result<(), CacheError> {
        let Plan {
            updates,
            mut publications,
            outcomes,
            registration,
            ..
        } = self
            .structure
            .plan_events(in_flight.iter().map(|pending| &pending.event))?;

        if !updates.is_empty() {
            let transitions: Vec<Transition> = updates.iter().map(Transition::from).collect();
            let child_insertions = match &registration {
                Some(registration) => self.child_insertions(in_flight, registration)?,
                None => Vec::new(),
            };
            let child_ids = match self
                .env
                .db
                .apply_tree_transition(TreeTransition {
                    updates,
                    child_insertions,
                })
                .await
            {
                Ok(child_ids) => child_ids,
                Err(error) => {
                    return fail_culprit(in_flight, &outcomes, registration.as_ref(), error);
                }
            };
            self.hit_hook(HookPoint::CommitterRoundAfterDbWrite, &transitions)
                .await;
            for transition in transitions {
                self.apply_to_cache(transition).await?;
            }
            if let Some(registration) = &registration {
                publications.extend(
                    self.bind_children(in_flight, registration, child_ids)
                        .await?,
                );
            }
        }

        assert_eq!(
            outcomes.len(),
            in_flight.len(),
            "the plan should have one outcome per event of the round"
        );
        // The planned events are answered in place, so every event not answered yet stays in
        // `in_flight`, which the committer's context guard owns: a panic never drops an unanswered
        // reply before the guard has shut the service down fatally, and the deferred events stay
        // behind for the committer to requeue.
        let mut outcomes = outcomes.into_iter();
        let planned_outcome = Cell::new(None);
        for pending in in_flight.extract_if(.., |_| {
            match outcomes
                .next()
                .expect("the plan should have one outcome per event of the round")
            {
                RoundOutcome::Planned(outcome) => {
                    planned_outcome.set(Some(outcome));
                    true
                }
                RoundOutcome::Deferred => false,
            }
        }) {
            let outcome = planned_outcome
                .take()
                .expect("an extracted event should have its planned outcome");
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

    /// # Type Parameters
    ///
    /// * `'event_lifetime` - The lifetime of the borrowed events of the round.
    ///
    /// # Returns
    ///
    /// The database part of `registration`: one insertion, under the registering job in the tree of
    /// the committer's root and in the registering job's resource group, per child of the event of
    /// `in_flight` that `registration` registers, in order, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`registered_children`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::jcb`] panics.
    fn child_insertions<'event_lifetime>(
        &self,
        in_flight: &'event_lifetime mut [Pending],
        registration: &PlannedRegistration,
    ) -> Result<Vec<ChildJobInsertion<'event_lifetime>>, InternalError> {
        let root_id = self.structure.root_id();
        let resource_group_id = self.jcb(registration.parent_id).owner_id();
        Ok(registered_children(in_flight, registration)?
            .iter()
            .map(|child| ChildJobInsertion {
                parent_id: registration.parent_id,
                root_id,
                resource_group_id,
                compressed_job_submission: child.submission(),
            })
            .collect())
    }

    /// Adds the children that `registration` registers, which the round has inserted under the IDs
    /// `child_ids`, in order, to the structure, the committer and the job cache. Takes the children
    /// out of the registering event of `in_flight`.
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
    /// * Forwards [`registered_children`]'s return values on failure.
    /// * Forwards [`TreeStructure::insert_children`]'s return values on failure.
    /// * Forwards [`JobCache::insert_many`](crate::state::JobCache::insert_many)'s return values on
    ///   failure.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::jcb`] panics.
    async fn bind_children(
        &mut self,
        in_flight: &mut [Pending],
        registration: &PlannedRegistration,
        child_ids: Vec<JobId>,
    ) -> Result<Vec<Publication>, CacheError> {
        let prepared_children = std::mem::take(registered_children(in_flight, registration)?);
        if child_ids.len() != prepared_children.len() {
            return Err(DbError::CorruptedDbState(format!(
                "the database inserted {} children of job {:?}, but the round registers {}",
                child_ids.len(),
                registration.parent_id,
                prepared_children.len()
            ))
            .into());
        }
        let parent = self.jcb(registration.parent_id).clone();
        let mut nodes = Vec::with_capacity(child_ids.len());
        let mut jcbs = Vec::with_capacity(child_ids.len());
        let mut publications = Vec::with_capacity(child_ids.len());
        for (child_id, prepared_child) in child_ids.into_iter().zip(prepared_children) {
            let task_indices = prepared_child.ready_task_indices().to_vec();
            let jcb =
                SharedJobControlBlock::from_prepared_child(child_id, &parent, prepared_child).await;
            nodes.push((child_id, jcb.tree_node().await));
            jcbs.push(jcb);
            publications.push(Publication::Ready {
                job_id: child_id,
                task_indices,
            });
        }
        self.structure
            .insert_children(registration.parent_id, nodes)?;
        self.env.job_cache.insert_many(jcbs.clone()).await?;
        self.jcbs
            .extend(jcbs.into_iter().map(|jcb| (jcb.id(), jcb)));
        Ok(publications)
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

/// # Type Parameters
///
/// * `'event_lifetime` - The lifetime of the borrowed events of the round.
///
/// # Returns
///
/// The children of the event of `in_flight` that `registration` registers on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`InternalError::RegistrationMismatch`] if that event is not a
///   [`TreeEventKind::RegularTasksDone`] of the registering job with the registered number of
///   children.
fn registered_children<'event_lifetime>(
    in_flight: &'event_lifetime mut [Pending],
    registration: &PlannedRegistration,
) -> Result<&'event_lifetime mut Vec<PreparedChild>, InternalError> {
    match in_flight.get_mut(registration.event_index) {
        Some(Pending {
            event:
                TreeEvent {
                    job_id,
                    kind: TreeEventKind::RegularTasksDone { children, .. },
                },
            ..
        }) if *job_id == registration.parent_id
            && children.len() == registration.children.count =>
        {
            Ok(children)
        }
        _ => Err(InternalError::RegistrationMismatch(registration.parent_id)),
    }
}

/// Turns an error of a round's write that one event of the round causes into a failure, on the
/// regular lane, of that event's job, in `in_flight`, whose outcomes are `outcomes`:
///
/// * A [`DbError::OutputsTooLarge`] fails the job whose outputs the database rejects: the round's
///   planned [`TreeEventKind::RegularTasksDone`] event of that job becomes that failure.
/// * A [`DbError::PayloadRejected`] of a round with a `registration` fails the registering job: the
///   registering event becomes that failure.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`CacheError`] converted from `error` if it is neither of the two errors above.
fn fail_culprit(
    in_flight: &mut [Pending],
    outcomes: &[RoundOutcome],
    registration: Option<&PlannedRegistration>,
    error: DbError,
) -> Result<(), CacheError> {
    let culprit_and_message = match (&error, registration) {
        (DbError::OutputsTooLarge { job_id, .. }, _) => in_flight
            .iter_mut()
            .zip(outcomes)
            .find_map(|(pending, outcome)| {
                (matches!(outcome, RoundOutcome::Planned(_))
                    && pending.event.job_id == *job_id
                    && matches!(pending.event.kind, TreeEventKind::RegularTasksDone { .. }))
                .then_some(pending)
            })
            .map(|pending| (pending, error.to_string())),
        (DbError::PayloadRejected(_), Some(registration)) => in_flight
            .get_mut(registration.event_index)
            .filter(|pending| pending.event.job_id == registration.parent_id)
            .map(|pending| (pending, "child payload rejected by the database".to_owned())),
        _ => None,
    };
    let Some((culprit, message)) = culprit_and_message else {
        return Err(error.into());
    };
    tracing::warn!(
        job_id = ? culprit.event.job_id,
        error = % error,
        "The database rejected the payload of a job's event. Failing the job."
    );
    culprit.event.kind = TreeEventKind::TerminalFailure {
        lane: Lane::Regular,
        message,
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
    use crate::cache::tree::ChildJobUsage;
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
    use crate::state::test_utils::create_test_prepared_children;
    use crate::state::tree_committer::spawn_committer;
    use crate::state::tree_committer::test_utils::ROOT_ID;
    use crate::state::tree_committer::test_utils::TIMEOUT;
    use crate::state::tree_committer::test_utils::TestTree;
    use crate::state::tree_committer::test_utils::payload_rejected;
    use crate::state::tree_committer::test_utils::pending;
    use crate::state::tree_committer::test_utils::registration;
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

    #[test]
    fn rejected_outputs_fail_only_the_planned_completion_of_their_job() -> anyhow::Result<()> {
        let mut in_flight = vec![
            pending(ROOT_ID.get() + 1, regular_tasks_done()),
            pending(ROOT_ID.get(), TreeEventKind::CommitDone),
            pending(ROOT_ID.get(), regular_tasks_done()),
        ];
        let outcomes = [
            RoundOutcome::Planned(TreeEventOutcome::Untracked),
            RoundOutcome::Planned(TreeEventOutcome::Stale(
                StaleStateError::JobNoLongerCommitReady,
            )),
            RoundOutcome::Planned(TreeEventOutcome::Applied(JobState::Succeeded)),
        ];
        let error = DbError::OutputsTooLarge {
            job_id: ROOT_ID,
            size: MOCK_MAX_OUTPUTS_SIZE + 1,
            limit: MOCK_MAX_OUTPUTS_SIZE,
        };
        let expected_message = error.to_string();

        fail_culprit(&mut in_flight, &outcomes, None, error)?;
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
    async fn a_rejected_child_payload_fails_only_the_registering_event() -> anyhow::Result<()> {
        let children = create_test_prepared_children(1).await;
        let planned_registration = PlannedRegistration {
            event_index: 1,
            parent_id: ROOT_ID,
            children: ChildJobUsage::of(&children),
        };
        let mut in_flight = vec![
            pending(ROOT_ID.get() + 1, regular_tasks_done()),
            pending(ROOT_ID.get(), registration(children)),
            pending(ROOT_ID.get(), start()),
        ];
        let outcomes = [
            RoundOutcome::Planned(TreeEventOutcome::Untracked),
            RoundOutcome::Planned(TreeEventOutcome::Applied(JobState::BlockingOnChildren)),
            RoundOutcome::Planned(TreeEventOutcome::Stale(StaleStateError::JobAlreadyStarted)),
        ];

        for (error, round_registration) in [
            (payload_rejected(), None),
            (
                DbError::Sql(sqlx::Error::PoolTimedOut),
                Some(&planned_registration),
            ),
        ] {
            let result = fail_culprit(&mut in_flight, &outcomes, round_registration, error);
            assert!(
                matches!(result, Err(CacheError::Internal(InternalError::Db(_)))),
                "a write error that no registering event causes gave {result:?}"
            );
        }
        assert!(
            in_flight.iter().all(|pending| !matches!(
                pending.event.kind,
                TreeEventKind::TerminalFailure { .. }
            )),
            "an error that no event causes should fail no event"
        );

        fail_culprit(
            &mut in_flight,
            &outcomes,
            Some(&planned_registration),
            payload_rejected(),
        )?;
        let [other_job_completion, failed_registration, stale_start] = in_flight.as_slice() else {
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
            matches!(
                &failed_registration.event.kind,
                TreeEventKind::TerminalFailure {
                    lane: Lane::Regular,
                    message,
                } if message == "child payload rejected by the database"
            ),
            "the registering event should become its job's failure"
        );
        assert!(
            matches!(stale_start.event.kind, TreeEventKind::Start { .. }),
            "a stale event of the job should be kept"
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
