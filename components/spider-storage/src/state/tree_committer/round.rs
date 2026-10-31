//! A round of a tree's committer: one event planned, written, applied, answered and published.

use std::collections::HashMap;
use std::sync::Arc;

use spider_core::job::JobState;
use spider_core::types::id::JobId;
use tokio::sync::oneshot;

use crate::cache::error::CacheError;
use crate::cache::job::SharedJobControlBlock;
use crate::cache::tree::Lane;
use crate::cache::tree::Plan;
use crate::cache::tree::PlannedUpdates;
use crate::cache::tree::Publication;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::TreeEventKind;
use crate::cache::tree::TreeEventOutcome;
use crate::cache::tree::TreeEventReply;
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
        SharedJobControlBlock<
            InboundQueueSenderType,
            DbConnectorType,
            TaskInstancePoolConnectorType,
        >,
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
        root: SharedJobControlBlock<
            InboundQueueSenderType,
            DbConnectorType,
            TaskInstancePoolConnectorType,
        >,
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

    /// Runs one round for `event`, answering it through `in_flight_reply`, which
    /// [`super::CommitterContextGuard`] owns.
    ///
    /// # Returns
    ///
    /// The failure to plan in place of `event`, whose reply stays in `in_flight_reply`, if the
    /// database rejected the outputs of `event`'s job as too large; [`None`] once the round has
    /// answered `event`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::execute_plan`]'s return values on failure.
    /// * Forwards [`Self::publish`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * `in_flight_reply` is empty when the round answers its event.
    /// * [`Self::execute_plan`] or [`Self::publish`] panics.
    pub(super) async fn run_round(
        &mut self,
        event: TreeEvent,
        in_flight_reply: &mut Option<oneshot::Sender<TreeEventReply>>,
    ) -> Result<Option<TreeEvent>, CacheError> {
        let job_id = event.job_id;
        let (outcome, publications) = match self.structure.plan_event(event) {
            Plan::Apply(plan) => match self.execute_plan(job_id, plan).await? {
                Execution::Executed {
                    new_state,
                    publications,
                } => (TreeEventOutcome::Applied(new_state), publications),
                Execution::OutputsRejected(failure) => return Ok(Some(failure)),
            },
            Plan::Stale(error) => (TreeEventOutcome::Stale(error), Vec::new()),
            Plan::Untracked => (TreeEventOutcome::Untracked, Vec::new()),
        };

        // The event's waiter may have dropped its future, which is not an error of the committer.
        let _ = in_flight_reply
            .take()
            .expect("a round should answer its event once")
            .send(Ok(outcome));

        for publication in publications {
            self.publish(publication).await?;
        }
        Ok(None)
    }

    /// # Returns
    ///
    /// Whether the committer's tree has no job left.
    pub(super) fn is_quiescent(&self) -> bool {
        self.structure.is_quiescent()
    }

    /// Writes `plan` to the database and applies it to the cache.
    ///
    /// # Returns
    ///
    /// [`Execution::Executed`] once the plan is written and applied, or
    /// [`Execution::OutputsRejected`] if the database rejects the outputs of `job_id` as too large.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`fail_job_with_oversized_outputs`]'s return values on failure.
    /// * Forwards [`Self::write_to_db`]'s return values on failure.
    /// * Forwards [`Self::apply_to_cache`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::apply_to_cache`] panics.
    async fn execute_plan(
        &mut self,
        job_id: JobId,
        plan: PlannedUpdates,
    ) -> Result<Execution, CacheError> {
        let WrittenPlan {
            transitions,
            publications,
            new_state,
        } = match self.write_to_db(plan).await {
            Ok(written_plan) => written_plan,
            Err(error) => {
                return Ok(Execution::OutputsRejected(fail_job_with_oversized_outputs(
                    job_id, error,
                )?));
            }
        };
        self.hit_hook(HookPoint::CommitterRoundAfterDbWrite, job_id)
            .await;

        for transition in transitions {
            self.apply_to_cache(transition).await?;
        }
        Ok(Execution::Executed {
            new_state,
            publications,
        })
    }

    /// Writes `plan`'s updates to the database in one transaction.
    ///
    /// # Returns
    ///
    /// The written plan, with its updates as the transitions to apply to the cached jobs, on
    /// success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`InternalJobOrchestration::apply_tree_transition`]'s return values on failure.
    async fn write_to_db(&self, plan: PlannedUpdates) -> Result<WrittenPlan, DbError> {
        let PlannedUpdates {
            updates,
            publications,
            new_state,
        } = plan;
        let transitions = updates.iter().map(Transition::from).collect();
        self.env
            .db
            .apply_tree_transition(TreeTransition { updates })
            .await?;
        Ok(WrittenPlan {
            transitions,
            publications,
            new_state,
        })
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
    ) -> &SharedJobControlBlock<
        InboundQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    > {
        self.jcbs
            .get(&job_id)
            .expect("a job of the tree's structure should have its control block in the committer")
    }

    /// Fires the committer's hooks at `point` for the job `job_id`.
    async fn hit_hook(&self, point: HookPoint, job_id: JobId) {
        // A pause that the service's shutdown ends lets the round go on, as the round would without
        // the hook.
        let _ = self.env.hooks.hit(point, || HookContext::job(job_id)).await;
    }
}

/// A plan written to the database: the transitions to apply to the cached jobs, the publications
/// and the event's new state.
struct WrittenPlan {
    transitions: Vec<Transition>,
    publications: Vec<Publication>,
    new_state: JobState,
}

/// The result of executing a plan.
enum Execution {
    /// The plan is written and applied. Carries the event's new state and the plan's publications.
    Executed {
        new_state: JobState,
        publications: Vec<Publication>,
    },

    /// The database rejected the outputs of the event's job as too large, so nothing is written.
    /// Carries the failure of the job to plan in place of the event.
    OutputsRejected(TreeEvent),
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

/// Turns a [`DbError::OutputsTooLarge`] of the round's job `job_id` into a failure of that job on
/// the regular lane.
///
/// # Returns
///
/// The failure event on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`CacheError`] converted from `error` if it is not a [`DbError::OutputsTooLarge`] of `job_id`.
fn fail_job_with_oversized_outputs(job_id: JobId, error: DbError) -> Result<TreeEvent, CacheError> {
    let DbError::OutputsTooLarge {
        job_id: culprit_id, ..
    } = &error
    else {
        return Err(error.into());
    };
    if *culprit_id != job_id {
        return Err(error.into());
    }
    tracing::warn!(
        job_id = ? job_id,
        error = % error,
        "The database rejected a job's outputs. Failing the job."
    );
    Ok(TreeEvent {
        job_id,
        kind: TreeEventKind::TerminalFailure {
            lane: Lane::Regular,
            message: error.to_string(),
        },
    })
}

#[cfg(test)]
mod tests {
    use spider_utils::shutdown::ShutdownHandle;
    use tokio::sync::mpsc;
    use tokio_util::task::TaskTracker;

    use super::*;
    use crate::cache::error::InternalError;
    use crate::cache::error::StaleStateError;
    use crate::cache::tree::TreeControlBlock;
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
            tree.db.errors.get(&ROOT_ID).map(|message| message.clone()),
            Some(expected_message)
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
        let root = create_test_jcb(ROOT_ID).await;
        root.apply_tree_state(JobState::Ready, JobState::Cancelled)
            .await?;
        let shutdown_handle = ShutdownHandle::new();
        let (_control_block, receivers) = TreeControlBlock::new(shutdown_handle.clone());
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
