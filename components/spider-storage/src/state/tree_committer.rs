//! The committer of a job tree.
//!
//! The committer of a job tree is the one task per tree that owns the tree's event receivers, its
//! structure and its non-terminal jobs' control blocks, and is the only writer of its jobs' states.
//! It takes one event at a time, fast-forward events first; a *round* plans the event, writes the
//! plan to the database, applies it to the cached jobs and the structure, enqueues newly terminal
//! jobs for cache GC, replies, and publishes the ready work. The committer exits when its tree is
//! quiescent or the service shuts down; any other exit shuts the service down fatally.

use std::collections::HashMap;
use std::collections::VecDeque;

use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_utils::shutdown::ShutdownHandle;
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;

use crate::cache::error::CacheError;
use crate::cache::job::SharedJobControlBlock;
use crate::cache::tree::EventReceivers;
use crate::cache::tree::Lane;
use crate::cache::tree::Pending;
use crate::cache::tree::Plan;
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
use crate::state::JobCacheGcHandle;
use crate::task_instance_pool::TaskInstancePoolConnector;
use crate::test_hooks::HookContext;
use crate::test_hooks::HookPoint;
use crate::test_hooks::TestHooks;

/// Everything a tree's committer uses besides its tree.
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
pub struct CommitterEnv<
    InboundQueueSenderType: InboundQueueSender,
    DbConnectorType: InternalJobOrchestration,
> {
    pub db: DbConnectorType,
    pub job_cache_gc_handle: JobCacheGcHandle,
    pub inbound_queue_sender: InboundQueueSenderType,
    pub shutdown_handle: ShutdownHandle,
    pub hooks: TestHooks,
}

/// Spawns into `task_tracker` the committer of the one-job tree rooted at `root`, which receives
/// the tree's events from `receivers` until the tree is quiescent or `env.shutdown_handle` is
/// cancelled.
///
/// `root` must not be terminal: a terminal root panics the spawned committer, which shuts the
/// service down fatally.
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector, which only
///   types the tree's job control blocks.
pub fn spawn_committer<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: InternalJobOrchestration + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
>(
    task_tracker: &TaskTracker,
    env: CommitterEnv<InboundQueueSenderType, DbConnectorType>,
    root: SharedJobControlBlock<
        InboundQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    >,
    receivers: EventReceivers,
) {
    task_tracker.spawn(run_committer(env, root, receivers));
}

/// The state that a tree's committer owns without a lock: its structure, which holds every
/// non-terminal job's state, and strong references to the control blocks of exactly those jobs.
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
struct Committer<
    InboundQueueSenderType: InboundQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    env: CommitterEnv<InboundQueueSenderType, DbConnectorType>,
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
    async fn new_root(
        env: CommitterEnv<InboundQueueSenderType, DbConnectorType>,
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

    /// Runs one round for `event`, answering it through `in_flight_reply`, which the loop owns so
    /// that the reply outlives the poison guard.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`fail_culprit`]'s return values on failure.
    /// * Forwards [`Self::write`]'s return values on failure.
    /// * Forwards [`Self::apply`]'s return values on failure.
    /// * Forwards [`Self::publish`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * `in_flight_reply` is empty when the round answers its event.
    /// * The failure that replaces the event of a job whose outputs are too large does not plan a
    ///   transition.
    /// * [`Self::apply`] or [`Self::publish`] panics.
    async fn run_round(
        &mut self,
        event: TreeEvent,
        in_flight_reply: &mut Option<oneshot::Sender<TreeEventReply>>,
    ) -> Result<(), CacheError> {
        const ANSWERED_ONCE: &str = "a round should answer its event once";

        let job_id = event.job_id;
        let (updates, publications, new_state) = match self.structure.plan_event(event) {
            Plan::Apply {
                updates,
                publications,
                new_state,
            } => (updates, publications, new_state),
            Plan::Stale(error) => {
                answer(
                    in_flight_reply.take().expect(ANSWERED_ONCE),
                    Ok(TreeEventOutcome::Stale(error)),
                );
                return Ok(());
            }
            Plan::Untracked => {
                answer(
                    in_flight_reply.take().expect(ANSWERED_ONCE),
                    Ok(TreeEventOutcome::Untracked),
                );
                return Ok(());
            }
        };
        let (transitions, publications, new_state) = match self.write(updates).await {
            Ok(transitions) => (transitions, publications, new_state),
            Err(error) => {
                let failure = fail_culprit(job_id, error)?;
                let Plan::Apply {
                    updates,
                    publications,
                    new_state,
                } = self.structure.plan_event(failure)
                else {
                    unreachable!(
                        "a regular-lane failure of a running job should plan a transition"
                    );
                };
                (self.write(updates).await?, publications, new_state)
            }
        };
        self.hit_hook(HookPoint::CommitterRoundAfterDbWrite, job_id)
            .await;
        for transition in transitions {
            self.apply(transition).await?;
        }
        answer(
            in_flight_reply.take().expect(ANSWERED_ONCE),
            Ok(TreeEventOutcome::Applied(new_state)),
        );
        for publication in publications {
            self.publish(publication).await?;
        }
        Ok(())
    }

    /// Writes a plan's updates to the database in one transaction.
    ///
    /// # Returns
    ///
    /// The transitions to apply to the cached jobs after the write on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`InternalJobOrchestration::apply_tree_transition`]'s return values on failure.
    async fn write(&self, updates: Vec<JobStateUpdate>) -> Result<Vec<Transition>, DbError> {
        let transitions = updates.iter().map(Transition::from).collect();
        self.env
            .db
            .apply_tree_transition(TreeTransition { updates })
            .await?;
        Ok(transitions)
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
    async fn apply(&mut self, transition: Transition) -> Result<(), CacheError> {
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

/// The committer's received events not yet taken, one buffer per event channel.
#[derive(Default)]
struct Buffers {
    fast_forward: VecDeque<Pending>,
    regular: VecDeque<Pending>,
}

impl Buffers {
    /// # Returns
    ///
    /// Whether neither buffer holds an event.
    // TODO: Make this function `const` once stable Rust allows `VecDeque::is_empty` in `const fn`
    // (see #544).
    #[allow(clippy::missing_const_for_fn)]
    fn is_empty(&self) -> bool {
        self.fast_forward.is_empty() && self.regular.is_empty()
    }

    /// Appends `pending` to the buffer of its event's channel.
    fn push_back(&mut self, pending: Pending) {
        if pending.event.kind.is_fast_forward() {
            self.fast_forward.push_back(pending);
        } else {
            self.regular.push_back(pending);
        }
    }

    /// Takes the oldest fast-forward event, or else the oldest regular event.
    ///
    /// # Returns
    ///
    /// The taken event, or [`None`] if both buffers are empty.
    fn take(&mut self) -> Option<Pending> {
        self.fast_forward
            .pop_front()
            .or_else(|| self.regular.pop_front())
    }

    /// Moves every event queued in `receivers` to the buffers without waiting: every fast-forward
    /// event, then every regular one.
    fn receive(&mut self, receivers: &mut EventReceivers) {
        while let Ok(pending) = receivers.fast_forward.try_recv() {
            self.push_back(pending);
        }
        while let Ok(pending) = receivers.regular.try_recv() {
            self.push_back(pending);
        }
    }
}

/// Owns the tree's event receivers. Dropping it armed shuts the service down fatally before the
/// receivers drop; the receivers then drop every queued event's reply, which wakes its waiter with
/// [`InternalError::TreePoisoned`](crate::cache::error::InternalError::TreePoisoned).
struct PoisonOnAbnormalExit {
    receivers: EventReceivers,
    shutdown_handle: ShutdownHandle,
    root_id: JobId,
    is_disarmed: bool,
}

impl PoisonOnAbnormalExit {
    /// Closes the tree's event receivers, answers every event the committer received and every
    /// event still queued with `make_reply()`, and disarms the guard.
    ///
    /// # Type Parameters
    ///
    /// * `MakeReplyType` - The type of the closure that builds the reply to each event.
    async fn close_and_answer<MakeReplyType: Fn() -> TreeEventReply>(
        mut self,
        mut buffers: Buffers,
        make_reply: MakeReplyType,
    ) {
        self.receivers.fast_forward.close();
        self.receivers.regular.close();
        // `try_recv` can miss a send in flight after `close`; `recv` returns `None` only once none
        // is.
        while let Some(pending) = self.receivers.fast_forward.recv().await {
            buffers.push_back(pending);
        }
        while let Some(pending) = self.receivers.regular.recv().await {
            buffers.push_back(pending);
        }
        while let Some(Pending { reply, .. }) = buffers.take() {
            answer(reply, make_reply());
        }
        self.is_disarmed = true;
    }
}

impl Drop for PoisonOnAbnormalExit {
    fn drop(&mut self) {
        if self.is_disarmed {
            return;
        }
        tracing::error!(
            root_id = ? self.root_id,
            "Tree committer exited abnormally. Shutting down the service."
        );
        self.shutdown_handle.shut_down_fatally();
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

/// Runs a tree's committer until its tree is quiescent or the service shuts down.
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
async fn run_committer<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: InternalJobOrchestration + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
>(
    env: CommitterEnv<InboundQueueSenderType, DbConnectorType>,
    root: SharedJobControlBlock<
        InboundQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    >,
    receivers: EventReceivers,
) {
    let root_id = root.id();
    let shutdown_handle = env.shutdown_handle.clone();
    // Declared before the guard so that they drop after it has shut the service down.
    let mut buffers = Buffers::default();
    let mut in_flight_reply: Option<oneshot::Sender<TreeEventReply>> = None;
    let mut poison_on_abnormal_exit = PoisonOnAbnormalExit {
        receivers,
        shutdown_handle: shutdown_handle.clone(),
        root_id,
        is_disarmed: false,
    };
    let mut committer = Committer::new_root(env, root).await;
    loop {
        if buffers.is_empty() {
            tokio::select! {
                biased;
                Some(pending) = poison_on_abnormal_exit.receivers.fast_forward.recv() => {
                    buffers.push_back(pending);
                }
                Some(pending) = poison_on_abnormal_exit.receivers.regular.recv() => {
                    buffers.push_back(pending);
                }
                () = shutdown_handle.cancelled() => {}
            }
        }
        buffers.receive(&mut poison_on_abnormal_exit.receivers);
        if shutdown_handle.is_cancelled() {
            poison_on_abnormal_exit
                .close_and_answer(buffers, || Err(CacheError::ServiceStopping))
                .await;
            return;
        }
        let Pending { event, reply } = buffers
            .take()
            .expect("a buffer should hold an event once the committer wakes before a shutdown");
        in_flight_reply.replace(reply);
        if let Err(error) = committer.run_round(event, &mut in_flight_reply).await {
            tracing::error!(
                root_id = ? root_id,
                error = % error,
                "A round of a tree's committer failed."
            );
            return;
        }
        if committer.structure.is_quiescent() {
            poison_on_abnormal_exit
                .close_and_answer(buffers, || Ok(TreeEventOutcome::Untracked))
                .await;
            return;
        }
    }
}

/// Turns a database error that the outputs of the round's job `job_id` cause into a failure of that
/// job on the regular lane.
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
fn fail_culprit(job_id: JobId, error: DbError) -> Result<TreeEvent, CacheError> {
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

/// Answers an event through its `reply` sender with `outcome`.
fn answer(reply: oneshot::Sender<TreeEventReply>, outcome: TreeEventReply) {
    // The event's waiter may have dropped its future, which is not an error of the committer.
    let _ = reply.send(outcome);
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use spider_core::task::TdlContext;
    use spider_core::task::TerminationTaskDescriptor;
    use spider_core::types::id::ResourceGroupId;
    use tokio::sync::mpsc;

    use super::*;
    use crate::cache::error::InternalError;
    use crate::cache::error::StaleStateError;
    use crate::cache::tree::TreeControlBlock;
    use crate::inbound_queue::CleanupTaskMarker;
    use crate::inbound_queue::CommitTaskMarker;
    use crate::inbound_queue::InboundQueueEntry;
    use crate::inbound_queue::create_inbound_queue;
    use crate::job_submission::create_validated_submission;
    use crate::state::test_utils::MOCK_MAX_OUTPUTS_SIZE;
    use crate::state::test_utils::MockDbConnector;
    use crate::state::test_utils::MockInboundQueueSender;
    use crate::state::test_utils::MockTaskInstancePoolConnector;
    use crate::state::test_utils::create_test_jcb;
    use crate::state::test_utils::create_test_task_graph_input;
    use crate::state::test_utils::create_test_task_graph_with_termination_tasks;

    const ROOT_ID: JobId = JobId::from(1);

    const TIMEOUT: Duration = Duration::from_secs(5);

    /// A one-job tree whose committer runs in its own task tracker.
    struct TestTree<InboundQueueSenderType: InboundQueueSender + 'static> {
        control_block: TreeControlBlock,
        jcb: SharedJobControlBlock<
            InboundQueueSenderType,
            MockDbConnector,
            MockTaskInstancePoolConnector,
        >,
        owner_id: ResourceGroupId,
        db: MockDbConnector,
        task_tracker: TaskTracker,
        shutdown_handle: ShutdownHandle,
        #[cfg(feature = "test-hooks")]
        hooks: TestHooks,
        gc_receiver: mpsc::UnboundedReceiver<JobId>,
    }

    impl<InboundQueueSenderType: InboundQueueSender + 'static> TestTree<InboundQueueSenderType> {
        /// Factory function.
        ///
        /// Registers a one-task root job as [`JobState::Ready`] in a mock database, with a commit
        /// task if `has_commit` and a cleanup task if `has_cleanup`, and spawns the committer of
        /// its tree, which publishes through `inbound_queue_sender`.
        ///
        /// # Returns
        ///
        /// The tree.
        ///
        /// # Panics
        ///
        /// Panics if the root's control block cannot be created.
        async fn spawn(
            has_commit: bool,
            has_cleanup: bool,
            inbound_queue_sender: InboundQueueSenderType,
        ) -> Self {
            let owner_id = ResourceGroupId::random();
            let db = MockDbConnector::default();
            db.states.insert(ROOT_ID, JobState::Ready);
            let shutdown_handle = ShutdownHandle::new();
            let hooks = TestHooks::default();
            hooks.register_runtime(&shutdown_handle);
            let task_graph = create_test_task_graph_with_termination_tasks(
                has_commit.then(termination_task),
                has_cleanup.then(termination_task),
            );
            let jcb = SharedJobControlBlock::create(
                ROOT_ID,
                owner_id,
                create_validated_submission(task_graph, create_test_task_graph_input()),
                inbound_queue_sender.clone(),
                db.clone(),
                MockTaskInstancePoolConnector,
                hooks.clone(),
            )
            .await
            .expect("the root's control block should be created");
            let (gc_sender, gc_receiver) = mpsc::unbounded_channel();
            let (control_block, receivers) = TreeControlBlock::new(shutdown_handle.clone());
            let task_tracker = TaskTracker::new();
            spawn_committer(
                &task_tracker,
                CommitterEnv {
                    db: db.clone(),
                    job_cache_gc_handle: JobCacheGcHandle::new(gc_sender),
                    inbound_queue_sender,
                    shutdown_handle: shutdown_handle.clone(),
                    hooks: hooks.clone(),
                },
                jcb.clone(),
                receivers,
            );
            Self {
                control_block,
                jcb,
                owner_id,
                db,
                task_tracker,
                shutdown_handle,
                #[cfg(feature = "test-hooks")]
                hooks,
                gc_receiver,
            }
        }

        /// Submits an event of `kind` of the root.
        ///
        /// # Returns
        ///
        /// A future that resolves to the event's reply.
        fn submit(
            &self,
            kind: TreeEventKind,
        ) -> impl Future<Output = TreeEventReply> + use<InboundQueueSenderType> {
            self.control_block.submit(TreeEvent {
                job_id: ROOT_ID,
                kind,
            })
        }

        /// Waits until the tree's committer exits.
        ///
        /// # Panics
        ///
        /// Panics if the committer does not exit within 5 seconds.
        async fn wait_for_exit(&self) {
            self.task_tracker.close();
            tokio::time::timeout(TIMEOUT, self.task_tracker.wait())
                .await
                .expect("the committer should exit within 5 seconds");
        }
    }

    fn termination_task() -> TerminationTaskDescriptor {
        TerminationTaskDescriptor {
            tdl_context: TdlContext {
                package: "test_pkg".to_owned(),
                task_func: "test_termination_fn".to_owned(),
            },
            execution_policy: None,
        }
    }

    fn start() -> TreeEventKind {
        TreeEventKind::Start {
            ready_task_indices: vec![0],
        }
    }

    fn regular_tasks_done() -> TreeEventKind {
        TreeEventKind::RegularTasksDone {
            outputs: vec![vec![1]],
        }
    }

    /// Awaits the reply to an event.
    ///
    /// # Returns
    ///
    /// The reply that `future` resolves to.
    ///
    /// # Panics
    ///
    /// Panics if `future` does not resolve within 5 seconds.
    async fn reply(future: impl Future<Output = TreeEventReply>) -> TreeEventReply {
        tokio::time::timeout(TIMEOUT, future)
            .await
            .expect("the event should be answered within 5 seconds")
    }

    #[tokio::test]
    async fn an_idle_committer_exits_when_the_service_shuts_down() {
        let tree = TestTree::spawn(false, false, MockInboundQueueSender).await;
        tokio::task::yield_now().await;

        tree.shutdown_handle.shut_down_gracefully();
        tree.wait_for_exit().await;
        assert!(!tree.shutdown_handle.is_fatal());
    }

    #[tokio::test]
    async fn an_error_in_a_round_poisons_the_service_and_wakes_every_waiter_with_tree_poisoned() {
        let tree = TestTree::spawn(false, false, MockInboundQueueSender).await;
        tree.db.states.remove(&ROOT_ID);

        let start = tree.submit(start());
        let cancel = tree.submit(TreeEventKind::CancelRoot);
        for reply in [reply(start).await, reply(cancel).await] {
            assert!(
                matches!(
                    reply,
                    Err(CacheError::Internal(InternalError::TreePoisoned))
                ),
                "an event of a failed committer gave {reply:?}"
            );
        }
        tree.wait_for_exit().await;
        assert!(tree.shutdown_handle.is_fatal());
    }

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
    async fn a_fast_forward_event_is_planned_before_an_earlier_regular_event() {
        let tree = TestTree::spawn(false, false, MockInboundQueueSender).await;

        let start = tree.submit(start());
        let cancel = tree.submit(TreeEventKind::CancelRoot);
        let cancel = reply(cancel).await;
        assert!(
            matches!(cancel, Ok(TreeEventOutcome::Applied(JobState::Cancelled))),
            "a cancel gave {cancel:?}"
        );
        let start = reply(start).await;
        assert!(
            matches!(start, Ok(TreeEventOutcome::Untracked)),
            "a start submitted before the cancel gave {start:?}"
        );
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

    #[test]
    fn an_armed_poison_guard_shuts_the_service_down_fatally_even_during_a_graceful_shutdown() {
        for (is_gracefully_cancelled_first, is_disarmed, is_cancelled, is_fatal) in [
            (false, false, true, true),
            (true, false, true, true),
            (false, true, false, false),
        ] {
            let shutdown_handle = ShutdownHandle::new();
            let (_control_block, receivers) = TreeControlBlock::new(shutdown_handle.clone());
            if is_gracefully_cancelled_first {
                shutdown_handle.shut_down_gracefully();
            }

            drop(PoisonOnAbnormalExit {
                receivers,
                shutdown_handle: shutdown_handle.clone(),
                root_id: ROOT_ID,
                is_disarmed,
            });
            assert_eq!(
                (shutdown_handle.is_cancelled(), shutdown_handle.is_fatal()),
                (is_cancelled, is_fatal),
                "a guard dropped with is_disarmed {is_disarmed} after a graceful shutdown \
                 {is_gracefully_cancelled_first}"
            );
        }
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
            CommitterEnv {
                db: MockDbConnector::default(),
                job_cache_gc_handle: JobCacheGcHandle::new(mpsc::unbounded_channel().0),
                inbound_queue_sender: MockInboundQueueSender,
                shutdown_handle: shutdown_handle.clone(),
                hooks: TestHooks::default(),
            },
            root,
            receivers,
        );
        task_tracker.close();
        tokio::time::timeout(TIMEOUT, task_tracker.wait()).await?;
        assert!(shutdown_handle.is_fatal());
        Ok(())
    }

    #[cfg(feature = "test-hooks")]
    mod hooked {
        use spider_core::task::TaskIndex;

        use super::*;
        use crate::test_hooks::ArmedHook;
        use crate::test_hooks::HookAction;
        use crate::test_hooks::HookCount;
        use crate::test_hooks::HookFilter;

        /// An inbound queue sender that panics on every send.
        #[derive(Clone)]
        struct PanickingInboundQueueSender;

        #[async_trait::async_trait]
        impl InboundQueueSender for PanickingInboundQueueSender {
            async fn send_task_ready(
                &self,
                _resource_group_id: ResourceGroupId,
                _job_id: JobId,
                _task_indices: Vec<TaskIndex>,
            ) -> Result<(), InternalError> {
                panic!("the test's inbound queue sender should panic on a send");
            }

            async fn send_commit_ready(
                &self,
                _resource_group_id: ResourceGroupId,
                _job_id: JobId,
            ) -> Result<(), InternalError> {
                panic!("the test's inbound queue sender should panic on a send");
            }

            async fn send_cleanup_ready(
                &self,
                _resource_group_id: ResourceGroupId,
                _job_id: JobId,
            ) -> Result<(), InternalError> {
                panic!("the test's inbound queue sender should panic on a send");
            }
        }

        /// Arms a pause, for one hit, of the rounds of `tree`'s committer after their database
        /// write.
        ///
        /// # Returns
        ///
        /// The armed pause.
        fn arm_pause<InboundQueueSenderType: InboundQueueSender + 'static>(
            tree: &TestTree<InboundQueueSenderType>,
        ) -> ArmedHook {
            tree.hooks.arm(
                HookPoint::CommitterRoundAfterDbWrite,
                HookFilter::job(ROOT_ID),
                HookAction::Pause,
                HookCount::Once,
            )
        }

        #[tokio::test]
        async fn committer_exits_once_its_tree_is_quiescent_and_answers_waiting_events_untracked()
        -> anyhow::Result<()> {
            let mut tree = TestTree::spawn(false, false, MockInboundQueueSender).await;
            let start = reply(tree.submit(start())).await;
            assert!(
                matches!(start, Ok(TreeEventOutcome::Applied(JobState::Running))),
                "a start gave {start:?}"
            );

            let pause = arm_pause(&tree);
            let regular_tasks_done = tree.submit(regular_tasks_done());
            let commit_done = tree.submit(TreeEventKind::CommitDone);
            pause.reached(1, TIMEOUT).await?;
            let cancel = tree.submit(TreeEventKind::CancelRoot);
            pause.release();

            let regular_tasks_done = reply(regular_tasks_done).await;
            assert!(
                matches!(
                    regular_tasks_done,
                    Ok(TreeEventOutcome::Applied(JobState::Succeeded))
                ),
                "the regular tasks being done gave {regular_tasks_done:?}"
            );
            for reply in [reply(commit_done).await, reply(cancel).await] {
                assert!(
                    matches!(reply, Ok(TreeEventOutcome::Untracked)),
                    "an event waiting when the tree became quiescent gave {reply:?}"
                );
            }
            assert_eq!(tree.gc_receiver.try_recv(), Ok(ROOT_ID));
            tree.wait_for_exit().await;
            assert!(!tree.shutdown_handle.is_cancelled());
            assert!(!tree.shutdown_handle.is_fatal());
            Ok(())
        }

        #[tokio::test]
        async fn shutdown_finishes_the_round_in_flight_and_answers_waiting_events_with_stopping()
        -> anyhow::Result<()> {
            let (inbound_queue_sender, inbound_queue_receiver) = create_inbound_queue();
            let tree = TestTree::spawn(false, false, inbound_queue_sender).await;
            let pause = arm_pause(&tree);
            let start = tree.submit(start());
            tokio::pin!(start);
            pause.reached(1, TIMEOUT).await?;

            assert_eq!(
                tree.db.states.get(&ROOT_ID).map(|state| *state),
                Some(JobState::Running)
            );
            assert_eq!(tree.jcb.state().await, JobState::Ready);
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut start)
                    .await
                    .is_err(),
                "a start should not be answered before its apply"
            );
            assert_eq!(
                inbound_queue_receiver.recv_tasks(1, Duration::ZERO).await?,
                []
            );

            let cancel = tree.submit(TreeEventKind::CancelRoot);
            tree.shutdown_handle.shut_down_gracefully();

            let start = reply(start).await;
            assert!(
                matches!(start, Ok(TreeEventOutcome::Applied(JobState::Running))),
                "the start in flight gave {start:?}"
            );
            let cancel = reply(cancel).await;
            assert!(
                matches!(cancel, Err(CacheError::ServiceStopping)),
                "a cancel waiting during a shutdown gave {cancel:?}"
            );
            assert_eq!(
                inbound_queue_receiver.recv_tasks(1, TIMEOUT).await?,
                [InboundQueueEntry {
                    resource_group_id: tree.owner_id,
                    job_id: ROOT_ID,
                    task_kind: 0,
                }]
            );
            tree.wait_for_exit().await;
            assert!(!tree.shutdown_handle.is_fatal());
            assert_eq!(tree.jcb.state().await, JobState::Running);
            assert_eq!(
                tree.db.states.get(&ROOT_ID).map(|state| *state),
                Some(JobState::Running)
            );
            Ok(())
        }

        #[tokio::test]
        async fn a_panic_in_a_round_poisons_the_service_even_during_a_graceful_shutdown()
        -> anyhow::Result<()> {
            let tree = TestTree::spawn(false, false, PanickingInboundQueueSender).await;
            let pause = arm_pause(&tree);
            let start = tree.submit(start());
            pause.reached(1, TIMEOUT).await?;
            let cancel = tree.submit(TreeEventKind::CancelRoot);
            tree.shutdown_handle.shut_down_gracefully();

            let start = reply(start).await;
            assert!(
                matches!(start, Ok(TreeEventOutcome::Applied(JobState::Running))),
                "a start whose publish panics gave {start:?}"
            );
            let cancel = reply(cancel).await;
            assert!(
                matches!(
                    cancel,
                    Err(CacheError::Internal(InternalError::TreePoisoned))
                ),
                "a cancel waiting when the committer panicked gave {cancel:?}"
            );
            tree.wait_for_exit().await;
            assert!(tree.shutdown_handle.is_fatal());
            Ok(())
        }
    }
}
