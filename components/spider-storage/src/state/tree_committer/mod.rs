//! The committer of a job tree.
//!
//! Each tree has one committer, a coroutine that is the only writer of its jobs' states. It owns:
//!
//! * The tree's event receivers.
//! * The tree's structure.
//! * The control blocks of the tree's non-terminal jobs.
//!
//! The committer takes one event at a time (fast-forward events first) and runs a *round* for it,
//! where each round executes the following steps:
//!
//! 1. Plans the event against the tree's structure.
//! 2. Writes the plan to the database.
//! 3. Applies the plan to the cached jobs and the structure, and enqueues newly terminal jobs for
//!    cache GC.
//! 4. Replies to the event.
//! 5. Publishes the ready work.
//!
//! The committer exits once its tree is quiescent or the service shuts down. Any other exit, from
//! an error or a panic, shuts the service down fatally.

mod round;
#[cfg(test)]
mod test_utils;

use std::collections::VecDeque;

use spider_core::types::id::JobId;
use spider_utils::shutdown::ShutdownHandle;
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;

use crate::cache::error::CacheError;
use crate::cache::job::SharedJobControlBlock;
use crate::cache::tree::EventReceivers;
use crate::cache::tree::Pending;
use crate::cache::tree::TreeEventOutcome;
use crate::cache::tree::TreeEventReply;
use crate::db::InternalJobOrchestration;
use crate::inbound_queue::InboundQueueSender;
use crate::state::JobCacheGcHandle;
use crate::state::tree_committer::round::Committer;
use crate::task_instance_pool::TaskInstancePoolConnector;
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

/// The committer's queue of received events that are not yet planned, whose [`Self::take`] returns
/// the fast-forward events first.
#[derive(Default)]
struct PendingQueue {
    fast_forward: VecDeque<Pending>,
    regular: VecDeque<Pending>,
}

impl PendingQueue {
    /// # Returns
    ///
    /// Whether the queue holds no event.
    // TODO: Make this function `const` once stable Rust allows `VecDeque::is_empty` in `const fn`
    // (see #544).
    #[allow(clippy::missing_const_for_fn)]
    fn is_empty(&self) -> bool {
        self.fast_forward.is_empty() && self.regular.is_empty()
    }

    /// Appends `pending` to the queue, behind the queued events of its channel.
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
    /// The taken event, or [`None`] if the queue is empty.
    fn take(&mut self) -> Option<Pending> {
        self.fast_forward
            .pop_front()
            .or_else(|| self.regular.pop_front())
    }

    /// Moves every event already in `receivers` to the queue without waiting: every fast-forward
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
    /// Closes the tree's event receivers, answers every event in `pending_queue` and every event
    /// still in the receivers with `make_reply()`, and disarms the guard.
    ///
    /// # Type Parameters
    ///
    /// * `MakeReplyType` - The type of the closure that builds the reply to each event.
    async fn close_and_answer<MakeReplyType: Fn() -> TreeEventReply>(
        mut self,
        mut pending_queue: PendingQueue,
        make_reply: MakeReplyType,
    ) {
        self.receivers.fast_forward.close();
        self.receivers.regular.close();
        // `try_recv` can miss a send in flight after `close`; `recv` returns `None` only once none
        // is.
        while let Some(pending) = self.receivers.fast_forward.recv().await {
            pending_queue.push_back(pending);
        }
        while let Some(pending) = self.receivers.regular.recv().await {
            pending_queue.push_back(pending);
        }
        while let Some(Pending { reply, .. }) = pending_queue.take() {
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
    // The pending queue and the in-flight reply are declared before the guard so that they drop
    // after it has shut the service down.
    let mut pending_queue = PendingQueue::default();
    let mut in_flight_reply: Option<oneshot::Sender<TreeEventReply>> = None;
    let mut poison_on_abnormal_exit = PoisonOnAbnormalExit {
        receivers,
        shutdown_handle: shutdown_handle.clone(),
        root_id,
        is_disarmed: false,
    };
    let mut committer = Committer::new_root(env, root).await;
    loop {
        if pending_queue.is_empty() {
            tokio::select! {
                biased;
                Some(pending) = poison_on_abnormal_exit.receivers.fast_forward.recv() => {
                    pending_queue.push_back(pending);
                }
                Some(pending) = poison_on_abnormal_exit.receivers.regular.recv() => {
                    pending_queue.push_back(pending);
                }
                () = shutdown_handle.cancelled() => {}
            }
        }
        pending_queue.receive(&mut poison_on_abnormal_exit.receivers);
        if shutdown_handle.is_cancelled() {
            poison_on_abnormal_exit
                .close_and_answer(pending_queue, || Err(CacheError::ServiceStopping))
                .await;
            return;
        }
        let Pending { event, reply } = pending_queue.take().expect(
            "the pending queue should hold an event once the committer wakes before a shutdown",
        );
        in_flight_reply.replace(reply);
        if let Err(error) = committer.run_round(event, &mut in_flight_reply).await {
            tracing::error!(
                root_id = ? root_id,
                error = % error,
                "A round of a tree's committer failed."
            );
            return;
        }
        if committer.is_quiescent() {
            poison_on_abnormal_exit
                .close_and_answer(pending_queue, || Ok(TreeEventOutcome::Untracked))
                .await;
            return;
        }
    }
}

/// Answers an event through its `reply` sender with `outcome`.
fn answer(reply: oneshot::Sender<TreeEventReply>, outcome: TreeEventReply) {
    // The event's waiter may have dropped its future, which is not an error of the committer.
    let _ = reply.send(outcome);
}

#[cfg(test)]
mod tests {
    use spider_core::job::JobState;

    use super::*;
    use crate::cache::error::InternalError;
    use crate::cache::tree::TreeControlBlock;
    use crate::cache::tree::TreeEventKind;
    use crate::state::test_utils::MockInboundQueueSender;
    use crate::state::tree_committer::test_utils::ROOT_ID;
    use crate::state::tree_committer::test_utils::TestTree;
    use crate::state::tree_committer::test_utils::reply;
    use crate::state::tree_committer::test_utils::start;

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

    #[cfg(feature = "test-hooks")]
    mod hooked {
        use std::time::Duration;

        use spider_core::task::TaskIndex;
        use spider_core::types::id::ResourceGroupId;

        use super::*;
        use crate::inbound_queue::InboundQueueEntry;
        use crate::inbound_queue::create_inbound_queue;
        use crate::state::tree_committer::test_utils::TIMEOUT;
        use crate::state::tree_committer::test_utils::regular_tasks_done;
        use crate::test_hooks::ArmedHook;
        use crate::test_hooks::HookAction;
        use crate::test_hooks::HookCount;
        use crate::test_hooks::HookFilter;
        use crate::test_hooks::HookPoint;

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
