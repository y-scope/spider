//! Unit tests for the tree committer's loop and its context guard.

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
fn an_armed_context_guard_shuts_the_service_down_fatally_even_during_a_graceful_shutdown() {
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

        drop(CommitterContextGuard {
            receivers,
            pending_queue: PendingQueue::default(),
            in_flight_reply: None,
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
