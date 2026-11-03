//! Unit tests for the tree committer's loop, its buffers and its context guard.

use spider_core::job::JobState;

use super::*;
use crate::cache::error::InternalError;
use crate::cache::error::StaleStateError;
use crate::cache::tree::Lane;
use crate::cache::tree::TreeControlBlock;
use crate::cache::tree::TreeEvent;
use crate::state::test_utils::MockInboundQueueSender;
use crate::state::tree_committer::test_utils::ROOT_ID;
use crate::state::tree_committer::test_utils::TestTree;
use crate::state::tree_committer::test_utils::pending;
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
        matches!(
            start,
            Ok(TreeEventOutcome::Stale(StaleStateError::JobAlreadyStarted))
        ),
        "a start submitted before the cancel gave {start:?}"
    );
}

#[test]
fn take_events_fills_a_round_with_cancels_and_failures_first() {
    const NUM_FAST_FORWARD: u64 = 300;
    const NUM_REGULAR: u64 = 5;
    let mut buffers = Buffers {
        fast_forward: (0..NUM_FAST_FORWARD)
            .map(|id| {
                let kind = if id % 2 == 0 {
                    failure("boom")
                } else {
                    TreeEventKind::CancelRoot
                };
                pending(id, kind)
            })
            .collect(),
        regular: (NUM_FAST_FORWARD..NUM_FAST_FORWARD + NUM_REGULAR)
            .map(|id| pending(id, TreeEventKind::CommitDone))
            .collect(),
    };
    let num_cancels = NUM_FAST_FORWARD / 2;
    let num_failures_taken = MAX_EVENTS_PER_ROUND as u64 - num_cancels;

    let round = buffers.take_events();
    assert_eq!(
        job_ids_of(&round),
        (0..num_cancels)
            .map(|index| 2 * index + 1)
            .chain((0..num_failures_taken).map(|index| 2 * index))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        job_ids_of(&buffers.fast_forward),
        (num_failures_taken..num_cancels)
            .map(|index| 2 * index)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        job_ids_of(&buffers.regular),
        (NUM_FAST_FORWARD..NUM_FAST_FORWARD + NUM_REGULAR).collect::<Vec<_>>()
    );

    let round = buffers.take_events();
    assert_eq!(
        job_ids_of(&round),
        (num_failures_taken..num_cancels)
            .map(|index| 2 * index)
            .chain(NUM_FAST_FORWARD..NUM_FAST_FORWARD + NUM_REGULAR)
            .collect::<Vec<_>>()
    );
    assert!(buffers.is_empty());
}

#[test]
fn take_events_takes_the_cancels_beyond_a_round_before_the_failures_of_the_next_round() {
    const NUM_FAILURES: u64 = 10;
    const NUM_FAST_FORWARD: u64 = NUM_FAILURES + MAX_EVENTS_PER_ROUND as u64 + 20;
    const NUM_REGULAR: u64 = 5;
    let mut buffers = Buffers {
        fast_forward: (0..NUM_FAST_FORWARD)
            .map(|id| {
                let kind = if id < NUM_FAILURES {
                    failure("boom")
                } else {
                    TreeEventKind::CancelRoot
                };
                pending(id, kind)
            })
            .collect(),
        regular: (NUM_FAST_FORWARD..NUM_FAST_FORWARD + NUM_REGULAR)
            .map(|id| pending(id, TreeEventKind::CommitDone))
            .collect(),
    };
    let first_cancel_left = NUM_FAILURES + MAX_EVENTS_PER_ROUND as u64;

    let round = buffers.take_events();
    assert_eq!(
        job_ids_of(&round),
        (NUM_FAILURES..first_cancel_left).collect::<Vec<_>>()
    );

    let round = buffers.take_events();
    assert_eq!(
        job_ids_of(&round),
        (first_cancel_left..NUM_FAST_FORWARD)
            .chain(0..NUM_FAILURES)
            .chain(NUM_FAST_FORWARD..NUM_FAST_FORWARD + NUM_REGULAR)
            .collect::<Vec<_>>()
    );
    assert!(buffers.is_empty());
}

#[test]
fn take_events_takes_at_most_a_round_of_regular_events() {
    let mut buffers = Buffers {
        fast_forward: VecDeque::from([pending(0, TreeEventKind::CancelRoot)]),
        regular: (1..=MAX_EVENTS_PER_ROUND as u64)
            .map(|id| pending(id, TreeEventKind::CommitDone))
            .collect(),
    };

    let round = buffers.take_events();
    assert_eq!(
        job_ids_of(&round),
        (0..MAX_EVENTS_PER_ROUND as u64).collect::<Vec<_>>()
    );
    assert_eq!(job_ids_of(&buffers.regular), [MAX_EVENTS_PER_ROUND as u64]);
}

#[test]
fn receive_moves_every_cancel_and_failure_but_at_most_a_round_of_other_events() {
    const NUM_FAST_FORWARD: usize = MAX_EVENTS_PER_ROUND + 44;
    const NUM_REGULAR: usize = MAX_EVENTS_PER_ROUND + 10;
    const NUM_TAKEN: usize = 20;
    let (control_block, mut receivers) = TreeControlBlock::new(ShutdownHandle::new());
    for _ in 0..NUM_REGULAR {
        drop(control_block.submit(TreeEvent {
            job_id: ROOT_ID,
            kind: TreeEventKind::CommitDone,
        }));
    }
    for _ in 0..NUM_FAST_FORWARD {
        drop(control_block.submit(TreeEvent {
            job_id: ROOT_ID,
            kind: TreeEventKind::CancelRoot,
        }));
    }
    let mut buffers = Buffers::default();

    buffers.receive(&mut receivers);
    assert_eq!(
        (buffers.fast_forward.len(), buffers.regular.len()),
        (NUM_FAST_FORWARD, MAX_EVENTS_PER_ROUND)
    );
    buffers.receive(&mut receivers);
    assert_eq!(buffers.regular.len(), MAX_EVENTS_PER_ROUND);

    buffers.regular.drain(..NUM_TAKEN);
    buffers.receive(&mut receivers);
    assert_eq!(buffers.regular.len(), NUM_REGULAR - NUM_TAKEN);
    assert!(
        receivers.fast_forward.is_empty() && receivers.regular.is_empty(),
        "every submitted event should be buffered"
    );
}

#[test]
fn requeued_events_go_back_to_the_front_of_their_own_buffer_in_order() {
    let mut buffers = Buffers {
        fast_forward: VecDeque::from([pending(9, TreeEventKind::CancelRoot)]),
        regular: VecDeque::from([pending(8, start())]),
    };
    buffers.requeue(vec![
        pending(1, failure("first")),
        pending(2, TreeEventKind::CommitDone),
        pending(3, failure("second")),
        pending(4, TreeEventKind::CleanupDone),
    ]);
    assert_eq!(job_ids_of(&buffers.fast_forward), [1, 3, 9]);
    assert_eq!(job_ids_of(&buffers.regular), [2, 4, 8]);
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
            buffers: Buffers::default(),
            in_flight: Vec::new(),
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

/// # Returns
///
/// A terminal failure, with `message`, of a regular task.
fn failure(message: &str) -> TreeEventKind {
    TreeEventKind::TerminalFailure {
        lane: Lane::Regular,
        message: message.to_owned(),
    }
}

/// # Type Parameters
///
/// * `'pending_lifetime` - The lifetime of the borrowed events.
///
/// # Returns
///
/// The job IDs of `events`, in order.
fn job_ids_of<'pending_lifetime>(
    events: impl IntoIterator<Item = &'pending_lifetime Pending>,
) -> Vec<u64> {
    events
        .into_iter()
        .map(|pending| pending.event.job_id.get())
        .collect()
}

#[cfg(feature = "test-hooks")]
mod hooked {
    use std::time::Duration;

    use spider_core::task::TaskIndex;
    use spider_core::types::id::ResourceGroupId;

    use super::*;
    use crate::db::DbError;
    use crate::inbound_queue::InboundQueueEntry;
    use crate::inbound_queue::create_inbound_queue;
    use crate::state::test_utils::MOCK_MAX_OUTPUTS_SIZE;
    use crate::state::tree_committer::test_utils::TIMEOUT;
    use crate::state::tree_committer::test_utils::join_reply;
    use crate::state::tree_committer::test_utils::regular_tasks_done;
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

    #[tokio::test]
    async fn committer_exits_once_its_tree_is_quiescent_and_answers_waiting_events_untracked()
    -> anyhow::Result<()> {
        let mut tree = TestTree::spawn(false, false, MockInboundQueueSender).await;
        let start = reply(tree.submit(start())).await;
        assert!(
            matches!(start, Ok(TreeEventOutcome::Applied(JobState::Running))),
            "a start gave {start:?}"
        );

        let pause = tree.arm_pause(HookPoint::CommitterRoundAfterDbWrite);
        let regular_tasks_done = tree.submit(regular_tasks_done());
        pause.reached(1, TIMEOUT).await?;
        let commit_done = tree.submit(TreeEventKind::CommitDone);
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
    async fn a_round_of_stale_events_writes_nothing_to_the_database() -> anyhow::Result<()> {
        let tree = TestTree::spawn(false, false, MockInboundQueueSender).await;
        let write_pause = tree.hooks.arm(
            HookPoint::CommitterRoundAfterDbWrite,
            HookFilter::default(),
            HookAction::Pause,
            HookCount::Once,
        );

        let commit_done = reply(tree.submit(TreeEventKind::CommitDone)).await;
        assert!(
            matches!(
                commit_done,
                Ok(TreeEventOutcome::Stale(
                    StaleStateError::JobNoLongerCommitReady
                ))
            ),
            "a commit of a ready job gave {commit_done:?}"
        );
        assert_eq!(write_pause.hits(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_finishes_the_round_in_flight_and_answers_waiting_events_with_stopping()
    -> anyhow::Result<()> {
        let (inbound_queue_sender, inbound_queue_receiver) = create_inbound_queue();
        let tree = TestTree::spawn(false, false, inbound_queue_sender).await;
        let pause = tree.arm_pause(HookPoint::CommitterRoundAfterDbWrite);
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
        let pause = tree.arm_pause(HookPoint::CommitterRoundAfterDbWrite);
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

    #[tokio::test]
    async fn two_failures_of_one_job_in_one_round_fail_it_once() -> anyhow::Result<()> {
        let tree = TestTree::spawn(false, false, MockInboundQueueSender).await;
        reply(tree.submit(start())).await?;

        let reply_join_handles = tree
            .submit_in_one_round(vec![failure("first"), failure("second")])
            .await;
        let [first_join_handle, second_join_handle] =
            <[_; 2]>::try_from(reply_join_handles).expect("two events were submitted");
        let first = join_reply(first_join_handle).await;
        assert!(
            matches!(first, Ok(TreeEventOutcome::Applied(JobState::Failed))),
            "the first failure gave {first:?}"
        );
        let second = join_reply(second_join_handle).await;
        assert!(
            matches!(
                second,
                Ok(TreeEventOutcome::Stale(
                    StaleStateError::JobAlreadyTerminated(JobState::Failed)
                ))
            ),
            "the second failure of the round gave {second:?}"
        );
        assert_eq!(
            tree.db.errors.get(&ROOT_ID).as_deref(),
            Some(&"first".to_owned())
        );
        assert_eq!(tree.jcb.state().await, JobState::Failed);
        tree.wait_for_exit().await;
        assert!(!tree.shutdown_handle.is_fatal());
        Ok(())
    }

    #[tokio::test]
    async fn a_cancel_is_planned_before_a_failure_that_arrived_first() -> anyhow::Result<()> {
        let tree = TestTree::spawn(false, true, MockInboundQueueSender).await;
        reply(tree.submit(start())).await?;

        let reply_join_handles = tree
            .submit_in_one_round(vec![failure("boom"), TreeEventKind::CancelRoot])
            .await;
        let [failure_join_handle, cancel_join_handle] =
            <[_; 2]>::try_from(reply_join_handles).expect("two events were submitted");
        let failure = join_reply(failure_join_handle).await;
        assert!(
            matches!(
                failure,
                Ok(TreeEventOutcome::Stale(StaleStateError::JobNoLongerRunning))
            ),
            "a failure that arrived before the round's cancel gave {failure:?}"
        );
        let cancel = join_reply(cancel_join_handle).await;
        assert!(
            matches!(
                cancel,
                Ok(TreeEventOutcome::Applied(JobState::CleanupReady))
            ),
            "a cancel gave {cancel:?}"
        );
        assert!(
            !tree.db.errors.contains_key(&ROOT_ID),
            "a stale failure should store no error"
        );
        assert_eq!(
            tree.db.states.get(&ROOT_ID).map(|state| *state),
            Some(JobState::CleanupReady)
        );
        assert_eq!(tree.jcb.state().await, JobState::CleanupReady);
        assert!(!tree.shutdown_handle.is_fatal());
        Ok(())
    }

    #[tokio::test]
    async fn a_deferred_event_waits_for_the_next_round_but_a_stale_event_behind_it_does_not()
    -> anyhow::Result<()> {
        let tree = TestTree::spawn(false, true, MockInboundQueueSender).await;
        reply(tree.submit(start())).await?;
        let round_pause = tree.hooks.arm(
            HookPoint::CommitterRoundAfterDbWrite,
            HookFilter::job(ROOT_ID),
            HookAction::Pause,
            HookCount::Times(2),
        );

        let reply_join_handles = tree
            .submit_in_one_round(vec![
                TreeEventKind::CleanupDone,
                TreeEventKind::CommitDone,
                TreeEventKind::CancelRoot,
            ])
            .await;
        let [
            cleanup_done_join_handle,
            commit_done_join_handle,
            cancel_join_handle,
        ] = <[_; 3]>::try_from(reply_join_handles).expect("three events were submitted");
        round_pause.reached(1, TIMEOUT).await?;
        assert_eq!(
            tree.db.states.get(&ROOT_ID).map(|state| *state),
            Some(JobState::CleanupReady)
        );
        round_pause.release();
        round_pause.reached(2, TIMEOUT).await?;
        let cancel = join_reply(cancel_join_handle).await;
        assert!(
            matches!(
                cancel,
                Ok(TreeEventOutcome::Applied(JobState::CleanupReady))
            ),
            "a cancel gave {cancel:?}"
        );
        let commit_done = join_reply(commit_done_join_handle).await;
        assert!(
            matches!(
                commit_done,
                Ok(TreeEventOutcome::Stale(
                    StaleStateError::JobNoLongerCommitReady
                ))
            ),
            "a stale commit behind a deferred cleanup gave {commit_done:?}"
        );
        assert_eq!(
            tree.db.states.get(&ROOT_ID).map(|state| *state),
            Some(JobState::Cancelled)
        );
        assert!(
            !cleanup_done_join_handle.is_finished(),
            "a deferred cleanup should not be answered before its round's apply"
        );
        round_pause.release();
        let cleanup_done = join_reply(cleanup_done_join_handle).await;
        assert!(
            matches!(
                cleanup_done,
                Ok(TreeEventOutcome::Applied(JobState::Cancelled))
            ),
            "a cleanup that waited for the next round gave {cleanup_done:?}"
        );
        tree.wait_for_exit().await;
        assert!(!tree.shutdown_handle.is_fatal());
        Ok(())
    }

    #[tokio::test]
    async fn rejected_outputs_put_the_whole_round_back_and_fail_their_job_in_the_next_round()
    -> anyhow::Result<()> {
        let mut tree = TestTree::spawn(true, false, MockInboundQueueSender).await;
        reply(tree.submit(start())).await?;

        let outputs = vec![vec![0; usize::try_from(MOCK_MAX_OUTPUTS_SIZE)? + 1]];
        let reply_join_handles = tree
            .submit_in_one_round(vec![
                TreeEventKind::RegularTasksDone { outputs },
                regular_tasks_done(),
                TreeEventKind::CommitDone,
            ])
            .await;
        let [
            oversized_join_handle,
            stale_join_handle,
            deferred_join_handle,
        ] = <[_; 3]>::try_from(reply_join_handles).expect("three events were submitted");
        let oversized = join_reply(oversized_join_handle).await;
        assert!(
            matches!(oversized, Ok(TreeEventOutcome::Applied(JobState::Failed))),
            "outputs too large for the database gave {oversized:?}"
        );
        for (event, reply) in [
            ("a stale completion", join_reply(stale_join_handle).await),
            ("a deferred commit", join_reply(deferred_join_handle).await),
        ] {
            assert!(
                matches!(
                    reply,
                    Ok(TreeEventOutcome::Stale(
                        StaleStateError::JobAlreadyTerminated(JobState::Failed)
                    ))
                ),
                "{event} of the round that the database rejected gave {reply:?}"
            );
        }
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
}
