//! The control block through which the jobs of a tree submit their events to its committer.

use std::future::Future;

use spider_utils::shutdown::ShutdownHandle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::cache::error::CacheError;
use crate::cache::error::InternalError;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::TreeEventOutcome;
use crate::cache::tree::TreeEventReply;

/// The handle through which every job of a tree submits its events to the tree's committer.
pub struct TreeControlBlock {
    fast_forward: mpsc::UnboundedSender<Pending>,
    regular: mpsc::UnboundedSender<Pending>,
    shutdown_handle: ShutdownHandle,
}

impl TreeControlBlock {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A pair, containing:
    ///
    /// * The control block of a new tree, whose refused events resolve to
    ///   [`CacheError::ServiceStopping`] once `shutdown_handle` is cancelled.
    /// * The receivers of the tree's events, for the tree's committer.
    #[must_use]
    pub fn new(shutdown_handle: ShutdownHandle) -> (Self, EventReceivers) {
        let (fast_forward, fast_forward_receiver) = mpsc::unbounded_channel();
        let (regular, regular_receiver) = mpsc::unbounded_channel();
        (
            Self {
                fast_forward,
                regular,
                shutdown_handle,
            },
            EventReceivers {
                fast_forward: fast_forward_receiver,
                regular: regular_receiver,
            },
        )
    }

    /// Sends `event` to the tree's committer at the call, before the returned future is first
    /// polled: a [`TreeEventKind::CancelRoot`](crate::cache::tree::TreeEventKind::CancelRoot) or
    /// a [`TreeEventKind::TerminalFailure`](crate::cache::tree::TreeEventKind::TerminalFailure) on
    /// the fast-forward channel, any other event on the regular channel.
    ///
    /// # Returns
    ///
    /// A future that resolves to the committer's reply to `event`, or to
    /// [`TreeEventOutcome::Untracked`] if the committer no longer takes events and the control
    /// block's shutdown handle is not cancelled.
    ///
    /// # Errors
    ///
    /// The future resolves to an error if:
    ///
    /// * [`CacheError::ServiceStopping`] if the committer no longer takes events once the control
    ///   block's shutdown handle is cancelled, in which case `event` was not and will not be
    ///   applied.
    /// * [`InternalError::TreePoisoned`] if the committer dropped `event` without a reply.
    /// * Any [`CacheError`] the committer replies to `event` with, e.g.,
    ///   [`CacheError::ServiceStopping`] if it shuts down before applying `event`.
    pub fn submit(&self, event: TreeEvent) -> impl Future<Output = TreeEventReply> + Send + use<> {
        let (reply, receiver) = oneshot::channel();
        let sender = if event.kind.is_fast_forward() {
            &self.fast_forward
        } else {
            &self.regular
        };
        let refusal = sender.send(Pending { event, reply }).is_err().then(|| {
            if self.shutdown_handle.is_cancelled() {
                Err(CacheError::ServiceStopping)
            } else {
                Ok(TreeEventOutcome::Untracked)
            }
        });
        async move {
            if let Some(refusal) = refusal {
                return refusal;
            }
            receiver
                .await
                .unwrap_or_else(|_| Err(InternalError::TreePoisoned.into()))
        }
    }
}

/// The receivers of a tree's two event channels, owned by the tree's committer.
pub struct EventReceivers {
    /// The [`TreeEventKind::CancelRoot`](crate::cache::tree::TreeEventKind::CancelRoot) and
    /// [`TreeEventKind::TerminalFailure`](crate::cache::tree::TreeEventKind::TerminalFailure)
    /// events.
    pub fast_forward: mpsc::UnboundedReceiver<Pending>,

    /// Every other event.
    pub regular: mpsc::UnboundedReceiver<Pending>,
}

/// An event on its way to the tree's committer, with the sender of its reply.
pub struct Pending {
    pub event: TreeEvent,
    pub reply: oneshot::Sender<TreeEventReply>,
}

#[cfg(test)]
mod tests {
    use std::mem::Discriminant;
    use std::mem::discriminant;

    use spider_core::job::JobState;
    use spider_utils::shutdown::ShutdownHandle;
    use tokio::sync::mpsc;

    use crate::cache::error::CacheError;
    use crate::cache::error::InternalError;
    use crate::cache::tree::Lane;
    use crate::cache::tree::Pending;
    use crate::cache::tree::TreeControlBlock;
    use crate::cache::tree::TreeEventKind;
    use crate::cache::tree::TreeEventOutcome;
    use crate::cache::tree::test_utils::failure;
    use crate::cache::tree::test_utils::root_event;
    use crate::cache::tree::test_utils::start;

    /// Drains the events that `receiver` holds.
    ///
    /// # Returns
    ///
    /// The discriminants of the drained events' kinds, in the order they were sent.
    fn drain_kind_discriminants(
        receiver: &mut mpsc::UnboundedReceiver<Pending>,
    ) -> Vec<Discriminant<TreeEventKind>> {
        let mut kind_discriminants = Vec::new();
        while let Ok(pending) = receiver.try_recv() {
            kind_discriminants.push(discriminant(&pending.event.kind));
        }
        kind_discriminants
    }

    #[test]
    fn submit_routes_cancels_and_failures_to_the_fast_forward_channel() {
        let (control_block, mut receivers) = TreeControlBlock::new(ShutdownHandle::new());
        let regular_tasks_done = || TreeEventKind::RegularTasksDone {
            outputs: Vec::new(),
        };
        for kind in [
            start(),
            TreeEventKind::CancelRoot,
            regular_tasks_done(),
            failure(Lane::Commit, "boom"),
            TreeEventKind::CommitDone,
            TreeEventKind::CleanupDone,
        ] {
            drop(control_block.submit(root_event(kind)));
        }

        assert_eq!(
            drain_kind_discriminants(&mut receivers.fast_forward),
            [
                discriminant(&TreeEventKind::CancelRoot),
                discriminant(&failure(Lane::Commit, "boom")),
            ]
        );
        assert_eq!(
            drain_kind_discriminants(&mut receivers.regular),
            [
                discriminant(&start()),
                discriminant(&regular_tasks_done()),
                discriminant(&TreeEventKind::CommitDone),
                discriminant(&TreeEventKind::CleanupDone),
            ]
        );
    }

    #[tokio::test]
    async fn a_submitted_event_resolves_to_its_reply_or_to_tree_poisoned_if_dropped_unanswered() {
        let (control_block, mut receivers) = TreeControlBlock::new(ShutdownHandle::new());
        let cancel_reply = control_block.submit(root_event(TreeEventKind::CancelRoot));
        let start_reply = control_block.submit(root_event(start()));

        receivers
            .fast_forward
            .try_recv()
            .expect("the cancel should be on the fast-forward channel")
            .reply
            .send(Ok(TreeEventOutcome::Applied(JobState::Cancelled)))
            .expect("the cancel's reply future should still hold its receiver");
        drop(
            receivers
                .regular
                .try_recv()
                .expect("the start should be on the regular channel"),
        );

        let reply = cancel_reply.await;
        assert!(
            matches!(reply, Ok(TreeEventOutcome::Applied(JobState::Cancelled))),
            "an answered event should resolve to its reply, got {reply:?}"
        );
        let reply = start_reply.await;
        assert!(
            matches!(
                reply,
                Err(CacheError::Internal(InternalError::TreePoisoned))
            ),
            "an event dropped without a reply should poison its tree, got {reply:?}"
        );
    }

    #[tokio::test]
    async fn a_submit_to_an_exited_committer_is_untracked() {
        let (control_block, receivers) = TreeControlBlock::new(ShutdownHandle::new());
        drop(receivers);

        let reply = control_block.submit(root_event(start())).await;
        assert!(
            matches!(reply, Ok(TreeEventOutcome::Untracked)),
            "a submit to an exited committer should be untracked, got {reply:?}"
        );
    }

    #[tokio::test]
    async fn a_submit_during_a_shutdown_is_stopping_only_once_the_committer_exits() {
        let shutdown_handle = ShutdownHandle::new();
        let (control_block, mut receivers) = TreeControlBlock::new(shutdown_handle.clone());
        shutdown_handle.shut_down_gracefully();

        let start_reply = control_block.submit(root_event(start()));
        receivers
            .regular
            .try_recv()
            .expect("a submit during a shutdown should still reach a live committer")
            .reply
            .send(Ok(TreeEventOutcome::Applied(JobState::Running)))
            .expect("the start's reply future should still hold its receiver");
        let reply = start_reply.await;
        assert!(
            matches!(reply, Ok(TreeEventOutcome::Applied(JobState::Running))),
            "a submit queued during a shutdown should resolve to the committer's reply, got \
             {reply:?}"
        );
        drop(receivers);

        let reply = control_block.submit(root_event(start())).await;
        assert!(
            matches!(reply, Err(CacheError::ServiceStopping)),
            "a submit to an exited committer during a shutdown should be stopping, got {reply:?}"
        );
    }
}
