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
#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::sync::Arc;

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
    env: Arc<CommitterEnv<InboundQueueSenderType, DbConnectorType>>,
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

    /// Puts `pending` at the front of the queue, ahead of the queued events of its channel.
    fn push_front(&mut self, pending: Pending) {
        if pending.event.kind.is_fast_forward() {
            self.fast_forward.push_front(pending);
        } else {
            self.regular.push_front(pending);
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

/// The committer's context: the tree's event receivers, its received events not yet planned and
/// the reply of the event whose round is running. Dropping it armed shuts the service down
/// fatally before its fields drop; they then drop every unanswered event's reply, which wakes its
/// waiter with [`InternalError::TreePoisoned`](crate::cache::error::InternalError::TreePoisoned).
struct CommitterContextGuard {
    receivers: EventReceivers,
    pending_queue: PendingQueue,

    /// The reply of the event whose round is running, which the round takes once it answers the
    /// event. The guard owns it, rather than the loop body, so that a round that errors or panics
    /// before answering drops it only after [`Drop::drop`] has shut the service down: the event's
    /// waiter never sees `TreePoisoned` before the service is fatal.
    in_flight_reply: Option<oneshot::Sender<TreeEventReply>>,

    shutdown_handle: ShutdownHandle,
    root_id: JobId,
    is_disarmed: bool,
}

impl CommitterContextGuard {
    /// Closes the tree's event receivers, answers every event in the pending queue and every event
    /// still in the receivers with `make_reply()`, and disarms the guard.
    ///
    /// # Type Parameters
    ///
    /// * `MakeReplyType` - The type of the closure that builds the reply to each event.
    async fn close_and_answer<MakeReplyType: Fn() -> TreeEventReply>(
        mut self,
        make_reply: MakeReplyType,
    ) {
        self.receivers.fast_forward.close();
        self.receivers.regular.close();
        // `try_recv` can miss a send in flight after `close`; `recv` returns `None` only once none
        // is.
        while let Some(pending) = self.receivers.fast_forward.recv().await {
            self.pending_queue.push_back(pending);
        }
        while let Some(pending) = self.receivers.regular.recv().await {
            self.pending_queue.push_back(pending);
        }
        while let Some(Pending { reply, .. }) = self.pending_queue.take() {
            // The event's waiter may have dropped its future, which is not an error of the
            // committer.
            let _ = reply.send(make_reply());
        }
        self.is_disarmed = true;
    }
}

impl Drop for CommitterContextGuard {
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
    env: Arc<CommitterEnv<InboundQueueSenderType, DbConnectorType>>,
    root: SharedJobControlBlock<
        InboundQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    >,
    receivers: EventReceivers,
) {
    let root_id = root.id();
    let shutdown_handle = env.shutdown_handle.clone();
    let mut ctx = CommitterContextGuard {
        receivers,
        pending_queue: PendingQueue::default(),
        in_flight_reply: None,
        shutdown_handle: shutdown_handle.clone(),
        root_id,
        is_disarmed: false,
    };
    let mut committer = Committer::new_root(env, root).await;
    loop {
        if ctx.pending_queue.is_empty() {
            tokio::select! {
                biased;
                Some(pending) = ctx.receivers.fast_forward.recv() => {
                    ctx.pending_queue.push_back(pending);
                }
                Some(pending) = ctx.receivers.regular.recv() => {
                    ctx.pending_queue.push_back(pending);
                }
                () = shutdown_handle.cancelled() => {}
            }
        }
        ctx.pending_queue.receive(&mut ctx.receivers);
        if shutdown_handle.is_cancelled() {
            ctx.close_and_answer(|| Err(CacheError::ServiceStopping))
                .await;
            return;
        }
        let Pending { event, reply } = ctx.pending_queue.take().expect(
            "the pending queue should hold an event once the committer wakes before a shutdown",
        );
        ctx.in_flight_reply.replace(reply);
        match committer.run_round(event, &mut ctx.in_flight_reply).await {
            Ok(Some(failure)) => {
                let reply = ctx.in_flight_reply.take().expect(
                    "a round that answered nothing should leave its event's reply in flight",
                );
                ctx.pending_queue.push_front(Pending {
                    event: failure,
                    reply,
                });
            }
            Ok(None) => {}
            Err(error) => {
                tracing::error!(
                    root_id = ? root_id,
                    error = % error,
                    "A round of a tree's committer failed."
                );
                return;
            }
        }
        if committer.is_quiescent() {
            ctx.close_and_answer(|| Ok(TreeEventOutcome::Untracked))
                .await;
            return;
        }
    }
}
