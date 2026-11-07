//! The committer of a job tree.
//!
//! Each tree has one committer, a coroutine that is the only writer of its jobs' states. It owns:
//!
//! * The tree's event receivers.
//! * The tree's structure.
//! * The control blocks of the tree's non-terminal jobs.
//!
//! The committer takes its received events in rounds of at most
//! [`MAX_EVENTS_PER_ROUND`](crate::limits::MAX_EVENTS_PER_ROUND) events, every cancel first, then
//! the terminal failures, then the oldest event that registers a child set, then the other events,
//! and runs each *round* in the following steps:
//!
//! 1. Plans the events against the tree's structure.
//! 2. Writes the plan, with the children that it registers, to the database in one transaction.
//! 3. Applies the plan to the cached jobs and the structure, adds the registered children to the
//!    tree and the job cache, and enqueues newly terminal jobs for cache GC.
//! 4. Replies to the planned events. The events that the plan defers go back to the front of the
//!    pending queue for the next round.
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
use tokio_util::task::TaskTracker;

use crate::cache::error::CacheError;
use crate::cache::job::SharedJobControlBlock;
use crate::cache::tree::EventReceivers;
use crate::cache::tree::Pending;
use crate::cache::tree::TreeEventKind;
use crate::cache::tree::TreeEventOutcome;
use crate::cache::tree::TreeEventReply;
use crate::db::InternalJobOrchestration;
use crate::inbound_queue::InboundQueueSender;
use crate::limits::MAX_EVENTS_PER_ROUND;
use crate::state::JobCache;
use crate::state::JobCacheGcHandle;
use crate::state::tree_committer::round::Committer;
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
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
pub struct CommitterEnv<
    InboundQueueSenderType: InboundQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    pub db: DbConnectorType,
    pub job_cache: JobCache<InboundQueueSenderType, TaskInstancePoolConnectorType>,
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
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
pub fn spawn_committer<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: InternalJobOrchestration + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
>(
    task_tracker: &TaskTracker,
    env: Arc<CommitterEnv<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>>,
    root: SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    receivers: EventReceivers,
) {
    task_tracker.spawn(run_committer(env, root, receivers));
}

/// The committer's queue of received events that are not yet planned, one queue per event channel.
/// Each holds the events that earlier rounds deferred first, then the events received since.
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

    /// Moves, without waiting, every event queued in `receivers`' fast-forward channel to the back
    /// of the queue, then the events queued in the regular channel until the queue holds
    /// [`MAX_EVENTS_PER_ROUND`] regular events.
    fn receive(&mut self, receivers: &mut EventReceivers) {
        while let Ok(pending) = receivers.fast_forward.try_recv() {
            self.fast_forward.push_back(pending);
        }
        while self.regular.len() < MAX_EVENTS_PER_ROUND
            && let Ok(pending) = receivers.regular.try_recv()
        {
            self.regular.push_back(pending);
        }
    }

    /// Takes the events of the next round, at most [`MAX_EVENTS_PER_ROUND`], in this order: every
    /// queued [`TreeEventKind::CancelRoot`], then the queued [`TreeEventKind::TerminalFailure`]s,
    /// then the oldest queued regular event that registers a child set (see
    /// [`TreeEventKind::registers`]), then the other regular events, each in its channel's order.
    ///
    /// # Returns
    ///
    /// The taken events, in that order.
    fn take_events(&mut self) -> Vec<Pending> {
        let (mut cancels, mut failures): (VecDeque<Pending>, VecDeque<Pending>) =
            std::mem::take(&mut self.fast_forward)
                .into_iter()
                .partition(|pending| matches!(pending.event.kind, TreeEventKind::CancelRoot));
        let mut events = Vec::new();
        for queue in [&mut cancels, &mut failures] {
            let num_taken = queue.len().min(MAX_EVENTS_PER_ROUND - events.len());
            events.extend(queue.drain(..num_taken));
        }
        cancels.append(&mut failures);
        self.fast_forward = cancels;
        if events.len() < MAX_EVENTS_PER_ROUND
            && let Some(registration_index) = self
                .regular
                .iter()
                .position(|pending| pending.event.kind.registers())
        {
            events.extend(self.regular.remove(registration_index));
        }
        let num_taken = self.regular.len().min(MAX_EVENTS_PER_ROUND - events.len());
        events.extend(self.regular.drain(..num_taken));
        events
    }

    /// Puts each event of `events` back at the front of the queue, ahead of the queued events of
    /// its channel, keeping their order.
    fn requeue(&mut self, events: Vec<Pending>) {
        for pending in events.into_iter().rev() {
            if pending.event.kind.is_fast_forward() {
                self.fast_forward.push_front(pending);
            } else {
                self.regular.push_front(pending);
            }
        }
    }
}

/// The committer's context: the tree's event receivers, its received events not yet planned and
/// the events of the round that is running. Dropping it armed shuts the service down fatally
/// before its fields drop; they then drop every unanswered event's reply, which wakes its
/// waiter with [`InternalError::TreePoisoned`](crate::cache::error::InternalError::TreePoisoned).
struct CommitterContextGuard {
    receivers: EventReceivers,
    pending_queue: PendingQueue,

    /// The events of the round that is running, which the round answers and removes, leaving the
    /// events to put back in the pending queue (see [`Committer::run_round`]). The guard owns
    /// them, rather than the round, so that a round that errors or panics before answering drops
    /// them only after [`Drop::drop`] has shut the service down: an event's waiter never sees
    /// `TreePoisoned` before the service is fatal.
    in_flight: Vec<Pending>,

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
            self.pending_queue.fast_forward.push_back(pending);
        }
        while let Some(pending) = self.receivers.regular.recv().await {
            self.pending_queue.regular.push_back(pending);
        }
        let pending_queue = std::mem::take(&mut self.pending_queue);
        for Pending { reply, .. } in pending_queue
            .fast_forward
            .into_iter()
            .chain(pending_queue.regular)
        {
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
    env: Arc<CommitterEnv<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>>,
    root: SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    receivers: EventReceivers,
) {
    let root_id = root.id();
    let shutdown_handle = env.shutdown_handle.clone();
    let hooks = env.hooks.clone();
    let mut ctx = CommitterContextGuard {
        receivers,
        pending_queue: PendingQueue::default(),
        in_flight: Vec::new(),
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
                    ctx.pending_queue.fast_forward.push_back(pending);
                }
                Some(pending) = ctx.receivers.regular.recv() => {
                    ctx.pending_queue.regular.push_back(pending);
                }
                () = shutdown_handle.cancelled() => {}
            }
        }
        // A pause that the service's shutdown ends lets the committer go on to stop, as it would
        // without the hook.
        let _ = hooks
            .hit(HookPoint::CommitterBeforeReceive, || {
                HookContext::job(root_id)
            })
            .await;
        ctx.pending_queue.receive(&mut ctx.receivers);
        if shutdown_handle.is_cancelled() {
            ctx.close_and_answer(|| Err(CacheError::ServiceStopping))
                .await;
            return;
        }
        ctx.in_flight = ctx.pending_queue.take_events();
        if let Err(error) = committer.run_round(&mut ctx.in_flight).await {
            tracing::error!(
                root_id = ? root_id,
                error = % error,
                "A round of a tree's committer failed."
            );
            return;
        }
        ctx.pending_queue
            .requeue(std::mem::take(&mut ctx.in_flight));
        if committer.is_quiescent() {
            ctx.close_and_answer(|| Ok(TreeEventOutcome::Untracked))
                .await;
            return;
        }
    }
}
