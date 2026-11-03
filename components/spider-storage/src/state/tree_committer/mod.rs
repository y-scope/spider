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
//! the terminal failures, then the other events, and runs each *round* in the following steps:
//!
//! 1. Plans the events against the tree's structure.
//! 2. Writes the plan to the database in one transaction.
//! 3. Applies the plan to the cached jobs and the structure, and enqueues newly terminal jobs for
//!    cache GC.
//! 4. Replies to the planned events. The events that the plan defers go back to the front of their
//!    buffers for the next round.
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
    root: SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    receivers: EventReceivers,
) {
    task_tracker.spawn(run_committer(env, root, receivers));
}

/// The committer's buffers of received events, one per event channel. Each holds the events that
/// earlier rounds deferred first, then the events received since.
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

    /// Moves, without waiting, every event queued in `receivers`' fast-forward channel to the back
    /// of its buffer, then the events queued in the regular channel until the regular buffer
    /// holds [`MAX_EVENTS_PER_ROUND`] events.
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
    /// buffered [`TreeEventKind::CancelRoot`], then the buffered
    /// [`TreeEventKind::TerminalFailure`]s, then the regular events, each in its buffer's order.
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
        for buffer in [&mut cancels, &mut failures, &mut self.regular] {
            let num_taken = buffer.len().min(MAX_EVENTS_PER_ROUND - events.len());
            events.extend(buffer.drain(..num_taken));
        }
        cancels.append(&mut failures);
        self.fast_forward = cancels;
        events
    }

    /// Puts each event of `events` back at the front of its buffer, keeping their order.
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

/// The committer's context: the tree's event receivers, its buffered events and the events of the
/// round that is running. Dropping it armed shuts the service down fatally before its fields drop;
/// they then drop every unanswered event's reply, which wakes its waiter with
/// [`InternalError::TreePoisoned`](crate::cache::error::InternalError::TreePoisoned).
struct CommitterContextGuard {
    receivers: EventReceivers,
    buffers: Buffers,

    /// The events of the round that is running, which the round answers and removes, leaving the
    /// events to put back in their buffers (see [`Committer::run_round`]). The guard owns them,
    /// rather than the round, so that a round that errors or panics before answering drops them
    /// only after [`Drop::drop`] has shut the service down: an event's waiter never sees
    /// `TreePoisoned` before the service is fatal.
    in_flight: Vec<Pending>,

    shutdown_handle: ShutdownHandle,
    root_id: JobId,
    is_disarmed: bool,
}

impl CommitterContextGuard {
    /// Closes the tree's event receivers, answers every buffered event and every event still in the
    /// receivers with `make_reply()`, and disarms the guard.
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
            self.buffers.fast_forward.push_back(pending);
        }
        while let Some(pending) = self.receivers.regular.recv().await {
            self.buffers.regular.push_back(pending);
        }
        let buffered = std::mem::take(&mut self.buffers);
        for Pending { reply, .. } in buffered.fast_forward.into_iter().chain(buffered.regular) {
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
    root: SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    receivers: EventReceivers,
) {
    let root_id = root.id();
    let shutdown_handle = env.shutdown_handle.clone();
    let hooks = env.hooks.clone();
    let mut ctx = CommitterContextGuard {
        receivers,
        buffers: Buffers::default(),
        in_flight: Vec::new(),
        shutdown_handle: shutdown_handle.clone(),
        root_id,
        is_disarmed: false,
    };
    let mut committer = Committer::new_root(env, root).await;
    loop {
        if ctx.buffers.is_empty() {
            tokio::select! {
                biased;
                Some(pending) = ctx.receivers.fast_forward.recv() => {
                    ctx.buffers.fast_forward.push_back(pending);
                }
                Some(pending) = ctx.receivers.regular.recv() => {
                    ctx.buffers.regular.push_back(pending);
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
        ctx.buffers.receive(&mut ctx.receivers);
        if shutdown_handle.is_cancelled() {
            ctx.close_and_answer(|| Err(CacheError::ServiceStopping))
                .await;
            return;
        }
        ctx.in_flight = ctx.buffers.take_events();
        if let Err(error) = committer.run_round(&mut ctx.in_flight).await {
            tracing::error!(
                root_id = ? root_id,
                error = % error,
                "A round of a tree's committer failed."
            );
            return;
        }
        ctx.buffers.requeue(std::mem::take(&mut ctx.in_flight));
        if committer.is_quiescent() {
            ctx.close_and_answer(|| Ok(TreeEventOutcome::Untracked))
                .await;
            return;
        }
    }
}
