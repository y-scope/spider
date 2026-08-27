//! The dispatch queue subsystem shared between the core and the dispatch service.
//!
//! The subsystem owns every channel involved in handing assignments to execution managers: one
//! unbounded queue per resource group, and one broadcast queue carrying the hints that steer
//! general execution managers towards the groups with uncovered work. An assignment is stored
//! exactly once, in the queue of the resource group that owns it.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use dashmap::DashMap;
use spider_core::session::SessionTracker;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SessionId;
use spider_core::types::scheduler::TaskAssignment;

use crate::error::SchedulerError;

/// The read side of one resource group's dispatch queue.
///
/// Clones share one queue and one hint counter, so the value is both what a pinned execution
/// manager blocks on and what the core wraps in a [`Hint`] for general execution managers.
#[derive(Clone, Debug)]
pub(super) struct RgDispatchQueueReader {
    inner: Arc<RgDispatchQueueReaderInner>,
}

impl RgDispatchQueueReader {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created reader over `receiver`.
    fn new(
        receiver: async_channel::Receiver<TaskAssignment>,
        rg_id: ResourceGroupId,
        session_id: SessionId,
    ) -> Self {
        Self {
            inner: Arc::new(RgDispatchQueueReaderInner {
                receiver,
                living_hint: AtomicUsize::new(0),
                rg_id,
                session_id,
            }),
        }
    }

    /// # Returns
    ///
    /// The resource group this queue belongs to.
    pub(super) fn rg_id(&self) -> ResourceGroupId {
        self.inner.rg_id
    }

    /// # Returns
    ///
    /// The session in which the resource group was created.
    pub(super) fn session_id(&self) -> SessionId {
        self.inner.session_id
    }

    /// Blocks until an assignment arrives or `wait_time` expires.
    ///
    /// Called by a pinned execution manager, which only reads from this queue and leaves the hint
    /// counter untouched.
    ///
    /// A closed queue yields [`None`] by design rather than an error: a session bump clears the
    /// registry under coroutines that are already blocked here, so closure is the ordinary end of a
    /// bumped-out request and must read as "nothing to hand out".
    ///
    /// # Returns
    ///
    /// The next assignment, or [`None`] if none arrived before `wait_time` expired or the queue was
    /// closed.
    pub(super) async fn recv_pinned(&self, wait_time: Duration) -> Option<TaskAssignment> {
        tokio::time::timeout(wait_time, self.inner.receiver.recv())
            .await
            .ok()?
            .ok()
    }
}

/// One resource group's claim on a general execution manager, drawn on that group's hint count.
///
/// A hint can only come out of the broadcast queue: both the wrapped reader and the constructor are
/// private, so no caller outside this module can turn a reader it happens to hold into a hint.
/// Spending one consumes it, and it is deliberately neither [`Clone`] nor [`Copy`], so "a hint is
/// spent at most once" is a property the type system enforces rather than a rule a call site
/// follows. Dropping a hint unspent withdraws nothing from its group's count, which is what lets a
/// caller discard a hint it must not act on.
#[derive(Debug)]
pub(super) struct Hint {
    reader: RgDispatchQueueReader,
}

impl Hint {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created hint drawn on `reader`'s resource group.
    const fn new(reader: RgDispatchQueueReader) -> Self {
        Self { reader }
    }

    /// # Returns
    ///
    /// The resource group this hint points at.
    pub(super) fn rg_id(&self) -> ResourceGroupId {
        self.reader.rg_id()
    }

    /// # Returns
    ///
    /// The session in which the resource group was created.
    pub(super) fn session_id(&self) -> SessionId {
        self.reader.session_id()
    }

    /// Spends the hint on a single non-blocking pop from the resource group it points at.
    ///
    /// A closed queue yields [`None`] by design rather than an error: a session bump clears the
    /// registry, so a hint carried across the bump names a closed queue and must read as "nothing
    /// to hand out", exactly like any other stale hint.
    ///
    /// # Cancel safety
    ///
    /// This method is synchronous, so its decrement and its pop cannot be separated by
    /// cancellation. The caller must still not yield between receiving the hint and calling it: a
    /// future can only be dropped at an await point, so an await in that window would let a
    /// cancelled request drop the hint without decrementing the counter, permanently overstating
    /// the group's coverage.
    ///
    /// # Returns
    ///
    /// The next assignment, or [`None`] if the hint was stale and the queue is empty, or if the
    /// queue was closed.
    pub(super) fn consume_and_try_recv(self) -> Option<TaskAssignment> {
        self.reader.inner.decrement_living_hint();
        self.reader.inner.receiver.try_recv().ok()
    }
}

/// The writer side of one resource group's dispatch queue, owned by the group's scheduling unit.
#[derive(Debug)]
pub(super) struct RgDispatchQueueWriter {
    sender: async_channel::Sender<TaskAssignment>,
    reader: RgDispatchQueueReader,
    broadcast_sender: async_channel::Sender<Hint>,
}

impl RgDispatchQueueWriter {
    /// Publishes `assignment` into the group's dispatch queue, then hints at it unless the group's
    /// outstanding hints already cover its queue.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::DispatchQueueClosed`] if either the group's dispatch queue is closed or
    ///   the broadcast queue is closed.
    pub(super) fn try_send(&self, assignment: TaskAssignment) -> Result<(), SchedulerError> {
        self.sender
            .try_send(assignment)
            .map_err(|_| SchedulerError::DispatchQueueClosed)?;

        let Some(hint) = self.try_make_hint() else {
            return Ok(());
        };
        self.broadcast_sender
            .try_send(hint)
            .map_err(|_| SchedulerError::DispatchQueueClosed)
    }

    /// # Returns
    ///
    /// The number of assignments currently queued for the resource group.
    pub(super) fn queue_len(&self) -> usize {
        self.sender.len()
    }

    /// Takes out one hint for the resource group, unless its outstanding hints already cover its
    /// queue.
    ///
    /// # Returns
    ///
    /// The hint taken out, through which a general execution manager pops from this group, or
    /// [`None`] if the group's outstanding hints already cover its queue and no hint is needed.
    fn try_make_hint(&self) -> Option<Hint> {
        // The queue size must be sampled before the hint count: a hint spent between the two
        // samples is then reflected in the count but not in the already-sampled size, so the
        // comparison can only take out a hint that turns out to be redundant, never leave a queued
        // assignment uncovered.
        let dispatch_queue_size = self.queue_len();
        if self.reader.inner.living_hint() >= dispatch_queue_size {
            return None;
        }

        self.reader.inner.increment_living_hint();
        Some(Hint::new(self.reader.clone()))
    }
}

/// The registry of dispatch queue endpoints, shared between the core and the dispatch service.
///
/// Either side may be the first to name a resource group: a pinned execution manager can connect
/// before any task of its group has been scheduled, so both lookups create the group on demand.
///
/// Every queue of the subsystem is unbounded. The admission threshold is what limits a group's
/// occupancy, so a channel bound would be a second, redundant limit whose only possible effect is
/// to reject a send the design's coverage proof requires to succeed.
#[derive(Clone, Debug)]
pub(super) struct DispatchQueueRegistry {
    inner: Arc<DispatchQueueRegistryInner>,
}

impl DispatchQueueRegistry {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created, empty registry stamping every group it creates with `session_tracker`'s
    /// current session.
    pub(super) fn new(session_tracker: SessionTracker) -> Self {
        let (broadcast_sender, broadcast_receiver) = async_channel::unbounded();
        Self {
            inner: Arc::new(DispatchQueueRegistryInner {
                table: DashMap::new(),
                session_tracker,
                broadcast_sender,
                broadcast_receiver,
            }),
        }
    }

    /// # Returns
    ///
    /// The read side of `rg_id`'s dispatch queue, creating the group if it has none.
    pub(super) fn get_dispatch_queue_reader(
        &self,
        rg_id: ResourceGroupId,
    ) -> RgDispatchQueueReader {
        self.get_or_create(rg_id).reader
    }

    /// # Returns
    ///
    /// The write side of `rg_id`'s dispatch queue, creating the group if it has none.
    pub(super) fn get_dispatch_queue_writer(
        &self,
        rg_id: ResourceGroupId,
    ) -> RgDispatchQueueWriter {
        self.get_or_create(rg_id)
            .writer(self.inner.broadcast_sender.clone())
    }

    /// Blocks until a hint is published or `wait_time` expires.
    ///
    /// Called by a general execution manager, which spends the returned hint through
    /// [`Hint::consume_and_try_recv`].
    ///
    /// The registry holds both ends of the broadcast queue, so the queue cannot close while the
    /// registry is alive and an unbounded wait would never end on an empty queue. The caller
    /// therefore has to bound the wait, exactly as it does for
    /// [`RgDispatchQueueReader::recv_pinned`].
    ///
    /// # Cancel safety
    ///
    /// Dropping the returned future before it resolves loses no hint: the wait takes a hint out of
    /// the broadcast queue only when it resolves, so a cancelled wait leaves every published hint
    /// there for another caller. The caller must not, however, be cancelled after the future
    /// resolves and before the hint is spent: a dropped hint is never withdrawn from its group's
    /// count and permanently overstates the group's coverage.
    ///
    /// # Returns
    ///
    /// The next published hint, or [`None`] if no hint was published before `wait_time` expired or
    /// the broadcast queue was closed.
    pub(super) async fn next_hint(&self, wait_time: Duration) -> Option<Hint> {
        tokio::time::timeout(wait_time, self.inner.broadcast_receiver.recv())
            .await
            .ok()?
            .ok()
    }

    /// # Returns
    ///
    /// The number of resource groups the registry currently holds.
    pub(super) fn len(&self) -> usize {
        self.inner.table.len()
    }

    /// Drops every resource group and discards every hint the broadcast queue still holds.
    ///
    /// An execution manager still blocked on a dropped group's reader keeps that queue alive, but
    /// every assignment in it fails the dispatch service's session check.
    ///
    /// The broadcast queue outlives the session bump, because the registry owns both of its ends,
    /// so a hint left in it would name a dropped group and would be handed to a general execution
    /// manager in the new session. The drain runs after the table is cleared so that it ends with
    /// the queue provably free of stale hints: every hint the drain could have missed would name a
    /// group the clear has already dropped. The core is the sole publisher of hints and is itself
    /// executing this call, so no concurrent publish can refill the queue behind the drain, which
    /// is what makes the order free of cost.
    ///
    /// The drain withdraws no hint from any group's count: those counts belong to groups the clear
    /// dropped, which no live reader consults again.
    pub(super) fn clear(&self) {
        self.inner.table.clear();
        while self.inner.broadcast_receiver.try_recv().is_ok() {}
    }

    /// Closes the broadcast queue, so that every later hint publication is rejected.
    ///
    /// No production path closes the queue; this exists so that a test can drive the core into the
    /// failure the closure represents.
    #[cfg(test)]
    pub(super) fn close_broadcast_queue(&self) {
        self.inner.broadcast_sender.close();
    }

    /// Closes `rg_id`'s dispatch queue, creating the group if it has none, so that every later
    /// publication into it is rejected.
    ///
    /// No production path closes a group's queue on its own; this exists so that a test can drive
    /// the core into the failure the closure represents.
    #[cfg(test)]
    pub(super) fn close_dispatch_queue(&self, rg_id: ResourceGroupId) {
        self.get_or_create(rg_id).sender.close();
    }

    /// # Returns
    ///
    /// The number of hints waiting in the broadcast queue.
    #[cfg(test)]
    pub(super) fn num_outstanding_hints(&self) -> usize {
        self.inner.broadcast_receiver.len()
    }

    /// # Returns
    ///
    /// Both ends of `rg_id`'s dispatch queue, creating the group if it has none.
    fn get_or_create(&self, rg_id: ResourceGroupId) -> RgDispatchQueueEndpoints {
        self.inner
            .table
            .entry(rg_id)
            .or_insert_with(|| {
                let (sender, receiver) = async_channel::unbounded();
                RgDispatchQueueEndpoints {
                    sender,
                    reader: RgDispatchQueueReader::new(
                        receiver,
                        rg_id,
                        self.inner.session_tracker.current(),
                    ),
                }
            })
            .value()
            .clone()
    }
}

/// Both ends of one resource group's dispatch queue.
#[derive(Clone, Debug)]
struct RgDispatchQueueEndpoints {
    /// The write side, from which the core builds the group's scheduling unit.
    sender: async_channel::Sender<TaskAssignment>,

    /// The read side, cloned into every execution manager request that touches the group.
    reader: RgDispatchQueueReader,
}

impl RgDispatchQueueEndpoints {
    /// # Returns
    ///
    /// A newly created writer pairing the group's sender with the group's own reader and with
    /// `broadcast_sender`.
    fn writer(&self, broadcast_sender: async_channel::Sender<Hint>) -> RgDispatchQueueWriter {
        RgDispatchQueueWriter {
            sender: self.sender.clone(),
            reader: self.reader.clone(),
            broadcast_sender,
        }
    }
}

/// The `Arc`-shared body of a [`DispatchQueueRegistry`].
///
/// Holding both ends of the broadcast queue is what keeps it open for as long as the registry
/// lives: an `async_channel` closes only once all its senders or all its receivers are gone.
#[derive(Debug)]
struct DispatchQueueRegistryInner {
    table: DashMap<ResourceGroupId, RgDispatchQueueEndpoints>,
    session_tracker: SessionTracker,
    broadcast_sender: async_channel::Sender<Hint>,
    broadcast_receiver: async_channel::Receiver<Hint>,
}

/// The `Arc`-shared body of an [`RgDispatchQueueReader`].
#[derive(Debug)]
struct RgDispatchQueueReaderInner {
    receiver: async_channel::Receiver<TaskAssignment>,
    living_hint: AtomicUsize,
    rg_id: ResourceGroupId,
    session_id: SessionId,
}

impl RgDispatchQueueReaderInner {
    /// # Returns
    ///
    /// The number of hints currently outstanding for the resource group.
    fn living_hint(&self) -> usize {
        self.living_hint.load(Ordering::Acquire)
    }

    /// Records one more outstanding hint for the resource group.
    fn increment_living_hint(&self) {
        self.living_hint.fetch_add(1, Ordering::Release);
    }

    /// Withdraws one outstanding hint from the resource group.
    ///
    /// The caller must hold exactly one outstanding hint for this group. A decrement with no hint
    /// outstanding wraps the count, permanently corrupting the group's hint accounting.
    fn decrement_living_hint(&self) {
        self.living_hint.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use spider_core::session::SessionTracker;
    use spider_core::task::TaskIndex;
    use spider_core::types::id::JobId;
    use spider_core::types::id::TaskAssignmentId;
    use spider_core::types::id::TaskId;

    use super::*;

    /// The resource group every test publishes into.
    const RG_ID: ResourceGroupId = ResourceGroupId::from(1);

    /// The second resource group of a test that needs two groups.
    const OTHER_RG_ID: ResourceGroupId = ResourceGroupId::from(2);

    /// The session the registry's tracker starts in.
    const SESSION_ID: SessionId = 7;

    /// The job every published assignment is drawn from.
    const JOB_ID: JobId = JobId::from(3);

    /// The time a test lets [`RgDispatchQueueReader::recv_pinned`] or
    /// [`DispatchQueueRegistry::next_hint`] block before it concludes that nothing is coming.
    const RECV_WAIT: Duration = Duration::from_millis(50);

    /// # Returns
    ///
    /// An assignment of `rg_id`'s `task_index`-th task.
    fn make_assignment(rg_id: ResourceGroupId, task_index: TaskIndex) -> TaskAssignment {
        TaskAssignment {
            id: TaskAssignmentId::from(task_index as u64),
            resource_group_id: rg_id,
            job_id: JOB_ID,
            task_id: TaskId::Index(task_index),
            session_id: SESSION_ID,
        }
    }

    /// Queues `assignment` on the group's dispatch queue, as one turn of the core's decision loop
    /// would.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`RgDispatchQueueWriter::try_send`]'s return values on failure.
    fn publish(writer: &RgDispatchQueueWriter, assignment: TaskAssignment) -> anyhow::Result<()> {
        writer.try_send(assignment)?;
        Ok(())
    }

    /// Pops one assignment without blocking, reaching the receiver directly so that observing a
    /// queue costs the tests neither an await nor a hint.
    ///
    /// # Returns
    ///
    /// The next assignment, or [`None`] if the queue is empty.
    fn try_pop(reader: &RgDispatchQueueReader) -> Option<TaskAssignment> {
        reader.inner.receiver.try_recv().ok()
    }

    /// Takes one hint without blocking, reaching the broadcast receiver directly so that observing
    /// the broadcast queue costs the tests no await.
    ///
    /// # Returns
    ///
    /// The next published hint, or [`None`] if no hint is outstanding.
    fn try_next_hint(registry: &DispatchQueueRegistry) -> Option<Hint> {
        registry.inner.broadcast_receiver.try_recv().ok()
    }

    #[test]
    fn get_or_create_returns_endpoints_onto_the_same_queue() -> anyhow::Result<()> {
        let session_tracker = SessionTracker::new(SESSION_ID);
        let registry = DispatchQueueRegistry::new(session_tracker.clone());
        let endpoints = registry.get_or_create(RG_ID);
        assert_eq!(registry.len(), 1);
        assert_eq!(endpoints.reader.rg_id(), RG_ID);
        assert_eq!(endpoints.reader.session_id(), SESSION_ID);

        // A second lookup must find the group rather than replace it, so the session it reports is
        // still the one it was created with.
        assert!(session_tracker.try_advance(SESSION_ID + 1));
        let looked_up = registry.get_or_create(RG_ID);
        assert_eq!(registry.len(), 1);
        assert_eq!(looked_up.reader.session_id(), SESSION_ID);

        let writer = registry.get_dispatch_queue_writer(RG_ID);
        let looked_up_writer = registry.get_dispatch_queue_writer(RG_ID);
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        assert_eq!(looked_up_writer.queue_len(), 1);
        assert_eq!(try_pop(&looked_up.reader), Some(assignment));
        assert_eq!(writer.queue_len(), 0);
        Ok(())
    }

    #[test]
    fn two_resource_groups_get_independent_queues() -> anyhow::Result<()> {
        let session_tracker = SessionTracker::new(SESSION_ID);
        let registry = DispatchQueueRegistry::new(session_tracker.clone());
        let endpoints = registry.get_or_create(RG_ID);
        assert!(session_tracker.try_advance(SESSION_ID + 1));
        let other_endpoints = registry.get_or_create(OTHER_RG_ID);
        assert_eq!(registry.len(), 2);
        assert_eq!(other_endpoints.reader.rg_id(), OTHER_RG_ID);
        assert_eq!(other_endpoints.reader.session_id(), SESSION_ID + 1);

        let writer = registry.get_dispatch_queue_writer(RG_ID);
        let other_writer = registry.get_dispatch_queue_writer(OTHER_RG_ID);
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        assert_eq!(
            try_next_hint(&registry).map(|hint| hint.rg_id()),
            Some(RG_ID)
        );

        assert_eq!(other_writer.queue_len(), 0);
        assert_eq!(try_pop(&other_endpoints.reader), None);

        let other_assignment = make_assignment(OTHER_RG_ID, 1);
        publish(&other_writer, other_assignment)?;
        assert_eq!(
            try_next_hint(&registry).map(|hint| hint.rg_id()),
            Some(OTHER_RG_ID)
        );
        assert_eq!(try_pop(&endpoints.reader), Some(assignment));
        Ok(())
    }

    #[test]
    fn get_dispatch_queue_reader_creates_a_missing_group() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        assert_eq!(registry.len(), 0);

        let reader = registry.get_dispatch_queue_reader(RG_ID);
        assert_eq!(registry.len(), 1);
        assert_eq!(reader.rg_id(), RG_ID);
        assert_eq!(reader.session_id(), SESSION_ID);
        assert_eq!(try_pop(&reader), None);

        // The group the reader created is the one the core later writes into.
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        assert_eq!(registry.len(), 1);
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        assert_eq!(writer.queue_len(), 1);
        assert_eq!(try_pop(&reader), Some(assignment));
        Ok(())
    }

    #[test]
    fn get_dispatch_queue_writer_creates_a_missing_group() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        assert_eq!(registry.len(), 0);

        let writer = registry.get_dispatch_queue_writer(RG_ID);
        assert_eq!(registry.len(), 1);
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        let hint = try_next_hint(&registry).expect("the queued assignment is uncovered");
        assert_eq!(hint.rg_id(), RG_ID);
        assert_eq!(hint.session_id(), SESSION_ID);

        // The group the writer created is the one the dispatch service later reads from.
        let reader = registry.get_dispatch_queue_reader(RG_ID);
        assert_eq!(registry.len(), 1);
        assert_eq!(try_pop(&reader), Some(assignment));
        Ok(())
    }

    #[test]
    fn a_group_created_after_the_session_advances_carries_the_new_session() {
        let session_tracker = SessionTracker::new(SESSION_ID);
        let registry = DispatchQueueRegistry::new(session_tracker.clone());
        let reader = registry.get_dispatch_queue_reader(RG_ID);
        assert_eq!(reader.session_id(), SESSION_ID);

        assert!(session_tracker.try_advance(SESSION_ID + 1));
        assert_eq!(
            registry.get_dispatch_queue_reader(OTHER_RG_ID).session_id(),
            SESSION_ID + 1
        );
        assert_eq!(reader.session_id(), SESSION_ID);
    }

    #[test]
    fn a_group_recreated_after_a_clear_carries_the_current_session() {
        let session_tracker = SessionTracker::new(SESSION_ID);
        let registry = DispatchQueueRegistry::new(session_tracker.clone());
        assert_eq!(
            registry.get_dispatch_queue_reader(RG_ID).session_id(),
            SESSION_ID
        );

        registry.clear();
        assert!(session_tracker.try_advance(SESSION_ID + 1));
        assert_eq!(
            registry.get_dispatch_queue_reader(RG_ID).session_id(),
            SESSION_ID + 1
        );
    }

    #[test]
    fn queue_len_tracks_sends_and_pops() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let endpoints = registry.get_or_create(RG_ID);
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        assert_eq!(writer.queue_len(), 0);

        for task_index in 0..3 {
            publish(&writer, make_assignment(RG_ID, task_index))?;
        }
        assert_eq!(writer.queue_len(), 3);

        assert_eq!(try_pop(&endpoints.reader), Some(make_assignment(RG_ID, 0)));
        assert_eq!(writer.queue_len(), 2);
        Ok(())
    }

    #[test]
    fn try_send_publishes_no_hint_while_the_queue_is_covered() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let reader = registry.get_dispatch_queue_reader(RG_ID);
        let writer = registry.get_dispatch_queue_writer(RG_ID);

        let first = make_assignment(RG_ID, 0);
        publish(&writer, first)?;
        let hint = try_next_hint(&registry).expect("the queued assignment is uncovered");
        assert_eq!(hint.rg_id(), RG_ID);

        // A pinned pop empties the queue without spending the hint, so the hint outlives the
        // assignment it was published for and covers whatever is queued next.
        assert_eq!(try_pop(&reader), Some(first));

        let second = make_assignment(RG_ID, 1);
        publish(&writer, second)?;
        assert_eq!(try_next_hint(&registry).map(|hint| hint.rg_id()), None);

        assert_eq!(hint.consume_and_try_recv(), Some(second));
        Ok(())
    }

    #[test]
    fn try_send_publishes_one_hint_per_uncovered_assignment() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        let first = make_assignment(RG_ID, 0);
        let second = make_assignment(RG_ID, 1);
        publish(&writer, first)?;
        publish(&writer, second)?;

        // Two queued assignments outrun a count that starts at zero by two, so two hints come out
        // and no third.
        let first_hint = try_next_hint(&registry).expect("neither queued assignment is covered");
        let second_hint =
            try_next_hint(&registry).expect("the second queued assignment is still uncovered");
        assert_eq!(try_next_hint(&registry).map(|hint| hint.rg_id()), None);

        assert_eq!(first_hint.consume_and_try_recv(), Some(first));
        assert_eq!(second_hint.consume_and_try_recv(), Some(second));
        Ok(())
    }

    #[test]
    fn try_send_reports_a_closed_group_queue() {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let endpoints = registry.get_or_create(RG_ID);
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        assert!(endpoints.reader.inner.receiver.close());

        // A closed queue has to reach the caller as an error it must handle, rather than as a
        // silently dropped assignment.
        assert!(matches!(
            writer.try_send(make_assignment(RG_ID, 0)),
            Err(SchedulerError::DispatchQueueClosed)
        ));
        assert_eq!(writer.queue_len(), 0);

        // A send that queued nothing must not hint at anything either.
        assert_eq!(try_next_hint(&registry).map(|hint| hint.rg_id()), None);
    }

    #[test]
    fn try_send_reports_a_closed_broadcast_queue() {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        assert!(registry.inner.broadcast_receiver.close());

        assert!(matches!(
            writer.try_send(make_assignment(RG_ID, 0)),
            Err(SchedulerError::DispatchQueueClosed)
        ));

        // The two closures report the same error, so the queue is what tells them apart: this one
        // reached the group's queue first and lost only the hint.
        assert_eq!(writer.queue_len(), 1);
    }

    #[test]
    fn try_send_publishes_a_hint_onto_the_same_queue() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;

        let hint = try_next_hint(&registry).expect("the queued assignment is uncovered");
        assert_eq!(hint.rg_id(), RG_ID);
        assert_eq!(hint.session_id(), SESSION_ID);
        assert_eq!(hint.consume_and_try_recv(), Some(assignment));
        assert_eq!(writer.queue_len(), 0);

        // The hint spent above was drawn on this writer's count, so the next assignment finds the
        // group uncovered again.
        let next = make_assignment(RG_ID, 1);
        publish(&writer, next)?;
        assert_eq!(
            try_next_hint(&registry).map(|hint| hint.rg_id()),
            Some(RG_ID)
        );
        Ok(())
    }

    #[test]
    fn writer_pairs_the_sender_with_its_own_group_reader() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let reader = registry.get_dispatch_queue_reader(RG_ID);
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        let other_writer = registry.get_dispatch_queue_writer(OTHER_RG_ID);

        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        assert_eq!(
            try_next_hint(&registry).map(|hint| hint.rg_id()),
            Some(RG_ID)
        );
        assert_eq!(other_writer.queue_len(), 0);

        // A pinned pop leaves that hint outstanding on this group's count alone.
        assert_eq!(try_pop(&reader), Some(assignment));

        // Were the two writers paired with one reader, the outstanding hint would already cover the
        // other group's assignment and no second hint would come out.
        let other_assignment = make_assignment(OTHER_RG_ID, 1);
        publish(&other_writer, other_assignment)?;
        let other_hint =
            try_next_hint(&registry).expect("the other group's queued assignment is uncovered");
        assert_eq!(other_hint.rg_id(), OTHER_RG_ID);
        assert_eq!(other_hint.consume_and_try_recv(), Some(other_assignment));
        Ok(())
    }

    #[tokio::test]
    async fn recv_pinned_pops_without_touching_the_hint_count() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let reader = registry.get_dispatch_queue_reader(RG_ID);
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        let first = make_assignment(RG_ID, 0);
        publish(&writer, first)?;
        let hint = try_next_hint(&registry).expect("the queued assignment is uncovered");

        assert_eq!(reader.recv_pinned(RECV_WAIT).await, Some(first));
        assert_eq!(writer.queue_len(), 0);
        assert_eq!(reader.recv_pinned(RECV_WAIT).await, None);

        // Neither the pop nor the expired wait spent the hint, so the next assignment is covered by
        // the one still outstanding.
        let second = make_assignment(RG_ID, 1);
        publish(&writer, second)?;
        assert_eq!(try_next_hint(&registry).map(|hint| hint.rg_id()), None);

        // Spending that hint uncovers the group again, so what the pinned pops left standing was
        // exactly the one hint.
        assert_eq!(hint.consume_and_try_recv(), Some(second));
        let third = make_assignment(RG_ID, 2);
        publish(&writer, third)?;
        assert_eq!(
            try_next_hint(&registry).map(|hint| hint.rg_id()),
            Some(RG_ID)
        );
        Ok(())
    }

    #[test]
    fn consume_and_try_recv_spends_one_hint_per_pop() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let reader = registry.get_dispatch_queue_reader(RG_ID);
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        let first = make_assignment(RG_ID, 0);
        let second = make_assignment(RG_ID, 1);
        publish(&writer, first)?;
        publish(&writer, second)?;
        let first_hint = try_next_hint(&registry).expect("neither queued assignment is covered");
        let second_hint =
            try_next_hint(&registry).expect("the second queued assignment is still uncovered");
        assert_eq!(first_hint.rg_id(), RG_ID);
        assert_eq!(second_hint.rg_id(), RG_ID);

        assert_eq!(first_hint.consume_and_try_recv(), Some(first));
        assert_eq!(writer.queue_len(), 1);

        // One hint went with the pop, so a pinned pop that empties the queue leaves exactly one
        // hint standing, and the next assignment comes out covered.
        assert_eq!(try_pop(&reader), Some(second));
        let third = make_assignment(RG_ID, 2);
        publish(&writer, third)?;
        assert_eq!(try_next_hint(&registry).map(|hint| hint.rg_id()), None);

        // That last hint was the last one: once it is spent, a newly queued assignment is
        // uncovered.
        assert_eq!(second_hint.consume_and_try_recv(), Some(third));
        let fourth = make_assignment(RG_ID, 3);
        publish(&writer, fourth)?;
        assert_eq!(
            try_next_hint(&registry).map(|hint| hint.rg_id()),
            Some(RG_ID)
        );
        Ok(())
    }

    #[test]
    fn consume_and_try_recv_spends_a_stale_hint_and_yields_nothing() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let reader = registry.get_dispatch_queue_reader(RG_ID);
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        let hint = try_next_hint(&registry).expect("the queued assignment is uncovered");

        // A pinned pop empties the queue without spending the hint, which is what leaves the hint
        // covering an assignment that is no longer there.
        assert_eq!(try_pop(&reader), Some(assignment));

        assert_eq!(hint.consume_and_try_recv(), None);

        // The stale hint was spent all the same, so the next assignment is uncovered.
        let next = make_assignment(RG_ID, 1);
        publish(&writer, next)?;
        let next_hint =
            try_next_hint(&registry).expect("the stale pop spent the group's only hint");
        assert_eq!(next_hint.consume_and_try_recv(), Some(next));
        Ok(())
    }

    #[test]
    fn clear_drops_every_group() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        registry.get_or_create(OTHER_RG_ID);
        assert_eq!(registry.len(), 2);

        registry.clear();
        assert_eq!(registry.len(), 0);

        // A group named again after a clear is a new group, whose queue the dropped group's writer
        // cannot reach.
        let recreated = registry.get_or_create(RG_ID);
        assert_eq!(registry.len(), 1);
        publish(&writer, make_assignment(RG_ID, 0))?;
        assert_eq!(registry.get_dispatch_queue_writer(RG_ID).queue_len(), 0);
        assert_eq!(try_pop(&recreated.reader), None);
        Ok(())
    }

    #[test]
    fn clear_drains_the_broadcast_queue() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        publish(&writer, make_assignment(RG_ID, 0))?;

        registry.clear();

        // The broadcast queue outlives the bump, so a hint published before it would otherwise be
        // handed to an execution manager of the new session.
        assert_eq!(try_next_hint(&registry).map(|hint| hint.rg_id()), None);

        // The drain leaves the queue usable: a hint published after the bump still comes through.
        let recreated_writer = registry.get_dispatch_queue_writer(RG_ID);
        publish(&recreated_writer, make_assignment(RG_ID, 1))?;
        assert_eq!(
            try_next_hint(&registry).map(|hint| hint.rg_id()),
            Some(RG_ID)
        );
        Ok(())
    }

    #[tokio::test]
    async fn clear_closes_the_queue_of_a_group_only_a_reader_holds() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let reader = registry.get_dispatch_queue_reader(RG_ID);
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        let hint = try_next_hint(&registry).expect("the queued assignment is uncovered");

        // The queue has to be drained for the wait below to end on closure rather than on the
        // assignment the hint was published for.
        assert_eq!(try_pop(&reader), Some(assignment));

        // The clear can only close the queue once no sender is left, so the writer has to go first.
        drop(writer);
        registry.clear();

        // The wait has to end on closure rather than on expiry, so that an execution manager
        // blocked on a group the clear removed is released instead of sitting out its full poll.
        let started_at = Instant::now();
        assert_eq!(reader.recv_pinned(RECV_WAIT).await, None);
        assert!(started_at.elapsed() < RECV_WAIT);

        assert_eq!(hint.consume_and_try_recv(), None);
        Ok(())
    }

    #[tokio::test]
    async fn next_hint_returns_a_published_hint() -> anyhow::Result<()> {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));
        let writer = registry.get_dispatch_queue_writer(RG_ID);
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;

        let hint = registry
            .next_hint(RECV_WAIT)
            .await
            .expect("the queued assignment is uncovered");
        assert_eq!(hint.rg_id(), RG_ID);
        assert_eq!(hint.session_id(), SESSION_ID);
        assert_eq!(hint.consume_and_try_recv(), Some(assignment));
        Ok(())
    }

    #[tokio::test]
    async fn next_hint_waits_out_an_empty_broadcast_queue() {
        let registry = DispatchQueueRegistry::new(SessionTracker::new(SESSION_ID));

        // The registry holds both ends of the broadcast queue, so the wait can only end on expiry.
        let started_at = Instant::now();
        assert_eq!(
            registry.next_hint(RECV_WAIT).await.map(|hint| hint.rg_id()),
            None
        );
        assert!(started_at.elapsed() >= RECV_WAIT);
    }
}
