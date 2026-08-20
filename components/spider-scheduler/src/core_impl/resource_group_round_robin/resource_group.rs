//! The per-resource-group dispatch queues shared between the core and the dispatch service.
//!
//! An assignment is stored exactly once, in the queue of the resource group that owns it. The table
//! below is the only structure both sides touch. It is `Arc`-backed and shards its locking, so a
//! request naming one resource group does not contend with a request naming another; everything the
//! core keeps about a resource group beyond these endpoints stays core-private.

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
/// manager blocks on and what the core publishes as a hint to general execution managers.
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
    /// resource group table under coroutines that are already blocked here, so closure is the
    /// ordinary end of a bumped-out request and must read as "nothing to hand out".
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

    /// Consumes one outstanding hint and attempts a single non-blocking pop.
    ///
    /// The caller must hold exactly one outstanding hint for this group. A decrement with no hint
    /// outstanding wraps the count, permanently corrupting the group's hint accounting.
    ///
    /// A closed queue yields [`None`] by design rather than an error: a session bump clears the
    /// resource group table, so a hint carried across the bump names a closed queue and must read
    /// as "nothing to hand out", exactly like any other stale hint.
    ///
    /// # Cancel safety
    ///
    /// This method is synchronous, so its decrement and its pop cannot be separated by
    /// cancellation. The caller must still not yield between receiving the hint and calling it: a
    /// future can only be dropped at an await point, so an await in that window would let a
    /// cancelled request consume the hint without decrementing the counter, permanently
    /// overstating the group's coverage.
    ///
    /// # Returns
    ///
    /// The next assignment, or [`None`] if the hint was stale and the queue is empty, or if the
    /// queue was closed.
    pub(super) fn consume_hint_and_try_recv(&self) -> Option<TaskAssignment> {
        self.inner.decrement_living_hint();
        self.inner.receiver.try_recv().ok()
    }
}

/// The writer side of one resource group's dispatch queue, owned by the group's scheduling unit.
///
/// The writer carries the group's own reader rather than a separate living hint counter handle, so
/// the hint count it operates is necessarily the one the read side spends.
#[derive(Debug)]
pub(super) struct RgDispatchQueueWriter {
    sender: async_channel::Sender<TaskAssignment>,
    reader: RgDispatchQueueReader,
}

impl RgDispatchQueueWriter {
    /// Publishes `assignment` into the group's dispatch queue.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::DispatchQueueClosed`] if the group's dispatch queue is closed.
    pub(super) fn try_send(&self, assignment: TaskAssignment) -> Result<(), SchedulerError> {
        self.sender
            .try_send(assignment)
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
    /// The reader a hint carries, through which a general execution manager pops from this group,
    /// or [`None`] if the group's outstanding hints already cover its queue and no hint is needed.
    pub(super) fn try_make_hint(&self) -> Option<RgDispatchQueueReader> {
        let dispatch_queue_size = self.queue_len();
        if self.reader.inner.living_hint() >= dispatch_queue_size {
            return None;
        }

        self.reader.inner.increment_living_hint();
        Some(self.reader.clone())
    }
}

/// The registry of dispatch queue endpoints, shared between the core and the dispatch service.
///
/// Either side may be the first to name a resource group -- a pinned execution manager can connect
/// before any task of its group has been scheduled -- so both lookups create the group on demand.
///
/// Per-group queues are unbounded. The admission threshold is what limits a group's occupancy, so a
/// channel bound would be a second, redundant limit whose only possible effect is to reject a send
/// the design's coverage proof requires to succeed.
#[derive(Clone, Debug)]
pub(super) struct ResourceGroupTable {
    table: Arc<DashMap<ResourceGroupId, RgDispatchQueueEndpoints>>,
    session_tracker: SessionTracker,
}

impl ResourceGroupTable {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created, empty table stamping every group it creates with `session_tracker`'s
    /// current session.
    pub(super) fn new(session_tracker: SessionTracker) -> Self {
        Self {
            table: Arc::new(DashMap::new()),
            session_tracker,
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
        self.get_or_create(rg_id).writer()
    }

    /// # Returns
    ///
    /// The number of resource groups the table currently holds.
    pub(super) fn len(&self) -> usize {
        self.table.len()
    }

    /// Drops every resource group.
    ///
    /// An execution manager still blocked on a dropped group's reader keeps that queue alive, but
    /// every assignment in it fails the dispatch service's session check.
    pub(super) fn clear(&self) {
        self.table.clear();
    }

    /// # Returns
    ///
    /// Both ends of `rg_id`'s dispatch queue, creating the group if it has none.
    fn get_or_create(&self, rg_id: ResourceGroupId) -> RgDispatchQueueEndpoints {
        self.table
            .entry(rg_id)
            .or_insert_with(|| {
                let (sender, receiver) = async_channel::unbounded();
                RgDispatchQueueEndpoints {
                    sender,
                    reader: RgDispatchQueueReader::new(
                        receiver,
                        rg_id,
                        self.session_tracker.current(),
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
    /// A newly created writer pairing the group's sender with the group's own reader.
    fn writer(&self) -> RgDispatchQueueWriter {
        RgDispatchQueueWriter {
            sender: self.sender.clone(),
            reader: self.reader.clone(),
        }
    }
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

    /// The session the table's tracker starts in.
    const SESSION_ID: SessionId = 7;

    /// The job every published assignment is drawn from.
    const JOB_ID: JobId = JobId::from(3);

    /// The time a test lets [`RgDispatchQueueReader::recv_pinned`] block before it concludes that
    /// nothing is coming.
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

    #[test]
    fn get_or_create_returns_endpoints_onto_the_same_queue() -> anyhow::Result<()> {
        let session_tracker = SessionTracker::new(SESSION_ID);
        let table = ResourceGroupTable::new(session_tracker.clone());
        let endpoints = table.get_or_create(RG_ID);
        assert_eq!(table.len(), 1);
        assert_eq!(endpoints.reader.rg_id(), RG_ID);
        assert_eq!(endpoints.reader.session_id(), SESSION_ID);

        // A second lookup must find the group rather than replace it, so the session it reports is
        // still the one it was created with.
        assert!(session_tracker.try_advance(SESSION_ID + 1));
        let looked_up = table.get_or_create(RG_ID);
        assert_eq!(table.len(), 1);
        assert_eq!(looked_up.reader.session_id(), SESSION_ID);

        let writer = endpoints.writer();
        let looked_up_writer = looked_up.writer();
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
        let table = ResourceGroupTable::new(session_tracker.clone());
        let endpoints = table.get_or_create(RG_ID);
        assert!(session_tracker.try_advance(SESSION_ID + 1));
        let other_endpoints = table.get_or_create(OTHER_RG_ID);
        assert_eq!(table.len(), 2);
        assert_eq!(other_endpoints.reader.rg_id(), OTHER_RG_ID);
        assert_eq!(other_endpoints.reader.session_id(), SESSION_ID + 1);

        let writer = endpoints.writer();
        let other_writer = other_endpoints.writer();
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        writer
            .try_make_hint()
            .expect("the queued assignment is uncovered");

        assert_eq!(other_writer.queue_len(), 0);
        assert_eq!(try_pop(&other_endpoints.reader), None);

        // The hint taken out above landed on this group's count alone, so the other group is still
        // uncovered and takes out a hint of its own.
        let other_assignment = make_assignment(OTHER_RG_ID, 1);
        publish(&other_writer, other_assignment)?;
        assert_eq!(
            other_writer.try_make_hint().map(|hint| hint.rg_id()),
            Some(OTHER_RG_ID)
        );
        assert_eq!(try_pop(&endpoints.reader), Some(assignment));
        Ok(())
    }

    #[test]
    fn get_dispatch_queue_reader_creates_a_missing_group() -> anyhow::Result<()> {
        let table = ResourceGroupTable::new(SessionTracker::new(SESSION_ID));
        assert_eq!(table.len(), 0);

        let reader = table.get_dispatch_queue_reader(RG_ID);
        assert_eq!(table.len(), 1);
        assert_eq!(reader.rg_id(), RG_ID);
        assert_eq!(reader.session_id(), SESSION_ID);
        assert_eq!(try_pop(&reader), None);

        // The group the reader created is the one the core later writes into.
        let writer = table.get_dispatch_queue_writer(RG_ID);
        assert_eq!(table.len(), 1);
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        assert_eq!(writer.queue_len(), 1);
        assert_eq!(try_pop(&reader), Some(assignment));
        Ok(())
    }

    #[test]
    fn get_dispatch_queue_writer_creates_a_missing_group() -> anyhow::Result<()> {
        let table = ResourceGroupTable::new(SessionTracker::new(SESSION_ID));
        assert_eq!(table.len(), 0);

        let writer = table.get_dispatch_queue_writer(RG_ID);
        assert_eq!(table.len(), 1);
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        let hint = writer
            .try_make_hint()
            .expect("the queued assignment is uncovered");
        assert_eq!(hint.rg_id(), RG_ID);
        assert_eq!(hint.session_id(), SESSION_ID);

        // The group the writer created is the one the dispatch service later reads from.
        let reader = table.get_dispatch_queue_reader(RG_ID);
        assert_eq!(table.len(), 1);
        assert_eq!(try_pop(&reader), Some(assignment));
        Ok(())
    }

    #[test]
    fn a_group_created_after_the_session_advances_carries_the_new_session() {
        let session_tracker = SessionTracker::new(SESSION_ID);
        let table = ResourceGroupTable::new(session_tracker.clone());
        let reader = table.get_dispatch_queue_reader(RG_ID);
        assert_eq!(reader.session_id(), SESSION_ID);

        assert!(session_tracker.try_advance(SESSION_ID + 1));
        assert_eq!(
            table.get_dispatch_queue_reader(OTHER_RG_ID).session_id(),
            SESSION_ID + 1
        );
        assert_eq!(reader.session_id(), SESSION_ID);
    }

    #[test]
    fn a_group_recreated_after_a_clear_carries_the_current_session() {
        let session_tracker = SessionTracker::new(SESSION_ID);
        let table = ResourceGroupTable::new(session_tracker.clone());
        assert_eq!(
            table.get_dispatch_queue_reader(RG_ID).session_id(),
            SESSION_ID
        );

        table.clear();
        assert!(session_tracker.try_advance(SESSION_ID + 1));
        assert_eq!(
            table.get_dispatch_queue_reader(RG_ID).session_id(),
            SESSION_ID + 1
        );
    }

    #[test]
    fn queue_len_tracks_sends_and_pops() -> anyhow::Result<()> {
        let table = ResourceGroupTable::new(SessionTracker::new(SESSION_ID));
        let endpoints = table.get_or_create(RG_ID);
        let writer = endpoints.writer();
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
    fn try_make_hint_declines_while_the_queue_is_covered() -> anyhow::Result<()> {
        let table = ResourceGroupTable::new(SessionTracker::new(SESSION_ID));
        let writer = table.get_dispatch_queue_writer(RG_ID);

        // An empty queue has nothing to cover.
        assert_eq!(writer.queue_len(), 0);
        assert_eq!(writer.try_make_hint().map(|hint| hint.rg_id()), None);

        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        let hint = writer
            .try_make_hint()
            .expect("the queued assignment is uncovered");

        // One assignment covered by one outstanding hint needs no second one, however often the
        // writer is asked.
        assert_eq!(writer.try_make_hint().map(|hint| hint.rg_id()), None);
        assert_eq!(writer.try_make_hint().map(|hint| hint.rg_id()), None);

        assert_eq!(hint.consume_hint_and_try_recv(), Some(assignment));
        Ok(())
    }

    #[test]
    fn try_make_hint_takes_out_one_hint_per_uncovered_assignment() -> anyhow::Result<()> {
        let table = ResourceGroupTable::new(SessionTracker::new(SESSION_ID));
        let endpoints = table.get_or_create(RG_ID);
        let writer = endpoints.writer();
        let first = make_assignment(RG_ID, 0);
        let second = make_assignment(RG_ID, 1);
        publish(&writer, first)?;
        publish(&writer, second)?;

        // Two queued assignments outrun a count of zero by two, so two hints come out and a third
        // does not.
        let first_hint = writer
            .try_make_hint()
            .expect("neither queued assignment is covered");
        let second_hint = writer
            .try_make_hint()
            .expect("the second queued assignment is still uncovered");
        assert_eq!(writer.try_make_hint().map(|hint| hint.rg_id()), None);

        assert_eq!(first_hint.consume_hint_and_try_recv(), Some(first));
        assert_eq!(second_hint.consume_hint_and_try_recv(), Some(second));
        Ok(())
    }

    #[test]
    fn try_send_reports_a_closed_queue() {
        let table = ResourceGroupTable::new(SessionTracker::new(SESSION_ID));
        let endpoints = table.get_or_create(RG_ID);
        let writer = endpoints.writer();
        assert!(endpoints.reader.inner.receiver.close());

        // A closed queue has to reach the caller as an error it must handle, rather than as a
        // silently dropped assignment.
        assert!(matches!(
            writer.try_send(make_assignment(RG_ID, 0)),
            Err(SchedulerError::DispatchQueueClosed)
        ));
        assert_eq!(writer.queue_len(), 0);
    }

    #[test]
    fn try_make_hint_returns_a_reader_onto_the_same_queue() -> anyhow::Result<()> {
        let table = ResourceGroupTable::new(SessionTracker::new(SESSION_ID));
        let endpoints = table.get_or_create(RG_ID);
        let writer = endpoints.writer();
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;

        let hint = writer
            .try_make_hint()
            .expect("the queued assignment is uncovered");
        assert_eq!(hint.rg_id(), RG_ID);
        assert_eq!(hint.session_id(), SESSION_ID);
        assert_eq!(hint.consume_hint_and_try_recv(), Some(assignment));
        assert_eq!(writer.queue_len(), 0);

        // The hint spent above was drawn on this writer's count, so the next assignment finds the
        // group uncovered again.
        let next = make_assignment(RG_ID, 1);
        publish(&writer, next)?;
        assert_eq!(writer.try_make_hint().map(|hint| hint.rg_id()), Some(RG_ID));
        Ok(())
    }

    #[test]
    fn endpoints_writer_pairs_the_sender_with_its_own_group_reader() -> anyhow::Result<()> {
        let table = ResourceGroupTable::new(SessionTracker::new(SESSION_ID));
        let endpoints = table.get_or_create(RG_ID);
        let other_endpoints = table.get_or_create(OTHER_RG_ID);
        let writer = endpoints.writer();
        let other_writer = other_endpoints.writer();

        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        let hint = writer
            .try_make_hint()
            .expect("the queued assignment is uncovered");
        assert_eq!(hint.rg_id(), RG_ID);
        assert_eq!(other_writer.queue_len(), 0);

        // Were the two writers paired with one reader, the hint above would already cover the other
        // group's assignment and no second hint would come out.
        let other_assignment = make_assignment(OTHER_RG_ID, 1);
        publish(&other_writer, other_assignment)?;
        let other_hint = other_writer
            .try_make_hint()
            .expect("the other group's queued assignment is uncovered");
        assert_eq!(other_hint.rg_id(), OTHER_RG_ID);

        assert_eq!(hint.consume_hint_and_try_recv(), Some(assignment));
        assert_eq!(
            other_hint.consume_hint_and_try_recv(),
            Some(other_assignment)
        );
        Ok(())
    }

    #[tokio::test]
    async fn recv_pinned_pops_without_touching_the_hint_count() -> anyhow::Result<()> {
        let table = ResourceGroupTable::new(SessionTracker::new(SESSION_ID));
        let endpoints = table.get_or_create(RG_ID);
        let writer = endpoints.writer();
        let first = make_assignment(RG_ID, 0);
        publish(&writer, first)?;
        let hint = writer
            .try_make_hint()
            .expect("the queued assignment is uncovered");

        assert_eq!(endpoints.reader.recv_pinned(RECV_WAIT).await, Some(first));
        assert_eq!(writer.queue_len(), 0);
        assert_eq!(endpoints.reader.recv_pinned(RECV_WAIT).await, None);

        // Neither the pop nor the expired wait spent the hint, so the next assignment is covered by
        // the one still outstanding.
        let second = make_assignment(RG_ID, 1);
        publish(&writer, second)?;
        assert_eq!(writer.try_make_hint().map(|hint| hint.rg_id()), None);

        // Spending that hint uncovers the group again, so what the pinned pops left standing was
        // exactly the one hint.
        assert_eq!(hint.consume_hint_and_try_recv(), Some(second));
        let third = make_assignment(RG_ID, 2);
        publish(&writer, third)?;
        assert_eq!(writer.try_make_hint().map(|hint| hint.rg_id()), Some(RG_ID));
        Ok(())
    }

    #[test]
    fn consume_hint_and_try_recv_spends_one_hint_per_pop() -> anyhow::Result<()> {
        let table = ResourceGroupTable::new(SessionTracker::new(SESSION_ID));
        let endpoints = table.get_or_create(RG_ID);
        let writer = endpoints.writer();
        let first = make_assignment(RG_ID, 0);
        let second = make_assignment(RG_ID, 1);
        publish(&writer, first)?;
        writer
            .try_make_hint()
            .expect("the first assignment is uncovered");
        publish(&writer, second)?;
        writer
            .try_make_hint()
            .expect("the second assignment is uncovered");
        assert_eq!(writer.try_make_hint().map(|hint| hint.rg_id()), None);

        assert_eq!(endpoints.reader.consume_hint_and_try_recv(), Some(first));
        assert_eq!(writer.queue_len(), 1);

        // One hint went with the pop, leaving the queue's single remaining assignment covered by
        // the single remaining hint.
        assert_eq!(writer.try_make_hint().map(|hint| hint.rg_id()), None);

        assert_eq!(endpoints.reader.consume_hint_and_try_recv(), Some(second));
        assert_eq!(writer.queue_len(), 0);

        // The second pop spent the last hint, so a newly queued assignment is uncovered.
        let third = make_assignment(RG_ID, 2);
        publish(&writer, third)?;
        assert_eq!(writer.try_make_hint().map(|hint| hint.rg_id()), Some(RG_ID));
        Ok(())
    }

    #[test]
    fn consume_hint_and_try_recv_spends_a_stale_hint_and_yields_nothing() -> anyhow::Result<()> {
        let table = ResourceGroupTable::new(SessionTracker::new(SESSION_ID));
        let endpoints = table.get_or_create(RG_ID);
        let writer = endpoints.writer();
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        let hint = writer
            .try_make_hint()
            .expect("the queued assignment is uncovered");

        // A pinned pop empties the queue without spending the hint, which is what leaves the hint
        // covering an assignment that is no longer there.
        assert_eq!(try_pop(&endpoints.reader), Some(assignment));

        assert_eq!(hint.consume_hint_and_try_recv(), None);

        // The stale hint was spent all the same, so the next assignment is uncovered.
        let next = make_assignment(RG_ID, 1);
        publish(&writer, next)?;
        let next_hint = writer
            .try_make_hint()
            .expect("the stale pop spent the group's only hint");
        assert_eq!(next_hint.consume_hint_and_try_recv(), Some(next));
        Ok(())
    }

    #[test]
    fn clear_drops_every_group() -> anyhow::Result<()> {
        let table = ResourceGroupTable::new(SessionTracker::new(SESSION_ID));
        let writer = table.get_dispatch_queue_writer(RG_ID);
        table.get_or_create(OTHER_RG_ID);
        assert_eq!(table.len(), 2);

        table.clear();
        assert_eq!(table.len(), 0);

        // A group named again after a clear is a new group, whose queue the dropped group's writer
        // cannot reach.
        let recreated = table.get_or_create(RG_ID);
        assert_eq!(table.len(), 1);
        publish(&writer, make_assignment(RG_ID, 0))?;
        assert_eq!(recreated.writer().queue_len(), 0);
        assert_eq!(try_pop(&recreated.reader), None);
        Ok(())
    }

    #[tokio::test]
    async fn clear_closes_the_queue_of_a_group_only_a_reader_holds() -> anyhow::Result<()> {
        let table = ResourceGroupTable::new(SessionTracker::new(SESSION_ID));
        let reader = table.get_dispatch_queue_reader(RG_ID);
        let writer = table.get_dispatch_queue_writer(RG_ID);
        let assignment = make_assignment(RG_ID, 0);
        publish(&writer, assignment)?;
        let hint = writer
            .try_make_hint()
            .expect("the queued assignment is uncovered");

        // The queue has to be drained for the wait below to end on closure rather than on the
        // assignment the hint was taken out for.
        assert_eq!(try_pop(&reader), Some(assignment));

        // The clear can only close the queue once no sender is left, so the writer has to go first.
        drop(writer);
        table.clear();

        // The wait has to end on closure rather than on expiry, so that an execution manager
        // blocked on a group the clear removed is released instead of sitting out its full poll.
        let started_at = Instant::now();
        assert_eq!(reader.recv_pinned(RECV_WAIT).await, None);
        assert!(started_at.elapsed() < RECV_WAIT);

        assert_eq!(hint.consume_hint_and_try_recv(), None);
        Ok(())
    }
}
