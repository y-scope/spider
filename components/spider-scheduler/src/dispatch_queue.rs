//! The dispatching queue that decouples the scheduler core's placement decisions from the
//! execution-manager-facing service.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use spider_core::types::id::SessionId;
use tokio::sync::RwLock;

use crate::error::SchedulerError;
use crate::types::TaskAssignment;

/// The writer side of the dispatching queue used by the scheduler core.
#[async_trait]
pub trait DispatchQueueSink: Send + Sync + Clone {
    /// Enqueues a task assignment for execution managers to consume.
    ///
    /// # Parameters
    ///
    /// * `assignment` - The task assignment to enqueue.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::DispatchQueueClosed`] if the dispatching queue is closed.
    async fn enqueue(&self, assignment: TaskAssignment) -> Result<(), SchedulerError>;

    /// Bumps the session ID and invalidates all queued task assignments.
    ///
    /// # Parameters
    ///
    /// * `new_session_id` - The new session ID. Must be greater than the current session ID.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::DispatchQueueClosed`] if the dispatching queue is closed.
    /// * [`SchedulerError::InvalidSessionId`] if the new session ID is not greater than the current
    ///   session ID.
    async fn bump_session_id(&self, new_session_id: SessionId) -> Result<(), SchedulerError>;

    /// # Returns
    ///
    /// The current size of the dispatch queue.
    fn size(&self) -> usize;
}

/// The reader side of the dispatching queue, drained by the execution-manager-facing service.
#[async_trait]
pub trait DispatchQueueSource: Send + Sync + Clone {
    /// Dequeues the next task assignment for an execution manager to execute.
    ///
    /// # Parameters
    ///
    /// * `wait_time` - The maximum amount of time to wait for a task assignment.
    ///
    /// # Returns
    ///
    /// `None` if no task assignment is available within the specified wait time, or a tuple
    /// containing:
    ///
    /// * The storage session associated with the assignment.
    /// * The next task assignment ready to execute.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::DispatchQueueClosed`] if the dispatching queue is closed.
    async fn dequeue(
        &self,
        wait_time: Duration,
    ) -> Result<Option<(SessionId, TaskAssignment)>, SchedulerError>;
}

/// A cloneable writer handle for the dispatching queue, implementing [`DispatchQueueSink`] using
/// an async channel.
///
/// # NOTE
///
/// The current implementation assumes that `enqueue` and `bump_session_id` will not be called
/// concurrently: `bump_session_id` must be called before consequent `enqueue` calls to make session
/// ID consistent with the enqueued assignments.
#[derive(Clone)]
pub struct DispatchQueueWriter {
    inner: Arc<DispatchQueueWriterInner>,
}

#[async_trait]
impl DispatchQueueSink for DispatchQueueWriter {
    async fn enqueue(&self, assignment: TaskAssignment) -> Result<(), SchedulerError> {
        self.inner
            .assignment_sender
            .send(assignment)
            .await
            .map_err(|_| SchedulerError::DispatchQueueClosed)
    }

    async fn bump_session_id(&self, new_session_id: SessionId) -> Result<(), SchedulerError> {
        let mut session_id_guard = self.inner.session_id.write().await;
        if new_session_id <= *session_id_guard {
            return Err(SchedulerError::InvalidSessionId(new_session_id));
        }
        *session_id_guard = new_session_id;
        while self.inner.assignment_receiver.try_recv().is_ok() {
            // Drain the queue.
        }

        // Lock session ID for the entire duration of the drain to exclude all readers.
        drop(session_id_guard);
        Ok(())
    }

    fn size(&self) -> usize {
        self.inner.assignment_sender.len()
    }
}

/// A cloneable reader handle for the dispatching queue, implementing [`DispatchQueueSource`] using
/// an async channel.
#[derive(Clone)]
pub struct DispatchQueueReader {
    inner: Arc<DispatchQueueReaderInner>,
}

#[async_trait]
impl DispatchQueueSource for DispatchQueueReader {
    async fn dequeue(
        &self,
        wait_time: Duration,
    ) -> Result<Option<(SessionId, TaskAssignment)>, SchedulerError> {
        // Lock session ID for the entire duration of the dequeue operation to exclude any
        // `bump_session_id` operations.
        let session_id_guard = self.inner.session_id.read().await;

        if let Ok(assignment) = self.inner.assignment_receiver.try_recv() {
            return Ok(Some((*session_id_guard, assignment)));
        }

        if wait_time.is_zero() {
            return Ok(None);
        }

        match tokio::time::timeout(wait_time, self.inner.assignment_receiver.recv()).await {
            Ok(Ok(assignment)) => Ok(Some((*session_id_guard, assignment))),
            Ok(Err(_)) => Err(SchedulerError::DispatchQueueClosed),
            Err(_) => Ok(None),
        }
    }
}

/// Dispatch queue factory.
///
/// # Returns
///
/// A tuple containing:
///
/// * The writer for the scheduler core to enqueue task assignments.
/// * The reader for the execution-manager-facing service to dequeue task assignments.
#[must_use]
pub fn create_dispatch_queue(
    capacity: usize,
    init_session_id: SessionId,
) -> (DispatchQueueWriter, DispatchQueueReader) {
    let (assignment_sender, assignment_receiver) = async_channel::bounded(capacity);
    let session_id = Arc::new(RwLock::new(init_session_id));
    let writer_inner = Arc::new(DispatchQueueWriterInner {
        session_id: session_id.clone(),
        assignment_sender,
        assignment_receiver: assignment_receiver.clone(),
    });
    let reader_inner = Arc::new(DispatchQueueReaderInner {
        session_id,
        assignment_receiver,
    });
    (
        DispatchQueueWriter {
            inner: writer_inner,
        },
        DispatchQueueReader {
            inner: reader_inner,
        },
    )
}

struct DispatchQueueWriterInner {
    session_id: Arc<RwLock<SessionId>>,
    assignment_sender: async_channel::Sender<TaskAssignment>,
    assignment_receiver: async_channel::Receiver<TaskAssignment>,
}

struct DispatchQueueReaderInner {
    session_id: Arc<RwLock<SessionId>>,
    assignment_receiver: async_channel::Receiver<TaskAssignment>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use anyhow::Result;
    use dashmap::DashMap;
    use dashmap::DashSet;
    use spider_core::types::id::JobId;
    use spider_core::types::id::ResourceGroupId;
    use spider_core::types::id::SessionId;
    use spider_core::types::id::TaskAssignmentId;
    use spider_core::types::id::TaskId;
    use tokio_util::task::TaskTracker;

    use super::*;
    use crate::error::SchedulerError;
    use crate::types::TaskAssignment;

    /// Generates a [`TaskId`] backed by a module-local monotonic counter.
    ///
    /// # Returns
    ///
    /// A new [`TaskId::Index`] whose inner value is unique within the test binary.
    fn next_task_id() -> TaskId {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        TaskId::Index(COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// # Returns
    ///
    /// Forwards [`make_assignment_with_task_id`]'s return values with `task_id` set with
    /// [`next_task_id`]'s return value.
    fn make_assignment() -> TaskAssignment {
        make_assignment_with_task_id(next_task_id())
    }

    /// # Returns
    ///
    /// A new [`TaskAssignment`] with the given `task_id` and other ID fields are auto-generated.
    fn make_assignment_with_task_id(task_id: TaskId) -> TaskAssignment {
        TaskAssignment {
            id: TaskAssignmentId::random(),
            resource_group_id: ResourceGroupId::random(),
            job_id: JobId::random(),
            task_id,
        }
    }

    /// Spawns `reader_count` reader tasks that each drain the queue with `wait_time` and count the
    /// assignments they receive, looping until the queue is closed.
    ///
    /// # Returns
    ///
    /// A vector of join handles, one per spawned task; each handle yields the number of assignments
    /// that the reader pulled from the queue.
    fn spawn_counting_readers(
        reader: &DispatchQueueReader,
        reader_count: usize,
        wait_time: Duration,
    ) -> Vec<tokio::task::JoinHandle<usize>> {
        (0..reader_count)
            .map(|_| {
                let r = reader.clone();
                tokio::spawn(async move {
                    let mut count = 0usize;
                    loop {
                        match r.dequeue(wait_time).await {
                            Ok(Some(_)) => count += 1,
                            Ok(None) => (),
                            Err(_) => break,
                        }
                    }
                    count
                })
            })
            .collect()
    }

    /// Drives the pair-consistency stress scenario for one or more concurrent readers.
    ///
    /// A single producer issues `ROUNDS` rounds of `[enqueue × k_i; bump_session_id(+1)]` with
    /// batch sizes drawn from a 64-bit LCG seeded by `rng_seed`, finishes with a final batch under
    /// the latest session, and drops the writer. `reader_count` reader tasks drain the queue
    /// concurrently, each delivered assignment is tagged at enqueue time, and pair consistency is
    /// verified across the collected results once all readers are closed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`tokio::task::JoinHandle`]'s return values on failure (indicating a task panic).
    async fn run_pair_consistency_stress(reader_count: usize, rng_seed: u64) -> Result<()> {
        const INIT_SESSION: SessionId = 1;
        const ROUNDS: usize = 20;
        const CAPACITY: usize = 16;
        const FINAL_BATCH: usize = 5;

        assert!(reader_count > 0, "`reader_count` must be positive");

        let (writer, reader) = create_dispatch_queue(CAPACITY, INIT_SESSION);
        let tagged: Arc<DashMap<TaskId, SessionId>> = Arc::new(DashMap::new());

        let tagged_for_writer = tagged.clone();
        let writer_handle = tokio::spawn(async move {
            let mut current_session = INIT_SESSION;
            let mut rng = rng_seed;
            for _ in 0..ROUNDS {
                // 64-bit LCG parameters
                const LCG_MULTIPLIER: u64 = 6_364_136_223_846_793_005;
                const LCG_INCREMENT: u64 = 1_442_695_040_888_963_407;
                rng = rng.wrapping_mul(LCG_MULTIPLIER).wrapping_add(LCG_INCREMENT);
                let k = usize::try_from(rng % (CAPACITY as u64 + 1))
                    .expect("modulo result fits in usize");
                for _ in 0..k {
                    let id = next_task_id();
                    tagged_for_writer.insert(id, current_session);
                    writer
                        .enqueue(make_assignment_with_task_id(id))
                        .await
                        .expect("enqueue failed");
                }
                current_session += 1;
                writer
                    .bump_session_id(current_session)
                    .await
                    .expect("bump failed");
            }
            // Final batch under the latest session, which guarantees the readers have something to
            // drain post-bump.
            for _ in 0..FINAL_BATCH {
                let id = next_task_id();
                tagged_for_writer.insert(id, current_session);
                writer
                    .enqueue(make_assignment_with_task_id(id))
                    .await
                    .expect("enqueue failed");
            }
            drop(writer);
        });

        let all_delivered: Arc<DashMap<TaskId, SessionId>> = Arc::new(DashMap::new());
        let duplicates: Arc<DashSet<TaskId>> = Arc::new(DashSet::new());
        let tracker = TaskTracker::new();
        for _ in 0..reader_count {
            let r = reader.clone();
            let delivered_for_reader = all_delivered.clone();
            let duplicates_for_reader = duplicates.clone();
            tracker.spawn(async move {
                loop {
                    match r.dequeue(Duration::from_millis(500)).await {
                        Ok(Some((session, assignment))) => {
                            if delivered_for_reader
                                .insert(assignment.task_id, session)
                                .is_some()
                            {
                                duplicates_for_reader.insert(assignment.task_id);
                            }
                        }
                        Ok(None) => (),
                        Err(_) => break,
                    }
                }
            });
        }
        tracker.close();
        drop(reader);

        writer_handle.await?;
        tracker.wait().await;

        assert!(
            duplicates.is_empty(),
            "duplicate deliveries: {:?}",
            duplicates.iter().map(|e| *e.key()).collect::<Vec<_>>(),
        );
        for entry in all_delivered.iter() {
            let task_id = *entry.key();
            let delivered_session = *entry.value();
            let expected = tagged.get(&task_id).map(|e| *e.value());
            assert_eq!(
                Some(delivered_session),
                expected,
                "pair stamp mismatch: task_id={task_id:?}, delivered={delivered_session}, \
                 expected={expected:?}",
            );
        }

        let delivered_count = all_delivered.len();
        assert!(
            delivered_count >= FINAL_BATCH,
            "expected at least the final batch ({FINAL_BATCH}) to be delivered, got \
             {delivered_count}",
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sanity_round_trip_and_initial_session() -> Result<()> {
        const SESSION_ID: SessionId = 1;
        let (writer, reader) = create_dispatch_queue(8, SESSION_ID);
        let assignment = make_assignment();

        writer.enqueue(assignment).await?;

        let (session, received) = reader
            .dequeue(Duration::from_millis(1))
            .await?
            .expect("expected an assignment");
        assert_eq!(session, SESSION_ID);
        assert_eq!(received, assignment);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn load_balancing_across_consumers() -> Result<()> {
        const N: usize = 100;
        const M: usize = 4;
        let (writer, reader) = create_dispatch_queue(32, 1);

        let reader_handles = spawn_counting_readers(&reader, M, Duration::from_millis(500));
        drop(reader);

        for _ in 0..N {
            writer
                .enqueue(make_assignment())
                .await
                .expect("enqueue failed");
        }
        drop(writer);

        let mut total = 0usize;
        for handle in reader_handles {
            total += handle.await?;
        }
        assert_eq!(total, N);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn many_readers_with_slow_producer() -> Result<()> {
        const N: usize = 10;
        const M: usize = 16;
        let (writer, reader) = create_dispatch_queue(8, 1);

        let reader_handles = spawn_counting_readers(&reader, M, Duration::from_millis(500));
        drop(reader);

        for _ in 0..N {
            writer
                .enqueue(make_assignment())
                .await
                .expect("enqueue failed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(writer);

        let mut total = 0usize;
        for handle in reader_handles {
            total += handle.await?;
        }
        assert_eq!(total, N);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bump_same_session_id_returns_invalid() -> Result<()> {
        const SESSION_ID: SessionId = 5;
        let (writer, _reader) = create_dispatch_queue(8, SESSION_ID);
        let result = writer.bump_session_id(SESSION_ID).await;
        assert!(
            matches!(result, Err(SchedulerError::InvalidSessionId(5))),
            "expected InvalidSessionId(5), got {result:?}",
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bump_smaller_session_id_returns_invalid() -> Result<()> {
        const SESSION_ID: SessionId = 5;
        const SMALLER_SESSION_ID: SessionId = SESSION_ID - 1;

        let (writer, _reader) = create_dispatch_queue(8, SESSION_ID);
        let result = writer.bump_session_id(SMALLER_SESSION_ID).await;
        assert!(matches!(
            result,
            Err(SchedulerError::InvalidSessionId(SMALLER_SESSION_ID))
        ));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bump_higher_session_id_succeeds() -> Result<()> {
        const SESSION_ID: SessionId = 5;
        const NEW_SESSION_ID: SessionId = SESSION_ID + 1;

        let (writer, reader) = create_dispatch_queue(8, SESSION_ID);
        writer.bump_session_id(NEW_SESSION_ID).await?;
        writer.enqueue(make_assignment()).await?;

        let (session, _) = reader
            .dequeue(Duration::from_secs(1))
            .await?
            .expect("expected an assignment");
        assert_eq!(session, NEW_SESSION_ID);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pre_bump_items_not_delivered() -> Result<()> {
        let (writer, reader) = create_dispatch_queue(8, 1);
        writer.enqueue(make_assignment()).await?;
        writer.enqueue(make_assignment()).await?;
        writer.bump_session_id(2).await?;

        let result = reader.dequeue(Duration::from_millis(100)).await?;
        assert_eq!(result, None);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn post_bump_items_paired_with_new_session() -> Result<()> {
        let (writer, reader) = create_dispatch_queue(8, 1);
        writer.bump_session_id(2).await?;
        let assignment = make_assignment();
        writer.enqueue(assignment).await?;

        let (session, received) = reader
            .dequeue(Duration::from_secs(1))
            .await?
            .expect("expected an assignment");
        assert_eq!(session, 2);
        assert_eq!(received, assignment);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn successive_bumps() -> Result<()> {
        let (writer, reader) = create_dispatch_queue(8, 1);
        writer.bump_session_id(2).await?;
        writer.bump_session_id(3).await?;

        let equal = writer.bump_session_id(3).await;
        assert!(
            matches!(equal, Err(SchedulerError::InvalidSessionId(3))),
            "expected InvalidSessionId(3), got {equal:?}",
        );
        let smaller = writer.bump_session_id(2).await;
        assert!(
            matches!(smaller, Err(SchedulerError::InvalidSessionId(2))),
            "expected InvalidSessionId(2), got {smaller:?}",
        );

        writer.enqueue(make_assignment()).await?;
        let (session, _) = reader
            .dequeue(Duration::from_secs(1))
            .await?
            .expect("expected an assignment");
        assert_eq!(session, 3);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn size_zero_after_bump() -> Result<()> {
        let (writer, _reader) = create_dispatch_queue(8, 1);
        writer.enqueue(make_assignment()).await?;
        writer.enqueue(make_assignment()).await?;
        writer.enqueue(make_assignment()).await?;
        assert_eq!(writer.size(), 3);

        writer.bump_session_id(2).await?;
        assert_eq!(writer.size(), 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_bump_pair_consistency() -> Result<()> {
        const INIT_SESSION: SessionId = 10;
        const MID_SESSION: SessionId = 20;
        const FINAL_SESSION: SessionId = 30;

        let (writer, reader) = create_dispatch_queue(8, INIT_SESSION);

        let pre_bump_ids: Vec<TaskId> = (0..3).map(|_| next_task_id()).collect();
        let post_bump_ids: Vec<TaskId> = (0..2).map(|_| next_task_id()).collect();
        let final_id = next_task_id();

        let pre_bump_for_writer = pre_bump_ids.clone();
        let post_bump_for_writer = post_bump_ids.clone();
        let writer_handle = tokio::spawn(async move {
            for &id in &pre_bump_for_writer {
                writer
                    .enqueue(make_assignment_with_task_id(id))
                    .await
                    .expect("enqueue failed");
            }
            // Wait for the reader to consume the batch before bumping, so the items survive into
            // the delivered set instead of being drained.
            while writer.size() > 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            writer
                .bump_session_id(MID_SESSION)
                .await
                .expect("bump to mid session failed");

            for &id in &post_bump_for_writer {
                writer
                    .enqueue(make_assignment_with_task_id(id))
                    .await
                    .expect("enqueue failed");
            }
            while writer.size() > 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            writer
                .bump_session_id(FINAL_SESSION)
                .await
                .expect("bump to final session failed");

            writer
                .enqueue(make_assignment_with_task_id(final_id))
                .await
                .expect("enqueue failed");
            drop(writer);
        });

        let mut delivered: HashMap<TaskId, SessionId> = HashMap::new();
        loop {
            match reader.dequeue(Duration::from_millis(100)).await {
                Ok(Some((session, assignment))) => {
                    let prior = delivered.insert(assignment.task_id, session);
                    assert_eq!(
                        prior, None,
                        "duplicate delivery for {:?}",
                        assignment.task_id
                    );
                }
                Ok(None) => (),
                Err(_) => break,
            }
        }
        writer_handle.await?;

        for &id in &pre_bump_ids {
            assert_eq!(
                delivered.get(&id).copied(),
                Some(INIT_SESSION),
                "pre-bump item not paired with initial session: {id:?}",
            );
        }
        for &id in &post_bump_ids {
            assert_eq!(
                delivered.get(&id).copied(),
                Some(MID_SESSION),
                "post-bump item not paired with mid session: {id:?}",
            );
        }
        assert_eq!(delivered.get(&final_id).copied(), Some(FINAL_SESSION));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pair_consistency_stress_with_one_reader() -> Result<()> {
        run_pair_consistency_stress(1, 1_234_567).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pair_consistency_stress_with_many_readers() -> Result<()> {
        run_pair_consistency_stress(4, 7_654_321).await
    }
}
