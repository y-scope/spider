//! The execution-manager-facing scheduler service.
//!
//! [`SchedulerServiceState`] is the domain layer behind the scheduler gRPC service. It serves
//! execution managers by draining task assignments from the dispatch queue and bookkeeping them in
//! the [`ExecutionManagerRegistry`]: assignment, completion, heartbeat, and shutdown. The service
//! is generic over its dispatch source, so the runtime can drive it with the real dispatch queue in
//! production or a mock in tests, while the [`ExecutionManagerRegistry`] is shared by value.

use std::sync::Arc;
use std::time::Duration;

use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::SchedulerId;
use spider_core::types::id::SessionId;
use spider_core::types::scheduler::TaskAssignmentRecord;

use crate::dispatch_queue::DispatchQueueSource;
use crate::error::SchedulerServiceError;
use crate::execution_manager_registry::ExecutionManagerRegistry;
use crate::types::TaskAssignment;

/// The execution-manager-facing scheduler service.
///
/// # Type Parameters
///
/// * `DispatchQueueSourceType` - The reader side of the dispatching queue the service drains.
#[derive(Clone)]
pub struct SchedulerServiceState<DispatchQueueSourceType: DispatchQueueSource + 'static> {
    inner: Arc<SchedulerServiceStateInner<DispatchQueueSourceType>>,
}

impl<DispatchQueueSourceType: DispatchQueueSource + 'static>
    SchedulerServiceState<DispatchQueueSourceType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly constructed [`SchedulerServiceState`].
    #[must_use]
    pub fn new(
        dispatch_source: DispatchQueueSourceType,
        registry: ExecutionManagerRegistry,
        scheduler_id: SchedulerId,
    ) -> Self {
        Self {
            inner: Arc::new(SchedulerServiceStateInner {
                dispatch_source,
                registry,
                scheduler_id,
            }),
        }
    }

    /// # Returns
    ///
    /// The scheduler identifier this service stamps onto every assignment it hands out.
    #[must_use]
    pub fn scheduler_id(&self) -> SchedulerId {
        self.inner.scheduler_id
    }

    /// Hands the next task assignment to an execution manager.
    ///
    /// If `prev_assignment` is supplied, the service acknowledges it as consumed by completing it
    /// in the registry on a best-effort, fire-and-forget basis: the completion is spawned as a
    /// background task, so it may land after this call returns, and any failure is logged rather
    /// than propagated. The service then drains the next assignment from the dispatch queue,
    /// waiting up to `wait_time` for one to arrive, and records the assignment against the
    /// execution manager in the registry before returning it.
    ///
    /// # Returns
    ///
    /// * A tuple on success, containing:
    ///   * The storage session the dispatch queue paired with the assignment.
    ///   * The task assignment handed to the execution manager.
    /// * `None` if no assignment becomes available within `wait_time`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`DispatchQueueSource::dequeue`]'s return values on failure.
    pub async fn next_task(
        &self,
        em_id: ExecutionManagerId,
        prev_assignment: Option<TaskAssignmentRecord>,
        wait_time: Duration,
    ) -> Result<Option<(SessionId, TaskAssignment)>, SchedulerServiceError> {
        if let Some(prev) = prev_assignment {
            // The previous assignment is handled in a fire-and-forget task. Errors are ignored but
            // logged for observability purposes.
            tokio::spawn(Self::complete_task_assignment(
                self.scheduler_id(),
                self.inner.registry.clone(),
                em_id,
                prev,
            ));
        }
        match self.inner.dispatch_source.dequeue(wait_time).await? {
            None => {
                tracing::info!(
                    scheduler_id = % self.scheduler_id(),
                    em_id = % em_id,
                    "No task assignment available within the specified wait time."
                );
                Ok(None)
            }
            Some((session_id, assignment)) => {
                tracing::info!(
                    scheduler_id = % self.scheduler_id(),
                    em_id = % em_id,
                    assignment_id = % assignment.id,
                    assignment_job_id = % assignment.job_id,
                    assignment_task_id = % assignment.task_id,
                    "Task dispatched to execution manager."
                );
                self.inner.registry.assign(em_id, assignment).await;
                Ok(Some((session_id, assignment)))
            }
        }
    }

    /// Refreshes the liveness of an execution manager.
    ///
    /// Registers the execution manager if this is its first heartbeat.
    ///
    /// # Errors
    ///
    /// This method does not currently return an error. The [`Result`] return type is retained for a
    /// uniform service surface alongside [`Self::next_task`] and [`Self::shutdown`].
    pub async fn heartbeat(&self, em_id: ExecutionManagerId) -> Result<(), SchedulerServiceError> {
        tracing::info!(
            scheduler_id = % self.scheduler_id(),
            em_id = % em_id,
            "Execution manager heartbeat received."
        );
        self.inner.registry.update_heartbeat(em_id).await;
        Ok(())
    }

    /// Signals that an execution manager is shutting down.
    ///
    /// Each assignment in `prev_assignments` is acknowledged as completed best-effort, then the
    /// execution manager is marked as dead in the registry.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ExecutionManagerRegistry::mark_as_dead`]'s return values on failure.
    pub async fn shutdown(
        &self,
        em_id: ExecutionManagerId,
        prev_assignments: Vec<TaskAssignmentRecord>,
    ) -> Result<(), SchedulerServiceError> {
        for prev in prev_assignments {
            Self::complete_task_assignment(
                self.scheduler_id(),
                self.inner.registry.clone(),
                em_id,
                prev,
            )
            .await;
        }
        self.inner.registry.mark_as_dead(em_id).await?;
        tracing::info!(
            scheduler_id = % self.scheduler_id(),
            em_id = % em_id,
            "Execution manager shutdown complete."
        );
        Ok(())
    }

    /// Marks a task assignment as completed in the registry.
    async fn complete_task_assignment(
        scheduler_id: SchedulerId,
        registry: ExecutionManagerRegistry,
        em_id: ExecutionManagerId,
        record: TaskAssignmentRecord,
    ) {
        if scheduler_id != record.from {
            tracing::warn!(
                scheduler_id = % scheduler_id,
                assignment_id = % record.id,
                from_scheduler_id = % record.from,
                from_em_id = % em_id,
                "Received a completed assignment from a stale scheduler. Skipping."
            );
            return;
        }
        let _ = registry
            .complete(em_id, record.id)
            .await
            .inspect_err(|error| {
                tracing::warn!(
                    scheduler_id = % scheduler_id,
                    assignment_id = % record.id,
                    from_scheduler_id = % record.from,
                    from_em_id = % em_id,
                    error = % error,
                    "Failed to complete a previously consumed assignment. Skipping."
                );
            });
    }
}

/// The shared inner state of [`SchedulerServiceState`].
///
/// # Type Parameters
///
/// * `DispatchQueueSourceType` - The reader side of the dispatching queue the service drains.
struct SchedulerServiceStateInner<DispatchQueueSourceType: DispatchQueueSource> {
    dispatch_source: DispatchQueueSourceType,
    registry: ExecutionManagerRegistry,
    scheduler_id: SchedulerId,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use async_trait::async_trait;
    use spider_core::types::id::ExecutionManagerId;
    use spider_core::types::id::JobId;
    use spider_core::types::id::ResourceGroupId;
    use spider_core::types::id::SchedulerId;
    use spider_core::types::id::SessionId;
    use spider_core::types::id::TaskAssignmentId;
    use spider_core::types::id::TaskId;
    use spider_core::types::scheduler::TaskAssignmentRecord;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::sync::mpsc::{self};
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::SchedulerServiceState;
    use crate::dispatch_queue::DispatchQueueSource;
    use crate::error::SchedulerError;
    use crate::execution_manager_registry::ExecutionManagerRegistry;
    use crate::execution_manager_registry::ExecutionManagerRegistryConfig;
    use crate::types::TaskAssignment;

    /// The storage session the mock dispatch source pairs with every assignment it returns.
    const SESSION_ID: SessionId = 7;

    /// The scheduler identifier the service stamps onto assignments.
    const SCHEDULER_ID: u64 = 42;

    /// The execution manager the tests drive the service with.
    const EM_ID: u64 = 1;

    /// The maximum time to wait for a rescheduled assignment before failing a test.
    const RESCHEDULE_TIMEOUT: Duration = Duration::from_secs(2);

    /// A [`DispatchQueueSource`] mock backed by a shared counter.
    ///
    /// Each [`DispatchQueueSource::dequeue`] claims one slot from the counter; while it is positive
    /// it returns a freshly minted task assignment, and once it reaches zero it returns [`None`]
    /// forever. Using a counter (rather than a canned list of assignments) lets the tests assert on
    /// dequeue behavior without coupling to recorded argument vectors.
    #[derive(Clone)]
    struct CounterDispatchSource {
        remaining: Arc<AtomicUsize>,
    }

    impl CounterDispatchSource {
        /// # Returns
        ///
        /// A new [`CounterDispatchSource`] that hands out `remaining` assignments before reporting
        /// an empty queue.
        fn new(remaining: usize) -> Self {
            Self {
                remaining: Arc::new(AtomicUsize::new(remaining)),
            }
        }
    }

    #[async_trait]
    impl DispatchQueueSource for CounterDispatchSource {
        async fn dequeue(
            &self,
            _wait_time: Duration,
        ) -> Result<Option<(SessionId, TaskAssignment)>, SchedulerError> {
            // Atomically claim one slot: return None once the counter is exhausted, otherwise
            // decrement and synthesize a fresh assignment.
            let mut current = self.remaining.load(Ordering::Relaxed);
            loop {
                if current == 0 {
                    return Ok(None);
                }
                match self.remaining.compare_exchange(
                    current,
                    current - 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return Ok(Some((SESSION_ID, make_assignment()))),
                    Err(actual) => current = actual,
                }
            }
        }
    }

    /// # Returns
    ///
    /// A fresh [`TaskAssignment`] with a unique, counter-derived identifier. The remaining
    /// identifier fields are fixed since the service tests do not assert on them.
    fn make_assignment() -> TaskAssignment {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = u64::try_from(COUNTER.fetch_add(1, Ordering::Relaxed))
            .expect("the counter is well below u64::MAX");
        TaskAssignment {
            id: TaskAssignmentId::from(id),
            resource_group_id: ResourceGroupId::from(0),
            job_id: JobId::from(0),
            task_id: TaskId::Index(0),
        }
    }

    /// # Returns
    ///
    /// A [`TaskAssignmentRecord`] acknowledging `id` as consumed, issued by this scheduler.
    fn record(id: TaskAssignmentId) -> TaskAssignmentRecord {
        TaskAssignmentRecord::new(id, SchedulerId::from(SCHEDULER_ID))
    }

    /// Builds a registry whose background liveness tracker never fires during a test (a one-hour
    /// cutoff with a one-minute interval), so registration, completion, and teardown can be driven
    /// explicitly rather than by timeouts.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The registry.
    /// * The receiver end of the re-schedule queue.
    /// * The registry-level cancellation token.
    fn build_registry() -> (
        ExecutionManagerRegistry,
        UnboundedReceiver<TaskAssignment>,
        CancellationToken,
    ) {
        let config = ExecutionManagerRegistryConfig {
            dead_em_cutoff_sec: NonZeroU64::new(3600).expect("the cutoff should be non-zero"),
            liveness_tracking_interval_ms: NonZeroU64::new(60_000)
                .expect("the interval should be non-zero"),
        };
        let cancellation_token = CancellationToken::new();
        let (reschedule_queue_sender, reschedule_queue_receiver) = mpsc::unbounded_channel();
        let registry = ExecutionManagerRegistry::new(
            &config,
            cancellation_token.clone(),
            reschedule_queue_sender,
        );
        (registry, reschedule_queue_receiver, cancellation_token)
    }

    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * A [`SchedulerServiceState`] over a [`CounterDispatchSource`] with `remaining` assignments,
    ///   backed by a fresh test registry.
    /// * The receiver end of the registry's re-schedule queue.
    /// * The registry-level cancellation token.
    fn build_service(
        remaining: usize,
    ) -> (
        SchedulerServiceState<CounterDispatchSource>,
        UnboundedReceiver<TaskAssignment>,
        CancellationToken,
    ) {
        let (registry, reschedule_queue_receiver, cancellation_token) = build_registry();
        let service = SchedulerServiceState::new(
            CounterDispatchSource::new(remaining),
            registry,
            SchedulerId::from(SCHEDULER_ID),
        );
        (service, reschedule_queue_receiver, cancellation_token)
    }

    #[tokio::test]
    async fn next_task_returns_none_when_queue_empty() -> anyhow::Result<()> {
        let (service, _reschedule_queue_receiver, _cancellation_token) = build_service(0);

        let result = service
            .next_task(
                ExecutionManagerId::from(EM_ID),
                None,
                Duration::from_millis(1),
            )
            .await?;
        assert_eq!(result, None);
        Ok(())
    }

    #[tokio::test]
    async fn next_task_assigns_and_records_assignment() -> anyhow::Result<()> {
        let (service, mut reschedule_queue_receiver, _cancellation_token) = build_service(1);
        let em_id = ExecutionManagerId::from(EM_ID);

        assert_eq!(service.scheduler_id(), SchedulerId::from(SCHEDULER_ID));

        let (session_id, assignment) = service
            .next_task(em_id, None, Duration::from_millis(1))
            .await?
            .expect("an assignment should be dequeued");
        assert_eq!(session_id, SESSION_ID);

        // The assignment was recorded against the execution manager, so shutting it down without
        // acknowledging the assignment reschedules it.
        service.shutdown(em_id, Vec::new()).await?;

        let rescheduled = timeout(RESCHEDULE_TIMEOUT, reschedule_queue_receiver.recv())
            .await
            .expect("the recorded assignment should be rescheduled before the timeout")
            .expect("the reschedule queue should remain open");
        assert_eq!(rescheduled.id, assignment.id);
        assert!(reschedule_queue_receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn heartbeat_registers_execution_manager() -> anyhow::Result<()> {
        let (service, mut reschedule_queue_receiver, _cancellation_token) = build_service(0);
        let em_id = ExecutionManagerId::from(EM_ID);

        service.heartbeat(em_id).await?;

        // `mark_as_dead` only succeeds for a registered execution manager, so a clean shutdown
        // confirms the heartbeat registered it.
        service.shutdown(em_id, Vec::new()).await?;
        assert!(reschedule_queue_receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_completes_prevs_and_reschedules_outstanding() -> anyhow::Result<()> {
        let (service, mut reschedule_queue_receiver, _cancellation_token) = build_service(3);
        let em_id = ExecutionManagerId::from(EM_ID);

        let (_, assignment_a) = service
            .next_task(em_id, None, Duration::from_millis(1))
            .await?
            .expect("the first assignment should be dequeued");
        let (_, assignment_b) = service
            .next_task(em_id, None, Duration::from_millis(1))
            .await?
            .expect("the second assignment should be dequeued");
        let (_, assignment_c) = service
            .next_task(em_id, None, Duration::from_millis(1))
            .await?
            .expect("the third assignment should be dequeued");

        // A and B are acknowledged as completed; C remains outstanding and is rescheduled.
        service
            .shutdown(
                em_id,
                vec![record(assignment_a.id), record(assignment_b.id)],
            )
            .await?;

        let rescheduled = timeout(RESCHEDULE_TIMEOUT, reschedule_queue_receiver.recv())
            .await
            .expect("the outstanding assignment should be rescheduled before the timeout")
            .expect("the reschedule queue should remain open");
        assert_eq!(rescheduled.id, assignment_c.id);
        assert!(reschedule_queue_receiver.try_recv().is_err());
        Ok(())
    }
}
