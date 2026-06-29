//! The execution-manager-facing scheduler service.
//!
//! [`SchedulerServiceState`] is the domain layer behind the scheduler gRPC service. It serves
//! execution managers by draining task assignments from the dispatch queue and bookkeeping them in
//! the [`ExecutionManagerRegistry`]: assignment, completion, heartbeat, and shutdown. The service
//! is generic over its dispatch source so the runtime can drive it with the real dispatch queue in
//! production or a mock in tests, while the [`ExecutionManagerRegistry`] is shared by value.

use std::time::Duration;

use spider_core::types::{
    id::{ExecutionManagerId, SchedulerId, SessionId},
    scheduler::TaskAssignmentRecord,
};

use crate::{
    dispatch_queue::DispatchQueueSource,
    error::SchedulerServiceError,
    execution_manager_registry::ExecutionManagerRegistry,
    types::TaskAssignment,
};

/// The execution-manager-facing scheduler service.
///
/// Sits between the dispatch queue and the execution manager registry, turning execution-manager
/// requests into registry bookkeeping. The service stamps every assignment it hands out with the
/// scheduler's own identifier, captured at registration time.
///
/// # Type Parameters
///
/// * `DispatchQueueSourceType` - The reader side of the dispatching queue the service drains.
#[derive(Clone)]
pub struct SchedulerServiceState<DispatchQueueSourceType: DispatchQueueSource> {
    dispatch_source: DispatchQueueSourceType,
    registry: ExecutionManagerRegistry,
    scheduler_id: SchedulerId,
}

impl<DispatchQueueSourceType: DispatchQueueSource> SchedulerServiceState<DispatchQueueSourceType> {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A new [`SchedulerServiceState`] that drains `dispatch_source`, tracks assignments in
    /// `registry`, and stamps `scheduler_id` onto every assignment it hands out.
    #[must_use]
    pub const fn new(
        dispatch_source: DispatchQueueSourceType,
        registry: ExecutionManagerRegistry,
        scheduler_id: SchedulerId,
    ) -> Self {
        Self {
            dispatch_source,
            registry,
            scheduler_id,
        }
    }

    /// # Returns
    ///
    /// The scheduler identifier this service stamps onto every assignment it hands out.
    #[must_use]
    pub const fn scheduler_id(&self) -> SchedulerId {
        self.scheduler_id
    }

    /// Hands the next task assignment to an execution manager.
    ///
    /// If `prev_assignment` is supplied, the execution manager first acknowledges it as consumed by
    /// completing it in the registry. The service then drains the next assignment from the dispatch
    /// queue, waiting up to `wait_time` for one to arrive, and records the assignment against the
    /// execution manager in the registry before returning it.
    ///
    /// # Parameters
    ///
    /// * `em_id` - The identity of the calling execution manager.
    /// * `prev_assignment` - The last assignment produced by this scheduler that the execution
    ///   manager has successfully consumed, or [`None`] if no previous assignment exists.
    /// * `wait_time` - The maximum duration to wait for an assignment to become available.
    ///
    /// # Returns
    ///
    /// A tuple on success, containing:
    ///
    /// * The storage session the dispatch queue paired with the assignment.
    /// * The task assignment handed to the execution manager.
    ///
    /// [`None`] is returned when no assignment becomes available within `wait_time`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerServiceError::EMRegistry`] if completing `prev_assignment` fails because the
    ///   execution manager or the assignment is not registered.
    /// * Forwards [`DispatchQueueSource::dequeue`]'s return values on failure.
    pub async fn next_task(
        &self,
        em_id: ExecutionManagerId,
        prev_assignment: Option<TaskAssignmentRecord>,
        wait_time: Duration,
    ) -> Result<Option<(SessionId, TaskAssignment)>, SchedulerServiceError> {
        if let Some(prev) = prev_assignment {
            self.registry.complete(em_id, prev.id).await?;
        }
        match self.dispatch_source.dequeue(wait_time).await? {
            None => Ok(None),
            Some((session_id, assignment)) => {
                self.registry.assign(em_id, assignment).await;
                Ok(Some((session_id, assignment)))
            }
        }
    }

    /// Refreshes the liveness of an execution manager.
    ///
    /// Registers the execution manager if this is its first heartbeat.
    ///
    /// # Parameters
    ///
    /// * `em_id` - The identity of the calling execution manager.
    ///
    /// # Errors
    ///
    /// This method does not currently return an error. The [`Result`] return type is retained for a
    /// uniform service surface alongside [`Self::next_task`] and [`Self::shutdown`].
    pub async fn heartbeat(&self, em_id: ExecutionManagerId) -> Result<(), SchedulerServiceError> {
        self.registry.update_heartbeat(em_id).await;
        Ok(())
    }

    /// Signals that an execution manager is shutting down.
    ///
    /// Each assignment in `prev_assignments` is acknowledged as completed best-effort: a missing
    /// assignment or execution manager is logged and skipped so teardown still proceeds. The
    /// execution manager is then marked dead, which reschedules any assignments still outstanding
    /// against it.
    ///
    /// # Parameters
    ///
    /// * `em_id` - The identity of the calling execution manager.
    /// * `prev_assignments` - The assignments produced by this scheduler that the execution manager
    ///   has successfully consumed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerServiceError::EMRegistry`] if marking the execution manager as dead fails.
    pub async fn shutdown(
        &self,
        em_id: ExecutionManagerId,
        prev_assignments: Vec<TaskAssignmentRecord>,
    ) -> Result<(), SchedulerServiceError> {
        for prev in prev_assignments {
            if let Err(error) = self.registry.complete(em_id, prev.id).await {
                tracing::warn!(
                    em_id = %em_id,
                    error = %error,
                    assignment_id = ?prev.id,
                    "Failed to complete a previously consumed assignment during shutdown. Skipping."
                );
            }
        }
        self.registry.mark_as_dead(em_id).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use spider_core::types::{
        id::{
            ExecutionManagerId,
            JobId,
            ResourceGroupId,
            SchedulerId,
            SessionId,
            TaskAssignmentId,
            TaskId,
        },
        scheduler::TaskAssignmentRecord,
    };
    use tokio::{
        sync::mpsc::{self, UnboundedReceiver},
        time::timeout,
    };
    use tokio_util::sync::CancellationToken;

    use super::SchedulerServiceState;
    use crate::{
        dispatch_queue::DispatchQueueSource,
        error::SchedulerError,
        execution_manager_registry::{ExecutionManagerRegistry, ExecutionManagerRegistryConfig},
        types::TaskAssignment,
    };

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
            dead_em_cutoff_sec: 3600,
            liveness_tracking_interval_ms: 60_000,
        };
        let cancellation_token = CancellationToken::new();
        let (reschedule_queue_sender, reschedule_queue_receiver) = mpsc::unbounded_channel();
        let registry = ExecutionManagerRegistry::new(
            &config,
            cancellation_token.clone(),
            reschedule_queue_sender,
        )
        .expect("the registry should be constructed successfully");
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
    async fn next_task_completes_prev_assignment() -> anyhow::Result<()> {
        let (service, mut reschedule_queue_receiver, _cancellation_token) = build_service(2);
        let em_id = ExecutionManagerId::from(EM_ID);

        let (_, assignment_a) = service
            .next_task(em_id, None, Duration::from_millis(1))
            .await?
            .expect("the first assignment should be dequeued");
        let (_, assignment_b) = service
            .next_task(
                em_id,
                Some(record(assignment_a.id)),
                Duration::from_millis(1),
            )
            .await?
            .expect("the second assignment should be dequeued");

        // The previous assignment was completed during the second call, so only the still
        // outstanding assignment B is rescheduled on shutdown.
        service.shutdown(em_id, Vec::new()).await?;

        let rescheduled = timeout(RESCHEDULE_TIMEOUT, reschedule_queue_receiver.recv())
            .await
            .expect("the outstanding assignment should be rescheduled before the timeout")
            .expect("the reschedule queue should remain open");
        assert_eq!(rescheduled.id, assignment_b.id);
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
