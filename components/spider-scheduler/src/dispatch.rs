use async_trait::async_trait;

use crate::{error::SchedulerError, types::TaskAssignment};

/// The write side of the dispatching queue used by the scheduler core.
///
/// Modeled as a trait so the scheduler core can be unit-tested against a recording sink without
/// standing up the execution-manager-facing service. The production implementation is backed by a
/// bounded single-producer/multi-consumer queue.
#[async_trait]
pub trait DispatchSink: Send + Sync {
    /// Enqueues a task assignment for execution managers to consume.
    ///
    /// Implementations backed by a bounded queue await while the queue is full, applying
    /// back-pressure to the scheduler core.
    ///
    /// # Parameters
    ///
    /// * `assignment` - The task assignment to enqueue.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::DispatchClosed`] if the dispatching queue is closed and can no longer
    ///   accept assignments.
    async fn dispatch(&self, assignment: TaskAssignment) -> Result<(), SchedulerError>;
}
