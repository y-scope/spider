//! The dispatching queue that decouples the scheduler core's placement decisions from the
//! execution-manager-facing service.

use async_trait::async_trait;
use spider_core::types::id::SessionId;

use crate::{error::SchedulerError, types::TaskAssignment};

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
}

/// The reader side of the dispatching queue, drained by the execution-manager-facing service.
#[async_trait]
pub trait DispatchQueueSource: Send + Sync + Clone {
    /// Dequeues the next task assignment for an execution manager to execute.
    ///
    /// # Returns
    ///
    /// A tuple on success, containing:
    ///
    /// * The storage session associated with the assignment.
    /// * The next task assignment ready to execute.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::DispatchQueueClosed`] if the dispatching queue is closed.
    async fn dequeue(&self) -> Result<(SessionId, TaskAssignment), SchedulerError>;
}
