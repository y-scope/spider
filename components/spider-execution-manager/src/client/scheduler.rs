//! Scheduler client trait.
//!
//! The execution manager acquires tasks from the scheduler through [`SchedulerClient`].

use async_trait::async_trait;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::scheduler::SchedulerResponse;
use spider_core::types::scheduler::TaskAssignmentRecord;

/// Errors returned by [`SchedulerClient::next_task`].
#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    /// Connection to the scheduler was lost or the request timed out. Callers may back off and
    /// retry.
    #[error("transport error: {0}")]
    Transport(String),

    /// The scheduler returned an error response.
    #[error("scheduler server error: {0}")]
    Server(String),

    /// The scheduler returned a malformed reply.
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// Client interface to the scheduler service.
#[async_trait]
pub trait SchedulerClient: Send + Sync {
    /// Blocks until a task is assigned to this execution manager.
    ///
    /// Implementations may long-poll the scheduler; callers should treat this call as a
    /// cancellation point.
    ///
    /// # Parameters
    ///
    /// * `em_id` - The identity of the calling execution manager.
    /// * `prev_assignment` - The last task assignment produced by the scheduler that is
    ///   successfully consumed by the execution manager.
    /// * `wait_time_ms` - The maximum duration, in milliseconds, the scheduler may block this call
    ///   waiting for a task assignment before returning `NoTask`.
    ///
    /// # Returns
    ///
    /// A [`SchedulerResponse`] describing the assigned task on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::Transport`] if the connection was lost or the request timed out.
    /// * [`SchedulerError::Server`] if the scheduler returned an error response.
    /// * [`SchedulerError::Protocol`] if the scheduler returned a malformed reply.
    async fn next_task(
        &self,
        em_id: ExecutionManagerId,
        prev_assignment: Option<TaskAssignmentRecord>,
        wait_time_ms: u64,
    ) -> Result<SchedulerResponse, SchedulerError>;

    /// Sends a heartbeat to the scheduler to refresh the liveness of the current execution manager.
    ///
    /// # Parameters
    ///
    /// * `em_id` - The identity of the calling execution manager.
    ///
    /// # Errors
    ///
    /// * [`SchedulerError::Transport`] if the connection was lost or the request timed out.
    /// * [`SchedulerError::Server`] if the scheduler returned an error response.
    async fn heartbeat(&self, em_id: ExecutionManagerId) -> Result<(), SchedulerError>;

    /// Signals the scheduler that the current execution manager is shutting down.
    ///
    /// This method is intended to be called during runtime shutdown. It does not return an error,
    /// since shutdown handling should proceed on a best-effort basis. The implementation may return
    /// immediately in a fire-and-forget manner.
    ///
    /// # Parameters
    ///
    /// * `em_id` - The identity of the calling execution manager.
    /// * `prev_assignments` - The task assignments produced by the scheduler that are successfully
    ///   consumed by the execution manager.`
    async fn shutdown(
        &self,
        em_id: ExecutionManagerId,
        prev_assignments: Vec<TaskAssignmentRecord>,
    );
}
