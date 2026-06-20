//! Scheduler client trait.
//!
//! The execution manager acquires tasks from the scheduler through [`SchedulerClient`].

use async_trait::async_trait;
use spider_core::types::{
    id::{ExecutionManagerId, SchedulerId, SessionId},
    scheduler::{TaskAssignment, TaskAssignmentRecord},
};

/// A task assignment handed to the execution manager by the scheduler.
///
/// `session_id` is the scheduler's view of storage's session at the moment the assignment was
/// produced. The execution manager pins this exact value on every subsequent storage call for the
/// attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerResponse {
    /// The task placement decision produced by the scheduler.
    pub task_assignment: TaskAssignment,

    /// The scheduler that produced the assignment.
    pub scheduler_id: SchedulerId,

    /// The scheduler's view of storage's session when the assignment was produced.
    pub session_id: SessionId,
}

/// Errors returned by [`SchedulerClient::next_task`].
#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    /// Connection to the scheduler was lost or the request timed out. Callers may back off and
    /// retry.
    #[error("transport error: {0}")]
    Transport(String),

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
    /// * [`SchedulerError::Protocol`] if the scheduler returned a malformed reply.
    async fn next_task(
        &self,
        em_id: ExecutionManagerId,
        prev_assignment: Option<TaskAssignmentRecord>,
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
