//! Scheduler client trait.
//!
//! The execution manager acquires tasks from the scheduler through [`SchedulerClient`].

use async_trait::async_trait;
use spider_core::types::id::{ExecutionManagerId, JobId, ResourceGroupId, SessionId, TaskId};

/// A task assignment handed to the execution manager by the scheduler.
///
/// `session_id` is the scheduler's view of storage's session at the moment the assignment was
/// produced. The execution manager pins this exact value on every subsequent storage call for the
/// attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerResponse {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub resource_group_id: ResourceGroupId,
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
    ) -> Result<SchedulerResponse, SchedulerError>;
}
