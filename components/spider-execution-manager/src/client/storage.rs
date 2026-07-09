//! Storage client trait.
//!
//! The execution manager interacts with the storage server through this trait to register a task
//! instance, fetch its [`ExecutionContext`], and report success or failure.

use async_trait::async_trait;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::ExecutionContext;

/// Errors returned by [`StorageClient`] operations.
///
/// The variants intentionally mirror the storage server's externally visible failure modes (see
/// `spider_storage::state::error::StorageServerError`) plus a transport bucket for connection /
/// serialization failures.
#[derive(Debug, thiserror::Error)]
pub enum StorageResponseError {
    /// The `session_id` carried with the request does not match storage's current session.
    #[error("stale session: {0}")]
    StaleSession(String),

    /// Storage's job cache rejected the operation as stale (e.g. the task or its job has already
    /// terminated).
    #[error("cache stale: {0}")]
    CacheStale(String),

    /// Connection lost, request timeout, or wire-format serialization failure. Callers may back off
    /// and retry.
    #[error("transport error: {0}")]
    Transport(String),

    /// The storage server returned an otherwise-uncategorized error.
    #[error("storage server: {0}")]
    Server(String),

    /// The input to the operation is invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

/// Client interface to the storage server.
#[async_trait]
pub trait StorageClient: Send + Sync {
    /// Registers a task instance and fetches its execution context.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The owning job.
    /// * `task_id` - The task being instantiated.
    /// * `em_id` - The identity of the calling execution manager.
    /// * `session_id` - The session id captured from the scheduler assignment, pinned for the
    ///   lifetime of the attempt.
    ///
    /// # Returns
    ///
    /// The [`ExecutionContext`] for the task instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageResponseError::StaleSession`] if `session_id` no longer matches storage's current
    ///   session.
    /// * [`StorageResponseError::CacheStale`] if storage's job cache rejected the registration.
    /// * [`StorageResponseError::Transport`] if the connection was lost or timed out.
    /// * [`StorageResponseError::Server`] if storage returned an otherwise-uncategorized error.
    async fn register_task_instance(
        &self,
        job_id: JobId,
        task_id: TaskId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
    ) -> Result<ExecutionContext, StorageResponseError>;

    /// Reports successful execution of a task instance.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The owning job.
    /// * `task_id` - The task that ran.
    /// * `task_instance_id` - The task instance that produced the outcome.
    /// * `em_id` - The identity of the calling execution manager.
    /// * `session_id` - The session id captured from the scheduler assignment.
    /// * `serialized_outputs` - The wire-format encoded task outputs buffer, forwarded verbatim to
    ///   storage. For commit tasks and cleanup tasks, this must be `None`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageResponseError::StaleSession`] if `session_id` no longer matches storage's current
    ///   session.
    /// * [`StorageResponseError::CacheStale`] if storage's job cache rejected the report.
    /// * [`StorageResponseError::Transport`] if the connection was lost or timed out.
    /// * [`StorageResponseError::Server`] if storage returned an otherwise-uncategorized error.
    /// * [`StorageResponseError::InvalidInput`] if `serialized_outputs` is `Some` for a commit or
    ///   cleanup task.
    async fn report_task_success(
        &self,
        job_id: JobId,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
        serialized_outputs: Option<Vec<u8>>,
    ) -> Result<(), StorageResponseError>;

    /// Reports failed execution of a task instance.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The owning job.
    /// * `task_id` - The task that ran.
    /// * `task_instance_id` - The task instance that produced the outcome.
    /// * `em_id` - The identity of the calling execution manager.
    /// * `session_id` - The session id captured from the scheduler assignment.
    /// * `error_message` - The formatted error message.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageResponseError::StaleSession`] if `session_id` no longer matches storage's current
    ///   session.
    /// * [`StorageResponseError::CacheStale`] if storage's job cache rejected the report.
    /// * [`StorageResponseError::Transport`] if the connection was lost or timed out.
    /// * [`StorageResponseError::Server`] if storage returned an otherwise-uncategorized error.
    async fn report_task_failure(
        &self,
        job_id: JobId,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
        error_message: String,
    ) -> Result<(), StorageResponseError>;
}
