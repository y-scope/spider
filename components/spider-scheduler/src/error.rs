//! The error types used in this crate.

use spider_core::types::id::JobId;
use spider_core::types::id::SessionId;

use crate::execution_manager_registry::ExecutionManagerRegistryError;

/// Errors returned by [`crate::storage_client::SchedulerStorageClient`] operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageClientError {
    /// No job with the requested identifier exists.
    #[error("job not found: {0:?}")]
    JobNotFound(JobId),

    /// The storage server returned an invalid input error.
    #[error("invalid storage request: {0}")]
    InvalidInput(String),

    /// The storage server returned an otherwise-uncategorized error.
    #[error("storage server error: {0}")]
    Server(String),

    /// The storage transport failed or returned malformed data.
    #[error("storage transport error: {0}")]
    Transport(String),
}

/// Errors returned by the scheduler runtime and its components.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    /// Forwarded from the storage client.
    #[error(transparent)]
    Storage(#[from] StorageClientError),

    /// The dispatching queue is closed and can no longer accept assignments.
    #[error("dispatching queue is closed")]
    DispatchQueueClosed,

    /// The session ID is invalid.
    #[error("invalid session ID: {0:?}")]
    InvalidSessionId(SessionId),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("async result not ready")]
    ResultNotReady,

    #[error(transparent)]
    SystemTime(#[from] std::time::SystemTimeError),
}

/// Errors returned by the scheduler runtime.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerRuntimeError {
    /// Forwarded from the storage client during scheduler registration.
    #[error(transparent)]
    StorageClient(#[from] StorageClientError),

    /// A background task did not stop before the configured timeout.
    #[error("scheduler runtime stop timed out: {0}")]
    Stopping(String),
}

/// Errors returned by [`crate::service::SchedulerServiceState`] operations.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerServiceError {
    /// Forwarded from the dispatch queue or the scheduler core.
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),

    /// Forwarded from the execution manager registry.
    #[error(transparent)]
    EMRegistry(#[from] ExecutionManagerRegistryError),
}
