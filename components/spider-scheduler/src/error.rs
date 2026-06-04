//! The error types used in this crate.

use spider_core::types::id::{JobId, SessionId};

/// Errors returned by [`crate::storage_client::SchedulerStorageClient`] operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageClientError {
    /// The inbound queue is closed and can no longer yield ready entries.
    #[error("inbound queue is closed")]
    InboundClosed,

    /// No job with the requested identifier exists.
    #[error("job not found: {0:?}")]
    JobNotFound(JobId),
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

    #[error("invalid config: {0}")]
    InvalidConfig(String),

    #[error("async result not ready")]
    ResultNotReady,
}
