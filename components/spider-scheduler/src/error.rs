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

    /// The scheduler's storage session is stale.
    #[error("stale storage session: {storage_session:?}")]
    StaleSession {
        /// Storage's current session ID.
        storage_session: SessionId,
    },

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
}
