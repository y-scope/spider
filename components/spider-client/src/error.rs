//! Client error type for the Spider storage gRPC services.

/// Errors returned by [`crate::client::SpiderClient`] operations.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The gRPC transport failed or the connection was lost or unestablished.
    #[error("transport error: {0}")]
    Transport(String),

    /// The storage server returned an otherwise-uncategorized error.
    #[error("storage server error: {0}")]
    Server(String),

    /// No job with the requested identifier exists.
    #[error("job not found")]
    JobNotFound,

    /// The job is not in a state that allows the requested operation.
    #[error("invalid job state: {0}")]
    InvalidJobState(String),

    /// The storage server rejected the request as invalid.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The resource group or password was rejected.
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),

    /// A failure to serialize, compress, or wire-frame a request payload.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// A failure to deserialize, decompress, or wire-frame a response payload.
    #[error("deserialization error: {0}")]
    Deserialization(String),

    /// The server returned an unspecified job state that has no core representation.
    #[error("job state unspecified")]
    UnspecifiedJobState,
}

/// Converts a displayable transport-layer error into [`ClientError::Transport`].
///
/// # Returns
///
/// A [`ClientError::Transport`] containing `error`'s display string.
pub(crate) fn to_transport_error(error: impl std::fmt::Display) -> ClientError {
    ClientError::Transport(error.to_string())
}
