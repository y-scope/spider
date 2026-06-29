//! Client error type for the Spider storage gRPC services.
//!
//! [`ClientError`] is the single error type returned by [`crate::client::SpiderClient`] methods.
//! It folds transport failures, tonic error status, and payload serialization and
//! deserialization failures into one concrete enum. See [`ClientError`] for the variants and when
//! each arises.

use spider_core::types::id::JobId;
use tonic::{Code, Status};

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
    #[error("job not found: {0:?}")]
    JobNotFound(JobId),

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
    #[error("job state is unspecified")]
    UnspecifiedJobState,
}

/// Maps a job-orchestration gRPC [`Status`] to a [`ClientError`].
///
/// `job_id` is the job the call targeted; it is attached to [`ClientError::JobNotFound`] when the
/// server reports `NOT_FOUND`.
///
/// # Returns
///
/// The [`ClientError`] for `status`'s code:
///
/// * [`ClientError::JobNotFound`] for `NOT_FOUND`.
/// * [`ClientError::InvalidJobState`] for `FAILED_PRECONDITION`.
/// * [`ClientError::InvalidArgument`] for `INVALID_ARGUMENT`.
/// * [`ClientError::Unauthenticated`] for `UNAUTHENTICATED`.
/// * [`ClientError::Transport`] for `UNAVAILABLE` (a lost or unestablished connection).
/// * [`ClientError::Server`] for any other code.
pub(crate) fn job_status_to_error(status: &Status, job_id: JobId) -> ClientError {
    match status.code() {
        Code::NotFound => ClientError::JobNotFound(job_id),
        Code::FailedPrecondition => ClientError::InvalidJobState(status.message().to_owned()),
        Code::InvalidArgument => ClientError::InvalidArgument(status.message().to_owned()),
        Code::Unauthenticated => ClientError::Unauthenticated(status.message().to_owned()),
        Code::Unavailable => ClientError::Transport(status.message().to_owned()),
        _ => ClientError::Server(status.message().to_owned()),
    }
}

/// Maps a resource-group-management gRPC [`Status`] to a [`ClientError`].
///
/// # Returns
///
/// The [`ClientError`] for `status`'s code:
///
/// * [`ClientError::InvalidArgument`] for `INVALID_ARGUMENT`.
/// * [`ClientError::Unauthenticated`] for `UNAUTHENTICATED` (an unknown or unauthorized resource
///   group, or an invalid password).
/// * [`ClientError::Transport`] for `UNAVAILABLE` (a lost or unestablished connection).
/// * [`ClientError::Server`] for any other code (including `NOT_FOUND`, `FAILED_PRECONDITION`, and
///   `INTERNAL`).
pub(crate) fn resource_group_status_to_error(status: &Status) -> ClientError {
    match status.code() {
        Code::InvalidArgument => ClientError::InvalidArgument(status.message().to_owned()),
        Code::Unauthenticated => ClientError::Unauthenticated(status.message().to_owned()),
        Code::Unavailable => ClientError::Transport(status.message().to_owned()),
        _ => ClientError::Server(status.message().to_owned()),
    }
}

/// Converts a displayable transport-layer error into [`ClientError::Transport`].
///
/// Used by the `connect` methods of [`crate::client::SpiderClient`],
/// [`crate::job::JobOrchestrationClient`],
/// and [`crate::resource_group::ResourceGroupManagementClient`] to fold `spider_utils::grpc::Error`
/// into [`ClientError`].
///
/// # Returns
///
/// A [`ClientError::Transport`] containing `error`'s display string.
pub(crate) fn to_transport_error(error: impl std::fmt::Display) -> ClientError {
    ClientError::Transport(error.to_string())
}
