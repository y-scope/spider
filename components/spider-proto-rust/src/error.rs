//! Error types for converting protobuf wire values into Spider core types.

/// Errors produced when converting a protobuf message into its Spider core representation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A protobuf task index could not be represented as a [`usize`].
    #[error("task index does not fit in `usize`: {0}")]
    TaskIndexOutOfRange(u64),

    /// A protobuf [`crate::common::TaskId`] carried no `kind`.
    #[error("task id missing kind")]
    TaskIdKindMissing,

    /// A protobuf [`crate::storage::JobState`] was left unspecified.
    #[error("job state is unspecified")]
    JobStateUnspecified,
}
