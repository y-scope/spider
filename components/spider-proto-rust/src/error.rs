//! Error types for converting protobuf wire values into Spider core types.

/// Errors produced when converting a protobuf message into its Spider core representation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A protobuf task index could not be represented as a [`usize`].
    #[error("task index does not fit in `usize`: {0}")]
    TaskIndexOutOfRange(u64),

    /// A protobuf [`crate::storage::TaskId`] carried no `kind`.
    #[error("task id missing kind")]
    TaskIdKindMissing,

    /// A protobuf [`crate::storage::JobState`] was left unspecified.
    #[error("job state is unspecified")]
    JobStateUnspecified,

    /// A protobuf [`crate::storage::BinaryPayload`] was left unspecified.
    #[error("binary payload encoding is unspecified")]
    BinaryPayloadEncodingUnspecified,

    /// A protobuf [`crate::storage::BinaryPayload`] carried an unknown encoding.
    #[error("binary payload encoding is unknown: {0}")]
    BinaryPayloadEncodingUnknown(i32),

    /// A protobuf [`crate::storage::BinaryPayload`] could not be decompressed.
    #[error("failed to decompress binary payload: {0}")]
    BinaryPayloadDecompression(String),
}
