//! Conversions from raw gRPC messages into their spider-native form.
//!
//! The generic [`RequestUnpack`] trait plus the [`UnpackError`] error type live here;
//! service-specific implementations are split into sibling modules:
//!
//! * [`common`] — shared helpers for `common.proto` types (e.g. [`common::TaskId`]).
//! * [`storage`] — request unpacking for `storage.proto`.
//! * [`scheduler`] — request unpacking for `scheduler.proto`.

mod common;
mod scheduler;
mod storage;

use tonic::Code;
use tonic::Status;

/// A lightweight version of [`Status`] containing only the error code and message.
pub struct UnpackError {
    code: Code,
    message: String,
}

impl From<UnpackError> for Status {
    fn from(error: UnpackError) -> Self {
        Self::new(error.code, error.message)
    }
}

/// Trait for unpacking an inbound gRPC request into its spider-native form.
///
/// Implemented by the server side (e.g. `storage.proto` handlers); [`UnpackError`] converts into a
/// [`Status`] to return to the caller.
pub trait RequestUnpack {
    type Unpacked;

    /// Unpacks the gRPC request into the spider-native form.
    ///
    /// # Returns
    ///
    /// The unpacked request on success.
    ///
    /// # Errors
    ///
    /// Returns a [`UnpackError`] on failure.
    fn unpack(self) -> Result<Self::Unpacked, UnpackError>;
}
