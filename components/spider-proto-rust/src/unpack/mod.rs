//! Conversions from raw gRPC messages into their spider-native form.
//!
//! The generic [`RequestUnpack`] and [`ResponseUnpack`] traits plus the [`UnpackError`] error type
//! live here; service-specific implementations are split into sibling modules:
//!
//! * [`common`] — shared helpers for `common.proto` types (e.g. [`common::TaskId`]).
//! * [`storage`] — request unpacking for `storage.proto`.
//! * [`scheduler`] — response unpacking for `scheduler.proto`.

mod common;
mod scheduler;
mod storage;

use tonic::{Code, Status};

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

/// Trait for unpacking an inbound gRPC response into its spider-native form.
///
/// Implemented by the client side (e.g. `scheduler.proto` consumers). Unlike [`RequestUnpack`], the
/// error is a plain message string: a client does not return a [`Status`], it maps the failure
/// into its own error type.
pub trait ResponseUnpack {
    type Unpacked;

    /// Unpacks the gRPC response into the spider-native form.
    ///
    /// # Returns
    ///
    /// The unpacked response on success.
    ///
    /// # Errors
    ///
    /// Returns the failure message on failure.
    fn unpack(self) -> Result<Self::Unpacked, String>;
}
