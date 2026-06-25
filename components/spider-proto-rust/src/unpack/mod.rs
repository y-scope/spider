//! Conversions from raw gRPC messages into their spider-native form.
//!
//! The generic [`Unpack`] trait and [`UnpackError`] error type live here; service-specific
//! implementations are split into sibling modules:
//!
//! * [`common`] — shared helpers for `common.proto` types (e.g. [`common::TaskId`]).
//! * [`storage`] — request unpacking for `storage.proto`.
//! * [`scheduler`] — response unpacking for `scheduler.proto`.

mod common;
mod scheduler;
mod storage;

use std::fmt::{Debug, Display};

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

impl Display for UnpackError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.message, formatter)
    }
}

impl Debug for UnpackError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UnpackError")
            .field("code", &self.code)
            .field("message", &self.message)
            .finish()
    }
}

impl std::error::Error for UnpackError {}

/// Trait for unpacking a gRPC message into its spider-native form.
///
/// Implemented for both inbound requests (server side, e.g. `storage.proto`) and inbound
/// responses (client side, e.g. `scheduler.proto`). On the server side [`UnpackError`] converts
/// into a [`Status`] to return to the caller; on the client side the caller maps it into its own
/// error type.
pub trait Unpack {
    type Unpacked;

    /// Unpacks the gRPC message into the spider-native form.
    ///
    /// # Returns
    ///
    /// The unpacked message on success.
    ///
    /// # Errors
    ///
    /// Returns a [`UnpackError`] on failure.
    fn unpack(self) -> Result<Self::Unpacked, UnpackError>;
}
