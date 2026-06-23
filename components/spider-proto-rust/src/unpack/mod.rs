//! Conversions from raw gRPC requests into their spider-native form.

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

/// Trait for unpacking gRPC requests into spider-native form.
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
