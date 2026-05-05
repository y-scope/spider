//! Errors produced while loading TDL packages or executing tasks across the FFI boundary.

use spider_tdl::{TdlError, Version};

/// All possible errors produced by the task executor.
///
/// [`TdlError`] (failure inside a user task) is wrapped via [`Self::TaskError`] so callers can
/// distinguish executor-internal failures from in-task failures.
#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    /// `dlopen` failed or a required FFI symbol was missing.
    #[error("failed to load TDL package library: {0}")]
    InvalidLibrary(#[from] libloading::Error),

    /// The package's declared `spider-tdl` ABI version is not compatible with the executor's.
    #[error(
        "incompatible spider-tdl version: \
         package={package_major}.{package_minor}.{package_patch}, \
         executor={executor_major}.{executor_minor}.{executor_patch}"
    )]
    IncompatibleVersion {
        package_major: u32,
        package_minor: u32,
        package_patch: u32,
        executor_major: u32,
        executor_minor: u32,
        executor_patch: u32,
    },

    /// Two packages with the same `package_name` were registered with the same manager.
    #[error("duplicate package name: {0}")]
    DuplicatePackage(String),

    /// The byte buffer contains invalid UTF-8 patterns.
    #[error("invalid UTF-8: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),

    /// A user task returned a [`TdlError`] across the FFI boundary.
    #[error("task execution failed: {0}")]
    TaskError(#[from] TdlError),

    /// The msgpack-encoded error payload returned by a failing task could not be decoded back into
    /// a [`TdlError`].
    #[error("failed to deserialize error payload: {0}")]
    ErrorPayloadDeserializationFailure(#[from] rmp_serde::decode::Error),
}

impl ExecutorError {
    /// Constructs an [`ExecutorError::IncompatibleVersion`] from the package and executor
    /// [`Version`] values.
    ///
    /// # Returns
    ///
    /// The constructed error variant.
    #[must_use]
    pub const fn incompatible_version(package: Version, executor: Version) -> Self {
        Self::IncompatibleVersion {
            package_major: package.major,
            package_minor: package.minor,
            package_patch: package.patch,
            executor_major: executor.major,
            executor_minor: executor.minor,
            executor_patch: executor.patch,
        }
    }
}
