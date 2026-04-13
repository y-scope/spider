//! Error types for the task executor.

use spider_tdl::TdlError;

/// Errors produced by the task executor when loading or invoking a TDL package.
#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    /// The shared library could not be loaded via `dlopen`.
    #[error("failed to load TDL package library: {0}")]
    LibraryLoad(#[from] libloading::Error),

    /// A package with the same name is already loaded.
    #[error("duplicate package name: {0}")]
    DuplicatePackage(String),

    /// The task returned an error, deserialized from the msgpack error payload.
    #[error("task execution failed: {0}")]
    TaskError(#[from] TdlError),

    /// The error payload returned by the task could not be deserialized.
    #[error("failed to deserialize error payload: {0}")]
    ErrorPayloadDeserialization(#[from] rmp_serde::decode::Error),
}
