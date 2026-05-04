//! Spider task executor for executing tasks from TDL packages.

pub mod error;
pub mod manager;

pub use error::ExecutorError;
pub use manager::{TdlPackage, TdlPackageManager};
