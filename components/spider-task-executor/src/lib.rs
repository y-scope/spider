//! Spider task executor for executing tasks from TDL packages.

pub mod error;
pub mod manager;
pub mod protocol;

pub use error::ExecutorError;
pub use manager::TdlPackage;
pub use manager::TdlPackageManager;
