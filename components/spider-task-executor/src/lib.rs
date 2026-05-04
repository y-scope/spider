//! Loads TDL packages compiled as cdylibs and dispatches task executions across the C-FFI
//! boundary.
//!
//! Two public types make up the surface:
//!
//! * [`TdlPackage`] wraps a single dlopen'd library and exposes its name, version, and a typed
//!   [`execute_task`](TdlPackage::execute_task) entry point.
//! * [`TdlPackageManager`] owns a collection of loaded packages keyed by package name and rejects
//!   duplicate loads.
//!
//! Both load APIs perform a `spider-tdl` ABI version handshake before installing the package and
//! return [`ExecutorError::IncompatibleVersion`] if the package was built against a non-compatible
//! `spider-tdl` release.

pub mod error;
pub mod manager;

pub use error::ExecutorError;
pub use manager::{TdlPackage, TdlPackageManager};
