//! Shared test utilities for the huntsman integration suites.
//!
//! Two concern areas:
//!
//! * [`executor`] — the `spider-task-executor` subprocess harness ([`ExecutorHandle`]) plus the TDL
//!   wire-payload helpers and environment readers the suites share.
//! * [`mock`] — in-process mock implementations of the execution manager's scheduler / storage /
//!   liveness client traits.
//!
//! Both modules' items are re-exported at the crate level, so tests can `use test_utils::*`-style
//! imports without naming the submodule.

mod executor;
mod mock;

pub use executor::*;
pub use mock::*;
