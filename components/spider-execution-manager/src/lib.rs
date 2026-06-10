//! Execution manager — the per-node service that drives Spider task execution against a
//! `spider-task-executor` subprocess.

pub mod client;
pub mod liveness;
pub mod process_pool;
pub mod runtime;
