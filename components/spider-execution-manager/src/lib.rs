//! Execution manager — the per-node service that drives Spider task execution against a
//! `spider-task-executor` subprocess.
//!
//! This first piece ships only the [`process_pool`] — a single-process supervisor that owns
//! one executor subprocess, serializes access to it with a semaphore, and respawns it on
//! hard timeout or crash. Higher-level execution-manager components (scheduler client,
//! storage client, main loop, heartbeat) land in subsequent PRs on top of this.

pub mod process_pool;
