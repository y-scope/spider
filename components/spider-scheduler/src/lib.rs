//! Scheduler skeleton for the Spider task-execution framework.
//!
//! This crate defines the core type and trait abstractions of the scheduler: the data types
//! exchanged with storage and execution managers ([`InboundEntry`], [`TaskAssignment`]), the
//! storage and dispatch seams ([`SchedulerStorageClient`], [`DispatchSink`]), and the pluggable
//! scheduling algorithm ([`SchedulerCore`]). Concrete implementations (the dispatch queue, the
//! runtime, and scheduling algorithms) build on top of these abstractions.

pub mod core;
pub mod dispatch;
pub mod error;
pub mod storage_client;
pub mod types;

pub use crate::{
    core::{SchedulerCore, ShutdownToken},
    dispatch::DispatchSink,
    error::{SchedulerError, StorageClientError},
    storage_client::SchedulerStorageClient,
    types::{InboundEntry, TaskAssignment},
};
