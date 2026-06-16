//! Trait and type abstractions for the Spider scheduler.
//!
//! The scheduler is the serial decision maker that turns ready tasks discovered by the storage
//! layer into assignments for execution managers. It owns placement and ordering policy, not
//! dependency resolution: storage decides *what* is ready, and the scheduler decides *in what
//! order* and *with what throttling* ready tasks are offered to the fleet.
//!
//! The crate defines three trait seams wired into a single pipeline — a storage client that polls
//! the ready queue, a core that makes serial decisions, and a dispatching queue that fans those
//! decisions out to execution managers:
//!
//! ```text
//!   storage  ── authoritative ready queue (owned by the storage layer, not this crate)
//!         │
//!         │  poll_ready / poll_commit_ready / poll_cleanup_ready  (SchedulerStorageClient)
//!         ▼
//!   ┌───────────────────┐
//!   │   SchedulerCore   │  serial loop: poll → decide → enqueue
//!   └───────────────────┘
//!         │
//!         │  enqueue             (DispatchQueueSink — writer side)
//!         ▼
//!   ┌───────────────────┐
//!   │  dispatch queue   │  bounded SPMC; a full queue back-pressures the core
//!   └───────────────────┘
//!         │
//!         │  dequeue             (DispatchQueueSource — reader side)
//!         ▼
//!   ┌───────────────────┐
//!   │ scheduler service │ ──▶ execution managers (concurrent fan-out)
//!   └───────────────────┘
//! ```

pub mod core;
pub mod core_impl;
pub mod dispatch_queue;
pub mod error;
pub mod storage_client;
pub mod types;

pub use crate::{
    core::SchedulerCore,
    dispatch_queue::{DispatchQueueSink, DispatchQueueSource},
    error::{SchedulerError, StorageClientError},
    storage_client::{GrpcSchedulerStorageClient, SchedulerStorageClient},
    types::{InboundEntry, TaskAssignment},
};
