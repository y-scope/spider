//! The data types the scheduler exchanges with the storage layer and execution managers.

use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
pub use spider_core::types::scheduler::TaskAssignment;

/// A ready task drained from the storage-owned inbound queue.
///
/// The storage client flattens storage's three ready lanes (regular, commit, and cleanup tasks)
/// into this uniform entry, resolving each to its [`TaskId`] so the scheduler core can treat every
/// ready task identically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboundEntry {
    /// The resource group that owns the job.
    pub resource_group_id: ResourceGroupId,

    /// The job the task belongs to.
    pub job_id: JobId,

    /// The ready task.
    pub task_id: TaskId,
}
