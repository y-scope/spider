use spider_core::types::id::{JobId, ResourceGroupId, TaskId};

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

/// A task placement decision written by the scheduler core to the dispatching queue.
///
/// Assignments are intentionally lightweight: they identify the task but carry no inputs. The
/// consuming execution manager registers the task instance against storage on pull to obtain the
/// execution context (inputs, timeouts, and the TDL context).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskAssignment {
    /// The resource group that owns the job.
    pub resource_group_id: ResourceGroupId,

    /// The job the task belongs to.
    pub job_id: JobId,

    /// The task to dispatch.
    pub task_id: TaskId,
}
