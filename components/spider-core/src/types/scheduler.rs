use std::net::IpAddr;

use crate::types::id::JobId;
use crate::types::id::ResourceGroupId;
use crate::types::id::SchedulerId;
use crate::types::id::SessionId;
use crate::types::id::TaskAssignmentId;
use crate::types::id::TaskId;

/// The currently registered scheduler endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegisteredScheduler {
    pub id: SchedulerId,
    pub ip_address: IpAddr,
    pub port: u16,
}

/// A task placement decision written by the scheduler core to the dispatching queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskAssignment {
    /// The unique ID of the task assignment.
    pub id: TaskAssignmentId,

    /// The resource group that owns the job.
    pub resource_group_id: ResourceGroupId,

    /// The job the task belongs to.
    pub job_id: JobId,

    /// The task to dispatch.
    pub task_id: TaskId,
}

/// A record of a task assignment previously produced by the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskAssignmentRecord {
    /// The unique ID of the task assignment.
    pub id: TaskAssignmentId,

    /// The scheduler where the record was issued from.
    pub from: SchedulerId,
}

impl TaskAssignmentRecord {
    #[must_use]
    pub const fn new(id: TaskAssignmentId, from: SchedulerId) -> Self {
        Self { id, from }
    }
}

/// A task assignment handed to the execution manager by the scheduler.
///
/// `session_id` is the scheduler's view of storage's session at the moment the assignment was
/// produced. The execution manager pins this exact value on every subsequent storage call for the
/// attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerResponse {
    /// The task placement decision produced by the scheduler.
    pub task_assignment: TaskAssignment,

    /// The scheduler that produced the assignment.
    pub scheduler_id: SchedulerId,

    /// The scheduler's view of storage's session when the assignment was produced.
    pub session_id: SessionId,
}
