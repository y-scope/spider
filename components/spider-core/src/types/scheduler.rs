use std::net::IpAddr;

use crate::types::id::{JobId, ResourceGroupId, SchedulerId, TaskAssignmentId, TaskId};

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
