//! Task execution metadata.

use serde::Deserialize;
use serde::Serialize;

use crate::types::id::JobId;
use crate::types::id::ResourceGroupId;
use crate::types::id::TaskId;
use crate::types::id::TaskInstanceId;

/// Metadata for one task execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(
    clippy::struct_field_names,
    reason = "keep the established ID field names"
)]
pub struct TaskContext {
    job_id: JobId,
    task_id: TaskId,
    task_instance_id: TaskInstanceId,
    resource_group_id: ResourceGroupId,
}

impl TaskContext {
    /// Creates task execution metadata.
    ///
    /// # Returns
    ///
    /// The task metadata.
    #[must_use]
    pub const fn new(
        job_id: JobId,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        resource_group_id: ResourceGroupId,
    ) -> Self {
        Self {
            job_id,
            task_id,
            task_instance_id,
            resource_group_id,
        }
    }

    #[must_use]
    pub const fn job_id(&self) -> JobId {
        self.job_id
    }

    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    #[must_use]
    pub const fn task_instance_id(&self) -> TaskInstanceId {
        self.task_instance_id
    }

    #[must_use]
    pub const fn resource_group_id(&self) -> ResourceGroupId {
        self.resource_group_id
    }
}
