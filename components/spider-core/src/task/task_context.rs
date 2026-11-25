//! Serializable task execution metadata.

use serde::Deserialize;
use serde::Serialize;

use crate::task::Error;
use crate::types::id::JobId;
use crate::types::id::ResourceGroupId;
use crate::types::id::TaskId;
use crate::types::id::TaskInstanceId;

/// Metadata for one task execution, including any serialized task-graph outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "UncheckedTaskContext")]
pub struct TaskContext {
    job_id: JobId,
    task_id: TaskId,
    task_instance_id: TaskInstanceId,
    resource_group_id: ResourceGroupId,
    serialized_task_graph_outputs: Option<Vec<u8>>,
}

impl TaskContext {
    /// Creates task metadata with serialized task-graph outputs for commit tasks.
    ///
    /// # Returns
    ///
    /// The task metadata on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidTaskContext`] if task-graph outputs are omitted for a commit task or
    ///   provided for any other task.
    pub fn new(
        job_id: JobId,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        resource_group_id: ResourceGroupId,
        serialized_task_graph_outputs: Option<Vec<u8>>,
    ) -> Result<Self, Error> {
        let is_commit_task = task_id == TaskId::Commit;
        if is_commit_task && serialized_task_graph_outputs.is_none() {
            return Err(Error::InvalidTaskContext(
                "task-graph outputs are required for a commit task but were not provided"
                    .to_owned(),
            ));
        }
        if !is_commit_task && serialized_task_graph_outputs.is_some() {
            return Err(Error::InvalidTaskContext(format!(
                "task-graph outputs are only valid for a commit task, but the current task is \
                 {task_id}"
            )));
        }
        Ok(Self {
            job_id,
            task_id,
            task_instance_id,
            resource_group_id,
            serialized_task_graph_outputs,
        })
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

    #[must_use]
    pub fn serialized_task_graph_outputs(&self) -> Option<&[u8]> {
        self.serialized_task_graph_outputs.as_deref()
    }
}

impl TryFrom<UncheckedTaskContext> for TaskContext {
    type Error = Error;

    fn try_from(unchecked: UncheckedTaskContext) -> Result<Self, Self::Error> {
        Self::new(
            unchecked.job_id,
            unchecked.task_id,
            unchecked.task_instance_id,
            unchecked.resource_group_id,
            unchecked.serialized_task_graph_outputs,
        )
    }
}

/// The fields of a [`TaskContext`] as decoded, before [`TaskContext::new`] checks them.
#[derive(Deserialize)]
struct UncheckedTaskContext {
    job_id: JobId,
    task_id: TaskId,
    task_instance_id: TaskInstanceId,
    resource_group_id: ResourceGroupId,
    serialized_task_graph_outputs: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use crate::task::Error;
    use crate::task::TaskContext;
    use crate::types::id::JobId;
    use crate::types::id::ResourceGroupId;
    use crate::types::id::TaskId;

    #[test]
    fn new_rejects_outputs_for_non_commit_task() {
        let result = TaskContext::new(
            JobId::random(),
            TaskId::Index(0),
            1,
            ResourceGroupId::random(),
            Some(Vec::new()),
        );
        assert!(matches!(result, Err(Error::InvalidTaskContext(_))));
    }

    #[test]
    fn new_rejects_missing_outputs_for_commit_task() {
        let result = TaskContext::new(
            JobId::random(),
            TaskId::Commit,
            1,
            ResourceGroupId::random(),
            None,
        );
        assert!(matches!(result, Err(Error::InvalidTaskContext(_))));
    }
}
