//! Runtime metadata passed to every task function.
//!
//! [`TaskContext`] is constructed and msgpack-serialized by the execution manager, passed opaquely
//! through the task executor, and deserialized inside the TDL package before being handed to the
//! user's task function as the first parameter.

use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::SerializedTaskOutputs;
use spider_core::types::io::TaskOutput;

use crate::error::TdlError;

/// Runtime metadata about the current task execution.
///
/// Every task function receives a [`TaskContext`] as its first parameter. It carries identifiers
/// that link the execution back to the job and task definitions in the storage layer.
///
/// Serialized via plain msgpack ([`rmp_serde::to_vec`] or [`rmp_serde::from_slice`]), separately
/// from the task inputs wire stream.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TaskContext {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub task_instance_id: TaskInstanceId,
    pub resource_group_id: ResourceGroupId,
    serialized_task_graph_outputs: Option<Vec<u8>>,
}

impl TaskContext {
    /// Creates a new [`TaskContext`].
    ///
    /// # Returns
    ///
    /// The constructed [`TaskContext`].
    #[must_use]
    pub const fn new(
        job_id: JobId,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        resource_group_id: ResourceGroupId,
        serialized_task_graph_outputs: Option<Vec<u8>>,
    ) -> Self {
        Self {
            job_id,
            task_id,
            task_instance_id,
            resource_group_id,
            serialized_task_graph_outputs,
        }
    }

    /// Deserializes the job's task-graph outputs carried by this context.
    ///
    /// # Returns
    ///
    /// The job's task-graph outputs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TdlError::Internal`] if:
    ///   * This context does not belong to a commit task, or,
    ///   * The task-graph outputs are not set.
    /// * Forwards [`SerializedTaskOutputs::deserialize_from_raw`]'s return values on failure.
    pub fn get_task_graph_outputs(&self) -> Result<Vec<TaskOutput>, TdlError> {
        if self.task_id != TaskId::Commit {
            return Err(TdlError::Internal(format!(
                "task-graph outputs are only available for commit tasks, but the current task is \
                 {}",
                self.task_id
            )));
        }
        let bytes = self
            .serialized_task_graph_outputs
            .as_ref()
            .ok_or_else(|| TdlError::Internal("task-graph outputs are not set".to_owned()))?;
        SerializedTaskOutputs::deserialize_from_raw(bytes)
            .map_err(|e| TdlError::DeserializationError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use spider_core::types::id::JobId;
    use spider_core::types::id::ResourceGroupId;
    use spider_core::types::id::TaskId;

    use super::TaskContext;

    #[test]
    fn round_trip_msgpack() -> anyhow::Result<()> {
        let ctx = TaskContext::new(
            JobId::random(),
            TaskId::Index(0),
            13,
            ResourceGroupId::random(),
            None,
        );
        let encoded = rmp_serde::to_vec(&ctx)?;
        let decoded: TaskContext = rmp_serde::from_slice(&encoded)?;
        assert_eq!(decoded, ctx);
        Ok(())
    }
}
