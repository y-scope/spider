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
    /// Creates a new [`TaskContext`], validating that the job's task-graph outputs are present for
    /// a commit task and omitted for any other task.
    ///
    /// The raw serialized outputs are stored as-is and only deserialized on demand by
    /// [`Self::get_task_graph_outputs`].
    ///
    /// # Returns
    ///
    /// The constructed [`TaskContext`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TdlError::InvalidTaskContext`] if:
    ///   * The task is a commit task but no task-graph outputs are provided, or,
    ///   * The task is not a commit task but task-graph outputs are provided.
    pub fn new(
        job_id: JobId,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        resource_group_id: ResourceGroupId,
        serialized_task_graph_outputs: Option<Vec<u8>>,
    ) -> Result<Self, TdlError> {
        let is_commit_task = task_id == TaskId::Commit;
        if is_commit_task && serialized_task_graph_outputs.is_none() {
            return Err(TdlError::InvalidTaskContext(
                "task-graph outputs are required for a commit task but were not provided"
                    .to_owned(),
            ));
        }
        if !is_commit_task && serialized_task_graph_outputs.is_some() {
            return Err(TdlError::InvalidTaskContext(format!(
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

    /// Deserializes the job's task-graph outputs carried by this context.
    ///
    /// # Returns
    ///
    /// The job's task-graph outputs (if any) or `None` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`SerializedTaskOutputs::deserialize_from_raw`]'s return values on failure.
    pub fn get_task_graph_outputs(&self) -> Result<Option<Vec<TaskOutput>>, TdlError> {
        match &self.serialized_task_graph_outputs {
            Some(bytes) => {
                let task_graph_outputs = SerializedTaskOutputs::deserialize_from_raw(bytes)
                    .map_err(|e| TdlError::DeserializationError(e.to_string()))?;
                Ok(Some(task_graph_outputs))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use spider_core::types::id::JobId;
    use spider_core::types::id::ResourceGroupId;
    use spider_core::types::id::TaskId;
    use spider_core::types::io::SerializedTaskOutputs;
    use spider_core::types::io::TaskOutput;

    use super::TaskContext;
    use crate::error::TdlError;

    /// Serializes the given task outputs into their raw wire buffer for constructing a
    /// [`TaskContext`].
    ///
    /// # Returns
    ///
    /// The raw serialized task-outputs buffer.
    ///
    /// # Panics
    ///
    /// Panics if [`SerializedTaskOutputs::serialize_with_size_hint`] returns an error.
    fn serialize_outputs(outputs: &[TaskOutput]) -> Vec<u8> {
        SerializedTaskOutputs::serialize_with_size_hint(outputs)
            .expect("task outputs must serialize")
            .to_raw()
    }

    #[test]
    fn round_trip_msgpack() -> anyhow::Result<()> {
        let ctx = TaskContext::new(
            JobId::random(),
            TaskId::Index(0),
            13,
            ResourceGroupId::random(),
            None,
        )?;
        let encoded = rmp_serde::to_vec(&ctx)?;
        let decoded: TaskContext = rmp_serde::from_slice(&encoded)?;
        assert_eq!(decoded, ctx);
        Ok(())
    }

    #[test]
    fn get_task_graph_outputs_returns_commit_outputs() -> anyhow::Result<()> {
        let outputs: Vec<TaskOutput> = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let ctx = TaskContext::new(
            JobId::random(),
            TaskId::Commit,
            1,
            ResourceGroupId::random(),
            Some(serialize_outputs(&outputs)),
        )?;
        assert_eq!(ctx.get_task_graph_outputs()?, Some(outputs));
        Ok(())
    }

    #[test]
    fn get_task_graph_outputs_returns_empty_commit_outputs() -> anyhow::Result<()> {
        let ctx = TaskContext::new(
            JobId::random(),
            TaskId::Commit,
            1,
            ResourceGroupId::random(),
            Some(serialize_outputs(&[])),
        )?;
        assert_eq!(ctx.get_task_graph_outputs()?, Some(Vec::new()));
        Ok(())
    }

    #[test]
    fn get_task_graph_outputs_returns_none_for_non_commit_task() -> anyhow::Result<()> {
        let ctx = TaskContext::new(
            JobId::random(),
            TaskId::Index(0),
            1,
            ResourceGroupId::random(),
            None,
        )?;
        assert!(ctx.get_task_graph_outputs()?.is_none());
        Ok(())
    }

    #[test]
    fn new_rejects_outputs_for_non_commit_task() {
        let outputs: Vec<TaskOutput> = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let result = TaskContext::new(
            JobId::random(),
            TaskId::Index(0),
            1,
            ResourceGroupId::random(),
            Some(serialize_outputs(&outputs)),
        );
        assert!(matches!(result, Err(TdlError::InvalidTaskContext(_))));
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
        assert!(matches!(result, Err(TdlError::InvalidTaskContext(_))));
    }

    #[test]
    fn get_task_graph_outputs_rejects_invalid_bytes() -> anyhow::Result<()> {
        let ctx = TaskContext::new(
            JobId::random(),
            TaskId::Commit,
            1,
            ResourceGroupId::random(),
            Some(vec![0xff, 0x00, 0x13, 0x37]),
        )?;
        assert!(matches!(
            ctx.get_task_graph_outputs(),
            Err(TdlError::DeserializationError(_))
        ));
        Ok(())
    }

    #[test]
    fn get_task_graph_outputs_survives_msgpack_round_trip() -> anyhow::Result<()> {
        let outputs: Vec<TaskOutput> = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let ctx = TaskContext::new(
            JobId::random(),
            TaskId::Commit,
            1,
            ResourceGroupId::random(),
            Some(serialize_outputs(&outputs)),
        )?;
        let encoded = rmp_serde::to_vec(&ctx)?;
        let decoded: TaskContext = rmp_serde::from_slice(&encoded)?;
        assert_eq!(decoded.get_task_graph_outputs()?, Some(outputs));
        Ok(())
    }
}
