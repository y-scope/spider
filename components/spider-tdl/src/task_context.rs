//! Runtime metadata passed to every task function.
//!
//! [`TaskContext`] is constructed and msgpack-serialized by the execution manager, passed opaquely
//! through the task executor, and deserialized inside the TDL package before being handed to the
//! user's task function as the first parameter.

use std::sync::Arc;
use std::sync::Mutex;

use spider_core::job::submission::CompressedJobSubmission;
use spider_core::task::TaskGraph;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::SerializedTaskOutputs;
use spider_core::types::io::TaskGraphInput;
use spider_core::types::io::TaskOutput;

use crate::error::TdlError;

/// Runtime metadata about the current task execution.
///
/// Every task function receives a [`TaskContext`] as its first parameter. It carries identifiers
/// that link the execution back to the job and task definitions in the storage layer.
///
/// Serialized via plain msgpack ([`rmp_serde::to_vec`] or [`rmp_serde::from_slice`]), separately
/// from the task inputs wire stream. Clones share child submissions for this execution; those
/// submissions are omitted from serialized metadata.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskContext {
    job_id: JobId,
    task_id: TaskId,
    task_instance_id: TaskInstanceId,
    resource_group_id: ResourceGroupId,
    serialized_task_graph_outputs: Option<Vec<u8>>,
    #[serde(skip)]
    child_jobs: ChildJobSubmissions,
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
            child_jobs: ChildJobSubmissions::default(),
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

    /// Collects a child job for submission when this task succeeds.
    ///
    /// Call this method before the task returns.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TdlError::InvalidTaskContext`] if this is a commit or cleanup task.
    /// * [`TdlError::SerializationError`] if [`TaskGraph::to_zstd_compressed_json`] or
    ///   [`TaskGraphInput::to_zstd_compressed_bytes`] fails.
    ///
    /// # Panics
    ///
    /// Panics if another panic poisoned the child submission state.
    pub fn create_child_job(
        &self,
        task_graph: &TaskGraph,
        task_graph_input: &TaskGraphInput,
    ) -> Result<(), TdlError> {
        if !matches!(self.task_id, TaskId::Index(_)) {
            return Err(TdlError::InvalidTaskContext(
                "termination tasks cannot submit child jobs".to_owned(),
            ));
        }
        let child = CompressedJobSubmission {
            compressed_serialized_task_graph: task_graph
                .to_zstd_compressed_json()
                .map_err(|e| TdlError::SerializationError(e.to_string()))?,
            compressed_serialized_job_inputs: task_graph_input
                .to_zstd_compressed_bytes()
                .map_err(|e| TdlError::SerializationError(e.to_string()))?,
        };
        self.child_jobs
            .submissions
            .lock()
            .expect("child submission lock must not be poisoned")
            .push(child);
        Ok(())
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

    pub(crate) const fn child_jobs(&self) -> &ChildJobSubmissions {
        &self.child_jobs
    }
}

/// Shared collection of child job submissions.
#[derive(Debug, Clone, Default)]
pub(crate) struct ChildJobSubmissions {
    submissions: Arc<Mutex<Vec<CompressedJobSubmission>>>,
}

impl ChildJobSubmissions {
    /// Takes the collected child submissions.
    ///
    /// # Returns
    ///
    /// The collected child submissions in submission order.
    pub(crate) fn take(&self) -> Vec<CompressedJobSubmission> {
        let mut submissions = self
            .submissions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::take(&mut *submissions)
    }
}

#[cfg(test)]
mod tests {
    use spider_core::task::TaskGraph;
    use spider_core::types::id::JobId;
    use spider_core::types::id::ResourceGroupId;
    use spider_core::types::id::TaskId;
    use spider_core::types::io::SerializedTaskOutputs;
    use spider_core::types::io::TaskGraphInputBuilder;
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

    /// # Returns
    ///
    /// A context with valid metadata for the given task kind.
    ///
    /// # Panics
    ///
    /// Panics if the task context cannot be constructed.
    fn create_context(task_id: TaskId) -> TaskContext {
        TaskContext::new(
            JobId::random(),
            task_id,
            13,
            ResourceGroupId::random(),
            (task_id == TaskId::Commit).then(|| serialize_outputs(&[])),
        )
        .expect("task context metadata must be valid")
    }

    /// # Returns
    ///
    /// A task graph containing one task with one integer input.
    ///
    /// # Panics
    ///
    /// Panics if the task descriptor is invalid.
    fn create_child_graph() -> TaskGraph {
        use spider_core::task::DataTypeDescriptor;
        use spider_core::task::TaskDescriptor;
        use spider_core::task::TdlContext;
        use spider_core::task::ValueTypeDescriptor;

        let mut graph = TaskGraph::default();
        graph
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: "test_pkg".to_owned(),
                    task_func: "child".to_owned(),
                },
                execution_policy: None,
                inputs: vec![DataTypeDescriptor::Value(ValueTypeDescriptor::int32())],
                outputs: Vec::new(),
                input_sources: None,
            })
            .expect("child task descriptor must be valid");
        graph
    }

    #[test]
    fn child_jobs_preserve_storage_encoding_and_order() -> anyhow::Result<()> {
        use spider_core::types::io::TaskGraphInput;

        let ctx = create_context(TaskId::Index(0));
        let graph = create_child_graph();
        let mut expected_inputs = Vec::new();
        for value in [11i32, 22] {
            let mut builder = TaskGraphInputBuilder::new();
            builder.append_task_input(&value)?;
            let input = builder.build();
            ctx.create_child_job(&graph, &input)?;
            expected_inputs.push(input);
        }
        let children = ctx.child_jobs().take();
        assert_eq!(children.len(), expected_inputs.len());
        for (child, expected_input) in children.iter().zip(expected_inputs) {
            assert_eq!(
                TaskGraph::from_zstd_compressed_json(&child.compressed_serialized_task_graph)?,
                graph
            );
            assert_eq!(
                TaskGraphInput::from_zstd_compressed_bytes(
                    &child.compressed_serialized_job_inputs
                )?,
                expected_input
            );
        }
        Ok(())
    }

    #[test]
    fn cloned_contexts_share_child_submissions() -> anyhow::Result<()> {
        let ctx = create_context(TaskId::Index(0));
        let cloned = ctx.clone();
        let graph = create_child_graph();
        let mut builder = TaskGraphInputBuilder::new();
        builder.append_task_input(&11i32)?;
        let input = builder.build();
        ctx.create_child_job(&graph, &input)?;
        cloned.create_child_job(&graph, &input)?;
        assert_eq!(cloned.child_jobs().take().len(), 2);
        assert_eq!(ctx.child_jobs().take(), Vec::new());
        Ok(())
    }

    #[test]
    fn termination_contexts_reject_child_jobs() -> anyhow::Result<()> {
        let graph = create_child_graph();
        let mut builder = TaskGraphInputBuilder::new();
        builder.append_task_input(&11i32)?;
        let input = builder.build();
        for task_id in [TaskId::Commit, TaskId::Cleanup] {
            let ctx = create_context(task_id);
            assert!(matches!(
                ctx.create_child_job(&graph, &input),
                Err(TdlError::InvalidTaskContext(_))
            ));
            assert_eq!(ctx.child_jobs().take(), Vec::new());
        }
        Ok(())
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
        assert_eq!(decoded.job_id(), ctx.job_id());
        assert_eq!(decoded.task_id(), ctx.task_id());
        assert_eq!(decoded.task_instance_id(), ctx.task_instance_id());
        assert_eq!(decoded.resource_group_id(), ctx.resource_group_id());
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
