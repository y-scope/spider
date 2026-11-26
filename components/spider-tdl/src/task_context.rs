//! Task execution metadata and child job submission handles.

use std::sync::Arc;
use std::sync::Mutex;

use spider_core::job::submission::CompressedJobSubmission;
use spider_core::job::submission::MAX_CHILDREN_PER_JOB;
use spider_core::job::submission::MAX_TASK_CHILDREN_SIZE;
use spider_core::task::TaskContext as CoreTaskContext;
use spider_core::task::TaskGraph;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::SerializedTaskOutputs;
use spider_core::types::io::TaskGraphInput;
use spider_core::types::io::TaskOutput;

use crate::error::TdlError;

/// Runtime context for a task execution. Clones share child job submissions.
#[derive(Debug, Clone)]
pub struct TaskContext {
    raw: CoreTaskContext,
    serialized_task_graph_outputs: Option<Vec<u8>>,
    child_job_submission_sender: ChildJobSubmissionSender,
}

impl TaskContext {
    #[must_use]
    pub const fn job_id(&self) -> JobId {
        self.raw.job_id()
    }

    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.raw.task_id()
    }

    #[must_use]
    pub const fn task_instance_id(&self) -> TaskInstanceId {
        self.raw.task_instance_id()
    }

    #[must_use]
    pub const fn resource_group_id(&self) -> ResourceGroupId {
        self.raw.resource_group_id()
    }

    /// Creates a child job submission to be processed if this task succeeds.
    ///
    /// A submission that would exceed the child count or size limit is rejected with an error and
    /// leaves previously accepted submissions intact.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TdlError::InvalidTaskContext`] if this is a commit or cleanup task.
    /// * Forwards [`ChildJobSubmissionSender::send`]'s return values on failure.
    pub fn create_child_job_submission(
        &self,
        task_graph: &TaskGraph,
        task_graph_input: &TaskGraphInput,
    ) -> Result<(), TdlError> {
        if !matches!(self.task_id(), TaskId::Index(_)) {
            return Err(TdlError::InvalidTaskContext(
                "termination tasks cannot submit child jobs".to_owned(),
            ));
        }
        self.child_job_submission_sender
            .send(task_graph, task_graph_input)
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
    /// * [`TdlError::DeserializationError`] if the task-graph outputs cannot be deserialized.
    pub fn get_task_graph_outputs(&self) -> Result<Option<Vec<TaskOutput>>, TdlError> {
        match self.serialized_task_graph_outputs.as_deref() {
            Some(bytes) => {
                let task_graph_outputs = SerializedTaskOutputs::deserialize_from_raw(bytes)
                    .map_err(|e| TdlError::DeserializationError(e.to_string()))?;
                Ok(Some(task_graph_outputs))
            }
            None => Ok(None),
        }
    }

    /// Creates a runtime context with the given metadata and serialized task-graph outputs.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The task context.
    /// * The receiver for the context's child job submissions.
    pub(crate) fn from_raw(
        raw: CoreTaskContext,
        serialized_task_graph_outputs: Option<Vec<u8>>,
    ) -> (Self, ChildJobSubmissionReceiver) {
        let submissions = Arc::new(Mutex::new(ChildJobSubmissions::Collecting {
            children: Vec::new(),
            compressed_size: 0,
        }));
        let context = Self {
            raw,
            serialized_task_graph_outputs,
            child_job_submission_sender: ChildJobSubmissionSender {
                submissions: Arc::clone(&submissions),
            },
        };
        (context, ChildJobSubmissionReceiver { submissions })
    }
}

/// Receives the child job submissions for one task context and its clones.
///
/// Dropping the receiver discards any uncollected submissions and closes the sender.
#[derive(Debug)]
#[must_use = "dropping the receiver closes child job submissions"]
pub(crate) struct ChildJobSubmissionReceiver {
    submissions: Arc<Mutex<ChildJobSubmissions>>,
}

impl ChildJobSubmissionReceiver {
    /// Closes the sender and takes the collected submissions.
    ///
    /// # Returns
    ///
    /// The child job submissions in submission order.
    #[must_use]
    pub(crate) fn take(self) -> Vec<CompressedJobSubmission> {
        match self.close() {
            ChildJobSubmissions::Collecting { children, .. } => children,
            ChildJobSubmissions::Closed => Vec::new(),
        }
    }

    /// Closes the sender and removes any pending submissions under the same lock.
    ///
    /// # Returns
    ///
    /// The submission state before closing.
    fn close(&self) -> ChildJobSubmissions {
        let mut submissions = self
            .submissions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::replace(&mut *submissions, ChildJobSubmissions::Closed)
    }
}

impl Drop for ChildJobSubmissionReceiver {
    fn drop(&mut self) {
        drop(self.close());
    }
}

/// Submissions buffered for a task and the state of their collection.
#[derive(Debug)]
enum ChildJobSubmissions {
    Collecting {
        children: Vec<CompressedJobSubmission>,
        compressed_size: u64,
    },
    Closed,
}

/// Sends child job submissions to the paired receiver.
#[derive(Debug, Clone)]
struct ChildJobSubmissionSender {
    submissions: Arc<Mutex<ChildJobSubmissions>>,
}

impl ChildJobSubmissionSender {
    /// Compresses and appends a child job submission.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TdlError::SerializationError`] if the graph or inputs cannot be serialized.
    /// * [`TdlError::InvalidTaskContext`] if the receiver has closed.
    /// * [`TdlError::ChildJobSubmissionLimitExceeded`] if the submission would exceed the child
    ///   count or compressed size limit.
    ///
    /// # Panics
    ///
    /// Panics if the submission lock is poisoned.
    fn send(
        &self,
        task_graph: &TaskGraph,
        task_graph_input: &TaskGraphInput,
    ) -> Result<(), TdlError> {
        let submission = CompressedJobSubmission {
            compressed_serialized_task_graph: task_graph
                .to_zstd_compressed_json()
                .map_err(|e| TdlError::SerializationError(e.to_string()))?,
            compressed_serialized_job_inputs: task_graph_input
                .to_zstd_compressed_bytes()
                .map_err(|e| TdlError::SerializationError(e.to_string()))?,
        };
        let mut submissions = self
            .submissions
            .lock()
            .expect("child submission lock must not be poisoned");
        let ChildJobSubmissions::Collecting {
            children,
            compressed_size,
        } = &mut *submissions
        else {
            return Err(TdlError::InvalidTaskContext(
                "child job submissions are closed".to_owned(),
            ));
        };
        let next_size = compressed_size.saturating_add(submission.size());
        if children.len() >= MAX_CHILDREN_PER_JOB || next_size > MAX_TASK_CHILDREN_SIZE {
            return Err(TdlError::ChildJobSubmissionLimitExceeded {
                count_limit: MAX_CHILDREN_PER_JOB,
                size_limit: MAX_TASK_CHILDREN_SIZE,
            });
        }
        *compressed_size = next_size;
        children.push(submission);
        drop(submissions);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use spider_core::job::submission::MAX_CHILDREN_PER_JOB;
    use spider_core::job::submission::MAX_TASK_CHILDREN_SIZE;
    use spider_core::task::TaskContext as CoreTaskContext;
    use spider_core::task::TaskGraph;
    use spider_core::types::id::JobId;
    use spider_core::types::id::ResourceGroupId;
    use spider_core::types::id::TaskId;
    use spider_core::types::io::SerializedTaskOutputs;
    use spider_core::types::io::TaskGraphInputBuilder;
    use spider_core::types::io::TaskOutput;

    use super::ChildJobSubmissionReceiver;
    use super::ChildJobSubmissions;
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
    /// A tuple containing:
    ///
    /// * A context with valid metadata for the given task kind.
    /// * The receiver for its child job submissions.
    fn create_context(task_id: TaskId) -> (TaskContext, ChildJobSubmissionReceiver) {
        let raw = CoreTaskContext::new(JobId::random(), task_id, 13, ResourceGroupId::random());
        TaskContext::from_raw(
            raw,
            (task_id == TaskId::Commit).then(|| serialize_outputs(&[])),
        )
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

        let (ctx, receiver) = create_context(TaskId::Index(0));
        let graph = create_child_graph();
        let mut expected_inputs = Vec::new();
        for value in [11i32, 22] {
            let mut builder = TaskGraphInputBuilder::new();
            builder.append_task_input(&value)?;
            let input = builder.build();
            ctx.create_child_job_submission(&graph, &input)?;
            expected_inputs.push(input);
        }
        let children = receiver.take();
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
        let (ctx, receiver) = create_context(TaskId::Index(0));
        let cloned = ctx.clone();
        let graph = create_child_graph();
        let mut builder = TaskGraphInputBuilder::new();
        builder.append_task_input(&11i32)?;
        let input = builder.build();
        ctx.create_child_job_submission(&graph, &input)?;
        cloned.create_child_job_submission(&graph, &input)?;
        assert_eq!(receiver.take().len(), 2);
        assert_eq!(
            cloned.create_child_job_submission(&graph, &input),
            Err(TdlError::InvalidTaskContext(
                "child job submissions are closed".to_owned()
            ))
        );
        Ok(())
    }

    #[test]
    fn oversized_child_submissions_do_not_consume_byte_budget() -> anyhow::Result<()> {
        let (ctx, receiver) = create_context(TaskId::Index(0));
        let graph = create_child_graph();
        let input = TaskGraphInputBuilder::new().build();
        let mut builder = TaskGraphInputBuilder::new();
        builder.append_task_input(&11i32)?;
        let larger_input = builder.build();
        let size = u64::try_from(
            graph.to_zstd_compressed_json()?.len() + input.to_zstd_compressed_bytes()?.len(),
        )?;
        let mut submissions = receiver.submissions.lock().expect("lock poisoned");
        let ChildJobSubmissions::Collecting {
            compressed_size, ..
        } = &mut *submissions
        else {
            panic!("expected a new collection");
        };
        *compressed_size = MAX_TASK_CHILDREN_SIZE - 2 * size;
        drop(submissions);

        ctx.create_child_job_submission(&graph, &input)?;
        let limit_error = Err(TdlError::ChildJobSubmissionLimitExceeded {
            count_limit: MAX_CHILDREN_PER_JOB,
            size_limit: MAX_TASK_CHILDREN_SIZE,
        });
        assert_eq!(
            ctx.create_child_job_submission(&graph, &larger_input),
            limit_error
        );
        ctx.create_child_job_submission(&graph, &input)?;
        assert_eq!(ctx.create_child_job_submission(&graph, &input), limit_error);
        assert_eq!(receiver.take().len(), 2);
        Ok(())
    }

    #[test]
    fn dropping_receiver_rejects_child_job_submissions() -> anyhow::Result<()> {
        let (ctx, receiver) = create_context(TaskId::Index(0));
        let graph = create_child_graph();
        let mut builder = TaskGraphInputBuilder::new();
        builder.append_task_input(&11i32)?;
        let input = builder.build();
        ctx.create_child_job_submission(&graph, &input)?;
        drop(receiver);
        assert_eq!(
            ctx.create_child_job_submission(&graph, &input),
            Err(TdlError::InvalidTaskContext(
                "child job submissions are closed".to_owned()
            ))
        );
        Ok(())
    }

    #[test]
    fn termination_contexts_reject_child_jobs() -> anyhow::Result<()> {
        let graph = create_child_graph();
        let mut builder = TaskGraphInputBuilder::new();
        builder.append_task_input(&11i32)?;
        let input = builder.build();
        for task_id in [TaskId::Commit, TaskId::Cleanup] {
            let (ctx, receiver) = create_context(task_id);
            assert_eq!(
                ctx.create_child_job_submission(&graph, &input),
                Err(TdlError::InvalidTaskContext(
                    "termination tasks cannot submit child jobs".to_owned()
                ))
            );
            assert_eq!(receiver.take(), Vec::new());
        }
        Ok(())
    }

    #[test]
    fn get_task_graph_outputs_returns_commit_outputs() -> anyhow::Result<()> {
        let outputs: Vec<TaskOutput> = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let raw = CoreTaskContext::new(
            JobId::random(),
            TaskId::Commit,
            1,
            ResourceGroupId::random(),
        );
        let (ctx, _receiver) = TaskContext::from_raw(raw, Some(serialize_outputs(&outputs)));
        assert_eq!(ctx.get_task_graph_outputs()?, Some(outputs));
        Ok(())
    }

    #[test]
    fn get_task_graph_outputs_returns_empty_commit_outputs() -> anyhow::Result<()> {
        let (ctx, _receiver) = create_context(TaskId::Commit);
        assert_eq!(ctx.get_task_graph_outputs()?, Some(Vec::new()));
        Ok(())
    }

    #[test]
    fn get_task_graph_outputs_returns_none_for_non_commit_task() -> anyhow::Result<()> {
        let (ctx, _receiver) = create_context(TaskId::Index(0));
        assert!(ctx.get_task_graph_outputs()?.is_none());
        Ok(())
    }

    #[test]
    fn get_task_graph_outputs_rejects_invalid_bytes() {
        let raw = CoreTaskContext::new(
            JobId::random(),
            TaskId::Commit,
            1,
            ResourceGroupId::random(),
        );
        let (ctx, _receiver) = TaskContext::from_raw(raw, Some(vec![0xff, 0x00, 0x13, 0x37]));
        assert!(matches!(
            ctx.get_task_graph_outputs(),
            Err(TdlError::DeserializationError(_))
        ));
    }
}
