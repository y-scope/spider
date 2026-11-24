//! Task execution metadata and child job submission handles.

use std::sync::Arc;
use std::sync::Mutex;

use spider_core::job::submission::CompressedJobSubmission;
use spider_core::job::submission::MAX_CHILDREN_PER_JOB;
use spider_core::job::submission::MAX_TASK_CHILDREN_SIZE;
use spider_core::task::TaskContextRaw;
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
    raw: TaskContextRaw,
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
    /// If the task exceeds the child count or size limit, its submissions are discarded and the
    /// limit failure is reported after execution.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TdlError::InvalidTaskContext`] if this is a commit or cleanup task, or the receiver has
    ///   closed.
    /// * [`TdlError::SerializationError`] if [`TaskGraph::to_zstd_compressed_json`] or
    ///   [`TaskGraphInput::to_zstd_compressed_bytes`] fails.
    ///
    /// # Panics
    ///
    /// Panics if another panic poisoned the child submission state.
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
    /// Returns [`TdlError::DeserializationError`] if the task-graph outputs cannot be deserialized.
    pub fn get_task_graph_outputs(&self) -> Result<Option<Vec<TaskOutput>>, TdlError> {
        match self.raw.serialized_task_graph_outputs() {
            Some(bytes) => {
                let task_graph_outputs = SerializedTaskOutputs::deserialize_from_raw(bytes)
                    .map_err(|e| TdlError::DeserializationError(e.to_string()))?;
                Ok(Some(task_graph_outputs))
            }
            None => Ok(None),
        }
    }

    /// Pairs task metadata with a new child job submission sender and receiver.
    ///
    /// # Returns
    ///
    /// * The task context.
    /// * The receiver for the context's child job submissions.
    pub(crate) fn from_raw(raw: TaskContextRaw) -> (Self, ChildJobSubmissionReceiver) {
        let submissions = Arc::new(Mutex::new(ChildJobSubmissions::Collecting {
            children: Vec::new(),
            compressed_size: 0,
        }));
        let context = Self {
            raw,
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
pub struct ChildJobSubmissionReceiver {
    submissions: Arc<Mutex<ChildJobSubmissions>>,
}

impl ChildJobSubmissionReceiver {
    /// Closes the sender and takes the collected submissions.
    ///
    /// # Returns
    ///
    /// The child job submissions in submission order on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TdlError::ChildJobSubmissionLimitExceeded`] if the task exceeded the child count or
    ///   compressed size limit.
    pub fn take(self) -> Result<Vec<CompressedJobSubmission>, TdlError> {
        match self.close() {
            ChildJobSubmissions::Collecting { children, .. } => Ok(children),
            ChildJobSubmissions::LimitExceeded => Err(TdlError::ChildJobSubmissionLimitExceeded {
                count_limit: MAX_CHILDREN_PER_JOB,
                size_limit: MAX_TASK_CHILDREN_SIZE,
            }),
            ChildJobSubmissions::Closed => Ok(Vec::new()),
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
    LimitExceeded,
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
    /// * [`TdlError::InvalidTaskContext`] if the receiver has closed.
    /// * [`TdlError::SerializationError`] if the graph or inputs cannot be serialized.
    ///
    /// # Panics
    ///
    /// Panics if the submission lock is poisoned.
    fn send(
        &self,
        task_graph: &TaskGraph,
        task_graph_input: &TaskGraphInput,
    ) -> Result<(), TdlError> {
        let mut submissions = self
            .submissions
            .lock()
            .expect("child submission lock must not be poisoned");
        let (children, compressed_size) = match &mut *submissions {
            ChildJobSubmissions::Collecting {
                children,
                compressed_size,
            } => (children, compressed_size),
            ChildJobSubmissions::LimitExceeded => return Ok(()),
            ChildJobSubmissions::Closed => {
                return Err(TdlError::InvalidTaskContext(
                    "child job submissions are closed".to_owned(),
                ));
            }
        };
        if children.len() >= MAX_CHILDREN_PER_JOB {
            *submissions = ChildJobSubmissions::LimitExceeded;
            return Ok(());
        }

        let submission = CompressedJobSubmission {
            compressed_serialized_task_graph: task_graph
                .to_zstd_compressed_json()
                .map_err(|e| TdlError::SerializationError(e.to_string()))?,
            compressed_serialized_job_inputs: task_graph_input
                .to_zstd_compressed_bytes()
                .map_err(|e| TdlError::SerializationError(e.to_string()))?,
        };
        let next_size = compressed_size.saturating_add(submission.size());
        if next_size > MAX_TASK_CHILDREN_SIZE {
            *submissions = ChildJobSubmissions::LimitExceeded;
        } else {
            *compressed_size = next_size;
            children.push(submission);
        }
        drop(submissions);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use spider_core::job::submission::MAX_CHILDREN_PER_JOB;
    use spider_core::job::submission::MAX_TASK_CHILDREN_SIZE;
    use spider_core::task::TaskContextRaw;
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
    /// * A context with valid metadata for the given task kind.
    /// * The receiver for its child job submissions.
    ///
    /// # Panics
    ///
    /// Panics if the task context cannot be constructed.
    fn create_context(task_id: TaskId) -> (TaskContext, ChildJobSubmissionReceiver) {
        let raw = TaskContextRaw::new(
            JobId::random(),
            task_id,
            13,
            ResourceGroupId::random(),
            (task_id == TaskId::Commit).then(|| serialize_outputs(&[])),
        )
        .expect("task context metadata must be valid");
        TaskContext::from_raw(raw)
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
        let children = receiver.take()?;
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
        assert_eq!(receiver.take()?.len(), 2);
        assert_eq!(
            cloned.create_child_job_submission(&graph, &input),
            Err(TdlError::InvalidTaskContext(
                "child job submissions are closed".to_owned()
            ))
        );
        Ok(())
    }

    #[test]
    fn child_submissions_over_byte_limit_are_discarded_after_execution() -> anyhow::Result<()> {
        let (ctx, receiver) = create_context(TaskId::Index(0));
        let graph = create_child_graph();
        let input = TaskGraphInputBuilder::new().build();
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
        *compressed_size = MAX_TASK_CHILDREN_SIZE - size;
        drop(submissions);

        ctx.create_child_job_submission(&graph, &input)?;
        let submissions = receiver.submissions.lock().expect("lock poisoned");
        assert!(matches!(*submissions, ChildJobSubmissions::Collecting {
            ref children,
            compressed_size: MAX_TASK_CHILDREN_SIZE,
        } if children.len() == 1));
        drop(submissions);

        ctx.create_child_job_submission(&graph, &input)?;
        ctx.create_child_job_submission(&graph, &input)?;

        assert_eq!(
            receiver.take(),
            Err(TdlError::ChildJobSubmissionLimitExceeded {
                count_limit: MAX_CHILDREN_PER_JOB,
                size_limit: MAX_TASK_CHILDREN_SIZE,
            })
        );
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
            assert_eq!(receiver.take()?, Vec::new());
        }
        Ok(())
    }

    #[test]
    fn raw_context_preserves_msgpack_layout() -> anyhow::Result<()> {
        let job_id = JobId::random();
        let task_id = TaskId::Index(0);
        let task_instance_id = 13;
        let resource_group_id = ResourceGroupId::random();
        let raw = TaskContextRaw::new(job_id, task_id, task_instance_id, resource_group_id, None)?;
        let expected = rmp_serde::to_vec(&(
            job_id,
            task_id,
            task_instance_id,
            resource_group_id,
            Option::<Vec<u8>>::None,
        ))?;
        assert_eq!(rmp_serde::to_vec(&raw)?, expected);
        let (ctx, _receiver) = TaskContext::from_raw(rmp_serde::from_slice(&expected)?);
        assert_eq!(ctx.job_id(), job_id);
        assert_eq!(ctx.task_id(), task_id);
        assert_eq!(ctx.task_instance_id(), task_instance_id);
        assert_eq!(ctx.resource_group_id(), resource_group_id);
        Ok(())
    }

    #[test]
    fn get_task_graph_outputs_returns_commit_outputs() -> anyhow::Result<()> {
        let outputs: Vec<TaskOutput> = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let raw = TaskContextRaw::new(
            JobId::random(),
            TaskId::Commit,
            1,
            ResourceGroupId::random(),
            Some(serialize_outputs(&outputs)),
        )?;
        let (ctx, _receiver) = TaskContext::from_raw(raw);
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
    fn get_task_graph_outputs_rejects_invalid_bytes() -> anyhow::Result<()> {
        let raw = TaskContextRaw::new(
            JobId::random(),
            TaskId::Commit,
            1,
            ResourceGroupId::random(),
            Some(vec![0xff, 0x00, 0x13, 0x37]),
        )?;
        let (ctx, _receiver) = TaskContext::from_raw(raw);
        assert!(matches!(
            ctx.get_task_graph_outputs(),
            Err(TdlError::DeserializationError(_))
        ));
        Ok(())
    }

    #[test]
    fn get_task_graph_outputs_survives_msgpack_round_trip() -> anyhow::Result<()> {
        let outputs: Vec<TaskOutput> = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let raw = TaskContextRaw::new(
            JobId::random(),
            TaskId::Commit,
            1,
            ResourceGroupId::random(),
            Some(serialize_outputs(&outputs)),
        )?;
        let encoded = rmp_serde::to_vec(&raw)?;
        let (ctx, _receiver) = TaskContext::from_raw(rmp_serde::from_slice(&encoded)?);
        assert_eq!(ctx.get_task_graph_outputs()?, Some(outputs));
        Ok(())
    }
}
