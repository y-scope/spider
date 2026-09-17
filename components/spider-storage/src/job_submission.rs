use spider_core::task::TaskGraph;
use spider_core::task::{self};
use spider_core::types::io::TaskGraphInput;

/// Errors produced while constructing a [`ValidatedJobSubmission`] from its compressed
/// serializations.
#[derive(Debug, thiserror::Error)]
pub enum JobSubmissionError {
    /// The compressed task graph could not be deserialized.
    #[error("failed to deserialize the compressed task graph: {0}")]
    TaskGraphDeserialization(#[from] task::Error),

    /// The compressed job inputs could not be deserialized into a [`TaskGraphInput`].
    #[error("failed to deserialize the job inputs: {0}")]
    InputsDeserialization(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The task graph contains no tasks.
    #[error("task graph must contain at least one task")]
    TaskGraphEmpty,

    /// The number of positional job inputs does not match the number of graph inputs.
    #[error("task graph input size mismatch: expected {expected}, got {actual}")]
    TaskGraphInputSizeMismatch { expected: usize, actual: usize },
}

/// A validated wrapper around a task graph, its corresponding job inputs, and the compressed
/// serializations that the database persists verbatim.
///
/// This type guarantees at construction time that:
///
/// * The compressed task graph and job inputs deserialize successfully.
/// * The task graph contains at least one task.
/// * The number of positional job inputs matches the number of graph inputs expected by the task
///   graph.
///
/// The compressed serializations are stored alongside the decoded forms so that the database can
/// persist them without recompressing. They are expected to be in the same format the database
/// stores: zstd-compressed JSON for the task graph and zstd-compressed serialized
/// [`TaskGraphInput`] for the job inputs.
///
/// By passing this type through the call chain, downstream consumers can trust the consistency
/// invariant without re-validating.
#[derive(Debug)]
pub struct ValidatedJobSubmission {
    task_graph: TaskGraph,
    task_graph_input: TaskGraphInput,
    compressed_serialized_task_graph: Vec<u8>,
    compressed_serialized_job_inputs: Vec<u8>,
}

impl ValidatedJobSubmission {
    /// Creates a new validated job submission from its compressed serializations.
    ///
    /// The compressed task graph and job inputs are deserialized in place, then validated for
    /// consistency. The compressed buffers are retained verbatim for the database to persist.
    ///
    /// # Returns
    ///
    /// The validated job submission on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TaskGraph::from_zstd_compressed_json`]'s return values on failure.
    /// * Forwards [`TaskGraphInput::from_zstd_compressed_bytes`]'s return values on failure.
    /// * [`JobSubmissionError::TaskGraphEmpty`] if the task graph contains no tasks.
    /// * [`JobSubmissionError::TaskGraphInputSizeMismatch`] if the number of positional inputs does
    ///   not match the number of graph inputs.
    pub fn create(
        compressed_serialized_task_graph: Vec<u8>,
        compressed_serialized_job_inputs: Vec<u8>,
    ) -> Result<Self, JobSubmissionError> {
        let task_graph = TaskGraph::from_zstd_compressed_json(&compressed_serialized_task_graph)?;
        let task_graph_input =
            TaskGraphInput::from_zstd_compressed_bytes(&compressed_serialized_job_inputs)
                .map_err(|e| JobSubmissionError::InputsDeserialization(Box::new(e)))?;

        let num_tasks = task_graph.get_num_tasks();
        if num_tasks == 0 {
            return Err(JobSubmissionError::TaskGraphEmpty);
        }
        let expected_num_inputs = task_graph.get_task_graph_input_indices().len();
        let actual_num_inputs = task_graph_input.get_positional_inputs().len();
        if expected_num_inputs != actual_num_inputs {
            return Err(JobSubmissionError::TaskGraphInputSizeMismatch {
                expected: expected_num_inputs,
                actual: actual_num_inputs,
            });
        }
        Ok(Self {
            task_graph,
            task_graph_input,
            compressed_serialized_task_graph,
            compressed_serialized_job_inputs,
        })
    }

    /// # Returns
    ///
    /// A reference to the validated task graph.
    #[must_use]
    pub const fn task_graph(&self) -> &TaskGraph {
        &self.task_graph
    }

    /// # Returns
    ///
    /// A reference to the zstd-compressed serialized task graph that the database persists
    /// verbatim.
    #[must_use]
    pub fn compressed_serialized_task_graph(&self) -> &[u8] {
        &self.compressed_serialized_task_graph
    }

    /// # Returns
    ///
    /// A reference to the zstd-compressed serialized job inputs that the database persists
    /// verbatim.
    #[must_use]
    pub fn compressed_serialized_job_inputs(&self) -> &[u8] {
        &self.compressed_serialized_job_inputs
    }

    /// Consumes the wrapper and returns the owned task graph and job inputs.
    ///
    /// The compressed serializations are not returned: they are only needed for database
    /// persistence, which reads them via the accessor methods before the wrapper is consumed.
    ///
    /// # Returns
    ///
    /// A tuple of `(task_graph, task_graph_input)`.
    #[must_use]
    pub fn into_parts(self) -> (TaskGraph, TaskGraphInput) {
        (self.task_graph, self.task_graph_input)
    }
}

/// Compresses a task graph into the zstd-compressed JSON format the database persists.
///
/// # Panics
///
/// Panics if task graph serialization or compression fails.
#[cfg(test)]
#[must_use]
pub fn compress_task_graph(task_graph: &TaskGraph) -> Vec<u8> {
    task_graph
        .to_zstd_compressed_json()
        .expect("task graph compression should succeed")
}

/// Compresses job inputs into the zstd-compressed serialized [`TaskGraphInput`] format the
/// database persists.
///
/// # Panics
///
/// Panics if input serialization or compression fails.
#[cfg(test)]
#[must_use]
pub fn compress_job_inputs(task_graph_input: &TaskGraphInput) -> Vec<u8> {
    task_graph_input
        .to_zstd_compressed_bytes()
        .expect("input compression should succeed")
}

/// Compresses a task graph and job inputs into the formats the database persists, then builds a
/// [`ValidatedJobSubmission`].
///
/// # Panics
///
/// Panics if compression or submission validation fails.
#[allow(clippy::needless_pass_by_value)]
#[cfg(test)]
#[must_use]
pub fn create_validated_submission(
    task_graph: TaskGraph,
    task_graph_input: TaskGraphInput,
) -> ValidatedJobSubmission {
    let compressed_task_graph = compress_task_graph(&task_graph);
    let compressed_job_inputs = compress_job_inputs(&task_graph_input);
    ValidatedJobSubmission::create(compressed_task_graph, compressed_job_inputs)
        .expect("job submission should be valid")
}

#[cfg(test)]
mod tests {
    use spider_core::task::DataTypeDescriptor;
    use spider_core::task::ExecutionPolicy;
    use spider_core::task::TaskDescriptor;
    use spider_core::task::TaskGraph as SubmittedTaskGraph;
    use spider_core::task::TdlContext;
    use spider_core::task::ValueTypeDescriptor;
    use spider_core::types::io::TaskGraphInputBuilder;

    use super::*;

    /// # Returns
    ///
    /// A submitted task graph with a single task that takes `num_inputs` byte-typed inputs and has
    /// no outputs.
    ///
    /// # Panics
    ///
    /// Panics if the task graph creation or the task insertion fails.
    fn create_single_task_graph(num_inputs: usize) -> SubmittedTaskGraph {
        let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
        let mut graph =
            SubmittedTaskGraph::new(None, None).expect("task graph creation should succeed");
        graph
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: "test_pkg".to_owned(),
                    task_func: "test_fn".to_owned(),
                },
                execution_policy: Some(ExecutionPolicy::default()),
                inputs: vec![bytes_type; num_inputs],
                outputs: vec![],
                input_sources: None,
            })
            .expect("task insertion should succeed");
        graph
    }

    #[test]
    fn valid_job_submission_succeeds() -> anyhow::Result<()> {
        let graph = create_single_task_graph(1);
        let mut builder = TaskGraphInputBuilder::new();
        builder.append_task_input(&[1u8; 4])?;
        let submission = create_validated_submission(graph, builder.build());
        assert_eq!(
            submission.task_graph().get_num_tasks(),
            1,
            "valid submission should succeed"
        );
        Ok(())
    }

    #[test]
    fn empty_task_graph_fails() {
        let graph =
            SubmittedTaskGraph::new(None, None).expect("task graph creation should succeed");
        let result = ValidatedJobSubmission::create(
            compress_task_graph(&graph),
            compress_job_inputs(&TaskGraphInputBuilder::new().build()),
        );
        assert!(
            matches!(result, Err(JobSubmissionError::TaskGraphEmpty)),
            "empty task graph should return TaskGraphEmpty"
        );
    }

    #[test]
    fn mismatched_input_count_fails() {
        let graph = create_single_task_graph(1);
        let result = ValidatedJobSubmission::create(
            compress_task_graph(&graph),
            compress_job_inputs(&TaskGraphInputBuilder::new().build()),
        );
        assert!(
            matches!(
                result,
                Err(JobSubmissionError::TaskGraphInputSizeMismatch {
                    expected: 1,
                    actual: 0
                })
            ),
            "mismatched input count should return TaskGraphInputSizeMismatch"
        );
    }

    #[test]
    fn shared_positional_inputs_match_graph_input_count() -> anyhow::Result<()> {
        const SHARED_INPUT: &str = "shared";

        let mut builder = TaskGraphInputBuilder::new();
        let shared_id = builder.create_shared_input_payload(SHARED_INPUT)?;
        builder.append_shared_task_input(shared_id)?;
        builder.append_shared_task_input(shared_id)?;
        let task_graph_input = builder.build();

        let submission = ValidatedJobSubmission::create(
            compress_task_graph(&create_single_task_graph(2)),
            compress_job_inputs(&task_graph_input),
        )?;
        let (_, validated_task_graph_input) = submission.into_parts();
        assert_eq!(validated_task_graph_input, task_graph_input);
        Ok(())
    }

    #[test]
    fn mismatched_shared_positional_input_count_fails() -> anyhow::Result<()> {
        let mut builder = TaskGraphInputBuilder::new();
        let first_shared_id = builder.create_shared_input_payload("first")?;
        builder.create_shared_input_payload("second")?;
        builder.append_shared_task_input(first_shared_id)?;

        let result = ValidatedJobSubmission::create(
            compress_task_graph(&create_single_task_graph(2)),
            compress_job_inputs(&builder.build()),
        );
        assert!(
            matches!(
                result,
                Err(JobSubmissionError::TaskGraphInputSizeMismatch {
                    expected: 2,
                    actual: 1
                })
            ),
            "positional input count mismatch should return TaskGraphInputSizeMismatch, got: \
             {result:?}"
        );
        Ok(())
    }

    #[test]
    fn into_parts_returns_owned_components() -> anyhow::Result<()> {
        let graph = create_single_task_graph(1);
        let mut builder = TaskGraphInputBuilder::new();
        builder.append_task_input(&[1u8; 4])?;
        let task_graph_input = builder.build();
        let submission = create_validated_submission(graph, task_graph_input.clone());
        let (graph, validated_task_graph_input) = submission.into_parts();
        assert_eq!(graph.get_num_tasks(), 1, "task graph should have 1 task");
        assert_eq!(
            validated_task_graph_input, task_graph_input,
            "task graph input should be preserved"
        );
        Ok(())
    }
}
