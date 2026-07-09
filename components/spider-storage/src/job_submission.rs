use spider_core::compression::decode_zstd_bytes;
use spider_core::task::TaskGraph;
use spider_core::task::{self};
use spider_core::types::io::TaskInput;
use spider_utils::wire;

/// Errors produced while constructing a [`ValidatedJobSubmission`] from its compressed
/// serializations.
#[derive(Debug, thiserror::Error)]
pub enum JobSubmissionError {
    /// The compressed task graph could not be deserialized.
    #[error("failed to deserialize the compressed task graph: {0}")]
    TaskGraphDeserialization(#[from] task::Error),

    /// The compressed job inputs could not be decompressed or unframed.
    #[error("failed to deserialize the job inputs: {0}")]
    InputsDeserialization(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The task graph contains no tasks.
    #[error("task graph must contain at least one task")]
    TaskGraphEmpty,

    /// The number of job inputs does not match the number of graph inputs.
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
/// * The number of job inputs matches the number of graph inputs expected by the task graph.
///
/// The compressed serializations are stored alongside the decoded forms so that the database can
/// persist them without recompressing. They are expected to be in the same format the database
/// stores: zstd-compressed JSON for the task graph and zstd-compressed TDL wire-framed bytes for
/// the job inputs.
///
/// By passing this type through the call chain, downstream consumers can trust the consistency
/// invariant without re-validating.
#[derive(Debug)]
pub struct ValidatedJobSubmission {
    task_graph: TaskGraph,
    inputs: Vec<TaskInput>,
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
    /// * Forwards [`decode_zstd_bytes`]'s return values on failure.
    /// * Forwards [`wire::unframe`]'s return values on failure.
    /// * [`JobSubmissionError::TaskGraphEmpty`] if the task graph contains no tasks.
    /// * [`JobSubmissionError::TaskGraphInputSizeMismatch`] if the number of inputs does not match
    ///   the number of graph inputs.
    pub fn create(
        compressed_serialized_task_graph: Vec<u8>,
        compressed_serialized_job_inputs: Vec<u8>,
    ) -> Result<Self, JobSubmissionError> {
        let task_graph = TaskGraph::from_zstd_compressed_json(&compressed_serialized_task_graph)?;
        let serialized_job_inputs = decode_zstd_bytes(&compressed_serialized_job_inputs)
            .map_err(|e| JobSubmissionError::InputsDeserialization(Box::new(e)))?;
        let inputs: Vec<TaskInput> = wire::unframe(&serialized_job_inputs)
            .map_err(|e| JobSubmissionError::InputsDeserialization(Box::new(e)))?
            .into_iter()
            .map(TaskInput::ValuePayload)
            .collect();

        let num_tasks = task_graph.get_num_tasks();
        if num_tasks == 0 {
            return Err(JobSubmissionError::TaskGraphEmpty);
        }
        let expected_num_inputs = task_graph.get_task_graph_input_indices().len();
        let actual_num_inputs = inputs.len();
        if expected_num_inputs != actual_num_inputs {
            return Err(JobSubmissionError::TaskGraphInputSizeMismatch {
                expected: expected_num_inputs,
                actual: actual_num_inputs,
            });
        }
        Ok(Self {
            task_graph,
            inputs,
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
    /// A reference to the validated job inputs.
    #[must_use]
    pub fn inputs(&self) -> &[TaskInput] {
        &self.inputs
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
    /// A tuple of `(task_graph, inputs)`.
    #[must_use]
    pub fn into_parts(self) -> (TaskGraph, Vec<TaskInput>) {
        (self.task_graph, self.inputs)
    }
}

#[cfg(test)]
use spider_core::compression::encode_zstd_bytes;
#[cfg(test)]
use spider_core::types::io::TaskInputsSerializer;

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

/// Compresses job inputs into the zstd-compressed TDL wire-framed format the database persists.
///
/// # Panics
///
/// Panics if input serialization or compression fails.
#[cfg(test)]
#[must_use]
pub fn compress_job_inputs(inputs: &[TaskInput]) -> Vec<u8> {
    let mut serializer = TaskInputsSerializer::new();
    for input in inputs {
        serializer
            .append(input.clone())
            .expect("input serialization should succeed");
    }
    encode_zstd_bytes(&serializer.release()).expect("input compression should succeed")
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
    inputs: Vec<TaskInput>,
) -> ValidatedJobSubmission {
    let compressed_task_graph = compress_task_graph(&task_graph);
    let compressed_job_inputs = compress_job_inputs(&inputs);
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

    use super::*;

    fn create_single_input_task_graph() -> SubmittedTaskGraph {
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
                inputs: vec![bytes_type],
                outputs: vec![],
                input_sources: None,
            })
            .expect("task insertion should succeed");
        graph
    }

    #[test]
    fn valid_job_submission_succeeds() {
        let graph = create_single_input_task_graph();
        let inputs = vec![TaskInput::ValuePayload(vec![1u8; 4])];
        let submission = create_validated_submission(graph, inputs);
        assert_eq!(
            submission.task_graph().get_num_tasks(),
            1,
            "valid submission should succeed"
        );
    }

    #[test]
    fn empty_task_graph_fails() {
        let graph =
            SubmittedTaskGraph::new(None, None).expect("task graph creation should succeed");
        let result =
            ValidatedJobSubmission::create(compress_task_graph(&graph), compress_job_inputs(&[]));
        assert!(
            matches!(result, Err(JobSubmissionError::TaskGraphEmpty)),
            "empty task graph should return TaskGraphEmpty"
        );
    }

    #[test]
    fn mismatched_input_count_fails() {
        let graph = create_single_input_task_graph();
        let result =
            ValidatedJobSubmission::create(compress_task_graph(&graph), compress_job_inputs(&[]));
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
    fn into_parts_returns_owned_components() {
        let graph = create_single_input_task_graph();
        let inputs = vec![TaskInput::ValuePayload(vec![1u8; 4])];
        let submission = create_validated_submission(graph, inputs);
        let (graph, inputs) = submission.into_parts();
        assert_eq!(graph.get_num_tasks(), 1, "task graph should have 1 task");
        assert_eq!(inputs.len(), 1, "should have 1 input");
    }
}
