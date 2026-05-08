use spider_core::{task::TaskGraph, types::io::TaskInput};

use super::error::InternalError;

/// A validated wrapper around a task graph and its corresponding job inputs.
///
/// This type guarantees at construction time that:
///
/// * The task graph contains at least one task.
/// * The number of job inputs matches the number of graph inputs expected by the task graph.
///
/// By passing this type through the call chain, downstream consumers can trust the consistency
/// invariant without re-validating.
#[derive(Debug)]
pub struct ValidatedJobSubmission {
    task_graph: TaskGraph,
    inputs: Vec<TaskInput>,
}

impl ValidatedJobSubmission {
    /// Creates a new validated job submission.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskGraphEmpty`] if the task graph contains no tasks.
    /// * [`InternalError::TaskGraphInputSizeMismatch`] if the number of inputs does not match the
    ///   number of graph inputs.
    pub fn validate(task_graph: TaskGraph, inputs: Vec<TaskInput>) -> Result<Self, InternalError> {
        let num_tasks = task_graph.get_num_tasks();
        if num_tasks == 0 {
            return Err(InternalError::TaskGraphEmpty);
        }
        let expected_inputs = task_graph.get_task_graph_input_indices().len();
        let actual_inputs = inputs.len();
        if expected_inputs != actual_inputs {
            return Err(InternalError::TaskGraphInputSizeMismatch {
                expected: expected_inputs,
                actual: actual_inputs,
            });
        }
        Ok(Self { task_graph, inputs })
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

    /// Consumes the wrapper and returns the owned task graph and job inputs.
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
mod tests {
    use spider_core::{
        task::{
            DataTypeDescriptor,
            ExecutionPolicy,
            TaskDescriptor,
            TaskGraph as SubmittedTaskGraph,
            TdlContext,
            ValueTypeDescriptor,
        },
        types::io::TaskInput,
    };

    use super::{super::error::InternalError, *};

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
        let result = ValidatedJobSubmission::validate(graph, inputs);
        assert!(result.is_ok(), "valid submission should succeed");
    }

    #[test]
    fn empty_task_graph_fails() {
        let graph =
            SubmittedTaskGraph::new(None, None).expect("task graph creation should succeed");
        let inputs = vec![];
        let result = ValidatedJobSubmission::validate(graph, inputs);
        assert!(
            matches!(result, Err(InternalError::TaskGraphEmpty)),
            "empty task graph should return EmptyTaskGraph"
        );
    }

    #[test]
    fn mismatched_input_count_fails() {
        let graph = create_single_input_task_graph();
        let inputs = vec![];
        let result = ValidatedJobSubmission::validate(graph, inputs);
        assert!(
            matches!(
                result,
                Err(InternalError::TaskGraphInputSizeMismatch {
                    expected: 1,
                    actual: 0
                })
            ),
            "mismatched input count should return InputCountMismatch"
        );
    }

    #[test]
    fn into_parts_returns_owned_components() {
        let graph = create_single_input_task_graph();
        let inputs = vec![TaskInput::ValuePayload(vec![1u8; 4])];
        let submission =
            ValidatedJobSubmission::validate(graph, inputs).expect("submission should be valid");
        let (graph, inputs) = submission.into_parts();
        assert_eq!(graph.get_num_tasks(), 1, "task graph should have 1 task");
        assert_eq!(inputs.len(), 1, "should have 1 input");
    }
}
