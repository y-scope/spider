use spider_derive::MySqlEnum;

use crate::{
    task::{Error, TaskGraph},
    types::io::TaskInput,
};

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
    /// * [`Error::InvalidJobSubmission`] if the task graph contains no tasks.
    /// * [`Error::InvalidJobSubmission`] if the number of inputs does not match the number of graph
    ///   inputs.
    pub fn new(task_graph: TaskGraph, inputs: Vec<TaskInput>) -> Result<Self, Error> {
        let num_tasks = task_graph.get_num_tasks();
        if num_tasks == 0 {
            return Err(Error::InvalidJobSubmission(
                "task graph must contain at least one task".to_owned(),
            ));
        }
        let expected_inputs = task_graph.get_task_graph_input_indices().len();
        let actual_inputs = inputs.len();
        if expected_inputs != actual_inputs {
            return Err(Error::InvalidJobSubmission(format!(
                "expected {expected_inputs} graph inputs, got {actual_inputs}"
            )));
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
    use super::*;
    use crate::task::{
        DataTypeDescriptor,
        ExecutionPolicy,
        TaskDescriptor,
        TaskGraph as SubmittedTaskGraph,
        TdlContext,
        ValueTypeDescriptor,
    };

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
        let result = ValidatedJobSubmission::new(graph, inputs);
        assert!(result.is_ok(), "valid submission should succeed");
    }

    #[test]
    fn empty_task_graph_fails() {
        let graph =
            SubmittedTaskGraph::new(None, None).expect("task graph creation should succeed");
        let inputs = vec![];
        let result = ValidatedJobSubmission::new(graph, inputs);
        assert!(
            matches!(result, Err(Error::InvalidJobSubmission(_))),
            "empty task graph should return InvalidJobSubmission"
        );
    }

    #[test]
    fn mismatched_input_count_fails() {
        let graph = create_single_input_task_graph();
        let inputs = vec![];
        let result = ValidatedJobSubmission::new(graph, inputs);
        assert!(
            matches!(result, Err(Error::InvalidJobSubmission(_))),
            "mismatched input count should return InvalidJobSubmission"
        );
    }

    #[test]
    fn into_parts_returns_owned_components() {
        let graph = create_single_input_task_graph();
        let inputs = vec![TaskInput::ValuePayload(vec![1u8; 4])];
        let submission =
            ValidatedJobSubmission::new(graph, inputs).expect("submission should be valid");
        let (graph, inputs) = submission.into_parts();
        assert_eq!(graph.get_num_tasks(), 1, "task graph should have 1 task");
        assert_eq!(inputs.len(), 1, "should have 1 input");
    }
}

/// Represents a job in the Spider scheduling framework.
pub struct Job {}

/// Enum for all possible states of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display, MySqlEnum)]
pub enum JobState {
    Ready,
    Running,
    CommitReady,
    CleanupReady,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobState {
    /// # Returns
    ///
    /// Whether the stat is a terminal state. Terminal states include:
    /// * [`JobState::Succeeded`]
    /// * [`JobState::Failed`]
    /// * [`JobState::Cancelled`]
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    /// # Returns
    ///
    /// Whether the state transition `from` -> `to` is valid.
    #[must_use]
    pub const fn is_valid_transition(from: Self, to: Self) -> bool {
        match to {
            Self::Ready => false,
            Self::Running => matches!(from, Self::Ready),
            Self::CommitReady => matches!(from, Self::Running),
            Self::CleanupReady => matches!(from, Self::Running | Self::CommitReady),
            Self::Succeeded => matches!(from, Self::Running | Self::CommitReady),
            Self::Failed => matches!(from, Self::Running | Self::CommitReady | Self::CleanupReady),
            Self::Cancelled => matches!(from, Self::Ready | Self::Running | Self::CleanupReady),
        }
    }

    #[must_use]
    pub const fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}
