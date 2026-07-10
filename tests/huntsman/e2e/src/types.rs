//! Public types shared across the end-to-end test driver.

use spider_core::task::TaskGraph;
use spider_core::types::io::TaskInput;
use spider_core::types::io::TaskOutput;

/// The terminal outcome of a job returned from the test driver.
pub enum TerminationResult {
    /// The job succeeded, carrying the collected task outputs.
    Success(Vec<TaskOutput>),

    /// The job failed, carrying the reported error message.
    Failure(String),

    /// The job was cancelled before completion.
    Cancelled,
}

/// A description of a single job to submit through the test driver.
pub struct JobSubmission {
    /// The external resource-group id the job is submitted under. The driver resolves it to a
    /// Spider-assigned ID, registering it on first use.
    pub resource_group_id: String,

    /// The task graph describing the job's computation.
    pub task_graph: TaskGraph,

    /// The inputs supplied to the job's entry tasks.
    pub inputs: Vec<TaskInput>,
}
