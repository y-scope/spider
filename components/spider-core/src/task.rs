mod type_descriptor;
mod task_graph;

use thiserror::Error;
pub use type_descriptor::*;
pub use task_graph::*;

#[derive(Error, Debug)]
pub enum Error {
    #[error("`serde_json::Error`: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("`rmp_serde::encode::Error`: {0}")]
    RmpSerdeEncodeError(#[from] rmp_serde::encode::Error),

    #[error("`rmp_serde::decode::Error`: {0}")]
    RmpSerdeDecodeError(#[from] rmp_serde::decode::Error),

    #[error("Invalid struct name: {0}")]
    InvalidStructName(String),

    #[error("Invalid task inputs: {0}")]
    InvalidTaskInputs(String),
}

/// Enum for all possible states of a task.
pub enum TaskState {
    PENDING,
    Ready,
    Running,
    Succeeded,
    Failed(String),
    Cancelled,
}

/// Represents a directed acyclic graph (DAG) of tasks.
pub struct TaskGraph {}

/// Represents metadata associated with a task.
pub struct TaskMetadata {}
