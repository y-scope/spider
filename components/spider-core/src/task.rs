mod task_graph;
mod type_descriptor;

use serde::{Deserialize, Serialize};
pub use task_graph::*;
use thiserror::Error;
pub use type_descriptor::*;

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
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum TaskState {
    Pending,
    Ready,
    Running,
    Succeeded,
    Failed(String),
    Cancelled,
}

impl TaskState {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed(_) | Self::Cancelled)
    }
}

/// Represents metadata associated with a task.
pub struct TaskMetadata {}

/// Execution policy for a task, controlling concurrency and retry behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPolicy {
    /// The maximum number of concurrent instances allowed for this task.
    pub max_num_instances: usize,

    /// The maximum number of retries allowed for this task on failure.
    pub max_num_retries: usize,
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
            max_num_instances: 1,
            max_num_retries: 0,
        }
    }
}
