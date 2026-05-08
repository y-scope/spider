mod task_graph;
mod type_descriptor;

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

    #[error("invalid struct name: {0}")]
    InvalidStructName(String),

    #[error("invalid task inputs: {0}")]
    InvalidTaskInputs(String),

    #[error("invalid execution policy: {0}")]
    InvalidExecutionPolicy(String),

    #[error("invalid timeout policy: {0}")]
    InvalidTimeoutPolicy(String),
}

/// Enum for all possible states of a task.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TaskState {
    Pending,
    Ready,
    Running,
    Succeeded,
    Failed(String),
    Cancelled,
}

impl TaskState {
    /// # Returns
    ///
    /// Whether the state is a terminal state. Terminal states include:
    ///
    /// * [`TaskState::Succeeded`]
    /// * [`TaskState::Failed`]
    /// * [`TaskState::Cancelled`]
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed(..) | Self::Cancelled)
    }
}

/// Represents metadata associated with a task.
pub struct TaskMetadata {}
