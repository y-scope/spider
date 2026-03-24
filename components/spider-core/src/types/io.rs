use serde::{Deserialize, Serialize};

use crate::{
    task::{TdlContext, TimeoutPolicy},
    types::id::TaskInstanceId,
};

/// Represents an input of a task.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum TaskInput {
    ValuePayload(Vec<u8>),
}

/// Represents an output of a task.
pub type TaskOutput = Vec<u8>;

/// The execution context for a task instance.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecutionContext {
    pub task_instance_id: TaskInstanceId,
    pub tdl_context: TdlContext,
    pub timeout_policy: TimeoutPolicy,
    pub inputs: Vec<TaskInput>,
}
