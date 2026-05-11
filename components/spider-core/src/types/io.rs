use serde::{Deserialize, Serialize};

use crate::{
    task::{TdlContext, TimeoutPolicy},
    types::id::TaskInstanceId,
};

/// Represents an input of a task.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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

/// Serialized job inputs, each element a msgpack-serialized [`TaskInput`].
pub type SerializedJobInputs = Vec<Vec<u8>>;

/// Deserializes msgpack-serialized job inputs.
///
/// # Errors
///
/// Returns `rmp_serde::decode::Error` if any input fails to deserialize.
pub fn deserialize_job_inputs(
    inputs: &SerializedJobInputs,
) -> Result<Vec<TaskInput>, rmp_serde::decode::Error> {
    inputs
        .iter()
        .map(|bytes| rmp_serde::from_slice(bytes))
        .collect()
}

/// Serializes job inputs to msgpack.
///
/// # Errors
///
/// Returns `rmp_serde::encode::Error` if any input fails to serialize.
pub fn serialize_job_inputs(
    inputs: &[TaskInput],
) -> Result<SerializedJobInputs, rmp_serde::encode::Error> {
    inputs.iter().map(rmp_serde::to_vec).collect()
}
