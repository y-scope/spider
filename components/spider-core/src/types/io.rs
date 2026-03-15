use serde::{Deserialize, Serialize};

/// Represents a value object.
pub struct Value {}

/// Represents a data object.
pub struct Data {}

/// Represents an input of a task.
#[derive(Serialize, Deserialize, Debug)]
pub enum TaskInput {
    ValuePayload(Vec<u8>),
}

/// Represents an output of a task.
pub type TaskOutput = Vec<u8>;
