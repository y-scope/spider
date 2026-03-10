/// Represents a value object.
pub struct Value {}

/// Represents a data object.
pub struct Data {}

/// Represents an input of a task.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct TaskInput {}

/// Represents an output of a task.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct TaskOutput {}
