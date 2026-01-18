/// Represents a task in the Spider scheduling framework.
pub struct Task {}

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
