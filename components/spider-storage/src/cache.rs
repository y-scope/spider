use spider_core::task::TaskIndex;

pub mod error;
pub mod io;
pub mod job;
mod sync;
pub mod task;

/// Identifier of a task inside a job.
pub enum TaskId {
    /// The index of the task in the job's task graph.
    Index(TaskIndex),

    /// The commit task.
    Commit,

    /// The cleanup task.
    Cleanup,
}
