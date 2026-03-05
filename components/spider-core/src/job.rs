/// Represents a job in the Spider scheduling framework.
pub struct Job {}

/// Enum for all possible states of a job.
#[derive(Debug)]
pub enum JobState {
    Ready,
    Running,
    CommitReady,
    CleanupReady,
    Succeeded,
    Failed,
    Cancelled,
}
