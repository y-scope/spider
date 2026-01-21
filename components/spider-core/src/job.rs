/// Represents a job in the Spider scheduling framework.
pub struct Job {}

/// Enum for all possible states of a job.
pub enum JobState {
    Running,
    PendingRetry,
    Succeeded,
    Failed,
    Cancelled,
}
