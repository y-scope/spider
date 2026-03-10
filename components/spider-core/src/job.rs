/// Represents a job in the Spider scheduling framework.
pub struct Job {}

/// Enum for all possible states of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::Display, strum_macros::EnumIter)]
pub enum JobState {
    Ready,
    Running,
    CommitReady,
    CleanupReady,
    Succeeded,
    Failed,
    Cancelled,
}
