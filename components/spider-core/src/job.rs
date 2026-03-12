use strum_macros::Display;

/// Represents a job in the Spider scheduling framework.
pub struct Job {}

/// Enum for all possible states of a job.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Ready,
    Running,
    CommitReady,
    CleanupReady,
    Succeeded,
    Failed,
    Cancelled,
}
