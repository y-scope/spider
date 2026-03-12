/// Represents a job in the Spider scheduling framework.
pub struct Job {}

/// Enum for all possible states of a job.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    strum_macros::Display,
    strum_macros::EnumIter,
    strum_macros::EnumString,
)]
pub enum JobState {
    Ready,
    Running,
    CommitReady,
    CleanupReady,
    Succeeded,
    Failed,
    Cancelled,
}


impl JobState {
    /// The set of terminal states from which a job cannot transition further.
    pub const TERMINAL: [Self; 3] = [Self::Succeeded, Self::Failed, Self::Cancelled];

    /// Checks if the job is in a terminal state (Succeeded, Failed, or Cancelled).
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}
