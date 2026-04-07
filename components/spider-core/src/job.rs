use spider_derive::MySqlEnum;

/// Represents a job in the Spider scheduling framework.
pub struct Job {}

/// Enum for all possible states of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::Display, MySqlEnum)]
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
    /// # Returns
    ///
    /// Whether the stat is a terminal state. Terminal states include:
    /// * [`JobState::Succeeded`]
    /// * [`JobState::Failed`]
    /// * [`JobState::Cancelled`]
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    /// # Returns
    ///
    /// Whether the state transition `from` -> `to` is valid.
    #[must_use]
    pub const fn is_valid_transition(from: Self, to: Self) -> bool {
        match to {
            Self::Ready => false,
            Self::Running => matches!(from, Self::Ready),
            Self::CommitReady => matches!(from, Self::Running),
            Self::CleanupReady => matches!(from, Self::Running | Self::CommitReady),
            Self::Succeeded => matches!(from, Self::Running | Self::CommitReady),
            Self::Failed => matches!(from, Self::Running | Self::CommitReady | Self::CleanupReady),
            Self::Cancelled => matches!(from, Self::Ready | Self::Running | Self::CleanupReady),
        }
    }

    #[must_use]
    pub const fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}
