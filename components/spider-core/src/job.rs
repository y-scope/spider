use spider_derive::MySqlEnum;

/// Represents a job in the Spider scheduling framework.
pub struct Job {}

/// Enum for all possible states of a job.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum_macros::Display, strum_macros::EnumIter, MySqlEnum,
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

/// The valid target states when cancelling a job.
///
/// * [`CancelTarget::CleanupReady`] — the job has a cleanup task to run.
/// * [`CancelTarget::Cancelled`] — the job can be terminated immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelTarget {
    CleanupReady,
    Cancelled,
}

impl CancelTarget {
    /// Converts this target into the corresponding [`JobState`].
    #[must_use]
    pub const fn into_job_state(self) -> JobState {
        match self {
            Self::CleanupReady => JobState::CleanupReady,
            Self::Cancelled => JobState::Cancelled,
        }
    }
}

/// The valid target states when committing job outputs.
///
/// * [`CommitTarget::CommitReady`] — the job has a commit task to run.
/// * [`CommitTarget::Succeeded`] — the job can be marked as succeeded immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitTarget {
    CommitReady,
    Succeeded,
}

impl CommitTarget {
    /// Converts this target into the corresponding [`JobState`].
    #[must_use]
    pub const fn into_job_state(self) -> JobState {
        match self {
            Self::CommitReady => JobState::CommitReady,
            Self::Succeeded => JobState::Succeeded,
        }
    }
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
}
