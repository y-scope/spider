use spider_derive::MySqlEnum;

/// Represents a job in the Spider scheduling framework.
pub struct Job {}

/// Enum for all possible states of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display, MySqlEnum)]
pub enum JobState {
    Ready,
    Running,
    CommitReady,
    CleanupReady,
    Succeeded,
    Failed,
    Cancelled,
    BlockingOnChildren,
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
            Self::BlockingOnChildren => matches!(from, Self::Running),
            Self::CommitReady => matches!(from, Self::Running | Self::BlockingOnChildren),
            Self::CleanupReady | Self::Succeeded => matches!(
                from,
                Self::Running | Self::CommitReady | Self::BlockingOnChildren
            ),
            Self::Failed => matches!(
                from,
                Self::Running | Self::CommitReady | Self::CleanupReady | Self::BlockingOnChildren
            ),
            Self::Cancelled => matches!(
                from,
                Self::Ready
                    | Self::Running
                    | Self::CommitReady
                    | Self::CleanupReady
                    | Self::BlockingOnChildren
            ),
        }
    }

    #[must_use]
    pub const fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}

#[cfg(test)]
mod tests {
    use super::JobState;

    #[test]
    fn is_valid_transition_matches_the_transition_table() {
        const ALL_STATES: [JobState; 8] = [
            JobState::Ready,
            JobState::Running,
            JobState::CommitReady,
            JobState::CleanupReady,
            JobState::Succeeded,
            JobState::Failed,
            JobState::Cancelled,
            JobState::BlockingOnChildren,
        ];
        const VALID_TRANSITIONS: [(JobState, JobState); 19] = [
            (JobState::Ready, JobState::Running),
            (JobState::Ready, JobState::Cancelled),
            (JobState::Running, JobState::BlockingOnChildren),
            (JobState::Running, JobState::CommitReady),
            (JobState::Running, JobState::CleanupReady),
            (JobState::Running, JobState::Succeeded),
            (JobState::Running, JobState::Failed),
            (JobState::Running, JobState::Cancelled),
            (JobState::BlockingOnChildren, JobState::CommitReady),
            (JobState::BlockingOnChildren, JobState::CleanupReady),
            (JobState::BlockingOnChildren, JobState::Succeeded),
            (JobState::BlockingOnChildren, JobState::Failed),
            (JobState::BlockingOnChildren, JobState::Cancelled),
            (JobState::CommitReady, JobState::CleanupReady),
            (JobState::CommitReady, JobState::Succeeded),
            (JobState::CommitReady, JobState::Failed),
            (JobState::CommitReady, JobState::Cancelled),
            (JobState::CleanupReady, JobState::Failed),
            (JobState::CleanupReady, JobState::Cancelled),
        ];

        for from in ALL_STATES {
            for to in ALL_STATES {
                let expected = VALID_TRANSITIONS.contains(&(from, to));
                assert_eq!(
                    JobState::is_valid_transition(from, to),
                    expected,
                    "{from} -> {to}"
                );
            }
        }
    }

    #[test]
    fn blocking_on_children_is_not_terminal() {
        assert!(!JobState::BlockingOnChildren.is_terminal());
    }
}
