//! The job-level events of a job tree and their outcomes.

use spider_core::job::JobState;
use spider_core::types::id::JobId;

use crate::cache::error::CacheError;
use crate::cache::error::StaleStateError;

/// A job-level event of one job of a tree.
#[derive(Debug)]
pub struct TreeEvent {
    pub job_id: JobId,
    pub kind: TreeEventKind,
}

/// The kind of a job-level event.
#[derive(Debug, PartialEq, Eq)]
pub enum TreeEventKind {
    /// The root is started.
    Start,

    /// The job's last regular task succeeded.
    RegularTasksDone,

    /// The job's commit task succeeded.
    CommitDone,

    /// The job's cleanup task succeeded.
    CleanupDone,

    /// A task of the job in the [`Lane`] `lane` failed with no retry left.
    TerminalFailure { lane: Lane, message: String },

    /// The root is cancelled.
    CancelRoot,
}

impl TreeEventKind {
    /// # Returns
    ///
    /// Whether an event of this kind travels on its tree's fast-forward channel: a
    /// [`Self::CancelRoot`] or a [`Self::TerminalFailure`].
    #[must_use]
    pub const fn is_fast_forward(&self) -> bool {
        matches!(self, Self::CancelRoot | Self::TerminalFailure { .. })
    }

    /// Checks that a job in `state` accepts an event of this kind.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::JobAlreadyStarted`] if a [`Self::Start`] finds the job not
    ///   [`JobState::Ready`].
    /// * [`StaleStateError::JobAlreadyTerminated`] if a task event ([`Self::RegularTasksDone`],
    ///   [`Self::CommitDone`], [`Self::CleanupDone`] or [`Self::TerminalFailure`]) finds the job
    ///   terminal, or a [`Self::CancelRoot`] finds it [`JobState::Succeeded`] or
    ///   [`JobState::Failed`].
    /// * [`StaleStateError::JobNoLongerRunning`], [`StaleStateError::JobNoLongerCommitReady`] or
    ///   [`StaleStateError::JobNoLongerCleanupReady`] if a task event finds the job non-terminal
    ///   but not in the state that runs the event's [`Lane`].
    /// * [`StaleStateError::JobCancellationAlreadyRequested`] if a [`Self::CancelRoot`] finds the
    ///   job [`JobState::CleanupReady`].
    /// * [`StaleStateError::JobAlreadyCancelled`] if a [`Self::CancelRoot`] finds the job
    ///   [`JobState::Cancelled`].
    pub const fn ensure_accepts(&self, state: JobState) -> Result<(), StaleStateError> {
        match self {
            Self::Start => match state {
                JobState::Ready => Ok(()),
                _ => Err(StaleStateError::JobAlreadyStarted),
            },
            Self::RegularTasksDone => Lane::Regular.ensure_accepts(state),
            Self::CommitDone => Lane::Commit.ensure_accepts(state),
            Self::CleanupDone => Lane::Cleanup.ensure_accepts(state),
            Self::TerminalFailure { lane, .. } => lane.ensure_accepts(state),
            Self::CancelRoot => match state {
                JobState::CleanupReady => Err(StaleStateError::JobCancellationAlreadyRequested),
                JobState::Cancelled => Err(StaleStateError::JobAlreadyCancelled),
                state if state.is_terminal() => Err(StaleStateError::JobAlreadyTerminated(state)),
                _ => Ok(()),
            },
        }
    }
}

/// The lane of a task: whether it is a regular, commit or cleanup task of its job.
///
/// A job runs the tasks of one lane per state: its regular tasks while [`JobState::Running`], its
/// commit task while [`JobState::CommitReady`] and its cleanup task while
/// [`JobState::CleanupReady`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    Regular,
    Commit,
    Cleanup,
}

impl Lane {
    /// Checks that a job in `state` runs the tasks of this lane.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::JobAlreadyTerminated`] if `state` is terminal.
    /// * [`StaleStateError::JobNoLongerRunning`], [`StaleStateError::JobNoLongerCommitReady`] or
    ///   [`StaleStateError::JobNoLongerCleanupReady`], by lane, if `state` is any other state that
    ///   does not run this lane.
    const fn ensure_accepts(self, state: JobState) -> Result<(), StaleStateError> {
        match (self, state) {
            (Self::Regular, JobState::Running)
            | (Self::Commit, JobState::CommitReady)
            | (Self::Cleanup, JobState::CleanupReady) => Ok(()),
            _ if state.is_terminal() => Err(StaleStateError::JobAlreadyTerminated(state)),
            (Self::Regular, _) => Err(StaleStateError::JobNoLongerRunning),
            (Self::Commit, _) => Err(StaleStateError::JobNoLongerCommitReady),
            (Self::Cleanup, _) => Err(StaleStateError::JobNoLongerCleanupReady),
        }
    }
}

/// The outcome of an event.
#[derive(Debug)]
pub enum TreeOutcome {
    /// The event was applied. Carries the job's new state.
    Applied(JobState),

    /// The job's state does not accept the event, so nothing was written for it.
    Stale(StaleStateError),

    /// The event's job is not in the tree, or the tree's committer no longer takes events.
    Untracked,
}

/// The reply to an event.
pub type TreeReply = Result<TreeOutcome, CacheError>;

#[cfg(test)]
mod tests {
    use spider_core::job::JobState;

    use crate::cache::error::StaleStateError;
    use crate::cache::tree::Lane;
    use crate::cache::tree::TreeEventKind;
    use crate::cache::tree::test_utils::failure;

    // `BlockingOnChildren` is left out: no job can be in it until a tree has children.
    const STATES: [JobState; 7] = [
        JobState::Ready,
        JobState::Running,
        JobState::CommitReady,
        JobState::CleanupReady,
        JobState::Succeeded,
        JobState::Failed,
        JobState::Cancelled,
    ];

    #[test]
    fn start_is_accepted_only_by_a_ready_job() {
        for state in STATES {
            let result = TreeEventKind::Start.ensure_accepts(state);
            let is_expected = if state == JobState::Ready {
                matches!(result, Ok(()))
            } else {
                matches!(result, Err(StaleStateError::JobAlreadyStarted))
            };
            assert!(is_expected, "a start in {state} gave {result:?}");
        }
    }

    #[test]
    fn a_task_event_is_accepted_only_in_its_lanes_state() {
        use std::mem::discriminant;

        for (kind, lane_state, no_longer_error) in [
            (
                TreeEventKind::RegularTasksDone,
                JobState::Running,
                StaleStateError::JobNoLongerRunning,
            ),
            (
                TreeEventKind::CommitDone,
                JobState::CommitReady,
                StaleStateError::JobNoLongerCommitReady,
            ),
            (
                TreeEventKind::CleanupDone,
                JobState::CleanupReady,
                StaleStateError::JobNoLongerCleanupReady,
            ),
            (
                failure(Lane::Regular, "boom"),
                JobState::Running,
                StaleStateError::JobNoLongerRunning,
            ),
            (
                failure(Lane::Commit, "boom"),
                JobState::CommitReady,
                StaleStateError::JobNoLongerCommitReady,
            ),
            (
                failure(Lane::Cleanup, "boom"),
                JobState::CleanupReady,
                StaleStateError::JobNoLongerCleanupReady,
            ),
        ] {
            for state in STATES {
                let result = kind.ensure_accepts(state);
                let is_expected = if state == lane_state {
                    matches!(result, Ok(()))
                } else if state.is_terminal() {
                    matches!(
                        result,
                        Err(StaleStateError::JobAlreadyTerminated(terminal_state))
                            if terminal_state == state
                    )
                } else {
                    result
                        .as_ref()
                        .is_err_and(|error| discriminant(error) == discriminant(&no_longer_error))
                };
                assert!(is_expected, "{kind:?} in {state} gave {result:?}");
            }
        }
    }

    #[test]
    fn cancel_root_is_accepted_until_the_root_is_cleanup_ready_or_terminal() {
        for state in [JobState::Ready, JobState::Running, JobState::CommitReady] {
            let result = TreeEventKind::CancelRoot.ensure_accepts(state);
            assert!(
                matches!(result, Ok(())),
                "a cancel in {state} gave {result:?}"
            );
        }

        let result = TreeEventKind::CancelRoot.ensure_accepts(JobState::CleanupReady);
        assert!(
            matches!(
                result,
                Err(StaleStateError::JobCancellationAlreadyRequested)
            ),
            "a cancel in CleanupReady gave {result:?}"
        );

        let result = TreeEventKind::CancelRoot.ensure_accepts(JobState::Cancelled);
        assert!(
            matches!(result, Err(StaleStateError::JobAlreadyCancelled)),
            "a cancel in Cancelled gave {result:?}"
        );

        for state in [JobState::Succeeded, JobState::Failed] {
            let result = TreeEventKind::CancelRoot.ensure_accepts(state);
            assert!(
                matches!(
                    result,
                    Err(StaleStateError::JobAlreadyTerminated(terminal_state))
                        if terminal_state == state
                ),
                "a cancel in {state} gave {result:?}"
            );
        }
    }
}
