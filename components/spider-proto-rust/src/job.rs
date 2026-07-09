//! Helpers for converting Spider job values to protobuf fields.

use spider_core::job::JobState;

use crate::error::Error;
use crate::storage;

impl From<JobState> for storage::JobState {
    fn from(state: JobState) -> Self {
        match state {
            JobState::Ready => Self::Ready,
            JobState::Running => Self::Running,
            JobState::CommitReady => Self::CommitReady,
            JobState::CleanupReady => Self::CleanupReady,
            JobState::Succeeded => Self::Succeeded,
            JobState::Failed => Self::Failed,
            JobState::Cancelled => Self::Cancelled,
        }
    }
}

impl TryFrom<storage::JobState> for JobState {
    type Error = Error;

    fn try_from(state: storage::JobState) -> Result<Self, Self::Error> {
        match state {
            storage::JobState::Unspecified => Err(Error::JobStateUnspecified),
            storage::JobState::Ready => Ok(Self::Ready),
            storage::JobState::Running => Ok(Self::Running),
            storage::JobState::CommitReady => Ok(Self::CommitReady),
            storage::JobState::CleanupReady => Ok(Self::CleanupReady),
            storage::JobState::Succeeded => Ok(Self::Succeeded),
            storage::JobState::Failed => Ok(Self::Failed),
            storage::JobState::Cancelled => Ok(Self::Cancelled),
        }
    }
}

#[cfg(test)]
mod tests {
    use spider_core::job::JobState;

    use crate::error::Error;
    use crate::storage::JobState as ProtocolJobState;

    #[test]
    fn job_state_to_protocol_converts_succeeded() {
        let protocol_state = ProtocolJobState::from(JobState::Succeeded);

        assert_eq!(protocol_state, ProtocolJobState::Succeeded);
    }

    #[test]
    fn protocol_job_state_to_core_converts_cleanup_ready() {
        let state = JobState::try_from(ProtocolJobState::CleanupReady)
            .expect("protocol job state conversion should succeed");

        assert_eq!(state, JobState::CleanupReady);
    }

    #[test]
    fn protocol_job_state_to_core_rejects_unspecified() {
        let error = JobState::try_from(ProtocolJobState::Unspecified)
            .expect_err("unspecified job state should fail");

        assert!(matches!(error, Error::JobStateUnspecified));
    }
}
