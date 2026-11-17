//! Helpers for converting Spider job values to protobuf fields.

use spider_core::job::JobState;
use spider_core::job::submission::CompressedJobSubmission;

use crate::error::Error;
use crate::storage;

impl From<CompressedJobSubmission> for storage::CompressedJobSubmission {
    fn from(submission: CompressedJobSubmission) -> Self {
        Self {
            compressed_serialized_task_graph: submission.compressed_serialized_task_graph,
            compressed_serialized_task_graph_input: submission.compressed_serialized_job_inputs,
        }
    }
}

impl From<storage::CompressedJobSubmission> for CompressedJobSubmission {
    fn from(submission: storage::CompressedJobSubmission) -> Self {
        Self {
            compressed_serialized_task_graph: submission.compressed_serialized_task_graph,
            compressed_serialized_job_inputs: submission.compressed_serialized_task_graph_input,
        }
    }
}

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
            JobState::BlockingOnChildren => Self::BlockingOnChildren,
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
            storage::JobState::BlockingOnChildren => Ok(Self::BlockingOnChildren),
        }
    }
}

#[cfg(test)]
mod tests {
    use spider_core::job::JobState;
    use spider_core::job::submission::CompressedJobSubmission;

    use crate::error::Error;
    use crate::storage;
    use crate::storage::JobState as ProtocolJobState;

    #[test]
    fn test_compressed_job_submission_preserves_graph_and_inputs() {
        let submission = CompressedJobSubmission {
            compressed_serialized_task_graph: vec![1, 2, 3],
            compressed_serialized_job_inputs: vec![4, 5],
        };
        let protocol_submission = storage::CompressedJobSubmission::from(submission.clone());
        assert_eq!(
            protocol_submission.compressed_serialized_task_graph,
            submission.compressed_serialized_task_graph
        );
        assert_eq!(
            protocol_submission.compressed_serialized_task_graph_input,
            submission.compressed_serialized_job_inputs
        );
        assert_eq!(
            CompressedJobSubmission::from(protocol_submission),
            submission
        );
    }

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
    fn blocking_on_children_round_trips() {
        let protocol_state = ProtocolJobState::from(JobState::BlockingOnChildren);
        assert_eq!(protocol_state, ProtocolJobState::BlockingOnChildren);

        let state = JobState::try_from(protocol_state)
            .expect("protocol job state conversion should succeed");
        assert_eq!(state, JobState::BlockingOnChildren);
    }

    #[test]
    fn protocol_job_state_to_core_rejects_unspecified() {
        let error = JobState::try_from(ProtocolJobState::Unspecified)
            .expect_err("unspecified job state should fail");

        assert!(matches!(error, Error::JobStateUnspecified));
    }
}
