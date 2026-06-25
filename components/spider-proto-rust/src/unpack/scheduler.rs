//! [`Unpack`] implementations for `scheduler.proto` responses.

use spider_core::types::{
    id::{JobId, ResourceGroupId, SchedulerId, TaskAssignmentId},
    scheduler::{SchedulerResponse, TaskAssignment},
};
use tonic::Code;

use crate::{
    common,
    scheduler::{NextTaskResponse, next_task_response},
    unpack::{Unpack, UnpackError, common::unpack_task_id},
};

/// Unpacks a [`NextTaskResponse`] into an optional [`SchedulerResponse`].
///
/// A `None` result means the scheduler long poll timed out without an assignment. The public
/// [`SchedulerClient`](crate::SchedulerClient) implementation retries this case so callers keep
/// the blocking trait semantics.
///
/// # Returns
///
/// `Some` containing a [`SchedulerResponse`] when a task is assigned, or `None` when no task is
/// currently available, on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`Code::InvalidArgument`] (as [`UnpackError`]) if the response is missing a result or contains
///   a malformed task ID.
impl Unpack for NextTaskResponse {
    type Unpacked = Option<SchedulerResponse>;

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        match self.result {
            Some(next_task_response::Result::Assignment(assignment)) => {
                let task_id = unpack_task_id(assignment.task_id).inspect_err(|error| {
                    tracing::error!(
                        error = %error.message,
                        request = "NextTask",
                        assignment_id = assignment.id,
                        "Failed to unpack response."
                    );
                })?;
                Ok(Some(SchedulerResponse {
                    task_assignment: TaskAssignment {
                        id: TaskAssignmentId::from(assignment.id),
                        resource_group_id: ResourceGroupId::from(assignment.resource_group_id),
                        job_id: JobId::from(assignment.job_id),
                        task_id,
                    },
                    scheduler_id: SchedulerId::from(assignment.scheduler_id),
                    session_id: assignment.session_id,
                }))
            }
            Some(next_task_response::Result::NoTask(common::Void {})) => Ok(None),
            None => Err(UnpackError {
                code: Code::InvalidArgument,
                message: "next task response missing result".to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use spider_core::types::{
        id::{JobId, ResourceGroupId, SchedulerId, TaskAssignmentId, TaskId},
        scheduler::{SchedulerResponse, TaskAssignment},
    };

    use crate::{
        common,
        scheduler::{NextTaskResponse, SchedulerAssignment, next_task_response},
        unpack::Unpack,
    };

    #[test]
    fn unpack_returns_assignment() {
        let response = NextTaskResponse {
            result: Some(next_task_response::Result::Assignment(
                SchedulerAssignment {
                    id: 7,
                    resource_group_id: 11,
                    job_id: 13,
                    task_id: Some(common::TaskId::from(TaskId::Commit)),
                    scheduler_id: 17,
                    session_id: 19,
                },
            )),
        };

        let assignment = response
            .unpack()
            .expect("scheduler response unpacking should succeed")
            .expect("scheduler response should contain an assignment");

        assert_eq!(
            assignment,
            SchedulerResponse {
                task_assignment: TaskAssignment {
                    id: TaskAssignmentId::from(7),
                    resource_group_id: ResourceGroupId::from(11),
                    job_id: JobId::from(13),
                    task_id: TaskId::Commit,
                },
                scheduler_id: SchedulerId::from(17),
                session_id: 19,
            }
        );
    }

    #[test]
    fn unpack_returns_none_for_no_task() {
        let response = NextTaskResponse {
            result: Some(next_task_response::Result::NoTask(common::Void {})),
        };

        let result = response
            .unpack()
            .expect("scheduler response unpacking should succeed");

        assert_eq!(result, None);
    }

    #[test]
    fn unpack_rejects_missing_result() {
        let result = NextTaskResponse { result: None }.unpack();
        assert!(result.is_err());
    }

    #[test]
    fn unpack_rejects_empty_assignment_task_id() {
        let response = NextTaskResponse {
            result: Some(next_task_response::Result::Assignment(
                SchedulerAssignment {
                    id: 7,
                    resource_group_id: 11,
                    job_id: 11,
                    task_id: None,
                    scheduler_id: 17,
                    session_id: 17,
                },
            )),
        };

        let result = response.unpack();
        assert!(result.is_err());
    }

    #[test]
    fn unpack_rejects_malformed_task_id() {
        let response = NextTaskResponse {
            result: Some(next_task_response::Result::Assignment(
                SchedulerAssignment {
                    id: 7,
                    resource_group_id: 11,
                    job_id: 13,
                    task_id: Some(common::TaskId { kind: None }),
                    scheduler_id: 17,
                    session_id: 19,
                },
            )),
        };

        let result = response.unpack();
        assert!(result.is_err());
    }
}
