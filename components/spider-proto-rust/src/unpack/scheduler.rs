//! Conversions between `scheduler.proto` messages and their spider-native forms.

use spider_core::types::{
    id::{JobId, ResourceGroupId, SchedulerId, TaskAssignmentId},
    scheduler::{SchedulerResponse, TaskAssignment, TaskAssignmentRecord},
};

use crate::{
    common,
    scheduler::{
        NextTaskResponse,
        TaskAssignmentRecord as ProtoTaskAssignmentRecord,
        next_task_response,
    },
    unpack::common::unpack_task_id,
};

/// Converts an assignment record into its protobuf representation.
impl From<TaskAssignmentRecord> for ProtoTaskAssignmentRecord {
    fn from(record: TaskAssignmentRecord) -> Self {
        Self {
            id: record.id.get(),
            from: record.from.get(),
        }
    }
}

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
/// Returns a message string if the response is missing a result or contains a malformed task ID.
impl TryFrom<NextTaskResponse> for Option<SchedulerResponse> {
    type Error = String;

    fn try_from(response: NextTaskResponse) -> Result<Self, String> {
        match response.result {
            Some(next_task_response::Result::Assignment(assignment)) => {
                let task_id = unpack_task_id(assignment.task_id)
                    .inspect_err(|error| {
                        tracing::error!(
                            error = % error.message,
                            request = "NextTask",
                            assignment_id = assignment.id,
                            "Failed to unpack response."
                        );
                    })
                    .map_err(|error| error.message)?;
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
            None => Err("next task response missing result".to_owned()),
        }
    }
}
