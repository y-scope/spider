//! gRPC-backed [`SchedulerClient`] implementation.

use async_trait::async_trait;
use spider_core::types::id::{ExecutionManagerId, JobId, ResourceGroupId, TaskId};
use spider_proto_rust::{
    common,
    scheduler::{
        self,
        NextTaskResponse,
        next_task_response,
        scheduler_service_client::SchedulerServiceClient,
    },
};
use tonic::transport::{Channel, Endpoint};

use crate::client::scheduler::{SchedulerClient, SchedulerError, SchedulerResponse};

/// gRPC-backed [`SchedulerClient`] implementation.
#[derive(Debug, Clone)]
pub struct GrpcSchedulerClient {
    client: SchedulerServiceClient<Channel>,
}

impl GrpcSchedulerClient {
    /// Connects to the scheduler gRPC endpoint.
    ///
    /// # Returns
    ///
    /// A new [`GrpcSchedulerClient`] connected to `endpoint` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::Transport`] if tonic cannot create or connect to the endpoint.
    pub async fn connect(endpoint: Endpoint) -> Result<Self, SchedulerError> {
        SchedulerServiceClient::connect(endpoint)
            .await
            .map(|client| Self { client })
            .map_err(to_transport_error)
    }
}

#[async_trait]
impl SchedulerClient for GrpcSchedulerClient {
    async fn next_task(
        &self,
        em_id: ExecutionManagerId,
    ) -> Result<SchedulerResponse, SchedulerError> {
        loop {
            let response = self
                .client
                .clone()
                .next_task(scheduler::NextTaskRequest {
                    execution_manager_id: em_id.get(),
                })
                .await
                .map_err(to_transport_error)?
                .into_inner();

            if let Some(assignment) = scheduler_response_to_result(response)? {
                return Ok(assignment);
            }
        }
    }
}

/// Converts a protobuf scheduler response into an optional scheduler assignment.
///
/// A `None` return means the scheduler long poll timed out without an assignment. The public
/// [`SchedulerClient`] implementation retries this case so callers keep the blocking trait
/// semantics.
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
/// * [`SchedulerError::Protocol`] if the response is missing a result or contains a malformed task
///   ID.
fn scheduler_response_to_result(
    response: NextTaskResponse,
) -> Result<Option<SchedulerResponse>, SchedulerError> {
    match response.result {
        Some(next_task_response::Result::Assignment(assignment)) => {
            let task_id = assignment
                .task_id
                .ok_or_else(|| SchedulerError::Protocol("assignment missing task ID".to_owned()))
                .and_then(|task_id| {
                    TaskId::try_from(task_id)
                        .map_err(|error| SchedulerError::Protocol(error.to_string()))
                })?;

            Ok(Some(SchedulerResponse {
                job_id: JobId::from(assignment.job_id),
                task_id,
                resource_group_id: ResourceGroupId::from(assignment.resource_group_id),
                session_id: assignment.session_id,
            }))
        }
        Some(next_task_response::Result::NoTask(common::Void {})) => Ok(None),
        None => Err(SchedulerError::Protocol(
            "next task response missing result".to_owned(),
        )),
    }
}

/// Converts a displayable transport-layer error into [`SchedulerError::Transport`].
///
/// # Returns
///
/// A [`SchedulerError::Transport`] containing `error`'s display string.
fn to_transport_error(error: impl std::fmt::Display) -> SchedulerError {
    SchedulerError::Transport(error.to_string())
}

#[cfg(test)]
mod tests {
    use spider_core::types::id::{JobId, ResourceGroupId, TaskId};
    use spider_proto_rust::{
        common,
        scheduler::{NextTaskResponse, SchedulerAssignment, next_task_response},
    };

    use super::scheduler_response_to_result;
    use crate::client::SchedulerResponse;

    #[test]
    fn scheduler_response_to_result_returns_assignment() {
        let response = NextTaskResponse {
            result: Some(next_task_response::Result::Assignment(
                SchedulerAssignment {
                    job_id: 11,
                    task_id: Some(common::TaskId::from(TaskId::Commit)),
                    resource_group_id: 13,
                    session_id: 17,
                },
            )),
        };

        let assignment = scheduler_response_to_result(response)
            .expect("scheduler response conversion should succeed")
            .expect("scheduler response should contain an assignment");

        assert_eq!(
            assignment,
            SchedulerResponse {
                job_id: JobId::from(11),
                task_id: TaskId::Commit,
                resource_group_id: ResourceGroupId::from(13),
                session_id: 17,
            }
        );
    }

    #[test]
    fn scheduler_response_to_result_rejects_missing_result() {
        let result = scheduler_response_to_result(NextTaskResponse { result: None });

        assert!(result.is_err());
    }

    #[test]
    fn scheduler_response_to_result_rejects_empty_assignment_task_id() {
        let response = NextTaskResponse {
            result: Some(next_task_response::Result::Assignment(
                SchedulerAssignment {
                    job_id: 11,
                    task_id: None,
                    resource_group_id: 13,
                    session_id: 17,
                },
            )),
        };

        let result = scheduler_response_to_result(response);

        assert!(result.is_err());
    }
}
