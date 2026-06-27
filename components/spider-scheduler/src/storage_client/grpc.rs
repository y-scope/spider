//! gRPC-backed [`SchedulerStorageClient`] implementation.

use std::{num::NonZeroUsize, time::Duration};

use async_trait::async_trait;
use spider_core::{
    job::JobState,
    types::id::{JobId, ResourceGroupId, SessionId, TaskId},
};
use spider_proto_rust::storage::{
    self,
    inbound_queue_service_client::InboundQueueServiceClient,
    job_orchestration_service_client::JobOrchestrationServiceClient,
};
use spider_utils::grpc::client::ConnectionPool;
use tonic::{
    Code,
    Status,
    transport::{Channel, Endpoint},
};

use crate::{
    error::StorageClientError,
    storage_client::SchedulerStorageClient,
    types::InboundEntry,
};

/// gRPC-backed [`SchedulerStorageClient`] implementation.
#[derive(Debug, Clone)]
pub struct GrpcSchedulerStorageClient {
    inbound_queue_connection_pool: ConnectionPool<InboundQueueServiceClient<Channel>>,
    job_orchestration_connection_pool: ConnectionPool<JobOrchestrationServiceClient<Channel>>,
}

impl GrpcSchedulerStorageClient {
    /// Connects pools of `pool_size` connections to the storage gRPC endpoint.
    ///
    /// # Returns
    ///
    /// A new [`GrpcSchedulerStorageClient`] connected to `endpoint` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageClientError::Transport`] if tonic cannot create or connect to the endpoint.
    pub async fn connect(
        endpoint: Endpoint,
        pool_size: NonZeroUsize,
    ) -> Result<Self, StorageClientError> {
        let inbound_queue_connection_pool =
            ConnectionPool::connect(endpoint.clone(), pool_size, |channel| {
                InboundQueueServiceClient::new(channel)
            })
            .await
            .map_err(to_transport_error)?;
        let job_orchestration_connection_pool =
            ConnectionPool::connect(endpoint, pool_size, |channel| {
                JobOrchestrationServiceClient::new(channel)
            })
            .await
            .map_err(to_transport_error)?;

        Ok(Self {
            inbound_queue_connection_pool,
            job_orchestration_connection_pool,
        })
    }
}

#[async_trait]
impl SchedulerStorageClient for GrpcSchedulerStorageClient {
    async fn poll_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
        let request = poll_ready_tasks_request(max_items, wait)?;
        let response = self
            .inbound_queue_connection_pool
            .get_client()
            .poll_ready_tasks(request)
            .await
            .map_err(|status| inbound_status_to_error(&status))?
            .into_inner();
        poll_ready_tasks_response_to_result(response)
    }

    async fn poll_commit_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
        let request = poll_ready_tasks_request(max_items, wait)?;
        let response = self
            .inbound_queue_connection_pool
            .get_client()
            .poll_ready_commit_tasks(request)
            .await
            .map_err(|status| inbound_status_to_error(&status))?
            .into_inner();
        poll_ready_tasks_response_to_result(response)
    }

    async fn poll_cleanup_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
        let request = poll_ready_tasks_request(max_items, wait)?;
        let response = self
            .inbound_queue_connection_pool
            .get_client()
            .poll_ready_cleanup_tasks(request)
            .await
            .map_err(|status| inbound_status_to_error(&status))?
            .into_inner();
        poll_ready_tasks_response_to_result(response)
    }

    async fn job_state(&self, job_id: JobId) -> Result<JobState, StorageClientError> {
        let request = storage::JobIdRequest {
            job_id: job_id.get(),
        };
        let response = self
            .job_orchestration_connection_pool
            .get_client()
            .get_job_state(request)
            .await
            .map_err(|status| match status.code() {
                Code::NotFound => StorageClientError::JobNotFound(job_id),
                _ => StorageClientError::Server(status.message().to_owned()),
            })?
            .into_inner();
        job_state_response_to_result(response)
    }

    async fn resend_ready_tasks(&self) -> Result<(), StorageClientError> {
        self.inbound_queue_connection_pool
            .get_client()
            .resend_ready_tasks(storage::ResendReadyTasksRequest {})
            .await
            .map_err(|status| inbound_status_to_error(&status))?;
        Ok(())
    }
}

/// Maps an inbound-queue gRPC [`Status`] to a [`StorageClientError`].
///
/// # Returns
///
/// The [`StorageClientError`] for `status`'s code:
///
/// * [`StorageClientError::InboundClosed`] for `UNAVAILABLE`.
/// * [`StorageClientError::InvalidInput`] for `INVALID_ARGUMENT`.
/// * [`StorageClientError::Server`] for any other code.
fn inbound_status_to_error(status: &Status) -> StorageClientError {
    match status.code() {
        Code::Unavailable => StorageClientError::InboundClosed,
        Code::InvalidArgument => to_invalid_input_error(status.message()),
        _ => StorageClientError::Server(status.message().to_owned()),
    }
}

/// # Returns
///
/// A [`storage::PollReadyTasksRequest`] carrying `max_items` and `wait` on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`StorageClientError::InvalidInput`] if either value cannot fit in the protobuf field type.
fn poll_ready_tasks_request(
    max_items: usize,
    wait: Duration,
) -> Result<storage::PollReadyTasksRequest, StorageClientError> {
    Ok(storage::PollReadyTasksRequest {
        max_items: u64::try_from(max_items).map_err(to_invalid_input_error)?,
        wait_ms: u64::try_from(wait.as_millis()).map_err(to_invalid_input_error)?,
    })
}

/// # Returns
///
/// [`storage::PollReadyTasksResponse`] converted into
/// [`Result<(SessionId, Vec<InboundEntry>), StorageClientError>`].
fn poll_ready_tasks_response_to_result(
    response: storage::PollReadyTasksResponse,
) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
    let tasks = response.tasks.ok_or_else(|| {
        StorageClientError::Transport(
            "poll ready tasks response missing `tasks` message".to_owned(),
        )
    })?;
    ready_tasks_to_result(tasks)
}

/// # Returns
///
/// [`storage::ReadyTasks`] converted into
/// [`Result<(SessionId, Vec<InboundEntry>), StorageClientError>`].
fn ready_tasks_to_result(
    tasks: storage::ReadyTasks,
) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
    let session_id = tasks.session_id;
    let entries = tasks
        .tasks
        .into_iter()
        .map(ready_task_to_inbound_entry)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((session_id, entries))
}

/// # Returns
///
/// `task` converted into an [`InboundEntry`] on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`StorageClientError::Transport`] if `task` is missing or has an invalid task ID.
fn ready_task_to_inbound_entry(
    task: storage::ReadyTask,
) -> Result<InboundEntry, StorageClientError> {
    let task_id = task
        .task_id
        .ok_or_else(|| StorageClientError::Transport("ready task missing task ID".to_owned()))
        .and_then(|task_id| {
            TaskId::try_from(task_id)
                .map_err(|error| StorageClientError::Transport(error.to_string()))
        })?;
    Ok(InboundEntry {
        resource_group_id: ResourceGroupId::from(task.resource_group_id),
        job_id: JobId::from(task.job_id),
        task_id,
    })
}

/// # Returns
///
/// [`storage::JobStateResponse`] converted into a [`JobState`] on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`StorageClientError::Transport`] if the response carries an unrecognized job state.
fn job_state_response_to_result(
    response: storage::JobStateResponse,
) -> Result<JobState, StorageClientError> {
    let proto_state = storage::JobState::try_from(response.state)
        .map_err(|error| StorageClientError::Transport(error.to_string()))?;
    JobState::try_from(proto_state)
        .map_err(|error| StorageClientError::Transport(error.to_string()))
}

/// Converts a displayable transport-layer error into [`StorageClientError::Transport`].
///
/// # Returns
///
/// A [`StorageClientError::Transport`] containing `error`'s display string.
fn to_transport_error(error: impl std::fmt::Display) -> StorageClientError {
    StorageClientError::Transport(error.to_string())
}

/// Converts a displayable out-of-range error into [`StorageClientError::InvalidInput`].
///
/// # Returns
///
/// A [`StorageClientError::InvalidInput`] containing `error`'s display string.
fn to_invalid_input_error(error: impl std::fmt::Display) -> StorageClientError {
    StorageClientError::InvalidInput(error.to_string())
}

#[cfg(test)]
mod tests {
    use spider_core::types::id::{JobId, ResourceGroupId, TaskId};
    use spider_proto_rust::{common, storage};

    use super::*;

    const SESSION_ID: SessionId = 11;
    const RESOURCE_GROUP_ID: u64 = 3;
    const JOB_ID: u64 = 5;
    const TASK_INDEX: usize = 7;

    #[test]
    fn poll_ready_tasks_response_converts_entries() {
        let response = storage::PollReadyTasksResponse {
            tasks: Some(storage::ReadyTasks {
                session_id: SESSION_ID,
                tasks: vec![storage::ReadyTask {
                    resource_group_id: RESOURCE_GROUP_ID,
                    job_id: JOB_ID,
                    task_id: Some(common::TaskId::from(TaskId::Index(TASK_INDEX))),
                }],
            }),
        };

        let (session_id, entries) = poll_ready_tasks_response_to_result(response)
            .expect("poll response conversion should succeed");

        assert_eq!(session_id, SESSION_ID);
        assert_eq!(
            entries,
            vec![InboundEntry {
                resource_group_id: ResourceGroupId::from(RESOURCE_GROUP_ID),
                job_id: JobId::from(JOB_ID),
                task_id: TaskId::Index(TASK_INDEX),
            }]
        );
    }

    #[test]
    fn poll_ready_tasks_response_rejects_missing_task_id() {
        const MISSING_TASK_ID_MESSAGE: &str = "missing task ID";

        let response = storage::PollReadyTasksResponse {
            tasks: Some(storage::ReadyTasks {
                session_id: SESSION_ID,
                tasks: vec![storage::ReadyTask {
                    resource_group_id: RESOURCE_GROUP_ID,
                    job_id: JOB_ID,
                    task_id: None,
                }],
            }),
        };

        match poll_ready_tasks_response_to_result(response) {
            Err(StorageClientError::Transport(message)) => {
                assert!(message.contains(MISSING_TASK_ID_MESSAGE));
            }
            result => panic!("unexpected poll response conversion result: {result:?}"),
        }
    }

    #[test]
    fn poll_ready_tasks_response_rejects_missing_tasks() {
        let response = storage::PollReadyTasksResponse { tasks: None };

        assert!(matches!(
            poll_ready_tasks_response_to_result(response),
            Err(StorageClientError::Transport(_))
        ));
    }

    #[test]
    fn inbound_status_maps_unavailable_to_inbound_closed() {
        let status = tonic::Status::unavailable("inbound queue is closed");

        assert!(matches!(
            inbound_status_to_error(&status),
            StorageClientError::InboundClosed
        ));
    }

    #[test]
    fn inbound_status_maps_invalid_argument_to_invalid_input() {
        const MESSAGE: &str = "bad max_items";
        let status = tonic::Status::invalid_argument(MESSAGE);

        match inbound_status_to_error(&status) {
            StorageClientError::InvalidInput(message) => assert_eq!(message, MESSAGE),
            error => panic!("unexpected inbound status mapping: {error:?}"),
        }
    }

    #[test]
    fn inbound_status_maps_other_codes_to_server() {
        let status = tonic::Status::internal("boom");

        assert!(matches!(
            inbound_status_to_error(&status),
            StorageClientError::Server(_)
        ));
    }
}
