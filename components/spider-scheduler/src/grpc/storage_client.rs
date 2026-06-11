//! gRPC-backed [`SchedulerStorageClient`] implementation.

use std::time::Duration;

use async_trait::async_trait;
use spider_core::{
    job::JobState,
    session::SessionTracker,
    types::id::{JobId, ResourceGroupId, SessionId, TaskId},
};
use spider_proto_rust::storage::{
    self,
    job_management_error,
    job_management_service_client::JobManagementServiceClient,
    job_state_response,
    poll_ready_tasks_response,
    scheduler_storage_error,
    scheduler_storage_service_client::SchedulerStorageServiceClient,
    session_management_service_client::SessionManagementServiceClient,
};
use tonic::transport::{Channel, Endpoint};

use crate::{
    error::StorageClientError,
    storage_client::SchedulerStorageClient,
    types::InboundEntry,
};

/// gRPC-backed [`SchedulerStorageClient`] implementation.
#[derive(Debug, Clone)]
pub struct GrpcSchedulerStorageClient {
    scheduler_client: SchedulerStorageServiceClient<Channel>,
    job_client: JobManagementServiceClient<Channel>,
    session_tracker: SessionTracker,
}

impl GrpcSchedulerStorageClient {
    /// Connects to the storage gRPC endpoint.
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
    /// * Forwards [`Self::get_initial_session`]'s return values on failure.
    pub async fn connect(endpoint: Endpoint) -> Result<Self, StorageClientError> {
        let channel = endpoint.connect().await.map_err(to_transport_error)?;
        let session_id = Self::get_initial_session(channel.clone()).await?;

        Ok(Self {
            scheduler_client: SchedulerStorageServiceClient::new(channel.clone()),
            job_client: JobManagementServiceClient::new(channel),
            session_tracker: SessionTracker::new(session_id),
        })
    }

    /// Fetches storage's current session ID.
    ///
    /// # Returns
    ///
    /// The current storage session ID on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageClientError::Transport`] if the request fails.
    async fn get_initial_session(channel: Channel) -> Result<SessionId, StorageClientError> {
        SessionManagementServiceClient::new(channel)
            .get_session(storage::Void {})
            .await
            .map(|response| response.into_inner().session_id)
            .map_err(to_transport_error)
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
            .scheduler_client
            .clone()
            .poll_ready_tasks(request)
            .await
            .map_err(to_transport_error)?
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
            .scheduler_client
            .clone()
            .poll_ready_commit_tasks(request)
            .await
            .map_err(to_transport_error)?
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
            .scheduler_client
            .clone()
            .poll_ready_cleanup_tasks(request)
            .await
            .map_err(to_transport_error)?
            .into_inner();
        poll_ready_tasks_response_to_result(response)
    }

    async fn job_state(&self, job_id: JobId) -> Result<JobState, StorageClientError> {
        let request = storage::JobIdRequest {
            job_id: job_id.get(),
            session_id: self.session_tracker.current(),
        };
        let response = self
            .job_client
            .clone()
            .get_job_state(request)
            .await
            .map_err(to_transport_error)?
            .into_inner();
        job_state_response_to_result(response, job_id)
    }
}

impl From<storage::SchedulerStorageError> for StorageClientError {
    fn from(error: storage::SchedulerStorageError) -> Self {
        scheduler_storage_error_to_client_error(error)
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
/// * [`StorageClientError::Transport`] if either value cannot fit in the protobuf field type.
fn poll_ready_tasks_request(
    max_items: usize,
    wait: Duration,
) -> Result<storage::PollReadyTasksRequest, StorageClientError> {
    Ok(storage::PollReadyTasksRequest {
        max_items: u64::try_from(max_items).map_err(to_transport_error)?,
        wait_ns: u64::try_from(wait.as_nanos()).map_err(to_transport_error)?,
    })
}

/// # Returns
///
/// [`storage::PollReadyTasksResponse`] converted into scheduler entries on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`StorageClientError::Transport`] if the response is malformed.
/// * Forwards [`StorageClientError::from`]'s return values on failure.
fn poll_ready_tasks_response_to_result(
    response: storage::PollReadyTasksResponse,
) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
    match response.result {
        Some(poll_ready_tasks_response::Result::Tasks(tasks)) => ready_tasks_to_result(tasks),
        Some(poll_ready_tasks_response::Result::Error(error)) => Err(error.into()),
        None => Err(StorageClientError::Transport(
            "poll ready tasks response missing `result` message".to_owned(),
        )),
    }
}

/// # Returns
///
/// [`storage::ReadyTasks`] converted into scheduler entries on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`StorageClientError::Transport`] if a ready task is missing or has an invalid task ID.
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
        .and_then(|task_id| TaskId::try_from(task_id).map_err(StorageClientError::Transport))?;
    Ok(InboundEntry {
        resource_group_id: ResourceGroupId::from(task.resource_group_id),
        job_id: JobId::from(task.job_id),
        task_id,
    })
}

/// # Returns
///
/// [`storage::JobStateResponse`] converted into [`JobState`] on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`StorageClientError::Transport`] if the response is malformed.
/// * Forwards [`StorageClientError::from`]'s return values on failure.
fn job_state_response_to_result(
    response: storage::JobStateResponse,
    job_id: JobId,
) -> Result<JobState, StorageClientError> {
    match response.result {
        Some(job_state_response::Result::State(state)) => storage::JobState::try_from(state)
            .map_err(|error| StorageClientError::Transport(error.to_string()))
            .and_then(|state| JobState::try_from(state).map_err(StorageClientError::Transport)),
        Some(job_state_response::Result::Error(error)) => {
            Err(job_management_error_to_client_error(error, job_id))
        }
        None => Err(StorageClientError::Transport(
            "job state response missing `result` message".to_owned(),
        )),
    }
}

/// # Returns
///
/// [`storage::SchedulerStorageError`] converted into [`StorageClientError`].
fn scheduler_storage_error_to_client_error(
    error: storage::SchedulerStorageError,
) -> StorageClientError {
    match scheduler_storage_error::ErrCode::try_from(error.err_code) {
        Ok(scheduler_storage_error::ErrCode::InboundClosed) => StorageClientError::InboundClosed,
        Ok(scheduler_storage_error::ErrCode::InvalidInput) => {
            StorageClientError::InvalidInput(error.message)
        }
        Ok(
            scheduler_storage_error::ErrCode::Server
            | scheduler_storage_error::ErrCode::Unspecified,
        ) => StorageClientError::Server(error.message),
        Err(error) => {
            StorageClientError::Transport(format!("unknown scheduler storage error kind: {error}"))
        }
    }
}

/// # Returns
///
/// [`storage::JobManagementError`] converted into [`StorageClientError`].
fn job_management_error_to_client_error(
    error: storage::JobManagementError,
    requested_job_id: JobId,
) -> StorageClientError {
    match job_management_error::ErrCode::try_from(error.err_code) {
        Ok(job_management_error::ErrCode::JobNotFound) => {
            StorageClientError::JobNotFound(requested_job_id)
        }
        Ok(job_management_error::ErrCode::StaleSession) => StorageClientError::StaleSession {
            storage_session: error.storage_session,
        },
        Ok(job_management_error::ErrCode::InvalidInput) => {
            StorageClientError::InvalidInput(error.message)
        }
        Ok(job_management_error::ErrCode::Server | job_management_error::ErrCode::Unspecified) => {
            StorageClientError::Server(error.message)
        }
        Err(error) => {
            StorageClientError::Transport(format!("unknown job management error kind: {error}"))
        }
    }
}

/// Converts a displayable transport-layer error into [`StorageClientError::Transport`].
///
/// # Returns
///
/// A [`StorageClientError::Transport`] containing `error`'s display string.
fn to_transport_error(error: impl std::fmt::Display) -> StorageClientError {
    StorageClientError::Transport(error.to_string())
}

#[cfg(test)]
mod tests {
    use spider_core::types::id::{JobId, ResourceGroupId, TaskId};
    use spider_proto_rust::storage::{self, poll_ready_tasks_response, scheduler_storage_error};

    use super::*;

    #[test]
    fn poll_ready_tasks_response_converts_entries() {
        let response = storage::PollReadyTasksResponse {
            result: Some(poll_ready_tasks_response::Result::Tasks(
                storage::ReadyTasks {
                    session_id: 11,
                    tasks: vec![storage::ReadyTask {
                        resource_group_id: 3,
                        job_id: 5,
                        task_id: Some(storage::TaskId::from(TaskId::Index(7))),
                    }],
                },
            )),
        };

        let (session_id, entries) = poll_ready_tasks_response_to_result(response)
            .expect("poll response conversion should succeed");

        assert_eq!(session_id, 11);
        assert_eq!(
            entries,
            vec![InboundEntry {
                resource_group_id: ResourceGroupId::from(3),
                job_id: JobId::from(5),
                task_id: TaskId::Index(7),
            }]
        );
    }

    #[test]
    fn poll_ready_tasks_response_rejects_missing_task_id() {
        let response = storage::PollReadyTasksResponse {
            result: Some(poll_ready_tasks_response::Result::Tasks(
                storage::ReadyTasks {
                    session_id: 11,
                    tasks: vec![storage::ReadyTask {
                        resource_group_id: 3,
                        job_id: 5,
                        task_id: None,
                    }],
                },
            )),
        };

        match poll_ready_tasks_response_to_result(response) {
            Err(StorageClientError::Transport(message)) => {
                assert!(message.contains("missing task ID"));
            }
            result => panic!("unexpected poll response conversion result: {result:?}"),
        }
    }

    #[test]
    fn scheduler_storage_error_maps_inbound_closed() {
        let error = storage::SchedulerStorageError {
            err_code: scheduler_storage_error::ErrCode::InboundClosed.into(),
            message: "closed".to_owned(),
        };

        assert!(matches!(
            StorageClientError::from(error),
            StorageClientError::InboundClosed
        ));
    }
}
