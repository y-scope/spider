//! gRPC-backed [`StorageClient`] implementation.
//!
//! Wraps the generated [`TaskInstanceManagementServiceClient`] and adapts its protobuf
//! request/response types to the transport-agnostic [`StorageClient`] trait.

use async_trait::async_trait;
use spider_core::types::{
    id::{ExecutionManagerId, JobId, SessionId, TaskId, TaskInstanceId},
    io::ExecutionContext,
};
use spider_proto_rust::{
    common,
    storage::{self, task_instance_management_service_client::TaskInstanceManagementServiceClient},
};
use tonic::{
    Code,
    Status,
    transport::{Channel, Endpoint},
};

use crate::client::storage::{StorageClient, StorageResponseError};

/// gRPC-backed [`StorageClient`] implementation.
#[derive(Debug, Clone)]
pub struct GrpcStorageClient {
    client: TaskInstanceManagementServiceClient<Channel>,
}

impl GrpcStorageClient {
    /// Connects to the storage gRPC endpoint.
    ///
    /// # Returns
    ///
    /// A new [`GrpcStorageClient`] connected to `endpoint` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageResponseError::Transport`] if tonic cannot create or connect to the endpoint.
    pub async fn connect(endpoint: Endpoint) -> Result<Self, StorageResponseError> {
        TaskInstanceManagementServiceClient::connect(endpoint)
            .await
            .map(|client| Self { client })
            .map_err(to_transport_error)
    }
}

#[async_trait]
impl StorageClient for GrpcStorageClient {
    async fn register_task_instance(
        &self,
        job_id: JobId,
        task_id: TaskId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
    ) -> Result<ExecutionContext, StorageResponseError> {
        let request = storage::RegisterTaskInstanceRequest {
            job_id: job_id.get(),
            task_id: Some(common::TaskId::from(task_id)),
            execution_manager_id: em_id.get(),
            session_id,
        };
        let response = self
            .client
            .clone()
            .register_task_instance(request)
            .await
            .map_err(|status| status_to_error(&status))?
            .into_inner();

        let execution_context = response.execution_context.ok_or_else(|| {
            StorageResponseError::Transport(
                "register task instance response missing execution context".to_owned(),
            )
        })?;
        ExecutionContext::try_from(execution_context)
            .map_err(|error| StorageResponseError::Transport(error.to_string()))
    }

    async fn report_task_success(
        &self,
        job_id: JobId,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
        serialized_outputs: Option<Vec<u8>>,
    ) -> Result<(), StorageResponseError> {
        let request = storage::ReportTaskSuccessRequest {
            job_id: job_id.get(),
            task_id: Some(common::TaskId::from(task_id)),
            execution_manager_id: em_id.get(),
            session_id,
            serialized_outputs: serialized_outputs.unwrap_or_default(),
            task_instance_id,
        };
        self.client
            .clone()
            .report_task_success(request)
            .await
            .map_err(|status| status_to_error(&status))?;
        Ok(())
    }

    async fn report_task_failure(
        &self,
        job_id: JobId,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
        error_message: String,
    ) -> Result<(), StorageResponseError> {
        let request = storage::ReportTaskFailureRequest {
            job_id: job_id.get(),
            task_id: Some(common::TaskId::from(task_id)),
            execution_manager_id: em_id.get(),
            session_id,
            error_message,
            task_instance_id,
        };
        self.client
            .clone()
            .report_task_failure(request)
            .await
            .map_err(|status| status_to_error(&status))?;
        Ok(())
    }
}

/// Maps a task-instance management gRPC [`Status`] to a [`StorageResponseError`].
///
/// # Returns
///
/// The [`StorageResponseError`] for `status`'s code:
///
/// * [`StorageResponseError::StaleSession`] for `UNAVAILABLE`.
/// * [`StorageResponseError::CacheStale`] for `FAILED_PRECONDITION`.
/// * [`StorageResponseError::JobGone`] for `NOT_FOUND`.
/// * [`StorageResponseError::InvalidInput`] for `INVALID_ARGUMENT`.
/// * [`StorageResponseError::Server`] for any other code.
fn status_to_error(status: &Status) -> StorageResponseError {
    match status.code() {
        Code::Unavailable => StorageResponseError::StaleSession(status.message().to_owned()),
        Code::FailedPrecondition => StorageResponseError::CacheStale(status.message().to_owned()),
        Code::NotFound => StorageResponseError::JobGone(status.message().to_owned()),
        Code::InvalidArgument => StorageResponseError::InvalidInput(status.message().to_owned()),
        _ => StorageResponseError::Server(status.message().to_owned()),
    }
}

/// Converts a displayable transport-layer error into [`StorageResponseError::Transport`].
///
/// # Returns
///
/// A [`StorageResponseError::Transport`] containing `error`'s display string.
fn to_transport_error(error: impl std::fmt::Display) -> StorageResponseError {
    StorageResponseError::Transport(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_maps_unavailable_to_stale_session() {
        match status_to_error(&Status::unavailable("storage session is 9")) {
            StorageResponseError::StaleSession(message) => assert!(message.contains('9')),
            error => panic!("unexpected error: {error:?}"),
        }
    }

    #[test]
    fn status_maps_invalid_argument_to_invalid_input() {
        match status_to_error(&Status::invalid_argument("bad task id")) {
            StorageResponseError::InvalidInput(message) => assert!(message.contains("bad task id")),
            error => panic!("unexpected error: {error:?}"),
        }
    }

    #[test]
    fn status_maps_not_found_to_job_gone() {
        match status_to_error(&Status::not_found("job 7 is gone")) {
            StorageResponseError::JobGone(message) => {
                assert!(message.contains("job 7 is gone"), "message: {message}");
            }
            error => panic!("unexpected error: {error:?}"),
        }
    }
}
