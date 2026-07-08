//! gRPC-backed [`StorageClient`] implementation.
//!
//! Wraps the generated [`TaskInstanceManagementServiceClient`] and adapts its protobuf
//! request/response types to the transport-agnostic [`StorageClient`] trait.

use std::num::NonZeroUsize;

use async_trait::async_trait;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::ExecutionContext;
use spider_proto_rust::common;
use spider_proto_rust::storage::TaskInstanceManagementServiceClient;
use spider_proto_rust::storage::{self};
use spider_utils::grpc::client::ConnectionPool;
use tonic::Code;
use tonic::Status;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use crate::client::storage::StorageClient;
use crate::client::storage::StorageResponseError;

/// gRPC-backed [`StorageClient`] implementation.
#[derive(Debug, Clone)]
pub struct GrpcStorageClient {
    connection_pool: ConnectionPool<TaskInstanceManagementServiceClient<Channel>>,
}

impl GrpcStorageClient {
    /// Connects a pool of `pool_size` connections to the storage gRPC endpoint.
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
    pub async fn connect(
        endpoint: Endpoint,
        pool_size: NonZeroUsize,
    ) -> Result<Self, StorageResponseError> {
        let connection_pool = ConnectionPool::connect(endpoint, pool_size, |channel| {
            TaskInstanceManagementServiceClient::new(channel)
        })
        .await
        .map_err(to_transport_error)?;

        Ok(Self { connection_pool })
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
            .connection_pool
            .get_client()
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
        self.connection_pool
            .get_client()
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
        self.connection_pool
            .get_client()
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
/// * [`StorageResponseError::StaleSession`] for `NOT_FOUND`.
/// * [`StorageResponseError::CacheStale`] for `FAILED_PRECONDITION`.
/// * [`StorageResponseError::InvalidInput`] for `INVALID_ARGUMENT`.
/// * [`StorageResponseError::Transport`] for `UNAVAILABLE` (a lost or unestablished connection).
/// * [`StorageResponseError::Server`] for any other code.
fn status_to_error(status: &Status) -> StorageResponseError {
    match status.code() {
        Code::NotFound => StorageResponseError::StaleSession(status.message().to_owned()),
        Code::FailedPrecondition => StorageResponseError::CacheStale(status.message().to_owned()),
        Code::InvalidArgument => StorageResponseError::InvalidInput(status.message().to_owned()),
        Code::Unavailable => to_transport_error(status.message()),
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
    fn status_maps_not_found_to_stale_session() {
        match status_to_error(&Status::not_found("storage session is 9")) {
            StorageResponseError::StaleSession(message) => assert!(message.contains('9')),
            error => panic!("unexpected error: {error:?}"),
        }
    }

    #[test]
    fn status_maps_unavailable_to_transport() {
        const MESSAGE: &str = "connection lost";
        match status_to_error(&Status::unavailable(MESSAGE)) {
            StorageResponseError::Transport(message) => {
                assert!(message.contains(MESSAGE));
            }
            error => panic!("unexpected error: {error:?}"),
        }
    }

    #[test]
    fn status_maps_invalid_argument_to_invalid_input() {
        const MESSAGE: &str = "bad task id";
        match status_to_error(&Status::invalid_argument(MESSAGE)) {
            StorageResponseError::InvalidInput(message) => assert!(message.contains(MESSAGE)),
            error => panic!("unexpected error: {error:?}"),
        }
    }
}
