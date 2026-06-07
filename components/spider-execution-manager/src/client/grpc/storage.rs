//! gRPC-backed [`StorageClient`] implementation.
//!
//! Wraps the generated [`TaskInstanceManagementServiceClient`] and adapts its protobuf
//! request/response types to the transport-agnostic [`StorageClient`] trait.

use async_trait::async_trait;
use spider_core::types::{
    id::{ExecutionManagerId, JobId, SessionId, TaskId},
    io::ExecutionContext,
};
use spider_proto_rust::storage::{
    self,
    register_task_instance_response,
    storage_error,
    storage_operation_response,
    task_instance_management_service_client::TaskInstanceManagementServiceClient,
};
use tonic::transport::{Channel, Endpoint};

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
            .map(|inner| Self { client: inner })
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
            task_id: Some(storage::TaskId::from(task_id)),
            execution_manager_id: em_id.get(),
            session_id,
        };
        let response = self
            .client
            .clone()
            .register_task_instance(request)
            .await
            .map_err(to_transport_error)?
            .into_inner();

        match response.result {
            Some(register_task_instance_response::Result::ExecutionContext(bytes)) => {
                bincode::deserialize(&bytes).map_err(|error| {
                    StorageResponseError::Transport(format!(
                        "failed to decode execution context: {error}"
                    ))
                })
            }
            Some(register_task_instance_response::Result::Error(error)) => Err(error.into()),
            None => Err(StorageResponseError::Transport(
                "register task instance response missing result".to_owned(),
            )),
        }
    }

    async fn report_task_success(
        &self,
        job_id: JobId,
        task_id: TaskId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
        serialized_outputs: Option<Vec<u8>>,
    ) -> Result<(), StorageResponseError> {
        let request = storage::ReportTaskSuccessRequest {
            job_id: job_id.get(),
            task_id: Some(storage::TaskId::from(task_id)),
            execution_manager_id: em_id.get(),
            session_id,
            serialized_outputs: serialized_outputs.unwrap_or_default(),
        };
        let response = self
            .client
            .clone()
            .report_task_success(request)
            .await
            .map_err(to_transport_error)?
            .into_inner();

        storage_operation_response_to_result(response)
    }

    async fn report_task_failure(
        &self,
        job_id: JobId,
        task_id: TaskId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
        error_message: String,
    ) -> Result<(), StorageResponseError> {
        let request = storage::ReportTaskFailureRequest {
            job_id: job_id.get(),
            task_id: Some(storage::TaskId::from(task_id)),
            execution_manager_id: em_id.get(),
            session_id,
            error_message,
        };
        let response = self
            .client
            .clone()
            .report_task_failure(request)
            .await
            .map_err(to_transport_error)?
            .into_inner();

        storage_operation_response_to_result(response)
    }
}

impl From<storage::StorageError> for StorageResponseError {
    fn from(error: storage::StorageError) -> Self {
        match storage_error::ErrCode::try_from(error.err_code) {
            Ok(storage_error::ErrCode::StaleSession) => Self::StaleSession {
                storage_session: error.storage_session,
            },
            Ok(storage_error::ErrCode::CacheStale) => Self::CacheStale(error.message),
            Ok(storage_error::ErrCode::Transport) => Self::Transport(error.message),
            Ok(storage_error::ErrCode::Server | storage_error::ErrCode::Unspecified) => {
                Self::Server(error.message)
            }
            Ok(storage_error::ErrCode::InvalidInput) => Self::InvalidInput(error.message),
            Err(error) => Self::Transport(format!("unknown storage error kind: {error}")),
        }
    }
}

/// # Returns
///
/// [`storage::StorageOperationResponse`] converted into [`Result<(), StorageResponseError>`].
fn storage_operation_response_to_result(
    response: storage::StorageOperationResponse,
) -> Result<(), StorageResponseError> {
    match response.result {
        Some(storage_operation_response::Result::Ok(_)) => Ok(()),
        Some(storage_operation_response::Result::Error(error)) => Err(error.into()),
        None => Err(StorageResponseError::Transport(
            "storage operation response missing `result` message".to_owned(),
        )),
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
    fn storage_error_maps_stale_session() {
        let error = storage::StorageError {
            err_code: storage_error::ErrCode::StaleSession.into(),
            message: "stale".to_owned(),
            storage_session: 7,
        };

        match StorageResponseError::from(error) {
            StorageResponseError::StaleSession { storage_session } => {
                assert_eq!(7, storage_session);
            }
            error => panic!("unexpected storage response error: {error:?}"),
        }
    }

    #[test]
    fn storage_error_maps_unknown_kind_to_transport_error() {
        let error = storage::StorageError {
            err_code: 99,
            message: "unknown".to_owned(),
            storage_session: 0,
        };

        match StorageResponseError::from(error) {
            StorageResponseError::Transport(message) => {
                assert!(message.contains("unknown storage error kind"));
            }
            error => panic!("unexpected storage response error: {error:?}"),
        }
    }

    #[test]
    fn missing_storage_operation_result_is_transport_error() {
        match storage_operation_response_to_result(storage::StorageOperationResponse {
            result: None,
        }) {
            Err(StorageResponseError::Transport(_)) => {}
            result => panic!("unexpected storage operation result: {result:?}"),
        }
    }
}
