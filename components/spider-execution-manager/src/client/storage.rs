//! Storage client trait.
//!
//! The execution manager interacts with the storage server through this trait to register a task
//! instance, fetch its [`ExecutionContext`], and report success or failure.

use async_trait::async_trait;
use spider_core::types::{
    id::{ExecutionManagerId, JobId, SessionId, TaskId},
    io::ExecutionContext,
};
use spider_proto_rust::{
    id::id_to_bytes,
    storage::{
        self,
        execution_manager_storage_client::ExecutionManagerStorageClient,
        register_task_instance_response,
        report_task_success_request,
        storage_error,
        storage_operation_response,
    },
};
use tonic::transport::{Channel, Endpoint};

/// Errors returned by [`StorageClient`] operations.
///
/// The variants intentionally mirror the storage server's externally visible failure modes (see
/// `spider_storage::state::error::StorageServerError`) plus a transport bucket for connection /
/// serialization failures.
#[derive(Debug, thiserror::Error)]
pub enum StorageResponseError {
    /// The `session_id` carried with the request does not match storage's current session.
    #[error("stale session (storage now at {storage_session})")]
    StaleSession { storage_session: SessionId },

    /// Storage's job cache rejected the operation as stale (e.g. the task or its job has already
    /// terminated).
    #[error("cache stale: {0}")]
    CacheStale(String),

    /// Connection lost, request timeout, or wire-format serialization failure. Callers may back off
    /// and retry.
    #[error("transport error: {0}")]
    Transport(String),

    /// The storage server returned an otherwise-uncategorized error.
    #[error("storage server: {0}")]
    Server(String),

    /// The input to the operation is invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

/// Client interface to the storage server.
#[async_trait]
pub trait StorageClient: Send + Sync {
    /// Registers a task instance and fetches its execution context.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The owning job.
    /// * `task_id` - The task being instantiated.
    /// * `em_id` - The identity of the calling execution manager.
    /// * `session_id` - The session id captured from the scheduler assignment, pinned for the
    ///   lifetime of the attempt.
    ///
    /// # Returns
    ///
    /// The [`ExecutionContext`] for the task instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageResponseError::StaleSession`] if `session_id` no longer matches storage's current
    ///   session.
    /// * [`StorageResponseError::CacheStale`] if storage's job cache rejected the registration.
    /// * [`StorageResponseError::Transport`] if the connection was lost or timed out.
    /// * [`StorageResponseError::Server`] if storage returned an otherwise-uncategorized error.
    async fn register_task_instance(
        &self,
        job_id: JobId,
        task_id: TaskId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
    ) -> Result<ExecutionContext, StorageResponseError>;

    /// Reports successful execution of a task instance.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The owning job.
    /// * `task_id` - The task that ran.
    /// * `em_id` - The identity of the calling execution manager.
    /// * `session_id` - The session id captured from the scheduler assignment.
    /// * `serialized_outputs` - The wire-format encoded task outputs buffer, forwarded verbatim to
    ///   storage. For commit tasks and cleanup tasks, this must be `None`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageResponseError::StaleSession`] if `session_id` no longer matches storage's current
    ///   session.
    /// * [`StorageResponseError::CacheStale`] if storage's job cache rejected the report.
    /// * [`StorageResponseError::Transport`] if the connection was lost or timed out.
    /// * [`StorageResponseError::Server`] if storage returned an otherwise-uncategorized error.
    /// * [`StorageResponseError::InvalidInput`] if `serialized_outputs` is `Some` for a commit or
    ///   cleanup task.
    async fn report_task_success(
        &self,
        job_id: JobId,
        task_id: TaskId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
        serialized_outputs: Option<Vec<u8>>,
    ) -> Result<(), StorageResponseError>;

    /// Reports failed execution of a task instance.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The owning job.
    /// * `task_id` - The task that ran.
    /// * `em_id` - The identity of the calling execution manager.
    /// * `session_id` - The session id captured from the scheduler assignment.
    /// * `error_message` - The formatted error message.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageResponseError::StaleSession`] if `session_id` no longer matches storage's current
    ///   session.
    /// * [`StorageResponseError::CacheStale`] if storage's job cache rejected the report.
    /// * [`StorageResponseError::Transport`] if the connection was lost or timed out.
    /// * [`StorageResponseError::Server`] if storage returned an otherwise-uncategorized error.
    async fn report_task_failure(
        &self,
        job_id: JobId,
        task_id: TaskId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
        error_message: String,
    ) -> Result<(), StorageResponseError>;
}

/// gRPC-backed [`StorageClient`] implementation.
#[derive(Debug, Clone)]
pub struct GrpcStorageClient {
    inner: ExecutionManagerStorageClient<Channel>,
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
        ExecutionManagerStorageClient::connect(endpoint)
            .await
            .map(|inner| Self { inner })
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
            job_id: id_to_bytes(&job_id),
            task_id: id_to_bytes(&task_id),
            execution_manager_id: id_to_bytes(&em_id),
            session_id,
        };
        let response = {
            let mut client = self.inner.clone();
            client
                .register_task_instance(request)
                .await
                .map_err(to_transport_error)?
                .into_inner()
        };

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
            job_id: id_to_bytes(&job_id),
            task_id: id_to_bytes(&task_id),
            execution_manager_id: id_to_bytes(&em_id),
            session_id,
            output_payload: serialized_outputs
                .map(report_task_success_request::OutputPayload::SerializedOutputs),
        };
        let response = {
            let mut client = self.inner.clone();
            client
                .report_task_success(request)
                .await
                .map_err(to_transport_error)?
                .into_inner()
        };

        storage_operation_response_to_result(
            response,
            "report task success response missing result",
        )
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
            job_id: id_to_bytes(&job_id),
            task_id: id_to_bytes(&task_id),
            execution_manager_id: id_to_bytes(&em_id),
            session_id,
            error_message,
        };
        let response = {
            let mut client = self.inner.clone();
            client
                .report_task_failure(request)
                .await
                .map_err(to_transport_error)?
                .into_inner()
        };

        storage_operation_response_to_result(
            response,
            "report task failure response missing result",
        )
    }
}

impl From<storage::StorageError> for StorageResponseError {
    fn from(error: storage::StorageError) -> Self {
        match storage_error::Kind::try_from(error.kind) {
            Ok(storage_error::Kind::StaleSession) => Self::StaleSession {
                storage_session: error.storage_session,
            },
            Ok(storage_error::Kind::CacheStale) => Self::CacheStale(error.message),
            Ok(storage_error::Kind::Transport) => Self::Transport(error.message),
            Ok(storage_error::Kind::Server | storage_error::Kind::Unspecified) => {
                Self::Server(error.message)
            }
            Ok(storage_error::Kind::InvalidInput) => Self::InvalidInput(error.message),
            Err(error) => Self::Transport(format!("unknown storage error kind: {error}")),
        }
    }
}

/// Converts a protobuf storage operation response into the local empty success/error result.
///
/// # Returns
///
/// `Ok(())` when the protobuf response contains an `ok` result on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`StorageResponseError::Transport`] if the response does not contain a result.
/// * Forwards [`StorageResponseError::from`]'s return values on failure.
fn storage_operation_response_to_result(
    response: storage::StorageOperationResponse,
    missing_result_message: &'static str,
) -> Result<(), StorageResponseError> {
    match response.result {
        Some(storage_operation_response::Result::Ok(_)) => Ok(()),
        Some(storage_operation_response::Result::Error(error)) => Err(error.into()),
        None => Err(StorageResponseError::Transport(
            missing_result_message.to_owned(),
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
            kind: storage_error::Kind::StaleSession.into(),
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
            kind: 99,
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
        match storage_operation_response_to_result(
            storage::StorageOperationResponse { result: None },
            "missing result",
        ) {
            Err(StorageResponseError::Transport(message)) => {
                assert_eq!("missing result", message);
            }
            result => panic!("unexpected storage operation result: {result:?}"),
        }
    }
}
