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
use crate::client::storage::TaskSuccessPayload;

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
        payload: TaskSuccessPayload,
    ) -> Result<(), StorageResponseError> {
        let TaskSuccessPayload {
            serialized_outputs,
            children,
        } = payload;
        if !matches!(task_id, TaskId::Index(_))
            && (serialized_outputs.is_some() || !children.is_empty())
        {
            return Err(StorageResponseError::InvalidInput(
                "termination tasks cannot report outputs or child jobs".to_owned(),
            ));
        }
        let request = storage::ReportTaskSuccessRequest {
            job_id: job_id.get(),
            task_id: Some(common::TaskId::from(task_id)),
            execution_manager_id: em_id.get(),
            session_id,
            serialized_outputs: serialized_outputs.unwrap_or_default(),
            children: children
                .into_iter()
                .map(|child| storage::ChildJobRequest {
                    compressed_serialized_task_graph: child.compressed_serialized_task_graph,
                    compressed_serialized_task_graph_input: child.compressed_serialized_job_inputs,
                })
                .collect(),
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
    use std::sync::Arc;

    use spider_core::job_submission::CompressedJobSubmission;
    use spider_core::job_submission::MAX_TASK_INSTANCE_MESSAGE_SIZE;
    use spider_proto_rust::storage::TaskInstanceManagementService;
    use spider_proto_rust::storage::TaskInstanceManagementServiceServer;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;
    use tonic::Request;
    use tonic::Response;
    use tonic::transport::Server;

    use super::*;

    #[derive(Clone, Default)]
    struct RecordingService {
        successes: Arc<Mutex<Vec<storage::ReportTaskSuccessRequest>>>,
    }

    #[async_trait]
    impl TaskInstanceManagementService for RecordingService {
        async fn register_task_instance(
            &self,
            _request: Request<storage::RegisterTaskInstanceRequest>,
        ) -> Result<Response<storage::RegisterTaskInstanceResponse>, Status> {
            Err(Status::unimplemented("task registration is unused"))
        }

        async fn report_task_success(
            &self,
            request: Request<storage::ReportTaskSuccessRequest>,
        ) -> Result<Response<common::Void>, Status> {
            self.successes.lock().await.push(request.into_inner());
            Ok(Response::new(common::Void {}))
        }

        async fn report_task_failure(
            &self,
            _request: Request<storage::ReportTaskFailureRequest>,
        ) -> Result<Response<common::Void>, Status> {
            Err(Status::unimplemented("task failure is unused"))
        }
    }

    #[tokio::test]
    async fn success_reports_preserve_payloads_and_identifiers_over_grpc() -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = Endpoint::from_shared(format!("http://{}", listener.local_addr()?))?;
        let incoming = futures_util::stream::unfold(listener, |listener| async {
            let connection = listener.accept().await.map(|(stream, _address)| stream);
            Some((connection, listener))
        });
        let service = RecordingService::default();
        let shutdown = CancellationToken::new();
        let shutdown_guard = shutdown.clone().drop_guard();
        let server = tokio::spawn(
            Server::builder()
                .add_service(
                    TaskInstanceManagementServiceServer::new(service.clone())
                        .max_decoding_message_size(MAX_TASK_INSTANCE_MESSAGE_SIZE),
                )
                .serve_with_incoming_shutdown(incoming, shutdown.cancelled_owned()),
        );
        let client = GrpcStorageClient::connect(endpoint, NonZeroUsize::MIN).await?;
        let children = vec![
            CompressedJobSubmission {
                compressed_serialized_task_graph: vec![0xff; 3 * 1024 * 1024],
                compressed_serialized_job_inputs: vec![0, 0xfe],
            },
            CompressedJobSubmission {
                compressed_serialized_task_graph: vec![0xfe; 2 * 1024 * 1024],
                compressed_serialized_job_inputs: vec![0xfd, 0],
            },
        ];
        let outputs = vec![0, 0xff, 7];
        let expected = storage::ReportTaskSuccessRequest {
            job_id: 11,
            task_id: Some(common::TaskId::from(TaskId::Index(7))),
            task_instance_id: 13,
            execution_manager_id: 17,
            session_id: 19,
            serialized_outputs: outputs.clone(),
            children: children
                .iter()
                .map(|child| storage::ChildJobRequest {
                    compressed_serialized_task_graph: child
                        .compressed_serialized_task_graph
                        .clone(),
                    compressed_serialized_task_graph_input: child
                        .compressed_serialized_job_inputs
                        .clone(),
                })
                .collect(),
        };
        client
            .report_task_success(
                JobId::from(expected.job_id),
                TaskId::Index(7),
                expected.task_instance_id,
                ExecutionManagerId::from(expected.execution_manager_id),
                expected.session_id,
                TaskSuccessPayload {
                    serialized_outputs: Some(outputs.clone()),
                    children: children.clone(),
                },
            )
            .await?;
        assert_eq!(*service.successes.lock().await, vec![expected]);

        for task_id in [TaskId::Commit, TaskId::Cleanup] {
            for (outputs, children) in [
                (Some(outputs.clone()), Vec::new()),
                (None, children.clone()),
            ] {
                let error = client
                    .report_task_success(
                        JobId::from(11),
                        task_id,
                        13,
                        ExecutionManagerId::from(17),
                        19,
                        TaskSuccessPayload {
                            serialized_outputs: outputs,
                            children,
                        },
                    )
                    .await
                    .expect_err("termination payloads must be rejected locally");
                assert!(matches!(error, StorageResponseError::InvalidInput(_)));
            }
        }
        assert_eq!(service.successes.lock().await.len(), 1);
        drop(client);
        drop(shutdown_guard);
        server.await??;
        Ok(())
    }

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
