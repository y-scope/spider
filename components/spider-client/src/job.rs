//! [`JobOrchestrationClient`] — gRPC client for the storage job-orchestration service.

use std::num::NonZeroUsize;

use spider_core::{
    compression::encode_zstd_bytes,
    job::JobState,
    task::TaskGraph,
    types::{
        id::{JobId, ResourceGroupId},
        io::{SerializedTaskOutputs, TaskInput, TaskInputsSerializer, TaskOutput},
    },
};
use spider_proto_rust::{
    error::Error as ProtoError,
    storage::{self, job_orchestration_service_client::JobOrchestrationServiceClient},
};
use spider_utils::grpc::client::ConnectionPool;
use tonic::{
    Code,
    Status,
    transport::{Channel, Endpoint},
};

use crate::error::{ClientError, job_status_to_error, to_transport_error};

/// gRPC client for the storage server's job-orchestration service.
///
/// Holds a round-robin pool of connections and exposes the job-lifecycle methods (submit, start,
/// cancel, get state, get outputs, get error). Build one with [`JobOrchestrationClient::connect`].
/// [`crate::client::SpiderClient`] wraps one of these alongside a
/// [`crate::resource_group::ResourceGroupManagementClient`] for callers who need both services
/// behind a single handle.
#[derive(Debug, Clone)]
pub struct JobOrchestrationClient {
    connection_pool: ConnectionPool<JobOrchestrationServiceClient<Channel>>,
}

impl JobOrchestrationClient {
    /// Connects a pool of `pool_size` connections to the job-orchestration gRPC endpoint.
    ///
    /// # Returns
    ///
    /// A new [`JobOrchestrationClient`] connected to `endpoint` on success.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Transport`] if tonic cannot establish a connection to `endpoint`.
    pub async fn connect(endpoint: Endpoint, pool_size: NonZeroUsize) -> Result<Self, ClientError> {
        let connection_pool = ConnectionPool::connect(endpoint, pool_size, |channel| {
            JobOrchestrationServiceClient::new(channel)
        })
        .await
        .map_err(to_transport_error)?;

        Ok(Self { connection_pool })
    }

    /// Serializes and zstd-compresses the task graph and inputs, registers the job, and returns
    /// its assigned id.
    ///
    /// # Returns
    ///
    /// The [`JobId`] the storage server assigned to the registered job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::Serialization`] if the task graph or inputs cannot be serialized or
    ///   compressed.
    /// * [`ClientError::InvalidArgument`] if the storage server rejects the task graph or inputs.
    /// * [`ClientError::Unauthenticated`] if the resource group is unknown or unauthorized.
    /// * [`ClientError::Transport`] if the gRPC transport fails or the connection is lost.
    /// * [`ClientError::Server`] for any other server-reported error.
    ///
    /// A freshly registered job has no id yet, so the server-reported `NOT_FOUND` and
    /// `FAILED_PRECONDITION` codes (which a job id would otherwise attach) cannot arise for
    /// registration and are folded into [`ClientError::Server`].
    pub async fn submit_job(
        &self,
        resource_group_id: ResourceGroupId,
        task_graph: &TaskGraph,
        inputs: Vec<TaskInput>,
    ) -> Result<JobId, ClientError> {
        let compressed_serialized_task_graph = task_graph
            .to_zstd_compressed_json()
            .map_err(|error| ClientError::Serialization(error.to_string()))?;
        let compressed_serialized_inputs = serialize_inputs(inputs)?;
        let request = storage::RegisterJobRequest {
            resource_group_id: resource_group_id.get(),
            compressed_serialized_task_graph,
            compressed_serialized_inputs,
        };
        let response = self
            .connection_pool
            .get_client()
            .register_job(request)
            .await
            .map_err(|status| submit_status_to_error(&status))?
            .into_inner();

        Ok(JobId::from(response.job_id))
    }

    /// Starts a registered job.
    ///
    /// # Returns
    ///
    /// The job's [`JobState`] after the start request is accepted on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::JobNotFound`] if no job with `job_id` exists.
    /// * [`ClientError::InvalidJobState`] if the job is not in a state that allows starting.
    /// * [`ClientError::Unauthenticated`] if the resource group is unknown or unauthorized.
    /// * [`ClientError::UnspecifiedJobState`] if the server reports an unspecified job state.
    /// * [`ClientError::Transport`] if the gRPC transport fails, the connection is lost, or the
    ///   server reports an unrecognized job state.
    /// * [`ClientError::Server`] for any other server-reported error.
    pub async fn start_job(&self, job_id: JobId) -> Result<JobState, ClientError> {
        let request = storage::JobIdRequest {
            job_id: job_id.get(),
        };
        let response = self
            .connection_pool
            .get_client()
            .start_job(request)
            .await
            .map_err(|status| job_status_to_error(&status, job_id))?
            .into_inner();

        job_state_response_to_result(response)
    }

    /// Cancels a job.
    ///
    /// # Returns
    ///
    /// The job's [`JobState`] after the cancellation request is accepted on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::JobNotFound`] if no job with `job_id` exists.
    /// * [`ClientError::InvalidJobState`] if the job is not in a state that allows cancellation.
    /// * [`ClientError::UnspecifiedJobState`] if the server reports an unspecified job state.
    /// * [`ClientError::Transport`] if the gRPC transport fails, the connection is lost, or the
    ///   server reports an unrecognized job state.
    /// * [`ClientError::Server`] for any other server-reported error.
    pub async fn cancel_job(&self, job_id: JobId) -> Result<JobState, ClientError> {
        let request = storage::JobIdRequest {
            job_id: job_id.get(),
        };
        let response = self
            .connection_pool
            .get_client()
            .cancel_job(request)
            .await
            .map_err(|status| job_status_to_error(&status, job_id))?
            .into_inner();

        job_state_response_to_result(response)
    }

    /// Gets the current state of a job.
    ///
    /// # Returns
    ///
    /// The job's current [`JobState`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::JobNotFound`] if no job with `job_id` exists.
    /// * [`ClientError::UnspecifiedJobState`] if the server reports an unspecified job state.
    /// * [`ClientError::Transport`] if the gRPC transport fails, the connection is lost, or the
    ///   server reports an unrecognized job state.
    /// * [`ClientError::Server`] for any other server-reported error.
    pub async fn get_job_state(&self, job_id: JobId) -> Result<JobState, ClientError> {
        let request = storage::JobIdRequest {
            job_id: job_id.get(),
        };
        let response = self
            .connection_pool
            .get_client()
            .get_job_state(request)
            .await
            .map_err(|status| job_status_to_error(&status, job_id))?
            .into_inner();

        job_state_response_to_result(response)
    }

    /// Gets a job's task outputs.
    ///
    /// # Returns
    ///
    /// The job's outputs, deserialized from the storage wire format into opaque msgpack payloads,
    /// on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::JobNotFound`] if no job with `job_id` exists.
    /// * [`ClientError::Deserialization`] if the returned outputs cannot be decompressed or
    ///   unframed.
    /// * [`ClientError::Transport`] if the gRPC transport fails or the connection is lost.
    /// * [`ClientError::Server`] for any other server-reported error.
    pub async fn get_job_outputs(&self, job_id: JobId) -> Result<Vec<TaskOutput>, ClientError> {
        let request = storage::JobIdRequest {
            job_id: job_id.get(),
        };
        let response = self
            .connection_pool
            .get_client()
            .get_job_outputs(request)
            .await
            .map_err(|status| job_status_to_error(&status, job_id))?
            .into_inner();

        SerializedTaskOutputs::deserialize_from_raw(&response.serialized_outputs)
            .map_err(|error| ClientError::Deserialization(error.to_string()))
    }

    /// Gets a job's error message.
    ///
    /// # Returns
    ///
    /// The job's error message on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::JobNotFound`] if no job with `job_id` exists.
    /// * [`ClientError::Transport`] if the gRPC transport fails or the connection is lost.
    /// * [`ClientError::Server`] for any other server-reported error.
    pub async fn get_job_error(&self, job_id: JobId) -> Result<String, ClientError> {
        let request = storage::JobIdRequest {
            job_id: job_id.get(),
        };
        let response = self
            .connection_pool
            .get_client()
            .get_job_error(request)
            .await
            .map_err(|status| job_status_to_error(&status, job_id))?
            .into_inner();

        Ok(response.error_message)
    }
}

/// Serializes and zstd-compresses a job's task inputs for the `RegisterJob` request.
///
/// # Returns
///
/// The zstd-compressed wire-format input bytes on success.
///
/// # Errors
///
/// Returns [`ClientError::Serialization`] if an input cannot be framed or the wire buffer cannot
/// be compressed.
fn serialize_inputs(inputs: Vec<TaskInput>) -> Result<Vec<u8>, ClientError> {
    let mut serializer = TaskInputsSerializer::new();
    for input in inputs {
        serializer
            .append(input)
            .map_err(|error| ClientError::Serialization(error.to_string()))?;
    }
    encode_zstd_bytes(&serializer.release())
        .map_err(|error| ClientError::Serialization(error.to_string()))
}

/// Converts a `RegisterJob` gRPC [`Status`] to a [`ClientError`].
///
/// Registration has no job id yet, so the `NOT_FOUND` and `FAILED_PRECONDITION` codes that
/// [`job_status_to_error`] would attach a job id to cannot arise here and fall back to
/// [`ClientError::Server`]. The remaining arms match [`job_status_to_error`].
///
/// # Returns
///
/// The [`ClientError`] for `status`'s code:
///
/// * [`ClientError::InvalidArgument`] for `INVALID_ARGUMENT`.
/// * [`ClientError::Unauthenticated`] for `UNAUTHENTICATED`.
/// * [`ClientError::Transport`] for `UNAVAILABLE` (a lost or unestablished connection).
/// * [`ClientError::Server`] for any other code.
fn submit_status_to_error(status: &Status) -> ClientError {
    match status.code() {
        Code::InvalidArgument => ClientError::InvalidArgument(status.message().to_owned()),
        Code::Unauthenticated => ClientError::Unauthenticated(status.message().to_owned()),
        Code::Unavailable => ClientError::Transport(status.message().to_owned()),
        _ => ClientError::Server(status.message().to_owned()),
    }
}

/// Converts a `JobStateResponse` into a [`JobState`].
///
/// # Returns
///
/// The [`JobState`] carried by `response` on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`ClientError::UnspecifiedJobState`] if the server reports an unspecified job state.
/// * [`ClientError::Transport`] if `response` carries an unrecognized job state.
fn job_state_response_to_result(
    response: storage::JobStateResponse,
) -> Result<JobState, ClientError> {
    let proto_state = storage::JobState::try_from(response.state)
        .map_err(|error| ClientError::Transport(error.to_string()))?;
    JobState::try_from(proto_state).map_err(|error| match error {
        ProtoError::JobStateUnspecified => ClientError::UnspecifiedJobState,
        other => ClientError::Transport(other.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use async_trait::async_trait;
    use spider_proto_rust::storage::job_orchestration_service_server::{
        JobOrchestrationService,
        JobOrchestrationServiceServer,
    };
    use tonic::{Request, Response, Status, transport::Server};

    use super::*;
    use crate::test_utils::{MockResponse, bind_ephemeral};

    /// Tracks how many times each `JobOrchestrationService` RPC was invoked.
    ///
    /// Mocks assert behavior through call counters rather than recorded name vectors.
    #[derive(Debug, Default)]
    struct CallCounts {
        register_job: AtomicU64,
        start_job: AtomicU64,
        cancel_job: AtomicU64,
        get_job_state: AtomicU64,
        get_job_outputs: AtomicU64,
        get_job_error: AtomicU64,
    }

    /// In-process mock [`JobOrchestrationService`] with configurable per-RPC responses and call
    /// counters.
    ///
    /// [`MockJobService::new`] builds a mock whose every RPC succeeds with a benign default; tests
    /// override the field for the RPC under test via struct-update syntax and share the
    /// [`CallCounts`] handle to assert call counts.
    struct MockJobService {
        counts: Arc<CallCounts>,
        register_job: MockResponse<u64>,
        start_job: MockResponse<storage::JobState>,
        cancel_job: MockResponse<storage::JobState>,
        get_job_state: MockResponse<storage::JobState>,
        get_job_outputs: MockResponse<Vec<TaskOutput>>,
        get_job_error: MockResponse<String>,
    }

    impl MockJobService {
        /// Builds a mock whose every RPC succeeds with a benign default value.
        fn new(counts: Arc<CallCounts>) -> Self {
            Self {
                counts,
                register_job: MockResponse::Success(0),
                start_job: MockResponse::Success(storage::JobState::Running),
                cancel_job: MockResponse::Success(storage::JobState::Running),
                get_job_state: MockResponse::Success(storage::JobState::Running),
                get_job_outputs: MockResponse::Success(Vec::new()),
                get_job_error: MockResponse::Success(String::new()),
            }
        }
    }

    #[async_trait]
    impl JobOrchestrationService for MockJobService {
        async fn register_job(
            &self,
            _request: Request<storage::RegisterJobRequest>,
        ) -> Result<Response<storage::RegisterJobResponse>, Status> {
            self.counts.register_job.fetch_add(1, Ordering::SeqCst);
            match &self.register_job {
                MockResponse::Success(job_id) => Ok(Response::new(storage::RegisterJobResponse {
                    job_id: *job_id,
                })),
                MockResponse::Error(status) => Err(status.clone()),
            }
        }

        async fn start_job(
            &self,
            _request: Request<storage::JobIdRequest>,
        ) -> Result<Response<storage::JobStateResponse>, Status> {
            self.counts.start_job.fetch_add(1, Ordering::SeqCst);
            match &self.start_job {
                MockResponse::Success(state) => Ok(Response::new(storage::JobStateResponse {
                    state: (*state).into(),
                })),
                MockResponse::Error(status) => Err(status.clone()),
            }
        }

        async fn cancel_job(
            &self,
            _request: Request<storage::JobIdRequest>,
        ) -> Result<Response<storage::JobStateResponse>, Status> {
            self.counts.cancel_job.fetch_add(1, Ordering::SeqCst);
            match &self.cancel_job {
                MockResponse::Success(state) => Ok(Response::new(storage::JobStateResponse {
                    state: (*state).into(),
                })),
                MockResponse::Error(status) => Err(status.clone()),
            }
        }

        async fn get_job_state(
            &self,
            _request: Request<storage::JobIdRequest>,
        ) -> Result<Response<storage::JobStateResponse>, Status> {
            self.counts.get_job_state.fetch_add(1, Ordering::SeqCst);
            match &self.get_job_state {
                MockResponse::Success(state) => Ok(Response::new(storage::JobStateResponse {
                    state: (*state).into(),
                })),
                MockResponse::Error(status) => Err(status.clone()),
            }
        }

        async fn get_job_outputs(
            &self,
            _request: Request<storage::JobIdRequest>,
        ) -> Result<Response<storage::JobOutputsResponse>, Status> {
            self.counts.get_job_outputs.fetch_add(1, Ordering::SeqCst);
            match &self.get_job_outputs {
                MockResponse::Success(outputs) => {
                    let serialized = SerializedTaskOutputs::serialize_with_size_hint(outputs)
                        .expect("serializing mock task outputs should succeed")
                        .to_raw();
                    Ok(Response::new(storage::JobOutputsResponse {
                        serialized_outputs: serialized,
                    }))
                }
                MockResponse::Error(status) => Err(status.clone()),
            }
        }

        async fn get_job_error(
            &self,
            _request: Request<storage::JobIdRequest>,
        ) -> Result<Response<storage::JobErrorResponse>, Status> {
            self.counts.get_job_error.fetch_add(1, Ordering::SeqCst);
            match &self.get_job_error {
                MockResponse::Success(message) => Ok(Response::new(storage::JobErrorResponse {
                    error_message: message.clone(),
                })),
                MockResponse::Error(status) => Err(status.clone()),
            }
        }
    }

    /// Spawns an in-process tonic server serving `mock` on an ephemeral port.
    ///
    /// # Returns
    ///
    /// The bound socket address and the spawned server task handle on success.
    fn serve(mock: MockJobService) -> anyhow::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
        let (addr, incoming) = bind_ephemeral()?;
        let join = tokio::spawn(async move {
            Server::builder()
                .add_service(JobOrchestrationServiceServer::new(mock))
                .serve_with_incoming(incoming)
                .await
                .expect("mock server should run");
        });
        Ok((addr, join))
    }

    /// Connects a single-connection [`JobOrchestrationClient`] to `addr`.
    async fn connect_client(addr: SocketAddr) -> anyhow::Result<JobOrchestrationClient> {
        let endpoint = Endpoint::from_shared(format!("http://{addr}"))?;
        let pool_size = NonZeroUsize::new(1).expect("one is nonzero");
        Ok(JobOrchestrationClient::connect(endpoint, pool_size).await?)
    }

    #[tokio::test]
    async fn submit_job_returns_server_assigned_id() -> anyhow::Result<()> {
        const EXPECTED_JOB_ID: u64 = 42;
        let counts = Arc::new(CallCounts::default());
        let mock = MockJobService {
            register_job: MockResponse::Success(EXPECTED_JOB_ID),
            ..MockJobService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;
        let job_id = connect_client(addr)
            .await?
            .submit_job(
                ResourceGroupId::from(3),
                &TaskGraph::new(None, None)?,
                Vec::new(),
            )
            .await?;

        assert_eq!(job_id, JobId::from(EXPECTED_JOB_ID));
        assert_eq!(counts.register_job.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn start_job_returns_server_state() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let mock = MockJobService {
            start_job: MockResponse::Success(storage::JobState::Running),
            ..MockJobService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;
        let state = connect_client(addr)
            .await?
            .start_job(JobId::from(7))
            .await?;

        assert_eq!(state, JobState::Running);
        assert_eq!(counts.start_job.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn cancel_job_returns_server_state() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let mock = MockJobService {
            cancel_job: MockResponse::Success(storage::JobState::Cancelled),
            ..MockJobService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;
        let state = connect_client(addr)
            .await?
            .cancel_job(JobId::from(8))
            .await?;

        assert_eq!(state, JobState::Cancelled);
        assert_eq!(counts.cancel_job.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_state_returns_server_state() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let mock = MockJobService {
            get_job_state: MockResponse::Success(storage::JobState::Succeeded),
            ..MockJobService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;
        let state = connect_client(addr)
            .await?
            .get_job_state(JobId::from(9))
            .await?;

        assert_eq!(state, JobState::Succeeded);
        assert_eq!(counts.get_job_state.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_outputs_round_trips_task_outputs() -> anyhow::Result<()> {
        let outputs: Vec<TaskOutput> = vec![vec![1, 2, 3], vec![4, 5]];
        let counts = Arc::new(CallCounts::default());
        let mock = MockJobService {
            get_job_outputs: MockResponse::Success(outputs.clone()),
            ..MockJobService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;
        let received = connect_client(addr)
            .await?
            .get_job_outputs(JobId::from(11))
            .await?;

        assert_eq!(received, outputs);
        assert_eq!(counts.get_job_outputs.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_error_returns_message() -> anyhow::Result<()> {
        const MESSAGE: &str = "task failed: boom";
        let counts = Arc::new(CallCounts::default());
        let mock = MockJobService {
            get_job_error: MockResponse::Success(MESSAGE.to_owned()),
            ..MockJobService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;
        let message = connect_client(addr)
            .await?
            .get_job_error(JobId::from(12))
            .await?;

        assert_eq!(message, MESSAGE);
        assert_eq!(counts.get_job_error.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn submit_job_maps_invalid_argument() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let mock = MockJobService {
            register_job: MockResponse::Error(Status::invalid_argument("bad task graph")),
            ..MockJobService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;

        match connect_client(addr)
            .await?
            .submit_job(
                ResourceGroupId::from(3),
                &TaskGraph::new(None, None)?,
                Vec::new(),
            )
            .await
        {
            Err(ClientError::InvalidArgument(message)) => {
                assert!(message.contains("bad task graph"));
            }
            result => panic!("expected InvalidArgument, got {result:?}"),
        }
        assert_eq!(counts.register_job.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn submit_job_maps_unauthenticated() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let mock = MockJobService {
            register_job: MockResponse::Error(Status::unauthenticated("invalid resource group")),
            ..MockJobService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;

        match connect_client(addr)
            .await?
            .submit_job(
                ResourceGroupId::from(3),
                &TaskGraph::new(None, None)?,
                Vec::new(),
            )
            .await
        {
            Err(ClientError::Unauthenticated(message)) => {
                assert!(message.contains("invalid resource group"));
            }
            result => panic!("expected Unauthenticated, got {result:?}"),
        }
        assert_eq!(counts.register_job.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_state_maps_not_found_to_job_not_found() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let job_id = JobId::from(13);
        let mock = MockJobService {
            get_job_state: MockResponse::Error(Status::not_found("job not found")),
            ..MockJobService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;

        match connect_client(addr).await?.get_job_state(job_id).await {
            Err(ClientError::JobNotFound(returned_id)) => assert_eq!(returned_id, job_id),
            result => panic!("expected JobNotFound, got {result:?}"),
        }
        assert_eq!(counts.get_job_state.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn start_job_maps_failed_precondition_to_invalid_job_state() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let mock = MockJobService {
            start_job: MockResponse::Error(Status::failed_precondition("job is running")),
            ..MockJobService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;

        match connect_client(addr).await?.start_job(JobId::from(14)).await {
            Err(ClientError::InvalidJobState(message)) => {
                assert!(message.contains("job is running"));
            }
            result => panic!("expected InvalidJobState, got {result:?}"),
        }
        assert_eq!(counts.start_job.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_state_maps_unavailable_to_transport() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let mock = MockJobService {
            get_job_state: MockResponse::Error(Status::unavailable("connection lost")),
            ..MockJobService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;

        match connect_client(addr)
            .await?
            .get_job_state(JobId::from(15))
            .await
        {
            Err(ClientError::Transport(message)) => assert!(message.contains("connection lost")),
            result => panic!("expected Transport, got {result:?}"),
        }
        assert_eq!(counts.get_job_state.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_state_maps_internal_to_server() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let mock = MockJobService {
            get_job_state: MockResponse::Error(Status::internal("internal error")),
            ..MockJobService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;

        match connect_client(addr)
            .await?
            .get_job_state(JobId::from(16))
            .await
        {
            Err(ClientError::Server(message)) => assert!(message.contains("internal error")),
            result => panic!("expected Server, got {result:?}"),
        }
        assert_eq!(counts.get_job_state.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_state_maps_unspecified_state() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let mock = MockJobService {
            get_job_state: MockResponse::Success(storage::JobState::Unspecified),
            ..MockJobService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;

        match connect_client(addr)
            .await?
            .get_job_state(JobId::from(17))
            .await
        {
            Err(ClientError::UnspecifiedJobState) => Ok(()),
            result => panic!("expected UnspecifiedJobState, got {result:?}"),
        }
    }
}
