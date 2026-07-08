//! gRPC client implementation wrapping [`JobOrchestrationServiceClient`].

use std::num::NonZeroUsize;

use spider_core::compression::encode_zstd_bytes;
use spider_core::job::JobState;
use spider_core::task::TaskGraph;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::io::SerializedTaskOutputs;
use spider_core::types::io::TaskInput;
use spider_core::types::io::TaskInputsSerializer;
use spider_core::types::io::TaskOutput;
use spider_proto_rust::error::Error as ProtoError;
use spider_proto_rust::storage::JobOrchestrationServiceClient;
use spider_proto_rust::storage::{self};
use spider_utils::grpc::client::ConnectionPool;
use tonic::Code;
use tonic::Status;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use crate::error::ClientError;
use crate::error::to_transport_error;

/// gRPC client for the storage server's job-orchestration service.
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
    /// Returns an error if:
    ///
    /// * [`ClientError::Transport`] if tonic cannot create or connect to the endpoint.
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
    /// * Forwards [`TaskGraph::to_zstd_compressed_json`]'s return values on failure as
    ///   [`ClientError::Serialization`].
    /// * Forwards [`JobOrchestrationServiceClient::register_job`]'s status on failure.
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
            .map_err(|status| job_status_to_error(&status))?
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
    /// * Forwards [`JobOrchestrationServiceClient::start_job`]'s status on failure.
    /// * Forwards [`job_state_response_to_result`]'s return values on failure.
    pub async fn start_job(&self, job_id: JobId) -> Result<JobState, ClientError> {
        let request = storage::JobIdRequest {
            job_id: job_id.get(),
        };
        let response = self
            .connection_pool
            .get_client()
            .start_job(request)
            .await
            .map_err(|status| job_status_to_error(&status))?
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
    /// * Forwards [`JobOrchestrationServiceClient::cancel_job`]'s status on failure.
    /// * Forwards [`job_state_response_to_result`]'s return values on failure.
    pub async fn cancel_job(&self, job_id: JobId) -> Result<JobState, ClientError> {
        let request = storage::JobIdRequest {
            job_id: job_id.get(),
        };
        let response = self
            .connection_pool
            .get_client()
            .cancel_job(request)
            .await
            .map_err(|status| job_status_to_error(&status))?
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
    /// * Forwards [`JobOrchestrationServiceClient::get_job_state`]'s status on failure.
    /// * Forwards [`job_state_response_to_result`]'s return values on failure.
    pub async fn get_job_state(&self, job_id: JobId) -> Result<JobState, ClientError> {
        let request = storage::JobIdRequest {
            job_id: job_id.get(),
        };
        let response = self
            .connection_pool
            .get_client()
            .get_job_state(request)
            .await
            .map_err(|status| job_status_to_error(&status))?
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
    /// * Forwards [`SerializedTaskOutputs::deserialize_from_raw`]'s return values on failure as
    ///   [`ClientError::Deserialization`].
    /// * Forwards [`JobOrchestrationServiceClient::get_job_outputs`]'s status on failure.
    pub async fn get_job_outputs(&self, job_id: JobId) -> Result<Vec<TaskOutput>, ClientError> {
        let request = storage::JobIdRequest {
            job_id: job_id.get(),
        };
        let response = self
            .connection_pool
            .get_client()
            .get_job_outputs(request)
            .await
            .map_err(|status| job_status_to_error(&status))?
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
    /// * Forwards [`JobOrchestrationServiceClient::get_job_error`]'s status on failure.
    pub async fn get_job_error(&self, job_id: JobId) -> Result<String, ClientError> {
        let request = storage::JobIdRequest {
            job_id: job_id.get(),
        };
        let response = self
            .connection_pool
            .get_client()
            .get_job_error(request)
            .await
            .map_err(|status| job_status_to_error(&status))?
            .into_inner();

        Ok(response.error_message)
    }
}

/// Serializes and zstd-compresses a job's task inputs for the
/// [`JobOrchestrationServiceClient::register_job`] request.
///
/// # Returns
///
/// The zstd-compressed wire-format input bytes on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`ClientError::Serialization`] if an input cannot be framed or the wire buffer cannot be
///   compressed.
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

/// Converts a [`storage::JobStateResponse`] into a [`JobState`].
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

/// Maps a job-orchestration gRPC [`Status`] to a [`ClientError`].
///
/// # Returns
///
/// The [`ClientError`] for `status`'s code:
///
/// * [`ClientError::JobNotFound`] for `NOT_FOUND`.
/// * [`ClientError::InvalidJobState`] for `FAILED_PRECONDITION`.
/// * [`ClientError::InvalidArgument`] for `INVALID_ARGUMENT`.
/// * [`ClientError::Unauthenticated`] for `UNAUTHENTICATED`.
/// * [`ClientError::Transport`] for `UNAVAILABLE` (a lost or unestablished connection).
/// * [`ClientError::Server`] for any other code.
fn job_status_to_error(status: &Status) -> ClientError {
    match status.code() {
        Code::NotFound => ClientError::JobNotFound,
        Code::FailedPrecondition => ClientError::InvalidJobState(status.message().to_owned()),
        Code::InvalidArgument => ClientError::InvalidArgument(status.message().to_owned()),
        Code::Unauthenticated => ClientError::Unauthenticated(status.message().to_owned()),
        Code::Unavailable => ClientError::Transport(status.message().to_owned()),
        _ => ClientError::Server(status.message().to_owned()),
    }
}
