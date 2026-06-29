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
    /// * [`ClientError::InvalidJobState`] if the job has not yet succeeded.
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
    /// * [`ClientError::InvalidJobState`] if the job has not yet failed.
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
