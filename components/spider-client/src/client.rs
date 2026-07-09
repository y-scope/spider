//! [`SpiderClient`] — the top-level handle holding the gRPC connection pools.

use std::num::NonZeroUsize;

use spider_core::job::JobState;
use spider_core::task::TaskGraph;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::io::TaskInput;
use spider_core::types::io::TaskOutput;
use tonic::transport::Endpoint;

use crate::error::ClientError;
use crate::grpc::job::JobOrchestrationClient;
use crate::grpc::resource_group::ResourceGroupManagementClient;

/// User-facing client for the Spider storage gRPC services.
#[derive(Debug, Clone)]
pub struct SpiderClient {
    job_orchestration: JobOrchestrationClient,
    resource_group: ResourceGroupManagementClient,
}

impl SpiderClient {
    /// Connects pools of `pool_size` connections to the storage gRPC endpoint.
    ///
    /// # Returns
    ///
    /// A new [`SpiderClient`] connected to `endpoint` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::Transport`] if tonic cannot create or connect to the endpoint.
    pub async fn connect(endpoint: Endpoint, pool_size: NonZeroUsize) -> Result<Self, ClientError> {
        let (job_orchestration, resource_group) = tokio::try_join!(
            JobOrchestrationClient::connect(endpoint.clone(), pool_size),
            ResourceGroupManagementClient::connect(endpoint, pool_size),
        )?;

        Ok(Self {
            job_orchestration,
            resource_group,
        })
    }

    /// Serializes and zstd-compresses the task graph and inputs, registers the job, and returns its
    /// assigned id.
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
    pub async fn submit_job(
        &self,
        resource_group_id: ResourceGroupId,
        task_graph: &TaskGraph,
        inputs: Vec<TaskInput>,
    ) -> Result<JobId, ClientError> {
        self.job_orchestration
            .submit_job(resource_group_id, task_graph, inputs)
            .await
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
        self.job_orchestration.start_job(job_id).await
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
        self.job_orchestration.cancel_job(job_id).await
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
        self.job_orchestration.get_job_state(job_id).await
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
        self.job_orchestration.get_job_outputs(job_id).await
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
        self.job_orchestration.get_job_error(job_id).await
    }

    /// Registers an external resource group and returns its server-assigned id.
    ///
    /// # Returns
    ///
    /// The [`ResourceGroupId`] the storage server assigned to the registered resource group on
    /// success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::InvalidArgument`] if the storage server rejects the request as invalid.
    /// * [`ClientError::Unauthenticated`] if the resource group is unknown or the password is
    ///   invalid.
    /// * [`ClientError::Transport`] if the gRPC transport fails or the connection is lost.
    /// * [`ClientError::Server`] for any other server-reported error.
    pub async fn add_resource_group(
        &self,
        external_resource_group_id: String,
        password: Vec<u8>,
    ) -> Result<ResourceGroupId, ClientError> {
        self.resource_group
            .add_resource_group(external_resource_group_id, password)
            .await
    }

    /// Verifies a resource group's password.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::InvalidArgument`] if the storage server rejects the request as invalid.
    /// * [`ClientError::Unauthenticated`] if the resource group is unknown or the password is
    ///   invalid.
    /// * [`ClientError::Transport`] if the gRPC transport fails or the connection is lost.
    /// * [`ClientError::Server`] for any other server-reported error.
    pub async fn verify_resource_group(
        &self,
        resource_group_id: ResourceGroupId,
        password: Vec<u8>,
    ) -> Result<(), ClientError> {
        self.resource_group
            .verify_resource_group(resource_group_id, password)
            .await
    }
}
