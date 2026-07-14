//! [`SpiderClient`] — the top-level handle holding the gRPC connection pools.

use std::num::NonZeroUsize;
use std::time::Duration;

use spider_core::job::JobState;
use spider_core::task::TaskGraph;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::io::TaskInput;
use spider_core::types::io::TaskOutput;
use spider_utils::grpc::retry::RetryConfig;
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
    /// Creates a builder for connecting a [`SpiderClient`] to `endpoint`.
    ///
    /// # Returns
    ///
    /// A [`SpiderClientBuilder`] for `endpoint` with default pool size and retry configuration.
    pub fn builder(endpoint: Endpoint) -> SpiderClientBuilder {
        SpiderClientBuilder {
            endpoint,
            pool_size: DEFAULT_POOL_SIZE,
            retry_config: RetryConfig::default(),
        }
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

/// Builder for configuring and connecting a [`SpiderClient`].
pub struct SpiderClientBuilder {
    endpoint: Endpoint,
    pool_size: NonZeroUsize,
    retry_config: RetryConfig,
}

impl SpiderClientBuilder {
    /// Sets the size of each gRPC connection pool.
    ///
    /// # Returns
    ///
    /// The builder with `pool_size` set.
    #[must_use]
    pub const fn pool_size(mut self, pool_size: NonZeroUsize) -> Self {
        self.pool_size = pool_size;
        self
    }

    /// Sets the number of retries allowed after the initial attempt.
    ///
    /// # Returns
    ///
    /// The builder with `max_retries` set.
    #[must_use]
    pub const fn max_retries(mut self, max_retries: usize) -> Self {
        self.retry_config.max_retries = max_retries;
        self
    }

    /// Sets the upper bound on the exponential backoff between attempts.
    ///
    /// # Returns
    ///
    /// The builder with `max_backoff` set.
    #[must_use]
    pub const fn max_backoff(mut self, max_backoff: Duration) -> Self {
        self.retry_config.max_backoff = max_backoff;
        self
    }

    /// Connects pools of the configured size to the storage gRPC endpoint.
    ///
    /// # Returns
    ///
    /// A new [`SpiderClient`] connected to the configured endpoint on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ClientError::Transport`] if tonic cannot create or connect to the endpoint.
    pub async fn connect(self) -> Result<SpiderClient, ClientError> {
        let (job_orchestration, resource_group) = tokio::try_join!(
            JobOrchestrationClient::connect(
                self.endpoint.clone(),
                self.pool_size,
                self.retry_config
            ),
            ResourceGroupManagementClient::connect(
                self.endpoint,
                self.pool_size,
                self.retry_config
            ),
        )?;

        Ok(SpiderClient {
            job_orchestration,
            resource_group,
        })
    }
}

const DEFAULT_POOL_SIZE: NonZeroUsize = NonZeroUsize::new(8).unwrap();
