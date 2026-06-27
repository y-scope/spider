//! [`SpiderClient`] — the top-level handle holding the gRPC connection pools.

use std::num::NonZeroUsize;

use spider_core::{
    job::JobState,
    task::TaskGraph,
    types::{
        id::{JobId, ResourceGroupId},
        io::{TaskInput, TaskOutput},
    },
};
use tonic::transport::Endpoint;

use crate::{
    error::ClientError,
    job::JobOrchestrationClient,
    resource_group::ResourceGroupManagementClient,
};

/// User-facing client for the Spider storage gRPC services.
///
/// Wraps a [`JobOrchestrationClient`] and a [`ResourceGroupManagementClient`] against the same
/// storage endpoint, so callers who need both job-lifecycle and resource-group operations get a
/// single handle and one [`SpiderClient::connect`] call. Callers who need only one service may
/// construct the inner client directly.
#[derive(Debug, Clone)]
pub struct SpiderClient {
    job_orchestration: JobOrchestrationClient,
    #[expect(
        dead_code,
        reason = "read by delegating resource_group methods in task 5"
    )]
    resource_group: ResourceGroupManagementClient,
}

impl SpiderClient {
    /// Connects pools of `pool_size` connections to the storage gRPC endpoint.
    ///
    /// Both the job-orchestration and resource-group-management services are reached through the
    /// same `endpoint`.
    ///
    /// # Returns
    ///
    /// A new [`SpiderClient`] connected to `endpoint` on success.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Transport`] if tonic cannot establish a connection to `endpoint`.
    pub async fn connect(endpoint: Endpoint, pool_size: NonZeroUsize) -> Result<Self, ClientError> {
        let job_orchestration =
            JobOrchestrationClient::connect(endpoint.clone(), pool_size).await?;
        let resource_group = ResourceGroupManagementClient::connect(endpoint, pool_size).await?;

        Ok(Self {
            job_orchestration,
            resource_group,
        })
    }

    /// Serializes and zstd-compresses the task graph and inputs, registers the job, and returns
    /// its assigned id. Delegates to [`JobOrchestrationClient::submit_job`].
    ///
    /// # Returns
    ///
    /// The [`JobId`] the storage server assigned to the registered job on success.
    ///
    /// # Errors
    ///
    /// See [`JobOrchestrationClient::submit_job`].
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

    /// Starts a registered job. Delegates to [`JobOrchestrationClient::start_job`].
    ///
    /// # Returns
    ///
    /// The job's [`JobState`] after the start request is accepted on success.
    ///
    /// # Errors
    ///
    /// See [`JobOrchestrationClient::start_job`].
    pub async fn start_job(&self, job_id: JobId) -> Result<JobState, ClientError> {
        self.job_orchestration.start_job(job_id).await
    }

    /// Cancels a job. Delegates to [`JobOrchestrationClient::cancel_job`].
    ///
    /// # Returns
    ///
    /// The job's [`JobState`] after the cancellation request is accepted on success.
    ///
    /// # Errors
    ///
    /// See [`JobOrchestrationClient::cancel_job`].
    pub async fn cancel_job(&self, job_id: JobId) -> Result<JobState, ClientError> {
        self.job_orchestration.cancel_job(job_id).await
    }

    /// Gets the current state of a job. Delegates to [`JobOrchestrationClient::get_job_state`].
    ///
    /// # Returns
    ///
    /// The job's current [`JobState`] on success.
    ///
    /// # Errors
    ///
    /// See [`JobOrchestrationClient::get_job_state`].
    pub async fn get_job_state(&self, job_id: JobId) -> Result<JobState, ClientError> {
        self.job_orchestration.get_job_state(job_id).await
    }

    /// Gets a job's task outputs. Delegates to [`JobOrchestrationClient::get_job_outputs`].
    ///
    /// # Returns
    ///
    /// The job's outputs, deserialized from the storage wire format into opaque msgpack payloads,
    /// on success.
    ///
    /// # Errors
    ///
    /// See [`JobOrchestrationClient::get_job_outputs`].
    pub async fn get_job_outputs(&self, job_id: JobId) -> Result<Vec<TaskOutput>, ClientError> {
        self.job_orchestration.get_job_outputs(job_id).await
    }

    /// Gets a job's error message. Delegates to [`JobOrchestrationClient::get_job_error`].
    ///
    /// # Returns
    ///
    /// The job's error message on success.
    ///
    /// # Errors
    ///
    /// See [`JobOrchestrationClient::get_job_error`].
    pub async fn get_job_error(&self, job_id: JobId) -> Result<String, ClientError> {
        self.job_orchestration.get_job_error(job_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_maps_unreachable_endpoint_to_transport_error() -> anyhow::Result<()> {
        // Port 1 is privileged with no listener, so the eager connect fails immediately with
        // ECONNREFUSED. The facade propagates the first inner client's transport error.
        let endpoint = Endpoint::from_static("http://127.0.0.1:1");
        let pool_size = NonZeroUsize::new(1).expect("one is nonzero");

        match SpiderClient::connect(endpoint, pool_size).await {
            Err(ClientError::Transport(_)) => Ok(()),
            result => panic!("expected transport error, got {result:?}"),
        }
    }
}
