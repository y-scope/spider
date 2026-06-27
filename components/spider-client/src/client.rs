//! [`SpiderClient`] — the top-level handle holding the gRPC connection pools.

use std::num::NonZeroUsize;

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
    #[expect(dead_code, reason = "read by delegating job methods in task 4")]
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
