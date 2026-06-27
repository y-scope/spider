//! [`JobOrchestrationClient`] — gRPC client for the storage job-orchestration service.

use std::num::NonZeroUsize;

use spider_proto_rust::storage::job_orchestration_service_client::JobOrchestrationServiceClient;
use spider_utils::grpc::client::ConnectionPool;
use tonic::transport::{Channel, Endpoint};

use crate::error::{ClientError, to_transport_error};

/// gRPC client for the storage server's job-orchestration service.
///
/// Holds a round-robin pool of connections and exposes the job-lifecycle methods (submit, start,
/// cancel, get state, get outputs, get error). Build one with [`JobOrchestrationClient::connect`].
/// [`crate::client::SpiderClient`] wraps one of these alongside a
/// [`crate::resource_group::ResourceGroupManagementClient`] for callers who need both services
/// behind a single handle.
#[derive(Debug, Clone)]
pub struct JobOrchestrationClient {
    #[expect(dead_code, reason = "read by job methods in task 4")]
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_maps_unreachable_endpoint_to_transport_error() -> anyhow::Result<()> {
        // Port 1 is privileged with no listener, so the eager connect fails immediately with
        // ECONNREFUSED.
        let endpoint = Endpoint::from_static("http://127.0.0.1:1");
        let pool_size = NonZeroUsize::new(1).expect("one is nonzero");

        match JobOrchestrationClient::connect(endpoint, pool_size).await {
            Err(ClientError::Transport(_)) => Ok(()),
            result => panic!("expected transport error, got {result:?}"),
        }
    }
}
