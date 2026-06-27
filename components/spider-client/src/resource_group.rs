//! [`ResourceGroupManagementClient`] — gRPC client for the storage resource-group-management
//! service.

use std::num::NonZeroUsize;

use spider_proto_rust::storage::resource_group_management_service_client as rg_client;
use spider_utils::grpc::client::ConnectionPool;
use tonic::transport::{Channel, Endpoint};

use crate::error::{ClientError, to_transport_error};

/// gRPC client for the storage server's resource-group-management service.
///
/// Holds a round-robin pool of connections and exposes the resource-group operations (add, verify).
/// Build one with [`ResourceGroupManagementClient::connect`]. [`crate::client::SpiderClient`] wraps
/// one of these alongside a [`crate::job::JobOrchestrationClient`] for callers who need both
/// services behind a single handle.
#[derive(Debug, Clone)]
pub struct ResourceGroupManagementClient {
    #[expect(dead_code, reason = "read by resource_group methods in task 5")]
    connection_pool: ConnectionPool<rg_client::ResourceGroupManagementServiceClient<Channel>>,
}

impl ResourceGroupManagementClient {
    /// Connects a pool of `pool_size` connections to the resource-group-management gRPC endpoint.
    ///
    /// # Returns
    ///
    /// A new [`ResourceGroupManagementClient`] connected to `endpoint` on success.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Transport`] if tonic cannot establish a connection to `endpoint`.
    pub async fn connect(endpoint: Endpoint, pool_size: NonZeroUsize) -> Result<Self, ClientError> {
        let connection_pool = ConnectionPool::connect(endpoint, pool_size, |channel| {
            rg_client::ResourceGroupManagementServiceClient::new(channel)
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

        match ResourceGroupManagementClient::connect(endpoint, pool_size).await {
            Err(ClientError::Transport(_)) => Ok(()),
            result => panic!("expected transport error, got {result:?}"),
        }
    }
}
