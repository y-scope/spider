//! [`ResourceGroupManagementClient`] — gRPC client for the storage resource-group-management
//! service.

use std::num::NonZeroUsize;

use spider_core::types::id::ResourceGroupId;
use spider_proto_rust::storage::{
    self,
    resource_group_management_service_client::ResourceGroupManagementServiceClient,
};
use spider_utils::grpc::client::ConnectionPool;
use tonic::transport::{Channel, Endpoint};

use crate::error::{ClientError, resource_group_status_to_error, to_transport_error};

/// gRPC client for the storage server's resource-group-management service.
///
/// Holds a round-robin pool of connections and exposes the resource-group operations (add, verify).
/// Build one with [`ResourceGroupManagementClient::connect`]. [`crate::client::SpiderClient`] wraps
/// one of these alongside a [`crate::job::JobOrchestrationClient`] for callers who need both
/// services behind a single handle.
#[derive(Debug, Clone)]
pub struct ResourceGroupManagementClient {
    connection_pool: ConnectionPool<ResourceGroupManagementServiceClient<Channel>>,
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
            ResourceGroupManagementServiceClient::new(channel)
        })
        .await
        .map_err(to_transport_error)?;

        Ok(Self { connection_pool })
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
        let request = storage::AddResourceGroupRequest {
            external_resource_group_id,
            password,
        };
        let response = self
            .connection_pool
            .get_client()
            .add_resource_group(request)
            .await
            .map_err(|status| resource_group_status_to_error(&status))?
            .into_inner();

        Ok(ResourceGroupId::from(response.resource_group_id))
    }

    /// Verifies a resource group's password.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success — the storage server's response is empty, so success is implicit.
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
        let request = storage::VerifyResourceGroupRequest {
            resource_group_id: resource_group_id.get(),
            password,
        };
        self.connection_pool
            .get_client()
            .verify_resource_group(request)
            .await
            .map_err(|status| resource_group_status_to_error(&status))?;
        Ok(())
    }
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
    use spider_proto_rust::{
        common::Void,
        storage::resource_group_management_service_server::{
            ResourceGroupManagementService,
            ResourceGroupManagementServiceServer,
        },
    };
    use tonic::{Request, Response, Status, transport::Server};

    use super::*;
    use crate::test_utils::{MockResponse, bind_ephemeral};

    /// Tracks how many times each `ResourceGroupManagementService` RPC was invoked.
    ///
    /// Mocks assert behavior through call counters rather than recorded name vectors.
    #[derive(Debug, Default)]
    struct CallCounts {
        add_resource_group: AtomicU64,
        verify_resource_group: AtomicU64,
    }

    /// In-process mock [`ResourceGroupManagementService`] with configurable per-RPC responses and
    /// call counters.
    ///
    /// [`MockResourceGroupService::new`] builds a mock whose every RPC succeeds with a benign
    /// default; tests override the field for the RPC under test via struct-update syntax and share
    /// the [`CallCounts`] handle to assert call counts.
    struct MockResourceGroupService {
        counts: Arc<CallCounts>,
        add_resource_group: MockResponse<u64>,
        verify_resource_group: MockResponse<()>,
    }

    impl MockResourceGroupService {
        /// Builds a mock whose every RPC succeeds with a benign default value.
        fn new(counts: Arc<CallCounts>) -> Self {
            Self {
                counts,
                add_resource_group: MockResponse::Success(0),
                verify_resource_group: MockResponse::Success(()),
            }
        }
    }

    #[async_trait]
    impl ResourceGroupManagementService for MockResourceGroupService {
        async fn add_resource_group(
            &self,
            _request: Request<storage::AddResourceGroupRequest>,
        ) -> Result<Response<storage::ResourceGroupIdResponse>, Status> {
            self.counts
                .add_resource_group
                .fetch_add(1, Ordering::SeqCst);
            match &self.add_resource_group {
                MockResponse::Success(resource_group_id) => {
                    Ok(Response::new(storage::ResourceGroupIdResponse {
                        resource_group_id: *resource_group_id,
                    }))
                }
                MockResponse::Error(status) => Err(status.clone()),
            }
        }

        async fn verify_resource_group(
            &self,
            _request: Request<storage::VerifyResourceGroupRequest>,
        ) -> Result<Response<Void>, Status> {
            self.counts
                .verify_resource_group
                .fetch_add(1, Ordering::SeqCst);
            match &self.verify_resource_group {
                MockResponse::Success(()) => Ok(Response::new(Void {})),
                MockResponse::Error(status) => Err(status.clone()),
            }
        }
    }

    /// Spawns an in-process tonic server serving `mock` on an ephemeral port.
    ///
    /// # Returns
    ///
    /// The bound socket address and the spawned server task handle on success.
    fn serve(
        mock: MockResourceGroupService,
    ) -> anyhow::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
        let (addr, incoming) = bind_ephemeral()?;
        let join = tokio::spawn(async move {
            Server::builder()
                .add_service(ResourceGroupManagementServiceServer::new(mock))
                .serve_with_incoming(incoming)
                .await
                .expect("mock server should run");
        });
        Ok((addr, join))
    }

    /// Connects a single-connection [`ResourceGroupManagementClient`] to `addr`.
    async fn connect_client(addr: SocketAddr) -> anyhow::Result<ResourceGroupManagementClient> {
        let endpoint = Endpoint::from_shared(format!("http://{addr}"))?;
        let pool_size = NonZeroUsize::new(1).expect("one is nonzero");
        Ok(ResourceGroupManagementClient::connect(endpoint, pool_size).await?)
    }

    #[tokio::test]
    async fn add_resource_group_returns_assigned_id() -> anyhow::Result<()> {
        const EXPECTED_RG_ID: u64 = 99;
        let counts = Arc::new(CallCounts::default());
        let mock = MockResourceGroupService {
            add_resource_group: MockResponse::Success(EXPECTED_RG_ID),
            ..MockResourceGroupService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;
        let resource_group_id = connect_client(addr)
            .await?
            .add_resource_group("rg-1".to_owned(), vec![1, 2, 3])
            .await?;

        assert_eq!(resource_group_id, ResourceGroupId::from(EXPECTED_RG_ID));
        assert_eq!(counts.add_resource_group.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn verify_resource_group_returns_ok_on_success() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let mock = MockResourceGroupService::new(counts.clone());
        let (addr, _join) = serve(mock)?;
        connect_client(addr)
            .await?
            .verify_resource_group(ResourceGroupId::from(7), vec![1, 2, 3])
            .await?;

        assert_eq!(counts.verify_resource_group.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn add_resource_group_maps_unauthenticated() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let mock = MockResourceGroupService {
            add_resource_group: MockResponse::Error(Status::unauthenticated("invalid password")),
            ..MockResourceGroupService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;

        match connect_client(addr)
            .await?
            .add_resource_group("rg-1".to_owned(), vec![1, 2, 3])
            .await
        {
            Err(ClientError::Unauthenticated(message)) => {
                assert!(message.contains("invalid password"));
            }
            result => panic!("expected Unauthenticated, got {result:?}"),
        }
        assert_eq!(counts.add_resource_group.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn add_resource_group_maps_invalid_argument() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let mock = MockResourceGroupService {
            add_resource_group: MockResponse::Error(Status::invalid_argument("bad external id")),
            ..MockResourceGroupService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;

        match connect_client(addr)
            .await?
            .add_resource_group("rg-1".to_owned(), vec![1, 2, 3])
            .await
        {
            Err(ClientError::InvalidArgument(message)) => {
                assert!(message.contains("bad external id"));
            }
            result => panic!("expected InvalidArgument, got {result:?}"),
        }
        assert_eq!(counts.add_resource_group.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn verify_resource_group_maps_unavailable_to_transport() -> anyhow::Result<()> {
        let counts = Arc::new(CallCounts::default());
        let mock = MockResourceGroupService {
            verify_resource_group: MockResponse::Error(Status::unavailable("connection lost")),
            ..MockResourceGroupService::new(counts.clone())
        };
        let (addr, _join) = serve(mock)?;

        match connect_client(addr)
            .await?
            .verify_resource_group(ResourceGroupId::from(8), vec![1, 2, 3])
            .await
        {
            Err(ClientError::Transport(message)) => assert!(message.contains("connection lost")),
            result => panic!("expected Transport, got {result:?}"),
        }
        assert_eq!(counts.verify_resource_group.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
