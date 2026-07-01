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
    pub(crate) async fn connect(
        endpoint: Endpoint,
        pool_size: NonZeroUsize,
    ) -> Result<Self, ClientError> {
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
    pub(crate) async fn add_resource_group(
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
    pub(crate) async fn verify_resource_group(
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
