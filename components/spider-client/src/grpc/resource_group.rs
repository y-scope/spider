//! gRPC client implementation wrapping [`ResourceGroupManagementServiceClient`].

use std::num::NonZeroUsize;

use spider_core::types::id::ResourceGroupId;
use spider_proto_rust::storage::ResourceGroupManagementServiceClient;
use spider_proto_rust::storage::{self};
use spider_utils::grpc::client::ConnectionPool;
use tonic::Code;
use tonic::Status;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use crate::error::ClientError;
use crate::error::to_transport_error;

/// gRPC client for the storage server's resource-group-management service.
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
    /// Returns an error if:
    ///
    /// * [`ClientError::Transport`] if tonic cannot create or connect to the endpoint.
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
    /// * Forwards [`ResourceGroupManagementServiceClient::add_resource_group`]'s status on failure.
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
    /// * Forwards [`ResourceGroupManagementServiceClient::verify_resource_group`]'s status on
    ///   failure.
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

/// Maps a resource-group-management gRPC [`Status`] to a [`ClientError`].
///
/// # Returns
///
/// The [`ClientError`] for `status`'s code:
///
/// * [`ClientError::InvalidArgument`] for `INVALID_ARGUMENT`.
/// * [`ClientError::Unauthenticated`] for `UNAUTHENTICATED` (an unknown or unauthorized resource
///   group, or an invalid password).
/// * [`ClientError::Transport`] for `UNAVAILABLE` (a lost or unestablished connection).
/// * [`ClientError::Server`] for any other code.
fn resource_group_status_to_error(status: &Status) -> ClientError {
    match status.code() {
        Code::InvalidArgument => ClientError::InvalidArgument(status.message().to_owned()),
        Code::Unauthenticated => ClientError::Unauthenticated(status.message().to_owned()),
        Code::Unavailable => ClientError::Transport(status.message().to_owned()),
        _ => ClientError::Server(status.message().to_owned()),
    }
}
