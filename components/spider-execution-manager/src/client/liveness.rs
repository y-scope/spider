//! Liveness client trait.
//!
//! The execution manager registers itself with storage at boot, then sends a periodic heartbeat.
//! Each heartbeat both keeps the EM marked alive and returns storage's current session id.

use std::net::IpAddr;
use std::sync::Arc;

use async_trait::async_trait;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SessionId;
use spider_core::types::resource_group::ExternalResourceGroupCredentials;

/// The execution manager's identity, resource group, and storage session at registration time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistrationResponse {
    pub em_id: ExecutionManagerId,
    pub session_id: SessionId,

    /// The internal resource group ID, if the execution manager belongs to a resource group.
    pub resource_group_id: Option<ResourceGroupId>,
}

/// Errors returned by [`LivenessClient`] operations.
#[derive(Debug, thiserror::Error)]
pub enum LivenessResponseError {
    /// Storage has reaped this execution manager.
    #[error("execution manager already marked dead")]
    MarkedDead,

    /// Resource group authentication failed.
    #[error("resource group authentication failed")]
    ResourceGroupAuth,

    /// Connection lost, request timeout, or wire-format serialization failure. Callers may back off
    /// and retry.
    #[error("transport error: {0}")]
    Transport(String),

    /// The execution manager id was rejected by storage (e.g. unknown id).
    #[error("execution manager id rejected: {0}")]
    IllegalId(String),
}

/// Client interface to the storage server's execution-manager liveness endpoint.
#[async_trait]
pub trait LivenessClient: Send + Sync {
    /// Registers the execution manager with storage and obtains its id.
    ///
    /// Called once at boot.
    ///
    /// # Parameters
    ///
    /// * `ip` - The advertised IP address of the execution manager process.
    /// * `resource_group_credentials` - Optional credentials for the execution manager's resource
    ///   group.
    ///
    /// # Returns
    ///
    /// The freshly assigned execution manager ID, current storage session ID, and optional internal
    /// resource group ID on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`LivenessResponseError::ResourceGroupAuth`] if resource group authentication fails.
    /// * [`LivenessResponseError::Transport`] if the connection was lost or timed out.
    async fn register(
        &self,
        ip: IpAddr,
        resource_group_credentials: Option<ExternalResourceGroupCredentials>,
    ) -> Result<RegistrationResponse, LivenessResponseError>;

    /// Sends one heartbeat for `em_id` and returns the storage's current session id.
    ///
    /// # Parameters
    ///
    /// * `em_id` - The execution manager id being heartbeated.
    ///
    /// # Returns
    ///
    /// The storage's current session id on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`LivenessResponseError::MarkedDead`] if storage has already reaped this execution
    ///   manager.
    /// * [`LivenessResponseError::Transport`] if the connection was lost or timed out.
    /// * [`LivenessResponseError::IllegalId`] if storage rejected the id.
    async fn heartbeat(
        &self,
        em_id: ExecutionManagerId,
    ) -> Result<SessionId, LivenessResponseError>;
}

#[async_trait]
impl<LivenessClientType: LivenessClient + ?Sized> LivenessClient for Arc<LivenessClientType> {
    async fn register(
        &self,
        ip: IpAddr,
        resource_group_credentials: Option<ExternalResourceGroupCredentials>,
    ) -> Result<RegistrationResponse, LivenessResponseError> {
        (**self).register(ip, resource_group_credentials).await
    }

    async fn heartbeat(
        &self,
        em_id: ExecutionManagerId,
    ) -> Result<SessionId, LivenessResponseError> {
        (**self).heartbeat(em_id).await
    }
}
