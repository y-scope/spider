//! Resource group types shared across Spider components.

/// Credentials identifying and authenticating an external resource group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalResourceGroupCredentials {
    /// The external resource group ID.
    pub external_resource_group_id: String,

    /// The resource group password.
    pub password: Vec<u8>,
}
