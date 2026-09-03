//! Resource group types shared across Spider components.

use secrecy::ExposeSecret;
use secrecy::SecretSlice;

/// Credentials identifying and authenticating an external resource group.
#[derive(Debug, Clone)]
pub struct ExternalResourceGroupCredentials {
    /// The external resource group ID.
    external_resource_group_id: String,

    /// The resource group password.
    password: SecretSlice<u8>,
}

impl ExternalResourceGroupCredentials {
    /// Creates external resource group credentials.
    ///
    /// # Returns
    ///
    /// The credentials containing the given external resource group ID and password.
    #[must_use]
    pub fn new(external_resource_group_id: String, password: Vec<u8>) -> Self {
        Self {
            external_resource_group_id,
            password: password.into(),
        }
    }

    /// # Returns
    ///
    /// The external resource group ID.
    #[must_use]
    pub fn get_external_resource_group_id(&self) -> &str {
        &self.external_resource_group_id
    }

    /// # Returns
    ///
    /// The resource group password.
    #[must_use]
    pub fn get_password(&self) -> &[u8] {
        self.password.expose_secret()
    }
}
