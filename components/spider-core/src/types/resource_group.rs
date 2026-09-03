//! Resource group types shared across Spider components.

use secrecy::ExposeSecret;
use secrecy::SecretSlice;

/// Environment variable that supplies the external resource group ID.
pub const EXTERNAL_RESOURCE_GROUP_ID_ENV: &str = "SPIDER_EXTERNAL_RESOURCE_GROUP_ID";

/// Environment variable that supplies the external resource group password.
pub const EXTERNAL_RESOURCE_GROUP_PASSWORD_ENV: &str = "SPIDER_EXTERNAL_RESOURCE_GROUP_PASSWORD";

/// Credentials identifying and authenticating an external resource group.
#[derive(Debug, Clone)]
pub struct ExternalResourceGroupCredentials {
    /// The external resource group ID.
    external_resource_group_id: String,

    /// The resource group password.
    password: SecretSlice<u8>,
}

impl ExternalResourceGroupCredentials {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The newly created external resource group credentials.
    #[must_use]
    pub fn new(external_resource_group_id: String, password: Vec<u8>) -> Self {
        Self {
            external_resource_group_id,
            password: password.into(),
        }
    }

    /// Factory function.
    ///
    /// Reads the external resource group credentials from the [`EXTERNAL_RESOURCE_GROUP_ID_ENV`]
    /// and [`EXTERNAL_RESOURCE_GROUP_PASSWORD_ENV`] environment variables.
    ///
    /// # Returns
    ///
    /// The credentials on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ExternalResourceGroupCredentialsError::MissingEnvVar`] if either environment variable is
    ///   unset.
    pub fn from_env() -> Result<Self, ExternalResourceGroupCredentialsError> {
        let external_resource_group_id =
            std::env::var(EXTERNAL_RESOURCE_GROUP_ID_ENV).map_err(|_| {
                ExternalResourceGroupCredentialsError::MissingEnvVar(EXTERNAL_RESOURCE_GROUP_ID_ENV)
            })?;
        let password = std::env::var(EXTERNAL_RESOURCE_GROUP_PASSWORD_ENV).map_err(|_| {
            ExternalResourceGroupCredentialsError::MissingEnvVar(
                EXTERNAL_RESOURCE_GROUP_PASSWORD_ENV,
            )
        })?;
        Ok(Self::new(external_resource_group_id, password.into_bytes()))
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

/// An error returned while reading external resource group credentials from the environment.
#[derive(Debug, thiserror::Error)]
pub enum ExternalResourceGroupCredentialsError {
    /// A required environment variable is unavailable.
    #[error("required environment variable `{0}` is not set")]
    MissingEnvVar(&'static str),
}
