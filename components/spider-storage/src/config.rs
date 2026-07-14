use std::net::IpAddr;

use secrecy::SecretString;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::state::runtime::RuntimeConfig;

/// Top-level configuration for the storage gRPC server.
///
/// Pairs the server's listening endpoint with the [`RuntimeConfig`] used to build the storage
/// runtime.
#[derive(Clone, Debug, Deserialize)]
pub struct ServerConfig {
    /// The IP address the gRPC server listens on.
    pub host: IpAddr,

    /// The port the gRPC server listens on.
    pub port: u16,

    /// Configuration for the storage runtime.
    pub runtime: RuntimeConfig,
}

/// Configuration parameters for connecting to the database.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub max_connections: u32,

    #[serde(skip)]
    pub credentials: DatabaseCredentials,
}

/// Credentials for authenticating with the database.
#[derive(Clone, Debug)]
pub struct DatabaseCredentials {
    pub username: String,
    pub password: SecretString,
}

impl DatabaseCredentials {
    /// Reads the database credentials from the [`DB_USERNAME_ENV`] and [`DB_PASSWORD_ENV`]
    /// environment variables.
    ///
    /// # Returns
    ///
    /// The credentials on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`CredentialsError::MissingEnvVar`] if either environment variable is unset.
    pub fn from_env() -> Result<Self, CredentialsError> {
        let username = std::env::var(DB_USERNAME_ENV)
            .map_err(|_| CredentialsError::MissingEnvVar(DB_USERNAME_ENV))?;
        let password = std::env::var(DB_PASSWORD_ENV)
            .map_err(|_| CredentialsError::MissingEnvVar(DB_PASSWORD_ENV))?;
        Ok(Self {
            username,
            password: SecretString::from(password),
        })
    }
}

impl Default for DatabaseCredentials {
    fn default() -> Self {
        Self {
            username: String::new(),
            password: SecretString::from(String::new()),
        }
    }
}

/// An error returned while reading database credentials from the environment.
#[derive(Debug, Error)]
pub enum CredentialsError {
    #[error("required environment variable `{0}` is not set")]
    MissingEnvVar(&'static str),
}

/// Environment variable that supplies the database username.
const DB_USERNAME_ENV: &str = "SPIDER_STORAGE_DB_USERNAME";

/// Environment variable that supplies the database password.
const DB_PASSWORD_ENV: &str = "SPIDER_STORAGE_DB_PASSWORD";

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    #[test]
    fn read_credential_from_env() -> anyhow::Result<()> {
        // SAFETY: these variables are read only by this test, so mutating this process-global state
        // does not race with other tests.

        // Both variables set: the credentials are read from the environment.
        unsafe {
            std::env::set_var(DB_USERNAME_ENV, "env-user");
            std::env::set_var(DB_PASSWORD_ENV, "env-pass");
        }
        let both_set = DatabaseCredentials::from_env();

        // A variable unset: an error naming the missing variable is returned.
        unsafe { std::env::remove_var(DB_PASSWORD_ENV) };
        let password_unset = DatabaseCredentials::from_env();

        unsafe { std::env::remove_var(DB_USERNAME_ENV) };

        let credentials = both_set?;
        assert_eq!(credentials.username, "env-user");
        assert_eq!(credentials.password.expose_secret(), "env-pass");
        assert!(matches!(
            password_unset,
            Err(CredentialsError::MissingEnvVar(var)) if var == DB_PASSWORD_ENV
        ));
        Ok(())
    }
}
