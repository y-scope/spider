use std::net::IpAddr;

use secrecy::SecretString;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::state::runtime::RuntimeConfig;

/// Environment variable that supplies the database username.
pub const DB_USERNAME_ENV: &str = "SPIDER_STORAGE_DB_USERNAME";

/// Environment variable that supplies the database password.
pub const DB_PASSWORD_ENV: &str = "SPIDER_STORAGE_DB_PASSWORD";

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
    /// The database host.
    pub host: String,

    /// The database port.
    pub port: u16,

    /// The name of the database.
    pub name: String,

    /// The maximum number of database connections maintained by the pool.
    pub max_connections: u32,

    /// The credentials used to connect to the database.
    ///
    /// When omitted, the credentials are read from the following environment variables:
    ///
    /// * Database username: `SPIDER_STORAGE_DB_USERNAME`
    /// * Database password: `SPIDER_STORAGE_DB_PASSWORD`
    #[serde(skip_serializing)]
    pub credentials: DatabaseCredentials,
}

/// Credentials for authenticating with the database.
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "Option<RawCredentials>")]
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

/// An error returned while reading database credentials from the environment.
#[derive(Debug, Error)]
pub enum CredentialsError {
    #[error("required environment variable `{0}` is not set")]
    MissingEnvVar(&'static str),
}

#[derive(Deserialize)]
struct RawCredentials {
    username: String,
    password: String,
}

impl TryFrom<Option<RawCredentials>> for DatabaseCredentials {
    type Error = CredentialsError;

    fn try_from(raw: Option<RawCredentials>) -> Result<Self, Self::Error> {
        match raw {
            Some(RawCredentials { username, password }) => Ok(Self {
                username,
                password: SecretString::from(password),
            }),
            None => Self::from_env(),
        }
    }
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;
    use serial_test::serial;

    use super::*;

    /// The database username supplied through the environment in these tests.
    const ENV_USERNAME: &str = "env-user";

    /// The database password supplied through the environment in these tests.
    const ENV_PASSWORD: &str = "env-pass";

    #[test]
    #[serial]
    fn deserialize_resolves_credentials_from_env() -> anyhow::Result<()> {
        // SAFETY: these variables are read-only by the serialized credential tests, so mutating
        // this process-global state does not race with other tests.
        unsafe {
            std::env::set_var(DB_USERNAME_ENV, ENV_USERNAME);
            std::env::set_var(DB_PASSWORD_ENV, ENV_PASSWORD);
        }
        let config: DatabaseConfig = serde_json::from_str(
            r#"{"host": "db", "port": 3306, "name": "spider", "max_connections": 8}"#,
        )?;
        unsafe {
            std::env::remove_var(DB_USERNAME_ENV);
            std::env::remove_var(DB_PASSWORD_ENV);
        }

        assert_eq!(config.credentials.username, ENV_USERNAME);
        assert_eq!(config.credentials.password.expose_secret(), ENV_PASSWORD);
        Ok(())
    }

    #[test]
    #[serial]
    fn read_credential_from_env() -> anyhow::Result<()> {
        // SAFETY: these variables are read only by this test, so mutating this process-global state
        // does not race with other tests.

        // Both variables set: the credentials are read from the environment.
        unsafe {
            std::env::set_var(DB_USERNAME_ENV, ENV_USERNAME);
            std::env::set_var(DB_PASSWORD_ENV, ENV_PASSWORD);
        }
        let both_set = DatabaseCredentials::from_env();

        // A variable unset: an error naming the missing variable is returned.
        unsafe { std::env::remove_var(DB_PASSWORD_ENV) };
        let password_unset = DatabaseCredentials::from_env();

        unsafe { std::env::remove_var(DB_USERNAME_ENV) };

        let credentials = both_set?;
        assert_eq!(credentials.username, ENV_USERNAME);
        assert_eq!(credentials.password.expose_secret(), ENV_PASSWORD);
        assert!(matches!(
            password_unset,
            Err(CredentialsError::MissingEnvVar(var)) if var == DB_PASSWORD_ENV
        ));
        Ok(())
    }
}
