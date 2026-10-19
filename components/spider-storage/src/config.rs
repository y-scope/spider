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

/// The default capacity of each connection's prepared statement cache, equal to sqlx's default.
pub const DEFAULT_STATEMENT_CACHE_CAPACITY: usize = 100;

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

    /// The ID of the `storage_fence` row that this storage service fences its writes with. Storage
    /// services that share a database and a fence ID fence each other.
    #[serde(skip)]
    pub fence_id: u64,

    /// The credentials used to connect to the database.
    ///
    /// By default, Spider expects the database credentials to be supplied through the following
    /// environment variables:
    ///
    /// * Database username: `SPIDER_STORAGE_DB_USERNAME`
    /// * Database password: `SPIDER_STORAGE_DB_PASSWORD`
    ///
    /// These can be overridden by explicitly supplying the `credentials` field in the
    /// configuration file.
    #[serde(skip_serializing)]
    pub credentials: DatabaseCredentials,

    /// The connection pool options.
    #[serde(default)]
    pub pool: DatabasePoolConfig,
}

/// Configuration of the database connection pool.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatabasePoolConfig {
    /// Whether a connection is pinged before it is handed out by the pool.
    pub test_before_acquire: bool,

    /// The capacity of each connection's prepared statement cache.
    pub statement_cache_capacity: usize,

    /// The number of connections the pool maintains even when they are idle.
    pub min_connections: u32,
}

impl Default for DatabasePoolConfig {
    fn default() -> Self {
        Self {
            test_before_acquire: false,
            statement_cache_capacity: DEFAULT_STATEMENT_CACHE_CAPACITY,
            min_connections: 0,
        }
    }
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
    use std::ffi::OsString;

    use secrecy::ExposeSecret;
    use serial_test::serial;

    use super::*;

    /// The database username supplied through the environment in these tests.
    const ENV_USERNAME: &str = "env-user";

    /// The database password supplied through the environment in these tests.
    const ENV_PASSWORD: &str = "env-pass";

    /// Restores the credential environment variables to their saved values when dropped.
    struct CredentialEnvVarsGuard {
        saved_values: [(&'static str, Option<OsString>); 2],
    }

    impl CredentialEnvVarsGuard {
        /// # Returns
        ///
        /// A guard that holds the current values of the credential environment variables.
        fn save() -> Self {
            Self {
                saved_values: [DB_USERNAME_ENV, DB_PASSWORD_ENV]
                    .map(|name| (name, std::env::var_os(name))),
            }
        }
    }

    impl Drop for CredentialEnvVarsGuard {
        fn drop(&mut self) {
            for (name, saved_value) in &self.saved_values {
                // SAFETY: these variables are read only by serialized tests, and the guard is only
                // dropped by serialized tests, so mutating this process-global state does not race
                // with other tests.
                unsafe {
                    match saved_value {
                        Some(saved_value) => std::env::set_var(name, saved_value),
                        None => std::env::remove_var(name),
                    }
                }
            }
        }
    }

    /// Deserializes a [`DatabaseConfig`] from the required keys, with explicit credentials, merged
    /// with `extra_keys`.
    ///
    /// # Returns
    ///
    /// The deserialized config on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`serde_json::from_value`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if `extra_keys` is not a JSON object.
    fn deserialize_with(extra_keys: serde_json::Value) -> serde_json::Result<DatabaseConfig> {
        let mut config = serde_json::json!({
            "host": "db",
            "port": 3306,
            "name": "spider",
            "max_connections": 8,
            "credentials": {"username": "user", "password": "pass"},
        });
        let serde_json::Value::Object(extra_keys) = extra_keys else {
            panic!("the extra keys should be a JSON object");
        };
        config
            .as_object_mut()
            .expect("the base config should be a JSON object")
            .extend(extra_keys);
        serde_json::from_value(config)
    }

    #[test]
    fn deserialize_fills_missing_lever_keys_with_defaults() -> anyhow::Result<()> {
        let config = deserialize_with(serde_json::json!({}))?;

        assert_eq!(config.fence_id, 0);
        assert_eq!(
            config.pool,
            DatabasePoolConfig {
                test_before_acquire: false,
                statement_cache_capacity: 100,
                min_connections: 0,
            }
        );
        Ok(())
    }

    #[test]
    fn deserialize_parses_every_lever_key() -> anyhow::Result<()> {
        let config = deserialize_with(serde_json::json!({
            "pool": {
                "test_before_acquire": true,
                "statement_cache_capacity": 7,
                "min_connections": 3,
            },
        }))?;

        assert_eq!(
            config.pool,
            DatabasePoolConfig {
                test_before_acquire: true,
                statement_cache_capacity: 7,
                min_connections: 3,
            }
        );
        Ok(())
    }

    #[test]
    fn deserialize_fills_missing_pool_keys_with_defaults() -> anyhow::Result<()> {
        let config = deserialize_with(serde_json::json!({"pool": {"min_connections": 4}}))?;

        assert_eq!(
            config.pool,
            DatabasePoolConfig {
                test_before_acquire: false,
                statement_cache_capacity: 100,
                min_connections: 4,
            }
        );
        Ok(())
    }

    #[test]
    fn deserialize_rejects_unknown_pool_keys() {
        let misspelled_pool_key =
            deserialize_with(serde_json::json!({"pool": {"test_before_acquir": false}}));

        assert!(
            misspelled_pool_key
                .expect_err("an unknown pool key should be rejected")
                .to_string()
                .contains("unknown field `test_before_acquir`")
        );
    }

    #[test]
    fn deserialize_accepts_unknown_top_level_keys() -> anyhow::Result<()> {
        let config = deserialize_with(serde_json::json!({
            "fence_in_write_statement": false,
            "fence_id": 7,
        }))?;

        assert_eq!(config.fence_id, 0);
        assert_eq!(config.pool, DatabasePoolConfig::default());
        Ok(())
    }

    #[serial]
    #[test]
    fn deserialize_resolves_credentials_from_env() -> anyhow::Result<()> {
        let _credential_env_vars_guard = CredentialEnvVarsGuard::save();
        // SAFETY: these variables are read only by serialized tests, so mutating this
        // process-global state does not race with other tests.
        unsafe {
            std::env::set_var(DB_USERNAME_ENV, ENV_USERNAME);
            std::env::set_var(DB_PASSWORD_ENV, ENV_PASSWORD);
        }
        let config: DatabaseConfig = serde_json::from_str(
            r#"{"host": "db", "port": 3306, "name": "spider", "max_connections": 8}"#,
        )?;

        assert_eq!(config.credentials.username, ENV_USERNAME);
        assert_eq!(config.credentials.password.expose_secret(), ENV_PASSWORD);
        Ok(())
    }

    #[serial]
    #[test]
    fn read_credential_from_env() -> anyhow::Result<()> {
        let _credential_env_vars_guard = CredentialEnvVarsGuard::save();
        // SAFETY: these variables are read only by serialized tests, so mutating this
        // process-global state does not race with other tests.

        // Both variables set: the credentials are read from the environment.
        unsafe {
            std::env::set_var(DB_USERNAME_ENV, ENV_USERNAME);
            std::env::set_var(DB_PASSWORD_ENV, ENV_PASSWORD);
        }
        let both_set = DatabaseCredentials::from_env();

        // A variable unset: an error naming the missing variable is returned.
        unsafe { std::env::remove_var(DB_PASSWORD_ENV) };
        let password_unset = DatabaseCredentials::from_env();

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
