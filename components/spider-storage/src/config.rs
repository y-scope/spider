use std::{fs::File, io, net::IpAddr, path::Path};

use secrecy::SecretString;
use serde::{Deserialize, Serialize};
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

impl ServerConfig {
    /// Loads a [`ServerConfig`] from the YAML file at the given path.
    ///
    /// # Returns
    ///
    /// The parsed [`ServerConfig`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`yaml_serde::from_reader`]'s return values on failure.
    pub fn from_yaml_file(path: &Path) -> Result<Self, ConfigError> {
        let file = File::open(path)?;
        let config = yaml_serde::from_reader(file)?;
        Ok(config)
    }
}

/// Errors returned while loading a [`ServerConfig`].
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Forwards an error from opening the configuration file.
    #[error("failed to open config file: {0}")]
    Io(#[from] io::Error),

    /// Forwards an error from deserializing the YAML configuration.
    #[error("failed to parse config file: {0}")]
    Parse(#[from] yaml_serde::Error),
}

/// Configuration parameters for connecting to the database.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub username: String,
    #[serde(skip_serializing)]
    pub password: SecretString,
    pub max_connections: u32,
}
