use std::net::IpAddr;

use secrecy::SecretString;
use serde::Deserialize;
use serde::Serialize;

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
    pub username: String,
    #[serde(skip_serializing)]
    pub password: SecretString,
    pub max_connections: u32,
}
