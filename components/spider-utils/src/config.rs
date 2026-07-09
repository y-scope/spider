use std::fs::File;
use std::io;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tonic::transport::Endpoint;

/// Errors returned while loading a yaml formatted configuration file.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Forwards an error from opening the configuration file.
    #[error("failed to open config file: {0}")]
    Io(#[from] io::Error),

    /// Forwards an error from deserializing the YAML configuration.
    #[error("failed to parse config file: {0}")]
    Parse(#[from] yaml_serde::Error),
}

/// A configuration type that can be loaded from a YAML file.
///
/// A blanket impl covers every [`DeserializeOwned`] type, so any config struct that derives
/// [`serde::Deserialize`] gets [`from_yaml_file`](YamlConfig::from_yaml_file) for free.
pub trait YamlConfig: DeserializeOwned {
    /// Loads the configuration from the YAML file at `path`.
    ///
    /// # Returns
    ///
    /// The parsed configuration on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ConfigError::Io`] if the file cannot be opened.
    /// * [`ConfigError::Parse`] if the YAML cannot be deserialized.
    fn from_yaml_file(path: &Path) -> Result<Self, ConfigError> {
        let file = File::open(path)?;
        let config = yaml_serde::from_reader(file)?;
        Ok(config)
    }
}

impl<ConfigType: DeserializeOwned> YamlConfig for ConfigType {}

/// The network location of a gRPC server.
#[derive(Clone, Debug, Deserialize)]
pub struct EndpointConfig {
    pub host: IpAddr,
    pub port: u16,
}

impl EndpointConfig {
    /// # Returns
    ///
    /// This endpoint's `host:port` as a [`SocketAddr`].
    #[must_use]
    pub const fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }

    /// Builds a tonic [`Endpoint`] pointing at this `host:port` over plaintext HTTP/2.
    ///
    /// # Returns
    ///
    /// The constructed [`Endpoint`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Endpoint::from_shared`]'s return values on failure.
    pub fn endpoint(&self) -> Result<Endpoint, tonic::transport::Error> {
        Endpoint::from_shared(format!("http://{}", self.socket_addr()))
    }
}
