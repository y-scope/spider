use secrecy::SecretString;
use serde::{Deserialize, Serialize};

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
