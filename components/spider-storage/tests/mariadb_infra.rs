use secrecy::SecretString;
use spider_core::types::id::ResourceGroupId;
use spider_storage::DatabaseConfig;
use spider_storage::db::MariaDbStorageConnector;
use spider_storage::db::ResourceGroupManagement;

/// Creates a [`MariaDbStorageConnector`] from environment variables.
///
/// # Returns
///
/// A connected [`MariaDbStorageConnector`] configured from environment variables.
///
/// # Panics
///
/// Panics if any required environment variable (`MARIADB_PORT`, `MARIADB_DATABASE`,
/// `MARIADB_USERNAME`, `MARIADB_PASSWORD`) is missing or if the connection fails.
pub async fn create_mariadb_connector() -> MariaDbStorageConnector {
    MariaDbStorageConnector::connect(&create_mariadb_config())
        .await
        .expect("connect failed")
}

/// Creates a [`DatabaseConfig`] from environment variables.
///
/// # Returns
///
/// A [`DatabaseConfig`] configured from environment variables.
///
/// # Panics
///
/// Panics if:
///
/// * Any required environment variable (`MARIADB_PORT`, `MARIADB_DATABASE`, `MARIADB_USERNAME`, or
///   `MARIADB_PASSWORD`) is missing.
/// * The value of `MARIADB_PORT` is invalid.
#[must_use]
pub fn create_mariadb_config() -> DatabaseConfig {
    let port: u16 = std::env::var("MARIADB_PORT")
        .expect("MARIADB_PORT")
        .parse()
        .expect("valid port");
    let database = std::env::var("MARIADB_DATABASE").expect("MARIADB_DATABASE");
    let username = std::env::var("MARIADB_USERNAME").expect("MARIADB_USERNAME");
    let password = std::env::var("MARIADB_PASSWORD").expect("MARIADB_PASSWORD");

    DatabaseConfig {
        host: "localhost".to_string(),
        port,
        name: database,
        username,
        password: SecretString::from(password),
        max_connections: 5,
    }
}

/// Registers a new resource group with a random external ID and a fixed test password.
///
/// # Returns
///
/// The [`ResourceGroupId`] of the newly created resource group.
///
/// # Panics
///
/// Panics if the resource group creation fails.
pub async fn create_test_resource_group(storage: &MariaDbStorageConnector) -> ResourceGroupId {
    let external_id = format!("test-resource-group-{}", rand::random::<u64>());
    storage
        .add(external_id, b"test-password".to_vec())
        .await
        .expect("add should succeed")
}
