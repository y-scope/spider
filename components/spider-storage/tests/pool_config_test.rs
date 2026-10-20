use std::time::Duration;

use secrecy::ExposeSecret;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::resource_group::ExternalResourceGroupCredentials;
use spider_storage::DatabaseConfig;
use spider_storage::DatabasePoolConfig;
use spider_storage::db::DbError;
use spider_storage::db::MariaDbStorageConnector;
use spider_storage::db::ResourceGroupManagement;
use sqlx::ConnectOptions;
use sqlx::Connection;
use sqlx::MySqlConnection;
use sqlx::mysql::MySqlConnectOptions;

use super::mariadb_infra::IsolatedTestDb;
use super::mariadb_infra::create_test_resource_group;

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn check_connection_before_use_replaces_a_killed_idle_connection() -> anyhow::Result<()> {
    let result = add_resource_group_after_killing_the_idle_connection(true).await;

    assert!(
        result.is_ok(),
        "expected the pool to replace the killed connection, got {result:?}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn no_check_connection_before_use_hands_out_a_killed_idle_connection() -> anyhow::Result<()> {
    let result = add_resource_group_after_killing_the_idle_connection(false).await;

    assert!(
        matches!(result, Err(DbError::Sql(sqlx::Error::Io(_)))),
        "expected an I/O error from the killed connection, got {result:?}"
    );
    Ok(())
}

/// Connects a storage connector with a single pooled connection, uses that connection once, kills
/// it while it is idle, and then adds a resource group through the connector.
///
/// # Returns
///
/// The result of [`ResourceGroupManagement::add`].
///
/// # Panics
///
/// Panics if:
///
/// * The connector fails to connect.
/// * The kill fails.
/// * Forwards [`IsolatedTestDb::create`]'s, [`IsolatedTestDb::drop_database`]'s,
///   [`create_test_resource_group`]'s, [`connect_outside_database`]'s and
///   [`wait_for_connection_ids`]'s panics.
async fn add_resource_group_after_killing_the_idle_connection(
    check_connection_before_use: bool,
) -> Result<ResourceGroupId, DbError> {
    const CONNECTION_IDS_QUERY: &str =
        "SELECT `ID` FROM `information_schema`.`PROCESSLIST` WHERE `DB` = ?;";
    const IDLE_CONNECTION_IDS_QUERY: &str = "SELECT `ID` FROM `information_schema`.`PROCESSLIST` \
                                             WHERE `DB` = ? AND `COMMAND` = 'Sleep';";

    let isolated_test_db = IsolatedTestDb::create().await;
    let base_config = isolated_test_db.config();
    let config = DatabaseConfig {
        max_connections: 1,
        pool: DatabasePoolConfig {
            check_connection_before_use,
        },
        ..base_config
    };
    let connector = MariaDbStorageConnector::connect(&config)
        .await
        .expect("connector should connect");
    create_test_resource_group(&connector).await;
    let mut observer = connect_outside_database(&config).await;

    let idle_connection_ids = wait_for_connection_ids(
        &mut observer,
        IDLE_CONNECTION_IDS_QUERY,
        &config.name,
        |ids| ids.len() == 1,
    )
    .await;
    for connection_id in idle_connection_ids {
        sqlx::raw_sql(&format!("KILL CONNECTION {connection_id};"))
            .execute(&mut observer)
            .await
            .expect("killing the idle pooled connection should succeed");
    }
    wait_for_connection_ids(
        &mut observer,
        CONNECTION_IDS_QUERY,
        &config.name,
        <[i64]>::is_empty,
    )
    .await;

    let result = connector
        .add(ExternalResourceGroupCredentials::new(
            format!("pool-config-test-{}", rand::random::<u64>()),
            b"test-password".to_vec(),
        ))
        .await;

    drop(connector);
    observer
        .close()
        .await
        .expect("closing the observer connection should succeed");
    isolated_test_db.drop_database().await;
    result
}

/// Opens a connection with `config`'s credentials that selects no database, so that it is not
/// one of the connections to `config.name`.
///
/// # Returns
///
/// The opened connection.
///
/// # Panics
///
/// Panics if the connection fails.
async fn connect_outside_database(config: &DatabaseConfig) -> MySqlConnection {
    MySqlConnectOptions::new()
        .host(&config.host)
        .port(config.port)
        .username(&config.credentials.username)
        .password(config.credentials.password.expose_secret())
        .connect()
        .await
        .expect("observer connection should succeed")
}

/// Polls the connection IDs that `query` selects for the database `name` until `is_done` accepts
/// them.
///
/// # Returns
///
/// The accepted connection IDs.
///
/// # Panics
///
/// Panics if:
///
/// * `query` fails.
/// * `is_done` accepts no polled IDs within `POLL_ATTEMPTS` attempts.
async fn wait_for_connection_ids(
    observer: &mut MySqlConnection,
    query: &'static str,
    name: &str,
    is_done: impl Fn(&[i64]) -> bool,
) -> Vec<i64> {
    const POLL_INTERVAL: Duration = Duration::from_millis(20);
    const POLL_ATTEMPTS: usize = 250;

    let mut connection_ids = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        connection_ids = sqlx::query_scalar(query)
            .bind(name)
            .fetch_all(&mut *observer)
            .await
            .expect("process list query should succeed");
        if is_done(&connection_ids) {
            return connection_ids;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    panic!("connection IDs to `{name}` did not reach the expected state: {connection_ids:?}");
}
