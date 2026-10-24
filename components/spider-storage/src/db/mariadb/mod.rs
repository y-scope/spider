mod execution_manager;
mod fence;
mod job;
mod mysql_error;
mod resource_group;
mod scheduler;
mod schema;

use std::time::Duration;

use const_format::formatcp;
use secrecy::ExposeSecret;
use spider_core::types::id::SessionId;
use sqlx::Connection;
use sqlx::MySqlPool;
use sqlx::mysql::MySqlPoolOptions;

use crate::config::DatabaseConfig;
use crate::db::DbError;
use crate::db::DbStorage;
use crate::db::SessionManagement;
use crate::db::mariadb::fence::DEFAULT_FENCE_ID;
use crate::db::mariadb::fence::bump_fence;
use crate::db::mariadb::schema::SESSIONS_TABLE_NAME;
use crate::db::mariadb::schema::STORAGE_FENCE_TABLE_NAME;
use crate::db::mariadb::schema::create_tables;

/// A cloneable storage connector for `MariaDB` database that implements Spider's DB protocols.
#[derive(Clone)]
pub struct MariaDbStorageConnector {
    pool: MySqlPool,
    session_id: SessionId,
    fence_id: u64,
}

impl MariaDbStorageConnector {
    /// Connects to database, initializes tables, and fences off every storage session that
    /// connected earlier with the same fence ID.
    ///
    /// # Returns
    ///
    /// A newly created [`MariaDbStorageConnector`] instance for connection on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::pool::PoolOptions::connect_with`]'s return values on failure.
    /// * Forwards [`create_tables`]'s return values on failure.
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    /// * Forwards [`bump_fence`]'s return values on failure.
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        const SET_WAIT_TIMEOUT_QUERY: &str =
            formatcp!("SET SESSION wait_timeout = {SESSION_WAIT_TIMEOUT_SEC};");
        const BUMP_SESSION_ID_QUERY: &str = formatcp!(
            "INSERT INTO `{table}` () VALUES ();",
            table = SESSIONS_TABLE_NAME,
        );
        const INSERT_FENCE_QUERY: &str = formatcp!(
            "INSERT IGNORE INTO `{table}` (`id`, `session_id`) VALUES (?, 0);",
            table = STORAGE_FENCE_TABLE_NAME,
        );

        let mysql_options = sqlx::mysql::MySqlConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .database(&config.name)
            .username(&config.credentials.username)
            .password(config.credentials.password.expose_secret());

        let pool = Self::pool_options(config)
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query(SET_WAIT_TIMEOUT_QUERY)
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(mysql_options)
            .await?;

        create_tables(&pool).await?;

        let fence_id = config.fence_id.unwrap_or(DEFAULT_FENCE_ID);
        sqlx::query(INSERT_FENCE_QUERY)
            .bind(fence_id)
            .execute(&pool)
            .await?;

        let session_id = SessionId::from(
            sqlx::query(BUMP_SESSION_ID_QUERY)
                .execute(&pool)
                .await?
                .last_insert_id(),
        );
        bump_fence(&pool, fence_id, session_id).await?;

        Ok(Self {
            pool,
            session_id,
            fence_id,
        })
    }

    /// # Returns
    ///
    /// The connection pool options built from `config`.
    fn pool_options(config: &DatabaseConfig) -> MySqlPoolOptions {
        let pool_options = MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .test_before_acquire(config.pool.check_connection_before_use);
        if config.pool.check_connection_before_use {
            return pool_options;
        }
        pool_options.max_lifetime(Duration::from_secs(UNPINGED_CONNECTION_MAX_LIFETIME_SEC))
    }
}

impl SessionManagement for MariaDbStorageConnector {
    fn session_id(&self) -> SessionId {
        self.session_id
    }
}

impl DbStorage for MariaDbStorageConnector {}

/// The number of seconds that a database session may idle before the server closes it, which must
/// stay above twice [`UNPINGED_CONNECTION_MAX_LIFETIME_SEC`].
const SESSION_WAIT_TIMEOUT_SEC: u64 = 900;

/// The maximum lifetime in seconds of a pooled connection when the pool does not ping a connection
/// before handing it out.
///
/// sqlx checks the lifetime of an idle connection only in its maintenance pass, which runs once per
/// this lifetime, so a pooled connection can idle on the server for up to twice this lifetime.
const UNPINGED_CONNECTION_MAX_LIFETIME_SEC: u64 = 300;
const _: () = assert!(2 * UNPINGED_CONNECTION_MAX_LIFETIME_SEC < SESSION_WAIT_TIMEOUT_SEC);

/// Runs `tx` on a freshly acquired pooled connection whose next transaction uses the `READ
/// COMMITTED` isolation level.
///
/// A `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` statement is issued on the connection before
/// `tx` runs. Because it omits `SESSION`/`GLOBAL`, it applies only to the next transaction started
/// on that connection. `tx` is expected to begin exactly one transaction to consume the setting,
/// and to commit or roll it back itself.
///
/// If `tx` returns an error, the connection is detached from the pool and closed rather than being
/// released back into it. This ensures a failed attempt can never hand a later, unrelated borrower
/// a connection still carrying the pending isolation change (or a half-open transaction).
///
/// # Type Parameters
///
/// * `ReturnType` - The return type of `tx`.
/// * `TransactionType` - The type of `tx`, which is an async function that takes a mutable
///   reference to the connection.
///
/// # Returns
///
/// The value returned by `tx` on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::Pool::acquire`]'s return values on failure.
/// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
/// * Forwards `tx`'s return values on failure.
async fn run_read_committed_tx<ReturnType, TransactionType>(
    pool: MySqlPool,
    tx: TransactionType,
) -> Result<ReturnType, DbError>
where
    for<'connection_lifetime> TransactionType:
        AsyncFnOnce(&'connection_lifetime mut sqlx::MySqlConnection) -> Result<ReturnType, DbError>,
{
    const SET_READ_COMMITTED: &str = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED";
    let mut conn = pool.acquire().await?;
    sqlx::query(SET_READ_COMMITTED).execute(&mut *conn).await?;
    let result = tx(&mut *conn).await;
    if result.is_err() {
        let _ = conn.detach().close().await;
    }
    result
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use secrecy::SecretString;
    use serial_test::serial;
    use sqlx::mysql::MySqlPoolOptions;

    use super::MariaDbStorageConnector;
    use super::SESSION_WAIT_TIMEOUT_SEC;
    use super::UNPINGED_CONNECTION_MAX_LIFETIME_SEC;
    use crate::config::DatabaseConfig;
    use crate::config::DatabaseCredentials;
    use crate::config::DatabasePoolConfig;

    /// # Returns
    ///
    /// A config of the test `MariaDB` database that the environment names, with a random fence ID
    /// and the default pool options, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`std::env::var`]'s return values on failure.
    /// * Forwards [`str::parse`]'s return values on failure.
    /// * Forwards [`DatabaseCredentials::from_env`]'s return values on failure.
    pub(super) fn mariadb_config() -> anyhow::Result<DatabaseConfig> {
        Ok(DatabaseConfig {
            host: "localhost".to_owned(),
            port: std::env::var("MARIADB_PORT")?.parse()?,
            name: std::env::var("MARIADB_DATABASE")?,
            fence_id: Some(rand::random()),
            credentials: DatabaseCredentials::from_env()?,
            ..base_config()
        })
    }

    /// # Returns
    ///
    /// A config of a database that the tests never connect to, with the default pool options.
    fn base_config() -> DatabaseConfig {
        DatabaseConfig {
            host: "db".to_owned(),
            port: 3306,
            name: "spider".to_owned(),
            max_connections: 8,
            fence_id: None,
            credentials: DatabaseCredentials {
                username: "user".to_owned(),
                password: SecretString::from("pass"),
            },
            pool: DatabasePoolConfig::default(),
        }
    }

    #[test]
    fn pool_options_apply_the_pool_config() {
        let config = DatabaseConfig {
            max_connections: 8,
            pool: DatabasePoolConfig {
                check_connection_before_use: true,
            },
            ..base_config()
        };

        let options = MariaDbStorageConnector::pool_options(&config);

        assert_eq!(options.get_max_connections(), config.max_connections);
        assert!(options.get_test_before_acquire());
        assert_eq!(
            options.get_max_lifetime(),
            MySqlPoolOptions::new().get_max_lifetime()
        );
    }

    #[test]
    fn pool_options_cap_the_lifetime_without_the_connection_check() {
        let config = DatabaseConfig {
            pool: DatabasePoolConfig {
                check_connection_before_use: false,
            },
            ..base_config()
        };

        let options = MariaDbStorageConnector::pool_options(&config);

        assert!(!options.get_test_before_acquire());
        assert_eq!(
            options.get_max_lifetime(),
            Some(Duration::from_secs(UNPINGED_CONNECTION_MAX_LIFETIME_SEC))
        );
    }

    #[tokio::test]
    #[ignore = "requires MariaDB"]
    #[serial]
    async fn every_pooled_connection_sets_the_session_wait_timeout() -> anyhow::Result<()> {
        const NUM_CONNECTIONS: u32 = 3;

        let config = DatabaseConfig {
            max_connections: NUM_CONNECTIONS,
            ..mariadb_config()?
        };
        let connector = MariaDbStorageConnector::connect(&config).await?;

        let mut connections = Vec::new();
        for _ in 0..NUM_CONNECTIONS {
            connections.push(connector.pool.acquire().await?);
        }
        for connection in &mut connections {
            let wait_timeout: u64 = sqlx::query_scalar("SELECT @@SESSION.wait_timeout;")
                .fetch_one(&mut **connection)
                .await?;
            assert_eq!(wait_timeout, SESSION_WAIT_TIMEOUT_SEC);
        }
        Ok(())
    }
}
