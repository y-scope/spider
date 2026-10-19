use std::env::VarError;
use std::fmt::Display;

use secrecy::ExposeSecret;
use secrecy::SecretString;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::resource_group::ExternalResourceGroupCredentials;
use spider_storage::DatabaseConfig;
use spider_storage::DatabaseCredentials;
use spider_storage::DatabasePoolConfig;
use spider_storage::db::MariaDbStorageConnector;
use spider_storage::db::ResourceGroupManagement;
use sqlx::ConnectOptions;
use sqlx::Connection;
use sqlx::MySqlPool;
use sqlx::mysql::MySqlConnectOptions;
use sqlx::mysql::MySqlDatabaseError;
use sqlx::mysql::MySqlPoolOptions;

/// A uniquely named database that exists for the duration of one test.
///
/// The database is dropped by [`Self::drop_database`], or otherwise when the value is dropped,
/// including when the test panics.
pub struct ScratchDatabase {
    name: String,
    test_username: String,
    root_options: MySqlConnectOptions,
    is_dropped: bool,
}

impl ScratchDatabase {
    /// Creates a uniquely named database and grants the test user every privilege on it.
    ///
    /// # Returns
    ///
    /// The newly created [`ScratchDatabase`].
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * The connection as `root` fails.
    /// * The database creation or the grant fails.
    /// * Forwards [`create_mariadb_config`]'s and [`mariadb_root_password`]'s panics.
    pub async fn create() -> Self {
        let base_config = create_mariadb_config();
        let root_options = MySqlConnectOptions::new()
            .host(&base_config.host)
            .port(base_config.port)
            .username("root")
            .password(&mariadb_root_password());
        let mut root_connection = root_options.connect().await.expect(
            "failed to connect as `root` with the password in `MARIADB_ROOT_PASSWORD`, or in \
             `SPIDER_STORAGE_DB_PASSWORD` if it is unset",
        );

        let name = format!("scratch_{:016x}", rand::random::<u64>());
        sqlx::raw_sql(&format!("CREATE DATABASE `{name}`;"))
            .execute(&mut root_connection)
            .await
            .expect("scratch database creation should succeed");
        let scratch = Self {
            name,
            test_username: base_config.credentials.username,
            root_options,
            is_dropped: false,
        };
        sqlx::raw_sql(&format!(
            "GRANT ALL PRIVILEGES ON `{name}`.* TO '{username}'@'%';",
            name = scratch.name,
            username = scratch.test_username,
        ))
        .execute(&mut root_connection)
        .await
        .expect("scratch database grant should succeed");
        root_connection
            .close()
            .await
            .expect("closing the `root` connection should succeed");
        scratch
    }

    /// # Returns
    ///
    /// A [`DatabaseConfig`] for this database, with a random fence ID.
    ///
    /// # Panics
    ///
    /// Forwards [`create_mariadb_config`]'s panics.
    #[must_use]
    pub fn config(&self) -> DatabaseConfig {
        DatabaseConfig {
            name: self.name.clone(),
            ..create_mariadb_config()
        }
    }

    /// Creates a user that holds every privilege on this database and no global privilege. The
    /// user is dropped with the database.
    ///
    /// # Returns
    ///
    /// The credentials of the new user.
    ///
    /// # Panics
    ///
    /// Forwards [`Self::create_user`]'s panics.
    pub async fn create_user_without_global_privileges(&self) -> DatabaseCredentials {
        self.create_user(false).await
    }

    /// Creates a user that holds every privilege on this database and, as its only global
    /// privilege, `PROCESS`. The user is dropped with the database.
    ///
    /// # Returns
    ///
    /// The credentials of the new user.
    ///
    /// # Panics
    ///
    /// Forwards [`Self::create_user`]'s panics.
    pub async fn create_user_with_the_process_privilege(&self) -> DatabaseCredentials {
        self.create_user(true).await
    }

    /// Drops the database.
    ///
    /// # Panics
    ///
    /// Panics if the database drop fails.
    pub async fn drop_database(mut self) {
        drop_scratch_database(&self.root_options, &self.name, &self.test_username)
            .await
            .expect("scratch database drop should succeed");
        self.is_dropped = true;
    }

    /// Creates a user that holds every privilege on this database, and the global `PROCESS`
    /// privilege if `has_process_privilege` is set.
    ///
    /// # Returns
    ///
    /// The credentials of the new user.
    ///
    /// # Panics
    ///
    /// Panics if the connection as `root`, the user creation or a grant fails.
    async fn create_user(&self, has_process_privilege: bool) -> DatabaseCredentials {
        const PASSWORD: &str = "scratch-password";

        let username = scratch_username(&self.name, has_process_privilege);
        let mut root_connection = self
            .root_options
            .connect()
            .await
            .expect("the connection as `root` should succeed");
        sqlx::raw_sql(&format!(
            "CREATE USER '{username}'@'%' IDENTIFIED BY '{PASSWORD}'; GRANT ALL PRIVILEGES ON \
             `{name}`.* TO '{username}'@'%';",
            name = self.name,
        ))
        .execute(&mut root_connection)
        .await
        .expect("the scratch user creation should succeed");
        if has_process_privilege {
            sqlx::raw_sql(&format!("GRANT PROCESS ON *.* TO '{username}'@'%';"))
                .execute(&mut root_connection)
                .await
                .expect("the `PROCESS` grant should succeed");
        }
        root_connection
            .close()
            .await
            .expect("closing the `root` connection should succeed");
        DatabaseCredentials {
            username,
            password: SecretString::from(PASSWORD),
        }
    }
}

impl Drop for ScratchDatabase {
    fn drop(&mut self) {
        if self.is_dropped {
            return;
        }
        let root_options = self.root_options.clone();
        let name = self.name.clone();
        let test_username = self.test_username.clone();
        let drop_result = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(sqlx::Error::Io)?
                .block_on(async {
                    kill_scratch_database_connections(&root_options, &name).await?;
                    drop_scratch_database(&root_options, &name, &test_username).await
                })
        })
        .join();
        match drop_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("Failed to drop scratch database `{}`: {e}", self.name),
            Err(_) => eprintln!("Panicked while dropping scratch database `{}`.", self.name),
        }
    }
}

/// Creates a [`MariaDbStorageConnector`] from environment variables.
///
/// # Returns
///
/// A connected [`MariaDbStorageConnector`] configured from environment variables.
///
/// # Panics
///
/// Panics if the connection fails, or forwards [`create_mariadb_config`]'s panics.
pub async fn create_mariadb_connector() -> MariaDbStorageConnector {
    MariaDbStorageConnector::connect(&create_mariadb_config())
        .await
        .expect("connect failed")
}

/// Creates a [`DatabaseConfig`] from environment variables.
///
/// # Returns
///
/// A [`DatabaseConfig`] configured from environment variables, with a random fence ID and the
/// overrides of [`apply_test_db_overrides`].
///
/// # Panics
///
/// Panics if:
///
/// * Any required environment variable (`MARIADB_PORT`, `MARIADB_DATABASE`,
///   `SPIDER_STORAGE_DB_USERNAME`, or `SPIDER_STORAGE_DB_PASSWORD`) is missing.
/// * The value of `MARIADB_PORT` is invalid.
/// * Forwards [`apply_test_db_overrides`]'s panics.
#[must_use]
pub fn create_mariadb_config() -> DatabaseConfig {
    let port: u16 = std::env::var("MARIADB_PORT")
        .expect("MARIADB_PORT")
        .parse()
        .expect("valid port");
    let database = std::env::var("MARIADB_DATABASE").expect("MARIADB_DATABASE");

    let mut config = DatabaseConfig {
        host: "localhost".to_string(),
        port,
        name: database,
        max_connections: 5,
        fence_id: rand::random(),
        credentials: DatabaseCredentials::from_env().expect("database credentials"),
        pool: DatabasePoolConfig::default(),
    };
    apply_test_db_overrides(&mut config);
    config
}

/// Overrides the database flags of `config` with the values of the following environment
/// variables, for each one that is set:
///
/// * `SPIDER_TEST_DB_CHECK_CONNECTION_BEFORE_USE` (`true` or `false`):
///   `pool.check_connection_before_use`.
///
/// # Panics
///
/// Forwards [`read_test_db_override`]'s panics.
pub fn apply_test_db_overrides(config: &mut DatabaseConfig) {
    if let Some(value) =
        read_test_db_override("SPIDER_TEST_DB_CHECK_CONNECTION_BEFORE_USE", str::parse)
    {
        config.pool.check_connection_before_use = value;
    }
}

/// Reads the `MariaDB` root password from `MARIADB_ROOT_PASSWORD`, falling back to
/// `SPIDER_STORAGE_DB_PASSWORD`, since the test container of `tools/scripts/mariadb/start.py` sets
/// both passwords to the same value.
///
/// # Returns
///
/// The `MariaDB` root password.
///
/// # Panics
///
/// Panics if neither environment variable is set.
#[must_use]
pub fn mariadb_root_password() -> String {
    std::env::var("MARIADB_ROOT_PASSWORD")
        .or_else(|_| std::env::var("SPIDER_STORAGE_DB_PASSWORD"))
        .expect("`MARIADB_ROOT_PASSWORD` or `SPIDER_STORAGE_DB_PASSWORD` should be set")
}

/// # Returns
///
/// The credentials of the `MariaDB` root user.
///
/// # Panics
///
/// Forwards [`mariadb_root_password`]'s panics.
#[must_use]
pub fn root_credentials() -> DatabaseCredentials {
    DatabaseCredentials {
        username: "root".to_owned(),
        password: SecretString::from(mariadb_root_password()),
    }
}

/// Creates a pool connected to `config`'s database with `config`'s credentials, without any of
/// the storage connector's initialization.
///
/// # Returns
///
/// The connected pool.
///
/// # Panics
///
/// Panics if the connection fails.
pub async fn create_raw_pool(config: &DatabaseConfig) -> MySqlPool {
    let options = MySqlConnectOptions::new()
        .host(&config.host)
        .port(config.port)
        .database(&config.name)
        .username(&config.credentials.username)
        .password(config.credentials.password.expose_secret());
    MySqlPoolOptions::new()
        .max_connections(config.max_connections)
        .connect_with(options)
        .await
        .expect("raw pool connection should succeed")
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
        .add(ExternalResourceGroupCredentials::new(
            external_id,
            b"test-password".to_vec(),
        ))
        .await
        .expect("add should succeed")
}

/// # Returns
///
/// Whether `error` is a database error whose `MySQL` error number is `number`.
#[must_use]
pub fn has_mysql_error_number(error: &sqlx::Error, number: u16) -> bool {
    let sqlx::Error::Database(database_error) = error else {
        return false;
    };
    database_error
        .try_downcast_ref::<MySqlDatabaseError>()
        .is_some_and(|mysql_error| mysql_error.number() == number)
}

/// Kills every connection to the database `name` over a new connection made with `root_options`.
///
/// A test that panics can leave a transaction open on a connection that only its own, now
/// blocked, runtime would roll back. Killing the connection releases the transaction's locks, so
/// that the database can be dropped.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::ConnectOptions::connect`]'s return values on failure.
/// * Forwards [`sqlx::query::QueryScalar::fetch_all`]'s return values on failure.
/// * Forwards [`sqlx::Connection::close`]'s return values on failure.
async fn kill_scratch_database_connections(
    root_options: &MySqlConnectOptions,
    name: &str,
) -> Result<(), sqlx::Error> {
    let mut root_connection = root_options.connect().await?;
    let connection_ids: Vec<i64> =
        sqlx::query_scalar("SELECT `ID` FROM `information_schema`.`PROCESSLIST` WHERE `DB` = ?;")
            .bind(name)
            .fetch_all(&mut root_connection)
            .await?;
    for connection_id in connection_ids {
        if let Err(e) = sqlx::raw_sql(&format!("KILL CONNECTION {connection_id};"))
            .execute(&mut root_connection)
            .await
        {
            eprintln!(
                "Failed to kill connection {connection_id} to `{name}`, which may have ended: {e}"
            );
        }
    }
    root_connection.close().await
}

/// Revokes `test_username`'s privileges on the database `name`, then drops the database and the
/// users that [`ScratchDatabase::create_user_without_global_privileges`] and
/// [`ScratchDatabase::create_user_with_the_process_privilege`] may have created for it, over a new
/// connection made with `root_options`.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::ConnectOptions::connect`]'s return values on failure.
/// * Forwards [`sqlx::RawSql::execute`]'s return values on failure, other than a missing grant.
/// * Forwards [`sqlx::Connection::close`]'s return values on failure.
async fn drop_scratch_database(
    root_options: &MySqlConnectOptions,
    name: &str,
    test_username: &str,
) -> Result<(), sqlx::Error> {
    const MYSQL_ER_NONEXISTING_GRANT: u16 = 1141;

    let mut root_connection = root_options.connect().await?;
    match sqlx::raw_sql(&format!(
        "REVOKE ALL PRIVILEGES ON `{name}`.* FROM '{test_username}'@'%';"
    ))
    .execute(&mut root_connection)
    .await
    {
        Ok(_) => {}
        Err(e) if has_mysql_error_number(&e, MYSQL_ER_NONEXISTING_GRANT) => {}
        Err(e) => return Err(e),
    }
    sqlx::raw_sql(&format!(
        "DROP DATABASE IF EXISTS `{name}`; DROP USER IF EXISTS '{username}'@'%', \
         '{process_username}'@'%';",
        username = scratch_username(name, false),
        process_username = scratch_username(name, true),
    ))
    .execute(&mut root_connection)
    .await?;
    root_connection.close().await
}

/// # Returns
///
/// The name of the user that [`ScratchDatabase::create_user`] creates for the database `name`,
/// with the global `PROCESS` privilege if `has_process_privilege` is set.
fn scratch_username(name: &str, has_process_privilege: bool) -> String {
    if has_process_privilege {
        return format!("{name}_process_user");
    }
    format!("{name}_user")
}

/// Reads the environment variable `name` and parses its value with `parse`.
///
/// # Type Parameters
///
/// * `ValueType` - The type of the parsed value.
/// * `ParseErrorType` - The error type of `parse`.
///
/// # Returns
///
/// The parsed value, or `None` if the variable is unset.
///
/// # Panics
///
/// Panics if:
///
/// * The variable's value is not valid Unicode.
/// * `parse` fails on the variable's value.
fn read_test_db_override<ValueType, ParseErrorType: Display>(
    name: &str,
    parse: impl FnOnce(&str) -> Result<ValueType, ParseErrorType>,
) -> Option<ValueType> {
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(VarError::NotPresent) => return None,
        Err(e @ VarError::NotUnicode(_)) => panic!("`{name}` should be valid Unicode: {e}"),
    };
    Some(
        parse(&value).unwrap_or_else(|e| panic!("`{name}` has an unparsable value `{value}`: {e}")),
    )
}
