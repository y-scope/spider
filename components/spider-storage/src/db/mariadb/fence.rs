use std::time::Duration;

use const_format::formatcp;
use spider_core::types::id::SessionId;
use sqlx::Connection;
use sqlx::MySqlConnection;
use sqlx::MySqlPool;

use crate::db::DbError;
use crate::db::mariadb::MariaDbStorageConnector;
use crate::db::mariadb::mysql_error::is_access_denied;
use crate::db::mariadb::mysql_error::is_lock_wait_timeout;
use crate::db::mariadb::schema::STORAGE_FENCE_TABLE_NAME;

/// The ID of the `storage_fence` row that a storage service fences its writes with unless
/// [`DatabaseConfig::fence_id`](crate::config::DatabaseConfig::fence_id) overrides it.
pub(super) const DEFAULT_FENCE_ID: u64 = 0;

/// The parameters of a write statement's fence predicate: the statement writes only while the
/// fence row `fence_id` holds `session_id`.
#[derive(Debug, Clone, Copy)]
pub(super) struct FencePredicate {
    pub(super) fence_id: u64,
    pub(super) session_id: SessionId,
}

impl MariaDbStorageConnector {
    /// Checks that no newer storage session has fenced the current session off.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryScalar::fetch_optional`]'s return values on failure.
    /// * Forwards [`Self::ensure_fence_holds`]'s return values on failure.
    pub(super) async fn check_fence(
        &self,
        connection: &mut MySqlConnection,
    ) -> Result<(), DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `session_id` FROM `{table}` WHERE `id` = ? LOCK IN SHARE MODE;",
            table = STORAGE_FENCE_TABLE_NAME,
        );

        let fence_session_id = sqlx::query_scalar::<_, SessionId>(QUERY)
            .bind(self.fence_id)
            .fetch_optional(connection)
            .await?;
        self.ensure_fence_holds(fence_session_id)
    }

    /// Reads the fence row without locking it to tell why a previous write with the fence in its
    /// statement wrote no row.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryScalar::fetch_optional`]'s return values on failure.
    /// * Forwards [`Self::ensure_fence_holds`]'s return values on failure.
    pub(super) async fn probe_fence(
        &self,
        connection: &mut MySqlConnection,
    ) -> Result<(), DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `session_id` FROM `{table}` WHERE `id` = ?;",
            table = STORAGE_FENCE_TABLE_NAME,
        );

        let fence_session_id = sqlx::query_scalar::<_, SessionId>(QUERY)
            .bind(self.fence_id)
            .fetch_optional(connection)
            .await?;
        self.ensure_fence_holds(fence_session_id)
    }

    /// # Returns
    ///
    /// The fence predicate's parameters for this connector.
    pub(super) const fn fence(&self) -> FencePredicate {
        FencePredicate {
            fence_id: self.fence_id,
            session_id: self.session_id,
        }
    }

    /// Checks that the current connector's session ID is what the current fence holds.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::Fenced`] if `fence_session_id` is a session ID other than this connector's.
    /// * [`DbError::CorruptedDbState`] if `fence_session_id` is `None`, that is, the fence row does
    ///   not exist.
    fn ensure_fence_holds(&self, fence_session_id: Option<SessionId>) -> Result<(), DbError> {
        let fence_session_id = fence_session_id.ok_or_else(|| {
            DbError::CorruptedDbState(format!("storage fence `{}` does not exist", self.fence_id))
        })?;
        if fence_session_id != self.session_id {
            return Err(DbError::Fenced {
                current: self.session_id,
                newer: fence_session_id,
            });
        }
        Ok(())
    }
}

/// Raises the fence row `fence_id` to `session_id` unless it already holds a newer session, so that
/// every storage session that connected earlier with the same fence ID fails its next write with
/// [`DbError::Fenced`].
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::Pool::acquire`]'s return values on failure.
/// * Forwards [`bump_fence_on_connection`]'s return values on failure.
pub(super) async fn bump_fence(
    pool: &MySqlPool,
    fence_id: u64,
    session_id: SessionId,
) -> Result<(), DbError> {
    let mut connection = pool.acquire().await?.detach();
    let result = bump_fence_on_connection(pool, &mut connection, fence_id, session_id).await;
    if let Err(e) = connection.close().await {
        tracing::warn!(error = % e, "Failed to close a detached database connection.");
    }
    result
}

/// Raises the fence row `fence_id` to `session_id` on `connection`, killing through `pool` the
/// connections that block the update and retrying the update when it times out waiting for a lock.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::query::Query::execute`]'s return values on failure, including a lock wait
///   timeout on the last attempt.
/// * Forwards [`sqlx::query::QueryScalar::fetch_one`]'s return values on failure.
async fn bump_fence_on_connection(
    pool: &MySqlPool,
    connection: &mut MySqlConnection,
    fence_id: u64,
    session_id: SessionId,
) -> Result<(), DbError> {
    const MAX_ATTEMPTS: usize = 5;

    sqlx::query("SET SESSION innodb_lock_wait_timeout = 5;")
        .execute(&mut *connection)
        .await?;
    let connection_id: u64 = sqlx::query_scalar("SELECT CONNECTION_ID();")
        .fetch_one(&mut *connection)
        .await?;

    let mut is_blocker_lookup_enabled = true;
    let mut attempt = 1;
    loop {
        const BLOCKER_LOOKUP_INTERVAL: Duration = Duration::from_secs(1);
        const BUMP_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `session_id` = GREATEST(`session_id`, ?) WHERE `id` = ?;",
            table = STORAGE_FENCE_TABLE_NAME,
        );

        let update = sqlx::query(BUMP_QUERY)
            .bind(session_id)
            .bind(fence_id)
            .execute(&mut *connection);
        tokio::pin!(update);
        let mut blocker_lookup_ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + BLOCKER_LOOKUP_INTERVAL,
            BLOCKER_LOOKUP_INTERVAL,
        );
        loop {
            tokio::select! {
                update_result = &mut update => match update_result {
                    Ok(_) => return Ok(()),
                    Err(e) if is_lock_wait_timeout(&e) && attempt < MAX_ATTEMPTS => {
                        tracing::warn!(
                            attempt,
                            "Timed out waiting for the storage fence. Retrying."
                        );
                        attempt += 1;
                        break;
                    }
                    Err(e) => return Err(e.into()),
                },
                _ = blocker_lookup_ticker.tick(), if is_blocker_lookup_enabled => {
                    if let Err(e) = kill_fence_blockers(pool, connection_id).await {
                        if !is_access_denied(&e) {
                            tracing::warn!(
                                error = % e,
                                "Failed to kill the connections that block the storage fence."
                            );
                            continue;
                        }
                        tracing::warn!(
                            error = % e,
                            "Cannot look up the connections that block the storage fence. \
                             Fall back to rely on the lock wait timeout."
                        );
                        is_blocker_lookup_enabled = false;
                    }
                }
            }
        }
    }
}

/// Kills every connection to this database whose transaction, open since before the connection
/// `waiting_connection_id` started waiting, blocks that connection.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::query::QueryScalar::fetch_all`]'s return values on failure.
async fn kill_fence_blockers(
    pool: &MySqlPool,
    waiting_connection_id: u64,
) -> Result<(), sqlx::Error> {
    const SELECT_BLOCKERS_QUERY: &str =
        "SELECT DISTINCT `blocker`.`trx_mysql_thread_id` FROM \
         `information_schema`.`INNODB_LOCK_WAITS` `lock_wait` JOIN \
         `information_schema`.`INNODB_TRX` `waiter` ON `waiter`.`trx_id` = \
         `lock_wait`.`requesting_trx_id` JOIN `information_schema`.`INNODB_TRX` `blocker` ON \
         `blocker`.`trx_id` = `lock_wait`.`blocking_trx_id` JOIN \
         `information_schema`.`PROCESSLIST` `blocker_process` ON `blocker_process`.`ID` = \
         `blocker`.`trx_mysql_thread_id` WHERE `waiter`.`trx_mysql_thread_id` = ? AND \
         `blocker`.`trx_mysql_thread_id` <> ? AND `blocker_process`.`DB` = DATABASE() AND \
         `blocker`.`trx_started` <= `waiter`.`trx_wait_started`;";

    let blocker_connection_ids: Vec<u64> = sqlx::query_scalar(SELECT_BLOCKERS_QUERY)
        .bind(waiting_connection_id)
        .bind(waiting_connection_id)
        .fetch_all(pool)
        .await?;
    for blocker_connection_id in blocker_connection_ids {
        match sqlx::raw_sql(&format!("KILL CONNECTION {blocker_connection_id};"))
            .execute(pool)
            .await
        {
            Ok(_) => {
                tracing::warn!(
                    blocker_connection_id,
                    "Killed a connection that blocked the storage fence."
                );
            }
            Err(e) => {
                tracing::warn!(
                    blocker_connection_id,
                    error = % e,
                    "Failed to kill a connection that blocked the storage fence."
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use const_format::formatcp;
    use serial_test::serial;
    use spider_core::types::id::SessionId;
    use sqlx::Connection;

    use crate::db::DbError;
    use crate::db::mariadb::MariaDbStorageConnector;
    use crate::db::mariadb::mysql_error::has_mysql_error_number;
    use crate::db::mariadb::schema::STORAGE_FENCE_TABLE_NAME;
    use crate::db::mariadb::tests::mariadb_config;

    #[tokio::test]
    #[ignore = "requires MariaDB"]
    #[serial]
    async fn the_fence_check_sees_the_bump_despite_an_older_snapshot() -> anyhow::Result<()> {
        const MYSQL_ER_CHECKREAD: u16 = 1020;
        const SELECT_FENCE_QUERY: &str = formatcp!(
            "SELECT `session_id` FROM `{table}` WHERE `id` = ?;",
            table = STORAGE_FENCE_TABLE_NAME,
        );

        let config = mariadb_config()?;
        for is_snapshot_isolation_on in [false, true] {
            let older = MariaDbStorageConnector::connect(&config).await?;
            let mut connection = older.pool.acquire().await?.detach();
            sqlx::raw_sql(if is_snapshot_isolation_on {
                "SET SESSION innodb_snapshot_isolation = ON;"
            } else {
                "SET SESSION innodb_snapshot_isolation = OFF;"
            })
            .execute(&mut connection)
            .await?;
            sqlx::raw_sql(
                "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION WITH \
                 CONSISTENT SNAPSHOT;",
            )
            .execute(&mut connection)
            .await?;
            let snapshot_session_id: SessionId = sqlx::query_scalar(SELECT_FENCE_QUERY)
                .bind(
                    config
                        .fence_id
                        .expect("the MariaDB test config should set a fence ID"),
                )
                .fetch_one(&mut connection)
                .await?;
            assert_eq!(snapshot_session_id, older.session_id);

            // A new connection will bump the session ID in the fence. Any connections from the old
            // audsession should be rejected.
            let newer = MariaDbStorageConnector::connect(&config).await?;
            let result = older.check_fence(&mut connection).await;
            sqlx::raw_sql("ROLLBACK;").execute(&mut connection).await?;
            connection.close().await?;

            match (result, is_snapshot_isolation_on) {
                (
                    Err(DbError::Fenced {
                        current,
                        newer: newer_session_id,
                    }),
                    false,
                ) => {
                    assert_eq!(
                        (current, newer_session_id),
                        (older.session_id, newer.session_id)
                    );
                }
                (Err(DbError::Sql(e)), true) => {
                    assert!(has_mysql_error_number(&e, &[MYSQL_ER_CHECKREAD]), "{e}");
                }
                (result, _) => panic!(
                    "unexpected fence check with snapshot isolation {is_snapshot_isolation_on}: \
                     {result:?}"
                ),
            }
        }
        Ok(())
    }
}
