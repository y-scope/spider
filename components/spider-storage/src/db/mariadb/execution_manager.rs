use std::net::IpAddr;

use async_trait::async_trait;
use const_format::formatcp;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::resource_group::ExternalResourceGroupCredentials;
use spider_derive::MySqlEnum;
use sqlx::Connection;

use crate::db::DbError;
use crate::db::ExecutionManagerLivenessManagement;
use crate::db::ResourceGroupManagement;
use crate::db::mariadb::MariaDbStorageConnector;
use crate::db::mariadb::run_read_committed_tx;
use crate::db::mariadb::schema::EXECUTION_MANAGERS_TABLE_NAME;
use crate::db::mariadb::schema::RESOURCE_GROUPS_TABLE_NAME;

#[async_trait]
impl ExecutionManagerLivenessManagement for MariaDbStorageConnector {
    async fn register_execution_manager(
        &self,
        ip_address: IpAddr,
        resource_group_credentials: Option<ExternalResourceGroupCredentials>,
    ) -> Result<(ExecutionManagerId, Option<ResourceGroupId>), DbError> {
        const INSERT_QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`ip_address`, `resource_group_id`) VALUES (?, ?);",
            table = EXECUTION_MANAGERS_TABLE_NAME,
        );
        const SELECT_RESOURCE_GROUP_QUERY: &str = formatcp!(
            "SELECT `id` FROM `{table}` WHERE `external_id` = ?;",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );

        let resource_group_id = if let Some(credentials) = resource_group_credentials {
            let resource_group_id =
                sqlx::query_scalar::<_, ResourceGroupId>(SELECT_RESOURCE_GROUP_QUERY)
                    .bind(credentials.get_external_resource_group_id())
                    .fetch_optional(&self.pool)
                    .await?
                    .ok_or_else(|| {
                        DbError::ExternalResourceGroupNotFound(
                            credentials.get_external_resource_group_id().to_owned(),
                        )
                    })?;
            ResourceGroupManagement::verify(self, resource_group_id, credentials.get_password())
                .await?;
            Some(resource_group_id)
        } else {
            None
        };

        let execution_manager_id = sqlx::query(INSERT_QUERY)
            .bind(ip_address.to_string())
            .bind(resource_group_id)
            .execute(&self.pool)
            .await
            .map(|result| ExecutionManagerId::from(result.last_insert_id()))
            .map_err(DbError::from)?;
        Ok((execution_manager_id, resource_group_id))
    }

    async fn update_execution_manager_heartbeat(
        &self,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<(), DbError> {
        const SELECT_STATE_FOR_UPDATE_QUERY: &str = formatcp!(
            "SELECT `state` FROM `{table}` WHERE `id` = ? FOR UPDATE;",
            table = EXECUTION_MANAGERS_TABLE_NAME,
        );
        const UPDATE_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `last_heartbeat_at` = CURRENT_TIMESTAMP WHERE `id` = ? AND \
             `state` = '{alive_state}';",
            table = EXECUTION_MANAGERS_TABLE_NAME,
            alive_state = ExecutionManagerState::Alive.as_str(),
        );

        let mut tx = self.pool.begin().await?;

        let state = sqlx::query_scalar::<_, ExecutionManagerState>(SELECT_STATE_FOR_UPDATE_QUERY)
            .bind(execution_manager_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IllegalExecutionManagerId(execution_manager_id))?;

        if state == ExecutionManagerState::Dead {
            return Err(DbError::ExecutionManagerAlreadyDead(execution_manager_id));
        }

        sqlx::query(UPDATE_QUERY)
            .bind(execution_manager_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn is_execution_manager_alive(
        &self,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<bool, DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `state` FROM `{table}` WHERE `id` = ?;",
            table = EXECUTION_MANAGERS_TABLE_NAME,
        );

        let Some(state) = sqlx::query_scalar::<_, ExecutionManagerState>(QUERY)
            .bind(execution_manager_id)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Err(DbError::IllegalExecutionManagerId(execution_manager_id));
        };

        match state {
            ExecutionManagerState::Alive => Ok(true),
            ExecutionManagerState::Dead => Ok(false),
        }
    }

    async fn get_dead_execution_managers(
        &self,
        stale_after_sec: u64,
    ) -> Result<Vec<ExecutionManagerId>, DbError> {
        run_read_committed_tx(self.pool.clone(), async move |connection| {
            get_dead_execution_managers(connection, stale_after_sec).await
        })
        .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, MySqlEnum)]
pub(super) enum ExecutionManagerState {
    Alive,
    Dead,
}

/// Marks stale execution managers dead and returns their IDs, in a transaction run on `conn`.
///
/// # Note
///
/// It is assumed that `conn` must be set to run its next transaction at the `READ COMMITTED`
/// isolation level. Under the default `REPEATABLE REA`, the confirming `SELECT ... FOR UPDATE` can
/// observe a row changed by a concurrent
/// [`ExecutionManagerLivenessManagement::update_execution_manager_heartbeat`] since the
/// transaction's read view was established and failed with a "record has changed" error (error code
/// 1020).
///
/// # Returns
///
/// A vector of dead execution manager IDs (removed from the table) on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::Connection::begin`]'s return values on failure.
/// * Forwards [`sqlx::query::QueryScalar::fetch_all`]'s return values on failure.
/// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
/// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
async fn get_dead_execution_managers(
    conn: &mut sqlx::MySqlConnection,
    stale_after_sec: u64,
) -> Result<Vec<ExecutionManagerId>, DbError> {
    const UPDATE_BATCH_SIZE: usize = 1000;

    const SELECT_CANDIDATES_QUERY: &str = formatcp!(
        "SELECT `id` FROM `{table}` WHERE `state` = '{alive_state}' AND `last_heartbeat_at` < \
         CURRENT_TIMESTAMP - INTERVAL ? SECOND ORDER BY `id`;",
        table = EXECUTION_MANAGERS_TABLE_NAME,
        alive_state = ExecutionManagerState::Alive.as_str(),
    );

    let mut tx = Connection::begin(conn).await?;
    let candidate_ids: Vec<ExecutionManagerId> = sqlx::query_scalar(SELECT_CANDIDATES_QUERY)
        .bind(stale_after_sec)
        .fetch_all(&mut *tx)
        .await?;

    let mut dead_ids: Vec<ExecutionManagerId> = Vec::with_capacity(candidate_ids.len());
    for candidate_batch in candidate_ids.chunks(UPDATE_BATCH_SIZE) {
        let placeholders = std::iter::repeat_n("?", candidate_batch.len())
            .collect::<Vec<_>>()
            .join(",");
        let select_for_update_stmt = format!(
            "SELECT `id` FROM `{table}` FORCE INDEX (PRIMARY) WHERE `id` IN ({placeholders}) AND \
             `state` = '{alive_state}' AND `last_heartbeat_at` < CURRENT_TIMESTAMP - INTERVAL ? \
             SECOND ORDER BY `id` FOR UPDATE;",
            table = EXECUTION_MANAGERS_TABLE_NAME,
            alive_state = ExecutionManagerState::Alive.as_str(),
        );
        let mut select_query = sqlx::query_scalar::<_, ExecutionManagerId>(&select_for_update_stmt);
        for execution_manager_id in candidate_batch {
            select_query = select_query.bind(execution_manager_id);
        }
        let confirmed_ids: Vec<ExecutionManagerId> = select_query
            .bind(stale_after_sec)
            .fetch_all(&mut *tx)
            .await?;

        if confirmed_ids.is_empty() {
            continue;
        }

        let placeholders = std::iter::repeat_n("?", confirmed_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let update_stmt = format!(
            "UPDATE `{EXECUTION_MANAGERS_TABLE_NAME}` SET `state` = '{dead_state}', \
             `death_confirmed_at` = CURRENT_TIMESTAMP WHERE `id` IN ({placeholders});",
            dead_state = ExecutionManagerState::Dead.as_str(),
        );
        let mut update_query = sqlx::query(&update_stmt);
        for execution_manager_id in &confirmed_ids {
            update_query = update_query.bind(execution_manager_id);
        }
        update_query.execute(&mut *tx).await?;

        dead_ids.extend(confirmed_ids);
    }

    tx.commit().await?;
    Ok(dead_ids)
}
