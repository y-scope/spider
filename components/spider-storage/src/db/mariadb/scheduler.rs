use async_trait::async_trait;
use const_format::formatcp;
use spider_core::types::id::SchedulerId;
use spider_core::types::scheduler::RegisteredScheduler;
use spider_utils::config::Host;

use crate::db::DbError;
use crate::db::SchedulerRegistrationManagement;
use crate::db::mariadb::MariaDbStorageConnector;
use crate::db::mariadb::schema::SCHEDULERS_TABLE_NAME;

#[async_trait]
impl SchedulerRegistrationManagement for MariaDbStorageConnector {
    async fn register_scheduler(&self, host: &str, port: u16) -> Result<SchedulerId, DbError> {
        const DELETE_QUERY: &str =
            formatcp!("DELETE FROM `{table}`;", table = SCHEDULERS_TABLE_NAME,);
        const INSERT_QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`host`, `port`) VALUES (?, ?);",
            table = SCHEDULERS_TABLE_NAME,
        );

        let mut tx = self.pool.begin().await?;
        sqlx::query(DELETE_QUERY).execute(&mut *tx).await?;
        let scheduler_id = sqlx::query(INSERT_QUERY)
            .bind(host)
            .bind(port)
            .execute(&mut *tx)
            .await?
            .last_insert_id();
        tx.commit().await?;
        Ok(SchedulerId::from(scheduler_id))
    }

    async fn get_schedulers(&self) -> Result<Vec<RegisteredScheduler>, DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `id`, `host`, `port` FROM `{table}` ORDER BY `id` ASC;",
            table = SCHEDULERS_TABLE_NAME,
        );

        let rows: Vec<SchedulerRowProjection> = sqlx::query_as(QUERY).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(SchedulerRowProjection::into_registered_scheduler)
            .collect()
    }

    async fn is_scheduler_registered(&self, scheduler_id: SchedulerId) -> Result<bool, DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `id` FROM `{table}` WHERE `id` = ?;",
            table = SCHEDULERS_TABLE_NAME,
        );

        let registered_scheduler_id: Option<SchedulerId> = sqlx::query_scalar(QUERY)
            .bind(scheduler_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(registered_scheduler_id.is_some())
    }
}

/// A raw row selected from the schedulers table.
#[derive(sqlx::FromRow)]
struct SchedulerRowProjection {
    id: SchedulerId,
    host: String,
    port: u16,
}

impl SchedulerRowProjection {
    /// Converts the row projection into [`RegisteredScheduler`].
    ///
    /// # Returns
    ///
    /// The registered scheduler on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::CorruptedDbState`] if the scheduler host is empty.
    fn into_registered_scheduler(self) -> Result<RegisteredScheduler, DbError> {
        let host = Host::new(self.host).map_err(|_| {
            DbError::CorruptedDbState(format!("scheduler `{}` has an empty host", self.id))
        })?;
        Ok(RegisteredScheduler {
            id: self.id,
            host,
            port: self.port,
        })
    }
}
