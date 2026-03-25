use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use const_format::formatcp;
use secrecy::ExposeSecret;
use spider_core::{
    job::{CancelTarget, CommitTarget, JobState},
    task::TaskGraph,
    types::{
        id::{JobId, ResourceGroupId},
        io::{TaskInput, TaskOutput},
    },
};
use sqlx::{MySqlPool, Row, mysql::MySqlDatabaseError};
use uuid::Uuid;

use crate::{
    config::DatabaseConfig,
    db::{
        DbError,
        DbStorage,
        ExternalJobOrchestration,
        InternalJobOrchestration,
        ResourceGroupManagement,
        error::ExpectedStates,
    },
};

#[derive(Clone)]
pub struct MariaDbStorageConnector {
    pool: MySqlPool,
}

impl MariaDbStorageConnector {
    /// Connects to database and initializes tables.
    ///
    /// # Parameters
    ///
    /// * `config` - Database configuration parameters.
    ///
    /// # Returns
    ///
    /// A new instance of `MariaDbStorageConnector` if connection and initialization succeed.
    ///
    /// # Errors
    ///
    /// Returns an error if
    ///
    /// * Forwards a [`sqlx::error::Error`] if database operation fails.
    pub async fn connect_and_initialize(config: &DatabaseConfig) -> Result<Self, DbError> {
        let mysql_options = sqlx::mysql::MySqlConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .database(&config.name)
            .username(&config.username)
            .password(config.password.expose_secret());

        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(mysql_options)
            .await?;

        let connector = Self { pool };
        connector.initialize().await?;
        Ok(connector)
    }
}

#[async_trait]
impl ExternalJobOrchestration for MariaDbStorageConnector {
    async fn register(
        &self,
        resource_group_id: ResourceGroupId,
        task_graph: Arc<TaskGraph>,
        job_inputs: Vec<TaskInput>,
    ) -> Result<JobId, DbError> {
        const INSERT_QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`resource_group_id`, `serialized_task_graph`, \
             `serialized_job_inputs`) VALUES (?, ?, ?) RETURNING CAST(`id` AS BINARY(16)) AS `id`;",
            table = JOBS_TABLE_NAME,
        );

        let serialized_task_graph = task_graph
            .to_json()
            .map_err(|e| DbError::TaskGraphSerializationFailure(Box::new(e)))?;
        let serialized_job_inputs =
            serde_json::to_string(&job_inputs).map_err(DbError::value_ser)?;

        let result = sqlx::query(INSERT_QUERY)
            .bind(resource_group_id.as_bytes().as_slice())
            .bind(serialized_task_graph)
            .bind(serialized_job_inputs)
            .fetch_one(&self.pool)
            .await;

        match result {
            Ok(row) => {
                let id_bytes: Vec<u8> = row.get(0);
                job_id_from_bytes(&id_bytes)
            }
            Err(sqlx::Error::Database(e))
                if e.try_downcast_ref::<MySqlDatabaseError>()
                    .is_some_and(|mysql_err| mysql_err.number() == MYSQL_ER_FK_CONSTRAINT) =>
            {
                Err(DbError::ResourceGroupNotFound(resource_group_id))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn start(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), DbError> {
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `state`, CAST(`resource_group_id` AS BINARY(16)) FROM `{table}` WHERE `id` = \
             ? FOR UPDATE;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ? WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let job_id_bytes = *job_id.as_bytes();
        let mut tx = self.pool.begin().await?;

        let row: Option<(JobState, Vec<u8>)> = sqlx::query_as(SELECT_QUERY)
            .bind(job_id_bytes.as_slice())
            .fetch_optional(&mut *tx)
            .await?;

        let (state, rg_id_bytes) = row.ok_or(DbError::JobNotFound(job_id))?;
        validate_resource_group_access(&rg_id_bytes, resource_group_id)?;

        if state != JobState::Ready {
            return Err(DbError::UnexpectedJobState {
                current: state,
                expected: ExpectedStates(vec![JobState::Ready]),
            });
        }

        sqlx::query(UPDATE_QUERY)
            .bind(JobState::Running)
            .bind(job_id_bytes.as_slice())
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn cancel(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
        target: CancelTarget,
    ) -> Result<(), DbError> {
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `state`, CAST(`resource_group_id` AS BINARY(16)) FROM `{table}` WHERE `id` = \
             ? FOR UPDATE;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_STATE_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ? WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_STATE_AND_END_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ?, `ended_at` = CURRENT_TIMESTAMP WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let new_state = target.into_job_state();
        let job_id_bytes = *job_id.as_bytes();
        let mut tx = self.pool.begin().await?;

        let row: Option<(JobState, Vec<u8>)> = sqlx::query_as(SELECT_QUERY)
            .bind(job_id_bytes.as_slice())
            .fetch_optional(&mut *tx)
            .await?;

        let (state, rg_id_bytes) = row.ok_or(DbError::JobNotFound(job_id))?;
        validate_resource_group_access(&rg_id_bytes, resource_group_id)?;

        if !JobState::is_valid_transition(state, new_state) {
            return Err(DbError::InvalidJobStateTransition {
                from: state,
                to: new_state,
            });
        }

        if new_state.is_terminal() {
            sqlx::query(UPDATE_STATE_AND_END_QUERY)
                .bind(new_state)
                .bind(job_id_bytes.as_slice())
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query(UPDATE_STATE_QUERY)
                .bind(new_state)
                .bind(job_id_bytes.as_slice())
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn get_state(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<JobState, DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `state`, CAST(`resource_group_id` AS BINARY(16)) FROM `{table}` WHERE `id` = \
             ?;",
            table = JOBS_TABLE_NAME,
        );

        let row: Option<(JobState, Vec<u8>)> = sqlx::query_as(QUERY)
            .bind(job_id.as_bytes().as_slice())
            .fetch_optional(&self.pool)
            .await?;

        let (state, rg_id_bytes) = row.ok_or(DbError::JobNotFound(job_id))?;
        validate_resource_group_access(&rg_id_bytes, resource_group_id)?;

        Ok(state)
    }

    async fn get_outputs(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<Vec<TaskOutput>, DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `state`, CAST(`resource_group_id` AS BINARY(16)), `serialized_job_outputs` \
             FROM `{table}` WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let job_id_bytes = *job_id.as_bytes();

        let row: Option<(JobState, Vec<u8>, Option<String>)> = sqlx::query_as(QUERY)
            .bind(job_id_bytes.as_slice())
            .fetch_optional(&self.pool)
            .await?;

        let (state, rg_id_bytes, serialized_outputs) = row.ok_or(DbError::JobNotFound(job_id))?;
        validate_resource_group_access(&rg_id_bytes, resource_group_id)?;

        if state != JobState::Succeeded {
            return Err(DbError::UnexpectedJobState {
                current: state,
                expected: ExpectedStates(vec![JobState::Succeeded]),
            });
        }

        let outputs_str = serialized_outputs.ok_or_else(|| {
            DbError::CorruptedDbState(format!(
                "job `{}` succeeded but has no serialized outputs",
                Uuid::from_bytes(job_id_bytes)
            ))
        })?;
        let outputs: Vec<TaskOutput> =
            serde_json::from_str(&outputs_str).map_err(DbError::value_de)?;
        Ok(outputs)
    }

    async fn get_error(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<String, DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `state`, CAST(`resource_group_id` AS BINARY(16)), `error_message` FROM \
             `{table}` WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let job_id_bytes = *job_id.as_bytes();

        let row: Option<(JobState, Vec<u8>, Option<String>)> = sqlx::query_as(QUERY)
            .bind(job_id_bytes.as_slice())
            .fetch_optional(&self.pool)
            .await?;

        let (state, rg_id_bytes, error_message) = row.ok_or(DbError::JobNotFound(job_id))?;
        validate_resource_group_access(&rg_id_bytes, resource_group_id)?;

        if state != JobState::Failed {
            return Err(DbError::UnexpectedJobState {
                current: state,
                expected: ExpectedStates(vec![JobState::Failed]),
            });
        }

        let message = error_message.ok_or_else(|| {
            DbError::CorruptedDbState(format!(
                "job `{}` failed but has no error message",
                Uuid::from_bytes(job_id_bytes)
            ))
        })?;
        Ok(message)
    }
}

#[async_trait]
impl InternalJobOrchestration for MariaDbStorageConnector {
    async fn set_state(&self, job_id: JobId, state: JobState) -> Result<(), DbError> {
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `state` FROM `{table}` WHERE `id` = ? FOR UPDATE;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ? WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_TERMINAL_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ?, `ended_at` = CURRENT_TIMESTAMP WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let job_id_bytes = *job_id.as_bytes();
        let mut tx = self.pool.begin().await?;

        let row: Option<(JobState,)> = sqlx::query_as(SELECT_QUERY)
            .bind(job_id_bytes.as_slice())
            .fetch_optional(&mut *tx)
            .await?;

        let (current_state,) = row.ok_or(DbError::JobNotFound(job_id))?;

        if !JobState::is_valid_transition(current_state, state) {
            return Err(DbError::InvalidJobStateTransition {
                from: current_state,
                to: state,
            });
        }

        if state.is_terminal() {
            sqlx::query(UPDATE_TERMINAL_QUERY)
                .bind(state)
                .bind(job_id_bytes.as_slice())
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query(UPDATE_QUERY)
                .bind(state)
                .bind(job_id_bytes.as_slice())
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn commit_outputs(
        &self,
        job_id: JobId,
        job_outputs: Vec<TaskOutput>,
        target: CommitTarget,
    ) -> Result<(), DbError> {
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `state` FROM `{table}` WHERE `id` = ? FOR UPDATE;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_SUCCEEDED_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ?, `serialized_job_outputs` = ?, `ended_at` = \
             CURRENT_TIMESTAMP WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_COMMIT_READY_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ?, `serialized_job_outputs` = ? WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let new_state = target.into_job_state();
        let job_id_bytes = *job_id.as_bytes();
        let mut tx = self.pool.begin().await?;

        let row: Option<(JobState,)> = sqlx::query_as(SELECT_QUERY)
            .bind(job_id_bytes.as_slice())
            .fetch_optional(&mut *tx)
            .await?;

        let (current_state,) = row.ok_or(DbError::JobNotFound(job_id))?;

        if !JobState::is_valid_transition(current_state, new_state) {
            return Err(DbError::InvalidJobStateTransition {
                from: current_state,
                to: new_state,
            });
        }

        let serialized_outputs = serde_json::to_string(&job_outputs).map_err(DbError::value_ser)?;

        if new_state.is_terminal() {
            sqlx::query(UPDATE_SUCCEEDED_QUERY)
                .bind(new_state)
                .bind(&serialized_outputs)
                .bind(job_id_bytes.as_slice())
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query(UPDATE_COMMIT_READY_QUERY)
                .bind(new_state)
                .bind(&serialized_outputs)
                .bind(job_id_bytes.as_slice())
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn cancel(&self, job_id: JobId, target: CancelTarget) -> Result<(), DbError> {
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `state` FROM `{table}` WHERE `id` = ? FOR UPDATE;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_STATE_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ? WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_STATE_AND_END_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ?, `ended_at` = CURRENT_TIMESTAMP WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let new_state = target.into_job_state();
        let job_id_bytes = *job_id.as_bytes();
        let mut tx = self.pool.begin().await?;

        let row: Option<(JobState,)> = sqlx::query_as(SELECT_QUERY)
            .bind(job_id_bytes.as_slice())
            .fetch_optional(&mut *tx)
            .await?;

        let (current_state,) = row.ok_or(DbError::JobNotFound(job_id))?;

        if !JobState::is_valid_transition(current_state, new_state) {
            return Err(DbError::InvalidJobStateTransition {
                from: current_state,
                to: new_state,
            });
        }

        if new_state.is_terminal() {
            sqlx::query(UPDATE_STATE_AND_END_QUERY)
                .bind(new_state)
                .bind(job_id_bytes.as_slice())
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query(UPDATE_STATE_QUERY)
                .bind(new_state)
                .bind(job_id_bytes.as_slice())
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn fail(&self, job_id: JobId, error_message: String) -> Result<(), DbError> {
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `state` FROM `{table}` WHERE `id` = ? FOR UPDATE;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ?, `error_message` = ?, `ended_at` = \
             CURRENT_TIMESTAMP WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let job_id_bytes = *job_id.as_bytes();
        let mut tx = self.pool.begin().await?;

        let row: Option<(JobState,)> = sqlx::query_as(SELECT_QUERY)
            .bind(job_id_bytes.as_slice())
            .fetch_optional(&mut *tx)
            .await?;

        let (current_state,) = row.ok_or(DbError::JobNotFound(job_id))?;

        if !JobState::is_valid_transition(current_state, JobState::Failed) {
            return Err(DbError::InvalidJobStateTransition {
                from: current_state,
                to: JobState::Failed,
            });
        }

        sqlx::query(UPDATE_QUERY)
            .bind(JobState::Failed)
            .bind(&error_message)
            .bind(job_id_bytes.as_slice())
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn delete_expired_terminated_jobs(
        &self,
        expire_after: Duration,
    ) -> Result<Vec<JobId>, DbError> {
        const SELECT_QUERY: &str = formatcp!(
            "SELECT CAST(`id` AS BINARY(16)) FROM `{table}` WHERE `state` IN \
             ('Succeeded','Failed','Cancelled') AND `ended_at` < NOW() - INTERVAL ? SECOND FOR \
             UPDATE;",
            table = JOBS_TABLE_NAME,
        );

        let timeout_secs = expire_after.as_secs();
        let mut tx = self.pool.begin().await?;

        let rows: Vec<(Vec<u8>,)> = sqlx::query_as(SELECT_QUERY)
            .bind(timeout_secs)
            .fetch_all(&mut *tx)
            .await?;

        let mut job_ids: Vec<JobId> = Vec::with_capacity(rows.len());
        for (id_bytes,) in &rows {
            job_ids.push(job_id_from_bytes(id_bytes)?);
        }

        if !job_ids.is_empty() {
            let placeholders = std::iter::repeat_n("?", job_ids.len())
                .collect::<Vec<_>>()
                .join(",");
            let delete_query =
                format!("DELETE FROM `{JOBS_TABLE_NAME}` WHERE `id` IN ({placeholders})");

            let mut query = sqlx::query(&delete_query);
            for job_id in &job_ids {
                query = query.bind(job_id.as_bytes().as_slice());
            }
            query.execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(job_ids)
    }
}

#[async_trait]
impl ResourceGroupManagement for MariaDbStorageConnector {
    async fn add(
        &self,
        external_resource_group_id: String,
        password: String,
    ) -> Result<ResourceGroupId, DbError> {
        const QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`external_id`, `password`) VALUES (?, ?) RETURNING CAST(`id` \
             AS BINARY(16)) AS `id`;",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );

        let result = sqlx::query(QUERY)
            .bind(external_resource_group_id.clone())
            .bind(password)
            .fetch_one(&self.pool)
            .await;

        match result {
            Ok(row) => {
                let id_bytes: Vec<u8> = row.get(0);
                resource_group_id_from_bytes(&id_bytes)
            }
            Err(sqlx::Error::Database(e))
                if e.try_downcast_ref::<MySqlDatabaseError>()
                    .is_some_and(|mysql_err| mysql_err.number() == MYSQL_ER_DUP_ENTRY) =>
            {
                Err(DbError::ResourceGroupAlreadyExists(
                    external_resource_group_id,
                ))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn verify(
        &self,
        resource_group_id: ResourceGroupId,
        password: String,
    ) -> Result<(), DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `password` FROM `{table}` WHERE `id` = ?;",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );

        let row: Option<(String,)> = sqlx::query_as(QUERY)
            .bind(resource_group_id.as_bytes().as_slice())
            .fetch_optional(&self.pool)
            .await?;

        match row {
            None => Err(DbError::ResourceGroupNotFound(resource_group_id)),
            Some((stored_password,)) => {
                use subtle::ConstantTimeEq;
                if stored_password.as_bytes().ct_eq(password.as_bytes()).into() {
                    Ok(())
                } else {
                    Err(DbError::InvalidPassword(resource_group_id))
                }
            }
        }
    }

    /// Force-deletes the resource group and **all** its jobs, including those in non-terminal
    /// states (e.g. `Running`, `CommitReady`, `CleanupReady`). The caller is responsible for
    /// ensuring that no jobs are actively being processed before calling this method.
    async fn delete(&self, resource_group_id: ResourceGroupId) -> Result<(), DbError> {
        const DELETE_JOBS_QUERY: &str = formatcp!(
            "DELETE FROM `{table}` WHERE `resource_group_id` = ?;",
            table = JOBS_TABLE_NAME,
        );
        const DELETE_RG_QUERY: &str = formatcp!(
            "DELETE FROM `{table}` WHERE `id` = ?;",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );

        let mut tx = self.pool.begin().await?;

        sqlx::query(DELETE_JOBS_QUERY)
            .bind(resource_group_id.as_bytes().as_slice())
            .execute(&mut *tx)
            .await?;

        let result = sqlx::query(DELETE_RG_QUERY)
            .bind(resource_group_id.as_bytes().as_slice())
            .execute(&mut *tx)
            .await?;

        if result.rows_affected() == 0 {
            return Err(DbError::ResourceGroupNotFound(resource_group_id));
        }

        tx.commit().await?;
        Ok(())
    }
}

impl DbStorage for MariaDbStorageConnector {}

/// `MySQL` error number for foreign key constraint violation.
const MYSQL_ER_FK_CONSTRAINT: u16 = 1452;
/// `MySQL` error number for duplicate entry.
const MYSQL_ER_DUP_ENTRY: u16 = 1062;

const RESOURCE_GROUPS_TABLE_NAME: &str = "resource_groups";
const JOBS_TABLE_NAME: &str = "jobs";

#[must_use]
const fn resource_groups_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{RESOURCE_GROUPS_TABLE_NAME}` (
  `id` UUID NOT NULL DEFAULT UUID_v7(),
  `external_id` VARCHAR(256) NOT NULL,
  `password` VARCHAR(2048) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `external_resource_group_id` (`external_id`)
);"
    )
}

#[must_use]
const fn jobs_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{JOBS_TABLE_NAME}` (
  `id` UUID NOT NULL DEFAULT UUID_v7(),
  `resource_group_id` UUID NOT NULL,
  `state` {state_enum} NOT NULL DEFAULT {default_state},
  `serialized_task_graph` LONGTEXT NOT NULL,
  `serialized_job_inputs` LONGTEXT NOT NULL,
  `serialized_job_outputs` LONGTEXT,
  `error_message` LONGTEXT,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ended_at` TIMESTAMP,
  `max_num_retries` INT UNSIGNED NOT NULL DEFAULT 0,
  `num_retries` INT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  CONSTRAINT `job_resource_group` FOREIGN KEY (`resource_group_id`)
    REFERENCES `{RESOURCE_GROUPS_TABLE_NAME}` (`id`)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);",
        state_enum = JobState::as_mysql_enum_decl(),
        default_state = JobState::Ready.as_quoted_str(),
    )
}

fn job_id_from_bytes(bytes: &[u8]) -> Result<JobId, DbError> {
    Uuid::from_slice(bytes)
        .map(JobId::from)
        .map_err(|e| DbError::CorruptedDbState(format!("invalid job UUID from database: {e}")))
}

fn resource_group_id_from_bytes(bytes: &[u8]) -> Result<ResourceGroupId, DbError> {
    Uuid::from_slice(bytes)
        .map(ResourceGroupId::from)
        .map_err(|e| {
            DbError::CorruptedDbState(format!("invalid resource group UUID from database: {e}"))
        })
}

fn validate_resource_group_access(
    rg_id_bytes: &[u8],
    expected: ResourceGroupId,
) -> Result<(), DbError> {
    let actual = resource_group_id_from_bytes(rg_id_bytes)?;
    if actual != expected {
        return Err(DbError::InvalidAccess(expected));
    }
    Ok(())
}

impl MariaDbStorageConnector {
    /// Initializes the database by creating necessary tables if they do not exist.
    ///
    /// # NOTE
    ///
    /// `MariaDB` does not support transactions for DDL statements. All DDL statements are
    /// automatically committed. Thus, this function executes each table creation query separately,
    /// and does not provide atomicity guarantees.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards a [`sqlx::query::Query::execute`]'s return values on failure.
    async fn initialize(&self) -> Result<(), DbError> {
        sqlx::query(resource_groups_creation_query())
            .execute(&self.pool)
            .await?;

        sqlx::query(jobs_creation_query())
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
