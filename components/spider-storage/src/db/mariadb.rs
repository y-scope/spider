use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use const_format::formatcp;
use spider_core::{
    job::JobState,
    task::TaskGraph,
    types::{
        id::{JobId, ResourceGroupId},
        io::{TaskInput, TaskOutput},
    },
};
use sqlx::{MySqlPool, Row};
use uuid::Uuid;

use super::sql_utils;
use crate::db::{
    DbError,
    ExternalJobOrchestration,
    InternalJobOrchestration,
    ResourceGroupManagement,
    error::ExpectedStates,
};

const RESOURCE_GROUPS_TABLE_NAME: &str = "resource_groups";
const JOBS_TABLE_NAME: &str = "jobs";

#[must_use]
const fn resource_groups_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{RESOURCE_GROUPS_TABLE_NAME}` (
  id UUID NOT NULL DEFAULT UUID_v7(),
  external_id VARCHAR(256) NOT NULL,
  password VARCHAR(2048) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `external_resource_group_id` (`external_id`)
);"
    )
}

#[must_use]
fn jobs_creation_query() -> String {
    format!(
        r"
CREATE TABLE IF NOT EXISTS `{JOBS_TABLE_NAME}` (
  id UUID NOT NULL DEFAULT UUID_v7(),
  resource_group_id UUID NOT NULL,
  state ENUM({state_enum}) NOT NULL DEFAULT 'Ready',
  serialized_task_graph LONGTEXT NOT NULL,
  serialized_job_inputs LONGTEXT NOT NULL,
  serialized_job_outputs LONGTEXT,
  error_message LONGTEXT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ended_at TIMESTAMP,
  max_num_retries INT UNSIGNED NOT NULL DEFAULT 0,
  num_retries INT UNSIGNED NOT NULL DEFAULT 0,
  commit_tdl_package VARCHAR(512),
  commit_tdl_function VARCHAR(512),
  cleanup_tdl_package VARCHAR(512),
  cleanup_tdl_function VARCHAR(512),
  PRIMARY KEY (`id`),
  CONSTRAINT `job_resource_group` FOREIGN KEY (`resource_group_id`)
    REFERENCES `{RESOURCE_GROUPS_TABLE_NAME}` (`id`)
);",
        state_enum = sql_utils::sql_enum_values::<JobState>()
    )
}

fn parse_job_id(id_str: &str) -> Result<JobId, DbError> {
    Uuid::parse_str(id_str)
        .map(JobId::from)
        .map_err(|e| DbError::CorruptedDbState(format!("invalid job UUID from database: {e}")))
}

fn parse_resource_group_id(id_str: &str) -> Result<ResourceGroupId, DbError> {
    Uuid::parse_str(id_str)
        .map(ResourceGroupId::from)
        .map_err(|e| {
            DbError::CorruptedDbState(format!(
                "invalid resource group UUID from database: {e}"
            ))
        })
}

#[derive(Clone)]
pub struct MariaDbStorage {
    pool: MySqlPool,
}

impl MariaDbStorage {
    #[must_use]
    pub const fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }
}

impl MariaDbStorage {
    /// Initializes the database by creating necessary tables if they do not exist.
    ///
    /// Note: `MariaDB` does not support transactions for DDL statements. All DDL statements are
    /// automatically committed. Thus, this function executes each table creation query separately,
    /// and does not provide atomicity guarantees.
    ///
    /// # Errors
    ///
    /// Returns an error if
    ///
    /// * Forwards a [`sqlx::error::Error`] if database operation fails.
    pub async fn initialize(&self) -> Result<(), DbError> {
        sqlx::query(resource_groups_creation_query())
            .execute(&self.pool)
            .await?;

        sqlx::query(jobs_creation_query().as_str())
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl ExternalJobOrchestration for MariaDbStorage {
    async fn register(
        &self,
        resource_group_id: ResourceGroupId,
        task_graph: Arc<TaskGraph>,
        job_inputs: Vec<TaskInput>,
    ) -> Result<JobId, DbError> {
        const INSERT_QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`resource_group_id`, `serialized_task_graph`, \
             `serialized_job_inputs`, `commit_tdl_package`, `commit_tdl_function`, \
             `cleanup_tdl_package`, `cleanup_tdl_function`) VALUES (?, ?, ?, ?, ?, ?, ?) \
             RETURNING CAST(`id` AS CHAR) AS `id`;",
            table = JOBS_TABLE_NAME,
        );

        let rg_id_str = resource_group_id.as_uuid_ref().to_string();

        let serialized_task_graph = task_graph
            .to_json()
            .map_err(|e| DbError::TaskGraphSerializationFailure(Box::new(e)))?;
        let serialized_job_inputs =
            serde_json::to_string(&job_inputs).map_err(DbError::value_ser)?;

        let result = sqlx::query(INSERT_QUERY)
            .bind(&rg_id_str)
            .bind(serialized_task_graph)
            .bind(serialized_job_inputs)
            .bind(None::<String>)
            .bind(None::<String>)
            .bind(None::<String>)
            .bind(None::<String>)
            .fetch_one(&self.pool)
            .await;

        match result {
            Ok(row) => {
                let id_str: String = row.get(0);
                parse_job_id(&id_str)
            }
            Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23000") => {
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
            "SELECT `state`, CAST(`resource_group_id` AS CHAR) FROM `{table}` WHERE `id` = ? FOR \
             UPDATE;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ? WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let mut tx = self.pool.begin().await?;
        let job_id_str = job_id.as_uuid_ref().to_string();

        let row: Option<(String, String)> = sqlx::query_as(SELECT_QUERY)
            .bind(&job_id_str)
            .fetch_optional(&mut *tx)
            .await?;

        let (state_str, rg_id_str) = row.ok_or(DbError::JobNotFound(job_id))?;
        sql_utils::validate_resource_group_access(&rg_id_str, resource_group_id)?;

        let state = sql_utils::parse_job_state(&state_str)?;
        if state != JobState::Ready {
            return Err(DbError::UnexpectedJobState {
                current: state,
                expected: ExpectedStates(vec![JobState::Ready]),
            });
        }

        sqlx::query(UPDATE_QUERY)
            .bind(JobState::Running.to_string())
            .bind(&job_id_str)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn cancel(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), DbError> {
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `state`, CAST(`resource_group_id` AS CHAR), `cleanup_tdl_package` FROM \
             `{table}` WHERE `id` = ? FOR UPDATE;",
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

        let mut tx = self.pool.begin().await?;
        let job_id_str = job_id.as_uuid_ref().to_string();

        let row: Option<(String, String, Option<String>)> = sqlx::query_as(SELECT_QUERY)
            .bind(&job_id_str)
            .fetch_optional(&mut *tx)
            .await?;

        let (state_str, rg_id_str, cleanup_tdl_package) =
            row.ok_or(DbError::JobNotFound(job_id))?;
        sql_utils::validate_resource_group_access(&rg_id_str, resource_group_id)?;

        let state = sql_utils::parse_job_state(&state_str)?;
        if state.is_terminal() {
            return Err(DbError::UnexpectedJobState {
                current: state,
                expected: ExpectedStates(vec![
                    JobState::Ready,
                    JobState::Running,
                    JobState::CommitReady,
                    JobState::CleanupReady,
                ]),
            });
        }

        if cleanup_tdl_package.is_some() {
            sqlx::query(UPDATE_STATE_QUERY)
                .bind(JobState::CleanupReady.to_string())
                .bind(&job_id_str)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query(UPDATE_STATE_AND_END_QUERY)
                .bind(JobState::Cancelled.to_string())
                .bind(&job_id_str)
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
            "SELECT `state`, CAST(`resource_group_id` AS CHAR) FROM `{table}` WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let row: Option<(String, String)> = sqlx::query_as(QUERY)
            .bind(job_id.as_uuid_ref().to_string())
            .fetch_optional(&self.pool)
            .await?;

        let (state_str, rg_id_str) = row.ok_or(DbError::JobNotFound(job_id))?;
        sql_utils::validate_resource_group_access(&rg_id_str, resource_group_id)?;

        sql_utils::parse_job_state(&state_str)
    }

    async fn get_outputs(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<Vec<TaskOutput>, DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `state`, CAST(`resource_group_id` AS CHAR), `serialized_job_outputs` FROM \
             `{table}` WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let job_id_str = job_id.as_uuid_ref().to_string();

        let row: Option<(String, String, Option<String>)> = sqlx::query_as(QUERY)
            .bind(&job_id_str)
            .fetch_optional(&self.pool)
            .await?;

        let (state_str, rg_id_str, serialized_outputs) =
            row.ok_or(DbError::JobNotFound(job_id))?;
        sql_utils::validate_resource_group_access(&rg_id_str, resource_group_id)?;

        let state = sql_utils::parse_job_state(&state_str)?;
        if state != JobState::Succeeded {
            return Err(DbError::UnexpectedJobState {
                current: state,
                expected: ExpectedStates(vec![JobState::Succeeded]),
            });
        }

        let outputs_str = serialized_outputs.ok_or_else(|| {
            DbError::CorruptedDbState(format!(
                "job `{job_id_str}` succeeded but has no serialized outputs"
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
            "SELECT `state`, CAST(`resource_group_id` AS CHAR), `error_message` FROM `{table}` \
             WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let job_id_str = job_id.as_uuid_ref().to_string();

        let row: Option<(String, String, Option<String>)> = sqlx::query_as(QUERY)
            .bind(&job_id_str)
            .fetch_optional(&self.pool)
            .await?;

        let (state_str, rg_id_str, error_message) = row.ok_or(DbError::JobNotFound(job_id))?;
        sql_utils::validate_resource_group_access(&rg_id_str, resource_group_id)?;

        let state = sql_utils::parse_job_state(&state_str)?;
        if state != JobState::Failed {
            return Err(DbError::UnexpectedJobState {
                current: state,
                expected: ExpectedStates(vec![JobState::Failed]),
            });
        }

        let message = error_message.ok_or_else(|| {
            DbError::CorruptedDbState(format!(
                "job `{job_id_str}` failed but has no error message"
            ))
        })?;
        Ok(message)
    }
}

#[async_trait]
impl InternalJobOrchestration for MariaDbStorage {
    async fn set_state(&self, job_id: JobId, state: JobState) -> Result<(), DbError> {
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `state` FROM `{table}` WHERE `id` = ? FOR UPDATE;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ? WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let job_id_str = job_id.as_uuid_ref().to_string();
        let mut tx = self.pool.begin().await?;

        let row: Option<(String,)> = sqlx::query_as(SELECT_QUERY)
            .bind(&job_id_str)
            .fetch_optional(&mut *tx)
            .await?;

        let (current_state_str,) = row.ok_or(DbError::JobNotFound(job_id))?;
        let current_state = sql_utils::parse_job_state(&current_state_str)?;

        if !JobState::is_valid_transition(current_state, state) {
            return Err(DbError::InvalidJobStateTransition {
                from: current_state,
                to: state,
            });
        }

        sqlx::query(UPDATE_QUERY)
            .bind(state.to_string())
            .bind(&job_id_str)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn commit_outputs(
        &self,
        job_id: JobId,
        job_outputs: Vec<TaskOutput>,
    ) -> Result<JobState, DbError> {
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `state`, `commit_tdl_package` FROM `{table}` WHERE `id` = ? FOR UPDATE;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_SUCCEEDED_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ?, `serialized_job_outputs` = ?, \
             `ended_at` = CURRENT_TIMESTAMP WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_COMMIT_READY_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ?, `serialized_job_outputs` = ? WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let job_id_str = job_id.as_uuid_ref().to_string();
        let mut tx = self.pool.begin().await?;

        let row: Option<(String, Option<String>)> = sqlx::query_as(SELECT_QUERY)
            .bind(&job_id_str)
            .fetch_optional(&mut *tx)
            .await?;

        let (state_str, commit_tdl_package) = row.ok_or(DbError::JobNotFound(job_id))?;
        let current_state = sql_utils::parse_job_state(&state_str)?;

        if current_state != JobState::Running {
            return Err(DbError::InvalidJobStateTransition {
                from: current_state,
                to: JobState::CommitReady,
            });
        }

        let serialized_outputs =
            serde_json::to_string(&job_outputs).map_err(DbError::value_ser)?;

        let new_state = if commit_tdl_package.is_some() {
            sqlx::query(UPDATE_COMMIT_READY_QUERY)
                .bind(JobState::CommitReady.to_string())
                .bind(&serialized_outputs)
                .bind(&job_id_str)
                .execute(&mut *tx)
                .await?;
            JobState::CommitReady
        } else {
            sqlx::query(UPDATE_SUCCEEDED_QUERY)
                .bind(JobState::Succeeded.to_string())
                .bind(&serialized_outputs)
                .bind(&job_id_str)
                .execute(&mut *tx)
                .await?;
            JobState::Succeeded
        };

        tx.commit().await?;
        Ok(new_state)
    }

    async fn cancel(&self, job_id: JobId) -> Result<JobState, DbError> {
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `state`, `cleanup_tdl_package` FROM `{table}` WHERE `id` = ? FOR UPDATE;",
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

        let job_id_str = job_id.as_uuid_ref().to_string();
        let mut tx = self.pool.begin().await?;

        let row: Option<(String, Option<String>)> = sqlx::query_as(SELECT_QUERY)
            .bind(&job_id_str)
            .fetch_optional(&mut *tx)
            .await?;

        let (state_str, cleanup_tdl_package) = row.ok_or(DbError::JobNotFound(job_id))?;
        let current_state = sql_utils::parse_job_state(&state_str)?;

        if current_state.is_terminal() {
            return Err(DbError::InvalidJobStateTransition {
                from: current_state,
                to: JobState::Cancelled,
            });
        }

        let new_state = if cleanup_tdl_package.is_some() {
            sqlx::query(UPDATE_STATE_QUERY)
                .bind(JobState::CleanupReady.to_string())
                .bind(&job_id_str)
                .execute(&mut *tx)
                .await?;
            JobState::CleanupReady
        } else {
            sqlx::query(UPDATE_STATE_AND_END_QUERY)
                .bind(JobState::Cancelled.to_string())
                .bind(&job_id_str)
                .execute(&mut *tx)
                .await?;
            JobState::Cancelled
        };

        tx.commit().await?;
        Ok(new_state)
    }

    async fn fail(&self, job_id: JobId, error_message: String) -> Result<(), DbError> {
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `state` FROM `{table}` WHERE `id` = ? FOR UPDATE;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ?, `error_message` = ?, \
             `ended_at` = CURRENT_TIMESTAMP WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let job_id_str = job_id.as_uuid_ref().to_string();
        let mut tx = self.pool.begin().await?;

        let row: Option<(String,)> = sqlx::query_as(SELECT_QUERY)
            .bind(&job_id_str)
            .fetch_optional(&mut *tx)
            .await?;

        let (state_str,) = row.ok_or(DbError::JobNotFound(job_id))?;
        let current_state = sql_utils::parse_job_state(&state_str)?;

        if !JobState::is_valid_transition(current_state, JobState::Failed) {
            return Err(DbError::InvalidJobStateTransition {
                from: current_state,
                to: JobState::Failed,
            });
        }

        sqlx::query(UPDATE_QUERY)
            .bind(JobState::Failed.to_string())
            .bind(&error_message)
            .bind(&job_id_str)
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
            "SELECT CAST(`id` AS CHAR) FROM `{table}` WHERE `state` IN \
             ('Succeeded','Failed','Cancelled') AND \
             `ended_at` < NOW() - INTERVAL ? SECOND;",
            table = JOBS_TABLE_NAME,
        );
        const DELETE_QUERY: &str = formatcp!(
            "DELETE FROM `{table}` WHERE `state` IN \
             ('Succeeded','Failed','Cancelled') AND \
             `ended_at` < NOW() - INTERVAL ? SECOND;",
            table = JOBS_TABLE_NAME,
        );

        let timeout_secs = expire_after.as_secs();
        let mut tx = self.pool.begin().await?;

        let rows: Vec<(String,)> = sqlx::query_as(SELECT_QUERY)
            .bind(timeout_secs)
            .fetch_all(&mut *tx)
            .await?;

        let mut job_ids: Vec<JobId> = Vec::with_capacity(rows.len());
        for (id_str,) in &rows {
            job_ids.push(parse_job_id(id_str)?);
        }

        if !job_ids.is_empty() {
            sqlx::query(DELETE_QUERY)
                .bind(timeout_secs)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(job_ids)
    }
}

#[async_trait]
impl ResourceGroupManagement for MariaDbStorage {
    async fn add(
        &self,
        external_resource_group_id: String,
        password: String,
    ) -> Result<ResourceGroupId, DbError> {
        const QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`external_id`, `password`) VALUES (?, ?) \
             RETURNING CAST(`id` AS CHAR) AS `id`;",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );

        let result = sqlx::query(QUERY)
            .bind(external_resource_group_id.clone())
            .bind(password)
            .fetch_one(&self.pool)
            .await;

        match result {
            Ok(row) => {
                let id_str: String = row.get(0);
                parse_resource_group_id(&id_str)
            }
            Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23000") => Err(
                DbError::ResourceGroupAlreadyExists(external_resource_group_id),
            ),
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
            .bind(resource_group_id.as_uuid_ref().to_string())
            .fetch_optional(&self.pool)
            .await?;

        match row {
            None => Err(DbError::ResourceGroupNotFound(resource_group_id)),
            Some((stored_password,)) if stored_password != password => {
                Err(DbError::InvalidPassword(resource_group_id))
            }
            Some(_) => Ok(()),
        }
    }

    async fn delete(&self, resource_group_id: ResourceGroupId) -> Result<(), DbError> {
        const DELETE_JOBS_QUERY: &str = formatcp!(
            "DELETE FROM `{table}` WHERE `resource_group_id` = ?;",
            table = JOBS_TABLE_NAME,
        );
        const DELETE_RG_QUERY: &str = formatcp!(
            "DELETE FROM `{table}` WHERE `id` = ?;",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );

        let rg_id_str = resource_group_id.as_uuid_ref().to_string();
        let mut tx = self.pool.begin().await?;

        sqlx::query(DELETE_JOBS_QUERY)
            .bind(&rg_id_str)
            .execute(&mut *tx)
            .await?;

        let result = sqlx::query(DELETE_RG_QUERY)
            .bind(&rg_id_str)
            .execute(&mut *tx)
            .await?;

        if result.rows_affected() == 0 {
            return Err(DbError::ResourceGroupNotFound(resource_group_id));
        }

        tx.commit().await?;
        Ok(())
    }
}
