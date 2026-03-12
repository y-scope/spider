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

use super::sql_utils;
use crate::db::{
    DbError,
    ExternalJobOrchestration,
    InternalJobOrchestration,
    ResourceGroupStorage,
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
    async fn register_job(
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
            .map_err(|e| DbError::DataIntegrity(format!("failed to serialize task graph: {e}")))?;
        let serialized_job_inputs = serde_json::to_string(&job_inputs)
            .map_err(|e| DbError::DataIntegrity(format!("failed to serialize job inputs: {e}")))?;

        let (commit_pkg, commit_fn) = task_graph.get_commit_task().map_or((None, None), |t| {
            (Some(t.tdl_package.clone()), Some(t.tdl_function.clone()))
        });
        let (cleanup_pkg, cleanup_fn) = task_graph.get_cleanup_task().map_or((None, None), |t| {
            (Some(t.tdl_package.clone()), Some(t.tdl_function.clone()))
        });

        let result = sqlx::query(INSERT_QUERY)
            .bind(&rg_id_str)
            .bind(serialized_task_graph)
            .bind(serialized_job_inputs)
            .bind(commit_pkg)
            .bind(commit_fn)
            .bind(cleanup_pkg)
            .bind(cleanup_fn)
            .fetch_one(&self.pool)
            .await;

        match result {
            Ok(row) => {
                let id_str: String = row.get(0);
                id_str.parse::<JobId>().map_err(|e| {
                    DbError::DataIntegrity(format!("invalid job UUID from database: {e}"))
                })
            }
            Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23000") => {
                Err(DbError::ResourceGroupNotFound(resource_group_id))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn start_job(
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

    async fn cancel_job(
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

    async fn get_job_state(
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

    async fn get_job_outputs(
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

        let (state_str, rg_id_str, serialized_outputs) = row.ok_or(DbError::JobNotFound(job_id))?;
        sql_utils::validate_resource_group_access(&rg_id_str, resource_group_id)?;

        let state = sql_utils::parse_job_state(&state_str)?;
        if state != JobState::Succeeded {
            return Err(DbError::UnexpectedJobState {
                current: state,
                expected: ExpectedStates(vec![JobState::Succeeded]),
            });
        }

        let outputs_str = serialized_outputs.ok_or_else(|| {
            DbError::DataIntegrity(format!(
                "job `{job_id_str}` succeeded but has no serialized outputs"
            ))
        })?;
        let outputs: Vec<TaskOutput> = serde_json::from_str(&outputs_str).map_err(|e| {
            DbError::DataIntegrity(format!("failed to deserialize job outputs: {e}"))
        })?;
        Ok(outputs)
    }

    async fn get_job_error(
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
            DbError::DataIntegrity(format!(
                "job `{job_id_str}` failed but has no error message"
            ))
        })?;
        Ok(message)
    }
}

#[async_trait]
impl InternalJobOrchestration for MariaDbStorage {
    async fn set_job_state(
        &self,
        job_id: JobId,
        old_state: Option<&[JobState]>,
        new_state: JobState,
    ) -> Result<(), DbError> {
        let job_id_str = job_id.as_uuid_ref().to_string();
        let mut tx = self.pool.begin().await?;

        let rows_affected = if let Some(old_states) = old_state {
            let state_list = sql_utils::sql_quoted_list(old_states);
            let query = format!(
                "UPDATE `{JOBS_TABLE_NAME}` SET `state` = ? WHERE `id` = ? AND `state` IN \
                 ({state_list});"
            );
            sqlx::query(&query)
                .bind(new_state.to_string())
                .bind(&job_id_str)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        } else {
            const QUERY: &str = formatcp!(
                "UPDATE `{table}` SET `state` = ? WHERE `id` = ?;",
                table = JOBS_TABLE_NAME,
            );
            sqlx::query(QUERY)
                .bind(new_state.to_string())
                .bind(&job_id_str)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        };

        if rows_affected == 0 {
            const CHECK_QUERY: &str = formatcp!(
                "SELECT `state` FROM `{table}` WHERE `id` = ?;",
                table = JOBS_TABLE_NAME,
            );

            let row: Option<(String,)> = sqlx::query_as(CHECK_QUERY)
                .bind(&job_id_str)
                .fetch_optional(&mut *tx)
                .await?;

            match row {
                None => return Err(DbError::JobNotFound(job_id)),
                Some((state_str,)) => {
                    let state = sql_utils::parse_job_state(&state_str)?;
                    return Err(DbError::InvalidJobStateTransition {
                        from: state,
                        to: new_state,
                    });
                }
            }
        }

        tx.commit().await?;
        Ok(())
    }

    async fn delete_jobs(&self, timeout: Duration) -> Result<Vec<JobId>, DbError> {
        let timeout_secs = timeout.as_secs();

        let select_query = format!(
            "SELECT CAST(`id` AS CHAR) FROM `{JOBS_TABLE_NAME}` WHERE `state` IN ({terminal}) AND \
             `ended_at` < NOW() - INTERVAL {timeout_secs} SECOND;",
            terminal = sql_utils::sql_quoted_list(&JobState::TERMINAL),
        );

        let mut tx = self.pool.begin().await?;

        let rows: Vec<(String,)> = sqlx::query_as(&select_query).fetch_all(&mut *tx).await?;

        let mut job_ids: Vec<JobId> = Vec::with_capacity(rows.len());
        for (id_str,) in &rows {
            job_ids.push(id_str.parse().map_err(|e: uuid::Error| {
                DbError::DataIntegrity(format!("invalid job UUID: {e}"))
            })?);
        }

        if !job_ids.is_empty() {
            let placeholders = vec!["?"; job_ids.len()].join(",");
            let delete_query =
                format!("DELETE FROM `{JOBS_TABLE_NAME}` WHERE `id` IN ({placeholders});");
            let mut query = sqlx::query(&delete_query);
            for job_id in &job_ids {
                query = query.bind(job_id.as_uuid_ref().to_string());
            }
            query.execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(job_ids)
    }

    async fn reset_jobs(&self) -> Result<Vec<JobId>, DbError> {
        let select_query = format!(
            "SELECT CAST(`id` AS CHAR) FROM `{JOBS_TABLE_NAME}` WHERE `state` NOT IN \
             ({non_reset});",
            non_reset = sql_utils::sql_quoted_list(&[
                JobState::Ready,
                JobState::Succeeded,
                JobState::Failed,
                JobState::Cancelled,
            ]),
        );

        let mut tx = self.pool.begin().await?;

        let rows: Vec<(String,)> = sqlx::query_as(&select_query).fetch_all(&mut *tx).await?;

        let mut job_ids: Vec<JobId> = Vec::with_capacity(rows.len());
        for (id_str,) in &rows {
            job_ids.push(id_str.parse().map_err(|e: uuid::Error| {
                DbError::DataIntegrity(format!("invalid job UUID: {e}"))
            })?);
        }

        if !job_ids.is_empty() {
            let placeholders = vec!["?"; job_ids.len()].join(",");
            let update_query = format!(
                "UPDATE `{JOBS_TABLE_NAME}` SET `state` = ? WHERE `id` IN ({placeholders});"
            );
            let mut query = sqlx::query(&update_query).bind(JobState::Ready.to_string());
            for job_id in &job_ids {
                query = query.bind(job_id.as_uuid_ref().to_string());
            }
            query.execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(job_ids)
    }
}

#[async_trait]
impl ResourceGroupStorage for MariaDbStorage {
    async fn add_resource_group(
        &self,
        external_resource_group_id: String,
        password: String,
    ) -> Result<ResourceGroupId, DbError> {
        const QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`external_id`, `password`) VALUES (?, ?)RETURNING CAST(`id` \
             AS CHAR) AS `id`; ;",
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
                id_str.parse::<ResourceGroupId>().map_err(|e| {
                    DbError::DataIntegrity(format!("invalid job UUID from database: {e}"))
                })
            }
            Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23000") => Err(
                DbError::ResourceGroupAlreadyExists(external_resource_group_id),
            ),
            Err(e) => Err(e.into()),
        }
    }

    async fn verify_resource_group(
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

    async fn delete_resource_group(
        &self,
        resource_group_id: ResourceGroupId,
    ) -> Result<(), DbError> {
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
