use std::net::IpAddr;

use async_trait::async_trait;
use const_format::formatcp;
use secrecy::ExposeSecret;
use spider_core::job::JobState;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SchedulerId;
use spider_core::types::id::SessionId;
use spider_core::types::io::SerializedTaskOutputs;
use spider_core::types::io::TaskOutput;
use spider_core::types::scheduler::RegisteredScheduler;
use spider_derive::MySqlEnum;
use sqlx::Connection;
use sqlx::MySqlPool;
use sqlx::mysql::MySqlDatabaseError;

use crate::config::DatabaseConfig;
use crate::db::DbError;
use crate::db::DbStorage;
use crate::db::ExecutionManagerLivenessManagement;
use crate::db::ExternalJobOrchestration;
use crate::db::InternalJobOrchestration;
use crate::db::RecoverableJobContext;
use crate::db::ResourceGroupManagement;
use crate::db::SchedulerRegistrationManagement;
use crate::db::SessionManagement;
use crate::db::error::ExpectedStates;
use crate::job_submission::ValidatedJobSubmission;

/// A cloneable storage connector for `MariaDB` database that implements Spider's DB protocols.
#[derive(Clone)]
pub struct MariaDbStorageConnector {
    pool: MySqlPool,
    session_id: SessionId,
}

impl MariaDbStorageConnector {
    /// Connects to database and initializes tables.
    ///
    /// # Parameters
    ///
    /// * `config`: Database configuration parameters for connecting to the database.
    ///
    /// # Returns
    ///
    /// A newly created [`MariaDbStorageConnector`] instance for connection on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::mysql::MySqlPoolOptions::connect`]'s return values on failure.
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    /// * Forwards [`sqlx::query::QueryScalar::fetch_one`]'s return values on failure.
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        const BUMP_SESSION_ID_QUERY: &str = formatcp!(
            "INSERT INTO `{table}` () VALUES () RETURNING `session_id`;",
            table = SESSIONS_TABLE_NAME,
        );

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

        // MariaDB does not support transactions for DDL statements. All DDL statements are
        // automatically committed. Thus, each table creation query is executed separately, and
        // atomicity is not guaranteed.
        sqlx::query(resource_groups_creation_query())
            .execute(&pool)
            .await?;
        sqlx::query(jobs_creation_query()).execute(&pool).await?;
        sqlx::query(sessions_creation_query())
            .execute(&pool)
            .await?;
        sqlx::query(execution_managers_creation_query())
            .execute(&pool)
            .await?;
        sqlx::query(schedulers_creation_query())
            .execute(&pool)
            .await?;

        let session_id = sqlx::query_scalar::<_, SessionId>(BUMP_SESSION_ID_QUERY)
            .fetch_one(&pool)
            .await?;

        Ok(Self { pool, session_id })
    }
}

#[async_trait]
impl ExternalJobOrchestration for MariaDbStorageConnector {
    async fn register(
        &self,
        resource_group_id: ResourceGroupId,
        job_submission: &ValidatedJobSubmission,
    ) -> Result<JobId, DbError> {
        const INSERT_QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`resource_group_id`, `compressed_serialized_task_graph`, \
             `compressed_serialized_job_inputs`) VALUES (?, ?, ?) RETURNING `id`;",
            table = JOBS_TABLE_NAME,
        );

        let compressed_serialized_task_graph = job_submission.compressed_serialized_task_graph();
        let compressed_serialized_job_inputs = job_submission.compressed_serialized_job_inputs();

        let job_id: JobId = sqlx::query_scalar(INSERT_QUERY)
            .bind(resource_group_id)
            .bind(compressed_serialized_task_graph)
            .bind(compressed_serialized_job_inputs)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::Database(e)
                    if e.try_downcast_ref::<MySqlDatabaseError>()
                        .is_some_and(|mysql_err| mysql_err.number() == MYSQL_ER_FK_CONSTRAINT) =>
                {
                    DbError::ResourceGroupNotFound(resource_group_id)
                }
                e => e.into(),
            })?;
        Ok(job_id)
    }

    async fn get_state(&self, job_id: JobId) -> Result<JobState, DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `state` FROM `{table}` WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let row: Option<(JobState,)> = sqlx::query_as(QUERY)
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await?;

        let (state,) = row.ok_or(DbError::JobNotFound(job_id))?;
        Ok(state)
    }

    async fn get_outputs(&self, job_id: JobId) -> Result<Vec<TaskOutput>, DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `state`, `serialized_job_outputs` FROM `{table}` WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let Some((state, serialized_outputs)) =
            sqlx::query_as::<_, (JobState, Option<Vec<u8>>)>(QUERY)
                .bind(job_id)
                .fetch_optional(&self.pool)
                .await?
        else {
            return Err(DbError::JobNotFound(job_id));
        };

        if state != JobState::Succeeded {
            return Err(DbError::UnexpectedJobState {
                current: state,
                expected: ExpectedStates(vec![JobState::Succeeded]),
            });
        }

        let outputs_bytes = serialized_outputs.ok_or_else(|| {
            DbError::CorruptedDbState(format!(
                "job `{job_id}` succeeded but has no serialized outputs"
            ))
        })?;
        let outputs = SerializedTaskOutputs::deserialize_from_raw(&outputs_bytes)
            .map_err(|e| DbError::ValueDeserializationFailure(Box::new(e)))?;
        Ok(outputs)
    }

    async fn get_error(&self, job_id: JobId) -> Result<String, DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `state`, `error_message` FROM `{table}` WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let Some((state, error_message)) = sqlx::query_as::<_, (JobState, Option<String>)>(QUERY)
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Err(DbError::JobNotFound(job_id));
        };

        if state != JobState::Failed {
            return Err(DbError::UnexpectedJobState {
                current: state,
                expected: ExpectedStates(vec![JobState::Failed]),
            });
        }

        let message = error_message.ok_or_else(|| {
            DbError::CorruptedDbState(format!("job `{job_id}` failed but has no error message"))
        })?;
        Ok(message)
    }
}

#[async_trait]
impl InternalJobOrchestration for MariaDbStorageConnector {
    async fn start(&self, job_id: JobId) -> Result<(), DbError> {
        let mut tx = self.pool.begin().await?;

        let state = fetch_job_state_for_update(&mut tx, job_id).await?;

        if state != JobState::Ready {
            return Err(DbError::UnexpectedJobState {
                current: state,
                expected: ExpectedStates(vec![JobState::Ready]),
            });
        }

        sqlx::query(UPDATE_JOB_STATE)
            .bind(JobState::Running)
            .bind(job_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn set_state(&self, job_id: JobId, state: JobState) -> Result<(), DbError> {
        let mut tx = self.pool.begin().await?;

        let current_state = fetch_job_state_for_update(&mut tx, job_id).await?;
        transition_job_state(&mut tx, job_id, current_state, state).await?;

        tx.commit().await?;
        Ok(())
    }

    async fn commit_outputs(
        &self,
        job_id: JobId,
        job_outputs: Vec<TaskOutput>,
        has_commit_task: bool,
    ) -> Result<(), DbError> {
        const UPDATE_SUCCEEDED_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ?, `serialized_job_outputs` = ?, `ended_at` = \
             CURRENT_TIMESTAMP WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );
        const UPDATE_COMMIT_READY_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ?, `serialized_job_outputs` = ? WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let new_state = if has_commit_task {
            JobState::CommitReady
        } else {
            JobState::Succeeded
        };
        let mut tx = self.pool.begin().await?;

        let current_state = fetch_job_state_for_update(&mut tx, job_id).await?;

        if !JobState::is_valid_transition(current_state, new_state) {
            return Err(DbError::InvalidJobStateTransition {
                from: current_state,
                to: new_state,
            });
        }

        let serialized_outputs = SerializedTaskOutputs::serialize_with_size_hint(&job_outputs)
            .map_err(|e| DbError::ValueSerializationFailure(Box::new(e)))?
            .to_raw();

        sqlx::query(if new_state.is_terminal() {
            UPDATE_SUCCEEDED_QUERY
        } else {
            UPDATE_COMMIT_READY_QUERY
        })
        .bind(new_state)
        .bind(&serialized_outputs)
        .bind(job_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn cancel(&self, job_id: JobId, has_cleanup_task: bool) -> Result<(), DbError> {
        let new_state = if has_cleanup_task {
            JobState::CleanupReady
        } else {
            JobState::Cancelled
        };
        let mut tx = self.pool.begin().await?;

        let current_state = fetch_job_state_for_update(&mut tx, job_id).await?;
        transition_job_state(&mut tx, job_id, current_state, new_state).await?;

        tx.commit().await?;
        Ok(())
    }

    async fn fail(&self, job_id: JobId, error_message: String) -> Result<(), DbError> {
        const UPDATE_QUERY: &str = formatcp!(
            "UPDATE `{table}` SET `state` = ?, `error_message` = ?, `ended_at` = \
             CURRENT_TIMESTAMP WHERE `id` = ?;",
            table = JOBS_TABLE_NAME,
        );

        let mut tx = self.pool.begin().await?;

        let current_state = fetch_job_state_for_update(&mut tx, job_id).await?;

        if !JobState::is_valid_transition(current_state, JobState::Failed) {
            return Err(DbError::InvalidJobStateTransition {
                from: current_state,
                to: JobState::Failed,
            });
        }

        sqlx::query(UPDATE_QUERY)
            .bind(JobState::Failed)
            .bind(&error_message)
            .bind(job_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn delete_expired_terminated_jobs(
        &self,
        expire_after_sec: u64,
    ) -> Result<Vec<JobId>, DbError> {
        const DELETE_BATCH_SIZE: usize = 1000;

        const SELECT_CANDIDATES_QUERY: &str = formatcp!(
            "SELECT `id` FROM `{table}` WHERE `state` IN \
             ('{succeeded_state}','{failed_state}','{cancelled_state}') AND `ended_at` < NOW() - \
             INTERVAL ? SECOND LIMIT {DELETE_BATCH_SIZE};",
            table = JOBS_TABLE_NAME,
            succeeded_state = JobState::Succeeded.as_str(),
            failed_state = JobState::Failed.as_str(),
            cancelled_state = JobState::Cancelled.as_str(),
        );

        let mut deleted_job_ids: Vec<JobId> = Vec::new();
        let mut tx = self.pool.begin().await?;

        loop {
            let candidate_ids: Vec<JobId> = sqlx::query_scalar(SELECT_CANDIDATES_QUERY)
                .bind(expire_after_sec)
                .fetch_all(&mut *tx)
                .await?;

            if candidate_ids.is_empty() {
                break;
            }

            let candidate_count = candidate_ids.len();
            let placeholders = std::iter::repeat_n("?", candidate_count)
                .collect::<Vec<_>>()
                .join(",");
            let delete_stmt = format!(
                "DELETE FROM `{table}` WHERE `id` IN ({placeholders}) AND `state` IN \
                 ('{succeeded_state}','{failed_state}','{cancelled_state}');",
                table = JOBS_TABLE_NAME,
                succeeded_state = JobState::Succeeded.as_str(),
                failed_state = JobState::Failed.as_str(),
                cancelled_state = JobState::Cancelled.as_str(),
            );
            let mut delete_query = sqlx::query(&delete_stmt);
            for job_id in &candidate_ids {
                delete_query = delete_query.bind(job_id);
            }
            let rows_affected = delete_query.execute(&mut *tx).await?.rows_affected();

            if rows_affected != candidate_count as u64 {
                return Err(DbError::CorruptedDbState(format!(
                    "expected to delete {candidate_count} rows but only {rows_affected} rows \
                     deleted"
                )));
            }

            deleted_job_ids.extend(candidate_ids);

            if candidate_count < DELETE_BATCH_SIZE {
                break;
            }
        }

        tx.commit().await?;
        Ok(deleted_job_ids)
    }

    async fn get_recoverable_jobs(&self) -> Result<Vec<RecoverableJobContext>, DbError> {
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `id`, `resource_group_id`, `state`, `compressed_serialized_task_graph`, \
             `compressed_serialized_job_inputs`, `serialized_job_outputs` FROM `{table}` WHERE \
             `state` IN \
             ('{ready_state}','{running_state}','{commit_ready_state}','{cleanup_ready_state}');",
            table = JOBS_TABLE_NAME,
            ready_state = JobState::Ready.as_str(),
            running_state = JobState::Running.as_str(),
            commit_ready_state = JobState::CommitReady.as_str(),
            cleanup_ready_state = JobState::CleanupReady.as_str(),
        );

        sqlx::query_as::<_, RecoverableJobRowProjection>(SELECT_QUERY)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(RecoverableJobRowProjection::into_recoverable_job_context)
            .collect()
    }
}

#[async_trait]
impl ResourceGroupManagement for MariaDbStorageConnector {
    async fn add(
        &self,
        external_resource_group_id: String,
        password: Vec<u8>,
    ) -> Result<ResourceGroupId, DbError> {
        const QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`external_id`, `password`) VALUES (?, ?) RETURNING `id`;",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );

        let resource_group_id = sqlx::query_scalar(QUERY)
            .bind(&external_resource_group_id)
            .bind(password)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::Database(e)
                    if e.try_downcast_ref::<MySqlDatabaseError>()
                        .is_some_and(|mysql_err| mysql_err.number() == MYSQL_ER_DUP_ENTRY) =>
                {
                    DbError::ResourceGroupAlreadyExists(external_resource_group_id)
                }
                e => e.into(),
            })?;

        Ok(resource_group_id)
    }

    async fn verify(
        &self,
        resource_group_id: ResourceGroupId,
        password: &[u8],
    ) -> Result<(), DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `password` FROM `{table}` WHERE `id` = ?;",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );

        use subtle::ConstantTimeEq;

        let Some(stored_password) = sqlx::query_scalar::<_, Vec<u8>>(QUERY)
            .bind(resource_group_id)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Err(DbError::ResourceGroupNotFound(resource_group_id));
        };

        if stored_password.ct_eq(password).into() {
            Ok(())
        } else {
            Err(DbError::InvalidPassword(resource_group_id))
        }
    }

    async fn delete(&self, _resource_group_id: ResourceGroupId) -> Result<(), DbError> {
        todo!("not implemented")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, MySqlEnum)]
enum ExecutionManagerState {
    Alive,
    Dead,
}

#[async_trait]
impl ExecutionManagerLivenessManagement for MariaDbStorageConnector {
    async fn register_execution_manager(
        &self,
        ip_address: IpAddr,
    ) -> Result<ExecutionManagerId, DbError> {
        const INSERT_QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`ip_address`) VALUES (?) RETURNING `id`;",
            table = EXECUTION_MANAGERS_TABLE_NAME,
        );

        sqlx::query_scalar(INSERT_QUERY)
            .bind(ip_address.to_string())
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
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

#[async_trait]
impl SchedulerRegistrationManagement for MariaDbStorageConnector {
    async fn register_scheduler(
        &self,
        ip_address: IpAddr,
        port: u16,
    ) -> Result<SchedulerId, DbError> {
        const DELETE_QUERY: &str =
            formatcp!("DELETE FROM `{table}`;", table = SCHEDULERS_TABLE_NAME,);
        const INSERT_QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`ip_address`, `port`) VALUES (?, ?) RETURNING `id`;",
            table = SCHEDULERS_TABLE_NAME,
        );

        let mut tx = self.pool.begin().await?;
        sqlx::query(DELETE_QUERY).execute(&mut *tx).await?;
        let scheduler_id = sqlx::query_scalar(INSERT_QUERY)
            .bind(ip_address.to_string())
            .bind(port)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(scheduler_id)
    }

    async fn get_schedulers(&self) -> Result<Vec<RegisteredScheduler>, DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `id`, `ip_address`, `port` FROM `{table}` ORDER BY `id` ASC;",
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

impl SessionManagement for MariaDbStorageConnector {
    fn session_id(&self) -> SessionId {
        self.session_id
    }
}

impl DbStorage for MariaDbStorageConnector {}

/// `MySQL` error number for foreign key constraint violation.
const MYSQL_ER_FK_CONSTRAINT: u16 = 1452;

/// `MySQL` error number for duplicate entry.
const MYSQL_ER_DUP_ENTRY: u16 = 1062;

const RESOURCE_GROUPS_TABLE_NAME: &str = "resource_groups";
const JOBS_TABLE_NAME: &str = "jobs";
const EXECUTION_MANAGERS_TABLE_NAME: &str = "execution_managers";
const SCHEDULERS_TABLE_NAME: &str = "schedulers";
const SESSIONS_TABLE_NAME: &str = "sessions";

const UPDATE_JOB_STATE: &str = formatcp!(
    "UPDATE `{table}` SET `state` = ? WHERE `id` = ?;",
    table = JOBS_TABLE_NAME,
);

#[must_use]
const fn resource_groups_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{RESOURCE_GROUPS_TABLE_NAME}` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `external_id` VARCHAR(256) NOT NULL,
  `password` VARBINARY(2048) NOT NULL,
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
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `resource_group_id` BIGINT UNSIGNED NOT NULL,
  `state` {state_enum} NOT NULL DEFAULT {default_state},
  `compressed_serialized_task_graph` LONGBLOB NOT NULL,
  `compressed_serialized_job_inputs` LONGBLOB NOT NULL,
  `serialized_job_outputs` LONGBLOB,
  `error_message` LONGTEXT,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ended_at` TIMESTAMP,
  `max_num_retries` INT UNSIGNED NOT NULL DEFAULT 0,
  `num_retries` INT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  INDEX `job_state` (`state`),
  CONSTRAINT `job_resource_group` FOREIGN KEY (`resource_group_id`)
    REFERENCES `{RESOURCE_GROUPS_TABLE_NAME}` (`id`)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);",
        state_enum = JobState::as_mysql_enum_decl(),
        default_state = JobState::Ready.as_quoted_str(),
    )
}

#[must_use]
const fn execution_managers_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{EXECUTION_MANAGERS_TABLE_NAME}` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `ip_address` VARCHAR(45) NOT NULL,
  `state` {state_enum} NOT NULL DEFAULT {default_state},
  `last_heartbeat_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `death_confirmed_at` TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `execution_manager_liveness` (`state`, `last_heartbeat_at`)
);",
        state_enum = ExecutionManagerState::as_mysql_enum_decl(),
        default_state = ExecutionManagerState::Alive.as_quoted_str(),
    )
}

#[must_use]
const fn schedulers_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{SCHEDULERS_TABLE_NAME}` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `ip_address` VARCHAR(45) NOT NULL,
  `port` SMALLINT UNSIGNED NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
);"
    )
}

#[must_use]
const fn sessions_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{SESSIONS_TABLE_NAME}` (
  `session_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`session_id`)
);"
    )
}

/// A raw row selected from the job table representing a recoverable job.
#[derive(sqlx::FromRow)]
struct RecoverableJobRowProjection {
    id: JobId,
    resource_group_id: ResourceGroupId,
    state: JobState,
    compressed_serialized_task_graph: Vec<u8>,
    compressed_serialized_job_inputs: Vec<u8>,
    serialized_job_outputs: Option<Vec<u8>>,
}

impl RecoverableJobRowProjection {
    /// Converts the row projection into [`RecoverableJobContext`].
    ///
    /// # Returns
    ///
    /// The context of the recoverable job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`SerializedTaskOutputs::deserialize_from_raw`]'s return values on failure.
    /// * Forwards [`ValidatedJobSubmission::create`]'s return values as
    ///   [`DbError::CorruptedDbState`] on failure.
    fn into_recoverable_job_context(self) -> Result<RecoverableJobContext, DbError> {
        let submission = ValidatedJobSubmission::create(
            self.compressed_serialized_task_graph,
            self.compressed_serialized_job_inputs,
        )
        .map_err(|e| DbError::CorruptedDbState(e.to_string()))?;
        let outputs = self
            .serialized_job_outputs
            .map(|outputs| {
                SerializedTaskOutputs::deserialize_from_raw(&outputs)
                    .map_err(|e| DbError::ValueDeserializationFailure(Box::new(e)))
            })
            .transpose()?;
        Ok(RecoverableJobContext {
            id: self.id,
            resource_group_id: self.resource_group_id,
            state: self.state,
            submission,
            outputs,
        })
    }
}

/// A raw row selected from the schedulers table.
#[derive(sqlx::FromRow)]
struct SchedulerRowProjection {
    id: SchedulerId,
    ip_address: String,
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
    /// * [`DbError::CorruptedDbState`] if the scheduler IP address is invalid.
    fn into_registered_scheduler(self) -> Result<RegisteredScheduler, DbError> {
        let ip_address = self.ip_address.parse().map_err(|error| {
            DbError::CorruptedDbState(format!(
                "scheduler `{}` has invalid IP address `{}`: {error}",
                self.id, self.ip_address
            ))
        })?;
        Ok(RegisteredScheduler {
            id: self.id,
            ip_address,
            port: self.port,
        })
    }
}

/// Gets the job state with exclusive lock on the row.
///
/// # Returns
///
/// The current state of the job on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`DbError::JobNotFound`] if the `job_id` does not exist.
/// * Forwards [`sqlx::query::Query::fetch_optional`]'s return values on failure.
async fn fetch_job_state_for_update(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    job_id: JobId,
) -> Result<JobState, DbError> {
    const SELECT_JOB_STATE_FOR_UPDATE: &str = formatcp!(
        "SELECT `state` FROM `{table}` WHERE `id` = ? FOR UPDATE;",
        table = JOBS_TABLE_NAME,
    );

    let state = sqlx::query_scalar::<_, JobState>(SELECT_JOB_STATE_FOR_UPDATE)
        .bind(job_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(DbError::JobNotFound(job_id))?;
    Ok(state)
}

/// Updates job state.
///
/// Updates the job end timestamp if job state is updated to a terminal state.
///
/// # Parameters
/// * `tx`: The transaction to execute the query in.
/// * `job_id`: The ID of the job to update the state for.
/// * `current_state`: The current state of the job. Used for validating state transition.
/// * `new_state`: The new state to update to.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`DbError::InvalidJobStateTransition`] if the job state transition is invalid.
/// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
async fn transition_job_state(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    job_id: JobId,
    current_state: JobState,
    new_state: JobState,
) -> Result<(), DbError> {
    const UPDATE_JOB_STATE_AND_END: &str = formatcp!(
        "UPDATE `{table}` SET `state` = ?, `ended_at` = CURRENT_TIMESTAMP WHERE `id` = ?;",
        table = JOBS_TABLE_NAME,
    );

    if !JobState::is_valid_transition(current_state, new_state) {
        return Err(DbError::InvalidJobStateTransition {
            from: current_state,
            to: new_state,
        });
    }

    sqlx::query(if new_state.is_terminal() {
        UPDATE_JOB_STATE_AND_END
    } else {
        UPDATE_JOB_STATE
    })
    .bind(new_state)
    .bind(job_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

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
