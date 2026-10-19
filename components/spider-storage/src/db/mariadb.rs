use std::net::IpAddr;
use std::time::Duration;

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
use spider_core::types::resource_group::ExternalResourceGroupCredentials;
use spider_core::types::scheduler::RegisteredScheduler;
use spider_derive::MySqlEnum;
use spider_utils::config::Host;
use sqlx::Connection;
use sqlx::MySql;
use sqlx::MySqlConnection;
use sqlx::MySqlPool;
use sqlx::mysql::MySqlArguments;
use sqlx::mysql::MySqlDatabaseError;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::query::Query;
use subtle::ConstantTimeEq;

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
    /// * Forwards [`Self::pool_options`]'s return values on failure.
    /// * Forwards [`sqlx::pool::PoolOptions::connect_with`]'s return values on failure.
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
            .password(config.credentials.password.expose_secret())
            .statement_cache_capacity(config.pool.statement_cache_capacity);

        let pool = Self::pool_options(config)?
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
        sqlx::query(storage_fence_creation_query())
            .execute(&pool)
            .await?;
        sqlx::query(INSERT_FENCE_QUERY)
            .bind(config.fence_id)
            .execute(&pool)
            .await?;

        let session_id = SessionId::from(
            sqlx::query(BUMP_SESSION_ID_QUERY)
                .execute(&pool)
                .await?
                .last_insert_id(),
        );
        bump_fence(&pool, config.fence_id, session_id).await?;

        Ok(Self {
            pool,
            session_id,
            fence_id: config.fence_id,
        })
    }

    /// Builds the connection pool options from `config`.
    ///
    /// # Returns
    ///
    /// The pool options on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::InvalidConfig`] if `config.pool.min_connections` exceeds
    ///   `config.max_connections`.
    fn pool_options(config: &DatabaseConfig) -> Result<MySqlPoolOptions, DbError> {
        if config.pool.min_connections > config.max_connections {
            return Err(DbError::InvalidConfig(format!(
                "`pool.min_connections` ({}) exceeds `max_connections` ({})",
                config.pool.min_connections, config.max_connections
            )));
        }
        let pool_options = MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.pool.min_connections)
            .test_before_acquire(config.pool.test_before_acquire);
        if config.pool.test_before_acquire {
            return Ok(pool_options);
        }
        Ok(pool_options.max_lifetime(Duration::from_secs(UNPINGED_CONNECTION_MAX_LIFETIME_SEC)))
    }

    /// Checks, inside a transaction that writes, that no newer storage session has fenced this one
    /// off.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryScalar::fetch_optional`]'s return values on failure.
    /// * Forwards [`Self::ensure_fence_holds`]'s return values on failure.
    async fn check_fence(&self, connection: &mut MySqlConnection) -> Result<(), DbError> {
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

    /// Reads the fence row without locking it, to tell why a write with the fence in its statement
    /// wrote no row.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::QueryScalar::fetch_optional`]'s return values on failure.
    /// * Forwards [`Self::ensure_fence_holds`]'s return values on failure.
    async fn probe_fence(&self, connection: &mut MySqlConnection) -> Result<(), DbError> {
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

    /// Checks that `fence_session_id`, the session ID read from the fence row, is this connector's.
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

    /// Builds the `jobs` insert of a registration with the fence in the statement. A fenced insert
    /// fails with [`MYSQL_ER_BAD_NULL_ERROR`].
    ///
    /// # Type Parameters
    ///
    /// * `'submission_lifetime` - The lifetime of `job_submission`, which the returned insert
    ///   borrows.
    ///
    /// # Returns
    ///
    /// The insert, bound to register `job_submission` in `resource_group_id` while the fence row
    /// holds this connector's session.
    fn fenced_register_query<'submission_lifetime>(
        &self,
        resource_group_id: ResourceGroupId,
        job_submission: &'submission_lifetime ValidatedJobSubmission,
    ) -> Query<'submission_lifetime, MySql, MySqlArguments> {
        const QUERY: &str = formatcp!(
            "INSERT INTO `{JOBS_TABLE_NAME}` (`resource_group_id`, \
             `compressed_serialized_task_graph`, `compressed_serialized_job_inputs`) VALUES \
             ((SELECT ? FROM `{STORAGE_FENCE_TABLE_NAME}` WHERE `id` = ? AND `session_id` = ? \
             LOCK IN SHARE MODE), ?, ?);"
        );

        sqlx::query(QUERY)
            .bind(resource_group_id)
            .bind(self.fence_id)
            .bind(self.session_id)
            .bind(job_submission.compressed_serialized_task_graph())
            .bind(job_submission.compressed_serialized_job_inputs())
    }
}

#[async_trait]
impl ExternalJobOrchestration for MariaDbStorageConnector {
    async fn register(
        &self,
        resource_group_id: ResourceGroupId,
        job_submission: &ValidatedJobSubmission,
    ) -> Result<JobId, DbError> {
        let mut connection = self.pool.acquire().await?;
        let job_id = execute_fenced_job_insert(
            self.fenced_register_query(resource_group_id, job_submission),
            &mut connection,
            resource_group_id,
        )
        .await?;
        if let Some(job_id) = job_id {
            return Ok(job_id);
        }

        self.probe_fence(&mut connection).await?;
        Err(DbError::CorruptedDbState(
            "fenced insert wrote no row while the fence holds this session".to_owned(),
        ))
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
        self.check_fence(&mut tx).await?;

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
        self.check_fence(&mut tx).await?;

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
        self.check_fence(&mut tx).await?;

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
        self.check_fence(&mut tx).await?;

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
        self.check_fence(&mut tx).await?;

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
        self.check_fence(&mut tx).await?;

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
        credentials: ExternalResourceGroupCredentials,
    ) -> Result<ResourceGroupId, DbError> {
        const QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`external_id`, `password`) VALUES (?, ?);",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );

        let resource_group_id = sqlx::query(QUERY)
            .bind(credentials.get_external_resource_group_id())
            .bind(credentials.get_password())
            .execute(&self.pool)
            .await
            .map_err(|e| match e {
                e if is_duplicate_entry(&e) => DbError::ResourceGroupAlreadyExists(
                    credentials.get_external_resource_group_id().to_owned(),
                ),
                e => e.into(),
            })?
            .last_insert_id();

        Ok(ResourceGroupId::from(resource_group_id))
    }

    async fn add_or_verify(
        &self,
        credentials: ExternalResourceGroupCredentials,
    ) -> Result<ResourceGroupId, DbError> {
        const INSERT_QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`external_id`, `password`) VALUES (?, ?);",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `id`, `password` FROM `{table}` WHERE `external_id` = ? LOCK IN SHARE MODE;",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );

        run_read_committed_tx(self.pool.clone(), async move |connection| {
            let mut tx = connection.begin().await?;
            if let Some((resource_group_id, stored_password)) =
                sqlx::query_as::<_, (ResourceGroupId, Vec<u8>)>(SELECT_QUERY)
                    .bind(credentials.get_external_resource_group_id())
                    .fetch_optional(&mut *tx)
                    .await?
            {
                if !bool::from(stored_password.ct_eq(credentials.get_password())) {
                    return Err(DbError::InvalidPassword(resource_group_id));
                }
                tx.commit().await?;
                return Ok(resource_group_id);
            }

            let inserted_resource_group_id = match sqlx::query(INSERT_QUERY)
                .bind(credentials.get_external_resource_group_id())
                .bind(credentials.get_password())
                .execute(&mut *tx)
                .await
            {
                Ok(result) => Some(ResourceGroupId::from(result.last_insert_id())),
                Err(error) if is_duplicate_entry(&error) => None,
                Err(error) => return Err(error.into()),
            };

            if let Some(resource_group_id) = inserted_resource_group_id {
                tx.commit().await?;
                return Ok(resource_group_id);
            }

            let (resource_group_id, stored_password) =
                sqlx::query_as::<_, (ResourceGroupId, Vec<u8>)>(SELECT_QUERY)
                    .bind(credentials.get_external_resource_group_id())
                    .fetch_one(&mut *tx)
                    .await?;
            if !bool::from(stored_password.ct_eq(credentials.get_password())) {
                return Err(DbError::InvalidPassword(resource_group_id));
            }
            tx.commit().await?;
            Ok(resource_group_id)
        })
        .await
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

/// `MySQL` error number for a lock wait timeout.
const MYSQL_ER_LOCK_WAIT_TIMEOUT: u16 = 1205;

/// `MySQL` error number for a missing privilege on an operation.
const MYSQL_ER_SPECIFIC_ACCESS_DENIED: u16 = 1227;

/// `MySQL` error number for a missing privilege on a table.
const MYSQL_ER_TABLE_ACCESS_DENIED: u16 = 1142;

/// `MySQL` error number for a `NULL` written to a `NOT NULL` column.
const MYSQL_ER_BAD_NULL_ERROR: u16 = 1048;

const RESOURCE_GROUPS_TABLE_NAME: &str = "resource_groups";
const JOBS_TABLE_NAME: &str = "jobs";
const EXECUTION_MANAGERS_TABLE_NAME: &str = "execution_managers";
const SCHEDULERS_TABLE_NAME: &str = "schedulers";
const SESSIONS_TABLE_NAME: &str = "sessions";
const STORAGE_FENCE_TABLE_NAME: &str = "storage_fence";

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
  `parent_id` BIGINT UNSIGNED DEFAULT NULL,
  `root_id` BIGINT UNSIGNED DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `job_state` (`state`),
  INDEX `job_parent` (`parent_id`),
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
  `resource_group_id` BIGINT UNSIGNED,
  `state` {state_enum} NOT NULL DEFAULT {default_state},
  `last_heartbeat_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `death_confirmed_at` TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `execution_manager_liveness` (`state`, `last_heartbeat_at`),
  CONSTRAINT `execution_manager_resource_group` FOREIGN KEY (`resource_group_id`)
    REFERENCES `{RESOURCE_GROUPS_TABLE_NAME}` (`id`)
    ON UPDATE RESTRICT ON DELETE RESTRICT
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
  `host` VARCHAR(255) NOT NULL,
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

#[must_use]
const fn storage_fence_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{STORAGE_FENCE_TABLE_NAME}` (
  `id` BIGINT UNSIGNED NOT NULL,
  `session_id` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`)
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

/// Runs `query`, a registration's `jobs` insert that carries the fence, on `connection`.
///
/// # Returns
///
/// The ID of the inserted job on success, or `None` if the insert wrote no row because its fence
/// predicate did not hold.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`DbError::CorruptedDbState`] if the insert wrote more than one row.
/// * Forwards [`sqlx::query::Query::execute`]'s return values as mapped by [`map_job_insert_error`]
///   on failure, other than a `NULL` resource group ID.
async fn execute_fenced_job_insert(
    query: Query<'_, MySql, MySqlArguments>,
    connection: &mut MySqlConnection,
    resource_group_id: ResourceGroupId,
) -> Result<Option<JobId>, DbError> {
    match query.execute(connection).await {
        Ok(result) => match result.rows_affected() {
            0 => Ok(None),
            1 => Ok(Some(JobId::from(result.last_insert_id()))),
            rows_affected => Err(DbError::CorruptedDbState(format!(
                "a fenced job insert wrote {rows_affected} rows"
            ))),
        },
        Err(e) if has_mysql_error_number(&e, &[MYSQL_ER_BAD_NULL_ERROR]) => Ok(None),
        Err(e) => Err(map_job_insert_error(e, resource_group_id)),
    }
}

/// Maps `error`, the failure of a `jobs` insert for `resource_group_id`, to a [`DbError`].
///
/// # Returns
///
/// [`DbError::ResourceGroupNotFound`] for a foreign key violation, or `error` as a [`DbError`]
/// otherwise.
fn map_job_insert_error(error: sqlx::Error, resource_group_id: ResourceGroupId) -> DbError {
    if has_mysql_error_number(&error, &[MYSQL_ER_FK_CONSTRAINT]) {
        return DbError::ResourceGroupNotFound(resource_group_id);
    }
    error.into()
}

/// Closes a connection detached from the pool, logging a failure instead of returning it.
async fn close_detached_connection(connection: MySqlConnection) {
    if let Err(e) = connection.close().await {
        tracing::warn!(error = % e, "Failed to close a detached database connection.");
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
/// * Forwards [`sqlx::query::Query::execute`]'s return values on failure, including a lock wait
///   timeout on the last attempt.
/// * Forwards [`sqlx::query::QueryScalar::fetch_one`]'s return values on failure.
async fn bump_fence(pool: &MySqlPool, fence_id: u64, session_id: SessionId) -> Result<(), DbError> {
    const MAX_ATTEMPTS: usize = 5;
    const BLOCKER_LOOKUP_INTERVAL: Duration = Duration::from_secs(1);
    const SET_LOCK_WAIT_TIMEOUT_QUERY: &str = "SET SESSION innodb_lock_wait_timeout = 5;";
    const SELECT_CONNECTION_ID_QUERY: &str = "SELECT CONNECTION_ID();";
    const BUMP_QUERY: &str = formatcp!(
        "UPDATE `{STORAGE_FENCE_TABLE_NAME}` SET `session_id` = GREATEST(`session_id`, ?) WHERE \
         `id` = ?;"
    );

    let mut connection = pool.acquire().await?.detach();
    let result: Result<(), DbError> = async {
        sqlx::query(SET_LOCK_WAIT_TIMEOUT_QUERY)
            .execute(&mut connection)
            .await?;
        let connection_id: u64 = sqlx::query_scalar(SELECT_CONNECTION_ID_QUERY)
            .fetch_one(&mut connection)
            .await?;

        let mut is_blocker_lookup_enabled = true;
        let mut attempt = 1;
        loop {
            let update = sqlx::query(BUMP_QUERY)
                .bind(session_id)
                .bind(fence_id)
                .execute(&mut connection);
            tokio::pin!(update);
            let mut blocker_lookup_ticker = tokio::time::interval_at(
                tokio::time::Instant::now() + BLOCKER_LOOKUP_INTERVAL,
                BLOCKER_LOOKUP_INTERVAL,
            );
            let update_result = loop {
                tokio::select! {
                    update_result = &mut update => break update_result,
                    _ = blocker_lookup_ticker.tick(), if is_blocker_lookup_enabled => {
                        match kill_fence_blockers(pool, connection_id).await {
                            Ok(()) => {}
                            Err(e) if is_access_denied(&e) => {
                                tracing::warn!(
                                    error = % e,
                                    "Cannot look up the connections that block the storage fence. \
                                     Relying on the lock wait timeout."
                                );
                                is_blocker_lookup_enabled = false;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = % e,
                                    "Failed to kill the connections that block the storage fence."
                                );
                            }
                        }
                    }
                }
            };

            match update_result {
                Ok(_) => return Ok(()),
                Err(e) if is_lock_wait_timeout(&e) && attempt < MAX_ATTEMPTS => {
                    tracing::warn!(
                        attempt,
                        "Timed out waiting for the storage fence. Retrying."
                    );
                    attempt += 1;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    .await;
    close_detached_connection(connection).await;
    result
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
        "SELECT DISTINCT `b`.`trx_mysql_thread_id` FROM `information_schema`.`INNODB_LOCK_WAITS` \
         `w` JOIN `information_schema`.`INNODB_TRX` `r` ON `r`.`trx_id` = `w`.`requesting_trx_id` \
         JOIN `information_schema`.`INNODB_TRX` `b` ON `b`.`trx_id` = `w`.`blocking_trx_id` JOIN \
         `information_schema`.`PROCESSLIST` `p` ON `p`.`ID` = `b`.`trx_mysql_thread_id` WHERE \
         `r`.`trx_mysql_thread_id` = ? AND `b`.`trx_mysql_thread_id` <> ? AND `p`.`DB` = \
         DATABASE() AND `b`.`trx_started` <= `r`.`trx_wait_started`;";

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

/// # Returns
///
/// Whether `error` is a duplicate-entry error reported by the database.
fn is_duplicate_entry(error: &sqlx::Error) -> bool {
    has_mysql_error_number(error, &[MYSQL_ER_DUP_ENTRY])
}

/// # Returns
///
/// Whether `error` is a lock wait timeout reported by the database.
fn is_lock_wait_timeout(error: &sqlx::Error) -> bool {
    has_mysql_error_number(error, &[MYSQL_ER_LOCK_WAIT_TIMEOUT])
}

/// # Returns
///
/// Whether `error` is a missing-privilege error reported by the database.
fn is_access_denied(error: &sqlx::Error) -> bool {
    has_mysql_error_number(
        error,
        &[
            MYSQL_ER_SPECIFIC_ACCESS_DENIED,
            MYSQL_ER_TABLE_ACCESS_DENIED,
        ],
    )
}

/// # Returns
///
/// Whether `error` is a database error whose `MySQL` error number is one of `numbers`.
fn has_mysql_error_number(error: &sqlx::Error, numbers: &[u16]) -> bool {
    let sqlx::Error::Database(error) = error else {
        return false;
    };
    error
        .try_downcast_ref::<MySqlDatabaseError>()
        .is_some_and(|mysql_error| numbers.contains(&mysql_error.number()))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use const_format::formatcp;
    use secrecy::SecretString;
    use serial_test::serial;
    use spider_core::types::id::SessionId;
    use sqlx::Connection;
    use sqlx::MySqlConnection;
    use sqlx::Row;
    use sqlx::mysql::MySqlPoolOptions;

    use super::MariaDbStorageConnector;
    use super::SESSION_WAIT_TIMEOUT_SEC;
    use super::STORAGE_FENCE_TABLE_NAME;
    use super::UNPINGED_CONNECTION_MAX_LIFETIME_SEC;
    use super::has_mysql_error_number;
    use crate::config::DatabaseConfig;
    use crate::config::DatabaseCredentials;
    use crate::config::DatabasePoolConfig;
    use crate::db::DbError;

    /// # Returns
    ///
    /// A config of a database that the tests never connect to, with the default pool options.
    fn base_config() -> DatabaseConfig {
        DatabaseConfig {
            host: "db".to_owned(),
            port: 3306,
            name: "spider".to_owned(),
            max_connections: 8,
            fence_id: 0,
            credentials: DatabaseCredentials {
                username: "user".to_owned(),
                password: SecretString::from("pass"),
            },
            pool: DatabasePoolConfig::default(),
        }
    }

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
    fn mariadb_config() -> anyhow::Result<DatabaseConfig> {
        Ok(DatabaseConfig {
            host: "localhost".to_owned(),
            port: std::env::var("MARIADB_PORT")?.parse()?,
            name: std::env::var("MARIADB_DATABASE")?,
            fence_id: rand::random(),
            credentials: DatabaseCredentials::from_env()?,
            ..base_config()
        })
    }

    /// # Returns
    ///
    /// The number of prepared statements that `connection`'s session has closed, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::RawSql::fetch_one`]'s return values on failure.
    /// * Forwards [`sqlx::Row::try_get`]'s return values on failure.
    /// * Forwards [`str::parse`]'s return values on failure.
    async fn num_closed_statements(connection: &mut MySqlConnection) -> anyhow::Result<usize> {
        let row = sqlx::raw_sql("SHOW SESSION STATUS LIKE 'Com_stmt_close';")
            .fetch_one(connection)
            .await?;
        Ok(row.try_get::<String, _>("Value")?.parse()?)
    }

    #[test]
    fn pool_options_apply_the_pool_config() -> anyhow::Result<()> {
        let config = DatabaseConfig {
            max_connections: 8,
            pool: DatabasePoolConfig {
                test_before_acquire: true,
                min_connections: 3,
                ..DatabasePoolConfig::default()
            },
            ..base_config()
        };

        let options = MariaDbStorageConnector::pool_options(&config)?;

        assert_eq!(options.get_max_connections(), config.max_connections);
        assert_eq!(options.get_min_connections(), config.pool.min_connections);
        assert!(options.get_test_before_acquire());
        assert_eq!(
            options.get_max_lifetime(),
            MySqlPoolOptions::new().get_max_lifetime()
        );
        Ok(())
    }

    #[test]
    fn pool_options_cap_the_lifetime_without_the_acquire_ping() -> anyhow::Result<()> {
        let config = DatabaseConfig {
            pool: DatabasePoolConfig {
                test_before_acquire: false,
                ..DatabasePoolConfig::default()
            },
            ..base_config()
        };

        let options = MariaDbStorageConnector::pool_options(&config)?;

        assert!(!options.get_test_before_acquire());
        assert_eq!(
            options.get_max_lifetime(),
            Some(Duration::from_secs(UNPINGED_CONNECTION_MAX_LIFETIME_SEC))
        );
        Ok(())
    }

    #[test]
    fn pool_options_reject_min_connections_above_max_connections() -> anyhow::Result<()> {
        let config_at_max = DatabaseConfig {
            max_connections: 4,
            pool: DatabasePoolConfig {
                min_connections: 4,
                ..DatabasePoolConfig::default()
            },
            ..base_config()
        };
        let config_above_max = DatabaseConfig {
            pool: DatabasePoolConfig {
                min_connections: 5,
                ..config_at_max.pool
            },
            ..config_at_max.clone()
        };

        MariaDbStorageConnector::pool_options(&config_at_max)?;
        let result = MariaDbStorageConnector::pool_options(&config_above_max);
        assert!(
            matches!(result, Err(DbError::InvalidConfig(_))),
            "expected an invalid config error, got {result:?}"
        );
        Ok(())
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

    #[tokio::test]
    #[ignore = "requires MariaDB"]
    #[serial]
    async fn pooled_connections_use_the_statement_cache_capacity() -> anyhow::Result<()> {
        const DISTINCT_QUERIES: [&str; 2] = ["SELECT 1;", "SELECT 2;"];

        let base_config = DatabaseConfig {
            max_connections: 1,
            ..mariadb_config()?
        };
        for (statement_cache_capacity, expected_num_closed_statements) in [
            (1, DISTINCT_QUERIES.len()),
            (DatabasePoolConfig::default().statement_cache_capacity, 0),
        ] {
            let config = DatabaseConfig {
                pool: DatabasePoolConfig {
                    statement_cache_capacity,
                    ..DatabasePoolConfig::default()
                },
                ..base_config.clone()
            };
            let connector = MariaDbStorageConnector::connect(&config).await?;
            let mut connection = connector.pool.acquire().await?;

            let num_closed_statements_before = num_closed_statements(&mut connection).await?;
            for query in DISTINCT_QUERIES {
                sqlx::query(query).execute(&mut *connection).await?;
            }
            let num_closed_statements_after = num_closed_statements(&mut connection).await?;

            assert_eq!(
                num_closed_statements_after - num_closed_statements_before,
                expected_num_closed_statements,
                "{config:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires MariaDB"]
    #[serial]
    async fn the_fence_check_sees_the_bump_despite_an_older_snapshot() -> anyhow::Result<()> {
        const MYSQL_ER_CHECKREAD: u16 = 1020;
        const SELECT_FENCE_QUERY: &str =
            formatcp!("SELECT `session_id` FROM `{STORAGE_FENCE_TABLE_NAME}` WHERE `id` = ?;");

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
                .bind(config.fence_id)
                .fetch_one(&mut connection)
                .await?;
            assert_eq!(snapshot_session_id, older.session_id);

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
