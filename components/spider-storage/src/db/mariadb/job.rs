use async_trait::async_trait;
use const_format::formatcp;
use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::io::SerializedTaskOutputs;
use spider_core::types::io::TaskOutput;
use sqlx::MySql;
use sqlx::MySqlConnection;
use sqlx::mysql::MySqlArguments;
use sqlx::query::Query;

use crate::db::DbError;
use crate::db::ExternalJobOrchestration;
use crate::db::InternalJobOrchestration;
use crate::db::RecoverableJobContext;
use crate::db::TreeTransition;
use crate::db::error::ExpectedStates;
use crate::db::mariadb::MariaDbStorageConnector;
use crate::db::mariadb::mysql_error::MYSQL_ER_BAD_NULL_ERROR;
use crate::db::mariadb::mysql_error::MYSQL_ER_FK_CONSTRAINT;
use crate::db::mariadb::mysql_error::has_mysql_error_number;
use crate::db::mariadb::schema::JOBS_TABLE_NAME;
use crate::db::mariadb::schema::STORAGE_FENCE_TABLE_NAME;
use crate::job_submission::ValidatedCompressedJobSubmission;
use crate::job_submission::ValidatedJobSubmission;

impl MariaDbStorageConnector {
    /// Builds the `jobs` insert of a registration with the fence in the statement. A fenced insert
    /// fails with [`MYSQL_ER_BAD_NULL_ERROR`].
    ///
    /// # Type Parameters
    ///
    /// * `'submission_lifetime` - The lifetime of `compressed_job_submission`, which the returned
    ///   insert borrows.
    ///
    /// # Returns
    ///
    /// The insert, bound to register `compressed_job_submission` in `resource_group_id` while the
    /// fence row holds this connector's session.
    pub(super) fn fenced_register_query<'submission_lifetime>(
        &self,
        resource_group_id: ResourceGroupId,
        compressed_job_submission: &'submission_lifetime ValidatedCompressedJobSubmission,
    ) -> Query<'submission_lifetime, MySql, MySqlArguments> {
        const QUERY: &str = formatcp!(
            "INSERT INTO `{jobs_table}` (`resource_group_id`, `compressed_serialized_task_graph`, \
             `compressed_serialized_job_inputs`) VALUES ((SELECT ? FROM `{fence_table}` WHERE \
             `id` = ? AND `session_id` = ? LOCK IN SHARE MODE), ?, ?);",
            jobs_table = JOBS_TABLE_NAME,
            fence_table = STORAGE_FENCE_TABLE_NAME,
        );

        sqlx::query(QUERY)
            .bind(resource_group_id)
            .bind(self.fence_id)
            .bind(self.session_id)
            .bind(compressed_job_submission.compressed_serialized_task_graph())
            .bind(compressed_job_submission.compressed_serialized_job_inputs())
    }

    /// Registers `compressed_job_submission` in `resource_group_id` with the fence in the `jobs`
    /// insert.
    ///
    /// # Returns
    ///
    /// The ID of the inserted job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::CorruptedDbState`] if the insert is fenced off while the fence still holds this
    ///   connector's session.
    /// * [`DbError::ResourceGroupNotFound`] if `resource_group_id` does not exist.
    /// * Forwards [`Self::probe_fence`]'s return values on failure.
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    async fn insert_job_with_fence(
        &self,
        connection: &mut MySqlConnection,
        resource_group_id: ResourceGroupId,
        compressed_job_submission: &ValidatedCompressedJobSubmission,
    ) -> Result<JobId, DbError> {
        match self
            .fenced_register_query(resource_group_id, compressed_job_submission)
            .execute(&mut *connection)
            .await
        {
            Ok(result) => Ok(JobId::from(result.last_insert_id())),
            Err(e) if has_mysql_error_number(&e, &[MYSQL_ER_BAD_NULL_ERROR]) => {
                self.probe_fence(connection).await?;
                Err(DbError::CorruptedDbState(
                    "fenced insert wrote no row while the fence holds this session".to_owned(),
                ))
            }
            Err(e) if has_mysql_error_number(&e, &[MYSQL_ER_FK_CONSTRAINT]) => {
                Err(DbError::ResourceGroupNotFound(resource_group_id))
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait]
impl ExternalJobOrchestration for MariaDbStorageConnector {
    async fn register(
        &self,
        resource_group_id: ResourceGroupId,
        compressed_job_submission: &ValidatedCompressedJobSubmission,
    ) -> Result<JobId, DbError> {
        let mut connection = self.pool.acquire().await?;
        self.insert_job_with_fence(
            &mut connection,
            resource_group_id,
            compressed_job_submission,
        )
        .await
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
    async fn apply_tree_transition(&self, transition: TreeTransition) -> Result<(), DbError> {
        self.write_tree_transition(transition).await
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
