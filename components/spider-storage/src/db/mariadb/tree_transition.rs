use const_format::formatcp;
use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::io::SerializedTaskOutputs;
use sqlx::MySql;
use sqlx::MySqlConnection;
use sqlx::QueryBuilder;

use crate::db::DbError;
use crate::db::JobStateUpdate;
use crate::db::TreeTransition;
use crate::db::mariadb::MariaDbStorageConnector;
use crate::db::mariadb::fence::FencePredicate;
use crate::db::mariadb::schema::JOBS_TABLE_NAME;
use crate::db::mariadb::schema::STORAGE_FENCE_TABLE_NAME;

impl MariaDbStorageConnector {
    /// Atomically writes job state updates guarded by their expected states and this connector's
    /// session fence.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`UpdateStatement::group`]'s return values on failure.
    /// * Forwards [`Self::apply_single_row_update_in_statement`]'s return values on failure.
    /// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
    /// * Forwards [`Self::check_fence`]'s return values on failure.
    /// * Forwards [`Self::execute_update_statement`]'s return values on failure.
    /// * Forwards [`UpdateStatement::ensure_every_row_matched`]'s return values on failure.
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    pub(super) async fn write_tree_transition(
        &self,
        transition: TreeTransition,
    ) -> Result<(), DbError> {
        let statements = UpdateStatement::group(
            transition.updates,
            self.max_statement_payload,
            self.max_row_payload,
        )?;
        if let [statement] = statements.as_slice()
            && statement.rows.len() == 1
        {
            return self.apply_single_row_update_in_statement(statement).await;
        }

        let mut tx = self.pool.begin().await?;
        self.check_fence(&mut tx).await?;
        for statement in &statements {
            let matched_rows = self
                .execute_update_statement(&mut tx, statement, false)
                .await?;
            statement.ensure_every_row_matched(matched_rows)?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Writes `statement`, a guarded update of one row, as one autocommit statement with the fence
    /// in the statement.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::Pool::acquire`]'s return values on failure.
    /// * Forwards [`Self::execute_update_statement`]'s return values on failure.
    /// * Forwards [`Self::probe_fence`]'s return values on failure, if the statement matched no
    ///   row.
    /// * Forwards [`UpdateStatement::ensure_every_row_matched`]'s return values on failure, if the
    ///   statement matched no row while the fence holds this connector's session.
    async fn apply_single_row_update_in_statement(
        &self,
        statement: &UpdateStatement,
    ) -> Result<(), DbError> {
        let mut connection = self.pool.acquire().await?;
        let matched_rows = self
            .execute_update_statement(&mut connection, statement, true)
            .await?;
        let Err(guard_miss) = statement.ensure_every_row_matched(matched_rows) else {
            return Ok(());
        };

        self.probe_fence(&mut connection).await?;
        Err(guard_miss)
    }

    /// Runs `statement` on `connection`. If `is_fence_check_needed`, the statement also requires
    /// that the fence row holds this connector's session, and takes a shared lock on that row.
    ///
    /// # Returns
    ///
    /// The number of rows that the statement matched on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
    async fn execute_update_statement(
        &self,
        connection: &mut MySqlConnection,
        statement: &UpdateStatement,
        is_fence_check_needed: bool,
    ) -> Result<u64, DbError> {
        let mut query_builder = statement.build_query(is_fence_check_needed.then(|| self.fence()));
        Ok(query_builder
            .build()
            .execute(connection)
            .await?
            .rows_affected())
    }
}

/// The maximum batching target for a statement's payload. A single row may exceed this target.
pub(super) const MAX_STATEMENT_PAYLOAD: u64 = 4 * 1024 * 1024;

/// The bytes reserved for an `UPDATE` statement's SQL, IDs, states, and protocol overhead.
pub(super) const MAX_STATEMENT_OVERHEAD: u64 = 64 * 1024;

/// The maximum number of IDs in one statement's `IN` list.
const IN_LIST_MAX_IDS: usize = 500;

/// One guarded `UPDATE` statement of a tree transition: every row moves from the same state to the
/// same state and gets the same error message.
#[derive(Debug)]
struct UpdateStatement {
    from: JobState,
    to: JobState,
    error_message: Option<String>,
    rows: UpdateRows,
}

impl UpdateStatement {
    /// Groups `updates` into guarded statements.
    ///
    /// Updates are grouped by their `from` and `to` states, whether they carry outputs, and their
    /// error message. Groups that carry outputs come first. Each group is cut into statements of
    /// at most [`IN_LIST_MAX_IDS`] rows. Serialized outputs and the shared error message count
    /// toward `max_payload`, capped at `max_row_payload`. A single row may exceed `max_payload`
    /// provided its outputs and error message together fit `max_row_payload`.
    ///
    /// # Returns
    ///
    /// The statements, in the order they must run, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::ValueSerializationFailure`] if any outputs fail to serialize.
    /// * [`DbError::ErrorMessageTooLarge`] if any error message exceeds `max_row_payload`.
    /// * [`DbError::OutputsTooLarge`] if any serialized outputs exceed the space remaining in
    ///   `max_row_payload` after their error message.
    fn group(
        updates: Vec<JobStateUpdate>,
        max_payload: u64,
        max_row_payload: u64,
    ) -> Result<Vec<Self>, DbError> {
        let max_payload = max_payload.min(max_row_payload);
        let mut outputs_groups = Vec::new();
        let mut plain_groups = Vec::new();
        for update in updates {
            let error_size = update
                .error_message
                .as_ref()
                .map_or(0, |message| message.len() as u64);
            if error_size > max_row_payload {
                return Err(DbError::ErrorMessageTooLarge {
                    job_id: update.job_id,
                    size: error_size,
                    limit: max_row_payload,
                });
            }
            let max_outputs_size = max_row_payload - error_size;
            let serialized_outputs = update
                .outputs
                .map(|outputs| {
                    SerializedTaskOutputs::serialize_with_size_hint(&outputs)
                        .map(|serialized| serialized.to_raw())
                        .map_err(|e| DbError::ValueSerializationFailure(Box::new(e)))
                })
                .transpose()?;
            if let Some(size) = serialized_outputs
                .as_ref()
                .map(|outputs| outputs.len() as u64)
                .filter(|size| *size > max_outputs_size)
            {
                return Err(DbError::OutputsTooLarge {
                    job_id: update.job_id,
                    size,
                    limit: max_outputs_size,
                });
            }
            match serialized_outputs {
                Some(serialized_outputs) => UpdateGroup::add(
                    &mut outputs_groups,
                    update.from,
                    update.to,
                    update.error_message,
                    OutputsRow {
                        job_id: update.job_id,
                        serialized_outputs,
                    },
                ),
                None => UpdateGroup::add(
                    &mut plain_groups,
                    update.from,
                    update.to,
                    update.error_message,
                    update.job_id,
                ),
            }
        }

        let mut statements = Vec::new();
        for group in outputs_groups {
            statements.extend(group.into_statements(
                max_payload,
                |row| row.serialized_outputs.len() as u64,
                UpdateRows::WithOutputs,
            ));
        }
        for group in plain_groups {
            statements.extend(group.into_statements(
                max_payload,
                |_| 0,
                UpdateRows::WithoutOutputs,
            ));
        }
        Ok(statements)
    }

    /// Builds the statement's SQL text and binds in one pass. With `fence`, the statement also
    /// requires that the fence row holds `fence`'s session, and takes a shared lock on that row.
    ///
    /// # Returns
    ///
    /// The query builder holding the statement's SQL text and binds.
    fn build_query(&self, fence: Option<FencePredicate>) -> QueryBuilder<'_, MySql> {
        const UPDATE_PREFIX: &str = formatcp!(
            "UPDATE `{table}` FORCE INDEX (PRIMARY) SET `state` = ",
            table = JOBS_TABLE_NAME,
        );
        const FENCE_PREDICATE_PREFIX: &str = formatcp!(
            " AND EXISTS (SELECT 1 FROM `{table}` WHERE `id` = ",
            table = STORAGE_FENCE_TABLE_NAME,
        );

        let mut query_builder = QueryBuilder::new(UPDATE_PREFIX);
        query_builder.push_bind(self.to);
        if let UpdateRows::WithOutputs(rows) = &self.rows {
            query_builder.push(", `serialized_job_outputs` = CASE `id`");
            for row in rows {
                query_builder
                    .push(" WHEN ")
                    .push_bind(row.job_id)
                    .push(" THEN ")
                    .push_bind(row.serialized_outputs.as_slice());
            }
            query_builder.push(" END");
        }
        if let Some(error_message) = &self.error_message {
            query_builder
                .push(", `error_message` = ")
                .push_bind(error_message);
        }
        if self.to.is_terminal() {
            query_builder.push(", `ended_at` = CURRENT_TIMESTAMP");
        }
        query_builder.push(" WHERE `id` IN (");
        let mut job_ids = query_builder.separated(",");
        self.rows.for_each_job_id(|job_id| {
            job_ids.push_bind(job_id);
        });
        query_builder.push(") AND `state` = ").push_bind(self.from);
        if let Some(fence) = fence {
            query_builder
                .push(FENCE_PREDICATE_PREFIX)
                .push_bind(fence.fence_id)
                .push(" AND `session_id` = ")
                .push_bind(fence.session_id)
                .push(" LOCK IN SHARE MODE)");
        }
        query_builder.push(';');
        query_builder
    }

    /// Checks that `matched_rows`, the number of rows that the statement matched, covers every one
    /// of its rows.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::CorruptedDbState`] if fewer rows matched than the statement updates, that is,
    ///   if a row is missing or not in the statement's `from` state.
    fn ensure_every_row_matched(&self, matched_rows: u64) -> Result<(), DbError> {
        if matched_rows == self.rows.len() as u64 {
            return Ok(());
        }
        let mut job_ids = Vec::with_capacity(self.rows.len());
        self.rows.for_each_job_id(|job_id| job_ids.push(job_id));
        Err(DbError::CorruptedDbState(format!(
            "expected jobs {job_ids:?} in state {from} to move to {to}, but only {matched_rows} \
             of them matched",
            from = self.from,
            to = self.to,
        )))
    }
}

/// The rows of an [`UpdateStatement`]: either every row carries outputs or none does.
#[derive(Debug)]
enum UpdateRows {
    WithOutputs(Vec<OutputsRow>),
    WithoutOutputs(Vec<JobId>),
}

impl UpdateRows {
    const fn len(&self) -> usize {
        match self {
            Self::WithOutputs(rows) => rows.len(),
            Self::WithoutOutputs(job_ids) => job_ids.len(),
        }
    }

    /// Calls `func` with the job ID of each row, in row order.
    ///
    /// # Type Parameters
    ///
    /// * `FuncType` - The type of `func`, which visits a job ID.
    fn for_each_job_id<FuncType: FnMut(JobId)>(&self, mut func: FuncType) {
        match self {
            Self::WithOutputs(rows) => {
                for row in rows {
                    func(row.job_id);
                }
            }
            Self::WithoutOutputs(job_ids) => {
                for job_id in job_ids {
                    func(*job_id);
                }
            }
        }
    }
}

/// A row of an [`UpdateStatement`] that carries outputs.
#[derive(Debug)]
struct OutputsRow {
    job_id: JobId,
    serialized_outputs: Vec<u8>,
}

/// The updates of a tree transition that share their `from` and `to` states and their error
/// message, before they are cut into [`UpdateStatement`]s.
///
/// # Type Parameters
///
/// * `RowType` - The type of the group's rows: [`OutputsRow`] for updates that carry outputs, or
///   [`JobId`] for updates that don't.
struct UpdateGroup<RowType> {
    from: JobState,
    to: JobState,
    error_message: Option<String>,
    rows: Vec<RowType>,
}

impl<RowType> UpdateGroup<RowType> {
    /// Adds `row` to the group in `groups` with the given `from`, `to`, and `error_message`,
    /// creating that group at the end of `groups` if none exists.
    fn add(
        groups: &mut Vec<Self>,
        from: JobState,
        to: JobState,
        error_message: Option<String>,
        row: RowType,
    ) {
        // A transition has few distinct groups, so a linear scan is cheap, and it keeps the
        // groups in the order their first update arrived.
        if let Some(group) = groups.iter_mut().find(|group| {
            group.from == from && group.to == to && group.error_message == error_message
        }) {
            group.rows.push(row);
        } else {
            groups.push(Self {
                from,
                to,
                error_message,
                rows: vec![row],
            });
        }
    }

    /// Cuts the group into statements of at most [`IN_LIST_MAX_IDS`] rows. A statement's payload
    /// is the group's error message plus each row's `row_payload`, and a statement takes no
    /// further row once that row would push its payload past `max_payload`, but always takes its
    /// first row.
    ///
    /// # Type Parameters
    ///
    /// * `RowPayloadType` - The type of `row_payload`, which returns a row's payload size.
    /// * `IntoRowsType` - The type of `into_rows`, which wraps a statement's rows into
    ///   [`UpdateRows`].
    ///
    /// # Returns
    ///
    /// The statements, in row order, with each statement's rows wrapped by `into_rows`.
    fn into_statements<
        RowPayloadType: Fn(&RowType) -> u64,
        IntoRowsType: Fn(Vec<RowType>) -> UpdateRows,
    >(
        self,
        max_payload: u64,
        row_payload: RowPayloadType,
        into_rows: IntoRowsType,
    ) -> Vec<UpdateStatement> {
        let base_payload = self
            .error_message
            .as_ref()
            .map_or(0, |message| message.len() as u64);
        let mut statements = Vec::new();
        let mut rows = self.rows.into_iter().peekable();
        while rows.peek().is_some() {
            let mut chunk = Vec::new();
            let mut payload = base_payload;
            while let Some(row) = rows.peek() {
                let row_payload = row_payload(row);
                if !chunk.is_empty()
                    && (chunk.len() == IN_LIST_MAX_IDS || payload + row_payload > max_payload)
                {
                    break;
                }
                payload += row_payload;
                chunk.push(rows.next().expect("a peeked row should exist"));
            }
            statements.push(UpdateStatement {
                from: self.from,
                to: self.to,
                error_message: self.error_message.clone(),
                rows: into_rows(chunk),
            });
        }
        statements
    }
}

#[cfg(test)]
mod tests {
    use spider_core::job::JobState;
    use spider_core::types::id::JobId;
    use spider_core::types::io::SerializedTaskOutputs;

    use super::IN_LIST_MAX_IDS;
    use super::UpdateStatement;
    use crate::db::DbError;
    use crate::db::JobStateUpdate;
    use crate::db::mariadb::fence::FencePredicate;

    #[test]
    fn updates_are_grouped_by_edge_with_outputs_groups_first() -> anyhow::Result<()> {
        let updates = vec![
            JobStateUpdate::new(JobId::from(1), JobState::Running, JobState::Cancelled),
            JobStateUpdate {
                outputs: Some(vec![vec![1]]),
                ..JobStateUpdate::new(JobId::from(2), JobState::Running, JobState::Succeeded)
            },
            JobStateUpdate::new(JobId::from(3), JobState::Running, JobState::Cancelled),
            JobStateUpdate {
                error_message: Some("a".to_owned()),
                ..JobStateUpdate::new(JobId::from(4), JobState::Running, JobState::Failed)
            },
            JobStateUpdate {
                error_message: Some("b".to_owned()),
                ..JobStateUpdate::new(JobId::from(5), JobState::Running, JobState::Failed)
            },
        ];
        let statements = UpdateStatement::group(updates, 1024, u64::MAX)?;
        assert_eq!(
            statements
                .iter()
                .map(|statement| (statement.from, statement.to, statement.rows.len()))
                .collect::<Vec<_>>(),
            vec![
                (JobState::Running, JobState::Succeeded, 1),
                (JobState::Running, JobState::Cancelled, 2),
                (JobState::Running, JobState::Failed, 1),
                (JobState::Running, JobState::Failed, 1),
            ]
        );
        for statement in &statements {
            for fence in [
                None,
                Some(FencePredicate {
                    fence_id: 0,
                    session_id: 0,
                }),
            ] {
                assert!(
                    statement
                        .build_query(fence)
                        .sql()
                        .contains("FORCE INDEX (PRIMARY)")
                );
            }
        }
        for count in [IN_LIST_MAX_IDS, IN_LIST_MAX_IDS + 1] {
            let updates = (0..count)
                .map(|id| {
                    JobStateUpdate::new(
                        JobId::from(id as u64),
                        JobState::Running,
                        JobState::Cancelled,
                    )
                })
                .collect();
            let statements = UpdateStatement::group(updates, 1024, u64::MAX)?;
            let mut expected = vec![IN_LIST_MAX_IDS];
            if count > IN_LIST_MAX_IDS {
                expected.push(1);
            }
            assert_eq!(
                statements
                    .iter()
                    .map(|statement| statement.rows.len())
                    .collect::<Vec<_>>(),
                expected
            );
        }
        Ok(())
    }

    #[test]
    fn shared_error_message_counts_once_per_statement() -> anyhow::Result<()> {
        let outputs = vec![vec![1, 2, 3]];
        let output_size = SerializedTaskOutputs::serialize_with_size_hint(&outputs)?
            .to_raw()
            .len() as u64;
        for error_message in [None, Some("é".repeat(10))] {
            let error_size = error_message
                .as_ref()
                .map_or(0, |message| message.len() as u64);
            let two_row_payload = error_size + 2 * output_size;
            let updates: Vec<_> = (0..3)
                .map(|id| JobStateUpdate {
                    outputs: Some(outputs.clone()),
                    error_message: error_message.clone(),
                    ..JobStateUpdate::new(JobId::from(id), JobState::Running, JobState::Failed)
                })
                .collect();
            for (target, hard_limit) in [(two_row_payload, u64::MAX), (u64::MAX, two_row_payload)] {
                let statements = UpdateStatement::group(updates.clone(), target, hard_limit)?;
                assert_eq!(
                    statements
                        .iter()
                        .map(|statement| statement.rows.len())
                        .collect::<Vec<_>>(),
                    vec![2, 1]
                );
            }
            let statements = UpdateStatement::group(updates, 0, error_size + output_size)?;
            assert_eq!(
                statements
                    .iter()
                    .map(|statement| statement.rows.len())
                    .collect::<Vec<_>>(),
                vec![1, 1, 1],
                "a row that fits the hard limit may exceed the batching target"
            );
        }
        Ok(())
    }

    #[test]
    fn error_messages_and_outputs_share_the_hard_payload_limit() -> anyhow::Result<()> {
        let job_id = JobId::from(3);
        let error_message = "é".to_owned();
        let error_size = error_message.len() as u64;
        let mut update = JobStateUpdate {
            error_message: Some(error_message),
            ..JobStateUpdate::new(job_id, JobState::Running, JobState::Failed)
        };
        let result = UpdateStatement::group(vec![update.clone()], u64::MAX, error_size - 1);
        assert!(
            matches!(
                result,
                Err(DbError::ErrorMessageTooLarge { job_id: culprit, size, limit })
                    if culprit == job_id && size == error_size && limit == error_size - 1
            ),
            "unexpected result {result:?}"
        );
        UpdateStatement::group(vec![update.clone()], 0, error_size)?;

        let outputs = vec![vec![1, 2, 3]];
        let output_size = SerializedTaskOutputs::serialize_with_size_hint(&outputs)?
            .to_raw()
            .len() as u64;
        update.outputs = Some(outputs);
        for error_size in [error_size, 0] {
            if error_size == 0 {
                update.error_message = None;
            }
            let combined_size = error_size + output_size;
            let updates = vec![
                JobStateUpdate::new(JobId::from(2), JobState::Ready, JobState::Running),
                update.clone(),
            ];
            let result = UpdateStatement::group(updates.clone(), u64::MAX, combined_size - 1);
            assert!(
                matches!(
                    result,
                    Err(DbError::OutputsTooLarge { job_id: culprit, size, limit })
                        if culprit == job_id && size == output_size && limit == output_size - 1
                ),
                "unexpected result {result:?}"
            );
            UpdateStatement::group(updates, 0, combined_size)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod fence_tests {
    //! Checks in-statement fencing against concurrent fence bumps and older transaction snapshots.

    use std::time::Duration;

    use spider_core::job::JobState;
    use spider_core::task::ExecutionPolicy;
    use spider_core::task::TaskDescriptor;
    use spider_core::task::TaskGraph;
    use spider_core::task::TdlContext;
    use spider_core::types::id::SessionId;
    use spider_core::types::io::TaskGraphInputBuilder;
    use sqlx::Acquire;
    use sqlx::MySql;
    use sqlx::MySqlConnection;
    use sqlx::MySqlPool;
    use sqlx::Transaction;

    use super::UpdateStatement;
    use crate::DatabaseConfig;
    use crate::db::DbError;
    use crate::db::ExternalJobOrchestration;
    use crate::db::InternalJobOrchestration;
    use crate::db::JobStateUpdate;
    use crate::db::MariaDbStorageConnector;
    use crate::db::SessionManagement;
    use crate::db::TreeTransition;
    use crate::job_submission::ValidatedJobSubmission;
    use crate::job_submission::create_validated_submission;
    use crate::mariadb_test_infra::IsolatedTestDb;
    use crate::mariadb_test_infra::create_raw_pool;
    use crate::mariadb_test_infra::create_test_resource_group;
    use crate::mariadb_test_infra::has_mysql_error_number;
    use crate::mariadb_test_infra::test_fence_id;
    use crate::mariadb_test_infra::wait_for_queued_fence_bump;

    /// A write issued by a superseded session from an older transaction snapshot.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum StaleWrite {
        Register,
        Start,
        SucceedWithOutputs,
    }

    #[tokio::test]
    #[ignore = "requires MariaDB"]
    async fn in_statement_updates_hold_the_fence_until_commit() -> anyhow::Result<()> {
        let job_submission = flat_submission();
        let isolated_test_db = IsolatedTestDb::create().await;
        let config = isolated_test_db.config();
        let raw_pool = create_raw_pool(&config).await;

        for isolation_level in ["REPEATABLE READ", "READ COMMITTED"] {
            for has_outputs in [false, true] {
                let holder = connect(&config).await;
                let rg_id = create_test_resource_group(&holder).await;
                let job_id = holder.register(rg_id, &job_submission).await?;
                let update = if has_outputs {
                    holder
                        .apply_tree_transition(TreeTransition {
                            updates: vec![JobStateUpdate::new(
                                job_id,
                                JobState::Ready,
                                JobState::Running,
                            )],
                        })
                        .await?;
                    JobStateUpdate {
                        outputs: Some(vec![b"outputs".to_vec()]),
                        ..JobStateUpdate::new(job_id, JobState::Running, JobState::Succeeded)
                    }
                } else {
                    JobStateUpdate::new(job_id, JobState::Ready, JobState::Running)
                };
                let mut raw_connection = raw_pool.acquire().await?;
                let mut raw_tx =
                    begin_at_isolation_level(&mut raw_connection, isolation_level).await?;
                let matched_rows = holder
                    .execute_update_statement(&mut raw_tx, &single_row_statement(update), true)
                    .await?;
                assert_eq!(
                    matched_rows, 1,
                    "outputs={has_outputs} at {isolation_level}"
                );
                commit_with_a_queued_fence_bump(&raw_pool, &config, isolation_level, raw_tx)
                    .await?;
            }
        }

        raw_pool.close().await;
        isolated_test_db.drop_database().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires MariaDB"]
    async fn in_statement_writes_after_the_bump_are_fenced_despite_an_older_snapshot()
    -> anyhow::Result<()> {
        let isolated_test_db = IsolatedTestDb::create().await;
        let config = isolated_test_db.config();
        let raw_pool = create_raw_pool(&config).await;
        let job_submission = flat_submission();

        for is_snapshot_isolation_on in [false, true] {
            for write in [
                StaleWrite::Register,
                StaleWrite::Start,
                StaleWrite::SucceedWithOutputs,
            ] {
                let holder = connect(&config).await;
                let rg_id = create_test_resource_group(&holder).await;
                let job_id = holder.register(rg_id, &job_submission).await?;
                let initial_state = if write == StaleWrite::SucceedWithOutputs {
                    holder
                        .apply_tree_transition(TreeTransition {
                            updates: vec![JobStateUpdate::new(
                                job_id,
                                JobState::Ready,
                                JobState::Running,
                            )],
                        })
                        .await?;
                    JobState::Running
                } else {
                    JobState::Ready
                };
                let initial_job_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM `jobs`;")
                    .fetch_one(&raw_pool)
                    .await?;
                let mut raw_connection = raw_pool.acquire().await?;
                begin_snapshot_and_read_the_fence(
                    &mut raw_connection,
                    is_snapshot_isolation_on,
                    &holder,
                )
                .await?;
                let newer = connect(&config).await;

                let result = match write {
                    StaleWrite::Register => holder
                        .fenced_register_query(rg_id, &job_submission)
                        .execute(&mut *raw_connection)
                        .await
                        .map(|result| result.rows_affected())
                        .map_err(DbError::from),
                    StaleWrite::Start => {
                        holder
                            .execute_update_statement(
                                &mut raw_connection,
                                &single_row_statement(JobStateUpdate::new(
                                    job_id,
                                    JobState::Ready,
                                    JobState::Running,
                                )),
                                true,
                            )
                            .await
                    }
                    StaleWrite::SucceedWithOutputs => {
                        holder
                            .execute_update_statement(
                                &mut raw_connection,
                                &single_row_statement(JobStateUpdate {
                                    outputs: Some(vec![b"outputs".to_vec()]),
                                    ..JobStateUpdate::new(
                                        job_id,
                                        JobState::Running,
                                        JobState::Succeeded,
                                    )
                                }),
                                true,
                            )
                            .await
                    }
                };
                sqlx::raw_sql("ROLLBACK;")
                    .execute(&mut *raw_connection)
                    .await?;
                assert_stale_write_wrote_nothing(&result, write, is_snapshot_isolation_on);
                let job_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM `jobs`;")
                    .fetch_one(&raw_pool)
                    .await?;
                assert_eq!(
                    job_count, initial_job_count,
                    "a stale write should add no job"
                );
                assert_eq!(newer.get_state(job_id).await?, initial_state);
                let has_outputs: bool = sqlx::query_scalar(
                    "SELECT `serialized_job_outputs` IS NOT NULL FROM `jobs` WHERE `id` = ?;",
                )
                .bind(job_id)
                .fetch_one(&raw_pool)
                .await?;
                assert!(!has_outputs, "a stale success should write no outputs");
            }
        }

        raw_pool.close().await;
        isolated_test_db.drop_database().await;
        Ok(())
    }

    /// # Returns
    ///
    /// A statement containing `update`.
    ///
    /// # Panics
    ///
    /// Panics if the update cannot be serialized or grouped into a statement.
    fn single_row_statement(update: JobStateUpdate) -> UpdateStatement {
        UpdateStatement::group(vec![update], u64::MAX, u64::MAX)
            .expect("the update should serialize")
            .pop()
            .expect("one update should produce a statement")
    }

    /// # Returns
    ///
    /// A connector connected to `config`.
    ///
    /// # Panics
    ///
    /// Panics if the connection fails.
    async fn connect(config: &DatabaseConfig) -> MariaDbStorageConnector {
        MariaDbStorageConnector::connect(config)
            .await
            .expect("connect should succeed")
    }

    /// # Returns
    ///
    /// A validated submission containing one task.
    ///
    /// # Panics
    ///
    /// Panics if the task graph cannot be created, populated, or validated.
    fn flat_submission() -> ValidatedJobSubmission {
        let mut task_graph =
            TaskGraph::new(None, None).expect("task graph creation should succeed");
        task_graph
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: "test_pkg".to_owned(),
                    task_func: "test_fn".to_owned(),
                },
                execution_policy: Some(ExecutionPolicy::default()),
                inputs: vec![],
                outputs: vec![],
                input_sources: None,
            })
            .expect("task insertion should succeed");
        create_validated_submission(task_graph, TaskGraphInputBuilder::new().build())
    }

    /// Starts a newer connector with `config`, checks that its fence bump queues behind `raw_tx`, a
    /// transaction at `isolation_level`, and does not finish, then commits `raw_tx` and waits for
    /// the newer connector.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`tokio::task::JoinError`] if the task running the newer connector's `connect` panics or
    ///   is cancelled.
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    /// * Forwards [`tokio::time::timeout`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if the fence bump does not queue within 5 seconds, or if it finishes before `raw_tx`
    /// commits.
    async fn commit_with_a_queued_fence_bump(
        raw_pool: &MySqlPool,
        config: &DatabaseConfig,
        isolation_level: &str,
        raw_tx: Transaction<'_, MySql>,
    ) -> anyhow::Result<()> {
        let connect_config = config.clone();
        let newer_connect_join_handle = tokio::spawn(async move { connect(&connect_config).await });
        tokio::time::timeout(
            Duration::from_secs(5),
            wait_for_queued_fence_bump(raw_pool, test_fence_id(config)),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "connect should queue its fence bump behind the open transaction at \
                 {isolation_level} with {config:?}"
            )
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !newer_connect_join_handle.is_finished(),
            "connect should wait for the open transaction at {isolation_level} with {config:?}"
        );
        raw_tx.commit().await?;
        tokio::time::timeout(Duration::from_secs(5), newer_connect_join_handle).await??;
        Ok(())
    }

    /// Begins a transaction at `isolation_level` on `connection`.
    ///
    /// # Type Parameters
    ///
    /// * `'connection` - The lifetime of the connection borrowed by the transaction.
    ///
    /// # Returns
    ///
    /// The transaction on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::RawSql::execute`]'s return values on failure.
    /// * Forwards [`sqlx::Acquire::begin`]'s return values on failure.
    async fn begin_at_isolation_level<'connection>(
        connection: &'connection mut MySqlConnection,
        isolation_level: &str,
    ) -> Result<Transaction<'connection, MySql>, sqlx::Error> {
        sqlx::raw_sql(&format!(
            "SET TRANSACTION ISOLATION LEVEL {isolation_level};"
        ))
        .execute(&mut *connection)
        .await?;
        connection.begin().await
    }

    /// Sets `innodb_snapshot_isolation` on `connection`, starts a transaction with a consistent
    /// snapshot on it, and reads `holder`'s fence row in that snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::RawSql::execute`]'s return values on failure.
    /// * Forwards [`sqlx::query::QueryScalar::fetch_one`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if the variable does not take the value, or if the snapshot's fence row does not hold
    /// `holder`'s session.
    async fn begin_snapshot_and_read_the_fence(
        connection: &mut MySqlConnection,
        is_snapshot_isolation_on: bool,
        holder: &MariaDbStorageConnector,
    ) -> Result<(), sqlx::Error> {
        sqlx::raw_sql(&format!(
            "SET SESSION innodb_snapshot_isolation = {};",
            if is_snapshot_isolation_on {
                "ON"
            } else {
                "OFF"
            }
        ))
        .execute(&mut *connection)
        .await?;
        let snapshot_isolation: i64 = sqlx::query_scalar("SELECT @@innodb_snapshot_isolation;")
            .fetch_one(&mut *connection)
            .await?;
        assert_eq!(snapshot_isolation == 1, is_snapshot_isolation_on);
        sqlx::raw_sql("START TRANSACTION WITH CONSISTENT SNAPSHOT;")
            .execute(&mut *connection)
            .await?;
        let seen_session_id: SessionId =
            sqlx::query_scalar("SELECT `session_id` FROM `storage_fence` WHERE `id` = ?;")
                .bind(holder.fence_id)
                .fetch_one(&mut *connection)
                .await?;
        assert_eq!(seen_session_id, holder.session_id());
        Ok(())
    }

    /// Checks that a stale `write` from an older snapshot wrote nothing: with snapshot isolation it
    /// fails with errno 1020; without it, the register fails with errno 1048 and the other writes
    /// match no row.
    ///
    /// # Panics
    ///
    /// Panics if `result` is not the expected outcome.
    fn assert_stale_write_wrote_nothing(
        result: &Result<u64, DbError>,
        write: StaleWrite,
        is_snapshot_isolation_on: bool,
    ) {
        const MYSQL_ER_CHECKREAD: u16 = 1020;
        const MYSQL_ER_BAD_NULL_ERROR: u16 = 1048;

        let expected_error_number = if is_snapshot_isolation_on {
            Some(MYSQL_ER_CHECKREAD)
        } else if write == StaleWrite::Register {
            Some(MYSQL_ER_BAD_NULL_ERROR)
        } else {
            None
        };
        match (result, expected_error_number) {
            (Ok(result), None) => {
                assert_eq!(*result, 0, "{write:?}");
            }
            (Err(DbError::Sql(e)), Some(expected_error_number))
                if has_mysql_error_number(e, expected_error_number) => {}
            (result, expected_error_number) => panic!(
                "{write:?} with snapshot isolation {is_snapshot_isolation_on}: expected error \
                 number {expected_error_number:?}, got {result:?}"
            ),
        }
    }
}
