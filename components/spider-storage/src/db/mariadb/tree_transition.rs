#[cfg(test)]
mod fence_tests;

use const_format::formatcp;
use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::io::SerializedTaskOutputs;
use sqlx::MySqlConnection;

use crate::db::DbError;
use crate::db::JobStateUpdate;
use crate::db::TreeTransition;
use crate::db::mariadb::MariaDbStorageConnector;
use crate::db::mariadb::fence::FencePredicate;
use crate::db::mariadb::schema::JOBS_TABLE_NAME;
use crate::db::mariadb::schema::STORAGE_FENCE_TABLE_NAME;

impl MariaDbStorageConnector {
    /// Writes `transition` as
    /// [`apply_tree_transition`](crate::db::InternalJobOrchestration::apply_tree_transition)
    /// specifies.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`UpdateStatement::group`]'s return values on failure.
    /// * Forwards [`Self::apply_single_row_update_in_statement`]'s return values on failure.
    /// * Forwards [`sqlx::Pool::begin`]'s return values on failure.
    /// * Forwards [`Self::check_fence`]'s return values on failure.
    /// * Forwards [`UpdateStatement::execute`]'s return values on failure.
    /// * Forwards [`UpdateStatement::ensure_every_row_matched`]'s return values on failure.
    /// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
    pub(super) async fn write_tree_transition(
        &self,
        transition: TreeTransition,
    ) -> Result<(), DbError> {
        let statements = UpdateStatement::group(
            transition.updates,
            self.max_statement_payload,
            self.max_outputs_size,
        )?;
        if let [statement] = statements.as_slice()
            && statement.rows.len() == 1
        {
            return self.apply_single_row_update_in_statement(statement).await;
        }

        let mut tx = self.pool.begin().await?;
        self.check_fence(&mut tx).await?;
        for statement in &statements {
            let matched_rows = statement.execute(&mut tx, None).await?;
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
    /// * Forwards [`UpdateStatement::execute`]'s return values on failure.
    /// * Forwards [`Self::probe_fence`]'s return values on failure, if the statement matched no
    ///   row.
    /// * Forwards [`UpdateStatement::ensure_every_row_matched`]'s return values on failure, if the
    ///   statement matched no row while the fence holds this connector's session.
    async fn apply_single_row_update_in_statement(
        &self,
        statement: &UpdateStatement,
    ) -> Result<(), DbError> {
        let mut connection = self.pool.acquire().await?;
        let matched_rows = statement
            .execute(&mut connection, Some(self.fence()))
            .await?;
        let Err(guard_miss) = statement.ensure_every_row_matched(matched_rows) else {
            return Ok(());
        };

        self.probe_fence(&mut connection).await?;
        Err(guard_miss)
    }
}

/// The upper bound of the payload bound to one statement.
pub(super) const MAX_STATEMENT_PAYLOAD: u64 = 4 * 1024 * 1024;

/// The bytes of a packet that an `UPDATE` statement's text and its values other than one row's
/// outputs may take.
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
    rows: Vec<UpdateRow>,
}

impl UpdateStatement {
    /// Groups `updates` into guarded statements.
    ///
    /// Updates are grouped by their `from` and `to` states, whether they carry outputs, and their
    /// error message. Groups that carry outputs come first. Each group is cut into statements of
    /// at most [`IN_LIST_MAX_IDS`] rows, and a group that carries outputs is also cut where a
    /// statement's outputs would exceed `max_payload`. Every statement holds at least one row.
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
    /// * [`DbError::OutputsTooLarge`] if any serialized outputs exceed `max_outputs_size`.
    fn group(
        updates: Vec<JobStateUpdate>,
        max_payload: u64,
        max_outputs_size: u64,
    ) -> Result<Vec<Self>, DbError> {
        let mut groups: Vec<Self> = Vec::new();
        for update in updates {
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
            let row = UpdateRow {
                job_id: update.job_id,
                serialized_outputs,
            };
            let has_outputs = row.serialized_outputs.is_some();
            if let Some(group) = groups.iter_mut().find(|group| {
                group.from == update.from
                    && group.to == update.to
                    && group.has_outputs() == has_outputs
                    && group.error_message == update.error_message
            }) {
                group.rows.push(row);
            } else {
                groups.push(Self {
                    from: update.from,
                    to: update.to,
                    error_message: update.error_message,
                    rows: vec![row],
                });
            }
        }
        groups.sort_by_key(|group| !group.has_outputs());

        let mut statements = Vec::new();
        for group in groups {
            let mut rows = group.rows.into_iter().peekable();
            while rows.peek().is_some() {
                let mut chunk = Vec::new();
                let mut payload: u64 = 0;
                while let Some(row) = rows.peek() {
                    let row_payload = row
                        .serialized_outputs
                        .as_ref()
                        .map_or(0, |outputs| outputs.len() as u64);
                    if !chunk.is_empty()
                        && (chunk.len() == IN_LIST_MAX_IDS || payload + row_payload > max_payload)
                    {
                        break;
                    }
                    payload += row_payload;
                    chunk.push(rows.next().expect("a peeked row should exist"));
                }
                statements.push(Self {
                    from: group.from,
                    to: group.to,
                    error_message: group.error_message.clone(),
                    rows: chunk,
                });
            }
        }
        Ok(statements)
    }

    /// Runs the statement on `connection`. With `fence`, the statement also requires that the fence
    /// row holds `fence`'s session, and takes a shared lock on that row.
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
    async fn execute(
        &self,
        connection: &mut MySqlConnection,
        fence: Option<FencePredicate>,
    ) -> Result<u64, DbError> {
        let sql = self.sql(fence.is_some());
        let mut query = sqlx::query(&sql).bind(self.to);
        if self.has_outputs() {
            for row in &self.rows {
                query = query
                    .bind(row.job_id)
                    .bind(row.serialized_outputs.as_deref());
            }
        }
        if let Some(error_message) = &self.error_message {
            query = query.bind(error_message);
        }
        for row in &self.rows {
            query = query.bind(row.job_id);
        }
        query = query.bind(self.from);
        if let Some(fence) = fence {
            query = query.bind(fence.fence_id).bind(fence.session_id);
        }
        Ok(query.execute(connection).await?.rows_affected())
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
        let job_ids: Vec<JobId> = self.rows.iter().map(|row| row.job_id).collect();
        Err(DbError::CorruptedDbState(format!(
            "expected jobs {job_ids:?} in state {from} to move to {to}, but only {matched_rows} \
             of them matched",
            from = self.from,
            to = self.to,
        )))
    }

    /// # Returns
    ///
    /// The statement's SQL text, with the fence predicate if `is_fenced`.
    fn sql(&self, is_fenced: bool) -> String {
        const FENCE_PREDICATE: &str = formatcp!(
            " AND EXISTS (SELECT 1 FROM `{table}` WHERE `id` = ? AND `session_id` = ? LOCK IN \
             SHARE MODE)",
            table = STORAGE_FENCE_TABLE_NAME,
        );
        const UPDATE_PREFIX: &str = formatcp!(
            "UPDATE `{table}` FORCE INDEX (PRIMARY) SET `state` = ?",
            table = JOBS_TABLE_NAME,
        );

        let mut sql = UPDATE_PREFIX.to_owned();
        if self.has_outputs() {
            sql.push_str(", `serialized_job_outputs` = CASE `id`");
            sql.push_str(&" WHEN ? THEN ?".repeat(self.rows.len()));
            sql.push_str(" END");
        }
        if self.error_message.is_some() {
            sql.push_str(", `error_message` = ?");
        }
        if self.to.is_terminal() {
            sql.push_str(", `ended_at` = CURRENT_TIMESTAMP");
        }
        sql.push_str(" WHERE `id` IN (");
        sql.push_str(&vec!["?"; self.rows.len()].join(","));
        sql.push_str(") AND `state` = ?");
        if is_fenced {
            sql.push_str(FENCE_PREDICATE);
        }
        sql.push(';');
        sql
    }

    /// # Returns
    ///
    /// Whether the statement's rows carry outputs.
    fn has_outputs(&self) -> bool {
        self.rows
            .first()
            .is_some_and(|row| row.serialized_outputs.is_some())
    }
}

/// A row of an [`UpdateStatement`].
#[derive(Debug)]
struct UpdateRow {
    job_id: JobId,
    serialized_outputs: Option<Vec<u8>>,
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

    /// # Returns
    ///
    /// The `(from, to, number of rows)` of every statement that `updates` are planned into.
    fn plan_shape(
        updates: Vec<JobStateUpdate>,
        max_payload: u64,
    ) -> Vec<(JobState, JobState, usize)> {
        UpdateStatement::group(updates, max_payload, u64::MAX)
            .expect("planning should succeed")
            .into_iter()
            .map(|statement| (statement.from, statement.to, statement.rows.len()))
            .collect()
    }

    #[test]
    fn updates_are_grouped_by_edge_with_outputs_groups_first() {
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
        assert_eq!(
            plan_shape(updates, 1024),
            vec![
                (JobState::Running, JobState::Succeeded, 1),
                (JobState::Running, JobState::Cancelled, 2),
                (JobState::Running, JobState::Failed, 1),
                (JobState::Running, JobState::Failed, 1),
            ]
        );
    }

    #[test]
    fn a_group_is_cut_into_chunks_of_at_most_the_in_list_limit() {
        let updates = (0..1_200)
            .map(|id| JobStateUpdate::new(JobId::from(id), JobState::Running, JobState::Cancelled))
            .collect();
        assert_eq!(
            plan_shape(updates, 1024),
            vec![
                (JobState::Running, JobState::Cancelled, IN_LIST_MAX_IDS),
                (JobState::Running, JobState::Cancelled, IN_LIST_MAX_IDS),
                (JobState::Running, JobState::Cancelled, 200),
            ]
        );
    }

    #[test]
    fn an_outputs_group_is_cut_where_the_payload_would_exceed_the_limit() {
        let outputs = vec![vec![0u8; 1000]];
        let row_size = SerializedTaskOutputs::serialize_with_size_hint(&outputs)
            .expect("serialization should succeed")
            .to_raw()
            .len() as u64;
        let updates: Vec<_> = (0..5)
            .map(|id| JobStateUpdate {
                outputs: Some(outputs.clone()),
                ..JobStateUpdate::new(JobId::from(id), JobState::Running, JobState::Succeeded)
            })
            .collect();
        assert_eq!(
            plan_shape(updates.clone(), 2 * row_size),
            vec![
                (JobState::Running, JobState::Succeeded, 2),
                (JobState::Running, JobState::Succeeded, 2),
                (JobState::Running, JobState::Succeeded, 1),
            ]
        );
        assert_eq!(
            plan_shape(updates, row_size / 2),
            vec![(JobState::Running, JobState::Succeeded, 1); 5],
            "every statement should carry at least one row"
        );
    }

    #[test]
    fn outputs_over_the_limit_name_their_job() {
        let job_id = JobId::from(3);
        let outputs = vec![vec![1u8, 2, 3]];
        let size = SerializedTaskOutputs::serialize_with_size_hint(&outputs)
            .expect("serialization should succeed")
            .to_raw()
            .len() as u64;
        let updates = || {
            vec![
                JobStateUpdate::new(JobId::from(2), JobState::Running, JobState::Succeeded),
                JobStateUpdate {
                    outputs: Some(outputs.clone()),
                    ..JobStateUpdate::new(job_id, JobState::Running, JobState::Succeeded)
                },
            ]
        };
        let result = UpdateStatement::group(updates(), u64::MAX, size - 1);
        assert!(
            matches!(
                result,
                Err(DbError::OutputsTooLarge { job_id: culprit, size: culprit_size, limit })
                    if culprit == job_id && culprit_size == size && limit == size - 1
            ),
            "unexpected result {result:?}"
        );
        assert!(UpdateStatement::group(updates(), u64::MAX, size).is_ok());
    }

    #[test]
    fn update_texts_carry_the_outputs_and_the_fence_predicate() -> anyhow::Result<()> {
        let statements = UpdateStatement::group(
            vec![
                JobStateUpdate {
                    outputs: Some(vec![vec![1]]),
                    ..JobStateUpdate::new(JobId::from(1), JobState::Running, JobState::Succeeded)
                },
                JobStateUpdate {
                    outputs: Some(vec![vec![2]]),
                    ..JobStateUpdate::new(JobId::from(2), JobState::Running, JobState::Succeeded)
                },
                JobStateUpdate {
                    error_message: Some("e".to_owned()),
                    ..JobStateUpdate::new(JobId::from(3), JobState::Running, JobState::Failed)
                },
            ],
            u64::MAX,
            u64::MAX,
        )?;
        let [with_outputs, with_error_message] = statements.as_slice() else {
            panic!("expected two statements, got {statements:?}");
        };

        assert_eq!(
            with_outputs.sql(false),
            "UPDATE `jobs` FORCE INDEX (PRIMARY) SET `state` = ?, `serialized_job_outputs` = CASE \
             `id` WHEN ? THEN ? WHEN ? THEN ? END, `ended_at` = CURRENT_TIMESTAMP WHERE `id` IN \
             (?,?) AND `state` = ?;"
        );
        assert_eq!(
            with_error_message.sql(true),
            "UPDATE `jobs` FORCE INDEX (PRIMARY) SET `state` = ?, `error_message` = ?, `ended_at` \
             = CURRENT_TIMESTAMP WHERE `id` IN (?) AND `state` = ? AND EXISTS (SELECT 1 FROM \
             `storage_fence` WHERE `id` = ? AND `session_id` = ? LOCK IN SHARE MODE);"
        );
        Ok(())
    }
}
