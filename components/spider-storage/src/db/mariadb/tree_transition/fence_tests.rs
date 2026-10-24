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
            let mut raw_tx = begin_at_isolation_level(&mut raw_connection, isolation_level).await?;
            let matched_rows = single_row_statement(update)
                .execute(&mut raw_tx, Some(holder.fence()))
                .await?;
            assert_eq!(
                matched_rows, 1,
                "outputs={has_outputs} at {isolation_level}"
            );
            commit_with_a_queued_fence_bump(&raw_pool, &config, isolation_level, raw_tx).await?;
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
                    single_row_statement(JobStateUpdate::new(
                        job_id,
                        JobState::Ready,
                        JobState::Running,
                    ))
                    .execute(&mut raw_connection, Some(holder.fence()))
                    .await
                }
                StaleWrite::SucceedWithOutputs => {
                    single_row_statement(JobStateUpdate {
                        outputs: Some(vec![b"outputs".to_vec()]),
                        ..JobStateUpdate::new(job_id, JobState::Running, JobState::Succeeded)
                    })
                    .execute(&mut raw_connection, Some(holder.fence()))
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
    let mut task_graph = TaskGraph::new(None, None).expect("task graph creation should succeed");
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
/// transaction at `isolation_level`, and does not finish, then commits `raw_tx` and waits for the
/// newer connector.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`tokio::task::JoinError`] if the task running the newer connector's `connect` panics or is
///   cancelled.
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
            "connect should queue its fence bump behind the open transaction at {isolation_level} \
             with {config:?}"
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
/// fails with errno 1020; without it, the register fails with errno 1048 and the other writes match
/// no row.
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
            "{write:?} with snapshot isolation {is_snapshot_isolation_on}: expected error number \
             {expected_error_number:?}, got {result:?}"
        ),
    }
}
