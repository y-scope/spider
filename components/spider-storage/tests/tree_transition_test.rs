use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::io::TaskOutput;
use spider_storage::db::DbError;
use spider_storage::db::ExternalJobOrchestration;
use spider_storage::db::InternalJobOrchestration;
use spider_storage::db::JobStateUpdate;
use spider_storage::db::MariaDbStorageConnector;
use spider_storage::db::SessionManagement;
use spider_storage::db::TreeTransition;
use sqlx::Row;

use super::mariadb_infra::IsolatedTestDb;
use super::mariadb_infra::create_raw_pool;
use super::mariadb_infra::create_test_resource_group;
use super::task_graph_builder::build_flat_task_graph;
use super::task_graph_builder::create_validated_submission;

/// # Returns
///
/// An update of each of `job_ids` from `from` to `to`, without outputs or an error message.
fn updates(job_ids: &[JobId], from: JobState, to: JobState) -> Vec<JobStateUpdate> {
    job_ids
        .iter()
        .map(|job_id| JobStateUpdate::new(*job_id, from, to))
        .collect()
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn guard_miss_is_corruption_and_rolls_back_every_earlier_statement() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let storage = MariaDbStorageConnector::connect(&config).await?;
    let pool = create_raw_pool(&config).await;
    let resource_group_id = create_test_resource_group(&storage).await;
    let job_ids = register_jobs(&storage, resource_group_id, 3).await;

    storage
        .apply_tree_transition(TreeTransition {
            updates: updates(&job_ids[..2], JobState::Ready, JobState::Running),
        })
        .await?;
    let mut tree_updates = job_ids[..2]
        .iter()
        .map(|job_id| JobStateUpdate {
            outputs: Some(vec![vec![7]]),
            ..JobStateUpdate::new(*job_id, JobState::Running, JobState::Succeeded)
        })
        .collect::<Vec<_>>();
    tree_updates.push(JobStateUpdate::new(
        job_ids[2],
        JobState::Running,
        JobState::Cancelled,
    ));
    let result = storage
        .apply_tree_transition(TreeTransition {
            updates: tree_updates,
        })
        .await;

    assert!(
        matches!(result, Err(DbError::CorruptedDbState(_))),
        "a guard miss should be corruption, got {result:?}"
    );
    let rows = sqlx::query(
        "SELECT `state`, `serialized_job_outputs`, `ended_at` IS NOT NULL AS `has_ended_at` FROM \
         `jobs` WHERE `resource_group_id` = ? ORDER BY `id`;",
    )
    .bind(resource_group_id)
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<JobState, _>("state"))
            .collect::<Vec<_>>(),
        vec![JobState::Running, JobState::Running, JobState::Ready]
    );
    for row in rows {
        assert_eq!(
            row.get::<Option<Vec<u8>>, _>("serialized_job_outputs"),
            None
        );
        assert!(!row.get::<bool, _>("has_ended_at"));
    }
    pool.close().await;
    drop(storage);
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn an_oversized_error_message_rejects_the_entire_transition() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let storage = MariaDbStorageConnector::connect(&config).await?;
    let pool = create_raw_pool(&config).await;
    let resource_group_id = create_test_resource_group(&storage).await;
    let job_ids = register_jobs(&storage, resource_group_id, 2).await;
    let max_allowed_packet: u64 = sqlx::query_scalar("SELECT @@max_allowed_packet;")
        .fetch_one(&pool)
        .await?;
    let size = max_allowed_packet + 1;
    let result = storage
        .apply_tree_transition(TreeTransition {
            updates: vec![
                JobStateUpdate::new(job_ids[0], JobState::Ready, JobState::Running),
                JobStateUpdate {
                    error_message: Some("x".repeat(usize::try_from(size)?)),
                    ..JobStateUpdate::new(job_ids[1], JobState::Ready, JobState::Failed)
                },
            ],
        })
        .await;

    assert!(
        matches!(
            result,
            Err(DbError::ErrorMessageTooLarge { job_id, size: actual_size, limit })
                if job_id == job_ids[1] && actual_size == size && limit < size
        ),
        "an oversized error should reject its job before any write, got {result:?}"
    );
    let rows = sqlx::query(
        "SELECT `state`, `ended_at` IS NOT NULL AS `has_ended_at` FROM `jobs` WHERE \
         `resource_group_id` = ? ORDER BY `id`;",
    )
    .bind(resource_group_id)
    .fetch_all(&pool)
    .await?;
    assert_eq!(rows.len(), job_ids.len());
    for row in rows {
        assert_eq!(row.get::<JobState, _>("state"), JobState::Ready);
        assert!(!row.get::<bool, _>("has_ended_at"));
    }
    pool.close().await;
    drop(storage);
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn chunks_of_one_group_commit_in_one_transaction() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let storage = MariaDbStorageConnector::connect(&config).await?;
    let pool = create_raw_pool(&config).await;
    let resource_group_id = create_test_resource_group(&storage).await;
    let job_ids = register_jobs(&storage, resource_group_id, 502).await;
    let (group, culprit) = job_ids.split_at(501);

    let mut tree_updates = updates(group, JobState::Ready, JobState::Running);
    tree_updates.push(JobStateUpdate::new(
        culprit[0],
        JobState::Running,
        JobState::Cancelled,
    ));
    let result = storage
        .apply_tree_transition(TreeTransition {
            updates: tree_updates,
        })
        .await;
    assert!(
        matches!(result, Err(DbError::CorruptedDbState(_))),
        "the guard miss after two chunks should be corruption, got {result:?}"
    );
    let states: Vec<JobState> = sqlx::query_scalar(
        "SELECT `state` FROM `jobs` WHERE `resource_group_id` = ? ORDER BY `id`;",
    )
    .bind(resource_group_id)
    .fetch_all(&pool)
    .await?;
    assert_eq!(states, vec![JobState::Ready; job_ids.len()]);

    storage
        .apply_tree_transition(TreeTransition {
            updates: updates(group, JobState::Ready, JobState::Running),
        })
        .await?;
    let states: Vec<JobState> = sqlx::query_scalar(
        "SELECT `state` FROM `jobs` WHERE `resource_group_id` = ? ORDER BY `id`;",
    )
    .bind(resource_group_id)
    .fetch_all(&pool)
    .await?;
    let mut expected_states = vec![JobState::Running; group.len()];
    expected_states.push(JobState::Ready);
    assert_eq!(states, expected_states);
    pool.close().await;
    drop(storage);
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn outputs_larger_than_one_statement_payload_are_all_written() -> anyhow::Result<()> {
    use rand::Rng;
    use rand::SeedableRng;
    use spider_core::types::io::SerializedTaskOutputs;

    const OUTPUT_SIZE: usize = 1536 * 1024;

    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let storage = MariaDbStorageConnector::connect(&config).await?;
    let pool = create_raw_pool(&config).await;
    let resource_group_id = create_test_resource_group(&storage).await;
    let job_ids = register_jobs(&storage, resource_group_id, 3).await;
    storage
        .apply_tree_transition(TreeTransition {
            updates: updates(&job_ids, JobState::Ready, JobState::Running),
        })
        .await?;

    let mut rng = rand::rngs::StdRng::seed_from_u64(0);
    let job_outputs: Vec<Vec<TaskOutput>> = job_ids
        .iter()
        .map(|_| {
            let mut output = vec![0; OUTPUT_SIZE];
            rng.fill(output.as_mut_slice());
            vec![output]
        })
        .collect();
    let mut serialized_size = 0;
    for outputs in &job_outputs {
        serialized_size += SerializedTaskOutputs::serialize_with_size_hint(outputs)?
            .to_raw()
            .len() as u64;
    }
    let max_allowed_packet: u64 = sqlx::query_scalar("SELECT @@max_allowed_packet;")
        .fetch_one(&pool)
        .await?;
    let max_statement_payload = (4 * 1024 * 1024).min(max_allowed_packet / 2);
    assert!(
        serialized_size > max_statement_payload,
        "serialized outputs must exceed one statement's payload budget"
    );
    let tree_updates = job_ids
        .iter()
        .zip(&job_outputs)
        .map(|(job_id, outputs)| JobStateUpdate {
            outputs: Some(outputs.clone()),
            ..JobStateUpdate::new(*job_id, JobState::Running, JobState::Succeeded)
        })
        .collect();
    storage
        .apply_tree_transition(TreeTransition {
            updates: tree_updates,
        })
        .await?;

    for (job_id, outputs) in job_ids.iter().zip(job_outputs) {
        assert_eq!(storage.get_outputs(*job_id).await?, outputs);
    }
    pool.close().await;
    drop(storage);
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_fenced_off_connector_writes_nothing() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let storage = MariaDbStorageConnector::connect(&config).await?;
    let pool = create_raw_pool(&config).await;
    let resource_group_id = create_test_resource_group(&storage).await;
    let job_ids = register_jobs(&storage, resource_group_id, 3).await;
    let newer = MariaDbStorageConnector::connect(&config).await?;

    for job_ids_to_start in [&job_ids[..1], &job_ids[1..]] {
        let result = storage
            .apply_tree_transition(TreeTransition {
                updates: updates(job_ids_to_start, JobState::Ready, JobState::Running),
            })
            .await;

        assert!(
            matches!(
                result,
                Err(DbError::Fenced { current, newer: newer_session_id })
                    if current == storage.session_id()
                        && newer_session_id == newer.session_id()
            ),
            "expected Fenced for {} jobs, got {result:?}",
            job_ids_to_start.len()
        );
    }
    for job_id in job_ids {
        assert_eq!(storage.get_state(job_id).await?, JobState::Ready);
    }
    pool.close().await;
    drop(storage);
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn only_terminal_states_set_the_end_timestamp() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let storage = MariaDbStorageConnector::connect(&config).await?;
    let pool = create_raw_pool(&config).await;
    let resource_group_id = create_test_resource_group(&storage).await;
    let job_ids = register_jobs(&storage, resource_group_id, 2).await;
    let (committing, failing) = (job_ids[0], job_ids[1]);

    storage
        .apply_tree_transition(TreeTransition {
            updates: updates(&job_ids, JobState::Ready, JobState::Running),
        })
        .await?;
    storage
        .apply_tree_transition(TreeTransition {
            updates: vec![
                JobStateUpdate {
                    outputs: Some(vec![vec![7]]),
                    ..JobStateUpdate::new(committing, JobState::Running, JobState::CommitReady)
                },
                JobStateUpdate {
                    error_message: Some("failed on purpose".to_owned()),
                    ..JobStateUpdate::new(failing, JobState::Running, JobState::Failed)
                },
            ],
        })
        .await?;
    let ended: Vec<bool> = sqlx::query_scalar(
        "SELECT `ended_at` IS NOT NULL FROM `jobs` WHERE `resource_group_id` = ? ORDER BY `id`;",
    )
    .bind(resource_group_id)
    .fetch_all(&pool)
    .await?;
    assert_eq!(ended, vec![false, true]);
    assert_eq!(storage.get_error(failing).await?, "failed on purpose");

    storage
        .apply_tree_transition(TreeTransition {
            updates: updates(&[committing], JobState::CommitReady, JobState::Succeeded),
        })
        .await?;
    let has_ended_at: bool =
        sqlx::query_scalar("SELECT `ended_at` IS NOT NULL FROM `jobs` WHERE `id` = ?;")
            .bind(committing)
            .fetch_one(&pool)
            .await?;
    assert!(has_ended_at);
    assert_eq!(storage.get_outputs(committing).await?, vec![vec![7u8]]);
    pool.close().await;
    drop(storage);
    isolated_test_db.drop_database().await;
    Ok(())
}

/// Registers `num_jobs` one-task jobs in `resource_group_id`.
///
/// # Returns
///
/// The registered job IDs in registration order.
///
/// # Panics
///
/// Panics if task graph creation or job registration fails.
async fn register_jobs(
    storage: &MariaDbStorageConnector,
    resource_group_id: ResourceGroupId,
    num_jobs: usize,
) -> Vec<JobId> {
    let (graph, inputs) = build_flat_task_graph(1, 16, false, false);
    let submission = create_validated_submission(graph, inputs);
    let mut job_ids = Vec::with_capacity(num_jobs);
    for _ in 0..num_jobs {
        job_ids.push(
            storage
                .register(resource_group_id, &submission)
                .await
                .expect("register should succeed"),
        );
    }
    job_ids
}
