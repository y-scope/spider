use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::io::TaskOutput;
use spider_storage::db::ChildJobInsert;
use spider_storage::db::DbError;
use spider_storage::db::ExternalJobOrchestration;
use spider_storage::db::InternalJobOrchestration;
use spider_storage::db::JobStateUpdate;
use spider_storage::db::MariaDbStorageConnector;
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
    const NUM_OUTPUT_JOBS: usize = 501;

    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let storage = MariaDbStorageConnector::connect(&config).await?;
    let pool = create_raw_pool(&config).await;
    let resource_group_id = create_test_resource_group(&storage).await;
    let job_ids = register_jobs(&storage, resource_group_id, NUM_OUTPUT_JOBS + 2).await;

    storage
        .apply_tree_transition(TreeTransition {
            updates: updates(
                &job_ids[..=NUM_OUTPUT_JOBS],
                JobState::Ready,
                JobState::Running,
            ),
            child_inserts: Vec::new(),
        })
        .await?;
    let mut tree_updates = job_ids[..NUM_OUTPUT_JOBS]
        .iter()
        .map(|job_id| JobStateUpdate {
            outputs: Some(vec![vec![7]]),
            ..JobStateUpdate::new(*job_id, JobState::Running, JobState::Succeeded)
        })
        .collect::<Vec<_>>();
    tree_updates.extend(updates(
        &job_ids[NUM_OUTPUT_JOBS..],
        JobState::Running,
        JobState::Cancelled,
    ));
    let result = storage
        .apply_tree_transition(TreeTransition {
            updates: tree_updates,
            child_inserts: Vec::new(),
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
    let mut expected_states = vec![JobState::Running; NUM_OUTPUT_JOBS + 1];
    expected_states.push(JobState::Ready);
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<JobState, _>("state"))
            .collect::<Vec<_>>(),
        expected_states
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
            child_inserts: Vec::new(),
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
            child_inserts: Vec::new(),
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
            child_inserts: Vec::new(),
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
            child_inserts: Vec::new(),
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
            child_inserts: Vec::new(),
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

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn child_inserts_return_their_ids_in_order_across_chunks() -> anyhow::Result<()> {
    const NUM_CHILDREN: usize = 1_200;

    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let storage = MariaDbStorageConnector::connect(&config).await?;
    let pool = create_raw_pool(&config).await;
    let resource_group_id = create_test_resource_group(&storage).await;
    let parent_id = register_jobs(&storage, resource_group_id, 1).await[0];
    storage
        .apply_tree_transition(TreeTransition {
            updates: updates(&[parent_id], JobState::Ready, JobState::Running),
            child_inserts: Vec::new(),
        })
        .await?;
    let task_graphs: Vec<Vec<u8>> = (0..NUM_CHILDREN)
        .map(|index| index.to_le_bytes().to_vec())
        .collect();

    let child_ids = storage
        .apply_tree_transition(TreeTransition {
            updates: updates(
                &[parent_id],
                JobState::Running,
                JobState::BlockingOnChildren,
            ),
            child_inserts: task_graphs
                .iter()
                .map(|task_graph| ChildJobInsert {
                    parent_id,
                    root_id: parent_id,
                    resource_group_id,
                    compressed_serialized_task_graph: task_graph,
                    compressed_serialized_job_inputs: &[],
                })
                .collect(),
        })
        .await?;

    let rows = sqlx::query(
        "SELECT `id`, `compressed_serialized_task_graph`, `parent_id`, `root_id`, `state` FROM \
         `jobs` WHERE `id` <> ? ORDER BY `id`;",
    )
    .bind(parent_id)
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<JobId, _>("id"))
            .collect::<Vec<_>>(),
        child_ids
    );
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<Vec<u8>, _>("compressed_serialized_task_graph"))
            .collect::<Vec<_>>(),
        task_graphs
    );
    for row in rows {
        assert_eq!(
            (
                row.get::<Option<JobId>, _>("parent_id"),
                row.get::<Option<JobId>, _>("root_id"),
                row.get::<JobState, _>("state"),
            ),
            (Some(parent_id), Some(parent_id), JobState::Running)
        );
    }
    assert_eq!(
        storage.get_state(parent_id).await?,
        JobState::BlockingOnChildren
    );
    pool.close().await;
    drop(storage);
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn only_a_child_insert_over_the_packet_size_is_a_payload_rejection() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let storage = MariaDbStorageConnector::connect(&config).await?;
    let pool = create_raw_pool(&config).await;
    let resource_group_id = create_test_resource_group(&storage).await;
    let job_ids = register_jobs(&storage, resource_group_id, 2).await;
    let parent_id = job_ids[0];
    storage
        .apply_tree_transition(TreeTransition {
            updates: updates(&job_ids, JobState::Ready, JobState::Running),
            child_inserts: Vec::new(),
        })
        .await?;
    let max_allowed_packet: u64 = sqlx::query_scalar("SELECT @@max_allowed_packet;")
        .fetch_one(&pool)
        .await?;
    let oversized_task_graph = vec![7u8; usize::try_from(max_allowed_packet)? + 1];

    let result = storage
        .apply_tree_transition(TreeTransition {
            updates: updates(
                &[parent_id],
                JobState::Running,
                JobState::BlockingOnChildren,
            ),
            child_inserts: vec![ChildJobInsert {
                parent_id,
                root_id: parent_id,
                resource_group_id,
                compressed_serialized_task_graph: &oversized_task_graph,
                compressed_serialized_job_inputs: &[],
            }],
        })
        .await;

    assert!(
        matches!(result, Err(DbError::PayloadRejected(_))),
        "an oversized child insert should be a payload rejection, got {result:?}"
    );
    for job_id in &job_ids {
        assert_eq!(storage.get_state(*job_id).await?, JobState::Running);
    }
    let num_jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM `jobs`;")
        .fetch_one(&pool)
        .await?;
    assert_eq!(num_jobs, i64::try_from(job_ids.len())?);
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
