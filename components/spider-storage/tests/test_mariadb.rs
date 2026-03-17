use std::sync::Arc;
use std::time::Duration;

use spider_core::{
    job::JobState,
    task::TaskGraph,
    types::{
        id::{Id, JobId, ResourceGroupId},
        io::TaskOutput,
    },
};
use spider_storage::db::{
    DbError,
    ExternalJobOrchestration,
    InternalJobOrchestration,
    MariaDbStorage,
    ResourceGroupManagement,
    sql_utils,
};
use sqlx::{MySqlPool, Row};

/// Copies an `Id` (the marker enums don't derive Copy, so we go through the UUID).
const fn copy_rg(id: &ResourceGroupId) -> ResourceGroupId {
    Id::from(*id.as_uuid_ref())
}

const fn copy_job(id: &JobId) -> JobId {
    Id::from(*id.as_uuid_ref())
}

async fn setup() -> (MariaDbStorage, MySqlPool) {
    let jdbc_url = std::env::var("SPIDER_STORAGE_URL").expect("SPIDER_STORAGE_URL must be set");
    let sqlx_url = sql_utils::jdbc_url_to_sqlx(&jdbc_url)
        .expect("SPIDER_STORAGE_URL must be a valid JDBC URL");
    let pool = MySqlPool::connect(&sqlx_url)
        .await
        .expect("failed to connect to MariaDB");
    let storage = MariaDbStorage::new(pool.clone());
    storage.initialize().await.expect("DB init failed");
    (storage, pool)
}

async fn create_test_resource_group(storage: &MariaDbStorage) -> ResourceGroupId {
    let external_id = uuid::Uuid::new_v4().to_string();
    storage
        .add(external_id, "test-password".to_string())
        .await
        .expect("add should succeed")
}

fn minimal_task_graph() -> TaskGraph {
    TaskGraph::default()
}

/// Insert a job with TDL fields set via raw SQL, since `register()` hardcodes them to `None`.
async fn register_job_with_tdl(
    pool: &MySqlPool,
    resource_group_id: &ResourceGroupId,
    commit_tdl_package: Option<&str>,
    cleanup_tdl_package: Option<&str>,
) -> JobId {
    let rg_id_str = resource_group_id.as_uuid_ref().to_string();
    let task_graph = TaskGraph::default();
    let serialized_task_graph = task_graph.to_json().expect("task graph serialization");
    let serialized_job_inputs =
        serde_json::to_string::<Vec<()>>(&vec![]).expect("inputs serialization");

    let row = sqlx::query(
        "INSERT INTO `jobs` (`resource_group_id`, `serialized_task_graph`, \
         `serialized_job_inputs`, `commit_tdl_package`, `commit_tdl_function`, \
         `cleanup_tdl_package`, `cleanup_tdl_function`) \
         VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING CAST(`id` AS CHAR) AS `id`;",
    )
    .bind(&rg_id_str)
    .bind(serialized_task_graph)
    .bind(serialized_job_inputs)
    .bind(commit_tdl_package)
    .bind(commit_tdl_package.map(|_| "commit_fn"))
    .bind(cleanup_tdl_package)
    .bind(cleanup_tdl_package.map(|_| "cleanup_fn"))
    .fetch_one(pool)
    .await
    .expect("raw SQL insert should succeed");

    let id_str: String = row.get(0);
    let uuid = uuid::Uuid::parse_str(&id_str).expect("valid UUID");
    JobId::from(uuid)
}

// ─── ExternalJobOrchestration ───────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_register_job() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    let state = storage
        .get_state(rg_id, job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Ready);
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_register_job_invalid_resource_group() {
    let (storage, _pool) = setup().await;
    let fake_rg_id = ResourceGroupId::new();

    let result = storage
        .register(fake_rg_id, Arc::new(minimal_task_graph()), vec![])
        .await;

    assert!(
        matches!(result, Err(DbError::ResourceGroupNotFound(_))),
        "expected ResourceGroupNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_start_job() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    storage
        .start(copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("start should succeed");

    let state = storage
        .get_state(rg_id, job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Running);
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_start_job_wrong_state() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    storage
        .start(copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("start should succeed");

    let result = storage.start(rg_id, job_id).await;
    assert!(
        matches!(result, Err(DbError::UnexpectedJobState { .. })),
        "expected UnexpectedJobState, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_cancel_job_without_cleanup_transitions_to_cancelled() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    storage
        .start(copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("start should succeed");

    ExternalJobOrchestration::cancel(&storage, copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("cancel should succeed");

    let state = storage
        .get_state(rg_id, job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Cancelled);
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_get_outputs_succeeded_job() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    storage
        .start(copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("start should succeed");

    let outputs = vec![TaskOutput {}];
    let new_state = InternalJobOrchestration::commit_outputs(&storage, copy_job(&job_id), outputs)
        .await
        .expect("commit_outputs should succeed");
    assert_eq!(new_state, JobState::Succeeded);

    let retrieved = storage
        .get_outputs(rg_id, job_id)
        .await
        .expect("get_outputs should succeed");
    assert_eq!(retrieved.len(), 1);
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_get_outputs_wrong_state() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    let result = storage.get_outputs(copy_rg(&rg_id), job_id).await;
    assert!(
        matches!(result, Err(DbError::UnexpectedJobState { .. })),
        "expected UnexpectedJobState"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_get_error_failed_job() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    storage
        .start(copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("start should succeed");

    InternalJobOrchestration::fail(&storage, copy_job(&job_id), "something broke".to_string())
        .await
        .expect("fail should succeed");

    let error_msg = storage
        .get_error(rg_id, job_id)
        .await
        .expect("get_error should succeed");
    assert_eq!(error_msg, "something broke");
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_get_error_wrong_state() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    let result = storage.get_error(copy_rg(&rg_id), job_id).await;
    assert!(
        matches!(result, Err(DbError::UnexpectedJobState { .. })),
        "expected UnexpectedJobState, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_cancel_job_with_cleanup_transitions_to_cleanup_ready() {
    let (storage, pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = register_job_with_tdl(&pool, &rg_id, None, Some("cleanup-pkg")).await;

    storage
        .start(copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("start should succeed");

    ExternalJobOrchestration::cancel(&storage, copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("cancel should succeed");

    let state = storage
        .get_state(rg_id, job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::CleanupReady);
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_cancel_already_terminal() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    storage
        .start(copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("start should succeed");

    ExternalJobOrchestration::cancel(&storage, copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("first cancel should succeed");

    let result =
        ExternalJobOrchestration::cancel(&storage, copy_rg(&rg_id), copy_job(&job_id)).await;
    assert!(
        matches!(result, Err(DbError::UnexpectedJobState { .. })),
        "expected UnexpectedJobState, got {result:?}"
    );
}

// ─── InternalJobOrchestration ───────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_set_state_valid_transition() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    InternalJobOrchestration::set_state(&storage, copy_job(&job_id), JobState::Running)
        .await
        .expect("set_state should succeed");

    let state = storage
        .get_state(rg_id, job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Running);
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_set_state_invalid_transition() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    // Ready -> Succeeded is not a valid transition
    let result =
        InternalJobOrchestration::set_state(&storage, copy_job(&job_id), JobState::Succeeded)
            .await;
    assert!(
        matches!(result, Err(DbError::InvalidJobStateTransition { .. })),
        "expected InvalidJobStateTransition, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_commit_outputs_without_commit_task() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    storage
        .start(copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("start should succeed");

    let new_state =
        InternalJobOrchestration::commit_outputs(&storage, copy_job(&job_id), vec![TaskOutput {}])
            .await
            .expect("commit_outputs should succeed");
    assert_eq!(new_state, JobState::Succeeded);
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_commit_outputs_with_commit_task() {
    let (storage, pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = register_job_with_tdl(&pool, &rg_id, Some("commit-pkg"), None).await;

    // Transition to Running via set_state
    InternalJobOrchestration::set_state(&storage, copy_job(&job_id), JobState::Running)
        .await
        .expect("set_state should succeed");

    let new_state =
        InternalJobOrchestration::commit_outputs(&storage, copy_job(&job_id), vec![TaskOutput {}])
            .await
            .expect("commit_outputs should succeed");
    assert_eq!(new_state, JobState::CommitReady);
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_commit_outputs_wrong_state() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    // Job is in Ready state, not Running
    let result =
        InternalJobOrchestration::commit_outputs(&storage, copy_job(&job_id), vec![TaskOutput {}])
            .await;
    assert!(
        matches!(result, Err(DbError::InvalidJobStateTransition { .. })),
        "expected InvalidJobStateTransition, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_internal_cancel_without_cleanup() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    storage
        .start(copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("start should succeed");

    let new_state = InternalJobOrchestration::cancel(&storage, copy_job(&job_id))
        .await
        .expect("cancel should succeed");
    assert_eq!(new_state, JobState::Cancelled);
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_internal_cancel_with_cleanup() {
    let (storage, pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = register_job_with_tdl(&pool, &rg_id, None, Some("cleanup-pkg")).await;

    InternalJobOrchestration::set_state(&storage, copy_job(&job_id), JobState::Running)
        .await
        .expect("set_state should succeed");

    let new_state = InternalJobOrchestration::cancel(&storage, copy_job(&job_id))
        .await
        .expect("cancel should succeed");
    assert_eq!(new_state, JobState::CleanupReady);
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_internal_cancel_terminal_state() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    storage
        .start(copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("start should succeed");

    // Cancel via external (goes to Cancelled terminal state)
    ExternalJobOrchestration::cancel(&storage, copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("external cancel should succeed");

    let result = InternalJobOrchestration::cancel(&storage, copy_job(&job_id)).await;
    assert!(
        matches!(result, Err(DbError::InvalidJobStateTransition { .. })),
        "expected InvalidJobStateTransition, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_fail_job() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    storage
        .start(copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("start should succeed");

    InternalJobOrchestration::fail(&storage, copy_job(&job_id), "some error".to_string())
        .await
        .expect("fail should succeed");

    let state = storage
        .get_state(rg_id, job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Failed);
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_fail_terminal_state() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    storage
        .start(copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("start should succeed");

    InternalJobOrchestration::fail(&storage, copy_job(&job_id), "first error".to_string())
        .await
        .expect("first fail should succeed");

    let result =
        InternalJobOrchestration::fail(&storage, copy_job(&job_id), "second error".to_string())
            .await;
    assert!(
        matches!(result, Err(DbError::InvalidJobStateTransition { .. })),
        "expected InvalidJobStateTransition, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_delete_expired_terminated_jobs() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    storage
        .start(copy_rg(&rg_id), copy_job(&job_id))
        .await
        .expect("start should succeed");

    InternalJobOrchestration::fail(&storage, copy_job(&job_id), "expired".to_string())
        .await
        .expect("fail should succeed");

    // Wait so that `ended_at` is strictly in the past relative to NOW() - INTERVAL.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let deleted = storage
        .delete_expired_terminated_jobs(Duration::from_secs(1))
        .await
        .expect("delete_expired should succeed");
    assert!(
        deleted.contains(&job_id),
        "expected job_id in deleted list, got {deleted:?}"
    );

    let result = storage.get_state(copy_rg(&rg_id), job_id).await;
    assert!(
        matches!(result, Err(DbError::JobNotFound(_))),
        "expected JobNotFound, got {result:?}"
    );
}

// ─── ResourceGroupManagement ────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_add_duplicate_resource_group() {
    let (storage, _pool) = setup().await;
    let external_id = uuid::Uuid::new_v4().to_string();

    storage
        .add(external_id.clone(), "password".to_string())
        .await
        .expect("first add should succeed");

    let result = storage.add(external_id, "password".to_string()).await;
    assert!(
        matches!(result, Err(DbError::ResourceGroupAlreadyExists(_))),
        "expected ResourceGroupAlreadyExists, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_verify_correct_password() {
    let (storage, _pool) = setup().await;

    let rg_id = storage
        .add(
            uuid::Uuid::new_v4().to_string(),
            "correct-password".to_string(),
        )
        .await
        .expect("add should succeed");

    storage
        .verify(rg_id, "correct-password".to_string())
        .await
        .expect("verify with correct password should succeed");
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_verify_wrong_password() {
    let (storage, _pool) = setup().await;

    let rg_id = storage
        .add(
            uuid::Uuid::new_v4().to_string(),
            "correct-password".to_string(),
        )
        .await
        .expect("add should succeed");

    let result = storage
        .verify(rg_id, "wrong-password".to_string())
        .await;
    assert!(
        matches!(result, Err(DbError::InvalidPassword(_))),
        "expected InvalidPassword, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_verify_nonexistent_resource_group() {
    let (storage, _pool) = setup().await;
    let fake_rg_id = ResourceGroupId::new();

    let result = storage
        .verify(fake_rg_id, "password".to_string())
        .await;
    assert!(
        matches!(result, Err(DbError::ResourceGroupNotFound(_))),
        "expected ResourceGroupNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_delete_resource_group() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    storage
        .delete(rg_id)
        .await
        .expect("delete should succeed");
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_delete_resource_group_with_jobs() {
    let (storage, _pool) = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    storage
        .delete(rg_id)
        .await
        .expect("delete should succeed even with jobs");
}

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_delete_nonexistent_resource_group() {
    let (storage, _pool) = setup().await;
    let fake_rg_id = ResourceGroupId::new();

    let result = storage.delete(fake_rg_id).await;
    assert!(
        matches!(result, Err(DbError::ResourceGroupNotFound(_))),
        "expected ResourceGroupNotFound, got {result:?}"
    );
}
