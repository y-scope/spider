use std::sync::Arc;

use spider_core::{
    job::JobState,
    task::TaskGraph,
    types::id::{Id, JobId, ResourceGroupId},
};
use spider_storage::db::{
    DbError,
    ExternalJobOrchestration,
    MariaDbStorage,
    ResourceGroupManagement,
    sql_utils,
};
use sqlx::MySqlPool;

/// Copies an `Id` (the marker enums don't derive Copy, so we go through the UUID).
const fn copy_rg(id: &ResourceGroupId) -> ResourceGroupId {
    Id::from(*id.as_uuid_ref())
}

const fn copy_job(id: &JobId) -> JobId {
    Id::from(*id.as_uuid_ref())
}

async fn setup() -> MariaDbStorage {
    let jdbc_url = std::env::var("SPIDER_STORAGE_URL").expect("SPIDER_STORAGE_URL must be set");
    let sqlx_url = sql_utils::jdbc_url_to_sqlx(&jdbc_url)
        .expect("SPIDER_STORAGE_URL must be a valid JDBC URL");
    let pool = MySqlPool::connect(&sqlx_url)
        .await
        .expect("failed to connect to MariaDB");
    let storage = MariaDbStorage::new(pool);
    storage.initialize().await.expect("DB init failed");
    storage
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

#[tokio::test]
#[ignore = "requires MariaDB via SPIDER_STORAGE_URL"]
async fn test_register_job() {
    let storage = setup().await;
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
    let storage = setup().await;
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
async fn test_cancel_job_without_cleanup_transitions_to_cancelled() {
    let storage = setup().await;
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
async fn test_start_job() {
    let storage = setup().await;
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
    let storage = setup().await;
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
