use std::{sync::Arc, time::Duration};

use secrecy::SecretString;
use spider_core::{
    job::{CancelTarget, CommitTarget, JobState},
    task::TaskGraph,
    types::id::{Id, JobId, ResourceGroupId},
};
use spider_storage::{
    DatabaseConfig,
    db::{
        DbError,
        ExternalJobOrchestration,
        InternalJobOrchestration,
        MariaDbStorageConnector,
        ResourceGroupManagement,
    },
};

/// Copies an `Id` (the marker enums don't derive Copy, so we go through the UUID).
const fn copy_rg(id: &ResourceGroupId) -> ResourceGroupId {
    Id::from_uuid(*id.as_uuid_ref())
}

const fn copy_job(id: &JobId) -> JobId {
    Id::from_uuid(*id.as_uuid_ref())
}

async fn setup() -> MariaDbStorageConnector {
    let port: u16 = std::env::var("MARIADB_PORT")
        .expect("MARIADB_PORT")
        .parse()
        .expect("valid port");
    let database = std::env::var("MARIADB_DATABASE").expect("MARIADB_DATABASE");
    let username = std::env::var("MARIADB_USERNAME").expect("MARIADB_USERNAME");
    let password = std::env::var("MARIADB_PASSWORD").expect("MARIADB_PASSWORD");

    let config = DatabaseConfig {
        host: "localhost".to_string(),
        port,
        name: database,
        username,
        password: SecretString::from(password),
        max_connections: 5,
    };
    MariaDbStorageConnector::connect_and_initialize(&config)
        .await
        .expect("connect_and_initialize failed")
}

async fn create_test_resource_group(storage: &MariaDbStorageConnector) -> ResourceGroupId {
    let external_id = uuid::Uuid::new_v4().to_string();
    storage
        .add(external_id, "test-password".to_string())
        .await
        .expect("add should succeed")
}

fn minimal_task_graph() -> TaskGraph {
    TaskGraph::default()
}

// ─── ExternalJobOrchestration ───────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires MariaDB"]
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
#[ignore = "requires MariaDB"]
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
#[ignore = "requires MariaDB"]
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
#[ignore = "requires MariaDB"]
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

#[tokio::test]
#[ignore = "requires MariaDB"]
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

    ExternalJobOrchestration::cancel(
        &storage,
        copy_rg(&rg_id),
        copy_job(&job_id),
        CancelTarget::Cancelled,
    )
    .await
    .expect("cancel should succeed");

    let state = storage
        .get_state(rg_id, job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Cancelled);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_get_outputs_succeeded_job() {
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

    let outputs = vec![vec![]];
    InternalJobOrchestration::commit_outputs(
        &storage,
        copy_job(&job_id),
        outputs,
        CommitTarget::Succeeded,
    )
    .await
    .expect("commit_outputs should succeed");

    let retrieved = storage
        .get_outputs(rg_id, job_id)
        .await
        .expect("get_outputs should succeed");
    assert_eq!(retrieved.len(), 1);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_get_outputs_wrong_state() {
    let storage = setup().await;
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
#[ignore = "requires MariaDB"]
async fn test_get_error_failed_job() {
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
#[ignore = "requires MariaDB"]
async fn test_get_error_wrong_state() {
    let storage = setup().await;
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
#[ignore = "requires MariaDB"]
async fn test_cancel_job_with_cleanup_transitions_to_cleanup_ready() {
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

    ExternalJobOrchestration::cancel(
        &storage,
        copy_rg(&rg_id),
        copy_job(&job_id),
        CancelTarget::CleanupReady,
    )
    .await
    .expect("cancel should succeed");

    let state = storage
        .get_state(rg_id, job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::CleanupReady);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_cancel_already_terminal() {
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

    ExternalJobOrchestration::cancel(
        &storage,
        copy_rg(&rg_id),
        copy_job(&job_id),
        CancelTarget::Cancelled,
    )
    .await
    .expect("first cancel should succeed");

    let result = ExternalJobOrchestration::cancel(
        &storage,
        copy_rg(&rg_id),
        copy_job(&job_id),
        CancelTarget::Cancelled,
    )
    .await;
    assert!(
        matches!(result, Err(DbError::UnexpectedJobState { .. })),
        "expected UnexpectedJobState, got {result:?}"
    );
}

// ─── InternalJobOrchestration ───────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_set_state_valid_transition() {
    let storage = setup().await;
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
#[ignore = "requires MariaDB"]
async fn test_set_state_invalid_transition() {
    let storage = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    // Ready -> Succeeded is not a valid transition
    let result =
        InternalJobOrchestration::set_state(&storage, copy_job(&job_id), JobState::Succeeded).await;
    assert!(
        matches!(result, Err(DbError::InvalidJobStateTransition { .. })),
        "expected InvalidJobStateTransition, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_commit_outputs_without_commit_task() {
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

    InternalJobOrchestration::commit_outputs(
        &storage,
        copy_job(&job_id),
        vec![vec![]],
        CommitTarget::Succeeded,
    )
    .await
    .expect("commit_outputs should succeed");

    let state = storage
        .get_state(rg_id, job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Succeeded);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_commit_outputs_with_commit_task() {
    let storage = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    // Transition to Running via set_state
    InternalJobOrchestration::set_state(&storage, copy_job(&job_id), JobState::Running)
        .await
        .expect("set_state should succeed");

    InternalJobOrchestration::commit_outputs(
        &storage,
        copy_job(&job_id),
        vec![vec![]],
        CommitTarget::CommitReady,
    )
    .await
    .expect("commit_outputs should succeed");

    let state = storage
        .get_state(rg_id, job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::CommitReady);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_commit_outputs_wrong_state() {
    let storage = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    // Job is in Ready state, not Running
    let result = InternalJobOrchestration::commit_outputs(
        &storage,
        copy_job(&job_id),
        vec![vec![]],
        CommitTarget::Succeeded,
    )
    .await;
    assert!(
        matches!(result, Err(DbError::InvalidJobStateTransition { .. })),
        "expected InvalidJobStateTransition, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_internal_cancel_without_cleanup() {
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

    InternalJobOrchestration::cancel(&storage, copy_job(&job_id), CancelTarget::Cancelled)
        .await
        .expect("cancel should succeed");

    let state = storage
        .get_state(rg_id, job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Cancelled);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_internal_cancel_with_cleanup() {
    let storage = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    let job_id = storage
        .register(copy_rg(&rg_id), Arc::new(minimal_task_graph()), vec![])
        .await
        .expect("register should succeed");

    InternalJobOrchestration::set_state(&storage, copy_job(&job_id), JobState::Running)
        .await
        .expect("set_state should succeed");

    InternalJobOrchestration::cancel(&storage, copy_job(&job_id), CancelTarget::CleanupReady)
        .await
        .expect("cancel should succeed");

    let state = storage
        .get_state(rg_id, job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::CleanupReady);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_internal_cancel_terminal_state() {
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

    // Cancel via external (goes to Cancelled terminal state)
    ExternalJobOrchestration::cancel(
        &storage,
        copy_rg(&rg_id),
        copy_job(&job_id),
        CancelTarget::Cancelled,
    )
    .await
    .expect("external cancel should succeed");

    let result =
        InternalJobOrchestration::cancel(&storage, copy_job(&job_id), CancelTarget::Cancelled)
            .await;
    assert!(
        matches!(result, Err(DbError::InvalidJobStateTransition { .. })),
        "expected InvalidJobStateTransition, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_fail_job() {
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
#[ignore = "requires MariaDB"]
async fn test_fail_terminal_state() {
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
#[ignore = "requires MariaDB"]
async fn test_delete_expired_terminated_jobs() {
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
#[ignore = "requires MariaDB"]
async fn test_add_duplicate_resource_group() {
    let storage = setup().await;
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
#[ignore = "requires MariaDB"]
async fn test_verify_correct_password() {
    let storage = setup().await;

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
#[ignore = "requires MariaDB"]
async fn test_verify_wrong_password() {
    let storage = setup().await;

    let rg_id = storage
        .add(
            uuid::Uuid::new_v4().to_string(),
            "correct-password".to_string(),
        )
        .await
        .expect("add should succeed");

    let result = storage.verify(rg_id, "wrong-password".to_string()).await;
    assert!(
        matches!(result, Err(DbError::InvalidPassword(_))),
        "expected InvalidPassword, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_verify_nonexistent_resource_group() {
    let storage = setup().await;
    let fake_rg_id = ResourceGroupId::new();

    let result = storage.verify(fake_rg_id, "password".to_string()).await;
    assert!(
        matches!(result, Err(DbError::ResourceGroupNotFound(_))),
        "expected ResourceGroupNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_delete_resource_group() {
    let storage = setup().await;
    let rg_id = create_test_resource_group(&storage).await;

    storage.delete(rg_id).await.expect("delete should succeed");
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_delete_resource_group_with_jobs() {
    let storage = setup().await;
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
#[ignore = "requires MariaDB"]
async fn test_delete_nonexistent_resource_group() {
    let storage = setup().await;
    let fake_rg_id = ResourceGroupId::new();

    let result = storage.delete(fake_rg_id).await;
    assert!(
        matches!(result, Err(DbError::ResourceGroupNotFound(_))),
        "expected ResourceGroupNotFound, got {result:?}"
    );
}
