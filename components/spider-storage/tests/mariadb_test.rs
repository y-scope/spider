use std::time::Duration;

use spider_core::{
    job::JobState,
    types::{
        id::{JobId, ResourceGroupId},
        io::TaskInput,
    },
};
use spider_storage::db::{
    DbError,
    ExternalJobOrchestration,
    InternalJobOrchestration,
    ResourceGroupManagement,
};

use super::{
    mariadb_infra::{create_mariadb_connector, create_test_resource_group},
    task_graph_builder::{SubmittedTaskGraph, build_flat_task_graph},
};

/// Input payload size in bytes for the single-task graph used by DB-layer tests.
const TEST_INPUT_PAYLOAD_SIZE: usize = 128;

/// Builds a task graph with a single task for DB-layer tests.
///
/// # Returns
///
/// Forwards `build_flat_task_graph`'s return values.
fn single_task_graph() -> (SubmittedTaskGraph, Vec<TaskInput>) {
    build_flat_task_graph(1, TEST_INPUT_PAYLOAD_SIZE, false, false)
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_register_job() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    let state = storage
        .get_state(job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Ready);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_register_job_invalid_resource_group() {
    let storage = create_mariadb_connector().await;
    let fake_rg_id = ResourceGroupId::new();
    let (graph, inputs) = single_task_graph();

    let result = storage
        .register(fake_rg_id, &graph, inputs.as_slice())
        .await;

    assert!(
        matches!(result, Err(DbError::ResourceGroupNotFound(_))),
        "expected ResourceGroupNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_start_job() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    storage.start(job_id).await.expect("start should succeed");

    let state = storage
        .get_state(job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Running);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_start_job_wrong_state() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    storage.start(job_id).await.expect("start should succeed");

    let result = storage.start(job_id).await;
    assert!(
        matches!(result, Err(DbError::UnexpectedJobState { .. })),
        "expected UnexpectedJobState, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_cancel_job_without_cleanup_transitions_to_cancelled() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    storage.start(job_id).await.expect("start should succeed");

    storage
        .cancel(job_id, false)
        .await
        .expect("cancel should succeed");

    let state = storage
        .get_state(job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Cancelled);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_get_outputs_succeeded_job() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    storage.start(job_id).await.expect("start should succeed");

    let outputs = vec![vec![1, 2, 3]];
    InternalJobOrchestration::commit_outputs(&storage, job_id, outputs.clone(), false)
        .await
        .expect("commit_outputs should succeed");

    let retrieved = storage
        .get_outputs(job_id)
        .await
        .expect("get_outputs should succeed");
    assert_eq!(retrieved, outputs);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_get_outputs_wrong_state() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    let result = storage.get_outputs(job_id).await;
    assert!(
        matches!(result, Err(DbError::UnexpectedJobState { .. })),
        "expected UnexpectedJobState"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_get_error_failed_job() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    storage.start(job_id).await.expect("start should succeed");

    InternalJobOrchestration::fail(&storage, job_id, "something broke".to_string())
        .await
        .expect("fail should succeed");

    let error_msg = storage
        .get_error(job_id)
        .await
        .expect("get_error should succeed");
    assert_eq!(error_msg, "something broke");
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_get_error_wrong_state() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    let result = storage.get_error(job_id).await;
    assert!(
        matches!(result, Err(DbError::UnexpectedJobState { .. })),
        "expected UnexpectedJobState, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_cancel_job_with_cleanup_transitions_to_cleanup_ready() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    storage.start(job_id).await.expect("start should succeed");

    InternalJobOrchestration::cancel(&storage, job_id, true)
        .await
        .expect("cancel should succeed");

    let state = storage
        .get_state(job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::CleanupReady);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_cancel_already_terminal() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    storage.start(job_id).await.expect("start should succeed");

    InternalJobOrchestration::cancel(&storage, job_id, false)
        .await
        .expect("first cancel should succeed");

    let result = InternalJobOrchestration::cancel(&storage, job_id, false).await;
    match result {
        Err(DbError::InvalidJobStateTransition { from, to }) => {
            assert_eq!(from, JobState::Cancelled);
            assert_eq!(to, JobState::Cancelled);
        }
        other => panic!("expected InvalidJobStateTransition, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_set_state_valid_transition() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    InternalJobOrchestration::set_state(&storage, job_id, JobState::Running)
        .await
        .expect("set_state should succeed");

    let state = storage
        .get_state(job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Running);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_set_state_invalid_transition() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    // Ready -> Succeeded is not a valid transition
    let result = InternalJobOrchestration::set_state(&storage, job_id, JobState::Succeeded).await;
    match result {
        Err(DbError::InvalidJobStateTransition { from, to }) => {
            assert_eq!(from, JobState::Ready);
            assert_eq!(to, JobState::Succeeded);
        }
        other => panic!("expected InvalidJobStateTransition, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_commit_outputs_without_commit_task() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    storage.start(job_id).await.expect("start should succeed");

    InternalJobOrchestration::commit_outputs(&storage, job_id, vec![vec![]], false)
        .await
        .expect("commit_outputs should succeed");

    let state = storage
        .get_state(job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Succeeded);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_commit_outputs_with_commit_task() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    // Transition to Running via set_state
    InternalJobOrchestration::set_state(&storage, job_id, JobState::Running)
        .await
        .expect("set_state should succeed");

    InternalJobOrchestration::commit_outputs(&storage, job_id, vec![vec![]], true)
        .await
        .expect("commit_outputs should succeed");

    let state = storage
        .get_state(job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::CommitReady);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_commit_outputs_wrong_state() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    // Job is in Ready state, not Running
    let result =
        InternalJobOrchestration::commit_outputs(&storage, job_id, vec![vec![]], false).await;
    match result {
        Err(DbError::InvalidJobStateTransition { from, to }) => {
            assert_eq!(from, JobState::Ready);
            assert_eq!(to, JobState::Succeeded);
        }
        other => panic!("expected InvalidJobStateTransition, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_fail_job() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    storage.start(job_id).await.expect("start should succeed");

    InternalJobOrchestration::fail(&storage, job_id, "some error".to_string())
        .await
        .expect("fail should succeed");

    let state = storage
        .get_state(job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Failed);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_fail_terminal_state() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    storage.start(job_id).await.expect("start should succeed");

    InternalJobOrchestration::fail(&storage, job_id, "first error".to_string())
        .await
        .expect("first fail should succeed");

    let result = InternalJobOrchestration::fail(&storage, job_id, "second error".to_string()).await;
    match result {
        Err(DbError::InvalidJobStateTransition { from, to }) => {
            assert_eq!(from, JobState::Failed);
            assert_eq!(to, JobState::Failed);
        }
        other => panic!("expected InvalidJobStateTransition, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires MariaDB"]
#[serial_test::serial]
async fn test_delete_expired_terminated_jobs() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    storage.start(job_id).await.expect("start should succeed");

    InternalJobOrchestration::fail(&storage, job_id, "expired".to_string())
        .await
        .expect("fail should succeed");

    // Wait so that `ended_at` is strictly in the past relative to NOW() - INTERVAL.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let deleted = storage
        .delete_expired_terminated_jobs(1)
        .await
        .expect("delete_expired should succeed");
    assert!(
        deleted.contains(&job_id),
        "expected job_id in deleted list, got {deleted:?}"
    );

    let result = storage.get_state(job_id).await;
    assert!(
        matches!(result, Err(DbError::JobNotFound(_))),
        "expected JobNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_add_duplicate_resource_group() {
    let storage = create_mariadb_connector().await;
    let external_id = uuid::Uuid::new_v4().to_string();

    storage
        .add(external_id.clone(), b"password".to_vec())
        .await
        .expect("first add should succeed");

    let result = storage.add(external_id, b"password".to_vec()).await;
    assert!(
        matches!(result, Err(DbError::ResourceGroupAlreadyExists(_))),
        "expected ResourceGroupAlreadyExists, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_verify_correct_password() {
    let storage = create_mariadb_connector().await;

    let rg_id = storage
        .add(
            uuid::Uuid::new_v4().to_string(),
            b"correct-password".to_vec(),
        )
        .await
        .expect("add should succeed");

    storage
        .verify(rg_id, b"correct-password")
        .await
        .expect("verify with correct password should succeed");
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_verify_wrong_password() {
    let storage = create_mariadb_connector().await;

    let rg_id = storage
        .add(
            uuid::Uuid::new_v4().to_string(),
            b"correct-password".to_vec(),
        )
        .await
        .expect("add should succeed");

    let result = storage.verify(rg_id, b"wrong-password").await;
    assert!(
        matches!(result, Err(DbError::InvalidPassword(_))),
        "expected InvalidPassword, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_verify_nonexistent_resource_group() {
    let storage = create_mariadb_connector().await;
    let fake_rg_id = ResourceGroupId::new();

    let result = storage.verify(fake_rg_id, b"password").await;
    assert!(
        matches!(result, Err(DbError::ResourceGroupNotFound(_))),
        "expected ResourceGroupNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_start_job_not_found() {
    let storage = create_mariadb_connector().await;
    let fake_job_id = JobId::new();

    let result = storage.start(fake_job_id).await;
    assert!(
        matches!(result, Err(DbError::JobNotFound(_))),
        "expected JobNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_set_state_job_not_found() {
    let storage = create_mariadb_connector().await;
    let fake_job_id = JobId::new();

    let result =
        InternalJobOrchestration::set_state(&storage, fake_job_id, JobState::Running).await;
    assert!(
        matches!(result, Err(DbError::JobNotFound(_))),
        "expected JobNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_get_state_job_not_found() {
    let storage = create_mariadb_connector().await;
    let fake_job_id = JobId::new();

    let result = storage.get_state(fake_job_id).await;
    assert!(
        matches!(result, Err(DbError::JobNotFound(_))),
        "expected JobNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_get_outputs_job_not_found() {
    let storage = create_mariadb_connector().await;
    let fake_job_id = JobId::new();

    let result = storage.get_outputs(fake_job_id).await;
    assert!(
        matches!(result, Err(DbError::JobNotFound(_))),
        "expected JobNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_get_error_job_not_found() {
    let storage = create_mariadb_connector().await;
    let fake_job_id = JobId::new();

    let result = storage.get_error(fake_job_id).await;
    assert!(
        matches!(result, Err(DbError::JobNotFound(_))),
        "expected JobNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_commit_outputs_job_not_found() {
    let storage = create_mariadb_connector().await;
    let fake_job_id = JobId::new();

    let result =
        InternalJobOrchestration::commit_outputs(&storage, fake_job_id, vec![vec![]], false).await;
    assert!(
        matches!(result, Err(DbError::JobNotFound(_))),
        "expected JobNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_cancel_job_not_found() {
    let storage = create_mariadb_connector().await;
    let fake_job_id = JobId::new();

    let result = InternalJobOrchestration::cancel(&storage, fake_job_id, false).await;
    assert!(
        matches!(result, Err(DbError::JobNotFound(_))),
        "expected JobNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_fail_job_not_found() {
    let storage = create_mariadb_connector().await;
    let fake_job_id = JobId::new();

    let result = InternalJobOrchestration::fail(&storage, fake_job_id, "error".to_string()).await;
    assert!(
        matches!(result, Err(DbError::JobNotFound(_))),
        "expected JobNotFound, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_cancel_from_ready_state() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    InternalJobOrchestration::cancel(&storage, job_id, false)
        .await
        .expect("cancel from Ready should succeed");

    let state = storage
        .get_state(job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Cancelled);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
#[serial_test::serial]
async fn test_delete_expired_terminated_jobs_no_match() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();

    let job_id = storage
        .register(rg_id, &graph, inputs.as_slice())
        .await
        .expect("register should succeed");

    storage.start(job_id).await.expect("start should succeed");

    InternalJobOrchestration::fail(&storage, job_id, "recent failure".to_string())
        .await
        .expect("fail should succeed");

    // Large window — the just-failed job should not be expired yet.
    let deleted = storage
        .delete_expired_terminated_jobs(60)
        .await
        .expect("delete_expired should succeed");
    assert!(
        !deleted.contains(&job_id),
        "recently failed job should not be expired, but it was deleted"
    );

    let state = storage
        .get_state(job_id)
        .await
        .expect("get_state should succeed");
    assert_eq!(state, JobState::Failed);
}
