use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::time::Duration;

use spider_core::job::JobState;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SchedulerId;
use spider_core::types::io::TaskInput;
use spider_storage::db::DbError;
use spider_storage::db::ExecutionManagerLivenessManagement;
use spider_storage::db::ExternalJobOrchestration;
use spider_storage::db::InternalJobOrchestration;
use spider_storage::db::MariaDbStorageConnector;
use spider_storage::db::ResourceGroupManagement;
use spider_storage::db::SchedulerRegistrationManagement;
use spider_storage::db::SessionManagement;
use tokio::task::JoinSet;

use super::mariadb_infra::create_mariadb_config;
use super::mariadb_infra::create_mariadb_connector;
use super::mariadb_infra::create_test_resource_group;
use super::task_graph_builder::SubmittedTaskGraph;
use super::task_graph_builder::build_flat_task_graph;
use super::task_graph_builder::create_validated_submission;

/// Input payload size in bytes for the single-task graph used by DB-layer tests.
const TEST_INPUT_PAYLOAD_SIZE: usize = 128;

/// Number of execution managers to register in multi-EM tests.
const TEST_NUM_EMS: usize = 3;
const TEST_SCHEDULER_PORT: u16 = 5678;
const TEST_UPDATED_SCHEDULER_PORT: u16 = 6789;

/// Builds a task graph with a single task for DB-layer tests.
///
/// # Returns
///
/// Forwards `build_flat_task_graph`'s return values.
fn single_task_graph() -> (SubmittedTaskGraph, Vec<TaskInput>) {
    build_flat_task_graph(1, TEST_INPUT_PAYLOAD_SIZE, false, false)
}

/// Registers a new execution manager with `127.0.0.1` as the IP address.
///
/// # Returns
///
/// The ID of the registered execution manager.
async fn register_test_em(storage: &MariaDbStorageConnector) -> ExecutionManagerId {
    storage
        .register_execution_manager(IpAddr::V4(Ipv4Addr::LOCALHOST))
        .await
        .expect("register_execution_manager should succeed")
}

/// # Returns
///
/// Whether the scheduler is registered.
async fn is_scheduler_registered(
    storage: &MariaDbStorageConnector,
    scheduler_id: SchedulerId,
) -> bool {
    storage
        .is_scheduler_registered(scheduler_id)
        .await
        .expect("is_scheduler_registered should succeed")
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_register_job() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let fake_rg_id = ResourceGroupId::random();
    let (graph, inputs) = single_task_graph();
    let job_submission = create_validated_submission(graph, inputs);

    let result = storage.register(fake_rg_id, &job_submission).await;

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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let (graph, inputs) = build_flat_task_graph(1, TEST_INPUT_PAYLOAD_SIZE, false, true);
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let (graph, inputs) = build_flat_task_graph(1, TEST_INPUT_PAYLOAD_SIZE, true, false);
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
#[serial_test::file_serial]
async fn test_delete_expired_terminated_jobs() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
    let external_id = format!("test-resource-group-{}", rand::random::<u64>());

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
            format!("test-resource-group-{}", rand::random::<u64>()),
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
            format!("test-resource-group-{}", rand::random::<u64>()),
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
    let fake_rg_id = ResourceGroupId::random();

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
    let fake_job_id = JobId::random();

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
    let fake_job_id = JobId::random();

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
    let fake_job_id = JobId::random();

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
    let fake_job_id = JobId::random();

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
    let fake_job_id = JobId::random();

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
    let fake_job_id = JobId::random();

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
    let fake_job_id = JobId::random();

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
    let fake_job_id = JobId::random();

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
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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
#[serial_test::file_serial]
async fn test_delete_expired_terminated_jobs_no_match() {
    let storage = create_mariadb_connector().await;
    let rg_id = create_test_resource_group(&storage).await;
    let (graph, inputs) = single_task_graph();
    let job_submission = create_validated_submission(graph, inputs);

    let job_id = storage
        .register(rg_id, &job_submission)
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

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_register_execution_manager() {
    let storage = create_mariadb_connector().await;
    let em_id = storage
        .register_execution_manager(IpAddr::V4(Ipv4Addr::LOCALHOST))
        .await
        .expect("register_execution_manager should succeed");

    let alive = storage
        .is_execution_manager_alive(em_id)
        .await
        .expect("is_execution_manager_alive should succeed");
    assert!(alive, "newly registered EM should be alive");
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_update_execution_manager_heartbeat() {
    let storage = create_mariadb_connector().await;
    let em_id = register_test_em(&storage).await;

    storage
        .update_execution_manager_heartbeat(em_id)
        .await
        .expect("update_execution_manager_heartbeat should succeed");
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_update_execution_manager_heartbeat_not_found() {
    let storage = create_mariadb_connector().await;
    let fake_em_id = ExecutionManagerId::random();

    let result = storage.update_execution_manager_heartbeat(fake_em_id).await;
    assert!(
        matches!(result, Err(DbError::IllegalExecutionManagerId(_))),
        "expected IllegalExecutionManagerId, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
#[serial_test::file_serial]
async fn test_update_execution_manager_heartbeat_already_dead() {
    let storage = create_mariadb_connector().await;
    let em_id = register_test_em(&storage).await;

    // Wait for the heartbeat to become stale, then mark the EM dead.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let dead = storage
        .get_dead_execution_managers(1)
        .await
        .expect("get_dead_execution_managers should succeed");
    assert!(
        dead.contains(&em_id),
        "expected em_id in dead list, got {dead:?}"
    );

    let result = storage.update_execution_manager_heartbeat(em_id).await;
    assert!(
        matches!(result, Err(DbError::ExecutionManagerAlreadyDead(_))),
        "expected ExecutionManagerAlreadyDead, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_is_execution_manager_alive_em_alive() {
    let storage = create_mariadb_connector().await;
    let em_id = register_test_em(&storage).await;

    let alive = storage
        .is_execution_manager_alive(em_id)
        .await
        .expect("is_execution_manager_alive should succeed");
    assert!(alive, "registered EM should be alive");
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_is_execution_manager_alive_em_not_found() {
    let storage = create_mariadb_connector().await;
    let fake_em_id = ExecutionManagerId::random();

    let result = storage.is_execution_manager_alive(fake_em_id).await;
    assert!(
        matches!(result, Err(DbError::IllegalExecutionManagerId(id)) if id == fake_em_id),
        "nonexistent EM should return IllegalExecutionManagerId, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
#[serial_test::file_serial]
async fn test_is_execution_manager_alive_em_dead() {
    let storage = create_mariadb_connector().await;
    let em_id = register_test_em(&storage).await;

    // Mark the EM dead.
    tokio::time::sleep(Duration::from_secs(2)).await;
    storage
        .get_dead_execution_managers(1)
        .await
        .expect("get_dead_execution_managers should succeed");

    let alive = storage
        .is_execution_manager_alive(em_id)
        .await
        .expect("is_execution_manager_alive should succeed");
    assert!(!alive, "dead EM should not be alive");
}

#[tokio::test]
#[ignore = "requires MariaDB"]
#[serial_test::file_serial]
async fn test_get_dead_execution_managers_none_stale() {
    let storage = create_mariadb_connector().await;
    let em_id = register_test_em(&storage).await;

    // Large window — the just-registered EM should not be stale yet.
    let dead = storage
        .get_dead_execution_managers(1)
        .await
        .expect("get_dead_execution_managers should succeed");
    assert!(
        !dead.contains(&em_id),
        "freshly registered EM should not be stale, got {dead:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
#[serial_test::file_serial]
async fn test_get_dead_execution_managers_marks_dead() {
    let storage = create_mariadb_connector().await;
    let em_id = register_test_em(&storage).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let dead = storage
        .get_dead_execution_managers(1)
        .await
        .expect("get_dead_execution_managers should succeed");
    assert!(
        dead.contains(&em_id),
        "expected em_id in dead list, got {dead:?}"
    );

    let alive = storage
        .is_execution_manager_alive(em_id)
        .await
        .expect("is_execution_manager_alive should succeed");
    assert!(!alive, "stale EM should be marked dead");
}

#[tokio::test]
#[ignore = "requires MariaDB"]
#[serial_test::file_serial]
async fn test_get_dead_execution_managers_atomic() {
    let storage = create_mariadb_connector().await;
    let em_id = register_test_em(&storage).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let dead_first = storage
        .get_dead_execution_managers(1)
        .await
        .expect("first get_dead_execution_managers should succeed");
    assert!(
        dead_first.contains(&em_id),
        "expected {em_id} in first dead list, got {dead_first:?}"
    );

    // Second call should not return the same EM again.
    let dead_second = storage
        .get_dead_execution_managers(1)
        .await
        .expect("second get_dead_execution_managers should succeed");
    assert!(
        !dead_second.contains(&em_id),
        "already-dead EM should not appear again, got {dead_second:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
#[serial_test::file_serial]
async fn test_get_dead_execution_managers_multiple() {
    let storage = create_mariadb_connector().await;
    let mut em_ids = Vec::with_capacity(3);
    for _ in 0..TEST_NUM_EMS {
        em_ids.push(register_test_em(&storage).await);
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    let dead = storage
        .get_dead_execution_managers(1)
        .await
        .expect("get_dead_execution_managers should succeed");

    for em_id in &em_ids {
        let alive = storage
            .is_execution_manager_alive(*em_id)
            .await
            .expect("is_execution_manager_alive should succeed");
        assert!(!alive, "EM {em_id:?} should be dead");
        assert!(
            dead.contains(em_id),
            "expected em_id {em_id:?} in dead list, got {dead:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
#[serial_test::file_serial]
async fn test_liveness_operations_no_deadlock_under_concurrency() {
    // Regression test for <https://github.com/y-scope/spider/issues/371>.
    const TEST_NUM_CONCURRENT_EMS: usize = 16;
    const TEST_NUM_DEADLOCK_ITERATIONS: usize = 10;
    const TEST_STALE_AFTER_SEC: u64 = 1;

    let mut config = create_mariadb_config();
    config.max_connections =
        u32::try_from(TEST_NUM_CONCURRENT_EMS).expect("EM count should fit in u32") + 4;
    let storage = MariaDbStorageConnector::connect(&config)
        .await
        .expect("connect should succeed");
    let cutoff_with_margin = Duration::from_secs(TEST_STALE_AFTER_SEC) + Duration::from_millis(100);

    for iteration in 0..TEST_NUM_DEADLOCK_ITERATIONS {
        let mut em_ids = Vec::with_capacity(TEST_NUM_CONCURRENT_EMS);
        for _ in 0..TEST_NUM_CONCURRENT_EMS {
            em_ids.push(register_test_em(&storage).await);
        }

        tokio::time::sleep(cutoff_with_margin).await;

        // Race every heartbeat against the single get_dead on one join set. The get_dead result is
        // mapped to `()` so all tasks share the `Result<(), DbError>` type and a single outcome
        // check: a deadlock or any other unexpected failure surfaces as an `Err` other than
        // `ExecutionManagerAlreadyDead`.
        let mut join_set: JoinSet<Result<(), DbError>> = JoinSet::new();
        for em_id in em_ids {
            let storage = storage.clone();
            join_set.spawn(async move { storage.update_execution_manager_heartbeat(em_id).await });
        }

        let storage = storage.clone();
        join_set.spawn(async move {
            storage
                .get_dead_execution_managers(TEST_STALE_AFTER_SEC)
                .await
                .map(|_| ())
        });

        while let Some(joined) = join_set.join_next().await {
            let result = joined.expect("task should not panic");
            assert!(
                matches!(
                    result,
                    Ok(()) | Err(DbError::ExecutionManagerAlreadyDead(_))
                ),
                "iteration {iteration}: liveness operation should succeed or find the EM already \
                 dead, got {result:?}"
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires MariaDB"]
#[serial_test::file_serial]
async fn test_register_scheduler_replaces_previous_scheduler() {
    let storage = create_mariadb_connector().await;
    let scheduler_ip_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let updated_scheduler_ip_address = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));

    let first_scheduler_id = storage
        .register_scheduler(scheduler_ip_address, TEST_SCHEDULER_PORT)
        .await
        .expect("first register_scheduler should succeed");
    let second_scheduler_id = storage
        .register_scheduler(updated_scheduler_ip_address, TEST_UPDATED_SCHEDULER_PORT)
        .await
        .expect("second register_scheduler should succeed");
    let schedulers = storage
        .get_schedulers()
        .await
        .expect("get_schedulers should succeed");

    assert_ne!(
        first_scheduler_id, second_scheduler_id,
        "new registration should allocate a fresh scheduler ID"
    );
    assert!(
        !is_scheduler_registered(&storage, first_scheduler_id).await,
        "old scheduler should be removed after a new registration"
    );
    assert!(
        is_scheduler_registered(&storage, second_scheduler_id).await,
        "new scheduler should remain registered"
    );
    assert_eq!(
        schedulers.len(),
        1,
        "only the latest scheduler should remain"
    );
    assert_eq!(schedulers[0].id, second_scheduler_id);
    assert_eq!(schedulers[0].ip_address, updated_scheduler_ip_address);
    assert_eq!(schedulers[0].port, TEST_UPDATED_SCHEDULER_PORT);
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_session_id_returned_after_connect() {
    let storage = create_mariadb_connector().await;
    let session_id = storage.session_id();
    assert!(
        session_id > 0,
        "session_id should be greater than 0 after connect, got {session_id}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn test_session_id_bumps_on_reconnect() {
    let storage1 = create_mariadb_connector().await;
    let session_id1 = storage1.session_id();

    let storage2 = create_mariadb_connector().await;
    let session_id2 = storage2.session_id();

    assert!(
        session_id2 > session_id1,
        "session_id should increase on reconnect, got {session_id1} -> {session_id2}"
    );
}
