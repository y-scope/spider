//! Hooked unit tests for the storage service state.

use std::fmt::Debug;
use std::time::Duration;

use spider_core::job::JobState;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TaskState;
use spider_core::task::TdlContext;
use spider_core::task::TerminationTaskDescriptor;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;

use crate::cache::error::CacheError;
use crate::cache::error::StaleStateError;
use crate::job_submission::compress_job_inputs;
use crate::job_submission::compress_task_graph;
use crate::state::StorageServerError;
use crate::state::service::test_utils::TEST_SESSION_ID;
use crate::state::service::test_utils::TestServiceState;
use crate::state::service::test_utils::create_test_job_submission;
use crate::state::service::test_utils::create_test_serialized_outputs;
use crate::state::service::test_utils::create_test_service;
use crate::state::service::test_utils::create_test_service_with_db;
use crate::state::test_utils::MockDbConnector;
use crate::state::test_utils::create_test_task_graph_input;
use crate::state::test_utils::create_test_task_graph_with_termination_tasks;
use crate::test_hooks::HookAction;
use crate::test_hooks::HookCount;
use crate::test_hooks::HookFilter;
use crate::test_hooks::HookPoint;

/// How long a test waits for a request to reach a paused hook.
const HOOK_TIMEOUT: Duration = Duration::from_secs(10);

/// The hook actions that make a handler body fail or panic.
const FAULT_ACTIONS: [HookAction; 2] = [HookAction::Crash, HookAction::Panic];

/// Registers and starts a one-task job in `service`.
///
/// # Returns
///
/// The ID of the job.
///
/// # Panics
///
/// Panics if the registration or the start fails.
async fn register_and_start_job(service: &TestServiceState) -> JobId {
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await
        .expect("the registration should succeed");
    service
        .start_job(job_id)
        .await
        .expect("the start should succeed");
    job_id
}

/// Registers and starts a one-task job in `service`, and creates an instance of its task.
///
/// # Returns
///
/// A pair, containing:
///
/// * The ID of the job.
/// * The ID of the task instance.
///
/// # Panics
///
/// Panics if any step fails.
async fn start_job_with_instance(service: &TestServiceState) -> (JobId, TaskInstanceId) {
    let job_id = register_and_start_job(service).await;
    let context = service
        .create_task_instance(
            TEST_SESSION_ID,
            job_id,
            TaskId::Index(0),
            ExecutionManagerId::random(),
        )
        .await
        .expect("the instance creation should succeed");
    (job_id, context.task_instance_id)
}

/// Registers and starts a one-task job with a commit task and a cleanup task in `service`,
/// drives the job until `task_id` is ready, and creates an instance of `task_id`.
///
/// # Returns
///
/// A pair, containing:
///
/// * The ID of the job.
/// * The ID of the task instance.
///
/// # Panics
///
/// Panics if any step fails.
async fn start_job_with_instance_of(
    service: &TestServiceState,
    task_id: TaskId,
) -> (JobId, TaskInstanceId) {
    let create_termination_task_descriptor = |task_func: &str| TerminationTaskDescriptor {
        tdl_context: TdlContext {
            package: "test_pkg".to_owned(),
            task_func: task_func.to_owned(),
        },
        execution_policy: Some(ExecutionPolicy::default()),
    };
    let task_graph = create_test_task_graph_with_termination_tasks(
        Some(create_termination_task_descriptor("commit_fn")),
        Some(create_termination_task_descriptor("cleanup_fn")),
    );
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compress_task_graph(&task_graph),
            compress_job_inputs(&create_test_task_graph_input()),
        )
        .await
        .expect("the registration should succeed");
    service
        .start_job(job_id)
        .await
        .expect("the start should succeed");
    match task_id {
        TaskId::Index(_) => {}
        TaskId::Commit => {
            let context = service
                .create_task_instance(
                    TEST_SESSION_ID,
                    job_id,
                    TaskId::Index(0),
                    ExecutionManagerId::random(),
                )
                .await
                .expect("the instance creation should succeed");
            let state = succeed(service, job_id, context.task_instance_id)
                .await
                .expect("the success should succeed");
            assert_eq!(state, JobState::CommitReady);
        }
        TaskId::Cleanup => {
            let state = service
                .cancel_job(job_id)
                .await
                .expect("the cancel should succeed");
            assert_eq!(state, JobState::CleanupReady);
        }
    }
    let context = service
        .create_task_instance(
            TEST_SESSION_ID,
            job_id,
            task_id,
            ExecutionManagerId::random(),
        )
        .await
        .expect("the instance creation should succeed");
    (job_id, context.task_instance_id)
}

/// Reports the success of `task_instance_id` of the only task of `job_id`.
///
/// # Returns
///
/// Forwards [`crate::state::service::ServiceState::succeed_task_instance`]'s return values.
async fn succeed(
    service: &TestServiceState,
    job_id: JobId,
    task_instance_id: TaskInstanceId,
) -> Result<JobState, StorageServerError> {
    service
        .succeed_task_instance(
            TEST_SESSION_ID,
            job_id,
            task_instance_id,
            0,
            create_test_serialized_outputs(),
        )
        .await
}

/// Waits until every task of the service's task tracker has exited.
///
/// # Panics
///
/// Panics if the tasks do not exit within [`HOOK_TIMEOUT`].
async fn wait_for_tasks(service: &TestServiceState) {
    service.inner.task_tracker.close();
    tokio::time::timeout(HOOK_TIMEOUT, service.inner.task_tracker.wait())
        .await
        .expect("every task of the task tracker should exit");
}

/// Asserts that `result` is an internal error, and that the service has shut down
/// fatally.
///
/// # Type Parameters
///
/// * `ValueType` - The value that `result` holds on success.
///
/// # Panics
///
/// Panics if `result` is not an internal error, or if the service has not shut down
/// fatally.
fn assert_shut_down_fatally<ValueType: Debug>(
    service: &TestServiceState,
    result: &Result<ValueType, StorageServerError>,
    context: &str,
) {
    assert!(
        matches!(
            result,
            Err(StorageServerError::Cache(CacheError::Internal(_)))
        ),
        "{context}: expected an internal error, got {result:?}"
    );
    assert!(
        service.inner.shutdown_handle.is_fatal(),
        "{context}: the shutdown should be fatal"
    );
    assert!(
        service.inner.shutdown_handle.is_cancelled(),
        "{context}: the service should be shutting down"
    );
}

#[tokio::test]
async fn a_fault_after_the_task_succeeded_shuts_down_fatally() {
    for (point, action) in [
        (HookPoint::TaskSuccessBeforeSuccessorLock, HookAction::Crash),
        (HookPoint::TaskSuccessAfterTcbFlip, HookAction::Crash),
        (HookPoint::TaskSuccessBeforeOutputsCommit, HookAction::Crash),
        (HookPoint::TaskSuccessAfterTcbFlip, HookAction::Panic),
    ] {
        let context = format!("{action:?} at {point:?}");
        let service = create_test_service();
        let (job_id, task_instance_id) = start_job_with_instance(&service).await;
        let hook = service
            .inner
            .hooks
            .arm(point, HookFilter::job(job_id), action, HookCount::Once);

        let result = succeed(&service, job_id, task_instance_id).await;

        assert_eq!(hook.hits(), 1, "{context}: the hook");
        assert_shut_down_fatally(&service, &result, &context);
    }
}

#[tokio::test]
async fn a_fault_before_the_pool_send_shuts_down_fatally() {
    for action in FAULT_ACTIONS {
        let context = format!("{action:?}");
        let service = create_test_service();
        let job_id = register_and_start_job(&service).await;
        let hook = service.inner.hooks.arm(
            HookPoint::CreateInstanceBeforePoolSend,
            HookFilter::job(job_id),
            action,
            HookCount::Once,
        );

        let result = service
            .create_task_instance(
                TEST_SESSION_ID,
                job_id,
                TaskId::Index(0),
                ExecutionManagerId::random(),
            )
            .await;

        assert_eq!(hook.hits(), 1, "{context}: the hook");
        assert_shut_down_fatally(&service, &result, &context);
    }
}

#[tokio::test]
async fn a_fault_after_the_task_failed_shuts_down_fatally() {
    for (task_id, action) in [TaskId::Index(0), TaskId::Commit, TaskId::Cleanup]
        .into_iter()
        .flat_map(|task_id| FAULT_ACTIONS.map(|action| (task_id, action)))
    {
        let context = format!("{action:?} of {task_id:?}");
        let service = create_test_service();
        let (job_id, task_instance_id) = start_job_with_instance_of(&service, task_id).await;
        let hook = service.inner.hooks.arm(
            HookPoint::TaskFailureBeforeSubmit,
            HookFilter::job(job_id),
            action,
            HookCount::Once,
        );

        let result = service
            .fail_task_instance(
                TEST_SESSION_ID,
                job_id,
                task_instance_id,
                task_id,
                "failure".to_owned(),
            )
            .await;

        assert_eq!(hook.hits(), 1, "{context}: the hook");
        assert_shut_down_fatally(&service, &result, &context);
    }
}

#[tokio::test]
async fn a_fault_after_the_registration_insert_shuts_down_fatally() {
    for action in FAULT_ACTIONS {
        let context = format!("{action:?}");
        let db = MockDbConnector::default();
        let service = create_test_service_with_db(db.clone());
        let hook = service.inner.hooks.arm(
            HookPoint::RegisterJobAfterInsert,
            HookFilter::default(),
            action,
            HookCount::Once,
        );
        let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
            create_test_job_submission();

        let result = service
            .register_job(
                ResourceGroupId::random(),
                compressed_serialized_task_graph,
                compressed_serialized_task_graph_input,
            )
            .await;

        assert_eq!(hook.hits(), 1, "{context}: the hook");
        let job_id = hook
            .last_context()
            .and_then(|hook_context| hook_context.job_ids.first().copied())
            .expect("the hook context should carry the job ID");
        assert_eq!(
            db.states.get(&job_id).map(|state| *state),
            Some(JobState::Ready),
            "{context}: the job's row"
        );
        assert!(
            service.inner.job_cache.get(job_id).await.is_none(),
            "{context}: the job should not be cached"
        );
        assert_shut_down_fatally(&service, &result, &context);
    }
}

#[tokio::test]
async fn a_failure_before_the_first_mutation_is_not_fatal() -> anyhow::Result<()> {
    let service = create_test_service();
    let (job_id, task_instance_id) = start_job_with_instance(&service).await;
    let unknown_instance_id = task_instance_id + 1;

    let result = succeed(&service, JobId::random(), task_instance_id).await;
    assert!(
        matches!(result, Err(StorageServerError::JobNotFound(_))),
        "a report of an unknown job should not find it, got {result:?}"
    );
    let result = succeed(&service, job_id, unknown_instance_id).await;
    assert!(
        matches!(
            result,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::InvalidTaskInstanceId
            )))
        ),
        "a success of an unknown instance should be stale, got {result:?}"
    );
    let result = service
        .fail_task_instance(
            TEST_SESSION_ID,
            job_id,
            unknown_instance_id,
            TaskId::Index(0),
            "unknown instance".to_owned(),
        )
        .await;
    assert!(
        matches!(
            result,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::InvalidTaskInstanceId
            )))
        ),
        "a failure of an unknown instance should be stale, got {result:?}"
    );

    let pause = service.inner.hooks.arm(
        HookPoint::TaskSuccessAfterTcbFlip,
        HookFilter::job(job_id),
        HookAction::Pause,
        HookCount::Once,
    );
    let report = tokio::spawn({
        let service = service.clone();
        async move { succeed(&service, job_id, task_instance_id).await }
    });
    pause.reached(1, HOOK_TIMEOUT).await?;
    let result = service
        .create_task_instance(
            TEST_SESSION_ID,
            job_id,
            TaskId::Index(0),
            ExecutionManagerId::random(),
        )
        .await;
    assert!(
        matches!(
            result,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::TaskAlreadyTerminated(TaskState::Succeeded)
            )))
        ),
        "an instance of a succeeded task should be stale, got {result:?}"
    );
    pause.release();
    assert_eq!(report.await??, JobState::Succeeded);

    let result = succeed(&service, job_id, task_instance_id).await;
    assert!(
        matches!(
            result,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::JobNoLongerRunning
            )))
        ),
        "a report after the job succeeded should be stale, got {result:?}"
    );
    assert!(!service.inner.shutdown_handle.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn a_stale_reply_after_the_submit_is_not_fatal() -> anyhow::Result<()> {
    let service = create_test_service();
    let (job_id, task_instance_id) = start_job_with_instance(&service).await;
    let pause = service.inner.hooks.arm(
        HookPoint::TaskSuccessBeforeOutputsCommit,
        HookFilter::job(job_id),
        HookAction::Pause,
        HookCount::Once,
    );
    let report = tokio::spawn({
        let service = service.clone();
        async move { succeed(&service, job_id, task_instance_id).await }
    });
    pause.reached(1, HOOK_TIMEOUT).await?;
    assert_eq!(service.cancel_job(job_id).await?, JobState::Cancelled);

    pause.release();
    let result = report.await?;

    assert!(
        matches!(
            result,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::JobAlreadyTerminated(JobState::Cancelled)
            )))
        ),
        "a completion after the cancel should be stale, got {result:?}"
    );
    assert!(!service.inner.shutdown_handle.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn a_failure_after_the_first_mutation_during_a_shutdown_is_not_fatal() -> anyhow::Result<()> {
    let service = create_test_service();
    let (job_id, task_instance_id) = start_job_with_instance(&service).await;
    let pause = service.inner.hooks.arm(
        HookPoint::TaskFailureBeforeSubmit,
        HookFilter::job(job_id),
        HookAction::Pause,
        HookCount::Once,
    );
    let failure = tokio::spawn({
        let service = service.clone();
        async move {
            service
                .fail_task_instance(
                    TEST_SESSION_ID,
                    job_id,
                    task_instance_id,
                    TaskId::Index(0),
                    "failure".to_owned(),
                )
                .await
        }
    });
    pause.reached(1, HOOK_TIMEOUT).await?;

    service.inner.shutdown_handle.shut_down_gracefully();
    let result = failure.await?;
    wait_for_tasks(&service).await;

    assert!(
        matches!(result, Err(StorageServerError::Stopping(_))),
        "a failure during the shutdown should be stopped, got {result:?}"
    );
    assert!(!service.inner.shutdown_handle.is_fatal());
    Ok(())
}

#[tokio::test]
async fn a_paused_report_stopped_by_a_shutdown_is_not_fatal() -> anyhow::Result<()> {
    let service = create_test_service();
    let (job_id, task_instance_id) = start_job_with_instance(&service).await;
    let pause = service.inner.hooks.arm(
        HookPoint::TaskSuccessBeforeOutputsCommit,
        HookFilter::job(job_id),
        HookAction::Pause,
        HookCount::Once,
    );
    let report = tokio::spawn({
        let service = service.clone();
        async move { succeed(&service, job_id, task_instance_id).await }
    });
    pause.reached(1, HOOK_TIMEOUT).await?;

    service.inner.shutdown_handle.shut_down_gracefully();
    let result = report.await?;
    wait_for_tasks(&service).await;

    assert!(
        matches!(result, Err(StorageServerError::Stopping(_))),
        "a paused report should be stopped by the shutdown, got {result:?}"
    );
    assert!(!service.inner.shutdown_handle.is_fatal());
    Ok(())
}
