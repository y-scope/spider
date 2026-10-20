//! Plain jobs on the tree committer through the storage service on `MariaDB`: stale starts,
//! dropped handlers, straggler failures and restarts.

use std::time::Duration;

use spider_core::job::JobState;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TaskState;
use spider_core::task::TimeoutPolicy;
use spider_core::types::id::JobId;
use spider_core::types::id::TaskId;
use spider_core::types::io::TaskGraphInput;
use spider_core::types::io::TaskOutputsSerializer;
use spider_storage::cache::error::CacheError;
use spider_storage::cache::error::StaleStateError;
use spider_storage::state::StorageServerError;
use spider_storage::task_instance_pool::TaskInstancePoolConfig;
use spider_storage::test_hooks::EventKindTag;
use spider_storage::test_hooks::HookAction;
use spider_storage::test_hooks::HookCount;
use spider_storage::test_hooks::HookFilter;
use spider_storage::test_hooks::HookPoint;

use super::job_tree_infra::TestEnv;
use super::job_tree_infra::build_chain_task_graph;
use super::task_graph_builder::SubmittedTaskGraph;
use super::task_graph_builder::build_flat_task_graph;

/// # Returns
///
/// The serialized outputs of a task whose only output is `value`.
///
/// # Panics
///
/// Panics if the serialization fails.
fn serialized_output(value: u8) -> Vec<u8> {
    TaskOutputsSerializer::from_tuple(&(vec![value; 4],))
        .expect("output serialization should succeed")
}

/// Registers a chain job of `num_tasks` tasks and starts it.
///
/// # Returns
///
/// The job ID.
///
/// # Panics
///
/// Panics if the registration or the start fails, or if the first task is not published.
async fn start_chain_job(env: &TestEnv, num_tasks: usize, max_num_instances: u32) -> JobId {
    let (graph, inputs) = build_chain_task_graph(
        num_tasks,
        ExecutionPolicy {
            max_num_instances,
            ..ExecutionPolicy::default()
        },
    );
    start_job(env, &graph, &inputs).await
}

/// Registers a job and starts it.
///
/// # Returns
///
/// The job ID.
///
/// # Panics
///
/// Panics if the registration or the start fails, or if exactly task 0 is not published.
async fn start_job(
    env: &TestEnv,
    task_graph: &SubmittedTaskGraph,
    task_graph_input: &TaskGraphInput,
) -> JobId {
    let job_id = env.register_job(task_graph, task_graph_input).await;
    env.service
        .start_job(job_id)
        .await
        .expect("the job should start");
    assert_eq!(env.poll_ready_tasks(job_id, 1).await, vec![0]);
    job_id
}

/// # Panics
///
/// Panics if the instance creation fails.
async fn create_instance(env: &TestEnv, job_id: JobId, task_id: TaskId) -> u64 {
    env.service
        .create_task_instance(
            env.service.session_id(),
            job_id,
            task_id,
            env.execution_manager_id,
        )
        .await
        .expect("the instance creation should succeed")
        .task_instance_id
}

/// Asserts that the job's cached and persisted states are both `expected`, and that the service
/// has not shut down fatally.
async fn assert_job_state(env: &TestEnv, job_id: JobId, expected: JobState) {
    assert_eq!(
        env.service
            .get_job_state(job_id)
            .await
            .expect("the job state should be readable"),
        expected
    );
    assert_eq!(env.row_state(job_id).await, expected);
    assert!(!env.is_fatal());
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn stale_start_job_is_stale_and_not_fatal() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let job_id = start_chain_job(&env, 1, 1).await;
    create_instance(&env, job_id, TaskId::Index(0)).await;

    assert_start_is_stale(&env, job_id).await;
    assert_job_state(&env, job_id, JobState::Running).await;

    let (graph, inputs) = build_flat_task_graph(1, 4, false, true);
    let job_id = env.register_job(&graph, &inputs).await;
    env.service.start_job(job_id).await?;
    assert_eq!(
        env.service.cancel_job(job_id).await?,
        JobState::CleanupReady
    );
    assert_start_is_stale(&env, job_id).await;
    assert_job_state(&env, job_id, JobState::CleanupReady).await;

    let job_id = start_chain_job(&env, 1, 1).await;
    assert_eq!(env.service.cancel_job(job_id).await?, JobState::Cancelled);
    assert_start_is_stale(&env, job_id).await;
    assert_job_state(&env, job_id, JobState::Cancelled).await;
    env.finish().await;
    Ok(())
}

/// Asserts that starting `job_id` again is answered as stale.
async fn assert_start_is_stale(env: &TestEnv, job_id: JobId) {
    let result = env.service.start_job(job_id).await;
    assert!(
        matches!(
            result,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::JobAlreadyStarted
            )))
        ),
        "a repeated start should be stale, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn dropped_report_after_the_tcb_flip_still_finishes_the_job() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let job_id = start_chain_job(&env, 1, 1).await;
    let instance_id = create_instance(&env, job_id, TaskId::Index(0)).await;
    let pause = env.hooks.arm(
        HookPoint::PhaseAAfterTcbFlip,
        HookFilter::job(job_id),
        HookAction::Pause,
        HookCount::Once,
    );
    let applied = env.hooks.arm(
        HookPoint::CommitterAfterApplyJob,
        HookFilter::job(job_id),
        HookAction::Observe,
        HookCount::Always,
    );

    let service = env.service.clone();
    let report = tokio::spawn(async move {
        service
            .succeed_task_instance(
                service.session_id(),
                job_id,
                instance_id,
                0,
                serialized_output(1),
            )
            .await
    });
    pause.reached(1).await;
    report.abort();
    assert!(report.await.is_err_and(|e| e.is_cancelled()));
    pause.release();
    tokio::time::timeout(Duration::from_secs(10), applied.reached(1)).await?;

    assert_job_state(&env, job_id, JobState::Succeeded).await;
    drop((pause, applied));
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn dropped_report_at_the_successor_lock_publishes_the_successor_once() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let job_id = start_chain_job(&env, 2, 1).await;
    let instance_id = create_instance(&env, job_id, TaskId::Index(0)).await;
    let pause = env.hooks.arm(
        HookPoint::PhaseABeforeSuccessorLock,
        HookFilter::job(job_id),
        HookAction::Pause,
        HookCount::Once,
    );

    let service = env.service.clone();
    let report = tokio::spawn(async move {
        service
            .succeed_task_instance(
                service.session_id(),
                job_id,
                instance_id,
                0,
                serialized_output(1),
            )
            .await
    });
    pause.reached(1).await;
    report.abort();
    assert!(report.await.is_err_and(|e| e.is_cancelled()));
    pause.release();

    assert_eq!(env.poll_ready_tasks(job_id, 1).await, vec![1]);
    let extra = env
        .service
        .poll_ready_tasks(16, Duration::from_millis(500))
        .await?;
    assert_eq!(
        extra.iter().filter(|entry| entry.job_id == job_id).count(),
        0,
        "the successor should be published exactly once"
    );

    let instance_id = create_instance(&env, job_id, TaskId::Index(1)).await;
    let state = env
        .service
        .succeed_task_instance(
            env.service.session_id(),
            job_id,
            instance_id,
            1,
            serialized_output(2),
        )
        .await?;
    assert_eq!(state, JobState::Succeeded);
    assert_job_state(&env, job_id, JobState::Succeeded).await;
    assert_eq!(
        env.service.get_job_outputs(job_id).await?,
        TaskOutputsSerializer::deserialize(&serialized_output(2))?
    );
    drop(pause);
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn dropped_instance_creation_still_hands_the_instance_to_the_pool() -> anyhow::Result<()> {
    let env = TestEnv::start_with_task_instance_pool(TaskInstancePoolConfig {
        gc_interval_sec: 1,
        ..TaskInstancePoolConfig::default()
    })
    .await;
    let (graph, inputs) = build_chain_task_graph(
        1,
        ExecutionPolicy {
            timeout_policy: TimeoutPolicy {
                soft_timeout_ms: 100,
                hard_timeout_ms: 60_000,
            },
            ..ExecutionPolicy::default()
        },
    );
    let job_id = start_job(&env, &graph, &inputs).await;
    let pause = env.hooks.arm(
        HookPoint::CreateInstanceBeforePoolSend,
        HookFilter::job(job_id),
        HookAction::Pause,
        HookCount::Once,
    );

    let service = env.service.clone();
    let execution_manager_id = env.execution_manager_id;
    let creation = tokio::spawn(async move {
        service
            .create_task_instance(
                service.session_id(),
                job_id,
                TaskId::Index(0),
                execution_manager_id,
            )
            .await
    });
    pause.reached(1).await;
    let instance_id = pause
        .last_context()
        .and_then(|context| context.task_instance_id)
        .expect("the hook context should carry the instance ID");
    creation.abort();
    assert!(creation.await.is_err_and(|e| e.is_cancelled()));
    pause.release();

    assert_eq!(
        env.poll_ready_tasks(job_id, 1).await,
        vec![0],
        "the pool should re-enqueue the task once the instance's soft timeout passes"
    );
    let state = env
        .service
        .succeed_task_instance(
            env.service.session_id(),
            job_id,
            instance_id,
            0,
            serialized_output(1),
        )
        .await?;
    assert_eq!(state, JobState::Succeeded);
    assert_job_state(&env, job_id, JobState::Succeeded).await;
    drop(pause);
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn straggler_failure_after_a_success_is_stale() -> anyhow::Result<()> {
    let env = TestEnv::start().await;

    assert_straggler_in_window_is_stale(&env, HookPoint::CommitterBeforeDbTransaction).await?;
    assert_straggler_in_window_is_stale(&env, HookPoint::CommitterAfterCommit).await?;

    let job_id = start_chain_job(&env, 1, 2).await;
    let first = create_instance(&env, job_id, TaskId::Index(0)).await;
    let straggler = create_instance(&env, job_id, TaskId::Index(0)).await;
    let state = env
        .service
        .succeed_task_instance(
            env.service.session_id(),
            job_id,
            first,
            0,
            serialized_output(1),
        )
        .await?;
    assert_eq!(state, JobState::Succeeded);
    let after_apply = env
        .service
        .fail_task_instance(
            env.service.session_id(),
            job_id,
            straggler,
            TaskId::Index(0),
            "straggler".to_owned(),
        )
        .await;
    assert!(
        matches!(
            after_apply,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::JobNoLongerRunning
            )))
        ),
        "a straggler failure after the apply should be stale, got {after_apply:?}"
    );
    assert_job_state(&env, job_id, JobState::Succeeded).await;
    env.finish().await;
    Ok(())
}

/// Parks the success of a one-task job's only task at `point` in the committer, fails a second
/// instance of the task in that window, and asserts that the failure is stale, that it changes
/// neither the cached nor the persisted state, and that the job then succeeds.
async fn assert_straggler_in_window_is_stale(
    env: &TestEnv,
    point: HookPoint,
) -> anyhow::Result<()> {
    let job_id = start_chain_job(env, 1, 2).await;
    let first = create_instance(env, job_id, TaskId::Index(0)).await;
    let straggler = create_instance(env, job_id, TaskId::Index(0)).await;
    let pause = env.hooks.arm(
        point,
        HookFilter {
            event_kind: Some(EventKindTag::RegularTasksDone),
            ..HookFilter::job(job_id)
        },
        HookAction::Pause,
        HookCount::Once,
    );
    let service = env.service.clone();
    let report = tokio::spawn(async move {
        service
            .succeed_task_instance(service.session_id(), job_id, first, 0, serialized_output(1))
            .await
    });
    pause.reached(1).await;
    let row_state_in_window = if point == HookPoint::CommitterAfterCommit {
        JobState::Succeeded
    } else {
        JobState::Running
    };
    assert_eq!(env.row_state(job_id).await, row_state_in_window);

    let in_window = env
        .service
        .fail_task_instance(
            env.service.session_id(),
            job_id,
            straggler,
            TaskId::Index(0),
            "straggler".to_owned(),
        )
        .await;
    assert!(
        matches!(
            in_window,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::TaskAlreadyTerminated(TaskState::Succeeded)
            )))
        ),
        "a straggler failure at {point:?} should be stale, got {in_window:?}"
    );
    assert_eq!(env.service.get_job_state(job_id).await?, JobState::Running);
    assert_eq!(env.row_state(job_id).await, row_state_in_window);

    pause.release();
    assert_eq!(report.await??, JobState::Succeeded);
    assert_job_state(env, job_id, JobState::Succeeded).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn cancel_after_a_restart_of_a_commit_ready_job_runs_its_cleanup() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let (graph, inputs) = build_flat_task_graph(1, 4, true, true);
    let job_id = start_job(&env, &graph, &inputs).await;
    let instance_id = create_instance(&env, job_id, TaskId::Index(0)).await;
    let state = env
        .service
        .succeed_task_instance(
            env.service.session_id(),
            job_id,
            instance_id,
            0,
            serialized_output(1),
        )
        .await?;
    assert_eq!(state, JobState::CommitReady);

    let env = env.restart().await;
    assert_eq!(
        env.service.cancel_job(job_id).await?,
        JobState::CleanupReady
    );
    assert_job_state(&env, job_id, JobState::CleanupReady).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let entries = env
                .service
                .poll_cleanup_ready_tasks(16, Duration::from_secs(1))
                .await
                .expect("polling should succeed");
            if entries.iter().any(|entry| entry.job_id == job_id) {
                break;
            }
        }
    })
    .await?;

    let cleanup_instance_id = create_instance(&env, job_id, TaskId::Cleanup).await;
    let state = env
        .service
        .succeed_cleanup_task_instance(env.service.session_id(), job_id, cleanup_instance_id)
        .await?;
    assert_eq!(state, JobState::Cancelled);
    assert_job_state(&env, job_id, JobState::Cancelled).await;
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn restart_recovers_a_running_job_and_republishes_its_ready_task() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let job_id = start_chain_job(&env, 1, 1).await;

    let env = env.restart().await;
    assert_eq!(env.poll_ready_tasks(job_id, 1).await, vec![0]);
    let instance_id = create_instance(&env, job_id, TaskId::Index(0)).await;
    let state = env
        .service
        .succeed_task_instance(
            env.service.session_id(),
            job_id,
            instance_id,
            0,
            serialized_output(1),
        )
        .await?;
    assert_eq!(state, JobState::Succeeded);
    assert_job_state(&env, job_id, JobState::Succeeded).await;
    env.finish().await;
    Ok(())
}
