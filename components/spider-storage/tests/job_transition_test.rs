//! Job transitions on the tree committer through the storage service on `MariaDB`.

use std::time::Duration;

use spider_core::job::JobState;
use spider_core::task::DataTypeDescriptor;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TaskDescriptor;
use spider_core::task::TaskIndex;
use spider_core::task::TaskState;
use spider_core::task::TdlContext;
use spider_core::task::TerminationTaskDescriptor;
use spider_core::task::TimeoutPolicy;
use spider_core::task::ValueTypeDescriptor;
use spider_core::types::id::JobId;
use spider_core::types::id::TaskId;
use spider_core::types::io::TaskGraphInput;
use spider_core::types::io::TaskGraphInputBuilder;
use spider_core::types::io::TaskOutputsSerializer;
use spider_storage::cache::error::CacheError;
use spider_storage::cache::error::StaleStateError;
use spider_storage::limits::MAX_ERROR_MESSAGE_CHARS;
use spider_storage::state::StorageServerError;
use spider_storage::task_instance_pool::TaskInstancePoolConfig;
use spider_storage::test_hooks::HookAction;
use spider_storage::test_hooks::HookCount;
use spider_storage::test_hooks::HookFilter;
use spider_storage::test_hooks::HookPoint;

use crate::runtime_infra::TestEnv;
use crate::task_graph_builder::SubmittedTaskGraph;

/// How long a test waits for a request to reach a paused hook, or for a request that must not wait
/// on a paused hook to return.
const HOOK_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn outputs_over_the_packet_size_fail_the_job() -> anyhow::Result<()> {
    const OUTPUT_SIZE: usize = 2 * 1024 * 1024;

    let env = TestEnv::start().await;
    let max_allowed_packet: u64 = sqlx::query_scalar("SELECT @@max_allowed_packet;")
        .fetch_one(&env.pool)
        .await?;
    let num_tasks = usize::try_from(max_allowed_packet)?.div_ceil(OUTPUT_SIZE) + 1;
    let job_id = start_independent_job(&env, num_tasks, ExecutionPolicy::default(), false).await;
    let mut state = JobState::Running;
    for task_index in 0..num_tasks {
        let task_instance_id = env.create_instance(job_id, TaskId::Index(task_index)).await;
        state = env
            .service
            .succeed_task_instance(
                env.service.session_id(),
                job_id,
                task_instance_id,
                task_index,
                serialized_output(incompressible(OUTPUT_SIZE, u64::try_from(task_index)?)),
            )
            .await?;
    }

    assert_eq!(state, JobState::Failed);
    env.assert_job_state(job_id, JobState::Failed).await;
    let error = env.service.get_job_error(job_id).await?;
    assert!(
        error.starts_with("job outputs of") && error.contains("serialized bytes exceed the limit"),
        "unexpected error `{error}`"
    );
    assert!(!has_outputs(&env, job_id).await);
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_failure_message_over_the_packet_size_is_truncated() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let job_id = start_independent_job(&env, 1, ExecutionPolicy::default(), false).await;
    let max_allowed_packet: u64 = sqlx::query_scalar("SELECT @@max_allowed_packet;")
        .fetch_one(&env.pool)
        .await?;
    let message = "x".repeat(usize::try_from(max_allowed_packet)? + 1);

    let task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    let state = env
        .service
        .fail_task_instance(
            env.service.session_id(),
            job_id,
            task_instance_id,
            TaskId::Index(0),
            message.clone(),
        )
        .await?;

    assert_eq!(state, JobState::Failed);
    env.assert_job_state(job_id, JobState::Failed).await;
    assert_eq!(
        env.service.get_job_error(job_id).await?,
        message[..MAX_ERROR_MESSAGE_CHARS]
    );
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_straggler_failure_in_the_committers_write_window_is_stale() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let job_id = env.start_chain_job(1, 2).await;
    let first_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    let straggler_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    let pause = env.hooks.arm(
        HookPoint::CommitterRoundAfterDbWrite,
        HookFilter::job(job_id),
        HookAction::Pause,
        HookCount::Once,
    );

    let service = env.service.clone();
    let success = tokio::spawn(async move {
        service
            .succeed_task_instance(
                service.session_id(),
                job_id,
                first_instance_id,
                0,
                serialized_output(vec![1; 4]),
            )
            .await
    });
    pause.reached(1, HOOK_TIMEOUT).await.expect(
        "the hook should be reached within the timeout; the request may have failed before \
         reaching the hook point",
    );
    assert_eq!(env.row_state(job_id).await, JobState::Succeeded);
    assert_eq!(env.service.get_job_state(job_id).await?, JobState::Running);

    let straggler_failure = tokio::time::timeout(
        HOOK_TIMEOUT,
        env.service.fail_task_instance(
            env.service.session_id(),
            job_id,
            straggler_instance_id,
            TaskId::Index(0),
            "straggler".to_owned(),
        ),
    )
    .await
    .expect("the straggler's failure should not wait for the paused round");
    assert!(
        matches!(
            straggler_failure,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::TaskAlreadyTerminated(TaskState::Succeeded)
            )))
        ),
        "a straggler failure in the committer's write window should be stale, got \
         {straggler_failure:?}"
    );
    assert_eq!(env.service.get_job_state(job_id).await?, JobState::Running);
    assert_eq!(env.row_state(job_id).await, JobState::Succeeded);

    pause.release();
    assert_eq!(success.await??, JobState::Succeeded);
    env.assert_job_state(job_id, JobState::Succeeded).await;
    drop(pause);
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_terminal_failure_decided_before_a_cancel_is_stale_and_the_cancel_stands()
-> anyhow::Result<()> {
    let env = TestEnv::start().await;

    let job_id = start_independent_job(&env, 1, ExecutionPolicy::default(), false).await;
    let failure = race_terminal_failure_with_cancel(&env, job_id, JobState::Cancelled).await;
    assert!(
        matches!(
            failure,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::JobAlreadyTerminated(JobState::Cancelled)
            )))
        ),
        "a failure decided before the cancel should be stale, got {failure:?}"
    );
    env.assert_job_state(job_id, JobState::Cancelled).await;

    let job_id = start_independent_job(&env, 1, ExecutionPolicy::default(), true).await;
    let failure = race_terminal_failure_with_cancel(&env, job_id, JobState::CleanupReady).await;
    assert!(
        matches!(
            failure,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::JobNoLongerRunning
            )))
        ),
        "a failure decided before the cancel should be stale, got {failure:?}"
    );
    env.assert_job_state(job_id, JobState::CleanupReady).await;
    let cleanup_entries = env
        .service
        .poll_cleanup_ready_tasks(16, Duration::from_secs(1))
        .await?;
    assert_eq!(
        cleanup_entries
            .iter()
            .filter(|entry| entry.job_id == job_id)
            .count(),
        1,
        "the cancel should publish the job's cleanup task"
    );

    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_job_failure_cancels_the_jobs_other_running_tasks() -> anyhow::Result<()> {
    let env = TestEnv::start_with_task_instance_pool(TaskInstancePoolConfig {
        gc_interval_sec: 1,
        ..TaskInstancePoolConfig::default()
    })
    .await;
    let job_id = start_independent_job(
        &env,
        2,
        ExecutionPolicy {
            timeout_policy: TimeoutPolicy {
                soft_timeout_ms: 100,
                hard_timeout_ms: 60_000,
            },
            ..ExecutionPolicy::default()
        },
        false,
    )
    .await;
    let failed_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    let running_instance_id = env.create_instance(job_id, TaskId::Index(1)).await;

    let state = env
        .service
        .fail_task_instance(
            env.service.session_id(),
            job_id,
            failed_instance_id,
            TaskId::Index(0),
            "failure".to_owned(),
        )
        .await?;
    assert_eq!(state, JobState::Failed);

    let entries = env
        .service
        .poll_ready_tasks(16, Duration::from_secs(3))
        .await?;
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.job_id == job_id)
            .map(|entry| entry.task_kind)
            .collect::<Vec<_>>(),
        Vec::<TaskIndex>::new(),
        "the failed job's other task should not be re-enqueued"
    );

    let success = env
        .service
        .succeed_task_instance(
            env.service.session_id(),
            job_id,
            running_instance_id,
            1,
            serialized_output(vec![1; 4]),
        )
        .await;
    assert!(
        matches!(
            success,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::JobNoLongerRunning
            )))
        ),
        "a success of the failed job's other task should be stale, got {success:?}"
    );
    env.assert_job_state(job_id, JobState::Failed).await;
    env.finish().await;
    Ok(())
}

/// Builds a job of `num_tasks` independent tasks under `execution_policy`, each taking and
/// returning one byte-typed value, with a cleanup task if `with_cleanup`.
///
/// # Returns
///
/// The task graph and its inputs.
///
/// # Panics
///
/// Panics if the graph fails to build.
fn build_independent_job(
    num_tasks: usize,
    execution_policy: ExecutionPolicy,
    with_cleanup: bool,
) -> (SubmittedTaskGraph, TaskGraphInput) {
    let execution_policy = Some(execution_policy);
    let cleanup_task = with_cleanup.then(|| TerminationTaskDescriptor {
        tdl_context: TdlContext {
            package: "test".to_owned(),
            task_func: "cleanup".to_owned(),
        },
        execution_policy: execution_policy.clone(),
    });
    let mut graph =
        SubmittedTaskGraph::new(None, cleanup_task).expect("task graph creation should succeed");
    let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
    let mut inputs = TaskGraphInputBuilder::new();
    for task_index in 0..num_tasks {
        graph
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: "test".to_owned(),
                    task_func: format!("task{task_index}"),
                },
                execution_policy: execution_policy.clone(),
                inputs: vec![bytes_type.clone()],
                outputs: vec![bytes_type.clone()],
                input_sources: None,
            })
            .expect("task insertion should succeed");
        inputs
            .append_task_input(&[0u8; 4])
            .expect("task input appending should succeed");
    }
    (graph, inputs.build())
}

/// Registers and starts a job of [`build_independent_job`].
///
/// # Returns
///
/// The job ID.
///
/// # Panics
///
/// Panics if the registration or the start fails, or if the job's tasks are not published.
async fn start_independent_job(
    env: &TestEnv,
    num_tasks: usize,
    execution_policy: ExecutionPolicy,
    with_cleanup: bool,
) -> JobId {
    let (graph, inputs) = build_independent_job(num_tasks, execution_policy, with_cleanup);
    let job_id = env.register_job(&graph, &inputs).await;
    env.service
        .start_job(job_id)
        .await
        .expect("the job should start");
    let mut task_indices = env.poll_ready_tasks(job_id, num_tasks).await;
    task_indices.sort_unstable();
    assert_eq!(task_indices, (0..num_tasks).collect::<Vec<_>>());
    job_id
}

/// Fails the only task of `job_id`, which has no retry left, cancels the job while the failure is
/// paused before its submit, and then lets the failure go on.
///
/// # Returns
///
/// The failure's result.
///
/// # Panics
///
/// Panics if the failure does not reach its hook or panics, or if the cancel does not move the job
/// to `cancelled_state`.
async fn race_terminal_failure_with_cancel(
    env: &TestEnv,
    job_id: JobId,
    cancelled_state: JobState,
) -> Result<JobState, StorageServerError> {
    let task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    let pause = env.hooks.arm(
        HookPoint::TaskFailureBeforeSubmit,
        HookFilter::job(job_id),
        HookAction::Pause,
        HookCount::Once,
    );

    let service = env.service.clone();
    let failure = tokio::spawn(async move {
        service
            .fail_task_instance(
                service.session_id(),
                job_id,
                task_instance_id,
                TaskId::Index(0),
                "failure".to_owned(),
            )
            .await
    });
    pause.reached(1, HOOK_TIMEOUT).await.expect(
        "the hook should be reached within the timeout; the request may have failed before \
         reaching the hook point",
    );
    assert_eq!(
        env.service
            .cancel_job(job_id)
            .await
            .expect("the cancel should succeed"),
        cancelled_state
    );

    pause.release();
    let result = failure.await.expect("the failure's task should not panic");
    drop(pause);
    result
}

/// # Returns
///
/// `size` pseudo-random bytes from `seed`.
fn incompressible(size: usize, seed: u64) -> Vec<u8> {
    let mut xorshift_state = 0x9e37_79b9_7f4a_7c15_u64 ^ seed;
    (0..size)
        .map(|_| {
            xorshift_state ^= xorshift_state << 13;
            xorshift_state ^= xorshift_state >> 7;
            xorshift_state ^= xorshift_state << 17;
            xorshift_state.to_le_bytes()[0]
        })
        .collect()
}

/// # Returns
///
/// The serialized outputs of a task whose only output is `value`.
///
/// # Panics
///
/// Panics if the serialization fails.
fn serialized_output(value: Vec<u8>) -> Vec<u8> {
    TaskOutputsSerializer::from_tuple(&(value,)).expect("output serialization should succeed")
}

/// # Returns
///
/// Whether `job_id`'s row in the database has job outputs.
///
/// # Panics
///
/// Panics if the query fails.
async fn has_outputs(env: &TestEnv, job_id: JobId) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT `serialized_job_outputs` IS NOT NULL FROM `jobs` WHERE `id` = ?;",
    )
    .bind(job_id)
    .fetch_one(&env.pool)
    .await
    .expect("the outputs query should succeed")
}
