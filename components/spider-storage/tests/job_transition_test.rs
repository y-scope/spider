//! Job transitions on the tree committer through the storage service on `MariaDB`.

use std::time::Duration;

use spider_core::job::JobState;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TaskIndex;
use spider_core::task::TaskState;
use spider_core::task::TimeoutPolicy;
use spider_core::types::id::TaskId;
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

use crate::runtime_infra::HOOK_TIMEOUT;
use crate::runtime_infra::TestEnv;

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn outputs_over_the_packet_size_fail_the_job() -> anyhow::Result<()> {
    const OUTPUT_SIZE: usize = 2 * 1024 * 1024;

    let env = TestEnv::start().await;
    let max_allowed_packet: u64 = sqlx::query_scalar("SELECT @@max_allowed_packet;")
        .fetch_one(&env.pool)
        .await?;
    let num_tasks = usize::try_from(max_allowed_packet)?.div_ceil(OUTPUT_SIZE) + 1;
    let job_id = env
        .start_independent_job(num_tasks, ExecutionPolicy::default(), false)
        .await;
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
    assert!(!env.has_outputs(job_id).await);
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_failure_message_over_the_packet_size_is_truncated() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let job_id = env
        .start_independent_job(1, ExecutionPolicy::default(), false)
        .await;
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

    let job_id = env
        .start_independent_job(1, ExecutionPolicy::default(), false)
        .await;
    let failure = env
        .race_terminal_failure_with_cancel(job_id, JobState::Cancelled)
        .await;
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

    let job_id = env
        .start_independent_job(1, ExecutionPolicy::default(), true)
        .await;
    let failure = env
        .race_terminal_failure_with_cancel(job_id, JobState::CleanupReady)
        .await;
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
    let job_id = env
        .start_independent_job(
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
