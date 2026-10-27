//! Storage service handlers whose callers drop them partway through, on `MariaDB`.

use std::time::Duration;

use spider_core::job::JobState;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TimeoutPolicy;
use spider_core::types::id::TaskId;
use spider_core::types::io::TaskOutputsSerializer;
use spider_storage::task_instance_pool::TaskInstancePoolConfig;
use spider_storage::test_hooks::HookAction;
use spider_storage::test_hooks::HookCount;
use spider_storage::test_hooks::HookFilter;
use spider_storage::test_hooks::HookPoint;

use crate::runtime_infra::TestEnv;
use crate::runtime_infra::build_chain_task_graph;

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn dropped_report_after_the_tcb_flip_still_finishes_the_job() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let job_id = env.start_chain_job(1, 1).await;
    let instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    let pause = env.hooks.arm(
        HookPoint::TaskSuccessAfterTcbFlip,
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
    env.wait_for_hook(&pause, 1).await;
    report.abort();
    assert!(report.await.is_err_and(|e| e.is_cancelled()));
    pause.release();
    env.wait_for_job_state(job_id, JobState::Succeeded).await;

    env.assert_job_state(job_id, JobState::Succeeded).await;
    drop(pause);
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn dropped_report_at_the_successor_lock_publishes_the_successor_once() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let job_id = env.start_chain_job(2, 1).await;
    let instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    let pause = env.hooks.arm(
        HookPoint::TaskSuccessBeforeSuccessorLock,
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
    env.wait_for_hook(&pause, 1).await;
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

    let instance_id = env.create_instance(job_id, TaskId::Index(1)).await;
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
    env.assert_job_state(job_id, JobState::Succeeded).await;
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
    let job_id = env.start_job(&graph, &inputs).await;
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
    env.wait_for_hook(&pause, 1).await;
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
    env.assert_job_state(job_id, JobState::Succeeded).await;
    drop(pause);
    env.finish().await;
    Ok(())
}

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
