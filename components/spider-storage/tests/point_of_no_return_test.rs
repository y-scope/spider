//! Storage service handler bodies around their first cache or database mutation, on `MariaDB`.

use spider_core::job::JobState;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TaskState;
use spider_core::types::id::JobId;
use spider_core::types::id::TaskId;
use spider_core::types::io::TaskOutputsSerializer;
use spider_storage::cache::error::CacheError;
use spider_storage::cache::error::StaleStateError;
use spider_storage::state::StorageServerError;
use spider_storage::test_hooks::HookAction;
use spider_storage::test_hooks::HookCount;
use spider_storage::test_hooks::HookFilter;
use spider_storage::test_hooks::HookPoint;

use crate::runtime_infra::TestEnv;
use crate::runtime_infra::build_chain_task_graph;

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_report_with_the_wrong_output_count_fails_its_task_instance() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let (graph, inputs) = build_chain_task_graph(
        1,
        ExecutionPolicy {
            max_num_retry: 1,
            ..ExecutionPolicy::default()
        },
    );
    let job_id = env.start_job(&graph, &inputs).await;
    let wrong_output_count = TaskOutputsSerializer::from_tuple(&(vec![1u8; 4], vec![2u8; 4]))?;

    let instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    let state = env
        .service
        .succeed_task_instance(
            env.service.session_id(),
            job_id,
            instance_id,
            0,
            wrong_output_count.clone(),
        )
        .await?;
    assert_eq!(
        state,
        JobState::Running,
        "the first report should be retried"
    );
    env.assert_job_state(job_id, JobState::Running).await;
    assert_eq!(env.poll_ready_tasks(job_id, 1).await, vec![0]);

    let instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    let state = env
        .service
        .succeed_task_instance(
            env.service.session_id(),
            job_id,
            instance_id,
            0,
            wrong_output_count,
        )
        .await?;
    assert_eq!(state, JobState::Failed, "the retry should fail the job");
    env.assert_job_state(job_id, JobState::Failed).await;
    let error = env.service.get_job_error(job_id).await?;
    assert!(
        error.contains("expected 1, got 2"),
        "the job's error should name the output counts, got {error:?}"
    );
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_fault_after_the_tcb_flip_is_fatal_and_a_restart_reruns_the_task() -> anyhow::Result<()> {
    let mut env = TestEnv::start().await;
    for point in [
        HookPoint::TaskSuccessAfterTcbFlip,
        HookPoint::TaskSuccessBeforeOutputsCommit,
    ] {
        let job_id = env.start_chain_job(1, 1).await;
        let instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
        let crash = env.hooks.arm(
            point,
            HookFilter::job(job_id),
            HookAction::Crash,
            HookCount::Once,
        );

        let result = env
            .service
            .succeed_task_instance(
                env.service.session_id(),
                job_id,
                instance_id,
                0,
                serialized_output(1),
            )
            .await;
        assert!(
            matches!(
                result,
                Err(StorageServerError::Cache(CacheError::Internal(_)))
            ),
            "{point:?}: the report should fail with an internal error, got {result:?}"
        );
        assert_eq!(crash.hits(), 1, "{point:?}: the crash");
        assert!(env.is_fatal(), "{point:?}: the shutdown should be fatal");
        assert_eq!(env.row_state(job_id).await, JobState::Running);
        drop(crash);

        env = env.restart().await;
        assert_eq!(env.poll_ready_tasks(job_id, 1).await, vec![0]);
        let instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
        let state = env
            .service
            .succeed_task_instance(
                env.service.session_id(),
                job_id,
                instance_id,
                0,
                serialized_output(2),
            )
            .await?;
        assert_eq!(state, JobState::Succeeded, "{point:?}: the rerun");
        env.assert_job_state(job_id, JobState::Succeeded).await;
        assert_eq!(
            env.service.get_job_outputs(job_id).await?,
            TaskOutputsSerializer::deserialize(&serialized_output(2))?
        );
    }
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn stale_reports_before_a_mutation_are_not_fatal() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let job_id = env.start_chain_job(2, 2).await;
    let first_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    let straggler_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    let state = env
        .service
        .succeed_task_instance(
            env.service.session_id(),
            job_id,
            first_instance_id,
            0,
            serialized_output(1),
        )
        .await?;
    assert_eq!(state, JobState::Running);
    assert_eq!(env.poll_ready_tasks(job_id, 1).await, vec![1]);

    let straggler_success = env
        .service
        .succeed_task_instance(
            env.service.session_id(),
            job_id,
            straggler_instance_id,
            0,
            serialized_output(2),
        )
        .await;
    assert!(
        matches!(
            straggler_success,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::TaskAlreadyTerminated(TaskState::Succeeded)
            )))
        ),
        "a straggler's success should be stale, got {straggler_success:?}"
    );
    let straggler_failure = env
        .service
        .fail_task_instance(
            env.service.session_id(),
            job_id,
            straggler_instance_id,
            TaskId::Index(0),
            "straggler".to_owned(),
        )
        .await;
    assert!(
        matches!(
            straggler_failure,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::InvalidTaskInstanceId
            )))
        ),
        "a failure of the removed straggler should be stale, got {straggler_failure:?}"
    );
    let unknown_job = env
        .service
        .succeed_task_instance(
            env.service.session_id(),
            JobId::random(),
            first_instance_id,
            0,
            serialized_output(1),
        )
        .await;
    assert!(
        matches!(unknown_job, Err(StorageServerError::JobNotFound(_))),
        "a report of an unknown job should not find it, got {unknown_job:?}"
    );
    assert!(!env.runtime.get_shutdown_handle().is_cancelled());

    let instance_id = env.create_instance(job_id, TaskId::Index(1)).await;
    let state = env
        .service
        .succeed_task_instance(
            env.service.session_id(),
            job_id,
            instance_id,
            1,
            serialized_output(3),
        )
        .await?;
    assert_eq!(state, JobState::Succeeded);
    env.assert_job_state(job_id, JobState::Succeeded).await;
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
