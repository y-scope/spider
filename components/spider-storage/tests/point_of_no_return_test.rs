//! Storage service handler bodies around their first cache or database mutation, on `MariaDB`.

use spider_core::job::JobState;
use spider_core::task::ExecutionPolicy;
use spider_core::types::id::TaskId;
use spider_core::types::io::TaskOutputsSerializer;

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
