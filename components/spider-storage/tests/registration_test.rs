//! Job payloads through the storage service on `MariaDB`: job outputs and failure messages larger
//! than the database's packet size.

use spider_core::job::JobState;
use spider_core::task::DataTypeDescriptor;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TaskDescriptor;
use spider_core::task::TdlContext;
use spider_core::task::ValueTypeDescriptor;
use spider_core::types::id::JobId;
use spider_core::types::id::TaskId;
use spider_core::types::io::TaskGraphInput;
use spider_core::types::io::TaskGraphInputBuilder;
use spider_core::types::io::TaskOutputsSerializer;
use spider_storage::limits::MAX_ERROR_MESSAGE_CHARS;

use super::job_tree_infra::TestEnv;
use super::task_graph_builder::SubmittedTaskGraph;

/// Builds a job of `num_tasks` independent tasks, each taking and returning one byte-typed value.
///
/// # Returns
///
/// The task graph and its inputs.
///
/// # Panics
///
/// Panics if the graph fails to build.
fn build_job(num_tasks: usize) -> (SubmittedTaskGraph, TaskGraphInput) {
    let execution_policy = Some(ExecutionPolicy {
        max_num_retry: 0,
        ..ExecutionPolicy::default()
    });
    let mut graph =
        SubmittedTaskGraph::new(None, None).expect("task graph creation should succeed");
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

/// Registers and starts a job of `num_tasks` independent tasks.
///
/// # Returns
///
/// The job ID.
///
/// # Panics
///
/// Panics if the registration or the start fails, or if the job's tasks are not published.
async fn start_job(env: &TestEnv, num_tasks: usize) -> JobId {
    let (graph, inputs) = build_job(num_tasks);
    let job_id = env.register_job(&graph, &inputs).await;
    env.service
        .start_job(job_id)
        .await
        .expect("the job should start");
    assert_eq!(
        env.poll_ready_tasks(job_id, num_tasks).await.len(),
        num_tasks
    );
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

/// Asserts that `job_id`'s served and persisted states are both `expected`, and that the service
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

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn outputs_over_the_packet_size_fail_the_job() -> anyhow::Result<()> {
    const NUM_TASKS: usize = 12;

    let env = TestEnv::start().await;
    let job_id = start_job(&env, NUM_TASKS).await;
    let mut state = JobState::Running;
    for task_index in 0..NUM_TASKS {
        let task_instance_id = create_instance(&env, job_id, TaskId::Index(task_index)).await;
        state = env
            .service
            .succeed_task_instance(
                env.service.session_id(),
                job_id,
                task_instance_id,
                task_index,
                serialized_output(incompressible(2 * 1024 * 1024, u64::try_from(task_index)?)),
            )
            .await?;
    }
    assert_eq!(state, JobState::Failed);
    assert_job_state(&env, job_id, JobState::Failed).await;
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
    let job_id = start_job(&env, 1).await;
    let max_allowed_packet: u64 = sqlx::query_scalar("SELECT @@max_allowed_packet;")
        .fetch_one(&env.pool)
        .await?;
    let message = "x".repeat(usize::try_from(max_allowed_packet)? + 1);

    let task_instance_id = create_instance(&env, job_id, TaskId::Index(0)).await;
    assert_eq!(
        env.service
            .fail_task_instance(
                env.service.session_id(),
                job_id,
                task_instance_id,
                TaskId::Index(0),
                message.clone(),
            )
            .await?,
        JobState::Failed
    );
    assert_job_state(&env, job_id, JobState::Failed).await;
    assert_eq!(
        env.service.get_job_error(job_id).await?,
        message[..MAX_ERROR_MESSAGE_CHARS]
    );
    env.finish().await;
    Ok(())
}
