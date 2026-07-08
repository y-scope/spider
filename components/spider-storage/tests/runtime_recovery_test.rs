use std::net::IpAddr;
use std::time::Duration;

use spider_core::job::JobState;
use spider_core::task::TaskIndex;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::TaskOutput;
use spider_core::types::io::TaskOutputsSerializer;
use spider_storage::cache::error::CacheError;
use spider_storage::cache::error::StaleStateError;
use spider_storage::db::ExternalJobOrchestration;
use spider_storage::ready_queue::CleanupTaskMarker;
use spider_storage::ready_queue::CommitTaskMarker;
use spider_storage::ready_queue::ReadyQueueConfig;
use spider_storage::ready_queue::ReadyQueueEntry;
use spider_storage::state::JobCacheGcConfig;
use spider_storage::state::Runtime;
use spider_storage::state::ServiceState;
use spider_storage::state::StorageServerError;
use spider_storage::state::create_runtime;
use spider_storage::state::runtime::RuntimeConfig;
use spider_storage::task_instance_pool::TaskInstancePoolConfig;

use crate::mariadb_infra::create_mariadb_config;
use crate::mariadb_infra::create_mariadb_connector;
use crate::task_graph_builder::build_flat_task_graph;
use crate::task_graph_builder::compress_job_inputs;
use crate::task_graph_builder::compress_task_graph;

#[tokio::test]
#[ignore = "requires MariaDB"]
#[serial_test::file_serial]
async fn restarted_storage_cache_recovers_ready_job() -> anyhow::Result<()> {
    let config = create_runtime_config();
    let (runtime, _) = create_runtime(&config).await?;
    let service = runtime.get_service_state();
    let job_id = register_job(&service, false, false).await?;
    assert_eq!(service.get_job_state(job_id).await?, JobState::Ready);
    runtime.stop().await?;

    let (recovered_runtime, _) = create_runtime(&config).await?;
    let recovered_service = recovered_runtime.get_service_state();
    recovered_service.start_job(job_id).await?;
    assert_eq!(
        recovered_service.get_job_state(job_id).await?,
        JobState::Running
    );
    let expected_outputs = run_single_task_job_to_succeed(&recovered_service, job_id).await?;
    assert_job_outputs_on_success(&recovered_service, job_id, &expected_outputs).await?;
    recovered_runtime.stop().await?;

    // Create another runtime to test the job state and outputs are persisted.
    let (recovered_runtime, _) = create_runtime(&config).await?;
    assert_job_outputs_on_success(
        &recovered_runtime.get_service_state(),
        job_id,
        &expected_outputs,
    )
    .await?;
    recovered_runtime.stop().await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
#[serial_test::file_serial]
async fn restarted_storage_cache_recovers_running_job_from_start() -> anyhow::Result<()> {
    let config = create_runtime_config();
    let (job_id, recovered_runtime) = restart_after_starting_job(&config, false, false).await?;
    let recovered_service = recovered_runtime.get_service_state();

    recovered_service.resend_ready_tasks().await?;
    let expected_outputs = run_single_task_job_to_succeed(&recovered_service, job_id).await?;
    assert_job_outputs_on_success(&recovered_service, job_id, &expected_outputs).await?;
    recovered_runtime.stop().await?;

    // Create another runtime to test the job state and outputs are persisted.
    let (recovered_runtime, _) = create_runtime(&config).await?;
    assert_job_outputs_on_success(
        &recovered_runtime.get_service_state(),
        job_id,
        &expected_outputs,
    )
    .await?;
    recovered_runtime.stop().await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
#[serial_test::file_serial]
async fn restarted_storage_cache_recovers_commit_ready_job() -> anyhow::Result<()> {
    let config = create_runtime_config();
    let (job_id, recovered_runtime) = restart_after_commit_ready(&config).await?;
    let recovered_service = recovered_runtime.get_service_state();

    recovered_service.resend_ready_tasks().await?;
    let ready_entries = recovered_service
        .poll_commit_ready_tasks(32, Duration::from_secs(1))
        .await?;
    let job_entries = find_entry_for_job(ready_entries, job_id);
    assert_eq!(job_entries.len(), 1);
    assert_eq!(job_entries[0].task_kind, CommitTaskMarker);

    let execution_manager_id = recovered_service
        .register_execution_manager(IpAddr::from([127, 0, 0, 1]))
        .await?;

    assert_regular_task_registration_rejected(&recovered_service, job_id, execution_manager_id)
        .await;

    let execution_context = recovered_service
        .create_task_instance(
            recovered_service.session_id(),
            job_id,
            spider_core::types::id::TaskId::Commit,
            execution_manager_id,
        )
        .await?;
    let state = recovered_service
        .succeed_commit_task_instance(
            recovered_service.session_id(),
            job_id,
            execution_context.task_instance_id,
        )
        .await?;
    assert_eq!(state, JobState::Succeeded);

    let expected_outputs = TaskOutputsSerializer::deserialize(&serialized_single_output()?)?;
    assert_job_outputs_on_success(&recovered_service, job_id, &expected_outputs).await?;
    recovered_runtime.stop().await?;

    // Create another runtime to test the job state and outputs are persisted.
    let (recovered_runtime, _) = create_runtime(&config).await?;
    assert_job_outputs_on_success(
        &recovered_runtime.get_service_state(),
        job_id,
        &expected_outputs,
    )
    .await?;
    recovered_runtime.stop().await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
#[serial_test::file_serial]
async fn restarted_storage_cache_recovers_cleanup_ready_job() -> anyhow::Result<()> {
    let config = create_runtime_config();
    let (job_id, recovered_runtime) = restart_after_cleanup_ready(&config).await?;
    let recovered_service = recovered_runtime.get_service_state();

    recovered_service.resend_ready_tasks().await?;
    let ready_entries = recovered_service
        .poll_cleanup_ready_tasks(32, Duration::from_secs(1))
        .await?;
    let job_entries = find_entry_for_job(ready_entries, job_id);
    assert_eq!(job_entries.len(), 1);
    assert_eq!(job_entries[0].task_kind, CleanupTaskMarker);

    let execution_manager_id = recovered_service
        .register_execution_manager(IpAddr::from([127, 0, 0, 1]))
        .await?;

    assert_regular_task_registration_rejected(&recovered_service, job_id, execution_manager_id)
        .await;

    let execution_context = recovered_service
        .create_task_instance(
            recovered_service.session_id(),
            job_id,
            spider_core::types::id::TaskId::Cleanup,
            execution_manager_id,
        )
        .await?;
    let state = recovered_service
        .succeed_cleanup_task_instance(
            recovered_service.session_id(),
            job_id,
            execution_context.task_instance_id,
        )
        .await?;
    assert_eq!(state, JobState::Cancelled);

    assert_eq!(
        create_mariadb_connector().await.get_state(job_id).await?,
        JobState::Cancelled
    );
    recovered_runtime.stop().await?;
    Ok(())
}

/// # Returns
///
/// A runtime configuration created for testing, with DB config given by [`create_mariadb_config`]
/// while other configurations set to default.
fn create_runtime_config() -> RuntimeConfig {
    RuntimeConfig {
        db_config: create_mariadb_config(),
        ready_queue_config: ReadyQueueConfig::default(),
        task_instance_pool_config: TaskInstancePoolConfig::default(),
        job_cache_gc_config: JobCacheGcConfig::default(),
    }
}

/// Starts a job, stops the runtime, and creates a replacement runtime over the same database.
///
/// # Returns
///
/// A tuple on success, containing:
///
/// * The job ID.
/// * Recovered runtime.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`create_runtime`]'s return values on failure.
/// * Forwards [`register_and_start_job`]'s return values on failure.
/// * Forwards [`Runtime::stop`]'s return values on failure.
async fn restart_after_starting_job(
    config: &RuntimeConfig,
    with_commit: bool,
    with_cleanup: bool,
) -> anyhow::Result<(
    JobId,
    Runtime<
        spider_storage::ready_queue::ReadyQueueSenderHandle,
        spider_storage::db::MariaDbStorageConnector,
        spider_storage::task_instance_pool::TaskInstancePoolHandle,
    >,
)> {
    let (runtime, _) = create_runtime(config).await?;
    let service = runtime.get_service_state();
    let job_id = register_and_start_job(&service, with_commit, with_cleanup).await?;
    runtime.stop().await?;

    let (recovered_runtime, _) = create_runtime(config).await?;
    Ok((job_id, recovered_runtime))
}

/// Drives a job to [`JobState::CommitReady`], stops the runtime, and creates a replacement runtime.
///
/// # Returns
///
/// A tuple on success, containing:
///
/// * The job ID.
/// * Recovered runtime.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`restart_after_starting_job`]'s return values on failure.
/// * Forwards [`ServiceState::poll_ready_tasks`]'s return values on failure.
/// * Forwards [`run_recovered_regular_task`]'s return values on failure.
/// * Forwards [`serialized_single_output`]'s return values on failure.
/// * Forwards [`ServiceState::succeed_task_instance`]'s return values on failure.
/// * Forwards [`Runtime::stop`]'s return values on failure.
/// * Forwards [`create_runtime`]'s return values on failure.
async fn restart_after_commit_ready(
    config: &RuntimeConfig,
) -> anyhow::Result<(
    JobId,
    Runtime<
        spider_storage::ready_queue::ReadyQueueSenderHandle,
        spider_storage::db::MariaDbStorageConnector,
        spider_storage::task_instance_pool::TaskInstancePoolHandle,
    >,
)> {
    let (job_id, runtime) = restart_after_starting_job(config, true, false).await?;
    let service = runtime.get_service_state();

    service.resend_ready_tasks().await?;
    let ready_entries = service.poll_ready_tasks(32, Duration::from_secs(1)).await?;
    let job_entries = find_entry_for_job(ready_entries, job_id);
    assert_eq!(job_entries.len(), 1);
    let ready_entry = job_entries[0];

    let task_instance_id =
        run_recovered_regular_task(&service, job_id, ready_entry.task_kind).await?;
    let state = service
        .succeed_task_instance(
            service.session_id(),
            job_id,
            task_instance_id,
            ready_entry.task_kind,
            serialized_single_output()?,
        )
        .await?;
    assert_eq!(state, JobState::CommitReady);
    runtime.stop().await?;

    let (recovered_runtime, _) = create_runtime(config).await?;
    Ok((job_id, recovered_runtime))
}

/// Drives a job to [`JobState::CleanupReady`], stops the runtime, and creates a replacement
/// runtime.
///
/// # Returns
///
/// A tuple on success, containing:
///
/// * The job ID.
/// * Recovered runtime.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`create_runtime`]'s return values on failure.
/// * Forwards [`register_and_start_job`]'s return values on failure.
/// * Forwards [`ServiceState::cancel_job`]'s return values on failure.
/// * Forwards [`Runtime::stop`]'s return values on failure.
async fn restart_after_cleanup_ready(
    config: &RuntimeConfig,
) -> anyhow::Result<(
    JobId,
    Runtime<
        spider_storage::ready_queue::ReadyQueueSenderHandle,
        spider_storage::db::MariaDbStorageConnector,
        spider_storage::task_instance_pool::TaskInstancePoolHandle,
    >,
)> {
    let (runtime, _) = create_runtime(config).await?;
    let service = runtime.get_service_state();
    let job_id = register_and_start_job(&service, false, true).await?;
    let state = service.cancel_job(job_id).await?;
    assert_eq!(state, JobState::CleanupReady);
    runtime.stop().await?;

    let (recovered_runtime, _) = create_runtime(config).await?;
    Ok((job_id, recovered_runtime))
}

/// Registers and starts a flat recovery-test job.
///
/// # Returns
///
/// The registered job ID on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`register_job`]'s return values on failure.
/// * Forwards [`ServiceState::start_job`]'s return values on failure.
async fn register_and_start_job<
    ReadyQueueSenderType: spider_storage::ready_queue::ReadyQueueSender,
    DbConnectorType: spider_storage::db::DbStorage,
    TaskInstancePoolConnectorType: spider_storage::task_instance_pool::TaskInstancePoolConnector,
>(
    service: &ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    with_commit: bool,
    with_cleanup: bool,
) -> anyhow::Result<JobId> {
    let job_id = register_job(service, with_commit, with_cleanup).await?;
    service.start_job(job_id).await?;
    Ok(job_id)
}

/// Registers a flat recovery-test job without starting it.
///
/// # Returns
///
/// The registered job ID on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`ServiceState::add_resource_group`]'s return values on failure.
/// * Forwards [`spider_core::task::TaskGraph::to_json`]'s return values on failure.
/// * Forwards [`ServiceState::register_job`]'s return values on failure.
async fn register_job<
    ReadyQueueSenderType: spider_storage::ready_queue::ReadyQueueSender,
    DbConnectorType: spider_storage::db::DbStorage,
    TaskInstancePoolConnectorType: spider_storage::task_instance_pool::TaskInstancePoolConnector,
>(
    service: &ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    with_commit: bool,
    with_cleanup: bool,
) -> anyhow::Result<JobId> {
    let rg_id = service
        .add_resource_group(
            format!("recovery-test-{}", rand::random::<u64>()),
            b"test-password".to_vec(),
        )
        .await?;
    let (task_graph, inputs) = build_flat_task_graph(1, 4, with_commit, with_cleanup);
    let compressed_task_graph = compress_task_graph(&task_graph)?;
    let compressed_inputs = compress_job_inputs(&inputs)?;
    Ok(service
        .register_job(rg_id, compressed_task_graph, compressed_inputs)
        .await?)
}

/// Registers an execution manager and creates an instance for a recovered regular task.
///
/// # Returns
///
/// The created task instance ID on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`ServiceState::register_execution_manager`]'s return values on failure.
/// * Forwards [`ServiceState::create_task_instance`]'s return values on failure.
async fn run_recovered_regular_task<
    ReadyQueueSenderType: spider_storage::ready_queue::ReadyQueueSender,
    DbConnectorType: spider_storage::db::DbStorage,
    TaskInstancePoolConnectorType: spider_storage::task_instance_pool::TaskInstancePoolConnector,
>(
    service: &ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    job_id: JobId,
    task_index: TaskIndex,
) -> anyhow::Result<TaskInstanceId> {
    let execution_manager_id = service
        .register_execution_manager(IpAddr::from([127, 0, 0, 1]))
        .await?;
    let execution_context = service
        .create_task_instance(
            service.session_id(),
            job_id,
            spider_core::types::id::TaskId::Index(task_index),
            execution_manager_id,
        )
        .await?;
    Ok(execution_context.task_instance_id)
}

/// Runs the ready task for a single-task job to completion.
///
/// # Returns
///
/// The expected job outputs on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`ServiceState::poll_ready_tasks`]'s return values on failure.
/// * Forwards [`run_recovered_regular_task`]'s return values on failure.
/// * Forwards [`serialized_single_output`]'s return values on failure.
/// * Forwards [`ServiceState::succeed_task_instance`]'s return values on failure.
/// * Forwards [`TaskOutputsSerializer::deserialize`]'s return values on failure.
async fn run_single_task_job_to_succeed<
    ReadyQueueSenderType: spider_storage::ready_queue::ReadyQueueSender,
    DbConnectorType: spider_storage::db::DbStorage,
    TaskInstancePoolConnectorType: spider_storage::task_instance_pool::TaskInstancePoolConnector,
>(
    service: &ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    job_id: JobId,
) -> anyhow::Result<Vec<TaskOutput>> {
    let ready_entries = service.poll_ready_tasks(32, Duration::from_secs(1)).await?;
    let job_entries = find_entry_for_job(ready_entries, job_id);
    assert_eq!(job_entries.len(), 1);
    let task_index = job_entries[0].task_kind;

    let task_instance_id = run_recovered_regular_task(service, job_id, task_index).await?;
    let state = service
        .succeed_task_instance(
            service.session_id(),
            job_id,
            task_instance_id,
            task_index,
            serialized_single_output()?,
        )
        .await?;
    assert_eq!(state, JobState::Succeeded);

    Ok(TaskOutputsSerializer::deserialize(
        &serialized_single_output()?,
    )?)
}

/// Asserts that registering a regular task instance on the given job is rejected because the job
/// has already reached a terminal state and is therefore no longer running.
///
/// # Panics
///
/// Panics if the registration succeeds or fails with an error other than
/// [`StaleStateError::JobNoLongerRunning`].
async fn assert_regular_task_registration_rejected<
    ReadyQueueSenderType: spider_storage::ready_queue::ReadyQueueSender,
    DbConnectorType: spider_storage::db::DbStorage,
    TaskInstancePoolConnectorType: spider_storage::task_instance_pool::TaskInstancePoolConnector,
>(
    service: &ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    job_id: JobId,
    execution_manager_id: ExecutionManagerId,
) {
    let result = service
        .create_task_instance(
            service.session_id(),
            job_id,
            spider_core::types::id::TaskId::Index(0),
            execution_manager_id,
        )
        .await;
    assert!(
        matches!(
            result,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::JobNoLongerRunning
            )))
        ),
        "registering a regular task on a terminal job should fail with JobNoLongerRunning, got \
         {result:?}"
    );
}

/// Asserts that the job is in [`JobState::Succeeded`] state and its outputs match the expected.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`ServiceState::get_job_state`]'s return values.
/// * Forwards [`ServiceState::get_job_outputs`]'s return values.
async fn assert_job_outputs_on_success<
    ReadyQueueSenderType: spider_storage::ready_queue::ReadyQueueSender,
    DbConnectorType: spider_storage::db::DbStorage,
    TaskInstancePoolConnectorType: spider_storage::task_instance_pool::TaskInstancePoolConnector,
>(
    service: &ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    job_id: JobId,
    expected_outputs: &[TaskOutput],
) -> anyhow::Result<()> {
    let state = service.get_job_state(job_id).await?;
    assert_eq!(state, JobState::Succeeded);
    let outputs = service.get_job_outputs(job_id).await?;
    assert_eq!(outputs, expected_outputs);
    Ok(())
}

/// Collects the ready-queue entries belonging to a job.
///
/// # Returns
///
/// The matching ready-queue entries, preserving their original order.
fn find_entry_for_job<TaskKind>(
    entries: Vec<ReadyQueueEntry<TaskKind>>,
    job_id: JobId,
) -> Vec<ReadyQueueEntry<TaskKind>> {
    entries
        .into_iter()
        .filter(|entry| entry.job_id == job_id)
        .collect()
}

/// Serializes the single output payload used by recovery tests.
///
/// # Returns
///
/// The serialized task output on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`TaskOutputsSerializer::from_tuple`]'s return values on failure.
fn serialized_single_output() -> anyhow::Result<Vec<u8>> {
    Ok(TaskOutputsSerializer::from_tuple(&(vec![1u8; 4],))?)
}
