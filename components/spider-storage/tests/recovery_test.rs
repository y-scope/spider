use std::{net::IpAddr, time::Duration};

use spider_core::{
    job::JobState,
    task::TaskIndex,
    types::{
        id::{JobId, TaskInstanceId},
        io::TaskInput,
    },
};
use spider_storage::{
    db::ExternalJobOrchestration,
    ready_queue::{ReadyQueueConfig, ReadyQueueEntry},
    state::{Runtime, ServiceState, StorageServerError, create_runtime},
    task_instance_pool::TaskInstancePoolConfig,
};
use spider_tdl::wire::{TaskInputsSerializer, TaskOutputsSerializer};

use crate::{
    mariadb_infra::{create_mariadb_config, create_mariadb_connector},
    task_graph_builder::build_flat_task_graph,
};

#[tokio::test]
async fn restarted_storage_cache_does_not_recover_ready_job() -> anyhow::Result<()> {
    let db_config = create_mariadb_config();
    let (runtime, _) = create_runtime(
        &db_config,
        &ReadyQueueConfig::default(),
        &TaskInstancePoolConfig::default(),
    )
    .await?;
    let service = runtime.get_service_state();
    let job_id = create_registered_job(&service, false, false).await?;
    assert_eq!(service.get_job_state(job_id).await?, JobState::Ready);
    runtime.stop().await?;

    let (recovered_runtime, _) = create_runtime(
        &db_config,
        &ReadyQueueConfig::default(),
        &TaskInstancePoolConfig::default(),
    )
    .await?;
    let recovered_service = recovered_runtime.get_service_state();
    let start_result = recovered_service.start_job(job_id).await;
    assert!(
        matches!(start_result, Err(StorageServerError::JobNotFound(id)) if id == job_id),
        "ready job should not be recovered into cache"
    );
    assert_eq!(
        recovered_service.get_job_state(job_id).await?,
        JobState::Ready
    );
    recovered_runtime.stop().await?;
    Ok(())
}

#[tokio::test]
async fn restarted_storage_cache_recovers_running_job_from_start() -> anyhow::Result<()> {
    let db_config = create_mariadb_config();
    let (job_id, recovered_service, recovered_runtime) =
        restart_after_starting_job(&db_config, false, false).await?;

    let ready_entries = recovered_service
        .poll_ready_tasks(32, Duration::from_secs(1))
        .await?;
    let ready_entry = find_entry_for_job(ready_entries, job_id);

    let task_instance_id =
        run_recovered_regular_task(&recovered_service, job_id, ready_entry.task_kind).await?;
    let state = recovered_service
        .succeed_task_instance(
            recovered_service.session_id(),
            job_id,
            task_instance_id,
            ready_entry.task_kind,
            serialized_single_output()?,
        )
        .await?;
    assert_eq!(state, JobState::Succeeded);

    assert_eq!(
        create_mariadb_connector().await.get_state(job_id).await?,
        JobState::Succeeded
    );
    recovered_runtime.stop().await?;
    Ok(())
}

#[tokio::test]
async fn restarted_storage_cache_recovers_commit_ready_job() -> anyhow::Result<()> {
    let db_config = create_mariadb_config();
    let (job_id, recovered_service, recovered_runtime) =
        restart_after_commit_ready(&db_config).await?;

    let ready_entries = recovered_service
        .poll_commit_ready_tasks(32, Duration::from_secs(1))
        .await?;
    let _ready_entry = find_entry_for_job(ready_entries, job_id);

    let execution_manager_id = recovered_service
        .register_execution_manager(IpAddr::from([127, 0, 0, 1]))
        .await?;
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
    assert_eq!(
        recovered_service.get_job_outputs(job_id).await?,
        expected_outputs
    );

    assert_eq!(
        create_mariadb_connector().await.get_state(job_id).await?,
        JobState::Succeeded
    );
    recovered_runtime.stop().await?;
    Ok(())
}

#[tokio::test]
async fn restarted_storage_cache_recovers_cleanup_ready_job() -> anyhow::Result<()> {
    let db_config = create_mariadb_config();
    let (job_id, recovered_service, recovered_runtime) =
        restart_after_cleanup_ready(&db_config).await?;

    let ready_entries = recovered_service
        .poll_cleanup_ready_tasks(32, Duration::from_secs(1))
        .await?;
    let _ready_entry = find_entry_for_job(ready_entries, job_id);

    let execution_manager_id = recovered_service
        .register_execution_manager(IpAddr::from([127, 0, 0, 1]))
        .await?;
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

/// Starts a job, stops the runtime, and creates a replacement runtime over the same database.
///
/// # Returns
///
/// The job ID, recovered service state, and recovered runtime on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`create_runtime`]'s return values on failure.
/// * Forwards [`create_and_start_job`]'s return values on failure.
/// * Forwards [`Runtime::stop`]'s return values on failure.
async fn restart_after_starting_job(
    db_config: &spider_storage::DatabaseConfig,
    with_commit: bool,
    with_cleanup: bool,
) -> anyhow::Result<(
    JobId,
    ServiceState<
        spider_storage::ready_queue::ReadyQueueSenderHandle,
        spider_storage::db::MariaDbStorageConnector,
        spider_storage::task_instance_pool::TaskInstancePoolHandle,
    >,
    Runtime<
        spider_storage::ready_queue::ReadyQueueSenderHandle,
        spider_storage::db::MariaDbStorageConnector,
        spider_storage::task_instance_pool::TaskInstancePoolHandle,
    >,
)> {
    let (runtime, _) = create_runtime(
        db_config,
        &ReadyQueueConfig::default(),
        &TaskInstancePoolConfig::default(),
    )
    .await?;
    let service = runtime.get_service_state();
    let job_id = create_and_start_job(&service, with_commit, with_cleanup).await?;
    runtime.stop().await?;

    let (recovered_runtime, _) = create_runtime(
        db_config,
        &ReadyQueueConfig::default(),
        &TaskInstancePoolConfig::default(),
    )
    .await?;
    let recovered_service = recovered_runtime.get_service_state();
    Ok((job_id, recovered_service, recovered_runtime))
}

/// Drives a job to [`JobState::CommitReady`], stops the runtime, and creates a replacement runtime.
///
/// # Returns
///
/// The job ID, recovered service state, and recovered runtime on success.
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
    db_config: &spider_storage::DatabaseConfig,
) -> anyhow::Result<(
    JobId,
    ServiceState<
        spider_storage::ready_queue::ReadyQueueSenderHandle,
        spider_storage::db::MariaDbStorageConnector,
        spider_storage::task_instance_pool::TaskInstancePoolHandle,
    >,
    Runtime<
        spider_storage::ready_queue::ReadyQueueSenderHandle,
        spider_storage::db::MariaDbStorageConnector,
        spider_storage::task_instance_pool::TaskInstancePoolHandle,
    >,
)> {
    let (job_id, service, runtime) = restart_after_starting_job(db_config, true, false).await?;
    let ready_entries = service.poll_ready_tasks(32, Duration::from_secs(1)).await?;
    let ready_entry = find_entry_for_job(ready_entries, job_id);
    let task_instance_id =
        run_recovered_regular_task(&service, job_id, ready_entry.task_kind).await?;
    let state = service
        .succeed_task_instance(
            service.session_id(),
            job_id,
            task_instance_id,
            0,
            serialized_single_output()?,
        )
        .await?;
    assert_eq!(state, JobState::CommitReady);
    runtime.stop().await?;

    let (recovered_runtime, _) = create_runtime(
        db_config,
        &ReadyQueueConfig::default(),
        &TaskInstancePoolConfig::default(),
    )
    .await?;
    let recovered_service = recovered_runtime.get_service_state();
    Ok((job_id, recovered_service, recovered_runtime))
}

/// Drives a job to [`JobState::CleanupReady`], stops the runtime, and creates a replacement
/// runtime.
///
/// # Returns
///
/// The job ID, recovered service state, and recovered runtime on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`create_runtime`]'s return values on failure.
/// * Forwards [`create_and_start_job`]'s return values on failure.
/// * Forwards [`ServiceState::cancel_job`]'s return values on failure.
/// * Forwards [`Runtime::stop`]'s return values on failure.
async fn restart_after_cleanup_ready(
    db_config: &spider_storage::DatabaseConfig,
) -> anyhow::Result<(
    JobId,
    ServiceState<
        spider_storage::ready_queue::ReadyQueueSenderHandle,
        spider_storage::db::MariaDbStorageConnector,
        spider_storage::task_instance_pool::TaskInstancePoolHandle,
    >,
    Runtime<
        spider_storage::ready_queue::ReadyQueueSenderHandle,
        spider_storage::db::MariaDbStorageConnector,
        spider_storage::task_instance_pool::TaskInstancePoolHandle,
    >,
)> {
    let (runtime, _) = create_runtime(
        db_config,
        &ReadyQueueConfig::default(),
        &TaskInstancePoolConfig::default(),
    )
    .await?;
    let service = runtime.get_service_state();
    let job_id = create_and_start_job(&service, false, true).await?;
    let state = service.cancel_job(job_id).await?;
    assert_eq!(state, JobState::CleanupReady);
    runtime.stop().await?;

    let (recovered_runtime, _) = create_runtime(
        db_config,
        &ReadyQueueConfig::default(),
        &TaskInstancePoolConfig::default(),
    )
    .await?;
    let recovered_service = recovered_runtime.get_service_state();
    Ok((job_id, recovered_service, recovered_runtime))
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
/// * Forwards [`create_registered_job`]'s return values on failure.
/// * Forwards [`ServiceState::start_job`]'s return values on failure.
async fn create_and_start_job<
    ReadyQueueSenderType: spider_storage::ready_queue::ReadyQueueSender,
    DbConnectorType: spider_storage::db::DbStorage,
    TaskInstancePoolConnectorType: spider_storage::task_instance_pool::TaskInstancePoolConnector,
>(
    service: &ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    with_commit: bool,
    with_cleanup: bool,
) -> anyhow::Result<JobId> {
    let job_id = create_registered_job(service, with_commit, with_cleanup).await?;
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
/// * Forwards [`serialize_inputs`]'s return values on failure.
/// * Forwards [`ServiceState::register_job`]'s return values on failure.
async fn create_registered_job<
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
    Ok(service
        .register_job(rg_id, task_graph.to_json()?, serialize_inputs(inputs)?)
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

/// Finds the ready-queue entry for a job.
///
/// # Returns
///
/// The matching ready-queue entry.
///
/// # Panics
///
/// Panics if no matching entry exists.
fn find_entry_for_job<TaskKind>(
    entries: Vec<ReadyQueueEntry<TaskKind>>,
    job_id: JobId,
) -> ReadyQueueEntry<TaskKind> {
    entries
        .into_iter()
        .find(|entry| entry.job_id == job_id)
        .expect("recovered job should be enqueued")
}

/// Serializes task inputs into the storage service wire format.
///
/// # Returns
///
/// The serialized task inputs on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`TaskInputsSerializer::append`]'s return values on failure.
fn serialize_inputs(inputs: Vec<TaskInput>) -> anyhow::Result<Vec<u8>> {
    let mut serializer = TaskInputsSerializer::new();
    for input in inputs {
        serializer.append(input)?;
    }
    Ok(serializer.release())
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
