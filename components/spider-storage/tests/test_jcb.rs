mod mariadb_infra;
mod scheduling_infra;
mod task_graph_builder;

use scheduling_infra::{
    CancelPolicy,
    DbConnectorFactory,
    InstrumentSender,
    default_output_handler,
    mariadb_db_connector_factory,
    noop_db_connector,
    run_workload,
    try_create_instrument_channel,
    write_instrument_results,
};
use spider_core::job::JobState;
use spider_storage::db::InternalJobOrchestration;
use task_graph_builder::{build_flat_task_graph, build_neural_net_task_graph};

/// Evaluates to the fully-qualified name of the enclosing function, stripping internal suffixes
/// like `::_f` and `::{{closure}}` that result from the macro expansion and `#[tokio::test]`.
macro_rules! function_name {
    () => {{
        fn _f() {}
        let name = std::any::type_name_of_val(&_f);
        let name = name
            .strip_suffix("::_f")
            .expect("function_name macro should always find ::_f suffix");
        name.strip_suffix("::{{closure}}").unwrap_or(name)
    }};
}

/// Runs the flat workload (10,000 independent tasks with commit + cleanup) to successful
/// completion.
async fn test_flat_success<Db: InternalJobOrchestration + 'static>(
    db_connector_factory: Box<DbConnectorFactory<Db>>,
    instrument_sender: Option<InstrumentSender>,
) {
    let (graph, inputs) = build_flat_task_graph(10_000, 1024, true, true);
    let num_tasks = graph.get_num_tasks();
    let result = run_workload(
        &graph,
        inputs,
        db_connector_factory,
        CancelPolicy::Never,
        default_output_handler(1024),
        false,
        instrument_sender,
    )
    .await;

    assert_eq!(
        result.terminal_state,
        JobState::Succeeded,
        "flat workload should succeed"
    );
    assert_eq!(
        result.task_success_count, num_tasks,
        "all tasks should be successfully completed"
    );
    assert_eq!(result.commit_count, 1, "commit task should execute once");
    assert_eq!(
        result.cleanup_count, 0,
        "cleanup task should not execute on success"
    );
}

/// Cancels the flat workload immediately after starting.
async fn test_flat_cancel<Db: InternalJobOrchestration + 'static>(
    db_connector_factory: Box<DbConnectorFactory<Db>>,
) {
    let (graph, inputs) = build_flat_task_graph(10_000, 1024, true, true);
    let result = run_workload(
        &graph,
        inputs,
        db_connector_factory,
        CancelPolicy::Immediate,
        default_output_handler(1024),
        false,
        None,
    )
    .await;

    assert_eq!(
        result.terminal_state,
        JobState::Cancelled,
        "immediately cancelled flat workload should reach Cancelled"
    );
    assert_eq!(
        result.commit_count, 0,
        "commit task should not execute on cancel"
    );
    assert_eq!(
        result.cleanup_count, 1,
        "cleanup task should execute once on cancel"
    );
}

/// Runs the neural-net workload (10 layers x 1,000 tasks, no termination tasks) to successful
/// completion.
async fn test_neural_net_success<Db: InternalJobOrchestration + 'static>(
    db_connector_factory: Box<DbConnectorFactory<Db>>,
    instrument_sender: Option<InstrumentSender>,
) {
    let (graph, inputs) = build_neural_net_task_graph();
    let num_tasks = graph.get_num_tasks();
    let result = run_workload(
        &graph,
        inputs,
        db_connector_factory,
        CancelPolicy::Never,
        default_output_handler(128),
        false,
        instrument_sender,
    )
    .await;

    assert_eq!(
        result.terminal_state,
        JobState::Succeeded,
        "neural-net workload should succeed"
    );
    assert_eq!(
        result.task_success_count, num_tasks,
        "all tasks should be successfully completed"
    );
    assert_eq!(
        result.commit_count, 0,
        "no commit task in neural-net workload"
    );
    assert_eq!(
        result.cleanup_count, 0,
        "no cleanup task in neural-net workload"
    );
}

/// Cancels the neural-net workload immediately after starting.
async fn test_neural_net_cancel<Db: InternalJobOrchestration + 'static>(
    db_connector_factory: Box<DbConnectorFactory<Db>>,
) {
    let (graph, inputs) = build_neural_net_task_graph();
    let result = run_workload(
        &graph,
        inputs,
        db_connector_factory,
        CancelPolicy::Immediate,
        default_output_handler(128),
        false,
        None,
    )
    .await;

    assert_eq!(
        result.terminal_state,
        JobState::Cancelled,
        "immediately cancelled neural-net workload should reach Cancelled"
    );
    assert_eq!(
        result.commit_count, 0,
        "no commit task in neural-net workload"
    );
    assert_eq!(
        result.cleanup_count, 0,
        "no cleanup task in neural-net workload"
    );
}

/// Runs a job whose tasks always fail (`max_num_retry = 3`, all instances fail). The job should
/// transition to [`JobState::Failed`] after retries are exhausted.
async fn test_always_fail_terminates_job<Db: InternalJobOrchestration + 'static>(
    db_connector_factory: Box<DbConnectorFactory<Db>>,
) {
    let (graph, inputs) = build_flat_task_graph(3, 128, false, false);
    let result = run_workload(
        &graph,
        inputs,
        db_connector_factory,
        CancelPolicy::Never,
        default_output_handler(128),
        true,
        None,
    )
    .await;

    assert_eq!(
        result.terminal_state,
        JobState::Failed,
        "always-fail task should cause job to fail"
    );
    assert_eq!(
        result.task_success_count, 0,
        "no tasks should succeed in always-fail mode"
    );
}

/// Races task execution against cancellation. A small flat workload (100 tasks with commit +
/// cleanup) is started and a cancel is issued concurrently after a short delay.
async fn test_concurrent_success_and_cancel<Db: InternalJobOrchestration + 'static>(
    db_connector_factory: Box<DbConnectorFactory<Db>>,
) {
    let (graph, inputs) = build_flat_task_graph(100, 128, true, true);
    let result = run_workload(
        &graph,
        inputs,
        db_connector_factory,
        CancelPolicy::Concurrent,
        default_output_handler(128),
        false,
        None,
    )
    .await;

    assert!(
        result.terminal_state == JobState::Succeeded
            || result.terminal_state == JobState::Cancelled,
        "concurrent success/cancel should produce Succeeded or Cancelled, got {:?}",
        result.terminal_state
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flat_success_without_db() {
    let channel = try_create_instrument_channel();
    let instrument_sender = channel.as_ref().map(|(sender, _)| sender.clone());
    test_flat_success(noop_db_connector(), instrument_sender).await;
    if let Some((_, receiver)) = channel {
        write_instrument_results(function_name!(), receiver);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flat_cancel_without_db() {
    test_flat_cancel(noop_db_connector()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_neural_net_success_without_db() {
    let channel = try_create_instrument_channel();
    let instrument_sender = channel.as_ref().map(|(sender, _)| sender.clone());
    test_neural_net_success(noop_db_connector(), instrument_sender).await;
    if let Some((_, receiver)) = channel {
        write_instrument_results(function_name!(), receiver);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_neural_net_cancel_without_db() {
    test_neural_net_cancel(noop_db_connector()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_always_fail_terminates_job_without_db() {
    test_always_fail_terminates_job(noop_db_connector()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_success_and_cancel_without_db() {
    test_concurrent_success_and_cancel(noop_db_connector()).await;
}

// ─── MariaDB integration tests ─────────────────────────────────────────────
//
// Neural-net MariaDB tests are omitted: the 25,000-input payload (10 layers × 1,000 tasks × 25
// inputs) exceeds MariaDB's `max_allowed_packet` limit during job registration.

use mariadb_infra::{create_test_resource_group, setup as mariadb_setup};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MariaDB"]
async fn test_flat_success_with_mariadb() {
    let storage = mariadb_setup().await;
    let rg_id = create_test_resource_group(&storage).await;
    test_flat_success(mariadb_db_connector_factory(storage, rg_id), None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MariaDB"]
async fn test_flat_cancel_with_mariadb() {
    let storage = mariadb_setup().await;
    let rg_id = create_test_resource_group(&storage).await;
    test_flat_cancel(mariadb_db_connector_factory(storage, rg_id)).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MariaDB"]
async fn test_always_fail_terminates_job_with_mariadb() {
    let storage = mariadb_setup().await;
    let rg_id = create_test_resource_group(&storage).await;
    test_always_fail_terminates_job(mariadb_db_connector_factory(storage, rg_id)).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MariaDB"]
async fn test_concurrent_success_and_cancel_with_mariadb() {
    let storage = mariadb_setup().await;
    let rg_id = create_test_resource_group(&storage).await;
    test_concurrent_success_and_cancel(mariadb_db_connector_factory(storage, rg_id)).await;
}
