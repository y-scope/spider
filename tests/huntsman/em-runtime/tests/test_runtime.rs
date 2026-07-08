//! Integration tests for [`spider_execution_manager::runtime::Runtime`].
//!
//! Each test wires up the runtime with the in-process mocks from `em_runtime_tests` plus a real
//! `spider-task-executor` binary spawned by the runtime's owned process pool. The binary path and
//! the TDL package staging directory are read from the same env vars the rest of the huntsman
//! integration suite uses (`SPIDER_TASK_EXECUTOR_BIN`, `SPIDER_TDL_PACKAGE_DIR`).
//!
//! All tests are `#[ignore]` so the workspace's plain `cargo test` doesn't run them.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use spider_core::task::TdlContext;
use spider_core::task::TimeoutPolicy;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SchedulerId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskAssignmentId;
use spider_core::types::id::TaskId;
use spider_core::types::io::ExecutionContext;
use spider_core::types::io::TaskInput;
use spider_core::types::io::TaskInputsSerializer;
use spider_core::types::scheduler::TaskAssignment;
use spider_core::types::scheduler::TaskAssignmentRecord;
use spider_execution_manager::client::SchedulerError;
use spider_execution_manager::client::SchedulerResponse;
use spider_execution_manager::client::StorageResponseError;
use spider_execution_manager::runtime::Runtime;
use spider_execution_manager::runtime::RuntimeConfig;
use spider_execution_manager::runtime::RuntimeError;
use test_utils::MockLiveness;
use test_utils::MockScheduler;
use test_utils::MockStorage;
use test_utils::PACKAGE_NAME;
use test_utils::decode_single_output;
use test_utils::single_input;
use test_utils::task_executor_bin;
use test_utils::tdl_package_dir;

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
const SLOW_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const BOUNDED_WAIT: Duration = Duration::from_secs(2);
const TIGHT_WAIT: Duration = Duration::from_millis(500);
const SCHEDULER_POLL_WAIT_MS: u64 = 2_000;

/// Builds a [`SchedulerResponse`] tagged with `session_id` and fresh ids for the rest.
///
/// # Returns
///
/// A scheduler assignment carrying freshly generated `job_id`, `task_id`, and `resource_group_id`
/// alongside the requested `session_id`.
fn assignment_with_session(session_id: u64) -> SchedulerResponse {
    SchedulerResponse {
        task_assignment: TaskAssignment {
            id: TaskAssignmentId::random(),
            resource_group_id: ResourceGroupId::random(),
            job_id: JobId::random(),
            task_id: TaskId::Index(0),
        },
        scheduler_id: SchedulerId::random(),
        session_id,
    }
}

/// Builds an [`ExecutionContext`] pointing at `task_func` in the integration package with the
/// given inputs. Uses a generous hard timeout so well-behaved tasks always finish before the
/// process pool kills them.
///
/// # Returns
///
/// A populated [`ExecutionContext`] suitable for handing to the runtime via
/// [`MockStorage::push_register_response`].
fn execution_context(task_func: &str, inputs: Vec<TaskInput>) -> ExecutionContext {
    let mut serializer = TaskInputsSerializer::new();
    for input in inputs {
        serializer
            .append(input)
            .expect("input serialization should succeed");
    }

    ExecutionContext {
        task_instance_id: 1,
        tdl_context: TdlContext {
            package: PACKAGE_NAME.to_owned(),
            task_func: task_func.to_owned(),
        },
        timeout_policy: TimeoutPolicy {
            soft_timeout_ms: 1_000,
            hard_timeout_ms: 5_000,
        },
        serialized_inputs: serializer.release(),
    }
}

/// Polls `predicate` every 5 ms until it returns `true` or `timeout` elapses.
///
/// # Returns
///
/// Whether `predicate` returned `true` before the deadline.
async fn wait_until(predicate: impl Fn() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while !predicate() {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    true
}

/// Builds a fresh [`RuntimeConfig`] pointing at the real executor binary, with a unique per-test
/// log directory and the requested `heartbeat_interval`.
///
/// # Returns
///
/// A [`RuntimeConfig`] ready to hand to [`Runtime::create`].
///
/// # Panics
///
/// Panics if the hard-coded loopback ip fails to parse — never in practice.
fn runtime_config(heartbeat_interval: Duration) -> RuntimeConfig {
    let unique = ExecutionManagerId::random();
    let log_dir = std::env::temp_dir().join(format!("spider-em-runtime-test-{unique}"));
    RuntimeConfig {
        em_ip: "127.0.0.1".parse().expect("parse loopback"),
        heartbeat_interval,
        scheduler_heartbeat_interval: heartbeat_interval,
        scheduler_poll_wait_ms: SCHEDULER_POLL_WAIT_MS,
        executor_binary_path: task_executor_bin(),
        package_dir: tdl_package_dir(),
        log_dir,
    }
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn create_registers_and_starts_heartbeats() -> anyhow::Result<()> {
    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::new();

    let (_runtime, token) = Runtime::create(
        scheduler.clone(),
        storage.clone(),
        Arc::new(liveness.clone()),
        runtime_config(HEARTBEAT_INTERVAL),
    )
    .await?;

    assert_eq!(liveness.register_calls().len(), 1);
    assert!(
        liveness.wait_for_heartbeats(1, BOUNDED_WAIT).await,
        "liveness actor should send at least one heartbeat after create returns; observed {} so \
         far",
        liveness.heartbeat_count()
    );

    token.cancel();
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn create_propagates_pool_init_error() {
    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::new();
    let bad_config = RuntimeConfig {
        executor_binary_path: PathBuf::from("/nonexistent/spider-task-executor"),
        ..runtime_config(HEARTBEAT_INTERVAL)
    };

    let result = Runtime::create(scheduler, storage, Arc::new(liveness), bad_config).await;
    match result {
        Err(RuntimeError::ProcessPool(_)) => {}
        Err(other) => panic!("expected ProcessPool error, got {other:?}"),
        Ok(_) => panic!("expected ProcessPool error, got Ok"),
    }
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn external_cancellation_returns_ok() -> anyhow::Result<()> {
    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::new();

    let (runtime, token) = Runtime::create(
        scheduler,
        storage,
        Arc::new(liveness.clone()),
        runtime_config(HEARTBEAT_INTERVAL),
    )
    .await?;

    let join = tokio::spawn(runtime.run());
    // Let at least one heartbeat happen so we know the loop is alive before cancelling.
    assert!(liveness.wait_for_heartbeats(1, BOUNDED_WAIT).await);

    token.cancel();
    let result = tokio::time::timeout(BOUNDED_WAIT, join)
        .await
        .context("run did not return within bounded time")?
        .context("run task panicked")?;
    assert!(matches!(result, Ok(())), "expected Ok(()), got {result:?}");
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn scheduler_error_is_retried() -> anyhow::Result<()> {
    const SESSION_ID: SessionId = 5;

    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::with_initial_session(SESSION_ID);

    // The first poll errors; the loop should log it and poll again rather than bail. The second
    // poll returns a real assignment, which we drop on the storage side to keep the test focused.
    scheduler.push(Err(SchedulerError::Transport("boom".to_owned())));
    scheduler.push(Ok(assignment_with_session(SESSION_ID)));
    storage.push_register_response(Err(StorageResponseError::CacheStale(
        "test drop".to_owned(),
    )));

    let (runtime, token) = Runtime::create(
        scheduler.clone(),
        storage.clone(),
        Arc::new(liveness),
        runtime_config(HEARTBEAT_INTERVAL),
    )
    .await?;

    let join = tokio::spawn(runtime.run());

    // Reaching register proves the loop retried past the scheduler error onto the next poll.
    assert!(
        wait_until(|| !storage.register_calls().is_empty(), BOUNDED_WAIT).await,
        "expected the loop to retry past the scheduler error and register the next assignment"
    );

    token.cancel();
    join.await??;
    assert_eq!(scheduler.outstanding(), &[]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn stale_bundle_is_dropped_without_register() -> anyhow::Result<()> {
    const CURRENT_SESSION: SessionId = 10;
    const STALE_SESSION: SessionId = 5;
    const { assert!(CURRENT_SESSION > STALE_SESSION) };

    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::with_initial_session(CURRENT_SESSION);

    let (runtime, token) = Runtime::create(
        scheduler.clone(),
        storage.clone(),
        Arc::new(liveness),
        runtime_config(HEARTBEAT_INTERVAL),
    )
    .await?;

    scheduler.push(Ok(assignment_with_session(STALE_SESSION)));
    let join = tokio::spawn(runtime.run());

    assert!(
        wait_until(|| scheduler.call_count() >= 2, BOUNDED_WAIT).await,
        "expected scheduler to be polled again after dropping stale bundle; call_count = {}",
        scheduler.call_count()
    );
    assert!(
        storage.register_calls().is_empty(),
        "storage should not be touched for a stale bundle"
    );

    token.cancel();
    join.await??;
    assert_eq!(scheduler.outstanding(), &[]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn newer_bundle_triggers_liveness_refresh() -> anyhow::Result<()> {
    const CURRENT_SESSION: SessionId = 5;
    const LATEST_SESSION: SessionId = 10;

    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::with_initial_session(CURRENT_SESSION);

    // Slow interval so we can be sure the second observed heartbeat is the refresh-induced one
    // (the periodic tick is 5 s away).
    let (runtime, token) = Runtime::create(
        scheduler.clone(),
        storage.clone(),
        Arc::new(liveness.clone()),
        runtime_config(SLOW_HEARTBEAT_INTERVAL),
    )
    .await?;

    // Wait for the periodic-interval's leading tick to settle so the count is a clean baseline.
    assert!(liveness.wait_for_heartbeats(1, BOUNDED_WAIT).await);
    let baseline = liveness.heartbeat_count();

    // The newer-session bundle: the runtime should call `LivenessHandle::refresh` before
    // registering. Drop the bundle on the storage side to keep the test focused on the refresh.
    scheduler.push(Ok(assignment_with_session(LATEST_SESSION)));
    storage.push_register_response(Err(StorageResponseError::CacheStale(
        "test drop".to_owned(),
    )));
    let join = tokio::spawn(runtime.run());

    assert!(
        liveness.wait_for_heartbeats(baseline + 1, TIGHT_WAIT).await,
        "expected an extra heartbeat (refresh) within {TIGHT_WAIT:?}; heartbeats = {}",
        liveness.heartbeat_count()
    );

    token.cancel();
    join.await??;
    assert_eq!(scheduler.outstanding(), &[]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn equal_session_passes_through_to_register() -> anyhow::Result<()> {
    const SESSION_ID: SessionId = 5;

    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::with_initial_session(SESSION_ID);

    let (runtime, token) = Runtime::create(
        scheduler.clone(),
        storage.clone(),
        Arc::new(liveness),
        runtime_config(HEARTBEAT_INTERVAL),
    )
    .await?;

    // Bundle session matches the tracker exactly — runtime should skip triage and call register.
    // Drop on the storage side so we don't need a real execution.
    scheduler.push(Ok(assignment_with_session(SESSION_ID)));
    storage.push_register_response(Err(StorageResponseError::CacheStale(
        "test drop".to_owned(),
    )));
    let join = tokio::spawn(runtime.run());

    assert!(
        wait_until(|| !storage.register_calls().is_empty(), BOUNDED_WAIT).await,
        "expected register_task_instance to be called with the bundle's session id"
    );
    let calls = storage.register_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].session_id, SESSION_ID);

    token.cancel();
    join.await??;
    assert_eq!(scheduler.outstanding(), &[]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn stale_session_drops_assignment_and_refreshes() -> anyhow::Result<()> {
    const CURRENT_SESSION: SessionId = 10;
    const STALE_SESSION: SessionId = 5;

    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::with_initial_session(STALE_SESSION);

    let (runtime, token) = Runtime::create(
        scheduler.clone(),
        storage.clone(),
        Arc::new(liveness.clone()),
        runtime_config(SLOW_HEARTBEAT_INTERVAL),
    )
    .await?;

    assert!(liveness.wait_for_heartbeats(1, BOUNDED_WAIT).await);
    let baseline = liveness.heartbeat_count();

    scheduler.push(Ok(assignment_with_session(STALE_SESSION)));
    storage.push_register_response(Err(StorageResponseError::StaleSession(format!(
        "storage now at {CURRENT_SESSION}"
    ))));
    let join = tokio::spawn(runtime.run());

    // Stale-session response triggers liveness refresh and drops the assignment.
    assert!(
        liveness.wait_for_heartbeats(baseline + 1, TIGHT_WAIT).await,
        "expected refresh-induced heartbeat after StaleSession; heartbeats = {}",
        liveness.heartbeat_count()
    );
    assert!(
        wait_until(|| scheduler.call_count() >= 2, BOUNDED_WAIT).await,
        "expected scheduler to be polled again after stale assignment was dropped"
    );
    assert_eq!(storage.register_calls().len(), 1);
    assert!(storage.success_reports().is_empty());
    assert!(storage.failure_reports().is_empty());

    token.cancel();
    join.await??;
    assert_eq!(scheduler.outstanding(), &[]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn recoverable_storage_errors_drop_assignment() -> anyhow::Result<()> {
    const SESSION_ID: SessionId = 5;

    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::with_initial_session(SESSION_ID);

    let (runtime, token) = Runtime::create(
        scheduler.clone(),
        storage.clone(),
        Arc::new(liveness),
        runtime_config(HEARTBEAT_INTERVAL),
    )
    .await?;

    // Two bundles, two recoverable register failures. Each one should cause the loop to drop the
    // assignment and poll the scheduler again.
    let recoverable_errors = [
        StorageResponseError::CacheStale("stale cache".to_owned()),
        StorageResponseError::StaleSession(format!("storage now at {}", SESSION_ID + 1)),
    ];
    let num_errors = recoverable_errors.len() as u64;
    for err in recoverable_errors {
        scheduler.push(Ok(assignment_with_session(SESSION_ID)));
        storage.push_register_response(Err(err));
    }
    let join = tokio::spawn(runtime.run());

    assert!(
        wait_until(|| scheduler.call_count() >= (num_errors + 1), BOUNDED_WAIT).await,
        "expected {} drops + 1 idle poll; call_count = {}",
        num_errors,
        scheduler.call_count()
    );
    assert_eq!(storage.register_calls().len(), usize::try_from(num_errors)?);
    assert!(storage.success_reports().is_empty());
    assert!(storage.failure_reports().is_empty());

    token.cancel();
    join.await??;
    assert_eq!(scheduler.outstanding(), &[]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn success_outcome_reports_outputs() -> anyhow::Result<()> {
    const SESSION_ID: SessionId = 5;

    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::with_initial_session(SESSION_ID);

    let (runtime, token) = Runtime::create(
        scheduler.clone(),
        storage.clone(),
        Arc::new(liveness.clone()),
        runtime_config(HEARTBEAT_INTERVAL),
    )
    .await?;
    let em_id = liveness.em_id();

    let response = assignment_with_session(SESSION_ID);
    scheduler.push(Ok(response));
    storage.push_register_response(Ok(execution_context("fibonacci", single_input(&10_u64))));
    let join = tokio::spawn(runtime.run());

    assert!(storage.wait_for_any_report(BOUNDED_WAIT).await);
    let reports = storage.success_reports();
    assert_eq!(reports.len(), 1);
    let report = &reports[0];
    assert_eq!(report.job_id, response.task_assignment.job_id);
    assert_eq!(report.task_id, response.task_assignment.task_id);
    assert_eq!(report.task_instance_id, 1);
    assert_eq!(report.em_id, em_id);
    assert_eq!(report.session_id, SESSION_ID);
    let outputs = report
        .serialized_outputs
        .as_ref()
        .context("success report should carry outputs")?;
    assert_eq!(decode_single_output::<u64>(outputs), 55);
    assert!(storage.failure_reports().is_empty());

    token.cancel();
    join.await??;
    assert_eq!(scheduler.outstanding(), &[]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn non_success_outcome_keeps_loop_serving() -> anyhow::Result<()> {
    const SESSION_ID: SessionId = 5;

    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::with_initial_session(SESSION_ID);

    let (runtime, token) = Runtime::create(
        scheduler.clone(),
        storage.clone(),
        Arc::new(liveness),
        runtime_config(HEARTBEAT_INTERVAL),
    )
    .await?;

    // First bundle: always_fail. Second bundle: fibonacci. If the loop bails after a failure
    // outcome, the second bundle never reaches register / report.
    scheduler.push(Ok(assignment_with_session(SESSION_ID)));
    storage.push_register_response(Ok(execution_context("always_fail", vec![])));
    scheduler.push(Ok(assignment_with_session(SESSION_ID)));
    storage.push_register_response(Ok(execution_context("fibonacci", single_input(&10_u64))));
    let join = tokio::spawn(runtime.run());

    assert!(
        wait_until(
            || !storage.failure_reports().is_empty() && !storage.success_reports().is_empty(),
            BOUNDED_WAIT,
        )
        .await,
        "expected one failure (always_fail) and one success (fibonacci) report; got success={} \
         failure={}",
        storage.success_reports().len(),
        storage.failure_reports().len()
    );

    token.cancel();
    join.await??;
    assert_eq!(scheduler.outstanding(), &[]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn storage_report_error_does_not_kill_runtime() -> anyhow::Result<()> {
    const SESSION_ID: SessionId = 5;

    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::with_initial_session(SESSION_ID);

    let (runtime, token) = Runtime::create(
        scheduler.clone(),
        storage.clone(),
        Arc::new(liveness),
        runtime_config(HEARTBEAT_INTERVAL),
    )
    .await?;

    // The first success report fails, the second succeeds. The runtime should keep serving
    // assignments either way.
    storage.push_success_response(Err(StorageResponseError::Server("report boom".to_owned())));
    scheduler.push(Ok(assignment_with_session(SESSION_ID)));
    storage.push_register_response(Ok(execution_context("fibonacci", single_input(&10_u64))));
    scheduler.push(Ok(assignment_with_session(SESSION_ID)));
    storage.push_register_response(Ok(execution_context("fibonacci", single_input(&10_u64))));
    let join = tokio::spawn(runtime.run());

    assert!(
        wait_until(|| storage.success_reports().len() >= 2, BOUNDED_WAIT).await,
        "expected two success reports; got {}",
        storage.success_reports().len()
    );

    token.cancel();
    join.await??;
    assert_eq!(scheduler.outstanding(), &[]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn drop_guard_cancels_token_when_run_future_dropped() -> anyhow::Result<()> {
    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::new();

    let (runtime, _token) = Runtime::create(
        scheduler,
        storage,
        Arc::new(liveness.clone()),
        runtime_config(HEARTBEAT_INTERVAL),
    )
    .await?;

    // Make sure the actor is actively ticking before we drop the runtime.
    assert!(liveness.wait_for_heartbeats(2, BOUNDED_WAIT).await);

    // Dropping the `runtime.run()` future inside a short timeout drops the Runtime itself, which
    // fires the `DropGuard` and cancels the token the liveness actor watches.
    let timeout_result = tokio::time::timeout(Duration::from_millis(150), runtime.run()).await;
    assert!(
        timeout_result.is_err(),
        "run unexpectedly returned within the timeout window: {timeout_result:?}"
    );

    // Give the actor a moment to observe cancellation and drain any in-flight heartbeat call.
    tokio::time::sleep(2 * HEARTBEAT_INTERVAL).await;
    let snapshot = liveness.heartbeat_count();

    // Five heartbeat intervals must elapse without the counter advancing.
    tokio::time::sleep(5 * HEARTBEAT_INTERVAL).await;
    let current = liveness.heartbeat_count();
    assert_eq!(
        current, snapshot,
        "liveness actor kept heartbeating after Runtime drop; was {snapshot}, now {current}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn liveness_and_scheduler_heartbeats_advance_in_lockstep() -> anyhow::Result<()> {
    const SESSION_ID: SessionId = 5;
    // Run for 3.5 intervals so the runtime is dropped mid-interval — comfortably clear of a tick
    // edge — leaving both heartbeat loops with the same count instead of racing one extra tick at
    // the boundary.
    const RUN_WINDOW: Duration = Duration::from_millis(350);
    const { assert!(RUN_WINDOW.as_millis() == 3 * HEARTBEAT_INTERVAL.as_millis() + 50) };

    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::with_initial_session(SESSION_ID);

    let (runtime, _token) = Runtime::create(
        scheduler.clone(),
        storage.clone(),
        Arc::new(liveness.clone()),
        runtime_config(HEARTBEAT_INTERVAL),
    )
    .await?;

    // A single assignment that storage skips without execution via a recoverable error that does
    // not refresh liveness, so the loop keeps serving (no real task is run) while both heartbeat
    // tasks tick off the same interval.
    scheduler.push(Ok(assignment_with_session(SESSION_ID)));
    storage.push_register_response(Err(StorageResponseError::CacheStale("skip".to_owned())));

    // Drop the runtime after the window: dropping the `run` future fires the `DropGuard`, which
    // cancels the liveness actor and the scheduler heartbeat task at the same point.
    let timeout_result = tokio::time::timeout(RUN_WINDOW, runtime.run()).await;
    assert!(
        timeout_result.is_err(),
        "run unexpectedly returned within the window: {timeout_result:?}"
    );

    // Let both tasks observe cancellation and stop before sampling their counters.
    tokio::time::sleep(2 * HEARTBEAT_INTERVAL).await;

    let liveness_beats = liveness.heartbeat_count();
    let scheduler_beats = scheduler.heartbeat_count();
    assert!(
        liveness_beats >= 2 && scheduler_beats >= 2,
        "expected both heartbeat loops to have ticked several times; liveness = {liveness_beats}, \
         scheduler = {scheduler_beats}"
    );
    assert_eq!(
        liveness_beats, scheduler_beats,
        "liveness and scheduler heartbeats should advance in lockstep off the same interval"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn cancellation_during_register_leaves_assignment_unacked() -> anyhow::Result<()> {
    const SESSION_ID: SessionId = 5;

    let scheduler = MockScheduler::new();
    let storage = MockStorage::new();
    let liveness = MockLiveness::with_initial_session(SESSION_ID);

    let (runtime, token) = Runtime::create(
        scheduler.clone(),
        storage.clone(),
        Arc::new(liveness),
        runtime_config(HEARTBEAT_INTERVAL),
    )
    .await?;

    // Storage parks the registration so we can cancel the runtime while it is in flight. The
    // assignment is therefore never "truly returned from storage", so it must not be acknowledged.
    storage.block_register();
    let response = assignment_with_session(SESSION_ID);
    scheduler.push(Ok(response));
    let join = tokio::spawn(runtime.run());

    // Wait until the registration is in flight (recorded, then parked) before cancelling, so the
    // cancellation lands inside `register_task_instance`.
    assert!(
        wait_until(|| !storage.register_calls().is_empty(), BOUNDED_WAIT).await,
        "expected the runtime to enter register_task_instance before cancelling"
    );

    token.cancel();
    join.await??;

    // Cancellation mid-register pushes nothing into `prev_assignments`, so the runtime never
    // acknowledges the assignment: the scheduler still holds it as outstanding after the runtime
    // exits.
    let expected = TaskAssignmentRecord::new(response.task_assignment.id, response.scheduler_id);
    assert_eq!(scheduler.outstanding(), &[expected]);
    Ok(())
}
