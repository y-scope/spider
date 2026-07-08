//! End-to-end tests of [`spider_execution_manager::process_pool::ProcessPool`] against the real
//! task-executor binary.
//!
//! Mirrors `tests/executor.rs` but exercises the pool's `execute` API rather than the raw
//! [`task_executor_tests::ExecutorHandle`]. Adds coverage for the two paths that respawn the
//! executor:
//!
//! * Hard timeout — a long-running task is force-killed when the parent's timer fires.
//! * Crash — a panicking task aborts the executor process.
//!
//! Each of those paths is followed by a second `execute` that asserts the pool transparently
//! respawned the child and is ready to serve again.

use std::time::Duration;

use spider_core::task::TdlContext;
use spider_core::task::TimeoutPolicy;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::io::ExecutionContext;
use spider_core::types::io::TaskInput;
use spider_core::types::io::TaskInputsSerializer;
use spider_execution_manager::process_pool::ExecuteRequest;
use spider_execution_manager::process_pool::Outcome;
use spider_execution_manager::process_pool::ProcessPool;
use spider_execution_manager::process_pool::ProcessPoolConfig;
use spider_task_executor::ExecutorError;
use spider_tdl::TdlError;
use test_utils::PACKAGE_NAME;
use test_utils::decode_single_output;
use test_utils::single_input;
use test_utils::task_executor_bin;
use test_utils::tdl_package_dir;

/// Generous timeout for tasks expected to finish quickly.
const NORMAL_TIMEOUT: Duration = Duration::from_secs(5);

/// Hard timeout chosen to fire well before [`SLOW_FIB_INDEX`] can complete even on a fast host.
/// Tokio's sleep granularity is comfortably below this value.
const SHORT_TIMEOUT: Duration = Duration::from_millis(200);

/// Fibonacci index whose naive-recursive execution takes well over [`SHORT_TIMEOUT`] on any
/// realistic host (`fib(45)` ~= 1.1×10^9 recursive calls — about a second in release mode).
const SLOW_FIB_INDEX: u64 = 45;

/// Builds a fresh [`ProcessPool`] wired to the test-harness env (executor binary + package dir)
/// with a unique temp log directory.
///
/// # Returns
///
/// A ready-to-use pool whose handle already holds a spawned executor.
///
/// # Panics
///
/// Panics if [`ProcessPool::new`] fails — i.e., the task-executor binary cannot be spawned.
fn build_pool() -> ProcessPool {
    let em_id = ExecutionManagerId::random();
    let log_dir = std::env::temp_dir().join(format!("spider-em-pool-test-{em_id}"));
    let config = ProcessPoolConfig {
        em_id,
        executor_binary_path: task_executor_bin(),
        package_dir: tdl_package_dir(),
        log_dir,
    };
    ProcessPool::new(config).expect("construct pool")
}

/// Builds an [`ExecuteRequest`] targeting `task_func` in the integration package.
///
/// # Returns
///
/// A request with fresh IDs, a placeholder [`TimeoutPolicy`] (which the pool ignores — the caller
/// supplies `hard_timeout` directly to [`ProcessPool::execute`]), and the supplied `inputs`.
fn make_request(task_func: &str, inputs: Vec<TaskInput>) -> ExecuteRequest {
    let mut serializer = TaskInputsSerializer::new();
    for input in inputs {
        serializer
            .append(input)
            .expect("input serialization should succeed");
    }

    ExecuteRequest {
        job_id: JobId::random(),
        task_id: TaskId::Index(0),
        resource_group_id: ResourceGroupId::random(),
        ctx: ExecutionContext {
            task_instance_id: 1,
            tdl_context: TdlContext {
                package: PACKAGE_NAME.to_owned(),
                task_func: task_func.to_owned(),
            },
            timeout_policy: TimeoutPolicy {
                soft_timeout_ms: 100,
                hard_timeout_ms: 1000,
            },
            serialized_inputs: serializer.release(),
        },
    }
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn fibonacci_succeeds() {
    let pool = build_pool();
    let outcome = pool
        .execute(
            make_request("fibonacci", single_input(&10_u64)),
            NORMAL_TIMEOUT,
        )
        .await
        .expect("execute");
    let Outcome::Success { outputs, .. } = outcome else {
        panic!("expected Success, got {outcome:?}");
    };
    assert_eq!(decode_single_output::<u64>(&outputs), 55);
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn always_fail_reports_task_error() {
    let pool = build_pool();
    let outcome = pool
        .execute(make_request("always_fail", vec![]), NORMAL_TIMEOUT)
        .await
        .expect("execute");
    let Outcome::InTaskFailure { error, .. } = outcome else {
        panic!("expected InTaskFailure, got {outcome:?}");
    };
    let err: ExecutorError = rmp_serde::from_slice(&error).expect("decode ExecutorError");
    let ExecutorError::TaskError(TdlError::ExecutionError(message)) = err else {
        panic!("expected TaskError(ExecutionError), got {err:?}");
    };
    assert!(
        message.contains("always_fail"),
        "unexpected message: {message}"
    );
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn always_panic_returns_crash_then_respawns() {
    let pool = build_pool();

    let outcome = pool
        .execute(make_request("always_panic", vec![]), NORMAL_TIMEOUT)
        .await
        .expect("execute (crash)");
    assert!(
        matches!(outcome, Outcome::ExecutorCrash { .. }),
        "expected ExecutorCrash, got {outcome:?}",
    );

    // The pool must have respawned the executor before returning. A follow-up call must succeed
    // against the fresh process.
    let outcome = pool
        .execute(
            make_request("fibonacci", single_input(&7_u64)),
            NORMAL_TIMEOUT,
        )
        .await
        .expect("execute (after respawn)");
    let Outcome::Success { outputs, .. } = outcome else {
        panic!("expected Success after respawn, got {outcome:?}");
    };
    assert_eq!(decode_single_output::<u64>(&outputs), 13);
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn hard_timeout_kills_then_respawns() {
    let pool = build_pool();

    let outcome = pool
        .execute(
            make_request("fibonacci", single_input(&SLOW_FIB_INDEX)),
            SHORT_TIMEOUT,
        )
        .await
        .expect("execute (timeout)");
    let Outcome::Timeout { hard_timeout } = outcome else {
        panic!("expected Timeout, got {outcome:?}");
    };
    assert_eq!(hard_timeout, SHORT_TIMEOUT);

    // The pool must have respawned the executor before returning. A follow-up call must succeed
    // against the fresh process.
    let outcome = pool
        .execute(
            make_request("fibonacci", single_input(&7_u64)),
            NORMAL_TIMEOUT,
        )
        .await
        .expect("execute (after respawn)");
    let Outcome::Success { outputs, .. } = outcome else {
        panic!("expected Success after respawn, got {outcome:?}");
    };
    assert_eq!(decode_single_output::<u64>(&outputs), 13);
}
