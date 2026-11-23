//! End-to-end tests of [`spider_execution_manager::process_pool::ProcessPool`] against the real
//! task-executor binary.
//!
//! Mirrors `tests/executor.rs` but exercises the pool's `execute` API rather than the raw
//! [`task_executor_tests::ExecutorHandle`]. Adds coverage for the paths that respawn the executor:
//!
//! * Hard timeout — a long-running task is force-killed when the parent's timer fires.
//! * Invalid response — malformed and oversized frames require a fresh executor.
//! * Crash — a panicking task aborts the executor process.
//!
//! Each of those paths is followed by a second `execute` that asserts the pool transparently
//! respawned the child and is ready to serve again.

use std::num::NonZeroUsize;
use std::time::Duration;

use spider_core::task::TdlContext;
use spider_core::task::TimeoutPolicy;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::io::ExecutionContext;
use spider_core::types::io::TaskInput;
use spider_core::types::io::TaskInputsSerializer;
use spider_execution_manager::executor_log_collection::{self};
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
use tokio_util::sync::CancellationToken;

/// Generous timeout for tasks expected to finish quickly.
const NORMAL_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum length, in bytes, of a captured executor log line.
const MAX_LOG_LINE_BYTES: usize = 64 * 1024;

/// Hard timeout chosen to fire well before [`SLOW_FIB_INDEX`] can complete even on a fast host.
/// Tokio's sleep granularity is comfortably below this value.
const SHORT_TIMEOUT: Duration = Duration::from_millis(200);

/// Fibonacci index whose naive-recursive execution takes well over [`SHORT_TIMEOUT`] on any
/// realistic host (`fib(45)` ~= 1.1×10^9 recursive calls — about a second in release mode).
const SLOW_FIB_INDEX: u64 = 45;

/// Builds a fresh [`ProcessPool`] wired to the test-harness env (executor binary + package dir),
/// with the executors' logs forwarded into a sink.
///
/// Must be called from within a tokio runtime: both the log actor and the pool's first executor are
/// spawned on the current runtime.
///
/// # Returns
///
/// A ready-to-use pool whose handle already holds a spawned executor.
///
/// # Panics
///
/// Panics if [`ProcessPool::new`] fails — i.e., the task-executor binary cannot be spawned.
fn build_pool() -> ProcessPool {
    let (log_handle, _log_join) =
        executor_log_collection::spawn(tokio::io::sink(), CancellationToken::new());
    let config = ProcessPoolConfig {
        executor_binary_path: task_executor_bin(),
        package_dir: tdl_package_dir(),
        max_log_line_bytes: NonZeroUsize::new(MAX_LOG_LINE_BYTES).expect("non-zero line cap"),
        inherited_env: Vec::new(),
    };
    ProcessPool::new(config, log_handle).expect("construct pool")
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
            serialized_task_io: serializer.release(),
        },
    }
}

/// Runs an invalid first response followed by a real executor in a replacement process.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`std::fs::create_dir`]'s return values on failure.
/// * Forwards [`std::os::unix::fs::symlink`]'s return values on failure.
/// * Forwards [`std::fs::write`]'s return values on failure.
/// * Forwards [`std::fs::set_permissions`]'s return values on failure.
/// * Forwards [`ProcessPool::new`]'s return values on failure.
/// * Forwards [`ProcessPool::execute`]'s return values on failure.
/// * Forwards [`std::fs::read_to_string`]'s return values on failure.
/// * Forwards [`std::fs::remove_dir_all`]'s return values on failure.
#[cfg(unix)]
async fn assert_invalid_response_respawns(frame: &str) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let directory =
        std::env::temp_dir().join(format!("spider-invalid-response-{}", JobId::random()));
    std::fs::create_dir(&directory)?;
    let script = directory.join("executor");
    let processes = directory.join("processes");
    std::os::unix::fs::symlink(task_executor_bin(), directory.join("real-executor"))?;
    std::fs::write(
        &script,
        format!(
            r#"#!/bin/sh
directory=${{0%/*}}
echo "$$" >> "$directory/processes"
if [ ! -e "$directory/spawned" ]; then
  touch "$directory/spawned"
  printf '{frame}'
  exec sleep 60
fi
exec "$directory/real-executor"
"#
        ),
    )?;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700))?;
    let (log_handle, _log_join) =
        executor_log_collection::spawn(tokio::io::sink(), CancellationToken::new());
    let pool = ProcessPool::new(
        ProcessPoolConfig {
            executor_binary_path: script,
            package_dir: tdl_package_dir(),
            max_log_line_bytes: NonZeroUsize::new(MAX_LOG_LINE_BYTES).expect("non-zero line cap"),
            inherited_env: Vec::new(),
        },
        log_handle,
    )?;
    let outcome = pool
        .execute(
            make_request("fibonacci", single_input(&7_u64)),
            NORMAL_TIMEOUT,
        )
        .await?;
    assert!(
        matches!(outcome, Outcome::InvalidResponse { .. }),
        "got {outcome:?}"
    );
    let outcome = pool
        .execute(
            make_request("fibonacci", single_input(&7_u64)),
            NORMAL_TIMEOUT,
        )
        .await?;
    let Outcome::Success { outputs, .. } = outcome else {
        panic!("expected Success after respawn, got {outcome:?}");
    };
    assert_eq!(decode_single_output::<u64>(&outputs), 13);
    let process_ids = std::fs::read_to_string(processes)?;
    let process_ids: Vec<_> = process_ids.lines().collect();
    assert_eq!(process_ids.len(), 2);
    assert_ne!(process_ids[0], process_ids[1]);
    drop(pool);
    std::fs::remove_dir_all(directory)?;
    Ok(())
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

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn oversized_frame_respawns_executor() -> anyhow::Result<()> {
    assert_invalid_response_respawns(r"\377\377\377\377").await
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn malformed_frame_respawns_executor() -> anyhow::Result<()> {
    assert_invalid_response_respawns(r"\000\000\000\001\377").await
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn response_larger_than_eight_mib_preserves_outputs() -> anyhow::Result<()> {
    const PAYLOAD_SIZE: usize = 10 * 1024 * 1024;
    let pool = build_pool();
    let outcome = pool
        .execute(
            make_request("generate_payload", single_input(&PAYLOAD_SIZE)),
            NORMAL_TIMEOUT,
        )
        .await?;
    let Outcome::Success {
        outputs, children, ..
    } = outcome
    else {
        panic!("expected Success, got {outcome:?}");
    };
    assert!(outputs.len() > 8 * 1024 * 1024);
    assert_eq!(
        decode_single_output::<String>(&outputs),
        "x".repeat(PAYLOAD_SIZE)
    );
    assert_eq!(
        children,
        [] as [spider_core::job::submission::CompressedJobSubmission; 0]
    );
    Ok(())
}
