//! End-to-end correctness tests against the `spider-task-executor` binary.
//!
//! Each test spawns a fresh executor subprocess via [`ExecutorHandle::spawn`], exchanges one framed
//! bincode request/response over the binary's stdin/stdout, and asserts on the result.

use spider_task_executor::ExecutorError;
use spider_task_executor::protocol::ExecutorOutcome;
use spider_task_executor::protocol::Response;
use spider_tdl::TdlError;
use test_utils::ExecutorHandle;
use test_utils::decode_single_output;
use test_utils::encode_no_inputs;
use test_utils::encode_single_input;
use test_utils::execute_request;

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn fibonacci_returns_correct_value() {
    let mut handle = ExecutorHandle::spawn();
    let input: u64 = 10;
    handle
        .send(&execute_request("fibonacci", encode_single_input(&input)))
        .await;
    let Response::Result { outcome, .. } = handle.recv().await;
    match outcome {
        ExecutorOutcome::Success { outputs } => {
            let got: u64 = decode_single_output(&outputs);
            // Fib(10) = 55
            assert_eq!(got, 55);
        }
        ExecutorOutcome::Failure { error } => {
            let err: ExecutorError =
                rmp_serde::from_slice(&error).expect("decode ExecutorError payload");
            panic!("expected Success for fibonacci(10), got Failure: {err:?}");
        }
    }
    handle.shutdown_clean().await;
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn always_fail_reports_task_error() {
    let mut handle = ExecutorHandle::spawn();
    handle
        .send(&execute_request("always_fail", encode_no_inputs()))
        .await;
    let Response::Result { outcome, .. } = handle.recv().await;
    match outcome {
        ExecutorOutcome::Success { outputs } => {
            panic!("expected Failure, got Success with {} bytes", outputs.len());
        }
        ExecutorOutcome::Failure { error } => {
            let err: ExecutorError =
                rmp_serde::from_slice(&error).expect("decode ExecutorError payload");
            let ExecutorError::TaskError(TdlError::ExecutionError(message)) = &err else {
                panic!("expected TaskError(ExecutionError), got {err:?}");
            };
            assert!(
                message.contains("always_fail"),
                "unexpected error message: {message}",
            );
        }
    }
    handle.shutdown_clean().await;
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn always_panic_crashes_the_process() {
    let mut handle = ExecutorHandle::spawn();
    handle
        .send(&execute_request("always_panic", encode_no_inputs()))
        .await;

    // A panic across the `extern "C"` boundary aborts the executor process. The parent must
    // observe stdout EOF (no further frames) and a non-zero exit status.
    let frame = handle.try_recv().await;
    assert!(
        frame.is_none(),
        "expected stdout EOF after panic, got a response frame: {frame:?}",
    );
    let status = handle.wait_for_exit().await;
    assert!(
        !status.success(),
        "expected non-zero exit after panic, got {status:?}",
    );
}
