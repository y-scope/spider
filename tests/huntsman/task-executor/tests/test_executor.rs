//! End-to-end correctness tests against the `spider-task-executor` binary.
//!
//! Tests spawn fresh executor subprocesses via [`ExecutorHandle::spawn`], exchange framed bincode
//! requests and responses over the binary's stdin/stdout, and assert on the results.

use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::io::SerializedTaskOutputs;
use spider_task_executor::ExecutorError;
use spider_task_executor::protocol::ExecutorOutcome;
use spider_task_executor::protocol::Request;
use spider_task_executor::protocol::Response;
use spider_tdl::TaskContext;
use spider_tdl::TdlError;
use test_utils::ExecutorHandle;
use test_utils::commit_execute_request;
use test_utils::decode_single_output;
use test_utils::encode_no_inputs;
use test_utils::encode_single_input;
use test_utils::execute_request;

/// Asserts that `outcome` is a task [`TdlError::ExecutionError`] whose message contains `needle`.
fn assert_task_execution_error(outcome: ExecutorOutcome, needle: &str) {
    match outcome {
        ExecutorOutcome::Success { outputs, .. } => {
            panic!("expected Failure, got Success with {} bytes", outputs.len());
        }
        ExecutorOutcome::Failure { error } => {
            let err: ExecutorError =
                rmp_serde::from_slice(&error).expect("decode ExecutorError payload");
            let ExecutorError::TaskError(TdlError::ExecutionError(message)) = &err else {
                panic!("expected TaskError(ExecutionError), got {err:?}");
            };
            assert!(
                message.contains(needle),
                "unexpected error message: {message}",
            );
        }
    }
}

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
        ExecutorOutcome::Success { outputs, children } => {
            assert_eq!(
                children,
                [] as [spider_core::job::submission::CompressedJobSubmission; 0]
            );
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
        ExecutorOutcome::Success { outputs, .. } => {
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

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn assert_outputs_sum_zero_succeeds_when_outputs_sum_to_zero() {
    let mut handle = ExecutorHandle::spawn();
    handle
        .send(&commit_execute_request(
            "assert_outputs_sum_zero",
            &[3_i64, -1, -2],
        ))
        .await;
    let Response::Result { outcome, .. } = handle.recv().await;
    match outcome {
        ExecutorOutcome::Success { .. } => {}
        ExecutorOutcome::Failure { error } => {
            let err: ExecutorError =
                rmp_serde::from_slice(&error).expect("decode ExecutorError payload");
            panic!("expected Success for zero-sum outputs, got Failure: {err:?}");
        }
    }
    handle.shutdown_clean().await;
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn assert_outputs_sum_zero_fails_when_outputs_do_not_sum_to_zero() {
    let mut handle = ExecutorHandle::spawn();
    handle
        .send(&commit_execute_request(
            "assert_outputs_sum_zero",
            &[1_i64, 2, 3],
        ))
        .await;
    let Response::Result { outcome, .. } = handle.recv().await;
    assert_task_execution_error(outcome, "sum to zero");
    handle.shutdown_clean().await;
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn assert_outputs_sum_zero_fails_when_outputs_empty() {
    let mut handle = ExecutorHandle::spawn();
    handle
        .send(&commit_execute_request::<i64>(
            "assert_outputs_sum_zero",
            &[],
        ))
        .await;
    let Response::Result { outcome, .. } = handle.recv().await;
    assert_task_execution_error(outcome, "empty");
    handle.shutdown_clean().await;
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn assert_outputs_sum_zero_fails_for_non_commit_task() {
    let mut handle = ExecutorHandle::spawn();
    handle
        .send(&execute_request(
            "assert_outputs_sum_zero",
            encode_no_inputs(),
        ))
        .await;
    let Response::Result { outcome, .. } = handle.recv().await;
    assert_task_execution_error(outcome, "commit task");
    handle.shutdown_clean().await;
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn response_larger_than_eight_mib_preserves_outputs() {
    const PAYLOAD_SIZE: usize = 10 * 1024 * 1024;
    let mut handle = ExecutorHandle::spawn();
    handle
        .send(&execute_request(
            "generate_payload",
            encode_single_input(&PAYLOAD_SIZE),
        ))
        .await;
    let Response::Result { outcome, .. } = handle.recv().await;
    let ExecutorOutcome::Success { outputs, children } = outcome else {
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
    handle.shutdown_clean().await;
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn child_submissions_preserve_storage_encoding_and_submission_order() -> anyhow::Result<()> {
    use spider_core::task::TaskGraph;
    use spider_core::types::io::TaskGraphInput;

    let mut handle = ExecutorHandle::spawn();
    for count in [2_u64, 1] {
        handle
            .send(&execute_request(
                "create_children",
                encode_single_input(&count),
            ))
            .await;
        let Response::Result { outcome, .. } = handle.recv().await;
        let ExecutorOutcome::Success { outputs, children } = outcome else {
            panic!("expected child-producing success, got {outcome:?}");
        };
        assert_eq!(decode_single_output::<u64>(&outputs), count);
        assert_eq!(children.len() as u64, count);
        for (index, child) in children.iter().enumerate() {
            let graph =
                TaskGraph::from_zstd_compressed_json(&child.compressed_serialized_task_graph)?;
            assert_eq!(graph.get_num_tasks(), 1);
            assert_eq!(
                graph
                    .get_task(0)
                    .expect("child graph contains task")
                    .get_tdl_context()
                    .task_func,
                "fibonacci"
            );
            let inputs = TaskGraphInput::from_zstd_compressed_bytes(
                &child.compressed_serialized_job_inputs,
            )?;
            let inputs = inputs.into_positional_inputs(|bytes| bytes);
            assert_eq!(inputs, vec![rmp_serde::to_vec(&(index as u64))?]);
        }
    }
    handle.shutdown_clean().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn failed_execution_discards_child_submissions() -> anyhow::Result<()> {
    let mut handle = ExecutorHandle::spawn();
    handle
        .send(&execute_request(
            "create_children_then_fail",
            encode_no_inputs(),
        ))
        .await;
    let Response::Result { outcome, .. } = handle.recv().await;
    let ExecutorOutcome::Failure { error } = outcome else {
        panic!("expected task failure, got {outcome:?}");
    };
    let error: ExecutorError = rmp_serde::from_slice(&error)?;
    assert!(
        matches!(error, ExecutorError::TaskError(TdlError::ExecutionError(_))),
        "got {error:?}"
    );
    handle
        .send(&execute_request("fibonacci", encode_single_input(&7_u64)))
        .await;
    let Response::Result { outcome, .. } = handle.recv().await;
    let ExecutorOutcome::Success { outputs, children } = outcome else {
        panic!("expected success after failed execution, got {outcome:?}");
    };
    assert_eq!(decode_single_output::<u64>(&outputs), 13);
    assert_eq!(children, []);
    handle.shutdown_clean().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn child_creation_followed_by_panic_does_not_report_success() -> anyhow::Result<()> {
    let mut handle = ExecutorHandle::spawn();
    handle
        .send(&execute_request(
            "create_children_then_panic",
            encode_no_inputs(),
        ))
        .await;
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
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn termination_tasks_reject_child_creation() -> anyhow::Result<()> {
    let mut handle = ExecutorHandle::spawn();
    for task_id in [TaskId::Commit, TaskId::Cleanup] {
        let outputs = if task_id == TaskId::Commit {
            Some(SerializedTaskOutputs::serialize_with_size_hint(&[])?.to_raw())
        } else {
            None
        };
        let context = TaskContext::new(
            JobId::random(),
            task_id,
            1,
            ResourceGroupId::random(),
            outputs,
        )?;
        let mut request = execute_request("create_children", encode_single_input(&1_u64));
        let Request::Execute { raw_ctx, .. } = &mut request else {
            panic!("expected execute request");
        };
        *raw_ctx = rmp_serde::to_vec(&context)?;
        handle.send(&request).await;
        let Response::Result { outcome, .. } = handle.recv().await;
        let ExecutorOutcome::Failure { error } = outcome else {
            panic!("expected child-creation rejection, got {outcome:?}");
        };
        let error: ExecutorError = rmp_serde::from_slice(&error)?;
        assert!(
            matches!(error, ExecutorError::TaskError(_)),
            "got {error:?}"
        );
    }
    handle.shutdown_clean().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib and `spider-task-executor` binary"]
async fn termination_task_outputs_survive_the_subprocess_protocol() -> anyhow::Result<()> {
    let mut handle = ExecutorHandle::spawn();
    for task_id in [TaskId::Commit, TaskId::Cleanup] {
        let outputs = if task_id == TaskId::Commit {
            Some(SerializedTaskOutputs::serialize_with_size_hint(&[])?.to_raw())
        } else {
            None
        };
        let context = TaskContext::new(
            JobId::random(),
            task_id,
            1,
            ResourceGroupId::random(),
            outputs,
        )?;
        let mut request = execute_request("fibonacci", encode_single_input(&7_u64));
        let Request::Execute { raw_ctx, .. } = &mut request else {
            panic!("expected execute request");
        };
        *raw_ctx = rmp_serde::to_vec(&context)?;
        handle.send(&request).await;
        let Response::Result { outcome, .. } = handle.recv().await;
        let ExecutorOutcome::Success { outputs, children } = outcome else {
            panic!("expected termination task success, got {outcome:?}");
        };
        assert_eq!(decode_single_output::<u64>(&outputs), 13);
        assert_eq!(children, []);
    }
    handle.shutdown_clean().await;
    Ok(())
}
