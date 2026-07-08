//! Executor subprocess harness plus the TDL wire-payload helpers the integration suites share.
//!
//! [`ExecutorHandle`] spawns the `spider-task-executor` binary as a child process, frames bincode
//! requests on its stdin and reads bincode responses from its stdout — the exact wire protocol of
//! [`spider_task_executor::protocol`].
//!
//! Every fallible operation in this harness panics with `.expect(...)` on failure; the tests are
//! infrastructure, not production code, and the panic message + backtrace is more useful at the
//! failure site than threading an error type through every helper.
//!
//! Environment:
//!
//! * `SPIDER_TASK_EXECUTOR_BIN` — absolute path to the executor binary.
//! * `SPIDER_TDL_PACKAGE_DIR` — directory the binary searches for TDL packages; gets forwarded to
//!   the child verbatim.

use std::path::PathBuf;
use std::process::Stdio;

use bytes::Bytes;
use futures_util::SinkExt;
use futures_util::StreamExt;
use spider_core::task::TdlContext;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::io::TaskInput;
use spider_core::types::io::TaskInputsSerializer;
use spider_core::types::io::TaskOutputsSerializer;
use spider_task_executor::protocol::Request;
use spider_task_executor::protocol::Response;
use spider_tdl::TaskContext;
use tokio::process::Child;
use tokio::process::ChildStdin;
use tokio::process::ChildStdout;
use tokio::process::Command;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;
use tokio_util::codec::LengthDelimitedCodec;

/// The TDL package name registered by `integration-test-tasks`.
pub const PACKAGE_NAME: &str = "integration_test_tasks";

/// One running executor subprocess plus framed handles to its stdin / stdout.
///
/// The subprocess will be killed when the handle is dropped.
pub struct ExecutorHandle {
    child: Child,
    requests: FramedWrite<ChildStdin, LengthDelimitedCodec>,
    responses: FramedRead<ChildStdout, LengthDelimitedCodec>,
}

impl ExecutorHandle {
    /// Spawns the executor binary with `SPIDER_TDL_PACKAGE_DIR` set; the child inherits the
    /// parent's stderr so panic / abort messages surface in the test log.
    ///
    /// # Returns
    ///
    /// A handle owning the running subprocess and framed I/O.
    ///
    /// # Panics
    ///
    /// Panics if the binary cannot be spawned or its stdio handles cannot be claimed.
    #[must_use]
    pub fn spawn() -> Self {
        let mut child = Command::new(task_executor_bin())
            .env("SPIDER_TDL_PACKAGE_DIR", tdl_package_dir())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn executor binary");
        let stdin = child.stdin.take().expect("stdin must be piped");
        let stdout = child.stdout.take().expect("stdout must be piped");
        Self {
            child,
            requests: FramedWrite::new(stdin, LengthDelimitedCodec::new()),
            responses: FramedRead::new(stdout, LengthDelimitedCodec::new()),
        }
    }

    /// Bincode-serializes `req` and writes one length-delimited frame to the executor's stdin.
    ///
    /// # Panics
    ///
    /// Panics if encoding fails or the stdin pipe cannot be written.
    pub async fn send(&mut self, req: &Request) {
        let bytes = bincode::serialize(req).expect("bincode encode Request");
        self.requests
            .send(Bytes::from(bytes))
            .await
            .expect("write request frame");
    }

    /// Reads exactly one length-delimited frame from the executor's stdout and bincode-decodes it.
    ///
    /// # Returns
    ///
    /// The next [`Response`] from the executor.
    ///
    /// # Panics
    ///
    /// Panics if stdout closes before a frame arrives, the frame I/O fails, or decoding fails.
    pub async fn recv(&mut self) -> Response {
        let frame = self
            .responses
            .next()
            .await
            .expect("executor closed stdout before reply")
            .expect("read response frame");
        bincode::deserialize(&frame).expect("bincode decode Response")
    }

    /// Reads at most one length-delimited frame, tolerating a clean EOF (which crash-path tests
    /// rely on to detect that the executor died).
    ///
    /// # Returns
    ///
    /// `Some(response)` if a frame was received, `None` if stdout closed cleanly first.
    ///
    /// # Panics
    ///
    /// Panics if the frame I/O fails for a reason other than EOF or if decoding fails.
    pub async fn try_recv(&mut self) -> Option<Response> {
        let frame = self.responses.next().await?;
        let bytes = frame.expect("read response frame");
        Some(bincode::deserialize(&bytes).expect("bincode decode Response"))
    }

    /// Sends [`Request::Shutdown`], closes stdin, and waits for the child to exit cleanly.
    ///
    /// # Panics
    ///
    /// Panics if waiting on the child fails or the child exits non-zero.
    pub async fn shutdown_clean(mut self) {
        self.send(&Request::Shutdown).await;
        // Close the stdin pipe so the child sees EOF after `Shutdown` is drained.
        drop(self.requests);
        let status = self.child.wait().await.expect("wait for executor");
        assert!(status.success(), "executor exited with status {status:?}");
    }

    /// Closes stdin and waits for the child to exit. Used by crash-path tests that don't expect
    /// a clean shutdown.
    ///
    /// # Returns
    ///
    /// The child's [`ExitStatus`](std::process::ExitStatus).
    ///
    /// # Panics
    ///
    /// Panics if waiting on the child fails.
    pub async fn wait_for_exit(mut self) -> std::process::ExitStatus {
        drop(self.requests);
        self.child.wait().await.expect("wait for executor")
    }
}

/// # Returns
///
/// The absolute path of the `spider-task-executor` binary, read from `SPIDER_TASK_EXECUTOR_BIN`.
///
/// # Panics
///
/// Panics if `SPIDER_TASK_EXECUTOR_BIN` is unset.
#[must_use]
pub fn task_executor_bin() -> PathBuf {
    std::env::var_os("SPIDER_TASK_EXECUTOR_BIN")
        .map(PathBuf::from)
        .expect("SPIDER_TASK_EXECUTOR_BIN env var not set")
}

/// # Returns
///
/// The TDL package staging directory, read from `SPIDER_TDL_PACKAGE_DIR`.
///
/// # Panics
///
/// Panics if `SPIDER_TDL_PACKAGE_DIR` is unset.
#[must_use]
pub fn tdl_package_dir() -> PathBuf {
    std::env::var_os("SPIDER_TDL_PACKAGE_DIR")
        .map(PathBuf::from)
        .expect("SPIDER_TDL_PACKAGE_DIR env var not set")
}

/// # Returns
///
/// A placeholder msgpack-encoded [`TaskContext`] suitable for a one-shot test invocation. The id
/// fields are fresh per call but the executor doesn't inspect them.
///
/// # Panics
///
/// Panics if msgpack encoding fails.
#[must_use]
pub fn build_ctx() -> Vec<u8> {
    let ctx = TaskContext {
        job_id: JobId::random(),
        task_id: TaskId::Index(0),
        task_instance_id: 1,
        resource_group_id: ResourceGroupId::random(),
    };
    rmp_serde::to_vec(&ctx).expect("serialize TaskContext")
}

/// Wraps `value` into a single-payload [`TaskInput`] list — the shape carried in
/// [`spider_core::types::io::ExecutionContext::inputs`] for a single-argument task.
///
/// # Type Parameters
///
/// * `ValueType` - The Serde-serializable value type carried as the task's single input.
///
/// # Returns
///
/// A [`Vec<TaskInput>`] of length 1 holding the msgpack-encoded `value`.
///
/// # Panics
///
/// Panics if msgpack encoding fails.
#[must_use]
pub fn single_input<ValueType: serde::Serialize>(value: &ValueType) -> Vec<TaskInput> {
    vec![TaskInput::ValuePayload(
        rmp_serde::to_vec(value).expect("msgpack encode input"),
    )]
}

/// # Type Parameters
///
/// * `ValueType` - The Serde-serializable value type passed as the task's single input.
///
/// # Returns
///
/// A wire-format buffer carrying one [`TaskInput::ValuePayload`] holding the msgpack-encoded
/// `value` — i.e. the same shape the parent ships for a single-argument task.
///
/// # Panics
///
/// Panics if msgpack encoding or wire-format append fails.
#[must_use]
pub fn encode_single_input<ValueType: serde::Serialize>(value: &ValueType) -> Vec<u8> {
    let mut inputs = TaskInputsSerializer::new();
    inputs
        .append(TaskInput::ValuePayload(
            rmp_serde::to_vec(value).expect("msgpack encode input"),
        ))
        .expect("append wire-format input");
    inputs.release()
}

/// # Returns
///
/// A wire-format buffer carrying zero inputs — for nullary tasks like `always_fail` and
/// `always_panic`.
#[must_use]
pub fn encode_no_inputs() -> Vec<u8> {
    TaskInputsSerializer::new().release()
}

/// # Type Parameters
///
/// * `OutputType` - The Serde-deserializable type the output payload should decode into.
///
/// # Returns
///
/// The single msgpack-encoded value carried in `output_bytes`, deserialized as `OutputType`.
///
/// # Panics
///
/// Panics if:
///
/// * The output buffer doesn't contain exactly one value.
/// * The msgpack decoding fails.
#[must_use]
pub fn decode_single_output<OutputType: serde::de::DeserializeOwned>(
    output_bytes: &[u8],
) -> OutputType {
    let outputs =
        TaskOutputsSerializer::deserialize(output_bytes).expect("decode wire-format outputs");
    assert_eq!(
        outputs.len(),
        1,
        "expected exactly one output payload, got {}",
        outputs.len(),
    );
    rmp_serde::from_slice(&outputs[0]).expect("msgpack decode output")
}

/// # Returns
///
/// A [`Request::Execute`] targeting `task_func` in the integration package.
#[must_use]
pub fn execute_request(task_func: &str, raw_inputs: Vec<u8>) -> Request {
    Request::Execute {
        tdl_context: TdlContext {
            package: PACKAGE_NAME.to_owned(),
            task_func: task_func.to_owned(),
        },
        raw_ctx: build_ctx(),
        raw_inputs,
    }
}
