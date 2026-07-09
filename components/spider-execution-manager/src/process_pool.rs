//! Process supervisor for `spider-task-executor` subprocesses.

use std::fs::File;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use futures_util::SinkExt;
use futures_util::StreamExt;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::io::ExecutionContext;
use spider_task_executor::protocol::ExecutorOutcome;
use spider_task_executor::protocol::Request;
use spider_task_executor::protocol::Response;
use spider_tdl::TaskContext;
use spider_utils::wire::WireError;
use tokio::process::Child;
use tokio::process::ChildStdin;
use tokio::process::ChildStdout;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;
use tokio_util::codec::LengthDelimitedCodec;

/// Pool configuration. Supplied once at construction time and never mutated.
#[derive(Debug, Clone)]
pub struct ProcessPoolConfig {
    /// Identity of the owning execution manager.
    pub em_id: ExecutionManagerId,

    /// Absolute path to the `spider-task-executor` binary the pool will spawn.
    pub executor_binary_path: PathBuf,

    /// Directory exposed to the child via `SPIDER_TDL_PACKAGE_DIR`. The executor resolves
    /// `${dir}/<package>/lib<package>.so` for each package it dispatches.
    pub package_dir: PathBuf,

    /// Directory the pool writes per-executor stderr log files into. Each spawn opens
    /// `<log_dir>/<em_id>-<executor_id>.log` in create-or-append mode and routes the child's
    /// stderr there.
    ///
    /// Per-spawn filenames mean each respawn naturally rotates onto a fresh file; a long-lived
    /// healthy executor accumulates into one file.
    pub log_dir: PathBuf,
}

/// Request to execute a task inside the spawned task executor.
#[derive(Debug)]
pub struct ExecuteRequest {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub resource_group_id: ResourceGroupId,
    pub ctx: ExecutionContext,
}

/// Outcome of a single [`ProcessPool::execute`] call.
#[derive(Debug)]
pub enum Outcome {
    /// Task ran to completion. `outputs` is the wire-format
    /// [`spider_core::types::io::TaskOutputsSerializer`] buffer ready to forward to storage as
    /// `serialized_outputs`. `elapsed_us` is the in-FFI duration measured by the executor.
    Success { outputs: Vec<u8>, elapsed_us: u64 },

    /// Task ran to completion but returned an error. `error` is the msgpack-encoded
    /// [`spider_task_executor::ExecutorError`].
    InTaskFailure { error: Vec<u8>, elapsed_us: u64 },

    /// `hard_timeout` elapsed before the executor replied. The pool has `SIGKILL`-ed the process.
    Timeout { hard_timeout: Duration },

    /// The executor process exited (or closed stdout) before replying.
    ExecutorCrash { exit_status: Option<i32> },
}

/// Internal failure of the pool itself, distinct from a task-execution [`Outcome`]. These indicate
/// the pool can't serve the current request (and possibly any future request).
///
/// This error may indicate a non-recoverable failure. The upper-level caller may need to close the
/// entire process pool and restart the execution manager service from the ground.
#[derive(Debug, thiserror::Error)]
pub enum InternalError {
    /// The pool was entered with no running executor.
    #[error("task executor process is not running")]
    NotRunning,

    /// Failed to spawn the executor (any I/O step during spawn — `create_dir_all`, log-file open,
    /// [`Command::spawn`], or claiming the piped stdio handles).
    #[error("failed to create an executor process: {0}")]
    ExecutorCreationFailure(#[from] std::io::Error),

    /// Failed to msgpack-encode the [`TaskContext`] when building the executor request.
    #[error("failed to encode task context: {0}")]
    EncodeTaskContext(#[from] rmp_serde::encode::Error),

    /// Failed to wire-format-encode the task inputs when building the executor request.
    #[error("failed to encode task inputs: {0}")]
    EncodeTaskInputs(#[from] WireError),
}

/// The process pool of pre-forked task executor subprocesses ready for task execution.
pub struct ProcessPool {
    config: ProcessPoolConfig,
    next_executor_id: AtomicU64,
    /// Lock-serializes concurrent [`Self::execute`] callers. The single executor means each caller
    /// takes the lock for the whole call, so the mutex is the entire concurrency gate.
    handle: Mutex<Option<ExecutorHandle>>,
}

impl ProcessPool {
    /// Factory function.
    ///
    /// Spawns the initial executor process and returns a ready-to-use pool.
    ///
    /// # Returns
    ///
    /// A pool whose handle already holds a freshly spawned executor on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::spawn_executor`]'s return values on failure.
    pub fn new(config: ProcessPoolConfig) -> Result<Self, InternalError> {
        let mut this = Self {
            config,
            handle: Mutex::new(None),
            next_executor_id: AtomicU64::new(0),
        };
        let handle = this.spawn_executor().inspect_err(|err| {
            tracing::error!(err = ? err, "Failed to spawn executor process on construction.");
        })?;
        *this.handle.get_mut() = Some(handle);
        Ok(this)
    }

    /// Runs one task on the pooled executor.
    ///
    /// Locks the handle so concurrent callers queue. Once inside, the request is bincode-framed
    /// onto the child's stdin and the parent races a deadline against the response frame. On
    /// timeout or crash the process is killed and respawned before the call returns; subsequent
    /// calls see a fresh executor.
    ///
    /// # Returns
    ///
    /// Exactly one [`Outcome`] variant describing the dispatch result on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::NotRunning`] if the pool's handle was empty at entry — meaning a prior
    ///   respawn failed and the pool is unrecoverable. The pool should be discarded.
    /// * Forwards [`build_request`]'s return values on failure.
    /// * Forwards [`Self::spawn_executor`]'s return values on failure.
    pub async fn execute(
        &self,
        request: ExecuteRequest,
        hard_timeout: Duration,
    ) -> Result<Outcome, InternalError> {
        let mut handle_guard = self.handle.lock().await;
        let handle = handle_guard.as_mut().ok_or(InternalError::NotRunning)?;
        tracing::info!(
            job_id = ? request.job_id,
            task_id = ? request.task_id,
            task_instance_id = ? request.ctx.task_instance_id,
            executor_id = handle.executor_id,
            "Task executor acquired for execution."
        );
        let frame_request = build_request(request)?;
        let outcome = handle.run(frame_request, hard_timeout).await;

        if matches!(
            outcome,
            Outcome::Timeout { .. } | Outcome::ExecutorCrash { .. }
        ) {
            // Dropping the handle will automatically kill the child process.
            drop(handle_guard.take());
            let new_handle = self.spawn_executor().inspect_err(|err| {
                tracing::error!(
                    err = ? err,
                    "Failed to respawn the executor process after a crash or timeout."
                );
            })?;
            tracing::info!(
                executor_id = new_handle.executor_id,
                "Executor respawned successfully."
            );
            *handle_guard = Some(new_handle);
        }

        drop(handle_guard);
        Ok(outcome)
    }

    /// Spawns the executor binary, allocates the next monotonic executor-id, opens the per-executor
    /// log file, and wraps the child's stdin/stdout in length-delimited codec frames.
    ///
    /// The child's stderr is redirected to `<log_dir>/<em_id>-<executor_id>.log` in
    /// create-or-append mode.
    ///
    /// # Returns
    ///
    /// A fully wired [`ExecutorHandle`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::ExecutorCreationFailure`] if the piped stdin or stdout handles cannot be
    ///   claimed after spawn.
    /// * Forwards [`std::fs::create_dir_all`]'s return values on failure.
    /// * Forwards [`std::fs::OpenOptions::open`]'s return values on failure.
    /// * Forwards [`Command::spawn`]'s return values on failure.
    fn spawn_executor(&self) -> Result<ExecutorHandle, InternalError> {
        let executor_id = self.next_executor_id.fetch_add(1, Ordering::Relaxed);
        std::fs::create_dir_all(&self.config.log_dir)?;
        let log_path = self
            .config
            .log_dir
            .join(format!("{}-{executor_id}.log", self.config.em_id));
        let log_file = File::options().create(true).append(true).open(&log_path)?;

        let mut command = Command::new(&self.config.executor_binary_path);
        command
            .env("SPIDER_TDL_PACKAGE_DIR", &self.config.package_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(log_file))
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| std::io::Error::other("executor stdin not piped"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| std::io::Error::other("executor stdout not piped"))?;
        tracing::info!(executor_id, "Executor spawned.");
        Ok(ExecutorHandle {
            executor_id,
            child,
            requests: FramedWrite::new(stdin, LengthDelimitedCodec::new()),
            responses: FramedRead::new(stdout, LengthDelimitedCodec::new()),
        })
    }
}

/// One running executor subprocess plus framed handles to its stdin / stdout.
struct ExecutorHandle {
    executor_id: u64,
    child: Child,
    requests: FramedWrite<ChildStdin, LengthDelimitedCodec>,
    responses: FramedRead<ChildStdout, LengthDelimitedCodec>,
}

impl ExecutorHandle {
    /// Sends `request` and awaits exactly one reply, racing it against `hard_timeout` and against
    /// stdout EOF (process death).
    ///
    /// # Returns
    ///
    /// Exactly one [`Outcome`] variant:
    ///
    /// * [`Outcome::Success`] or [`Outcome::InTaskFailure`] from a well-formed reply.
    /// * [`Outcome::Timeout`] if `hard_timeout` fires.
    /// * [`Outcome::ExecutorCrash`] on any write/read/decode failure (which all imply the child is
    ///   no longer usable).
    ///
    /// # Panics
    ///
    /// Panics if [`bincode::serialize`] fails to encode `request` — the protocol types are
    /// `derive(Serialize)` and serialize trivially, so an encoding failure indicates programmer
    /// error rather than a runtime condition.
    async fn run(&mut self, request: Request, hard_timeout: Duration) -> Outcome {
        let bytes = bincode::serialize(&request).expect("bincode encode Request");
        if let Err(err) = self.requests.send(Bytes::from(bytes)).await {
            tracing::warn!(
                executor_id = self.executor_id,
                err = ? err,
                "Failed to send request to executor."
            );
            return Outcome::ExecutorCrash {
                exit_status: self.poll_exit_code(),
            };
        }

        tokio::select! {
            biased;
            frame = self.responses.next() => match frame {
                Some(Ok(bytes)) => match bincode::deserialize::<Response>(&bytes) {
                    Ok(Response::Result { outcome, elapsed_us }) => match outcome {
                        ExecutorOutcome::Success { outputs } => {
                            Outcome::Success { outputs, elapsed_us }
                        }
                        ExecutorOutcome::Failure { error } => {
                            Outcome::InTaskFailure { error, elapsed_us }
                        }
                    },
                    Err(err) => {
                        tracing::error!(
                            executor_id = self.executor_id,
                            err = ? err,
                            "Failed to decode executor's response. Considered as crashed."
                        );
                        Outcome::ExecutorCrash { exit_status: self.poll_exit_code() }
                    }
                },
                Some(Err(err)) => {
                    tracing::error!(
                        executor_id = self.executor_id,
                        err = ? err,
                        "Failed to receive executor's response."
                    );
                    Outcome::ExecutorCrash { exit_status: self.poll_exit_code() }
                }
                None => Outcome::ExecutorCrash { exit_status: self.poll_exit_code() },
            },
            () = tokio::time::sleep(hard_timeout) => {
                tracing::warn!(executor_id = self.executor_id, "Executor time out triggered.");
                Outcome::Timeout { hard_timeout }
            }
        }
    }

    /// Non-blocking peek at the child's exit status.
    ///
    /// # Returns
    ///
    /// `Some(code)` if the child has already exited with a code; `None` if it is still running, was
    /// terminated by a signal, or `try_wait` itself errored.
    fn poll_exit_code(&mut self) -> Option<i32> {
        self.child
            .try_wait()
            .ok()
            .flatten()
            .and_then(|status| status.code())
    }
}

/// Builds the wire [`Request::Execute`] from caller inputs.
///
/// # Returns
///
/// A populated [`Request::Execute`] with `raw_ctx` set to the msgpack-encoded [`TaskContext`] and
/// `raw_inputs` set to the serialized execution inputs on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`rmp_serde::to_vec`]'s return values on failure.
fn build_request(request: ExecuteRequest) -> Result<Request, InternalError> {
    let ExecuteRequest {
        job_id,
        task_id,
        resource_group_id,
        ctx,
    } = request;
    let ExecutionContext {
        task_instance_id,
        tdl_context,
        timeout_policy: _,
        serialized_inputs,
    } = ctx;
    let raw_ctx = rmp_serde::to_vec(&TaskContext {
        job_id,
        task_id,
        task_instance_id,
        resource_group_id,
    })?;
    Ok(Request::Execute {
        tdl_context,
        raw_ctx,
        raw_inputs: serialized_inputs,
    })
}
