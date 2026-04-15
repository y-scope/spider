//! Reusable mock scheduling framework for concurrent JCB integration testing.
//!
//! This module provides a complete test harness that drives concurrent job execution through the
//! [`SharedJobControlBlock`] with 64 async worker tasks. It is designed to be reused across
//! different test files and DB connector implementations.
//!
//! # Mock system architecture
//!
//! ```text
//!     Test Harness --> JCB -- "ready tasks" --> MPMC Queue --> 64 Workers
//!                      ^                                         |
//!                      +--- "register / succeed / fail" ---------+
//!                      |                                         |
//!                      +--- "new ready tasks" --> Queue          |
//!                                                                |
//!     Workers -- "terminal state" --> Harness                    |
//!     Workers -- "latency samples" --> Instrumentation           |
//! ```
//!
//! **Data flow**: The harness creates a JCB and calls `start()`, which enqueues initially ready
//! tasks into the MPMC channel. 64 workers concurrently dequeue one task at a time, call JCB
//! methods (register -> succeed/fail), which may enqueue newly ready child tasks back into the
//! channel. When a worker observes a terminal [`JobState`], it broadcasts via a `watch` channel.
//! The harness receives the signal and sends a done notification, causing all workers to exit their
//! `select!` loop.
//!
//! **Cancellation**: For immediate cancel, the harness calls `jcb.cancel()` synchronously after
//! `start()` but before workers process any messages. For concurrent cancel, a separate spawned
//! task cancels after a 1ms delay, racing against worker execution.
//!
//! # Mock components
//!
//! * [`MockReadyQueueSender`] -- backed by `async_channel` (true MPMC). Task-ready batches from the
//!   JCB are flattened to one message per task index for maximum worker concurrency.
//! * [`NoopDbConnector`] -- stateless stub returning appropriate state transitions based on
//!   commit/cleanup task presence. Generic over `DbConnectorType` so a real connector can be
//!   swapped in.
//! * [`MockTaskInstancePool`] -- atomic counter for ID allocation, no-op registration.
//!
//! # Worker architecture
//!
//! Each worker owns a cloned [`WorkerContext`] (no shared `Arc` indirection -- all fields are
//! cheaply cloneable via `Arc` or built-in `Clone`). Workers `tokio::select!` between the
//! ready-queue receiver and a done signal (`watch<bool>`).
//!
//! **Failure injection**: 50% of first-seen tasks spawn 2 concurrent `tokio::spawn` coroutines
//! that each independently register and fail a task instance. This exercises both the retry
//! counter (`max_num_retry >= 3`) and the concurrent instance limit (`max_num_instances >= 2`).
//!
//! # Instrumentation
//!
//! The [`InstrumentedJcb`] wrapper optionally records per-operation latency for three JCB methods:
//!
//! * `create_task_instance` (Registration)
//! * `succeed_task_instance` / `succeed_commit_task_instance` / `succeed_cleanup_task_instance`
//!   (Success)
//! * `fail_task_instance` (Failure)
//!
//! Each sample records the elapsed [`Duration`] and whether the call returned `Ok` or `Err`,
//! producing separate `(ok)` and `(err)` rows in the output. When `instrument_sender` is `None`,
//! calls are forwarded directly with zero overhead -- no [`Instant::now()`] is called.
//!
//! Instrumentation is enabled by setting the `SPIDER_TEST_INSTRUMENT_OUTPUT_DIR` environment
//! variable. When set, tests create an `mpsc::unbounded_channel`, pass the sender into
//! [`run_workload`], and after completion collect all samples into a table formatted by the
//! [`tabled`] crate.
//!
//! # Generic test design
//!
//! All test logic is generic over `DbConnectorType` via an async factory callback
//! `AsyncFnOnce() -> DbConnectorType`, so the same tests can be rerun with a real
//! DB connector later.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::bail;
use async_trait::async_trait;
use dashmap::DashMap;
use rand::{Rng, SeedableRng};
use spider_core::{
    job::JobState,
    task::{TaskGraph as SubmittedTaskGraph, TaskIndex},
    types::{
        id::{JobId, ResourceGroupId, TaskInstanceId},
        io::{ExecutionContext, TaskInput, TaskOutput},
    },
};
use spider_storage::{
    cache::{
        TaskId,
        error::{CacheError, InternalError},
        job::SharedJobControlBlock,
        task::{SharedTaskControlBlock, SharedTerminationTaskControlBlock},
    },
    db::{DbError, ExternalJobOrchestration, InternalJobOrchestration, MariaDbStorageConnector},
    ready_queue::ReadyQueueSender,
    task_instance_pool::TaskInstancePoolConnector,
};
use tabled::{Table, Tabled};
use tokio::sync::{mpsc, watch};

/// A handler that generates mock task outputs from an [`ExecutionContext`], which simulates the
/// execution of a TDL task.
pub type TaskOutputHandler = Arc<dyn Fn(&ExecutionContext) -> Vec<TaskOutput> + Send + Sync>;

/// Sender half for instrument samples. Workers send timing data through this channel.
pub type InstrumentSender = mpsc::UnboundedSender<InstrumentSample>;

/// A single latency sample collected from a JCB operation.
///
/// Each variant carries the elapsed [`Duration`] and a `bool` indicating whether the JCB call
/// returned `Ok` (`true`) or `Err` (`false`).
pub enum InstrumentSample {
    /// Latency of a [`SharedJobControlBlock::create_task_instance`] call.
    Registration { elapsed: Duration, ok: bool },

    /// Latency of a [`SharedJobControlBlock::succeed_task_instance`] (or commit/cleanup) call.
    Success { elapsed: Duration, ok: bool },

    /// Latency of a [`SharedJobControlBlock::fail_task_instance`] call.
    Failure { elapsed: Duration, ok: bool },
}

/// Describes when (if ever) to cancel the job.
pub enum CancelPolicy {
    /// Do not cancel.
    Never,

    /// Cancel immediately after `start()`, before any worker processes a message.
    Immediate,

    /// Cancel concurrently — spawn a separate task that cancels after a short delay.
    Concurrent,
}

/// A stateless DB connector stub.
#[derive(Clone)]
pub struct NoopDbConnector {}

#[async_trait]
impl InternalJobOrchestration for NoopDbConnector {
    async fn start(&self, _job_id: JobId) -> Result<(), DbError> {
        Ok(())
    }

    async fn set_state(&self, _job_id: JobId, _state: JobState) -> Result<(), DbError> {
        Ok(())
    }

    async fn commit_outputs(
        &self,
        _job_id: JobId,
        _job_outputs: Vec<TaskOutput>,
        _has_commit_task: bool,
    ) -> Result<(), DbError> {
        Ok(())
    }

    async fn cancel(&self, _job_id: JobId, _has_cleanup_task: bool) -> Result<(), DbError> {
        Ok(())
    }

    async fn fail(&self, _job_id: JobId, _error_message: String) -> Result<(), DbError> {
        Ok(())
    }

    async fn delete_expired_terminated_jobs(
        &self,
        _expire_after_sec: u64,
    ) -> Result<Vec<JobId>, DbError> {
        Ok(Vec::new())
    }
}

/// The result of running a workload to completion.
pub struct WorkloadResult {
    /// The [`JobId`] of the job that was run.
    pub job_id: JobId,

    /// The terminal [`JobState`] observed by a worker.
    pub terminal_state: JobState,

    /// The number of regular tasks that were successfully completed by workers.
    pub task_success_count: usize,

    /// The number of times a commit task was successfully processed.
    pub commit_count: usize,

    /// The number of times a cleanup task was successfully processed.
    pub cleanup_count: usize,
}

/// Type alias for the DB connector factory return type.
pub type FactoryReturn<DbConnectorType> = (DbConnectorType, JobId, ResourceGroupId);

/// An async DB connector factory for [`run_workload`].
///
/// # Type Parameters
///
/// * `DbConnectorType` - The DB-layer connector implementation.
///
/// Receives the submitted task graph and job inputs, performs any required DB setup (e.g. job
/// registration), and returns the connector along with the [`JobId`] and [`ResourceGroupId`] to
/// use for the JCB.
pub trait DbConnectorFactory<DbConnectorType: InternalJobOrchestration>:
    AsyncFnOnce(&SubmittedTaskGraph, &[TaskInput]) -> FactoryReturn<DbConnectorType> + Send {
}

impl<DbConnectorType: InternalJobOrchestration, AsyncFunc> DbConnectorFactory<DbConnectorType>
    for AsyncFunc
where
    AsyncFunc:
        AsyncFnOnce(&SubmittedTaskGraph, &[TaskInput]) -> FactoryReturn<DbConnectorType> + Send,
{
}

/// Creates a [`NoopDbConnector`] with default [`JobId`] and [`ResourceGroupId`].
#[must_use]
pub fn noop_db_connector_factory() -> impl DbConnectorFactory<NoopDbConnector> {
    async |_, _| {
        (
            NoopDbConnector {},
            JobId::default(),
            ResourceGroupId::default(),
        )
    }
}

/// # Returns
///
/// A default (noop) output handler that produces one output of `output_size` bytes per task. This
/// output is currently independent of the execution context.
#[must_use]
pub fn default_output_handler(output_size: usize) -> TaskOutputHandler {
    Arc::new(move |_: &ExecutionContext| -> Vec<TaskOutput> { vec![vec![0u8; output_size]] })
}

/// Creates an instrument sender/receiver pair if [`INSTRUMENT_OUTPUT_DIR_ENV`] is set.
///
/// # Returns
///
/// * `Some((sender, receiver))` if the env var is set.
/// * `None` otherwise.
#[must_use]
pub fn try_create_instrument_channel()
-> Option<(InstrumentSender, mpsc::UnboundedReceiver<InstrumentSample>)> {
    std::env::var(INSTRUMENT_OUTPUT_DIR_ENV)
        .ok()
        .map(|_| mpsc::unbounded_channel())
}

/// Writes the collected instrument results for a test to the output file.
///
/// The results are appended to a file named `test_jcb` (derived from this test file) under the
/// directory specified by [`INSTRUMENT_OUTPUT_DIR_ENV`].
///
/// # Panics
///
/// Panics if:
///
/// * [`INSTRUMENT_OUTPUT_DIR_ENV`] is not set.
/// * The output file cannot be opened or written to.
pub fn write_instrument_results(
    test_name: &str,
    receiver: mpsc::UnboundedReceiver<InstrumentSample>,
) {
    let output_dir = std::env::var(INSTRUMENT_OUTPUT_DIR_ENV)
        .expect("SPIDER_TEST_INSTRUMENT_OUTPUT_DIR should be set when writing results");

    let table = collect_instrument_table(receiver);
    let output = format!("\n{test_name}\n{table}\n");

    let path = std::path::Path::new(&output_dir).join("test_jcb");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap_or_else(|e| {
            panic!(
                "failed to open instrument output file {}: {e}",
                path.display()
            )
        });
    std::io::Write::write_all(&mut file, output.as_bytes()).unwrap_or_else(|e| {
        panic!(
            "failed to write instrument output to {}: {e}",
            path.display()
        )
    });
}

/// Creates a JCB from the given task graph, starts it, spawns workers, and runs to completion.
///
/// # Type Parameters
///
/// * `DbConnectorType` - The DB-layer connector implementation. Must be `'static` so that worker
///   tasks can be spawned onto the tokio runtime.
///
/// # Panics
///
/// Panics if:
///
/// * JCB creation or startup fails.
/// * A worker task panics.
/// * A worker returns an unexpected (non-stale-state) error.
/// * A cancel operation returns an unexpected error.
///
/// # Returns
///
/// A [`WorkloadResult`] containing the terminal state and commit/cleanup execution counts.
pub async fn run_workload<DbConnectorType: InternalJobOrchestration + 'static>(
    submitted_task_graph: &SubmittedTaskGraph,
    inputs: Vec<TaskInput>,
    db_connector_factory: impl DbConnectorFactory<DbConnectorType>,
    cancel_policy: CancelPolicy,
    output_handler: TaskOutputHandler,
    always_fail: bool,
    instrument_sender: Option<InstrumentSender>,
) -> WorkloadResult {
    // Create mock components.
    let (ready_sender, ready_receiver) = async_channel::unbounded::<ReadyMessage>();
    let ready_queue_sender = MockReadyQueueSender {
        sender: ready_sender,
    };
    let (db_connector, job_id, resource_group_id) =
        db_connector_factory(submitted_task_graph, &inputs).await;
    let task_instance_pool = MockTaskInstancePool::new();

    // Create and start the JCB.
    let inner_jcb = SharedJobControlBlock::create(
        job_id,
        resource_group_id,
        submitted_task_graph,
        inputs,
        ready_queue_sender,
        db_connector,
        task_instance_pool,
    )
    .await
    .expect("failed to create JCB");

    inner_jcb.start().await.expect("failed to start JCB");

    let jcb = InstrumentedJcb {
        jcb: inner_jcb,
        instrument_sender,
    };

    let (terminal_state_sender, mut terminal_state_receiver) =
        watch::channel::<Option<JobState>>(None);
    let (done_sender, done_receiver) = watch::channel::<bool>(false);
    let task_success_count = Arc::new(AtomicUsize::new(0));
    let commit_count = Arc::new(AtomicUsize::new(0));
    let cleanup_count = Arc::new(AtomicUsize::new(0));

    let ctx = WorkerContext {
        receiver: ready_receiver,
        jcb: jcb.clone(),
        terminal_state_sender: terminal_state_sender.clone(),
        done_receiver: done_receiver.clone(),
        seen_tasks: Arc::new(DashMap::new()),
        output_handler: output_handler.clone(),
        task_success_count: task_success_count.clone(),
        commit_count: commit_count.clone(),
        cleanup_count: cleanup_count.clone(),
        always_fail,
    };

    // Spawn workers.
    let mut join_set = tokio::task::JoinSet::new();
    for _ in 0..NUM_WORKERS {
        let worker_ctx = ctx.clone();
        join_set.spawn(async move { run_worker(worker_ctx).await });
    }

    // Apply cancellation policy.
    let cancel_handle = match cancel_policy {
        CancelPolicy::Never => None,
        CancelPolicy::Immediate => {
            match jcb.cancel().await {
                Ok(state) if state.is_terminal() => {
                    let _ = terminal_state_sender.send(Some(state));
                }
                Ok(_) => {}
                Err(e) if is_stale_state(&e) => {}
                Err(e) => panic!("unexpected cancel error: {e:?}"),
            }
            None
        }
        CancelPolicy::Concurrent => {
            let jcb_clone = jcb.clone();
            let terminal_sender_clone = terminal_state_sender.clone();
            Some(tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                match jcb_clone.cancel().await {
                    Ok(state) if state.is_terminal() => {
                        let _ = terminal_sender_clone.send(Some(state));
                    }
                    Ok(_) | Err(CacheError::StaleState(_)) => {}
                    Err(e) => panic!("unexpected cancel error: {e:?}"),
                }
            }))
        }
    };

    // Wait for a terminal state, then signal workers to exit.
    terminal_state_receiver
        .wait_for(Option::is_some)
        .await
        .expect("terminal state watch channel should not be dropped");
    let terminal_state = terminal_state_receiver
        .borrow()
        .expect("terminal state should be set after wait_for");

    let _ = done_sender.send(true);

    while let Some(result) = join_set.join_next().await {
        result
            .expect("worker task should not panic")
            .expect("worker early returns on error");
    }

    if let Some(handle) = cancel_handle {
        handle.await.expect("cancel task should not panic");
    }

    WorkloadResult {
        job_id,
        terminal_state,
        task_success_count: task_success_count.load(Ordering::Relaxed),
        commit_count: commit_count.load(Ordering::Relaxed),
        cleanup_count: cleanup_count.load(Ordering::Relaxed),
    }
}

/// Creates a DB connector factory that registers a job via [`MariaDbStorageConnector`].
///
/// The returned closure receives the submitted task graph and job inputs from [`run_workload`],
/// registers the job via [`ExternalJobOrchestration::register`], and returns the connector along
/// with the resulting [`JobId`] and [`ResourceGroupId`].
///
/// # Panics
///
/// Panics if job registration fails.
#[must_use]
pub fn mariadb_db_connector_factory(
    storage: MariaDbStorageConnector,
    rg_id: ResourceGroupId,
) -> impl DbConnectorFactory<MariaDbStorageConnector> {
    async move |graph, inputs| {
        let job_id = ExternalJobOrchestration::register(&storage, rg_id, graph, inputs)
            .await
            .expect("register should succeed");
        (storage, job_id, rg_id)
    }
}

/// The number of concurrent worker tasks to spawn.
const NUM_WORKERS: usize = 64;

/// The environment variable that, when set, enables instrumentation output. Its value is the
/// directory path where the instrumentation file will be written.
const INSTRUMENT_OUTPUT_DIR_ENV: &str = "SPIDER_TEST_INSTRUMENT_OUTPUT_DIR";

/// The concrete JCB type parameterized by the DB connector.
///
/// # Type Parameters
///
/// * `DbConnectorType` - The DB-layer connector implementation.
type TestJcb<DbConnectorType> =
    SharedJobControlBlock<MockReadyQueueSender, DbConnectorType, MockTaskInstancePool>;

/// A message sent through the mock ready queue.
///
/// Each message represents a single schedulable unit of work. Task-ready batches from the JCB are
/// flattened into one message per task index so that workers receive tasks individually, enabling
/// better concurrency across the worker pool.
///
/// Since these tests run a single job at a time, messages do not carry a job ID.
enum ReadyMessage {
    /// A single task is ready to be scheduled.
    Task { task_index: TaskIndex },

    /// The commit task is ready to be scheduled.
    Commit,

    /// The cleanup task is ready to be scheduled.
    Cleanup,
}

/// A mock [`ReadyQueueSender`] backed by an [`async_channel::Sender`].
///
/// Workers each hold a cloned [`async_channel::Receiver`] and can concurrently await messages
/// without any mutex serialization. The `job_id` parameter in each trait method is discarded since
/// only one job runs at a time.
#[derive(Clone)]
struct MockReadyQueueSender {
    sender: async_channel::Sender<ReadyMessage>,
}

#[async_trait]
impl ReadyQueueSender for MockReadyQueueSender {
    async fn send_task_ready(
        &self,
        _job_id: JobId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError> {
        for task_index in task_indices {
            self.sender
                .send(ReadyMessage::Task { task_index })
                .await
                .map_err(|_| InternalError::ReadyQueueSendFailure("channel closed".to_owned()))?;
        }
        Ok(())
    }

    async fn send_commit_ready(&self, _job_id: JobId) -> Result<(), InternalError> {
        self.sender
            .send(ReadyMessage::Commit)
            .await
            .map_err(|_| InternalError::ReadyQueueSendFailure("channel closed".to_owned()))
    }

    async fn send_cleanup_ready(&self, _job_id: JobId) -> Result<(), InternalError> {
        self.sender
            .send(ReadyMessage::Cleanup)
            .await
            .map_err(|_| InternalError::ReadyQueueSendFailure("channel closed".to_owned()))
    }
}

/// A mock task instance pool that hands out monotonically increasing [`TaskInstanceId`]s.
///
/// Registration calls are no-ops — the pool does not track which instances are registered.
#[derive(Clone)]
struct MockTaskInstancePool {
    next_id: Arc<std::sync::atomic::AtomicU64>,
}

impl MockTaskInstancePool {
    /// Creates a new pool with IDs starting at 1.
    fn new() -> Self {
        Self {
            next_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }
}

#[async_trait]
impl TaskInstancePoolConnector for MockTaskInstancePool {
    fn get_next_available_task_instance_id(&self) -> TaskInstanceId {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn register_task_instance(
        &self,
        _task_instance_id: TaskInstanceId,
        _tcb: SharedTaskControlBlock,
    ) -> Result<(), InternalError> {
        Ok(())
    }

    async fn register_termination_task_instance(
        &self,
        _task_instance_id: TaskInstanceId,
        _termination_tcb: SharedTerminationTaskControlBlock,
    ) -> Result<(), InternalError> {
        Ok(())
    }
}

/// Thin wrapper around [`TestJcb`] that optionally records latency samples for each JCB
/// operation. When `instrument_sender` is `None`, calls are forwarded without any timing
/// overhead.
///
/// # Type Parameters
///
/// * `DbConnectorType` - The DB-layer connector implementation used by the JCB.
#[derive(Clone)]
struct InstrumentedJcb<DbConnectorType: InternalJobOrchestration> {
    jcb: TestJcb<DbConnectorType>,
    instrument_sender: Option<InstrumentSender>,
}

impl<DbConnectorType: InternalJobOrchestration> InstrumentedJcb<DbConnectorType> {
    /// Wraps [`SharedJobControlBlock::create_task_instance`] with optional latency recording.
    async fn create_task_instance(&self, task_id: TaskId) -> Result<ExecutionContext, CacheError> {
        if let Some(sender) = &self.instrument_sender {
            let start = Instant::now();
            let result = self.jcb.create_task_instance(task_id).await;
            let _ = sender.send(InstrumentSample::Registration {
                elapsed: start.elapsed(),
                ok: result.is_ok(),
            });
            result
        } else {
            self.jcb.create_task_instance(task_id).await
        }
    }

    /// Wraps [`SharedJobControlBlock::succeed_task_instance`] with optional latency recording.
    async fn succeed_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_index: TaskIndex,
        task_outputs: Vec<TaskOutput>,
    ) -> Result<JobState, CacheError> {
        if let Some(sender) = &self.instrument_sender {
            let start = Instant::now();
            let result = self
                .jcb
                .succeed_task_instance(task_instance_id, task_index, task_outputs)
                .await;
            let _ = sender.send(InstrumentSample::Success {
                elapsed: start.elapsed(),
                ok: result.is_ok(),
            });
            result
        } else {
            self.jcb
                .succeed_task_instance(task_instance_id, task_index, task_outputs)
                .await
        }
    }

    /// Wraps [`SharedJobControlBlock::fail_task_instance`] with optional latency recording.
    async fn fail_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_id: TaskId,
        error_message: String,
    ) -> Result<JobState, CacheError> {
        if let Some(sender) = &self.instrument_sender {
            let start = Instant::now();
            let result = self
                .jcb
                .fail_task_instance(task_instance_id, task_id, error_message)
                .await;
            let _ = sender.send(InstrumentSample::Failure {
                elapsed: start.elapsed(),
                ok: result.is_ok(),
            });
            result
        } else {
            self.jcb
                .fail_task_instance(task_instance_id, task_id, error_message)
                .await
        }
    }

    /// Wraps [`SharedJobControlBlock::succeed_commit_task_instance`].
    async fn succeed_commit_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, CacheError> {
        if let Some(sender) = &self.instrument_sender {
            let start = Instant::now();
            let result = self
                .jcb
                .succeed_commit_task_instance(task_instance_id)
                .await;
            let _ = sender.send(InstrumentSample::Success {
                elapsed: start.elapsed(),
                ok: result.is_ok(),
            });
            result
        } else {
            self.jcb
                .succeed_commit_task_instance(task_instance_id)
                .await
        }
    }

    /// Wraps [`SharedJobControlBlock::succeed_cleanup_task_instance`].
    async fn succeed_cleanup_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, CacheError> {
        if let Some(sender) = &self.instrument_sender {
            let start = Instant::now();
            let result = self
                .jcb
                .succeed_cleanup_task_instance(task_instance_id)
                .await;
            let _ = sender.send(InstrumentSample::Success {
                elapsed: start.elapsed(),
                ok: result.is_ok(),
            });
            result
        } else {
            self.jcb
                .succeed_cleanup_task_instance(task_instance_id)
                .await
        }
    }

    /// Wraps [`SharedJobControlBlock::cancel`].
    async fn cancel(&self) -> Result<JobState, CacheError> {
        self.jcb.cancel().await
    }
}

/// Per-worker context. All fields are cheaply cloneable (via `Arc` or built-in `Clone` impls),
/// so each worker owns its own copy without shared `Arc<WorkerContext>` indirection.
///
/// # Type Parameters
///
/// * `DbConnectorType` - The DB-layer connector implementation used by the JCB.
#[derive(Clone)]
struct WorkerContext<DbConnectorType: InternalJobOrchestration> {
    /// The MPMC ready-queue receiver. Each clone can concurrently await messages without
    /// serialization.
    receiver: async_channel::Receiver<ReadyMessage>,

    /// The instrumented JCB under test.
    jcb: InstrumentedJcb<DbConnectorType>,

    /// Watch channel sender — workers send the terminal [`JobState`] when observed.
    terminal_state_sender: watch::Sender<Option<JobState>>,

    /// Watch channel receiver — workers use this to detect shutdown.
    done_receiver: watch::Receiver<bool>,

    /// Tracks which [`TaskIndex`](es) have been seen for failure injection.
    seen_tasks: Arc<DashMap<TaskIndex, ()>>,

    /// Generates mock task outputs from an [`ExecutionContext`].
    output_handler: TaskOutputHandler,

    /// Counter incremented when a regular task is successfully completed.
    task_success_count: Arc<AtomicUsize>,

    /// Counter incremented when a commit task is successfully processed.
    commit_count: Arc<AtomicUsize>,

    /// Counter incremented when a cleanup task is successfully processed.
    cleanup_count: Arc<AtomicUsize>,

    /// If `true`, workers always fail task instances instead of applying the random injection
    /// policy. Used by the always-fail test case.
    always_fail: bool,
}

/// A single row in the instrumentation output table.
#[derive(Tabled)]
struct LatencyRow {
    #[tabled(rename = "Operation")]
    operation: &'static str,
    #[tabled(rename = "Count")]
    count: usize,
    #[tabled(rename = "Avg (ms)")]
    avg_ms: String,
    #[tabled(rename = "P50 (ms)")]
    p50_ms: String,
    #[tabled(rename = "P95 (ms)")]
    p95_ms: String,
    #[tabled(rename = "P99 (ms)")]
    p99_ms: String,
}

impl LatencyRow {
    /// Computes a latency row from a slice of duration samples. Returns a row with zeroes if
    /// `samples` is empty.
    ///
    /// # Returns
    ///
    /// The computed latency row.
    fn from_samples(operation: &'static str, samples: &mut [Duration]) -> Self {
        if samples.is_empty() {
            return Self {
                operation,
                count: 0,
                avg_ms: "N/A".to_owned(),
                p50_ms: "N/A".to_owned(),
                p95_ms: "N/A".to_owned(),
                p99_ms: "N/A".to_owned(),
            };
        }
        samples.sort();
        let count = samples.len();
        let sum: Duration = samples.iter().sum();
        #[allow(clippy::cast_precision_loss)]
        let avg = sum.as_secs_f64() * 1000.0 / count as f64;
        let p50 = samples[count / 2].as_secs_f64() * 1000.0;
        let p95 = samples[count * 95 / 100].as_secs_f64() * 1000.0;
        let p99 = samples[count * 99 / 100].as_secs_f64() * 1000.0;
        Self {
            operation,
            count,
            avg_ms: format!("{avg:.3}"),
            p50_ms: format!("{p50:.3}"),
            p95_ms: format!("{p95:.3}"),
            p99_ms: format!("{p99:.3}"),
        }
    }
}

/// # Returns
///
/// Whether the error is a [`CacheError::StaleState`] variant.
///
/// Stale-state errors are expected during concurrent execution (e.g., a task was already succeeded
/// by another worker) and should be silently ignored.
const fn is_stale_state(err: &CacheError) -> bool {
    matches!(err, CacheError::StaleState(_))
}

/// If `state` is terminal, broadcasts it on the watch channel.
///
/// # Type Parameters
///
/// * `DbConnectorType` - The DB-layer connector implementation used by the JCB.
fn broadcast_if_terminated<DbConnectorType: InternalJobOrchestration>(
    ctx: &WorkerContext<DbConnectorType>,
    state: JobState,
) {
    if state.is_terminal() {
        let _ = ctx.terminal_state_sender.send(Some(state));
    }
}

/// Collects instrument samples from the receiver and formats them into a table string using
/// [`tabled`].
fn collect_instrument_table(receiver: mpsc::UnboundedReceiver<InstrumentSample>) -> String {
    let mut registration_ok = Vec::new();
    let mut registration_err = Vec::new();
    let mut success_ok = Vec::new();
    let mut success_err = Vec::new();
    let mut failure_ok = Vec::new();
    let mut failure_err = Vec::new();

    let mut receiver = receiver;
    while let Ok(sample) = receiver.try_recv() {
        match sample {
            InstrumentSample::Registration { elapsed, ok: true } => {
                registration_ok.push(elapsed);
            }
            InstrumentSample::Registration { elapsed, ok: false } => registration_err.push(elapsed),
            InstrumentSample::Success { elapsed, ok: true } => success_ok.push(elapsed),
            InstrumentSample::Success { elapsed, ok: false } => success_err.push(elapsed),
            InstrumentSample::Failure { elapsed, ok: true } => failure_ok.push(elapsed),
            InstrumentSample::Failure { elapsed, ok: false } => failure_err.push(elapsed),
        }
    }

    let candidates: [(&'static str, &mut [Duration]); 6] = [
        ("Registration (ok)", &mut registration_ok),
        ("Registration (err)", &mut registration_err),
        ("Success (ok)", &mut success_ok),
        ("Success (err)", &mut success_err),
        ("Failure (ok)", &mut failure_ok),
        ("Failure (err)", &mut failure_err),
    ];

    let rows: Vec<LatencyRow> = candidates
        .into_iter()
        .map(|(name, samples)| LatencyRow::from_samples(name, samples))
        .collect();

    Table::new(rows).to_string()
}

/// Runs a single worker that consumes [`ReadyMessage`]s from the shared queue and drives task
/// execution through the JCB.
///
/// The worker loops until either the done signal fires or the receiver is closed (returns `None`).
///
/// # Type Parameters
///
/// * `DbConnectorType` - The DB-layer connector implementation used by the JCB.
///
/// # Failure injection
///
/// For each task index received, the worker flips a coin:
///
/// * **Heads (50%)**: succeed the task directly.
/// * **Tails (50%)**: check if the task has been seen before (via `seen_tasks`). If first-seen,
///   spawn two concurrent coroutines that each register and fail a task instance (exercising retry
///   logic). If already seen, succeed.
///
/// All [`CacheError::StaleState`] errors are silently ignored — they are expected when multiple
/// workers race on the same task or when the job has already terminated.
///
/// # Errors
///
/// Forwards all errors that are not [`CacheError::StaleState`].
async fn run_worker<DbConnectorType: InternalJobOrchestration + 'static>(
    mut ctx: WorkerContext<DbConnectorType>,
) -> anyhow::Result<()> {
    let mut rng = rand::rngs::StdRng::from_os_rng();
    loop {
        let msg = tokio::select! {
            msg = ctx.receiver.recv() => msg,
            _ = ctx.done_receiver.wait_for(|&done| done) => break,
        };
        let Ok(msg) = msg else {
            break;
        };

        match msg {
            ReadyMessage::Task { task_index } => {
                if let Err(e) = process_task(&ctx, &mut rng, task_index).await
                    && !is_stale_state(&e)
                {
                    bail!(e);
                }
            }
            ReadyMessage::Commit => {
                if let Err(e) = process_commit(&ctx).await
                    && !is_stale_state(&e)
                {
                    bail!(e);
                }
            }
            ReadyMessage::Cleanup => {
                if let Err(e) = process_cleanup(&ctx).await
                    && !is_stale_state(&e)
                {
                    bail!(e);
                }
            }
        }
    }
    Ok(())
}

/// Processes a single task index to simulate task execution according to the given policy.
///
/// # Type Parameters
///
/// * `DbConnectorType` - The DB-layer connector implementation used by the JCB.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`SharedJobControlBlock::create_task_instance`]'s return values on failure.
/// * Forwards [`SharedJobControlBlock::succeed_task_instance`]'s return values on failure.
/// * Forwards [`SharedJobControlBlock::fail_task_instance`]'s return values on failure.
async fn process_task<DbConnectorType: InternalJobOrchestration + 'static>(
    ctx: &WorkerContext<DbConnectorType>,
    rng: &mut impl Rng,
    task_index: TaskIndex,
) -> Result<(), CacheError> {
    if ctx.always_fail {
        let exec_ctx = ctx
            .jcb
            .create_task_instance(TaskId::Index(task_index))
            .await?;
        let state = ctx
            .jcb
            .fail_task_instance(
                exec_ctx.task_instance_id,
                TaskId::Index(task_index),
                "always fail".to_owned(),
            )
            .await?;
        broadcast_if_terminated(ctx, state);
        return Ok(());
    }

    if ctx.seen_tasks.insert(task_index, ()).is_none() && rng.random_bool(0.5) {
        // Failure injection: spawn two concurrent coroutines that each register and fail a task
        // instance, exercising both the retry and concurrent-instance logic.
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let jcb = ctx.jcb.clone();
                let terminal_sender = ctx.terminal_state_sender.clone();
                tokio::spawn(async move {
                    let exec_ctx = jcb.create_task_instance(TaskId::Index(task_index)).await?;
                    let state = jcb
                        .fail_task_instance(
                            exec_ctx.task_instance_id,
                            TaskId::Index(task_index),
                            "injected failure".to_owned(),
                        )
                        .await?;
                    if state.is_terminal() {
                        let _ = terminal_sender.send(Some(state));
                    }
                    Ok::<(), CacheError>(())
                })
            })
            .collect();
        for handle in handles {
            match handle
                .await
                .expect("failure injection task should not panic")
            {
                Ok(()) => {}
                Err(e) if is_stale_state(&e) => {}
                Err(e) => return Err(e),
            }
        }
        return Ok(());
    }

    let exec_ctx = ctx
        .jcb
        .create_task_instance(TaskId::Index(task_index))
        .await?;
    let outputs = (ctx.output_handler)(&exec_ctx);
    let state = ctx
        .jcb
        .succeed_task_instance(exec_ctx.task_instance_id, task_index, outputs)
        .await?;
    ctx.task_success_count.fetch_add(1, Ordering::Relaxed);
    broadcast_if_terminated(ctx, state);
    Ok(())
}

/// Processes a commit-ready message: creates a commit task instance and succeeds it.
///
/// # Type Parameters
///
/// * `DbConnectorType` - The DB-layer connector implementation used by the JCB.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`SharedJobControlBlock::create_task_instance`]'s return values on failure.
/// * Forwards [`SharedJobControlBlock::succeed_commit_task_instance`]'s return values of failure.
async fn process_commit<DbConnectorType: InternalJobOrchestration>(
    ctx: &WorkerContext<DbConnectorType>,
) -> Result<(), CacheError> {
    let exec_ctx = ctx.jcb.create_task_instance(TaskId::Commit).await?;
    let state = ctx
        .jcb
        .succeed_commit_task_instance(exec_ctx.task_instance_id)
        .await?;
    ctx.commit_count.fetch_add(1, Ordering::Relaxed);
    broadcast_if_terminated(ctx, state);
    Ok(())
}

/// Processes a cleanup-ready message: creates a cleanup task instance and succeeds it.
///
/// # Type Parameters
///
/// * `DbConnectorType` - The DB-layer connector implementation used by the JCB.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`SharedJobControlBlock::create_task_instance`]'s return values on failure.
/// * Forwards [`SharedJobControlBlock::succeed_cleanup_task_instance`]'s return values of failure.
async fn process_cleanup<DbConnectorType: InternalJobOrchestration>(
    ctx: &WorkerContext<DbConnectorType>,
) -> Result<(), CacheError> {
    let exec_ctx = ctx.jcb.create_task_instance(TaskId::Cleanup).await?;
    let state = ctx
        .jcb
        .succeed_cleanup_task_instance(exec_ctx.task_instance_id)
        .await?;
    ctx.cleanup_count.fetch_add(1, Ordering::Relaxed);
    broadcast_if_terminated(ctx, state);
    Ok(())
}
