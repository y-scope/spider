//! Integration tests for [`SharedJobControlBlock`].
//!
//! These tests exercise the full job lifecycle (create → start → execute → terminate) by wiring
//! the JCB to mock implementations of the three external components it depends on:
//!
//! * **[`MockReadyQueueSender`]** — an unbounded MPSC channel that carries [`ReadyMessage`]s.
//!   Workers share the receiver via `Arc<Mutex<...>>` for MPMC semantics.
//! * **[`NoopDbConnector`]** — a stateless stub that returns the appropriate [`JobState`]
//!   transitions based on whether the job has commit/cleanup tasks.
//! * **[`MockTaskInstancePool`]** — an atomic counter that hands out unique [`TaskInstanceId`]s
//!   without tracking registrations.
//!
//! The test harness ([`run_workload`]) spawns 64 concurrent worker tasks that consume messages
//! from the ready queue and drive task execution through the JCB. A configurable output handler
//! ([`TaskOutputHandler`]) generates mock outputs from each task's [`ExecutionContext`], and a
//! failure injection policy randomly fails first-seen tasks to exercise retry logic.
//!
//! # Test cases
//!
//! | Test | Workload | Cancellation | Expected terminal state |
//! |---|---|---|---|
//! | [`test_flat_success`] | 10k independent tasks | none | `Succeeded` |
//! | [`test_flat_cancel`] | 10k independent tasks | immediate | `Cancelled` |
//! | [`test_neural_net_success`] | 10×1000 layered DAG | none | `Succeeded` |
//! | [`test_neural_net_cancel`] | 10×1000 layered DAG | immediate | `Cancelled` |
//! | [`test_always_fail_terminates_job`] | 1 task, always fail | none | `Failed` |
//! | [`test_concurrent_success_and_cancel`] | 100 tasks | concurrent | `Succeeded` or `Cancelled` |

mod task_graph_builder;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::bail;
use async_trait::async_trait;
use dashmap::DashMap;
use rand::{Rng, SeedableRng};
use spider_core::{
    job::JobState,
    task::TaskIndex,
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
    db::{DbError, InternalJobOrchestration},
    ready_queue::ReadyQueueSender,
    task_instance_pool::TaskInstancePoolConnector,
};
use task_graph_builder::{build_flat_task_graph, build_neural_net_task_graph};
use tokio::sync::{Mutex, mpsc, watch};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The number of concurrent worker tasks to spawn.
const NUM_WORKERS: usize = 64;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// The submitted task graph type from spider-core.
type SubmittedTaskGraph = spider_core::task::TaskGraph;

/// The concrete JCB type used throughout these tests.
type TestJcb = SharedJobControlBlock<MockReadyQueueSender, NoopDbConnector, MockTaskInstancePool>;

/// A handler that generates mock task outputs from an [`ExecutionContext`].
///
/// Takes the full execution context (containing `task_instance_id`, `tdl_context`, `inputs`,
/// `timeout_policy`) and returns a `Vec<TaskOutput>`. This allows tests to:
///
/// * Assert input contents by inspecting `exec_ctx.inputs`.
/// * Vary output generation based on `tdl_context.task_func`.
/// * Produce outputs of the correct count and size.
type TaskOutputHandler = Arc<dyn Fn(&ExecutionContext) -> Vec<TaskOutput> + Send + Sync>;

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

/// A mock [`ReadyQueueSender`] backed by a `tokio::sync::mpsc::UnboundedSender`.
///
/// Workers share the corresponding receiver via `Arc<Mutex<UnboundedReceiver<ReadyMessage>>>` for
/// MPMC semantics. The `job_id` parameter in each trait method is discarded since only one job
/// runs at a time.
#[derive(Clone)]
struct MockReadyQueueSender {
    tx: mpsc::UnboundedSender<ReadyMessage>,
}

#[async_trait]
impl ReadyQueueSender for MockReadyQueueSender {
    async fn send_task_ready(
        &self,
        _job_id: JobId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError> {
        for task_index in task_indices {
            self.tx
                .send(ReadyMessage::Task { task_index })
                .map_err(|_| InternalError::ReadyQueueSendFailure("channel closed".to_owned()))?;
        }
        Ok(())
    }

    async fn send_commit_ready(&self, _job_id: JobId) -> Result<(), InternalError> {
        self.tx
            .send(ReadyMessage::Commit)
            .map_err(|_| InternalError::ReadyQueueSendFailure("channel closed".to_owned()))
    }

    async fn send_cleanup_ready(&self, _job_id: JobId) -> Result<(), InternalError> {
        self.tx
            .send(ReadyMessage::Cleanup)
            .map_err(|_| InternalError::ReadyQueueSendFailure("channel closed".to_owned()))
    }
}

/// A stateless DB connector stub.
///
/// Returns the appropriate [`JobState`] transitions based on whether the job has commit/cleanup
/// tasks. All other methods are no-ops. Designed so a real DB connector can be swapped in later
/// by changing the type parameter on the JCB.
#[derive(Clone)]
struct NoopDbConnector {
    has_commit_task: bool,
    has_cleanup_task: bool,
}

#[async_trait]
impl InternalJobOrchestration for NoopDbConnector {
    async fn set_state(&self, _job_id: JobId, _state: JobState) -> Result<(), DbError> {
        Ok(())
    }

    async fn commit_outputs(
        &self,
        _job_id: JobId,
        _job_outputs: Vec<TaskOutput>,
    ) -> Result<JobState, DbError> {
        if self.has_commit_task {
            Ok(JobState::CommitReady)
        } else {
            Ok(JobState::Succeeded)
        }
    }

    async fn start(&self, _job_id: JobId) -> Result<(), DbError> {
        Ok(())
    }

    async fn cancel(&self, _job_id: JobId) -> Result<JobState, DbError> {
        if self.has_cleanup_task {
            Ok(JobState::CleanupReady)
        } else {
            Ok(JobState::Cancelled)
        }
    }

    async fn fail(&self, _job_id: JobId, _error_message: String) -> Result<(), DbError> {
        Ok(())
    }

    async fn delete_expired_terminated_jobs(
        &self,
        _expire_after: std::time::Duration,
    ) -> Result<Vec<JobId>, DbError> {
        Ok(Vec::new())
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

/// Shared state passed to every worker.
struct WorkerContext {
    /// The shared ready-queue receiver (MPMC via mutex).
    receiver: Arc<Mutex<mpsc::UnboundedReceiver<ReadyMessage>>>,

    /// The JCB under test.
    jcb: TestJcb,

    /// Watch channel sender — workers send the terminal [`JobState`] when observed.
    terminal_state_tx: watch::Sender<Option<JobState>>,

    /// Watch channel receiver — workers use this to detect shutdown.
    done_rx: watch::Receiver<bool>,

    /// Tracks which [`TaskIndex`]es have been seen for failure injection.
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

/// The result of running a workload to completion.
struct WorkloadResult {
    /// The terminal [`JobState`] observed by a worker.
    terminal_state: JobState,

    /// The number of regular tasks that were successfully completed by workers.
    task_success_count: usize,

    /// The number of times a commit task was successfully processed.
    commit_count: usize,

    /// The number of times a cleanup task was successfully processed.
    cleanup_count: usize,
}

/// Describes when (if ever) to cancel the job.
enum CancelPolicy {
    /// Do not cancel.
    Never,

    /// Cancel immediately after `start()`, before any worker processes a message.
    Immediate,

    /// Cancel concurrently — spawn a separate task that cancels after a short delay.
    Concurrent,
}

// ---------------------------------------------------------------------------
// Const functions
// ---------------------------------------------------------------------------

/// Returns `true` if the error is a [`CacheError::StaleState`] variant.
///
/// Stale-state errors are expected during concurrent execution (e.g., a task was already succeeded
/// by another worker) and should be silently ignored.
const fn is_stale_state(err: &CacheError) -> bool {
    matches!(err, CacheError::StaleState(_))
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

/// If `state` is terminal, broadcasts it on the watch channel.
fn check_terminal(ctx: &WorkerContext, state: JobState) {
    if state.is_terminal() {
        // Ignore send errors — the receiver may already have a terminal value.
        let _ = ctx.terminal_state_tx.send(Some(state));
    }
}

/// Creates a default output handler that produces one output of `output_size` bytes per task.
fn default_output_handler(output_size: usize) -> TaskOutputHandler {
    Arc::new(move |_exec_ctx: &ExecutionContext| -> Vec<TaskOutput> {
        vec![vec![0u8; output_size]]
    })
}

/// Runs a single worker that consumes [`ReadyMessage`]s from the shared queue and drives task
/// execution through the JCB.
///
/// The worker loops until either the done signal fires or the receiver is closed (returns `None`).
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
async fn run_worker(ctx: Arc<WorkerContext>) -> anyhow::Result<()> {
    let mut rng = rand::rngs::StdRng::from_os_rng();
    let mut done_rx = ctx.done_rx.clone();
    loop {
        // Use `select!` so the worker can be interrupted by the done signal even while waiting
        // for the receiver lock. The lock guard is dropped when the losing branch is cancelled.
        let msg = tokio::select! {
            msg = async {
                let mut rx = ctx.receiver.lock().await;
                rx.recv().await
            } => msg,
            _ = done_rx.wait_for(|&done| done) => break,
        };
        let Some(msg) = msg else {
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

/// Processes a single task index: applies failure injection, then succeeds or fails the task.
///
/// Returns the [`CacheError`] from the JCB if any non-stale error occurs, so the caller can
/// decide whether to propagate or ignore it.
async fn process_task(
    ctx: &WorkerContext,
    rng: &mut impl Rng,
    task_index: TaskIndex,
) -> Result<(), CacheError> {
    // Always-fail mode: every instance fails unconditionally.
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
        check_terminal(ctx, state);
        return Ok(());
    }

    let should_inject_failure = rng.random_bool(0.5);
    if should_inject_failure {
        let is_first_seen = ctx.seen_tasks.insert(task_index, ()).is_none();
        if is_first_seen {
            // Spawn two concurrent coroutines that each register and fail a task instance,
            // exercising both the retry and concurrent-instance logic.
            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let jcb = ctx.jcb.clone();
                    let terminal_tx = ctx.terminal_state_tx.clone();
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
                            let _ = terminal_tx.send(Some(state));
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
        // Already seen — fall through to succeed.
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
    check_terminal(ctx, state);
    Ok(())
}

/// Processes a commit-ready message: creates a commit task instance and succeeds it.
async fn process_commit(ctx: &WorkerContext) -> Result<(), CacheError> {
    let exec_ctx = ctx.jcb.create_task_instance(TaskId::Commit).await?;
    let state = ctx
        .jcb
        .succeed_commit_task_instance(exec_ctx.task_instance_id)
        .await?;
    ctx.commit_count.fetch_add(1, Ordering::Relaxed);
    check_terminal(ctx, state);
    Ok(())
}

/// Processes a cleanup-ready message: creates a cleanup task instance and succeeds it.
async fn process_cleanup(ctx: &WorkerContext) -> Result<(), CacheError> {
    let exec_ctx = ctx.jcb.create_task_instance(TaskId::Cleanup).await?;
    let state = ctx
        .jcb
        .succeed_cleanup_task_instance(exec_ctx.task_instance_id)
        .await?;
    ctx.cleanup_count.fetch_add(1, Ordering::Relaxed);
    check_terminal(ctx, state);
    Ok(())
}

/// Creates a JCB from the given task graph, starts it, spawns workers, and runs to completion.
///
/// # Parameters
///
/// * `submitted_task_graph` -- the task graph to execute.
/// * `inputs` -- job inputs matching the graph's input tasks.
/// * `cancel_policy` -- whether and when to cancel the job.
/// * `output_handler` -- generates mock task outputs from each [`ExecutionContext`].
/// * `always_fail` -- if `true`, workers always fail every task instance (no random injection).
///
/// # Returns
///
/// A [`WorkloadResult`] containing the terminal state and commit/cleanup execution counts.
async fn run_workload(
    submitted_task_graph: &SubmittedTaskGraph,
    inputs: Vec<TaskInput>,
    cancel_policy: CancelPolicy,
    output_handler: TaskOutputHandler,
    always_fail: bool,
) -> anyhow::Result<WorkloadResult> {
    let has_commit_task = submitted_task_graph.get_commit_task_descriptor().is_some();
    let has_cleanup_task = submitted_task_graph.get_cleanup_task_descriptor().is_some();

    // Create mock components.
    let (tx, rx) = mpsc::unbounded_channel::<ReadyMessage>();
    let receiver = Arc::new(Mutex::new(rx));
    let ready_queue_sender = MockReadyQueueSender { tx };
    let db_connector = NoopDbConnector {
        has_commit_task,
        has_cleanup_task,
    };
    let task_instance_pool = MockTaskInstancePool::new();

    // Create and start the JCB.
    let jcb = SharedJobControlBlock::create(
        JobId::default(),
        ResourceGroupId::default(),
        submitted_task_graph,
        inputs,
        ready_queue_sender,
        db_connector,
        task_instance_pool,
    )
    .await?;

    jcb.start().await?;

    // Set up the terminal-state watch channel and done signal before cancellation, so that
    // an immediate cancel that produces a terminal state can be captured.
    let (terminal_tx, mut terminal_rx) = watch::channel::<Option<JobState>>(None);
    let (done_tx, done_rx) = watch::channel::<bool>(false);

    // Apply cancellation policy.
    let cancel_handle = match cancel_policy {
        CancelPolicy::Never => None,
        CancelPolicy::Immediate => {
            // Cancel synchronously before workers process any messages.
            match jcb.cancel().await {
                Ok(state) if state.is_terminal() => {
                    let _ = terminal_tx.send(Some(state));
                }
                Ok(_) => {}
                Err(e) if is_stale_state(&e) => {}
                Err(e) => bail!(e),
            }
            None
        }
        CancelPolicy::Concurrent => {
            let jcb_clone = jcb.clone();
            let tx_clone = terminal_tx.clone();
            Some(tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                match jcb_clone.cancel().await {
                    Ok(state) if state.is_terminal() => {
                        let _ = tx_clone.send(Some(state));
                    }
                    Ok(_) | Err(CacheError::StaleState(_)) => {}
                    Err(e) => panic!("unexpected cancel error: {e:?}"),
                }
            }))
        }
    };

    let task_success_count = Arc::new(AtomicUsize::new(0));
    let commit_count = Arc::new(AtomicUsize::new(0));
    let cleanup_count = Arc::new(AtomicUsize::new(0));

    let ctx = Arc::new(WorkerContext {
        receiver: receiver.clone(),
        jcb,
        terminal_state_tx: terminal_tx,
        done_rx,
        seen_tasks: Arc::new(DashMap::new()),
        output_handler,
        task_success_count: task_success_count.clone(),
        commit_count: commit_count.clone(),
        cleanup_count: cleanup_count.clone(),
        always_fail,
    });

    // Spawn workers.
    let mut join_set = tokio::task::JoinSet::new();
    for _ in 0..NUM_WORKERS {
        let ctx = ctx.clone();
        join_set.spawn(async move { run_worker(ctx).await });
    }

    // Wait for a terminal state, then signal workers to exit.
    terminal_rx
        .wait_for(Option::is_some)
        .await
        .expect("terminal state watch channel should not be dropped");
    let terminal_state = terminal_rx
        .borrow()
        .expect("terminal state should be set after wait_for");

    // Signal all workers to exit via the done channel.
    let _ = done_tx.send(true);

    // Await all workers and propagate the first error.
    while let Some(result) = join_set.join_next().await {
        result.expect("worker task should not panic")?;
    }

    // Await the concurrent cancel task if any.
    if let Some(handle) = cancel_handle {
        handle.await.expect("cancel task should not panic");
    }

    Ok(WorkloadResult {
        terminal_state,
        task_success_count: task_success_count.load(Ordering::Relaxed),
        commit_count: commit_count.load(Ordering::Relaxed),
        cleanup_count: cleanup_count.load(Ordering::Relaxed),
    })
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

/// Runs the flat workload (10,000 independent tasks with commit + cleanup) to successful
/// completion.
///
/// Verifies that:
/// * The terminal state is `Succeeded`.
/// * All 10,000 tasks were successfully completed.
/// * The commit task executed exactly once.
/// * The cleanup task did not execute.
#[tokio::test(flavor = "multi_thread")]
async fn test_flat_success() -> anyhow::Result<()> {
    let (graph, inputs) = build_flat_task_graph(10_000, 1024, true, true);
    let num_tasks = graph.get_num_tasks();
    let result = run_workload(
        &graph,
        inputs,
        CancelPolicy::Never,
        default_output_handler(1024),
        false,
    )
    .await?;

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
    Ok(())
}

/// Cancels the flat workload immediately after starting. Since the MPSC channel is FIFO and
/// cancel is called before any worker processes a message, the job deterministically reaches
/// `Cancelled`.
///
/// Verifies that:
/// * The terminal state is `Cancelled`.
/// * The commit task did not execute.
/// * The cleanup task executed exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn test_flat_cancel() -> anyhow::Result<()> {
    let (graph, inputs) = build_flat_task_graph(10_000, 1024, true, true);
    let result = run_workload(
        &graph,
        inputs,
        CancelPolicy::Immediate,
        default_output_handler(1024),
        false,
    )
    .await?;

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
    Ok(())
}

/// Runs the neural-net workload (10 layers × 1,000 tasks, no termination tasks) to successful
/// completion.
///
/// Verifies that:
/// * The terminal state is `Succeeded`.
/// * All 10,000 tasks were successfully completed.
/// * No commit or cleanup tasks executed (since the graph has none).
#[tokio::test(flavor = "multi_thread")]
async fn test_neural_net_success() -> anyhow::Result<()> {
    let (graph, inputs) = build_neural_net_task_graph();
    let num_tasks = graph.get_num_tasks();
    let result = run_workload(
        &graph,
        inputs,
        CancelPolicy::Never,
        default_output_handler(128),
        false,
    )
    .await?;

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
    Ok(())
}

/// Cancels the neural-net workload immediately after starting. Since there is no cleanup task,
/// the job transitions directly to `Cancelled`.
///
/// Verifies that:
/// * The terminal state is `Cancelled`.
/// * No commit or cleanup tasks executed.
#[tokio::test(flavor = "multi_thread")]
async fn test_neural_net_cancel() -> anyhow::Result<()> {
    let (graph, inputs) = build_neural_net_task_graph();
    let result = run_workload(
        &graph,
        inputs,
        CancelPolicy::Immediate,
        default_output_handler(128),
        false,
    )
    .await?;

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
    Ok(())
}

/// Runs a single task that always fails (`max_num_retry = 3`, all instances fail). The job
/// should transition to `Failed` after retries are exhausted.
#[tokio::test(flavor = "multi_thread")]
async fn test_always_fail_terminates_job() -> anyhow::Result<()> {
    let (graph, inputs) = build_flat_task_graph(1, 128, false, false);
    let result = run_workload(
        &graph,
        inputs,
        CancelPolicy::Never,
        default_output_handler(128),
        true,
    )
    .await?;

    assert_eq!(
        result.terminal_state,
        JobState::Failed,
        "always-fail task should cause job to fail"
    );
    assert_eq!(
        result.task_success_count, 0,
        "no tasks should succeed in always-fail mode"
    );
    Ok(())
}

/// Races task execution against cancellation. A small flat workload (100 tasks with commit +
/// cleanup) is started and a cancel is issued concurrently after a short delay.
///
/// The terminal state must be exactly one of `Succeeded` or `Cancelled` — no other state is
/// valid.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_success_and_cancel() -> anyhow::Result<()> {
    let (graph, inputs) = build_flat_task_graph(100, 128, true, true);
    let result = run_workload(
        &graph,
        inputs,
        CancelPolicy::Concurrent,
        default_output_handler(128),
        false,
    )
    .await?;

    assert!(
        result.terminal_state == JobState::Succeeded
            || result.terminal_state == JobState::Cancelled,
        "concurrent success/cancel should produce Succeeded or Cancelled, got {:?}",
        result.terminal_state
    );
    Ok(())
}
