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
use task_graph_builder::{SubmittedTaskGraph, build_flat_task_graph, build_neural_net_task_graph};
use tokio::sync::watch;

/// The number of concurrent worker tasks to spawn.
const NUM_WORKERS: usize = 64;

/// The concrete JCB type used throughout these tests.
type TestJcb = SharedJobControlBlock<MockReadyQueueSender, NoopDbConnector, MockTaskInstancePool>;

/// A handler that generates mock task outputs from an [`ExecutionContext`], which simulates the
/// execution of a TDL task.
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

/// Per-worker context. All fields are cheaply cloneable (via `Arc` or built-in `Clone` impls),
/// so each worker owns its own copy without shared `Arc<WorkerContext>` indirection.
#[derive(Clone)]
struct WorkerContext {
    /// The MPMC ready-queue receiver. Each clone can concurrently await messages without
    /// serialization.
    receiver: async_channel::Receiver<ReadyMessage>,

    /// The JCB under test.
    jcb: TestJcb,

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
fn broadcast_if_terminated(ctx: &WorkerContext, state: JobState) {
    if state.is_terminal() {
        // Ignore send errors — the receiver may already have a terminal value.
        let _ = ctx.terminal_state_sender.send(Some(state));
    }
}

/// # Returns
///
/// A default (noop) output handler that produces one output of `output_size` bytes per task. This
/// output is currently independent of the execution context.
fn default_output_handler(output_size: usize) -> TaskOutputHandler {
    Arc::new(move |_: &ExecutionContext| -> Vec<TaskOutput> { vec![vec![0u8; output_size]] })
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
///
/// # Errors
///
/// Forwards all errors that are not [`CacheError::StaleState`].
async fn run_worker(mut ctx: WorkerContext) -> anyhow::Result<()> {
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
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`SharedTerminationTaskControlBlock::register_task_instance`]'s return values on
///   failure.
/// * Forwards [`SharedTerminationTaskControlBlock::succeed_task_instance`]'s return values on
///   failure.
/// * Forwards [`SharedTerminationTaskControlBlock::fail_task_instance`]'s return values on failure.
async fn process_task(
    ctx: &WorkerContext,
    rng: &mut impl Rng,
    task_index: TaskIndex,
) -> Result<(), CacheError> {
    if ctx.always_fail {
        // In always-fail, every instance fails unconditionally.
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
        // Failure ingestion
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let jcb = ctx.jcb.clone();
                let terminal_tx = ctx.terminal_state_sender.clone();
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
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`SharedJobControlBlock::create_task_instance`]'s return values on failure.
/// * Forwards [`SharedJobControlBlock::succeed_commit_task_instance`]'s return values of failure.
async fn process_commit(ctx: &WorkerContext) -> Result<(), CacheError> {
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
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`SharedJobControlBlock::create_task_instance`]'s return values on failure.
/// * Forwards [`SharedJobControlBlock::succeed_cleanup_task_instance`]'s return values of failure.
async fn process_cleanup(ctx: &WorkerContext) -> Result<(), CacheError> {
    let exec_ctx = ctx.jcb.create_task_instance(TaskId::Cleanup).await?;
    let state = ctx
        .jcb
        .succeed_cleanup_task_instance(exec_ctx.task_instance_id)
        .await?;
    ctx.cleanup_count.fetch_add(1, Ordering::Relaxed);
    broadcast_if_terminated(ctx, state);
    Ok(())
}

/// Creates a JCB from the given task graph, starts it, spawns workers, and runs to completion.
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
) -> WorkloadResult {
    let has_commit_task = submitted_task_graph.get_commit_task_descriptor().is_some();
    let has_cleanup_task = submitted_task_graph.get_cleanup_task_descriptor().is_some();

    // Create mock components.
    let (tx, rx) = async_channel::unbounded::<ReadyMessage>();
    let ready_queue_sender = MockReadyQueueSender { sender: tx };
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
    .await
    .expect("failed to create JCB");

    jcb.start().await.expect("failed to start JCB");

    let (terminal_state_sender, mut terminal_state_receiver) =
        watch::channel::<Option<JobState>>(None);
    let (done_sender, done_receiver) = watch::channel::<bool>(false);
    let task_success_count = Arc::new(AtomicUsize::new(0));
    let commit_count = Arc::new(AtomicUsize::new(0));
    let cleanup_count = Arc::new(AtomicUsize::new(0));

    let ctx = WorkerContext {
        receiver: rx,
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
            // Cancel synchronously before workers process any messages.
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
            let tx_clone = terminal_state_sender.clone();
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

    // Wait for a terminal state, then signal workers to exit.
    terminal_state_receiver
        .wait_for(Option::is_some)
        .await
        .expect("terminal state watch channel should not be dropped");
    let terminal_state = terminal_state_receiver
        .borrow()
        .expect("terminal state should be set after wait_for");

    // Signal all workers to exit via the done channel. Ignore the error since all workers might be
    // closed, meaning that no living receiver.
    let _ = done_sender.send(true);

    while let Some(result) = join_set.join_next().await {
        result
            .expect("worker task should not panic")
            .expect("worker early returns on error");
    }

    // Await the concurrent cancel task if any.
    if let Some(handle) = cancel_handle {
        handle.await.expect("cancel task should not panic");
    }

    WorkloadResult {
        terminal_state,
        task_success_count: task_success_count.load(Ordering::Relaxed),
        commit_count: commit_count.load(Ordering::Relaxed),
        cleanup_count: cleanup_count.load(Ordering::Relaxed),
    }
}

/// Runs the flat workload (10,000 independent tasks with commit + cleanup) to successful
/// completion.
///
/// Verifies that:
///
/// * The terminal state is [`JobState::Succeeded`].
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
    Ok(())
}

/// Cancels the flat workload immediately after starting.
///
/// Verifies that:
///
/// * The terminal state is [`JobState::Cancelled`].
/// * The commit task did not execute.
/// * The cleanup task executed exactly once.
///
/// # NOTE
///
/// Since we spawn workers before cancelling the job, there is a chance that when the cancellation
/// is issued, the commit task has finished. However, this should rarely happen in practice, since:
///
/// * It takes a while for workers to consume all tasks, which should give the main coroutine enough
///   time to cancel the job.
/// * The underlying ready-queue is FIFO. As long as cleanup-ready message is sent before
///   commit-ready, the job should be cancellable.
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
    Ok(())
}

/// Runs the neural-net workload (10 layers × 1,000 tasks, no termination tasks) to successful
/// completion.
///
/// Verifies that:
///
/// * The terminal state is [`JobState::Succeeded`].
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
    Ok(())
}

/// Cancels the neural-net workload immediately after starting.
///
/// Verifies that:
///
/// * The terminal state is [`JobState::Cancelled`].
/// * No commit or cleanup tasks executed.
///
/// # NOTE
///
/// Since we spawn workers before cancelling the job, there is a chance that when the cancellation
/// is issued, all tasks have finished. However, this should rarely happen in practice, since it
/// takes a while for workers to consume all tasks (especially when needing to resolve
/// dependencies), which should give the main coroutine enough time to cancel the job before it
/// terminates.
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
    Ok(())
}

/// Runs a job whose tasks always fail (`max_num_retry = 3`, all instances fail). The job should
/// transition to [`JobState::Failed`] after retries are exhausted.
#[tokio::test(flavor = "multi_thread")]
async fn test_always_fail_terminates_job() -> anyhow::Result<()> {
    let (graph, inputs) = build_flat_task_graph(3, 128, false, false);
    let result = run_workload(
        &graph,
        inputs,
        CancelPolicy::Never,
        default_output_handler(128),
        true,
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
    Ok(())
}

/// Races task execution against cancellation. A small flat workload (100 tasks with commit +
/// cleanup) is started and a cancel is issued concurrently after a short delay.
///
/// The terminal state must be exactly one of [`JobState::Succeeded`] or [`JobState::Cancelled`]: no
/// other state is valid.
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
    .await;

    assert!(
        result.terminal_state == JobState::Succeeded
            || result.terminal_state == JobState::Cancelled,
        "concurrent success/cancel should produce Succeeded or Cancelled, got {:?}",
        result.terminal_state
    );
    Ok(())
}
