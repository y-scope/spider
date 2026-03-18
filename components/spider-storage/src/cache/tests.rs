use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use spider_core::{
    job::JobState,
    task::{
        BytesTypeDescriptor, DataTypeDescriptor, ExecutionPolicy, TaskDescriptor,
        TaskGraph as CoreTaskGraph, TaskIndex, TaskInputOutputIndex, TerminationTaskDescriptor,
        ValueTypeDescriptor,
    },
    types::{
        id::{JobId, ResourceGroupId, TaskInstanceId},
        io::{TaskInput, TaskOutput},
    },
};
use tokio::sync::Mutex;

use crate::{
    cache::{
        build_job,
        error::{CacheError, InternalError, RejectionError},
        job::{ReadyQueueConnector, TaskInstancePoolConnector},
        task::{SharedTaskControlBlock, SharedTerminationTaskControlBlock},
        types::{ExecutionContext, TaskId},
    },
    db::{DbError, InternalJobOrchestration},
};

// =============================================================================
// Mock implementations
// =============================================================================

type ReadyTaskList = Arc<Mutex<Vec<(JobId, Vec<TaskIndex>)>>>;

/// A mock ready queue that records all ready-task notifications.
///
/// When `worker_txs` is set, it round-robin dispatches newly-ready task indices across
/// per-worker channels so that workers can pick them up without contention on a shared receiver.
struct MockReadyQueue {
    ready_tasks: ReadyTaskList,
    commit_ready_count: Arc<AtomicU64>,
    cleanup_ready_count: Arc<AtomicU64>,
    worker_txs: Option<Vec<tokio::sync::mpsc::UnboundedSender<TaskIndex>>>,
    round_robin_counter: AtomicU64,
}

impl MockReadyQueue {
    fn new() -> Self {
        Self {
            ready_tasks: Arc::new(Mutex::new(Vec::new())),
            commit_ready_count: Arc::new(AtomicU64::new(0)),
            cleanup_ready_count: Arc::new(AtomicU64::new(0)),
            worker_txs: None,
            round_robin_counter: AtomicU64::new(0),
        }
    }

    fn with_worker_channels(txs: Vec<tokio::sync::mpsc::UnboundedSender<TaskIndex>>) -> Self {
        Self {
            ready_tasks: Arc::new(Mutex::new(Vec::new())),
            commit_ready_count: Arc::new(AtomicU64::new(0)),
            cleanup_ready_count: Arc::new(AtomicU64::new(0)),
            worker_txs: Some(txs),
            round_robin_counter: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl ReadyQueueConnector for MockReadyQueue {
    async fn send_task_ready(
        &self,
        job_id: JobId,
        task_ids: Vec<TaskIndex>,
    ) -> Result<(), InternalError> {
        if let Some(txs) = &self.worker_txs {
            let num_workers = txs.len();
            for &idx in &task_ids {
                let worker = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) as usize
                    % num_workers;
                let _ = txs[worker].send(idx);
            }
        }
        self.ready_tasks.lock().await.push((job_id, task_ids));
        Ok(())
    }

    async fn send_commit_ready(&self, _job_id: JobId) -> Result<(), InternalError> {
        self.commit_ready_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn send_cleanup_ready(&self, _job_id: JobId) -> Result<(), InternalError> {
        self.cleanup_ready_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct MockDb {
    has_commit_task: bool,
}

impl MockDb {
    fn new(has_commit_task: bool) -> Self {
        Self { has_commit_task }
    }
}

#[async_trait]
impl InternalJobOrchestration for MockDb {
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

    async fn cancel(&self, _job_id: JobId) -> Result<JobState, DbError> {
        Ok(JobState::Cancelled)
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

struct MockInstancePool {
    next_id: AtomicU64,
}

impl MockInstancePool {
    fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
        }
    }
}

#[async_trait]
impl TaskInstancePoolConnector for MockInstancePool {
    fn get_next_available_task_instance_id(&self) -> TaskInstanceId {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn register_task_instance(
        &self,
        _task_instance_id: TaskInstanceId,
        _task: SharedTaskControlBlock,
    ) -> Result<(), InternalError> {
        Ok(())
    }

    async fn register_termination_task_instance(
        &self,
        _task_instance_id: TaskInstanceId,
        _termination_task: SharedTerminationTaskControlBlock,
    ) -> Result<(), InternalError> {
        Ok(())
    }
}

// =============================================================================
// Test execution framework
// =============================================================================

/// Pluggable per-task behavior during test execution.
///
/// Implementations control what happens when a worker picks up a task: they can complete it
/// immediately, inject failures, add delays, or register multiple instances.
#[async_trait]
trait TaskHandler: Send + Sync {
    /// Returns the number of concurrent instances to register per task.
    fn num_instances(&self) -> usize {
        1
    }

    /// Called for each task instance. Returns outputs to submit on success, or an error message
    /// to fail the instance.
    async fn handle_instance(
        &self,
        task_index: TaskIndex,
        instance_index: usize,
        ctx: &ExecutionContext,
    ) -> Result<Vec<TaskOutput>, String>;
}

/// Default handler: immediately completes each task with 1KB outputs (1 instance per task).
struct ImmediateCompletionHandler {
    num_outputs_per_task: usize,
}

#[async_trait]
impl TaskHandler for ImmediateCompletionHandler {
    async fn handle_instance(
        &self,
        _task_index: TaskIndex,
        _instance_index: usize,
        _ctx: &ExecutionContext,
    ) -> Result<Vec<TaskOutput>, String> {
        Ok((0..self.num_outputs_per_task)
            .map(|_| make_1kb_payload())
            .collect())
    }
}

/// Handler that runs 3 instances per task: 2 succeed and 1 fails.
///
/// The last instance always fails; the first two succeed. Since instances run concurrently, the
/// first completion wins and subsequent completions/failures receive rejection errors (e.g.
/// `TaskAlreadyTerminated`), which are handled gracefully by the worker loop.
struct MultiInstancePartialFailHandler {
    num_outputs_per_task: usize,
    num_instances: usize,
}

#[async_trait]
impl TaskHandler for MultiInstancePartialFailHandler {
    fn num_instances(&self) -> usize {
        self.num_instances
    }

    async fn handle_instance(
        &self,
        _task_index: TaskIndex,
        instance_index: usize,
        _ctx: &ExecutionContext,
    ) -> Result<Vec<TaskOutput>, String> {
        if instance_index < self.num_instances - 1 {
            Ok((0..self.num_outputs_per_task)
                .map(|_| make_1kb_payload())
                .collect())
        } else {
            Err(format!("simulated failure for instance {instance_index}"))
        }
    }
}

/// Handler where every instance always fails. Used to test retry exhaustion.
struct AlwaysFailHandler;

#[async_trait]
impl TaskHandler for AlwaysFailHandler {
    async fn handle_instance(
        &self,
        task_index: TaskIndex,
        instance_index: usize,
        _ctx: &ExecutionContext,
    ) -> Result<Vec<TaskOutput>, String> {
        Err(format!(
            "permanent failure for task {task_index} instance {instance_index}"
        ))
    }
}

/// Returns true if the error is a rejection (expected under concurrency), false if internal.
/// Internal errors are unexpected and should be propagated.
fn is_rejection(err: &CacheError) -> bool {
    matches!(err, CacheError::Rejection(_))
}

/// A single instance's timing: which task, which instance, how long from register to complete/fail.
#[derive(Debug)]
struct InstanceLatency {
    task_index: TaskIndex,
    duration: Duration,
}

/// Collected results from a test run.
struct TestResult {
    total_execution_time: Duration,
    /// Per-instance latencies (one entry per `create_task_instance` → `complete/fail` cycle).
    /// For single-instance tests, there is one entry per task. For multi-instance tests, there
    /// are multiple entries per task.
    instance_latencies: Vec<InstanceLatency>,
    /// Number of unique tasks that were dispatched to workers.
    tasks_dispatched: usize,
    final_state: JobState,
    ready_queue_call_count: usize,
    total_tasks_reported_ready: usize,
}

impl TestResult {
    fn report(
        &self,
        test_name: &str,
        num_workers: usize,
        graph_construction_time: Duration,
        build_job_time: Duration,
    ) {
        let mut sorted: Vec<f64> = self
            .instance_latencies
            .iter()
            .map(|l| l.duration.as_secs_f64() * 1000.0)
            .collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).expect("latencies should be comparable"));

        let num_instances = sorted.len();
        let avg = if num_instances > 0 {
            sorted.iter().sum::<f64>() / num_instances as f64
        } else {
            0.0
        };
        let p50 = percentile(&sorted, 50.0);
        let p95 = percentile(&sorted, 95.0);
        let p99 = percentile(&sorted, 99.0);

        eprintln!();
        eprintln!("=== {test_name} ({num_workers} workers) ===");
        eprintln!(
            "  graph_construction:           {:>10.2} ms",
            graph_construction_time.as_secs_f64() * 1000.0
        );
        eprintln!(
            "  build_job:                   {:>10.2} ms",
            build_job_time.as_secs_f64() * 1000.0
        );
        eprintln!(
            "  total_execution:             {:>10.2} ms",
            self.total_execution_time.as_secs_f64() * 1000.0
        );
        eprintln!("  tasks_dispatched:             {:>10}", self.tasks_dispatched);
        eprintln!("  instances_measured:            {:>10}", num_instances);
        eprintln!("  avg_per_instance_latency:      {avg:>10.3} ms");
        eprintln!("  p50_per_instance_latency:      {p50:>10.3} ms");
        eprintln!("  p95_per_instance_latency:      {p95:>10.3} ms");
        eprintln!("  p99_per_instance_latency:      {p99:>10.3} ms");
        eprintln!(
            "  ready_queue_calls:            {:>10}",
            self.ready_queue_call_count
        );
        eprintln!(
            "  total_tasks_reported_ready:   {:>10}",
            self.total_tasks_reported_ready
        );
        eprintln!();
    }
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (pct / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Full entry point for a scheduled test: builds the job, runs workers, returns results.
///
/// Each worker gets its own dedicated channel. The `MockReadyQueue` round-robins newly-ready
/// task indices across worker channels, eliminating contention on a shared receiver.
///
/// When `task_handler.num_instances() > 1`, the worker dispatches multiple concurrent instances
/// per task. Each instance calls `create_task_instance` independently; one succeeds, the rest
/// may get rejection errors (e.g. `TaskAlreadyTerminated`) which are handled gracefully.
#[allow(clippy::too_many_lines)]
async fn run_scheduled_test(
    graph: &CoreTaskGraph,
    job_inputs: Vec<TaskInput>,
    num_workers: usize,
    task_handler: Arc<dyn TaskHandler>,
) -> (TestResult, Duration) {
    // Create per-worker channels.
    let mut worker_txs = Vec::with_capacity(num_workers);
    let mut worker_rxs = Vec::with_capacity(num_workers);
    for _ in 0..num_workers {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<TaskIndex>();
        worker_txs.push(tx);
        worker_rxs.push(rx);
    }

    let ready_queue = MockReadyQueue::with_worker_channels(worker_txs.clone());
    let ready_tasks_ref = ready_queue.ready_tasks.clone();

    let build_start = Instant::now();
    let (jcb, initial_ready) = build_job(
        JobId::new(),
        ResourceGroupId::new(),
        graph,
        job_inputs,
        ready_queue,
        MockDb::new(false),
        MockInstancePool::new(),
    )
    .expect("build_job should succeed");
    let build_job_time = build_start.elapsed();

    // Seed initial ready tasks round-robin across worker channels.
    for (i, &idx) in initial_ready.iter().enumerate() {
        worker_txs[i % num_workers]
            .send(idx)
            .expect("worker channel should be open during seeding");
    }

    let jcb = Arc::new(jcb);
    let latencies: Arc<Mutex<Vec<InstanceLatency>>> = Arc::new(Mutex::new(Vec::new()));
    let tasks_dispatched = Arc::new(AtomicU64::new(0));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let num_instances_per_task = task_handler.num_instances();
    let num_tasks = graph.get_num_tasks();

    let exec_start = Instant::now();

    // Spawn workers, each with its own receiver.
    let mut worker_handles = Vec::with_capacity(num_workers);
    for mut rx in worker_rxs {
        let jcb = Arc::clone(&jcb);
        let handler = Arc::clone(&task_handler);
        let latencies = Arc::clone(&latencies);
        let tasks_dispatched = Arc::clone(&tasks_dispatched);
        let done = Arc::clone(&done);

        worker_handles.push(tokio::spawn(async move {
            loop {
                if done.load(Ordering::Relaxed) {
                    break;
                }

                let task_idx = match rx.try_recv() {
                    Ok(idx) => idx,
                    Err(_) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                };

                tasks_dispatched.fetch_add(1, Ordering::Relaxed);

                let (terminal, mut instance_lats) = if num_instances_per_task == 1 {
                    execute_single_instance(&jcb, &*handler, task_idx).await
                } else {
                    execute_multi_instance(
                        &jcb, &*handler, task_idx, num_instances_per_task,
                    ).await
                };

                latencies.lock().await.append(&mut instance_lats);

                if terminal {
                    done.store(true, Ordering::Relaxed);
                    break;
                }
            }
        }));
    }

    drop(worker_txs);

    for handle in worker_handles {
        handle.await.expect("worker task should not panic");
    }

    let total_execution_time = exec_start.elapsed();

    let ready_queue_snapshot = ready_tasks_ref.lock().await;
    let ready_queue_call_count = ready_queue_snapshot.len();
    let total_tasks_reported_ready: usize = ready_queue_snapshot
        .iter()
        .map(|(_, ids)| ids.len())
        .sum();
    drop(ready_queue_snapshot);

    let instance_latencies = Arc::try_unwrap(latencies)
        .expect("all workers should have finished by now")
        .into_inner();

    let tasks_dispatched = tasks_dispatched.load(Ordering::Relaxed) as usize;
    let final_state = if tasks_dispatched == num_tasks {
        JobState::Succeeded
    } else {
        JobState::Failed
    };

    let result = TestResult {
        total_execution_time,
        instance_latencies,
        tasks_dispatched,
        final_state,
        ready_queue_call_count,
        total_tasks_reported_ready,
    };

    (result, build_job_time)
}

type JcbType = crate::cache::job::JobControlBlock<MockReadyQueue, MockDb, MockInstancePool>;

/// Executes a single instance for the given task.
/// Returns `(is_terminal, instance_latencies)`.
async fn execute_single_instance(
    jcb: &Arc<JcbType>,
    handler: &dyn TaskHandler,
    task_idx: TaskIndex,
) -> (bool, Vec<InstanceLatency>) {
    let inst_start = Instant::now();

    let ctx = match jcb.create_task_instance(TaskId::TaskIndex(task_idx)).await {
        Ok(ctx) => ctx,
        Err(e) => {
            assert!(
                is_rejection(&e),
                "create_task_instance for task {task_idx} returned unexpected error: {e:?}"
            );
            return (false, Vec::new());
        }
    };

    let terminal = match handler.handle_instance(task_idx, 0, &ctx).await {
        Ok(outputs) => {
            match jcb
                .complete_task_instance(ctx.task_instance_id, task_idx, outputs)
                .await
            {
                Ok(state) => state.is_terminal(),
                Err(e) => {
                    assert!(
                        is_rejection(&e),
                        "complete_task_instance for task {task_idx} returned unexpected error: {e:?}"
                    );
                    false
                }
            }
        }
        Err(error_message) => {
            match jcb
                .fail_task_instance(
                    ctx.task_instance_id,
                    TaskId::TaskIndex(task_idx),
                    error_message,
                )
                .await
            {
                Ok(state) => state.is_terminal(),
                Err(e) => {
                    assert!(
                        is_rejection(&e),
                        "fail_task_instance for task {task_idx} returned unexpected error: {e:?}"
                    );
                    false
                }
            }
        }
    };

    let lat = InstanceLatency {
        task_index: task_idx,
        duration: inst_start.elapsed(),
    };
    (terminal, vec![lat])
}

/// Executes multiple concurrent instances for the given task.
/// Returns `(is_terminal, instance_latencies)` with one latency entry per instance.
/// Each instance's timing covers the full cycle: `create_task_instance` → handler →
/// `complete/fail_task_instance`.
async fn execute_multi_instance(
    jcb: &Arc<JcbType>,
    handler: &dyn TaskHandler,
    task_idx: TaskIndex,
    num_instances: usize,
) -> (bool, Vec<InstanceLatency>) {
    // Pre-compute each instance's outcome so we can move it into the spawned task.
    // We use a dummy ExecutionContext for the handler since the real one is created inside
    // the coroutine.
    let dummy_ctx = ExecutionContext {
        task_instance_id: 0,
        tdl_context: crate::cache::types::TdlContext {
            package: String::new(),
            func: String::new(),
        },
        inputs: None,
    };
    let mut outcomes: Vec<Result<Vec<TaskOutput>, String>> = Vec::with_capacity(num_instances);
    for i in 0..num_instances {
        outcomes.push(handler.handle_instance(task_idx, i, &dummy_ctx).await);
    }

    // Spawn one coroutine per instance. Each coroutine does the full cycle:
    // create_task_instance → complete/fail → record latency.
    let mut handles = Vec::with_capacity(num_instances);
    for (instance_index, outcome) in outcomes.into_iter().enumerate() {
        let jcb = Arc::clone(jcb);

        handles.push(tokio::spawn(async move {
            let inst_start = Instant::now();

            let ctx = match jcb.create_task_instance(TaskId::TaskIndex(task_idx)).await {
                Ok(ctx) => ctx,
                Err(e) => {
                    assert!(
                        is_rejection(&e),
                        "create_task_instance for task {task_idx} instance {instance_index} \
                         returned unexpected error: {e:?}"
                    );
                    let lat = InstanceLatency {
                        task_index: task_idx,
                        duration: inst_start.elapsed(),
                    };
                    return (false, lat);
                }
            };

            let terminal = match outcome {
                Ok(outputs) => {
                    match jcb
                        .complete_task_instance(ctx.task_instance_id, task_idx, outputs)
                        .await
                    {
                        Ok(state) => state.is_terminal(),
                        Err(e) => {
                            assert!(
                                is_rejection(&e),
                                "complete_task_instance for task {task_idx} instance \
                                 {instance_index} returned unexpected error: {e:?}"
                            );
                            false
                        }
                    }
                }
                Err(error_message) => {
                    match jcb
                        .fail_task_instance(
                            ctx.task_instance_id,
                            TaskId::TaskIndex(task_idx),
                            error_message,
                        )
                        .await
                    {
                        Ok(state) => state.is_terminal(),
                        Err(e) => {
                            assert!(
                                is_rejection(&e),
                                "fail_task_instance for task {task_idx} instance \
                                 {instance_index} returned unexpected error: {e:?}"
                            );
                            false
                        }
                    }
                }
            };

            let lat = InstanceLatency {
                task_index: task_idx,
                duration: inst_start.elapsed(),
            };
            (terminal, lat)
        }));
    }

    let mut terminal = false;
    let mut lats = Vec::with_capacity(handles.len());
    for handle in handles {
        let (t, lat) = handle.await.expect("instance task should not panic");
        if t {
            terminal = true;
        }
        lats.push(lat);
    }
    (terminal, lats)
}

// =============================================================================
// Graph builders
// =============================================================================

fn bytes_type() -> DataTypeDescriptor {
    DataTypeDescriptor::Value(ValueTypeDescriptor::Bytes(BytesTypeDescriptor {}))
}

fn make_1kb_payload() -> Vec<u8> {
    vec![0xAB_u8; 1024]
}

/// Builds a flat graph of `num_tasks` independent tasks, each with `num_inputs` graph-level
/// inputs and `num_outputs` outputs.
fn build_flat_graph(
    num_tasks: usize,
    num_inputs_per_task: usize,
    num_outputs_per_task: usize,
) -> (CoreTaskGraph, Vec<TaskInput>) {
    build_flat_graph_with_policy(
        num_tasks,
        num_inputs_per_task,
        num_outputs_per_task,
        ExecutionPolicy::default(),
    )
}

fn build_flat_graph_with_policy(
    num_tasks: usize,
    num_inputs_per_task: usize,
    num_outputs_per_task: usize,
    policy: ExecutionPolicy,
) -> (CoreTaskGraph, Vec<TaskInput>) {
    let mut graph = CoreTaskGraph::default();
    for i in 0..num_tasks {
        graph
            .insert_task(TaskDescriptor {
                tdl_package: "pkg".into(),
                tdl_function: format!("fn_{i}"),
                inputs: vec![bytes_type(); num_inputs_per_task],
                outputs: vec![bytes_type(); num_outputs_per_task],
                input_sources: None,
                execution_policy: policy.clone(),
            })
            .expect("flat graph task insertion should succeed");
    }
    let job_inputs: Vec<TaskInput> = (0..num_tasks * num_inputs_per_task)
        .map(|_| TaskInput::ValuePayload(make_1kb_payload()))
        .collect();
    (graph, job_inputs)
}

/// Builds a layered neural-network-style graph.
///
/// Returns `(graph, job_inputs, layers)` where `layers[i]` contains the task indices for layer
/// `i`. Layer 0 tasks are input tasks with `fan_in` graph-level inputs each. Tasks in
/// subsequent layers receive outputs from `fan_in` tasks in the previous layer using circular
/// connectivity: task at position `p` in layer `L` receives outputs from positions
/// `(p - fan_in/2) % width .. (p - fan_in/2 + fan_in - 1) % width` in layer `L-1`.
fn build_neural_net_graph(
    num_layers: usize,
    width: usize,
    fan_in: usize,
) -> (CoreTaskGraph, Vec<TaskInput>, Vec<Vec<TaskIndex>>) {
    build_neural_net_graph_with_policy(
        num_layers,
        width,
        fan_in,
        ExecutionPolicy::default(),
    )
}

fn build_neural_net_graph_with_policy(
    num_layers: usize,
    width: usize,
    fan_in: usize,
    policy: ExecutionPolicy,
) -> (CoreTaskGraph, Vec<TaskInput>, Vec<Vec<TaskIndex>>) {
    let mut graph = CoreTaskGraph::default();
    let mut layers: Vec<Vec<TaskIndex>> = Vec::with_capacity(num_layers);

    let mut layer_0 = Vec::with_capacity(width);
    for i in 0..width {
        let idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: "pkg".into(),
                tdl_function: format!("L0_{i}"),
                inputs: vec![bytes_type(); fan_in],
                outputs: vec![bytes_type()],
                input_sources: None,
                execution_policy: policy.clone(),
            })
            .expect("neural net layer 0 task insertion should succeed");
        layer_0.push(idx);
    }
    layers.push(layer_0);

    let half = fan_in / 2;
    for layer_idx in 1..num_layers {
        let prev_layer = &layers[layer_idx - 1];
        let mut current_layer = Vec::with_capacity(width);

        for p in 0..width {
            let input_sources: Vec<TaskInputOutputIndex> = (0..fan_in)
                .map(|k| {
                    let src_pos = (p + width - half + k) % width;
                    TaskInputOutputIndex {
                        task_idx: prev_layer[src_pos],
                        position: 0,
                    }
                })
                .collect();

            let idx = graph
                .insert_task(TaskDescriptor {
                    tdl_package: "pkg".into(),
                    tdl_function: format!("L{layer_idx}_{p}"),
                    inputs: vec![bytes_type(); fan_in],
                    outputs: vec![bytes_type()],
                    input_sources: Some(input_sources),
                    execution_policy: policy.clone(),
                })
                .expect("neural net layer task insertion should succeed");
            current_layer.push(idx);
        }
        layers.push(current_layer);
    }

    let job_inputs: Vec<TaskInput> = (0..width * fan_in)
        .map(|_| TaskInput::ValuePayload(make_1kb_payload()))
        .collect();

    (graph, job_inputs, layers)
}

// =============================================================================
// Stage 1 tests (existing)
// =============================================================================

/// Tests the factory and end-to-end execution with a simple linear chain: A -> B -> C.
///
/// # Graph topology
///
/// ```text
///   [job_input] -> A -> B -> C -> [job_output]
/// ```
///
/// # Verifies
///
/// - `build_job` correctly identifies only A as initially ready (B and C depend on predecessors).
/// - Job inputs are pre-populated: A receives the original `b"hello"` bytes.
/// - Dataflow wiring works across the chain: A's output `b"world"` is delivered as B's input, and
///   B's output `b"done"` is delivered as C's input.
/// - The job remains in `Running` state while tasks are still incomplete.
/// - The job transitions to `Succeeded` once the final task (C) completes, since there is no commit
///   task configured.
#[tokio::test]
async fn test_factory_linear_chain() {
    let mut graph = CoreTaskGraph::default();
    let task_a = graph
        .insert_task(TaskDescriptor {
            tdl_package: "pkg".into(),
            tdl_function: "fn_a".into(),
            inputs: vec![bytes_type()],
            outputs: vec![bytes_type()],
            input_sources: None,
            execution_policy: ExecutionPolicy::default(),
        })
        .expect("task A insertion should succeed");
    let task_b = graph
        .insert_task(TaskDescriptor {
            tdl_package: "pkg".into(),
            tdl_function: "fn_b".into(),
            inputs: vec![bytes_type()],
            outputs: vec![bytes_type()],
            input_sources: Some(vec![TaskInputOutputIndex {
                task_idx: task_a,
                position: 0,
            }]),
            execution_policy: ExecutionPolicy::default(),
        })
        .expect("task B insertion should succeed");
    let task_c = graph
        .insert_task(TaskDescriptor {
            tdl_package: "pkg".into(),
            tdl_function: "fn_c".into(),
            inputs: vec![bytes_type()],
            outputs: vec![bytes_type()],
            input_sources: Some(vec![TaskInputOutputIndex {
                task_idx: task_b,
                position: 0,
            }]),
            execution_policy: ExecutionPolicy::default(),
        })
        .expect("task C insertion should succeed");

    let job_inputs = vec![TaskInput::ValuePayload(b"hello".to_vec())];

    let (jcb, ready_indices) = build_job(
        JobId::new(),
        ResourceGroupId::new(),
        &graph,
        job_inputs,
        MockReadyQueue::new(),
        MockDb::new(false),
        MockInstancePool::new(),
    )
    .expect("build_job should succeed for linear chain");

    assert_eq!(ready_indices, vec![task_a]);

    let ctx_a = jcb
        .create_task_instance(TaskId::TaskIndex(task_a))
        .await
        .expect("create instance for task A should succeed");
    let inputs_a = ctx_a.inputs.expect("task A should have inputs");
    assert_eq!(inputs_a.len(), 1);
    assert_eq!(
        inputs_a[0],
        TaskInput::ValuePayload(b"hello".to_vec()),
        "task A should receive the job input"
    );

    let state = jcb
        .complete_task_instance(ctx_a.task_instance_id, task_a, vec![b"world".to_vec()])
        .await
        .expect("complete task A should succeed");
    assert_eq!(state, JobState::Running);

    let ctx_b = jcb
        .create_task_instance(TaskId::TaskIndex(task_b))
        .await
        .expect("create instance for task B should succeed");
    let inputs_b = ctx_b.inputs.expect("task B should have inputs");
    assert_eq!(
        inputs_b[0],
        TaskInput::ValuePayload(b"world".to_vec()),
        "task B should receive task A's output"
    );

    let state = jcb
        .complete_task_instance(ctx_b.task_instance_id, task_b, vec![b"done".to_vec()])
        .await
        .expect("complete task B should succeed");
    assert_eq!(state, JobState::Running);

    let ctx_c = jcb
        .create_task_instance(TaskId::TaskIndex(task_c))
        .await
        .expect("create instance for task C should succeed");
    let inputs_c = ctx_c.inputs.expect("task C should have inputs");
    assert_eq!(inputs_c[0], TaskInput::ValuePayload(b"done".to_vec()));

    let state = jcb
        .complete_task_instance(ctx_c.task_instance_id, task_c, vec![b"final".to_vec()])
        .await
        .expect("complete task C should succeed");
    assert_eq!(
        state,
        JobState::Succeeded,
        "job should succeed after all tasks complete"
    );
}

/// Tests the factory and end-to-end execution with a diamond DAG that exercises fan-out and
/// fan-in.
///
/// # Graph topology
///
/// ```text
///                ┌─> B ─┐
///   [job_input] -> A       -> D -> [job_output]
///                └─> C ─┘
/// ```
///
/// # Verifies
///
/// - Only A is initially ready.
/// - Completing A unblocks both B and C simultaneously.
/// - D is not unblocked until *both* B and C have completed (fan-in gate).
/// - The job transitions to `Succeeded` once D completes.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_factory_diamond_dag() {
    let mut graph = CoreTaskGraph::default();
    let task_a = graph
        .insert_task(TaskDescriptor {
            tdl_package: "pkg".into(),
            tdl_function: "fn_a".into(),
            inputs: vec![bytes_type()],
            outputs: vec![bytes_type(), bytes_type()],
            input_sources: None,
            execution_policy: ExecutionPolicy::default(),
        })
        .expect("task A insertion should succeed");
    let task_b = graph
        .insert_task(TaskDescriptor {
            tdl_package: "pkg".into(),
            tdl_function: "fn_b".into(),
            inputs: vec![bytes_type()],
            outputs: vec![bytes_type()],
            input_sources: Some(vec![TaskInputOutputIndex {
                task_idx: task_a,
                position: 0,
            }]),
            execution_policy: ExecutionPolicy::default(),
        })
        .expect("task B insertion should succeed");
    let task_c = graph
        .insert_task(TaskDescriptor {
            tdl_package: "pkg".into(),
            tdl_function: "fn_c".into(),
            inputs: vec![bytes_type()],
            outputs: vec![bytes_type()],
            input_sources: Some(vec![TaskInputOutputIndex {
                task_idx: task_a,
                position: 1,
            }]),
            execution_policy: ExecutionPolicy::default(),
        })
        .expect("task C insertion should succeed");
    let task_d = graph
        .insert_task(TaskDescriptor {
            tdl_package: "pkg".into(),
            tdl_function: "fn_d".into(),
            inputs: vec![bytes_type(), bytes_type()],
            outputs: vec![bytes_type()],
            input_sources: Some(vec![
                TaskInputOutputIndex {
                    task_idx: task_b,
                    position: 0,
                },
                TaskInputOutputIndex {
                    task_idx: task_c,
                    position: 0,
                },
            ]),
            execution_policy: ExecutionPolicy::default(),
        })
        .expect("task D insertion should succeed");

    let job_inputs = vec![TaskInput::ValuePayload(b"input".to_vec())];
    let ready_queue = MockReadyQueue::new();
    let ready_tasks_ref = ready_queue.ready_tasks.clone();

    let (jcb, ready_indices) = build_job(
        JobId::new(),
        ResourceGroupId::new(),
        &graph,
        job_inputs,
        ready_queue,
        MockDb::new(false),
        MockInstancePool::new(),
    )
    .expect("build_job should succeed for diamond DAG");

    assert_eq!(ready_indices, vec![task_a]);

    let ctx_a = jcb
        .create_task_instance(TaskId::TaskIndex(task_a))
        .await
        .expect("create instance for task A should succeed");
    let state = jcb
        .complete_task_instance(
            ctx_a.task_instance_id,
            task_a,
            vec![b"out_b".to_vec(), b"out_c".to_vec()],
        )
        .await
        .expect("complete task A should succeed");
    assert_eq!(state, JobState::Running);

    let queued = ready_tasks_ref.lock().await;
    assert_eq!(queued.len(), 1);
    let (_, ref task_ids) = queued[0];
    assert!(task_ids.contains(&task_b));
    assert!(task_ids.contains(&task_c));
    drop(queued);

    let ctx_b = jcb
        .create_task_instance(TaskId::TaskIndex(task_b))
        .await
        .expect("create instance for task B should succeed");
    jcb.complete_task_instance(ctx_b.task_instance_id, task_b, vec![b"b_out".to_vec()])
        .await
        .expect("complete task B should succeed");

    let ctx_c = jcb
        .create_task_instance(TaskId::TaskIndex(task_c))
        .await
        .expect("create instance for task C should succeed");
    jcb.complete_task_instance(ctx_c.task_instance_id, task_c, vec![b"c_out".to_vec()])
        .await
        .expect("complete task C should succeed");

    let ctx_d = jcb
        .create_task_instance(TaskId::TaskIndex(task_d))
        .await
        .expect("create instance for task D should succeed");
    let state = jcb
        .complete_task_instance(ctx_d.task_instance_id, task_d, vec![b"final".to_vec()])
        .await
        .expect("complete task D should succeed");
    assert_eq!(state, JobState::Succeeded);
}

/// Tests the commit task lifecycle: job transitions through `CommitReady` before `Succeeded`.
///
/// # Graph topology
///
/// ```text
///   A -> [job_output]
///   (commit task: commit_fn)
/// ```
///
/// # Verifies
///
/// - After A completes, the job transitions to `CommitReady`.
/// - The commit task can be registered and completed, transitioning to `Succeeded`.
#[tokio::test]
async fn test_factory_with_commit_task() {
    let mut graph = CoreTaskGraph::default();
    let task_a = graph
        .insert_task(TaskDescriptor {
            tdl_package: "pkg".into(),
            tdl_function: "fn_a".into(),
            inputs: vec![],
            outputs: vec![bytes_type()],
            input_sources: None,
            execution_policy: ExecutionPolicy::default(),
        })
        .expect("task A insertion should succeed");

    graph.set_commit_task(TerminationTaskDescriptor {
        tdl_package: "pkg".into(),
        tdl_function: "commit_fn".into(),
        execution_policy: ExecutionPolicy::default(),
    });

    let ready_queue = MockReadyQueue::new();
    let commit_count = ready_queue.commit_ready_count.clone();

    let (jcb, ready_indices) = build_job(
        JobId::new(),
        ResourceGroupId::new(),
        &graph,
        vec![],
        ready_queue,
        MockDb::new(true),
        MockInstancePool::new(),
    )
    .expect("build_job should succeed for commit task test");

    assert_eq!(ready_indices, vec![task_a]);

    let ctx_a = jcb
        .create_task_instance(TaskId::TaskIndex(task_a))
        .await
        .expect("create instance for task A should succeed");
    let state = jcb
        .complete_task_instance(ctx_a.task_instance_id, task_a, vec![b"output".to_vec()])
        .await
        .expect("complete task A should succeed");
    assert_eq!(state, JobState::CommitReady);
    assert_eq!(commit_count.load(Ordering::Relaxed), 1);

    let ctx_commit = jcb
        .create_task_instance(TaskId::Commit)
        .await
        .expect("create commit instance should succeed");
    assert!(ctx_commit.inputs.is_none());
    let state = jcb
        .complete_commit_task_instance(ctx_commit.task_instance_id)
        .await
        .expect("complete commit task should succeed");
    assert_eq!(state, JobState::Succeeded);
}

// =============================================================================
// Stage 2 tests: scheduler smoke tests
// =============================================================================

/// Smoke test: validates the test scheduler with a small independent graph (10 tasks, 2 workers).
///
/// # Purpose
///
/// Ensures the `run_scheduled_test` infrastructure works correctly before scaling to 10k tasks.
///
/// # Verifies
///
/// - All 10 tasks complete.
/// - Final job state is `Succeeded`.
/// - No ready-queue propagation (no dependencies).
#[tokio::test]
async fn test_scheduler_smoke_independent() {
    let (graph, job_inputs) = build_flat_graph(10, 1, 1);
    let handler: Arc<dyn TaskHandler> = Arc::new(ImmediateCompletionHandler {
        num_outputs_per_task: 1,
    });
    let (result, _) = run_scheduled_test(&graph, job_inputs, 2, handler).await;

    assert_eq!(result.tasks_dispatched, 10, "all 10 tasks should complete");
    assert_eq!(result.final_state, JobState::Succeeded);
    assert_eq!(result.ready_queue_call_count, 0);
}

/// Smoke test: validates the test scheduler with a small layered graph (3 layers × 5 tasks,
/// fan-in=2, 2 workers).
///
/// # Verifies
///
/// - All 15 tasks complete.
/// - Final job state is `Succeeded`.
/// - Tasks in layers 1 and 2 (10 total) are reported ready via the ready queue.
#[tokio::test]
async fn test_scheduler_smoke_layered() {
    let (graph, job_inputs, _layers) = build_neural_net_graph(3, 5, 2);
    assert_eq!(graph.get_num_tasks(), 15);

    let handler: Arc<dyn TaskHandler> = Arc::new(ImmediateCompletionHandler {
        num_outputs_per_task: 1,
    });
    let (result, _) = run_scheduled_test(&graph, job_inputs, 2, handler).await;

    assert_eq!(result.tasks_dispatched, 15, "all 15 tasks should complete");
    assert_eq!(result.final_state, JobState::Succeeded);
    assert_eq!(
        result.total_tasks_reported_ready, 10,
        "layers 1 and 2 (5+5 tasks) should be reported ready"
    );
}

/// Smoke test: validates multi-instance execution with a small graph (5 tasks, 2 workers,
/// 3 instances per task where 2 succeed and 1 fails).
///
/// # Purpose
///
/// Ensures the multi-instance path handles concurrent instance registration, completion, and
/// rejection errors (e.g. `TaskAlreadyTerminated`) gracefully before scaling up.
///
/// # Verifies
///
/// - All 5 tasks complete despite 1 out of 3 instances failing per task.
/// - Final job state is `Succeeded`.
#[tokio::test]
async fn test_scheduler_smoke_multi_instance() {
    let policy = ExecutionPolicy {
        max_num_instances: 3,
        max_num_retries: 2,
    };
    let (graph, job_inputs) = build_flat_graph_with_policy(5, 1, 1, policy);
    let handler: Arc<dyn TaskHandler> = Arc::new(MultiInstancePartialFailHandler {
        num_outputs_per_task: 1,
        num_instances: 3,
    });
    let (result, _) = run_scheduled_test(&graph, job_inputs, 2, handler).await;

    assert_eq!(result.tasks_dispatched, 5, "all 5 tasks should complete");
    assert_eq!(result.final_state, JobState::Succeeded);
}

// =============================================================================
// Stage 2 tests: large-scale performance
// =============================================================================

/// Large-scale performance baseline: 10,000 independent tasks with zero dependencies.
///
/// # Purpose
///
/// Establishes a baseline for cache-layer throughput with no dependency overhead.
///
/// # Graph topology
///
/// ```text
///   [20,000 job_inputs (1KB each)]
///       T_0    (2 in, 1 out) -> [job_output_0]
///       ...
///       T_9999 (2 in, 1 out) -> [job_output_9999]
/// ```
///
/// # Metrics captured
///
/// Graph construction, `build_job`, total execution (128 workers), per-task latency (avg/p50/p95/p99).
///
/// # How to interpret results
///
/// Compare per-task latency against the neural-network test. The difference isolates
/// dependency-tracking overhead.
///
/// Run with `cargo test test_scale_10k_independent -- --nocapture` to see timing output.
#[tokio::test]
async fn test_scale_10k_independent() {
    const NUM_TASKS: usize = 10_000;
    const NUM_WORKERS: usize = 128;

    let graph_start = Instant::now();
    let (graph, job_inputs) = build_flat_graph(NUM_TASKS, 2, 1);
    let graph_time = graph_start.elapsed();

    let handler: Arc<dyn TaskHandler> = Arc::new(ImmediateCompletionHandler {
        num_outputs_per_task: 1,
    });
    let (result, build_job_time) =
        run_scheduled_test(&graph, job_inputs, NUM_WORKERS, handler).await;

    assert_eq!(
        result.tasks_dispatched,
        NUM_TASKS,
        "all tasks should have completed"
    );
    assert_eq!(result.final_state, JobState::Succeeded);
    assert_eq!(
        result.ready_queue_call_count, 0,
        "no ready-queue propagation for independent tasks"
    );

    result.report("10k Independent Tasks", NUM_WORKERS, graph_time, build_job_time);
}

/// Large-scale dependency test: 10,000 tasks in a 10-layer × 1000-wide neural-network topology.
///
/// # Purpose
///
/// Measures dependency-tracking overhead with fan-in=10, fan-out=10.
///
/// # Graph topology
///
/// ```text
///   Layer 0: T_0..T_999       [10 graph inputs, 1KB] -> 1 output
///   ...
///   Layer 9: T_9000..T_9999   [10 inputs from layer 8] -> [job outputs]
///   Circular connectivity: (p-5)%1000 .. (p+4)%1000.
/// ```
///
/// # How to interpret results
///
/// Compare against 10k independent test. Difference = dependency overhead.
///
/// Run with `cargo test test_scale_10k_neural_net -- --nocapture` to see timing output.
#[tokio::test]
async fn test_scale_10k_neural_net() {
    const NUM_LAYERS: usize = 10;
    const WIDTH: usize = 1000;
    const FAN_IN: usize = 10;
    const NUM_WORKERS: usize = 128;

    let graph_start = Instant::now();
    let (graph, job_inputs, _layers) = build_neural_net_graph(NUM_LAYERS, WIDTH, FAN_IN);
    let graph_time = graph_start.elapsed();

    assert_eq!(graph.get_num_tasks(), NUM_LAYERS * WIDTH);

    let handler: Arc<dyn TaskHandler> = Arc::new(ImmediateCompletionHandler {
        num_outputs_per_task: 1,
    });
    let (result, build_job_time) =
        run_scheduled_test(&graph, job_inputs, NUM_WORKERS, handler).await;

    assert_eq!(
        result.tasks_dispatched,
        NUM_LAYERS * WIDTH,
        "all tasks should have completed"
    );
    assert_eq!(result.final_state, JobState::Succeeded);
    assert_eq!(
        result.total_tasks_reported_ready,
        (NUM_LAYERS - 1) * WIDTH,
        "9 layers × 1000 tasks should be reported ready"
    );

    result.report(
        "10k Neural Net (10x1000, fan=10)",
        NUM_WORKERS,
        graph_time,
        build_job_time,
    );
}

/// Large-scale multi-instance test: 10,000 independent tasks, 3 instances per task (2 succeed,
/// 1 fails), 128 workers.
///
/// # Purpose
///
/// Measures the overhead of concurrent multi-instance registration and the rejection-error path
/// when instances race to complete/fail the same task.
///
/// # How to interpret results
///
/// Compare against the single-instance 10k independent test. The difference shows the cost of
/// instance contention, registration overhead, and rejection handling.
///
/// Run with `cargo test test_scale_10k_multi_instance -- --nocapture` to see timing output.
#[tokio::test]
async fn test_scale_10k_multi_instance() {
    const NUM_TASKS: usize = 10_000;
    const NUM_WORKERS: usize = 128;

    let policy = ExecutionPolicy {
        max_num_instances: 3,
        max_num_retries: 2,
    };
    let graph_start = Instant::now();
    let (graph, job_inputs) = build_flat_graph_with_policy(NUM_TASKS, 2, 1, policy);
    let graph_time = graph_start.elapsed();

    let handler: Arc<dyn TaskHandler> = Arc::new(MultiInstancePartialFailHandler {
        num_outputs_per_task: 1,
        num_instances: 3,
    });
    let (result, build_job_time) =
        run_scheduled_test(&graph, job_inputs, NUM_WORKERS, handler).await;

    assert_eq!(
        result.tasks_dispatched,
        NUM_TASKS,
        "all tasks should have completed"
    );
    assert_eq!(result.final_state, JobState::Succeeded);

    result.report(
        "10k Independent (3 instances, 2 success + 1 fail)",
        NUM_WORKERS,
        graph_time,
        build_job_time,
    );
}

/// Large-scale multi-instance dependency test: 10,000 tasks in a 10-layer × 1000-wide
/// neural-network topology, 3 instances per task (2 succeed, 1 fails), 128 workers.
///
/// # Purpose
///
/// Combines dependency-tracking overhead with multi-instance contention. Each task completion
/// acquires locks on 10 child TCBs while also racing against sibling instances that may
/// complete or fail concurrently.
///
/// # Graph topology
///
/// Same as [`test_scale_10k_neural_net`] (10 layers × 1000, fan-in=10, circular connectivity),
/// but with `max_num_instances=3` and `max_num_retries=2`.
///
/// # How to interpret results
///
/// Compare against the single-instance neural-net test to isolate multi-instance overhead in a
/// dependency-heavy graph.
///
/// Run with `cargo test test_scale_10k_neural_net_multi_instance -- --nocapture` to see timing.
#[tokio::test]
async fn test_scale_10k_neural_net_multi_instance() {
    const NUM_LAYERS: usize = 10;
    const WIDTH: usize = 1000;
    const FAN_IN: usize = 10;
    const NUM_WORKERS: usize = 128;

    let policy = ExecutionPolicy {
        max_num_instances: 3,
        max_num_retries: 2,
    };
    let graph_start = Instant::now();
    let (graph, job_inputs, _layers) =
        build_neural_net_graph_with_policy(NUM_LAYERS, WIDTH, FAN_IN, policy);
    let graph_time = graph_start.elapsed();

    assert_eq!(graph.get_num_tasks(), NUM_LAYERS * WIDTH);

    let handler: Arc<dyn TaskHandler> = Arc::new(MultiInstancePartialFailHandler {
        num_outputs_per_task: 1,
        num_instances: 3,
    });
    let (result, build_job_time) =
        run_scheduled_test(&graph, job_inputs, NUM_WORKERS, handler).await;

    assert_eq!(
        result.tasks_dispatched,
        NUM_LAYERS * WIDTH,
        "all tasks should have completed"
    );
    assert_eq!(result.final_state, JobState::Succeeded);
    assert_eq!(
        result.total_tasks_reported_ready,
        (NUM_LAYERS - 1) * WIDTH,
        "9 layers x 1000 tasks should be reported ready"
    );

    result.report(
        "10k Neural Net (10x1000, fan=10, 3 instances, 2 success + 1 fail)",
        NUM_WORKERS,
        graph_time,
        build_job_time,
    );
}

/// Tests that a job fails when all task instances always fail and retries are exhausted.
///
/// # Purpose
///
/// Validates the retry-exhaustion → job failure path. Every instance of every task fails,
/// consuming all retries. The first task to exhaust its retries causes the entire job to fail.
///
/// # Graph topology
///
/// ```text
///   T_0..T_9 (1 input, 1 output, all independent)
///   ExecutionPolicy: max_num_instances=1, max_num_retries=2
/// ```
///
/// # Verifies
///
/// - The job reaches `Failed` state (not `Succeeded`).
/// - Not all tasks complete (the job fails early once one task exhausts retries).
#[tokio::test]
async fn test_always_fail_exhausts_retries() {
    const NUM_TASKS: usize = 10;
    const NUM_WORKERS: usize = 4;

    let policy = ExecutionPolicy {
        max_num_instances: 1,
        max_num_retries: 2,
    };
    let (graph, job_inputs) = build_flat_graph_with_policy(NUM_TASKS, 1, 1, policy);

    let handler: Arc<dyn TaskHandler> = Arc::new(AlwaysFailHandler);
    let (result, _) = run_scheduled_test(&graph, job_inputs, NUM_WORKERS, handler).await;

    assert_eq!(
        result.final_state,
        JobState::Failed,
        "job should fail when all instances always fail and retries are exhausted"
    );
}

// =============================================================================
// Stage 2 tests: ready-queue correctness
// =============================================================================

/// Correctness test for the neural-network ready-queue propagation.
///
/// # Purpose
///
/// Verifies that the ready queue receives exactly the right task indices after each layer
/// completes. Uses sequential execution for determinism.
///
/// # Verifies
///
/// - After completing layer L, exactly the 1000 tasks in layer L+1 are reported ready.
/// - No duplicates.
/// - The final layer triggers no further ready notifications.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_neural_net_ready_queue_correctness() {
    const NUM_LAYERS: usize = 10;
    const WIDTH: usize = 1000;
    const FAN_IN: usize = 10;

    let (graph, job_inputs, layers) = build_neural_net_graph(NUM_LAYERS, WIDTH, FAN_IN);

    let ready_queue = MockReadyQueue::new();
    let ready_tasks_ref = ready_queue.ready_tasks.clone();

    let (jcb, initial_ready) = build_job(
        JobId::new(),
        ResourceGroupId::new(),
        &graph,
        job_inputs,
        ready_queue,
        MockDb::new(false),
        MockInstancePool::new(),
    )
    .expect("build_job should succeed for correctness test");

    let initial_set: HashSet<TaskIndex> = initial_ready.into_iter().collect();
    let expected_layer_0: HashSet<TaskIndex> = layers[0].iter().copied().collect();
    assert_eq!(initial_set, expected_layer_0, "only layer 0 should be initially ready");

    for (layer_idx, layer) in layers.iter().enumerate() {
        ready_tasks_ref.lock().await.clear();

        for &task_idx in layer {
            let ctx = jcb
                .create_task_instance(TaskId::TaskIndex(task_idx))
                .await
                .expect("create instance should succeed in correctness test");
            jcb.complete_task_instance(ctx.task_instance_id, task_idx, vec![make_1kb_payload()])
                .await
                .expect("complete task should succeed in correctness test");
        }

        if layer_idx < NUM_LAYERS - 1 {
            let snapshot = ready_tasks_ref.lock().await;
            let mut reported_ready: Vec<TaskIndex> = snapshot
                .iter()
                .flat_map(|(_, ids)| ids.iter().copied())
                .collect();
            drop(snapshot);

            let unique: HashSet<TaskIndex> = reported_ready.iter().copied().collect();
            assert_eq!(
                unique.len(),
                reported_ready.len(),
                "layer {layer_idx}: no task should be reported ready more than once"
            );

            reported_ready.sort_unstable();
            let mut expected: Vec<TaskIndex> = layers[layer_idx + 1].clone();
            expected.sort_unstable();
            assert_eq!(
                reported_ready, expected,
                "layer {layer_idx}: reported ready tasks should match layer {}",
                layer_idx + 1
            );
        } else {
            let snapshot = ready_tasks_ref.lock().await;
            let reported_count: usize = snapshot.iter().map(|(_, ids)| ids.len()).sum();
            assert_eq!(
                reported_count, 0,
                "last layer should not trigger any ready notifications"
            );
        }
    }
}
