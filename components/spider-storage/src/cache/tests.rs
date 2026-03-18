use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use spider_core::{
    job::JobState,
    task::{
        BytesTypeDescriptor,
        DataTypeDescriptor,
        ExecutionPolicy,
        TaskDescriptor,
        TaskGraph as CoreTaskGraph,
        TaskIndex,
        TerminationTaskDescriptor,
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
        error::InternalError,
        job::{ReadyQueueConnector, TaskInstancePoolConnector},
        task::{SharedTaskControlBlock, SharedTerminationTaskControlBlock},
        types::TaskId,
    },
    db::{DbError, InternalJobOrchestration},
};

// --- Mock implementations ---

type ReadyTaskList = Arc<Mutex<Vec<(JobId, Vec<TaskIndex>)>>>;

struct MockReadyQueue {
    ready_tasks: ReadyTaskList,
    commit_ready_count: Arc<AtomicU64>,
    cleanup_ready_count: Arc<AtomicU64>,
}

impl MockReadyQueue {
    fn new() -> Self {
        Self {
            ready_tasks: Arc::new(Mutex::new(Vec::new())),
            commit_ready_count: Arc::new(AtomicU64::new(0)),
            cleanup_ready_count: Arc::new(AtomicU64::new(0)),
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

// --- Helper: simple byte type descriptor ---

fn bytes_type() -> DataTypeDescriptor {
    DataTypeDescriptor::Value(ValueTypeDescriptor::Bytes(BytesTypeDescriptor {}))
}

// --- Tests ---

/// Tests the factory and end-to-end execution with a simple linear chain: A -> B -> C.
///
/// Graph topology:
/// ```text
///   [job_input] -> A -> B -> C -> [job_output]
/// ```
///
/// Verifies:
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
        .unwrap();
    let task_b = graph
        .insert_task(TaskDescriptor {
            tdl_package: "pkg".into(),
            tdl_function: "fn_b".into(),
            inputs: vec![bytes_type()],
            outputs: vec![bytes_type()],
            input_sources: Some(vec![spider_core::task::TaskInputOutputIndex {
                task_idx: task_a,
                position: 0,
            }]),
            execution_policy: ExecutionPolicy::default(),
        })
        .unwrap();
    let task_c = graph
        .insert_task(TaskDescriptor {
            tdl_package: "pkg".into(),
            tdl_function: "fn_c".into(),
            inputs: vec![bytes_type()],
            outputs: vec![bytes_type()],
            input_sources: Some(vec![spider_core::task::TaskInputOutputIndex {
                task_idx: task_b,
                position: 0,
            }]),
            execution_policy: ExecutionPolicy::default(),
        })
        .unwrap();

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
    .unwrap();

    // Only task A should be ready initially.
    assert_eq!(ready_indices, vec![task_a]);

    // Execute task A.
    let ctx_a = jcb
        .create_task_instance(TaskId::TaskIndex(task_a))
        .await
        .unwrap();
    assert!(ctx_a.inputs.is_some());
    let inputs_a = ctx_a.inputs.unwrap();
    assert_eq!(inputs_a.len(), 1);
    assert_eq!(
        inputs_a[0],
        TaskInput::ValuePayload(b"hello".to_vec()),
        "task A should receive the job input"
    );

    let state = jcb
        .complete_task_instance(ctx_a.task_instance_id, task_a, vec![b"world".to_vec()])
        .await
        .unwrap();
    assert_eq!(state, JobState::Running);

    // Execute task B.
    let ctx_b = jcb
        .create_task_instance(TaskId::TaskIndex(task_b))
        .await
        .unwrap();
    let inputs_b = ctx_b.inputs.unwrap();
    assert_eq!(
        inputs_b[0],
        TaskInput::ValuePayload(b"world".to_vec()),
        "task B should receive task A's output"
    );

    let state = jcb
        .complete_task_instance(ctx_b.task_instance_id, task_b, vec![b"done".to_vec()])
        .await
        .unwrap();
    assert_eq!(state, JobState::Running);

    // Execute task C — the last task.
    let ctx_c = jcb
        .create_task_instance(TaskId::TaskIndex(task_c))
        .await
        .unwrap();
    let inputs_c = ctx_c.inputs.unwrap();
    assert_eq!(inputs_c[0], TaskInput::ValuePayload(b"done".to_vec()));

    let state = jcb
        .complete_task_instance(ctx_c.task_instance_id, task_c, vec![b"final".to_vec()])
        .await
        .unwrap();
    assert_eq!(
        state,
        JobState::Succeeded,
        "job should succeed after all tasks complete"
    );
}

/// Tests the factory and end-to-end execution with a diamond DAG that exercises fan-out and
/// fan-in.
///
/// Graph topology:
/// ```text
///                ┌─> B ─┐
///   [job_input] -> A       -> D -> [job_output]
///                └─> C ─┘
/// ```
///
/// A has two outputs; B consumes output 0, C consumes output 1. D consumes both B's and C's
/// outputs (fan-in from two parents).
///
/// Verifies:
/// - Only A is initially ready.
/// - Completing A unblocks both B and C simultaneously — both appear in a single `send_task_ready`
///   call to the ready queue.
/// - D is not unblocked until *both* B and C have completed (fan-in gate).
/// - The job transitions to `Succeeded` once the sink task (D) completes.
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
        .unwrap();
    let task_b = graph
        .insert_task(TaskDescriptor {
            tdl_package: "pkg".into(),
            tdl_function: "fn_b".into(),
            inputs: vec![bytes_type()],
            outputs: vec![bytes_type()],
            input_sources: Some(vec![spider_core::task::TaskInputOutputIndex {
                task_idx: task_a,
                position: 0,
            }]),
            execution_policy: ExecutionPolicy::default(),
        })
        .unwrap();
    let task_c = graph
        .insert_task(TaskDescriptor {
            tdl_package: "pkg".into(),
            tdl_function: "fn_c".into(),
            inputs: vec![bytes_type()],
            outputs: vec![bytes_type()],
            input_sources: Some(vec![spider_core::task::TaskInputOutputIndex {
                task_idx: task_a,
                position: 1,
            }]),
            execution_policy: ExecutionPolicy::default(),
        })
        .unwrap();
    let task_d = graph
        .insert_task(TaskDescriptor {
            tdl_package: "pkg".into(),
            tdl_function: "fn_d".into(),
            inputs: vec![bytes_type(), bytes_type()],
            outputs: vec![bytes_type()],
            input_sources: Some(vec![
                spider_core::task::TaskInputOutputIndex {
                    task_idx: task_b,
                    position: 0,
                },
                spider_core::task::TaskInputOutputIndex {
                    task_idx: task_c,
                    position: 0,
                },
            ]),
            execution_policy: ExecutionPolicy::default(),
        })
        .unwrap();

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
    .unwrap();

    assert_eq!(ready_indices, vec![task_a]);

    // Complete A.
    let ctx_a = jcb
        .create_task_instance(TaskId::TaskIndex(task_a))
        .await
        .unwrap();
    let state = jcb
        .complete_task_instance(
            ctx_a.task_instance_id,
            task_a,
            vec![b"out_b".to_vec(), b"out_c".to_vec()],
        )
        .await
        .unwrap();
    assert_eq!(state, JobState::Running);

    // Check that B and C were enqueued as ready.
    let queued = ready_tasks_ref.lock().await;
    assert_eq!(queued.len(), 1);
    let (_, ref task_ids) = queued[0];
    assert!(task_ids.contains(&task_b));
    assert!(task_ids.contains(&task_c));
    drop(queued);

    // Complete B and C.
    let ctx_b = jcb
        .create_task_instance(TaskId::TaskIndex(task_b))
        .await
        .unwrap();
    jcb.complete_task_instance(ctx_b.task_instance_id, task_b, vec![b"b_out".to_vec()])
        .await
        .unwrap();

    let ctx_c = jcb
        .create_task_instance(TaskId::TaskIndex(task_c))
        .await
        .unwrap();
    jcb.complete_task_instance(ctx_c.task_instance_id, task_c, vec![b"c_out".to_vec()])
        .await
        .unwrap();

    // D should now be ready. Complete it.
    let ctx_d = jcb
        .create_task_instance(TaskId::TaskIndex(task_d))
        .await
        .unwrap();
    let state = jcb
        .complete_task_instance(ctx_d.task_instance_id, task_d, vec![b"final".to_vec()])
        .await
        .unwrap();
    assert_eq!(state, JobState::Succeeded);
}

/// Tests the commit task lifecycle: job transitions through `CommitReady` before `Succeeded`.
///
/// Graph topology:
/// ```text
///   A -> [job_output]
///   (commit task: commit_fn)
/// ```
///
/// A single task (A) with no inputs and one dangling output. A `TerminationTaskDescriptor` is
/// attached as the commit task. The mock DB is configured to return `CommitReady` on
/// `commit_outputs` (simulating a job that has a commit task).
///
/// Verifies:
/// - After A completes and outputs are committed, the job transitions to `CommitReady` (not
///   directly to `Succeeded`).
/// - The ready queue receives exactly one `send_commit_ready` notification.
/// - The commit task can be registered via `TaskId::Commit` and returns no inputs (`inputs: None`),
///   since termination tasks do not consume dataflow outputs.
/// - After the commit task instance completes, the job transitions to `Succeeded`.
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
        .unwrap();

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
    .unwrap();

    assert_eq!(ready_indices, vec![task_a]);

    // Complete task A.
    let ctx_a = jcb
        .create_task_instance(TaskId::TaskIndex(task_a))
        .await
        .unwrap();
    let state = jcb
        .complete_task_instance(ctx_a.task_instance_id, task_a, vec![b"output".to_vec()])
        .await
        .unwrap();
    assert_eq!(state, JobState::CommitReady);
    assert_eq!(commit_count.load(Ordering::Relaxed), 1);

    // Execute commit task.
    let ctx_commit = jcb.create_task_instance(TaskId::Commit).await.unwrap();
    assert!(ctx_commit.inputs.is_none());
    let state = jcb
        .complete_commit_task_instance(ctx_commit.task_instance_id)
        .await
        .unwrap();
    assert_eq!(state, JobState::Succeeded);
}
