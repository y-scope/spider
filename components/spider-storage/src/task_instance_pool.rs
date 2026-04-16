use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use spider_core::{
    task::TimeoutPolicy,
    types::id::{JobId, TaskInstanceId, WorkerId},
};

use crate::{
    cache::{
        TaskId,
        error::InternalError,
        task::{SharedTaskControlBlock, SharedTerminationTaskControlBlock},
    },
    ready_queue::ReadyQueueSender,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskInstanceRecord {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub task_instance_id: TaskInstanceId,
    pub worker_id: WorkerId,
    pub registered_at: SystemTime,
    pub timeout_policy: TimeoutPolicy,
}

#[allow(dead_code)]
#[derive(Clone)]
enum TaskControlBlockHandle {
    Regular(SharedTaskControlBlock),
    Termination(SharedTerminationTaskControlBlock),
}

#[allow(dead_code)]
#[derive(Clone)]
struct RunningTaskInstanceEntry {
    record: TaskInstanceRecord,
    task_control_block: TaskControlBlockHandle,
}

struct TaskInstancePoolState {
    running_task_instances: HashMap<TaskInstanceId, RunningTaskInstanceEntry>,
    worker_task_instances: HashMap<WorkerId, HashSet<TaskInstanceId>>,
    previous_gc_cycle_start: SystemTime,
}

impl TaskInstancePoolState {
    fn new(previous_gc_cycle_start: SystemTime) -> Self {
        Self {
            running_task_instances: HashMap::new(),
            worker_task_instances: HashMap::new(),
            previous_gc_cycle_start,
        }
    }
}

#[derive(Clone)]
pub struct TaskInstancePool<ReadyQueueSenderType: ReadyQueueSender> {
    ready_queue_sender: ReadyQueueSenderType,
    next_task_instance_id: Arc<AtomicU64>,
    state: Arc<Mutex<TaskInstancePoolState>>,
}

impl<ReadyQueueSenderType: ReadyQueueSender> TaskInstancePool<ReadyQueueSenderType> {
    #[must_use]
    pub fn new(ready_queue_sender: ReadyQueueSenderType) -> Self {
        Self::new_with_gc_window_start(ready_queue_sender, SystemTime::now())
    }

    fn new_with_gc_window_start(
        ready_queue_sender: ReadyQueueSenderType,
        previous_gc_cycle_start: SystemTime,
    ) -> Self {
        Self {
            ready_queue_sender,
            next_task_instance_id: Arc::new(AtomicU64::new(1)),
            state: Arc::new(Mutex::new(TaskInstancePoolState::new(
                previous_gc_cycle_start,
            ))),
        }
    }

    pub async fn run_gc_cycle(&self) -> Result<(), InternalError> {
        self.run_gc_cycle_at(SystemTime::now()).await
    }

    pub(crate) async fn run_gc_cycle_at(
        &self,
        current_gc_cycle_start: SystemTime,
    ) -> Result<(), InternalError> {
        let timed_out_records = {
            let mut state = self
                .state
                .lock()
                .expect("task instance pool mutex should not be poisoned");
            let previous_gc_cycle_start = state.previous_gc_cycle_start;
            state.previous_gc_cycle_start = current_gc_cycle_start;

            state
                .running_task_instances
                .values()
                .filter_map(|entry| {
                    let soft_timeout_deadline =
                        entry
                            .record
                            .registered_at
                            .checked_add(Duration::from_millis(
                                entry.record.timeout_policy.soft_timeout_ms,
                            ))?;
                    if previous_gc_cycle_start < soft_timeout_deadline
                        && soft_timeout_deadline <= current_gc_cycle_start
                    {
                        return Some(entry.record.clone());
                    }
                    None
                })
                .collect::<Vec<_>>()
        };

        for record in timed_out_records {
            self.reenqueue_task(record).await?;
        }
        Ok(())
    }

    async fn register_running_task_instance(
        &self,
        task_control_block: TaskControlBlockHandle,
        record: TaskInstanceRecord,
    ) -> Result<(), InternalError> {
        let mut state = self
            .state
            .lock()
            .expect("task instance pool mutex should not be poisoned");
        let task_instance_id = record.task_instance_id;
        if state.running_task_instances.contains_key(&task_instance_id) {
            return Err(InternalError::TaskInstancePoolCorrupted(format!(
                "task instance {task_instance_id} already registered"
            )));
        }
        state.running_task_instances.insert(
            task_instance_id,
            RunningTaskInstanceEntry {
                record: record.clone(),
                task_control_block,
            },
        );
        state
            .worker_task_instances
            .entry(record.worker_id)
            .or_default()
            .insert(task_instance_id);
        Ok(())
    }

    fn unregister_running_task_instance_inner(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Option<RunningTaskInstanceEntry> {
        let mut state = self
            .state
            .lock()
            .expect("task instance pool mutex should not be poisoned");
        let entry = state.running_task_instances.remove(&task_instance_id)?;
        if let Some(worker_task_instances) =
            state.worker_task_instances.get_mut(&entry.record.worker_id)
        {
            worker_task_instances.remove(&task_instance_id);
            if worker_task_instances.is_empty() {
                state.worker_task_instances.remove(&entry.record.worker_id);
            }
        }
        Some(entry)
    }

    async fn reenqueue_task(&self, record: TaskInstanceRecord) -> Result<(), InternalError> {
        match record.task_id {
            TaskId::Index(task_index) => {
                self.ready_queue_sender
                    .send_task_ready(record.job_id, vec![task_index])
                    .await
            }
            TaskId::Commit => {
                self.ready_queue_sender
                    .send_commit_ready(record.job_id)
                    .await
            }
            TaskId::Cleanup => {
                self.ready_queue_sender
                    .send_cleanup_ready(record.job_id)
                    .await
            }
        }
    }
}

/// Connector for creating and registering task instances in the task instance pool.
///
/// This trait is invoked by the cache layer to allocate task instance IDs and register newly
/// created task instances.
#[async_trait]
pub trait TaskInstancePoolConnector: Clone + Send + Sync {
    /// Allocates a new task instance ID.
    ///
    /// Implementations must guarantee that each returned ID is globally unique across all
    /// invocations.
    ///
    /// # Returns
    ///
    /// A unique task instance ID.
    fn get_next_available_task_instance_id(&self) -> TaskInstanceId;

    /// Registers a task instance with the given task control block (TCB).
    ///
    /// # Parameters
    ///
    /// * `tcb` - The task control block associated with the task instance.
    /// * `registration` - The running-record metadata associated with the task instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the task instance cannot be registered in the pool.
    async fn register_task_instance(
        &self,
        tcb: SharedTaskControlBlock,
        registration: TaskInstanceRecord,
    ) -> Result<(), InternalError>;

    /// Registers a termination task instance with the given termination task control block.
    ///
    /// # Parameters
    ///
    /// * `termination_tcb` - The termination task control block associated with the task instance.
    /// * `registration` - The running-record metadata associated with the task instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the task instance cannot be registered in the pool.
    async fn register_termination_task_instance(
        &self,
        termination_tcb: SharedTerminationTaskControlBlock,
        registration: TaskInstanceRecord,
    ) -> Result<(), InternalError>;

    /// Unregisters a running task instance.
    ///
    /// Missing task-instance IDs must be treated as a no-op to keep completion paths race-safe.
    async fn unregister_running_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<(), InternalError>;

    /// Removes and returns all live task instances associated with the given worker.
    async fn drain_worker_task_instances(
        &self,
        worker_id: WorkerId,
    ) -> Result<Vec<TaskInstanceRecord>, InternalError>;
}

#[async_trait]
impl<ReadyQueueSenderType: ReadyQueueSender> TaskInstancePoolConnector
    for TaskInstancePool<ReadyQueueSenderType>
{
    fn get_next_available_task_instance_id(&self) -> TaskInstanceId {
        self.next_task_instance_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn register_task_instance(
        &self,
        tcb: SharedTaskControlBlock,
        registration: TaskInstanceRecord,
    ) -> Result<(), InternalError> {
        self.register_running_task_instance(TaskControlBlockHandle::Regular(tcb), registration)
            .await
    }

    async fn register_termination_task_instance(
        &self,
        termination_tcb: SharedTerminationTaskControlBlock,
        registration: TaskInstanceRecord,
    ) -> Result<(), InternalError> {
        self.register_running_task_instance(
            TaskControlBlockHandle::Termination(termination_tcb),
            registration,
        )
        .await
    }

    async fn unregister_running_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<(), InternalError> {
        let _ = self.unregister_running_task_instance_inner(task_instance_id);
        Ok(())
    }

    async fn drain_worker_task_instances(
        &self,
        worker_id: WorkerId,
    ) -> Result<Vec<TaskInstanceRecord>, InternalError> {
        let task_instance_ids = {
            let mut state = self
                .state
                .lock()
                .expect("task instance pool mutex should not be poisoned");
            state
                .worker_task_instances
                .remove(&worker_id)
                .unwrap_or_default()
        };

        let mut records = Vec::with_capacity(task_instance_ids.len());
        for task_instance_id in task_instance_ids {
            if let Some(entry) = self.unregister_running_task_instance_inner(task_instance_id) {
                records.push(entry.record);
            }
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use async_trait::async_trait;
    use spider_core::{
        task::{
            DataTypeDescriptor, ExecutionPolicy, TaskDescriptor, TaskGraph as SubmittedTaskGraph,
            TdlContext, ValueTypeDescriptor,
        },
        types::{
            id::{JobId, WorkerId},
            io::TaskInput,
        },
    };

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum ReadyMessage {
        Task(JobId, usize),
        Commit(JobId),
        Cleanup(JobId),
    }

    #[derive(Clone, Default)]
    struct MockReadyQueueSender {
        sent_messages: Arc<Mutex<Vec<ReadyMessage>>>,
    }

    impl MockReadyQueueSender {
        fn take_messages(&self) -> Vec<ReadyMessage> {
            std::mem::take(
                &mut *self
                    .sent_messages
                    .lock()
                    .expect("ready queue mutex should not be poisoned"),
            )
        }
    }

    #[async_trait]
    impl ReadyQueueSender for MockReadyQueueSender {
        async fn send_task_ready(
            &self,
            job_id: JobId,
            task_indices: Vec<usize>,
        ) -> Result<(), InternalError> {
            let mut sent_messages = self
                .sent_messages
                .lock()
                .expect("ready queue mutex should not be poisoned");
            for task_index in task_indices {
                sent_messages.push(ReadyMessage::Task(job_id, task_index));
            }
            Ok(())
        }

        async fn send_commit_ready(&self, job_id: JobId) -> Result<(), InternalError> {
            self.sent_messages
                .lock()
                .expect("ready queue mutex should not be poisoned")
                .push(ReadyMessage::Commit(job_id));
            Ok(())
        }

        async fn send_cleanup_ready(&self, job_id: JobId) -> Result<(), InternalError> {
            self.sent_messages
                .lock()
                .expect("ready queue mutex should not be poisoned")
                .push(ReadyMessage::Cleanup(job_id));
            Ok(())
        }
    }

    async fn build_single_task_tcb() -> SharedTaskControlBlock {
        let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
        let mut submitted =
            SubmittedTaskGraph::new(None, None).expect("task graph creation should succeed");
        submitted
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: "test_pkg".to_owned(),
                    task_func: "test_fn".to_owned(),
                },
                execution_policy: Some(ExecutionPolicy::default()),
                inputs: vec![bytes_type.clone()],
                outputs: vec![bytes_type],
                input_sources: None,
            })
            .expect("task insertion should succeed");
        let task_graph = crate::cache::task::TaskGraph::create(
            &submitted,
            vec![TaskInput::ValuePayload(vec![0u8; 4])],
        )
        .await
        .expect("cache task graph creation should succeed");
        task_graph
            .get_task_control_block(0)
            .expect("task control block should exist")
    }

    fn make_record(
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        worker_id: WorkerId,
        registered_at: SystemTime,
    ) -> TaskInstanceRecord {
        TaskInstanceRecord {
            job_id: JobId::new(),
            task_id,
            task_instance_id,
            worker_id,
            registered_at,
            timeout_policy: TimeoutPolicy {
                soft_timeout_ms: 100,
                hard_timeout_ms: 200,
            },
        }
    }

    #[tokio::test]
    async fn register_and_unregister_update_both_indexes() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let pool = TaskInstancePool::new(ready_queue_sender);
        let tcb = build_single_task_tcb().await;
        let worker_id = WorkerId::new();
        let record = make_record(TaskId::Index(0), 1, worker_id, SystemTime::now());

        pool.register_task_instance(tcb, record.clone())
            .await
            .expect("registration should succeed");

        {
            let state = pool
                .state
                .lock()
                .expect("task instance pool mutex should not be poisoned");
            assert!(
                state
                    .running_task_instances
                    .contains_key(&record.task_instance_id)
            );
            assert_eq!(
                state.worker_task_instances.get(&worker_id),
                Some(&HashSet::from([record.task_instance_id]))
            );
        }

        pool.unregister_running_task_instance(record.task_instance_id)
            .await
            .expect("unregister should succeed");

        let state = pool
            .state
            .lock()
            .expect("task instance pool mutex should not be poisoned");
        assert!(state.running_task_instances.is_empty());
        assert!(state.worker_task_instances.is_empty());
    }

    #[tokio::test]
    async fn drain_worker_task_instances_removes_only_target_worker() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let pool = TaskInstancePool::new(ready_queue_sender);
        let tcb = build_single_task_tcb().await;
        let worker_a = WorkerId::new();
        let worker_b = WorkerId::new();
        let now = SystemTime::now();

        let record_a = make_record(TaskId::Index(0), 1, worker_a, now);
        let record_b = make_record(TaskId::Index(0), 2, worker_a, now);
        let record_c = make_record(TaskId::Index(0), 3, worker_b, now);

        pool.register_task_instance(tcb.clone(), record_a.clone())
            .await
            .expect("registration should succeed");
        pool.register_task_instance(tcb.clone(), record_b.clone())
            .await
            .expect("registration should succeed");
        pool.register_task_instance(tcb, record_c.clone())
            .await
            .expect("registration should succeed");

        let drained = pool
            .drain_worker_task_instances(worker_a)
            .await
            .expect("drain should succeed");

        assert_eq!(drained.len(), 2);
        assert!(drained.contains(&record_a));
        assert!(drained.contains(&record_b));

        let state = pool
            .state
            .lock()
            .expect("task instance pool mutex should not be poisoned");
        assert_eq!(state.running_task_instances.len(), 1);
        assert!(
            state
                .running_task_instances
                .contains_key(&record_c.task_instance_id)
        );
        assert_eq!(
            state.worker_task_instances.get(&worker_b),
            Some(&HashSet::from([record_c.task_instance_id]))
        );
    }

    #[tokio::test]
    async fn soft_timeout_reenqueues_exactly_once_without_removing_running_record() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let initial_gc_window_start = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let pool = TaskInstancePool::new_with_gc_window_start(
            ready_queue_sender.clone(),
            initial_gc_window_start,
        );
        let tcb = build_single_task_tcb().await;
        let worker_id = WorkerId::new();
        let registered_at = initial_gc_window_start + Duration::from_millis(50);
        let record = make_record(TaskId::Index(7), 1, worker_id, registered_at);

        pool.register_task_instance(tcb, record.clone())
            .await
            .expect("registration should succeed");

        pool.run_gc_cycle_at(initial_gc_window_start + Duration::from_millis(149))
            .await
            .expect("gc should succeed");
        assert!(ready_queue_sender.take_messages().is_empty());

        pool.run_gc_cycle_at(initial_gc_window_start + Duration::from_millis(150))
            .await
            .expect("gc should succeed");
        assert_eq!(
            ready_queue_sender.take_messages(),
            vec![ReadyMessage::Task(record.job_id, 7)]
        );

        {
            let state = pool
                .state
                .lock()
                .expect("task instance pool mutex should not be poisoned");
            assert!(
                state
                    .running_task_instances
                    .contains_key(&record.task_instance_id)
            );
            assert_eq!(
                state.worker_task_instances.get(&worker_id),
                Some(&HashSet::from([record.task_instance_id]))
            );
        }

        pool.run_gc_cycle_at(initial_gc_window_start + Duration::from_millis(250))
            .await
            .expect("gc should succeed");
        assert!(ready_queue_sender.take_messages().is_empty());
    }
}
