//! Task instance pool for tracking running task instances and re-enqueuing timed-out work.
//!
//! This module provides the [`TaskInstancePool`] which tracks in-flight task instances across
//! workers. It serves two purposes:
//!
//! * **Soft-timeout recovery**: When a task instance exceeds its soft timeout, the pool re-enqueues
//!   the task so a new instance can be scheduled, while the original instance remains live until it
//!   completes or is force-removed.
//! * **Dead-worker recovery**: During each GC cycle, the pool queries the [`WorkerLivenessStore`]
//!   to detect dead workers, force-removes their instances from the task control blocks, and
//!   re-enqueues the corresponding tasks.
//!
//! The pool is also responsible for draining all instances associated with a worker when the
//! scheduler needs to reclaim work.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        Mutex,
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
    db::DbError,
    ready_queue::ReadyQueueSender,
};

/// Metadata for one running task instance tracked by the task instance pool.
///
/// This record carries the information needed to re-enqueue soft-timed-out work and to remove all
/// live task instances associated with a worker during recovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskInstanceRecord {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub task_instance_id: TaskInstanceId,
    pub worker_id: WorkerId,
    pub registered_at: SystemTime,
    pub timeout_policy: TimeoutPolicy,
}

/// Store for tracking worker liveness state.
///
/// Implementations persist worker heartbeat state durably and provide an atomic operation to detect
/// and mark dead workers for recovery.
#[async_trait]
pub trait WorkerLivenessStore: Clone + Send + Sync {
    /// Returns the IDs of workers whose last heartbeat is before `stale_before`, after marking them
    /// dead.
    ///
    /// This operation is atomic: once a worker is returned by this method, it will not be returned
    /// again in subsequent calls.
    ///
    /// # Returns
    ///
    /// A vector of dead worker IDs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards the underlying store's return values on failure.
    async fn get_dead_workers(&self, stale_before: SystemTime) -> Result<Vec<WorkerId>, DbError>;
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
    /// * [`InternalError::TaskInstancePoolCorrupted`] if the task instance cannot be registered in
    ///   the pool.
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
    /// * [`InternalError::TaskInstancePoolCorrupted`] if the task instance cannot be registered in
    ///   the pool.
    async fn register_termination_task_instance(
        &self,
        termination_tcb: SharedTerminationTaskControlBlock,
        registration: TaskInstanceRecord,
    ) -> Result<(), InternalError>;

    /// Removes and returns all live task instances associated with the given worker.
    ///
    /// # Returns
    ///
    /// The records of all task instances that were associated with the given worker.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the pool is in an inconsistent state.
    async fn drain_worker_task_instances(
        &self,
        worker_id: WorkerId,
    ) -> Result<Vec<TaskInstanceRecord>, InternalError>;
}

/// Tracks running task instances and re-enqueues tasks whose soft timeout has elapsed.
#[derive(Clone)]
pub struct TaskInstancePool<
    ReadyQueueSenderType: ReadyQueueSender,
    WorkerLivenessStoreType: WorkerLivenessStore,
> {
    ready_queue_sender: ReadyQueueSenderType,
    worker_liveness_store: WorkerLivenessStoreType,
    worker_stale_cutoff: Duration,
    next_task_instance_id: Arc<AtomicU64>,
    state: Arc<Mutex<TaskInstancePoolState>>,
}

impl<ReadyQueueSenderType: ReadyQueueSender, WorkerLivenessStoreType: WorkerLivenessStore>
    TaskInstancePool<ReadyQueueSenderType, WorkerLivenessStoreType>
{
    /// Factory function.
    ///
    /// # Parameters
    ///
    /// * `ready_queue_sender` - The sender for re-enqueuing tasks to the ready queue.
    /// * `worker_liveness_store` - The store for querying dead workers during GC.
    /// * `worker_stale_cutoff` - The duration after which a worker with no heartbeat is considered
    ///   stale by the pool's GC cycle.
    ///
    /// # Returns
    ///
    /// The created [`TaskInstancePool`].
    #[must_use]
    pub fn new(
        ready_queue_sender: ReadyQueueSenderType,
        worker_liveness_store: WorkerLivenessStoreType,
        worker_stale_cutoff: Duration,
    ) -> Self {
        Self {
            ready_queue_sender,
            worker_liveness_store,
            worker_stale_cutoff,
            next_task_instance_id: Arc::new(AtomicU64::new(1)),
            state: Arc::new(Mutex::new(TaskInstancePoolState::new())),
        }
    }

    /// Runs one soft-timeout GC cycle using the current wall-clock time as the evaluation time.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::run_gc_cycle_at`]'s return values on failure.
    pub async fn run_gc_cycle(&self) -> Result<(), InternalError> {
        self.run_gc_cycle_at(SystemTime::now()).await
    }
}

/// A type-erased control block that holds either a regular or a termination TCB.
#[derive(Clone)]
enum AnySharedControlBlock {
    Task(SharedTaskControlBlock),
    Termination(SharedTerminationTaskControlBlock),
}

impl AnySharedControlBlock {
    async fn force_remove_task_instance(&self, instance_id: TaskInstanceId) -> bool {
        match self {
            Self::Task(tcb) => tcb.force_remove_task_instance(instance_id).await,
            Self::Termination(tcb) => tcb.force_remove_task_instance(instance_id).await,
        }
    }

    async fn has_task_instance(&self, instance_id: TaskInstanceId) -> bool {
        match self {
            Self::Task(tcb) => tcb.has_task_instance(instance_id).await,
            Self::Termination(tcb) => tcb.has_task_instance(instance_id).await,
        }
    }
}

/// A running task-instance entry tracked by the task instance pool.
///
/// This entry combines the externally visible [`TaskInstanceRecord`] with the associated control
/// block and the internal GC bookkeeping state.
#[derive(Clone)]
struct RunningTaskInstanceEntry {
    record: TaskInstanceRecord,
    control_block: AnySharedControlBlock,
    gc_processed: bool,
}

/// The mutable state held by the task instance pool.
///
/// A single `Vec` stores all running task instances. Operations that need to find or remove entries
/// use linear scan, which is sufficient because the pool is small and GC is not speed-sensitive.
struct TaskInstancePoolState {
    running_task_instances: Vec<RunningTaskInstanceEntry>,
}

impl TaskInstancePoolState {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The created [`TaskInstancePoolState`].
    const fn new() -> Self {
        Self {
            running_task_instances: Vec::new(),
        }
    }
}

impl<ReadyQueueSenderType: ReadyQueueSender, WorkerLivenessStoreType: WorkerLivenessStore>
    TaskInstancePool<ReadyQueueSenderType, WorkerLivenessStoreType>
{
    /// Runs one GC cycle using the given wall-clock time as the evaluation time.
    ///
    /// The cycle performs three checks via a single linear scan of all running task instances:
    ///
    /// 1. **Dead worker recovery**: Instances assigned to dead workers are force-removed from their
    ///    TCB, re-enqueued, and removed from the pool.
    /// 2. **Soft-timeout re-enqueue**: Instances whose soft timeout has elapsed (and have not yet
    ///    been processed by a prior cycle) are re-enqueued. The entry stays in the pool so the
    ///    original instance can still complete normally.
    /// 3. **Already-terminated cleanup**: Instances whose TCB no longer tracks them (task completed
    ///    via the normal succeed/fail path) are simply removed from the pool.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if dead-worker recovery or timed-out task re-enqueueing fails.
    /// * Forwards [`WorkerLivenessStore::get_dead_workers`]'s errors on failure.
    async fn run_gc_cycle_at(&self, gc_started_at: SystemTime) -> Result<(), InternalError> {
        let dead_workers = self
            .worker_liveness_store
            .get_dead_workers(
                gc_started_at
                    .checked_sub(self.worker_stale_cutoff)
                    .unwrap_or(SystemTime::UNIX_EPOCH),
            )
            .await
            .map_err(|e| InternalError::TaskInstancePoolCorrupted(e.to_string()))?;
        let dead_worker_set: Vec<WorkerId> = dead_workers;

        // Phase 1: Collect work to do under a single lock acquisition.
        let (dead_worker_entries, soft_timeout_entries, live_entries) = {
            let state = self
                .state
                .lock()
                .expect("task instance pool mutex should not be poisoned");

            let mut dead_worker_entries = Vec::new();
            let mut soft_timeout_entries = Vec::new();
            let mut live_entries = Vec::new();

            for entry in &state.running_task_instances {
                if dead_worker_set.contains(&entry.record.worker_id) {
                    dead_worker_entries.push((entry.record.clone(), entry.control_block.clone()));
                } else if !entry.gc_processed {
                    let Some(soft_timeout_deadline) =
                        entry
                            .record
                            .registered_at
                            .checked_add(Duration::from_millis(
                                entry.record.timeout_policy.soft_timeout_ms,
                            ))
                    else {
                        live_entries.push((entry.record.clone(), entry.control_block.clone()));
                        continue;
                    };
                    if soft_timeout_deadline <= gc_started_at {
                        soft_timeout_entries.push(entry.record.clone());
                    } else {
                        live_entries.push((entry.record.clone(), entry.control_block.clone()));
                    }
                } else {
                    live_entries.push((entry.record.clone(), entry.control_block.clone()));
                }
            }

            drop(state);
            (dead_worker_entries, soft_timeout_entries, live_entries)
        };

        // Phase 2: Re-enqueue dead-worker instances, then force-remove from TCBs.
        for (record, control_block) in &dead_worker_entries {
            self.re_enqueue_task(record.clone()).await?;
            control_block
                .force_remove_task_instance(record.task_instance_id)
                .await;
        }

        // Phase 3: Re-enqueue soft-timed-out instances.
        for record in &soft_timeout_entries {
            self.re_enqueue_task(record.clone()).await?;
            self.set_gc_processed(record.task_instance_id, true);
        }

        // Phase 4: Check if live entries' TCBs still track them. If not, the task completed
        // via the normal path — collect IDs for removal.
        let mut terminated_ids: Vec<TaskInstanceId> = Vec::new();
        for (record, control_block) in &live_entries {
            if !control_block
                .has_task_instance(record.task_instance_id)
                .await
            {
                terminated_ids.push(record.task_instance_id);
            }
        }

        // Phase 5: Remove dead-worker and terminated entries from the pool by ID.
        let ids_to_remove: HashSet<TaskInstanceId> = dead_worker_entries
            .into_iter()
            .map(|(record, _)| record.task_instance_id)
            .chain(terminated_ids)
            .collect();
        {
            let mut state = self
                .state
                .lock()
                .expect("task instance pool mutex should not be poisoned");
            state
                .running_task_instances
                .retain(|entry| !ids_to_remove.contains(&entry.record.task_instance_id));
        }

        Ok(())
    }

    /// Updates the per-entry GC bookkeeping flag if the entry is still tracked by the pool.
    fn set_gc_processed(&self, task_instance_id: TaskInstanceId, gc_processed: bool) {
        let mut state = self
            .state
            .lock()
            .expect("task instance pool mutex should not be poisoned");
        if let Some(entry) = state
            .running_task_instances
            .iter_mut()
            .find(|entry| entry.record.task_instance_id == task_instance_id)
        {
            entry.gc_processed = gc_processed;
        }
    }

    /// Registers a running task instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskInstancePoolCorrupted`] if the task instance ID is already tracked.
    fn register_running_task_instance(
        &self,
        control_block: AnySharedControlBlock,
        record: TaskInstanceRecord,
    ) -> Result<(), InternalError> {
        let mut state = self
            .state
            .lock()
            .expect("task instance pool mutex should not be poisoned");
        let task_instance_id = record.task_instance_id;
        if state
            .running_task_instances
            .iter()
            .any(|entry| entry.record.task_instance_id == task_instance_id)
        {
            return Err(InternalError::TaskInstancePoolCorrupted(format!(
                "task instance {task_instance_id} already registered"
            )));
        }
        state.running_task_instances.push(RunningTaskInstanceEntry {
            record,
            control_block,
            gc_processed: false,
        });
        drop(state);
        Ok(())
    }

    /// Re-enqueues the task corresponding to the given running-record metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if sending the corresponding ready-queue event fails.
    async fn re_enqueue_task(&self, record: TaskInstanceRecord) -> Result<(), InternalError> {
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

#[async_trait]
impl<ReadyQueueSenderType: ReadyQueueSender, WorkerLivenessStoreType: WorkerLivenessStore>
    TaskInstancePoolConnector for TaskInstancePool<ReadyQueueSenderType, WorkerLivenessStoreType>
{
    fn get_next_available_task_instance_id(&self) -> TaskInstanceId {
        self.next_task_instance_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn register_task_instance(
        &self,
        tcb: SharedTaskControlBlock,
        registration: TaskInstanceRecord,
    ) -> Result<(), InternalError> {
        self.register_running_task_instance(AnySharedControlBlock::Task(tcb), registration)
    }

    async fn register_termination_task_instance(
        &self,
        termination_tcb: SharedTerminationTaskControlBlock,
        registration: TaskInstanceRecord,
    ) -> Result<(), InternalError> {
        self.register_running_task_instance(
            AnySharedControlBlock::Termination(termination_tcb),
            registration,
        )
    }

    async fn drain_worker_task_instances(
        &self,
        worker_id: WorkerId,
    ) -> Result<Vec<TaskInstanceRecord>, InternalError> {
        let mut state = self
            .state
            .lock()
            .expect("task instance pool mutex should not be poisoned");
        let mut records = Vec::new();
        let mut i = 0;
        while i < state.running_task_instances.len() {
            if state.running_task_instances[i].record.worker_id == worker_id {
                let entry = state.running_task_instances.swap_remove(i);
                records.push(entry.record);
            } else {
                i += 1;
            }
        }
        drop(state);
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use async_trait::async_trait;
    use spider_core::{
        task::{
            DataTypeDescriptor,
            ExecutionPolicy,
            TaskDescriptor,
            TaskGraph as SubmittedTaskGraph,
            TdlContext,
            ValueTypeDescriptor,
        },
        types::{
            id::{JobId, WorkerId},
            io::TaskInput,
        },
    };

    use super::*;
    use crate::cache::error::{CacheError, StaleStateError};

    /// A [`WorkerLivenessStore`] that always returns an empty dead-worker list.
    #[derive(Clone, Default)]
    struct NoopWorkerLivenessStore;

    #[async_trait]
    impl WorkerLivenessStore for NoopWorkerLivenessStore {
        async fn get_dead_workers(
            &self,
            _stale_before: SystemTime,
        ) -> Result<Vec<WorkerId>, DbError> {
            Ok(Vec::new())
        }
    }

    /// A [`WorkerLivenessStore`] that returns a preconfigured list of dead workers.
    #[derive(Clone, Default)]
    struct MockWorkerLivenessStore {
        dead_workers: Arc<Mutex<Vec<WorkerId>>>,
    }

    impl MockWorkerLivenessStore {
        fn set_dead_workers(&self, workers: Vec<WorkerId>) {
            *self
                .dead_workers
                .lock()
                .expect("dead workers mutex should not be poisoned") = workers;
        }
    }

    #[async_trait]
    impl WorkerLivenessStore for MockWorkerLivenessStore {
        async fn get_dead_workers(
            &self,
            _stale_before: SystemTime,
        ) -> Result<Vec<WorkerId>, DbError> {
            Ok(self
                .dead_workers
                .lock()
                .expect("dead workers mutex should not be poisoned")
                .clone())
        }
    }

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
            for task_index in task_indices {
                self.sent_messages
                    .lock()
                    .expect("ready queue mutex should not be poisoned")
                    .push(ReadyMessage::Task(job_id, task_index));
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

    #[derive(Clone, Default)]
    struct FlakyReadyQueueSender {
        fail_task_ready_once: Arc<Mutex<bool>>,
        sent_messages: Arc<Mutex<Vec<ReadyMessage>>>,
    }

    impl FlakyReadyQueueSender {
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
    impl ReadyQueueSender for FlakyReadyQueueSender {
        async fn send_task_ready(
            &self,
            job_id: JobId,
            task_indices: Vec<usize>,
        ) -> Result<(), InternalError> {
            let mut fail_task_ready_once = self
                .fail_task_ready_once
                .lock()
                .expect("ready queue mutex should not be poisoned");
            if *fail_task_ready_once {
                *fail_task_ready_once = false;
                return Err(InternalError::ReadyQueueSendFailure(
                    "injected failure".to_owned(),
                ));
            }
            drop(fail_task_ready_once);

            for task_index in task_indices {
                self.sent_messages
                    .lock()
                    .expect("ready queue mutex should not be poisoned")
                    .push(ReadyMessage::Task(job_id, task_index));
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

    fn pool_has_entry<S: ReadyQueueSender, L: WorkerLivenessStore>(
        pool: &TaskInstancePool<S, L>,
        task_instance_id: TaskInstanceId,
    ) -> bool {
        let state = pool
            .state
            .lock()
            .expect("task instance pool mutex should not be poisoned");
        state
            .running_task_instances
            .iter()
            .any(|entry| entry.record.task_instance_id == task_instance_id)
    }

    #[tokio::test]
    async fn register_updates_pool() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let pool = TaskInstancePool::new(
            ready_queue_sender,
            NoopWorkerLivenessStore,
            Duration::from_mins(1),
        );
        let tcb = build_single_task_tcb().await;
        let worker_id = WorkerId::new();
        let record = make_record(TaskId::Index(0), 1, worker_id, SystemTime::now());

        pool.register_task_instance(tcb, record.clone())
            .await
            .expect("registration should succeed");

        assert!(pool_has_entry(&pool, record.task_instance_id));
    }

    #[tokio::test]
    async fn soft_timeout_reenqueues_exactly_once_and_keeps_original_instance_live() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let pool = TaskInstancePool::new(
            ready_queue_sender.clone(),
            NoopWorkerLivenessStore,
            Duration::from_mins(1),
        );
        let initial_gc_cycle_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let tcb = build_single_task_tcb().await;
        let worker_id = WorkerId::new();
        let registered_at = initial_gc_cycle_at + Duration::from_millis(50);
        let record = make_record(TaskId::Index(0), 1, worker_id, registered_at);

        tcb.register_task_instance(record.task_instance_id)
            .await
            .expect("task control block registration should succeed");
        pool.register_task_instance(tcb.clone(), record.clone())
            .await
            .expect("registration should succeed");

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(149))
            .await
            .expect("gc should succeed");
        assert!(ready_queue_sender.take_messages().is_empty());

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(150))
            .await
            .expect("gc should succeed");
        assert_eq!(
            ready_queue_sender.take_messages(),
            vec![ReadyMessage::Task(record.job_id, 0)]
        );

        let replacement_result = tcb.register_task_instance(2).await;
        assert!(
            matches!(
                replacement_result,
                Err(CacheError::StaleState(
                    StaleStateError::TaskInstanceLimitExceeded
                ))
            ),
            "soft-timeout should not evict the original instance, got: {replacement_result:?}"
        );

        assert!(pool_has_entry(&pool, record.task_instance_id));

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(250))
            .await
            .expect("gc should succeed");
        assert!(ready_queue_sender.take_messages().is_empty());
    }

    #[tokio::test]
    async fn soft_timeout_retries_after_ready_queue_send_failure() {
        let ready_queue_sender = FlakyReadyQueueSender {
            fail_task_ready_once: Arc::new(Mutex::new(true)),
            ..FlakyReadyQueueSender::default()
        };
        let pool = TaskInstancePool::new(
            ready_queue_sender.clone(),
            NoopWorkerLivenessStore,
            Duration::from_mins(1),
        );
        let initial_gc_cycle_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let tcb = build_single_task_tcb().await;
        let record = make_record(
            TaskId::Index(0),
            1,
            WorkerId::new(),
            initial_gc_cycle_at + Duration::from_millis(50),
        );

        tcb.register_task_instance(record.task_instance_id)
            .await
            .expect("task control block registration should succeed");
        pool.register_task_instance(tcb, record.clone())
            .await
            .expect("registration should succeed");

        let first_gc_result = pool
            .run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(150))
            .await;
        assert!(
            matches!(
                first_gc_result,
                Err(InternalError::ReadyQueueSendFailure(_))
            ),
            "first GC cycle should surface the ready-queue failure, got: {first_gc_result:?}"
        );
        assert!(ready_queue_sender.take_messages().is_empty());

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(250))
            .await
            .expect("gc should retry a previously failed soft-timeout re-enqueue");
        assert_eq!(
            ready_queue_sender.take_messages(),
            vec![ReadyMessage::Task(record.job_id, 0)]
        );

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(350))
            .await
            .expect("gc should not re-enqueue the same soft-timeout twice");
        assert!(ready_queue_sender.take_messages().is_empty());
    }

    #[tokio::test]
    async fn dead_worker_recovery_removes_entries_and_reenqueues() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let liveness_store = MockWorkerLivenessStore::default();
        let pool = TaskInstancePool::new(
            ready_queue_sender.clone(),
            liveness_store.clone(),
            Duration::from_mins(1),
        );
        let initial_gc_cycle_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);

        let tcb_dead = build_single_task_tcb().await;
        let tcb_alive = build_single_task_tcb().await;
        let dead_worker = WorkerId::new();
        let alive_worker = WorkerId::new();

        let dead_record = make_record(
            TaskId::Index(0),
            1,
            dead_worker,
            initial_gc_cycle_at + Duration::from_millis(10),
        );
        let alive_record = make_record(
            TaskId::Index(0),
            2,
            alive_worker,
            initial_gc_cycle_at + Duration::from_millis(10),
        );

        tcb_dead
            .register_task_instance(dead_record.task_instance_id)
            .await
            .expect("task control block registration should succeed");
        tcb_alive
            .register_task_instance(alive_record.task_instance_id)
            .await
            .expect("task control block registration should succeed");

        pool.register_task_instance(tcb_dead.clone(), dead_record.clone())
            .await
            .expect("registration should succeed");
        pool.register_task_instance(tcb_alive.clone(), alive_record.clone())
            .await
            .expect("registration should succeed");

        assert!(pool_has_entry(&pool, dead_record.task_instance_id));
        assert!(pool_has_entry(&pool, alive_record.task_instance_id));

        liveness_store.set_dead_workers(vec![dead_worker]);

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(50))
            .await
            .expect("gc should succeed");

        // Dead worker's instance should be removed from the pool and re-enqueued.
        assert!(
            !pool_has_entry(&pool, dead_record.task_instance_id),
            "dead worker entry should be removed"
        );
        assert!(
            pool_has_entry(&pool, alive_record.task_instance_id),
            "alive worker entry should remain"
        );
        assert_eq!(
            ready_queue_sender.take_messages(),
            vec![ReadyMessage::Task(dead_record.job_id, 0)],
            "dead worker's task should be re-enqueued"
        );

        // The TCB should have been force-removed, allowing a new registration.
        let new_instance_result = tcb_dead.register_task_instance(3).await;
        assert!(
            new_instance_result.is_ok(),
            "force_remove should have freed a slot, got: {new_instance_result:?}"
        );
    }

    #[tokio::test]
    async fn gc_removes_entries_whose_tcb_has_completed() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let pool = TaskInstancePool::new(
            ready_queue_sender.clone(),
            NoopWorkerLivenessStore,
            Duration::from_mins(1),
        );
        let initial_gc_cycle_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);

        let tcb = build_single_task_tcb().await;
        let worker_id = WorkerId::new();
        let record = make_record(
            TaskId::Index(0),
            1,
            worker_id,
            initial_gc_cycle_at + Duration::from_millis(10),
        );

        tcb.register_task_instance(record.task_instance_id)
            .await
            .expect("task control block registration should succeed");
        pool.register_task_instance(tcb.clone(), record.clone())
            .await
            .expect("registration should succeed");

        assert!(pool_has_entry(&pool, record.task_instance_id));

        // Simulate the task completing via the normal succeed path (TCB removes the instance).
        tcb.succeed_task_instance(record.task_instance_id, vec![vec![0u8; 4]])
            .await
            .expect("task success should succeed");

        // The pool still has the entry — GC should detect the TCB no longer tracks it.
        assert!(pool_has_entry(&pool, record.task_instance_id));

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(50))
            .await
            .expect("gc should succeed");

        assert!(
            !pool_has_entry(&pool, record.task_instance_id),
            "completed-task entry should be removed by GC"
        );
        assert!(
            ready_queue_sender.take_messages().is_empty(),
            "no re-enqueue for a task that completed normally"
        );
    }
}
