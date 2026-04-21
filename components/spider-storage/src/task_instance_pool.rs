//! Task instance pool for tracking running task instances and re-enqueuing timed-out work.
//!
//! This module provides the [`TaskInstancePoolHandle`] — a non-generic connector that tracks
//! in-flight task instances across execution managers. It serves two purposes:
//!
//! * **Soft-timeout recovery**: When a task instance exceeds its soft timeout, the pool re-enqueues
//!   the task so a new instance can be scheduled, while the original instance remains live until it
//!   completes or is force-removed.
//! * **Dead-execution-manager recovery**: During each GC cycle, the pool queries the
//!   [`ExecutionManagerLivenessStore`] to detect dead execution managers, force-removes their
//!   instances from the task control blocks, and re-enqueues the corresponding tasks.
//!
//! Internally, the pool runs as a single-owner coroutine: a tokio task owns the mutable state
//! directly (no mutex), processing registration messages and GC timers via `tokio::select!`.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use spider_core::{
    task::TimeoutPolicy,
    types::id::{ExecutionManagerId, JobId, TaskInstanceId},
};
use tokio::sync::mpsc;

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
/// This metadata carries the information needed to re-enqueue soft-timed-out work and to remove all
/// live task instances associated with an execution manager during recovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskInstanceMetadata {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub task_instance_id: TaskInstanceId,
    pub execution_manager_id: ExecutionManagerId,
    pub registered_at: SystemTime,
    pub timeout_policy: TimeoutPolicy,
}

/// Store for tracking execution manager liveness state.
///
/// Implementations persist execution manager heartbeat state durably and provide an atomic
/// operation to detect and mark dead execution managers for recovery.
#[async_trait]
pub trait ExecutionManagerLivenessStore: Clone + Send + Sync {
    /// Checks whether the execution manager with the given ID is alive.
    ///
    /// # Parameters
    ///
    /// * `id` - The execution manager ID to check.
    ///
    /// # Returns
    ///
    /// `true` if the execution manager is alive, `false` otherwise, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards the underlying store's return values on failure.
    async fn is_execution_manager_alive(&self, id: ExecutionManagerId) -> Result<bool, DbError>;

    /// Returns the IDs of execution managers whose last heartbeat is before `stale_before`, after
    /// marking them dead.
    ///
    /// This operation is atomic: once an execution manager is returned by this method, it will not
    /// be returned again in subsequent calls.
    ///
    /// # Parameters
    ///
    /// * `stale_before` - The cutoff time; execution managers with no heartbeat after this time are
    ///   considered dead.
    ///
    /// # Returns
    ///
    /// A vector of dead execution manager IDs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards the underlying store's return values on failure.
    async fn get_dead_execution_managers(
        &self,
        stale_before: SystemTime,
    ) -> Result<Vec<ExecutionManagerId>, DbError>;
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
    /// If the execution manager is dead, the pool force-removes the instance from the TCB and
    /// re-enqueues the task.
    ///
    /// # Parameters
    ///
    /// * `tcb` - The task control block associated with the task instance.
    /// * `registration` - The metadata associated with the task instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`InternalError::TaskInstancePoolCorrupted`] if the pool coroutine has
    ///   terminated.
    async fn register_task_instance(
        &self,
        tcb: SharedTaskControlBlock,
        registration: TaskInstanceMetadata,
    ) -> Result<(), InternalError>;

    /// Registers a termination task instance with the given termination task control block.
    ///
    /// If the execution manager is dead, the pool force-removes the instance from the TCB and
    /// re-enqueues the task.
    ///
    /// # Parameters
    ///
    /// * `termination_tcb` - The termination task control block associated with the task instance.
    /// * `registration` - The metadata associated with the task instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`InternalError::TaskInstancePoolCorrupted`] if the pool coroutine has
    ///   terminated.
    async fn register_termination_task_instance(
        &self,
        termination_tcb: SharedTerminationTaskControlBlock,
        registration: TaskInstanceMetadata,
    ) -> Result<(), InternalError>;
}

/// A type-erased control block that holds either a regular or a termination TCB.
#[derive(Clone)]
enum Tcb {
    Task(SharedTaskControlBlock),
    Termination(SharedTerminationTaskControlBlock),
}

impl Tcb {
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
/// This entry combines the externally visible [`TaskInstanceMetadata`] with the associated control
/// block and the internal soft-timeout bookkeeping flag.
#[derive(Clone)]
struct PoolEntry {
    record: TaskInstanceMetadata,
    control_block: Tcb,
    soft_timeout_handled: bool,
}

/// The mutable state held by the task instance pool coroutine.
///
/// A single `Vec` stores all running task instances. Operations that need to find or remove entries
/// use linear scan, which is sufficient because the pool is small and GC is not speed-sensitive.
struct PoolState {
    running_task_instances: Vec<PoolEntry>,
    known_live_execution_managers: HashSet<ExecutionManagerId>,
}

impl PoolState {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The created [`PoolState`].
    fn new() -> Self {
        Self {
            running_task_instances: Vec::new(),
            known_live_execution_managers: HashSet::new(),
        }
    }
}

/// Messages sent to the task instance pool coroutine.
enum PoolMessage {
    /// Register a task instance.
    Register {
        control_block: Tcb,
        record: TaskInstanceMetadata,
    },
}

/// A handle to the task instance pool for creating and registering task instances.
///
/// This is a non-generic, `Clone`-able connector. All clones share the same channel and atomic ID
/// counter, so they talk to the same underlying pool coroutine.
#[derive(Clone)]
pub struct TaskInstancePoolHandle {
    next_task_instance_id: Arc<AtomicU64>,
    sender: mpsc::Sender<PoolMessage>,
}

impl TaskInstancePoolHandle {
    /// Creates a new task instance pool and returns a handle to it.
    ///
    /// # Parameters
    ///
    /// * `ready_queue_sender` - The sender for re-enqueuing tasks to the ready queue.
    /// * `execution_manager_liveness_store` - The store for querying execution manager liveness.
    /// * `execution_manager_stale_cutoff` - The duration after which an execution manager with no
    ///   heartbeat is considered stale by the pool's GC cycle.
    /// * `gc_interval` - The interval between automatic GC cycles.
    ///
    /// # Returns
    ///
    /// A [`TaskInstancePoolHandle`] connected to the newly spawned pool coroutine.
    ///
    /// # Backpressure
    ///
    /// The pool uses a bounded channel (capacity 128) between the handle and the coroutine. Because
    /// `register_task_instance` is called while the caller holds the JCB read lock, if the
    /// coroutine stalls (e.g., during a GC cycle or a slow liveness check), 128 pending
    /// registrations will cause subsequent `create_task_instance` callers to block under the
    /// read lock, potentially starving write-lock holders (`succeed_task_instance`, `cancel`).
    /// If this becomes an issue under load, consider widening the buffer or restructuring to
    /// avoid holding the lock during registration.
    #[must_use]
    pub fn new<ReadyQueue, LivenessStore>(
        ready_queue_sender: ReadyQueue,
        execution_manager_liveness_store: LivenessStore,
        execution_manager_stale_cutoff: Duration,
        gc_interval: Duration,
    ) -> Self
    where
        ReadyQueue: ReadyQueueSender + 'static,
        LivenessStore: ExecutionManagerLivenessStore + 'static, {
        let next_task_instance_id = Arc::new(AtomicU64::new(1));
        let (sender, receiver) = mpsc::channel(128);

        let pool = TaskInstancePool {
            ready_queue_sender,
            execution_manager_liveness_store,
            execution_manager_stale_cutoff,
            state: PoolState::new(),
            receiver,
        };
        tokio::spawn(async move {
            pool.run(gc_interval).await;
        });

        Self {
            next_task_instance_id,
            sender,
        }
    }
}

/// The task instance pool, running as a tokio coroutine.
///
/// This struct owns all mutable pool state and processes messages from [`TaskInstancePoolHandle`]
/// instances. It is consumed by [`tokio::spawn`] and never exposed publicly.
struct TaskInstancePool<ReadyQueue: ReadyQueueSender, LivenessStore: ExecutionManagerLivenessStore>
{
    ready_queue_sender: ReadyQueue,
    execution_manager_liveness_store: LivenessStore,
    execution_manager_stale_cutoff: Duration,
    state: PoolState,
    receiver: mpsc::Receiver<PoolMessage>,
}

impl<ReadyQueue: ReadyQueueSender, LivenessStore: ExecutionManagerLivenessStore>
    TaskInstancePool<ReadyQueue, LivenessStore>
{
    /// Runs the coroutine loop, processing messages and GC timer ticks.
    async fn run(mut self, gc_interval: Duration) {
        let mut gc_interval = tokio::time::interval(gc_interval);
        // The first tick completes immediately; skip it so we don't GC right at startup.
        gc_interval.tick().await;

        loop {
            tokio::select! {
                message = self.receiver.recv() => {
                    let Some(message) = message else {
                        break;
                    };
                    if !self.handle_message(message).await {
                        break;
                    }
                }
                _ = gc_interval.tick() => {
                    let _ = self.run_gc_cycle_at(SystemTime::now()).await;
                }
            }
        }
    }

    /// Handles a single pool message.
    ///
    /// # Returns
    ///
    /// `false` if the coroutine should shut down.
    async fn handle_message(&mut self, message: PoolMessage) -> bool {
        match message {
            PoolMessage::Register {
                control_block,
                record,
            } => {
                let em_id = record.execution_manager_id;
                let is_known_live = self.state.known_live_execution_managers.contains(&em_id);
                let is_alive = if is_known_live {
                    true
                } else {
                    match self
                        .execution_manager_liveness_store
                        .is_execution_manager_alive(em_id)
                        .await
                    {
                        Ok(alive) => alive,
                        Err(_) => {
                            return true;
                        }
                    }
                };

                if is_alive {
                    if !is_known_live {
                        self.state.known_live_execution_managers.insert(em_id);
                    }
                    self.state.running_task_instances.push(PoolEntry {
                        record,
                        control_block,
                        soft_timeout_handled: false,
                    });
                } else if control_block
                    .has_task_instance(record.task_instance_id)
                    .await
                {
                    if self.re_enqueue_task(&record).await.is_err() {
                        // TCB still owns the instance; a subsequent GC cycle will retry.
                    } else {
                        control_block
                            .force_remove_task_instance(record.task_instance_id)
                            .await;
                    }
                }
            }
        }
        true
    }

    /// Runs one GC cycle using the given wall-clock time as the evaluation time.
    ///
    /// The cycle performs three checks via a single linear scan of all running task instances:
    ///
    /// 1. **Dead execution manager recovery**: Instances assigned to dead execution managers are
    ///    force-removed from their TCB, re-enqueued, and removed from the pool.
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
    /// * Forwards [`ExecutionManagerLivenessStore::get_dead_execution_managers`]'s return values on
    ///   failure.
    /// * Forwards [`ReadyQueueSender::send_task_ready`]'s,
    ///   [`ReadyQueueSender::send_commit_ready`]'s, or [`ReadyQueueSender::send_cleanup_ready`]'s
    ///   return values on failure.
    async fn run_gc_cycle_at(&mut self, gc_started_at: SystemTime) -> Result<(), InternalError> {
        let dead_execution_managers = self
            .execution_manager_liveness_store
            .get_dead_execution_managers(
                gc_started_at
                    .checked_sub(self.execution_manager_stale_cutoff)
                    .unwrap_or(SystemTime::UNIX_EPOCH),
            )
            .await
            .map_err(|e| InternalError::TaskInstancePoolCorrupted(e.to_string()))?;

        for execution_manager_id in &dead_execution_managers {
            self.state
                .known_live_execution_managers
                .remove(execution_manager_id);
        }

        // Phase 1: Collect work to do.
        let mut dead_execution_manager_entries = Vec::new();
        let mut soft_timeout_entries = Vec::new();
        let mut live_entries = Vec::new();

        for entry in &mut self.state.running_task_instances {
            if dead_execution_managers.contains(&entry.record.execution_manager_id) {
                dead_execution_manager_entries
                    .push((entry.record.clone(), entry.control_block.clone()));
            } else if !entry.soft_timeout_handled {
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
                    entry.soft_timeout_handled = true;
                    soft_timeout_entries.push(entry.record.clone());
                } else {
                    live_entries.push((entry.record.clone(), entry.control_block.clone()));
                }
            } else {
                live_entries.push((entry.record.clone(), entry.control_block.clone()));
            }
        }

        // Phase 2: Re-enqueue dead-execution-manager instances, then force-remove from TCBs.
        // Check TCB membership first: if the task already completed, skip re-enqueue.
        let mut dead_em_ids_to_remove: Vec<TaskInstanceId> = Vec::new();
        for (record, control_block) in &dead_execution_manager_entries {
            if control_block
                .has_task_instance(record.task_instance_id)
                .await
            {
                self.re_enqueue_task(record).await?;
                control_block
                    .force_remove_task_instance(record.task_instance_id)
                    .await;
            } else {
                dead_em_ids_to_remove.push(record.task_instance_id);
            }
        }

        // Phase 3: Re-enqueue soft-timed-out instances.
        for (i, record) in soft_timeout_entries.iter().enumerate() {
            if let Err(e) = self.re_enqueue_task(record).await {
                // Roll back soft_timeout_handled for this and all remaining entries.
                for remaining in &soft_timeout_entries[i..] {
                    self.set_soft_timeout_handled(remaining.task_instance_id, false);
                }
                return Err(e);
            }
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

        // Phase 5: Remove dead-execution-manager and terminated entries from the pool by ID.
        let ids_to_remove: HashSet<TaskInstanceId> = dead_execution_manager_entries
            .into_iter()
            .map(|(record, _)| record.task_instance_id)
            .chain(dead_em_ids_to_remove)
            .chain(terminated_ids)
            .collect();
        self.state
            .running_task_instances
            .retain(|entry| !ids_to_remove.contains(&entry.record.task_instance_id));

        Ok(())
    }

    /// Updates the per-entry soft-timeout bookkeeping flag if the entry is still tracked by the
    /// pool.
    fn set_soft_timeout_handled(
        &mut self,
        task_instance_id: TaskInstanceId,
        soft_timeout_handled: bool,
    ) {
        if let Some(entry) = self
            .state
            .running_task_instances
            .iter_mut()
            .find(|entry| entry.record.task_instance_id == task_instance_id)
        {
            entry.soft_timeout_handled = soft_timeout_handled;
        }
    }

    /// Re-enqueues the task corresponding to the given metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ReadyQueueSender::send_task_ready`]'s return values on failure.
    /// * Forwards [`ReadyQueueSender::send_commit_ready`]'s return values on failure.
    /// * Forwards [`ReadyQueueSender::send_cleanup_ready`]'s return values on failure.
    async fn re_enqueue_task(&self, record: &TaskInstanceMetadata) -> Result<(), InternalError> {
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
impl TaskInstancePoolConnector for TaskInstancePoolHandle {
    fn get_next_available_task_instance_id(&self) -> TaskInstanceId {
        self.next_task_instance_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn register_task_instance(
        &self,
        tcb: SharedTaskControlBlock,
        registration: TaskInstanceMetadata,
    ) -> Result<(), InternalError> {
        self.sender
            .send(PoolMessage::Register {
                control_block: Tcb::Task(tcb),
                record: registration,
            })
            .await
            .map_err(|e| {
                InternalError::TaskInstancePoolCorrupted(format!(
                    "task instance pool coroutine is dead: {e}"
                ))
            })
    }

    async fn register_termination_task_instance(
        &self,
        termination_tcb: SharedTerminationTaskControlBlock,
        registration: TaskInstanceMetadata,
    ) -> Result<(), InternalError> {
        self.sender
            .send(PoolMessage::Register {
                control_block: Tcb::Termination(termination_tcb),
                record: registration,
            })
            .await
            .map_err(|e| {
                InternalError::TaskInstancePoolCorrupted(format!(
                    "task instance pool coroutine is dead: {e}"
                ))
            })
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
            id::{ExecutionManagerId, JobId},
            io::TaskInput,
        },
    };
    use tokio::sync::Mutex;

    use super::*;

    /// A [`ExecutionManagerLivenessStore`] that returns a preconfigured list of dead execution
    /// managers and tracks how many times `is_execution_manager_alive` was called.
    #[derive(Clone, Default)]
    struct MockExecutionManagerLivenessStore {
        dead_execution_managers: Arc<Mutex<Vec<ExecutionManagerId>>>,
        alive_call_count: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl ExecutionManagerLivenessStore for MockExecutionManagerLivenessStore {
        async fn is_execution_manager_alive(
            &self,
            _id: ExecutionManagerId,
        ) -> Result<bool, DbError> {
            self.alive_call_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(true)
        }

        async fn get_dead_execution_managers(
            &self,
            _stale_before: SystemTime,
        ) -> Result<Vec<ExecutionManagerId>, DbError> {
            Ok(self.dead_execution_managers.lock().await.clone())
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum ReadyMessage {
        Task(JobId, usize),
        Commit(JobId),
        Cleanup(JobId),
    }

    #[derive(Clone, Default)]
    struct MockReadyQueueSender {
        sent_messages: Arc<Mutex<Vec<ReadyMessage>>>,
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
                    .await
                    .push(ReadyMessage::Task(job_id, task_index));
            }
            Ok(())
        }

        async fn send_commit_ready(&self, job_id: JobId) -> Result<(), InternalError> {
            self.sent_messages
                .lock()
                .await
                .push(ReadyMessage::Commit(job_id));
            Ok(())
        }

        async fn send_cleanup_ready(&self, job_id: JobId) -> Result<(), InternalError> {
            self.sent_messages
                .lock()
                .await
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
        execution_manager_id: ExecutionManagerId,
        registered_at: SystemTime,
    ) -> TaskInstanceMetadata {
        TaskInstanceMetadata {
            job_id: JobId::new(),
            task_id,
            task_instance_id,
            execution_manager_id,
            registered_at,
            timeout_policy: TimeoutPolicy {
                soft_timeout_ms: 100,
                hard_timeout_ms: 200,
            },
        }
    }

    /// A [`ExecutionManagerLivenessStore`] where all EMs are reported as dead.
    #[derive(Clone, Default)]
    struct RejectAllLivenessStore;

    #[async_trait]
    impl ExecutionManagerLivenessStore for RejectAllLivenessStore {
        async fn is_execution_manager_alive(
            &self,
            _id: ExecutionManagerId,
        ) -> Result<bool, DbError> {
            Ok(false)
        }

        async fn get_dead_execution_managers(
            &self,
            _stale_before: SystemTime,
        ) -> Result<Vec<ExecutionManagerId>, DbError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn dead_execution_manager_registration_triggers_recovery() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let pool = TaskInstancePoolHandle::new(
            ready_queue_sender.clone(),
            RejectAllLivenessStore,
            Duration::from_mins(1),
            Duration::from_mins(1),
        );
        let tcb = build_single_task_tcb().await;
        let task_instance_id = 1;
        let _ = tcb
            .register_task_instance(task_instance_id)
            .await
            .expect("TCB registration should succeed");
        let record = make_record(
            TaskId::Index(0),
            task_instance_id,
            ExecutionManagerId::new(),
            SystemTime::now(),
        );
        let job_id = record.job_id;

        pool.register_task_instance(tcb.clone(), record)
            .await
            .unwrap();

        // Give the pool coroutine time to process the message.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let messages = ready_queue_sender.sent_messages.lock().await.clone();
        assert!(
            messages.contains(&ReadyMessage::Task(job_id, 0)),
            "task should be re-enqueued for dead EM, got: {messages:?}"
        );
        assert!(
            !tcb.has_task_instance(task_instance_id).await,
            "task instance should be force-removed from TCB for dead EM"
        );
    }

    #[tokio::test]
    async fn valid_em_is_cached_and_subsequent_registrations_skip_verify() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let liveness_store = MockExecutionManagerLivenessStore::default();
        let pool = TaskInstancePoolHandle::new(
            ready_queue_sender,
            liveness_store.clone(),
            Duration::from_mins(1),
            Duration::from_mins(1),
        );
        let execution_manager_id = ExecutionManagerId::new();

        // First registration should succeed via the verify call.
        let tcb1 = build_single_task_tcb().await;
        let record1 = make_record(TaskId::Index(0), 1, execution_manager_id, SystemTime::now());
        pool.register_task_instance(tcb1, record1).await.unwrap();

        // Second registration for the same EM should also succeed via the live-set fast path.
        let tcb2 = build_single_task_tcb().await;
        let record2 = make_record(TaskId::Index(0), 2, execution_manager_id, SystemTime::now());
        pool.register_task_instance(tcb2, record2).await.unwrap();

        // Give the pool coroutine time to process both messages.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The liveness store should have been called exactly once (for the first registration).
        // The second registration should have hit the cached live-set fast path.
        assert_eq!(
            liveness_store
                .alive_call_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "liveness store should be called exactly once for two registrations with the same EM"
        );
    }
}
