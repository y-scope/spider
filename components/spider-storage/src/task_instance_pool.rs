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
use tokio::sync::{mpsc, oneshot};

use crate::{
    cache::{
        TaskId,
        error::{CacheError, InternalError, StaleStateError},
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
    /// # Returns
    ///
    /// `true` if the execution manager is alive, `false` otherwise.
    async fn is_execution_manager_alive(&self, id: ExecutionManagerId) -> bool;

    /// Returns the IDs of execution managers whose last heartbeat is before `stale_before`, after
    /// marking them dead.
    ///
    /// This operation is atomic: once an execution manager is returned by this method, it will not
    /// be returned again in subsequent calls.
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
    /// # Parameters
    ///
    /// * `tcb` - The task control block associated with the task instance.
    /// * `registration` - The metadata associated with the task instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskInstancePoolCorrupted`] if the task instance cannot be registered in
    ///   the pool.
    /// * [`StaleStateError::ExecutionManagerIsDead`] if the execution manager is known to be dead.
    async fn register_task_instance(
        &self,
        tcb: SharedTaskControlBlock,
        registration: TaskInstanceMetadata,
    ) -> Result<(), CacheError>;

    /// Registers a termination task instance with the given termination task control block.
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
    /// * [`InternalError::TaskInstancePoolCorrupted`] if the task instance cannot be registered in
    ///   the pool.
    /// * [`StaleStateError::ExecutionManagerIsDead`] if the execution manager is known to be dead.
    async fn register_termination_task_instance(
        &self,
        termination_tcb: SharedTerminationTaskControlBlock,
        registration: TaskInstanceMetadata,
    ) -> Result<(), CacheError>;
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
    /// Register a regular task instance.
    Register {
        control_block: Tcb,
        record: TaskInstanceMetadata,
        response_tx: oneshot::Sender<Result<(), CacheError>>,
    },

    /// Run a GC cycle at a specific time (used for testing).
    RunGcAt {
        gc_started_at: SystemTime,
        response_tx: oneshot::Sender<Result<(), InternalError>>,
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
    /// * `execution_manager_liveness_store` - The store for querying dead execution managers during
    ///   GC.
    /// * `execution_manager_stale_cutoff` - The duration after which an execution manager with no
    ///   heartbeat is considered stale by the pool's GC cycle.
    /// * `gc_interval` - The interval between automatic GC cycles.
    ///
    /// # Returns
    ///
    /// A [`TaskInstancePoolHandle`] connected to the newly spawned pool coroutine.
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

    /// Runs one GC cycle at the given time, waiting for the result.
    ///
    /// This is intended for testing. Production GC runs automatically on the internal timer.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * The pool coroutine has been dropped.
    /// * Forwards [`InternalError`] from the GC cycle itself.
    pub async fn run_gc_cycle_at(&self, gc_started_at: SystemTime) -> Result<(), InternalError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(PoolMessage::RunGcAt {
                gc_started_at,
                response_tx,
            })
            .await
            .map_err(|_| {
                InternalError::TaskInstancePoolCorrupted("pool coroutine dropped".into())
            })?;
        response_rx.await.map_err(|_| {
            InternalError::TaskInstancePoolCorrupted("pool coroutine dropped".into())
        })?
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
    /// Returns `false` if the coroutine should shut down.
    async fn handle_message(&mut self, message: PoolMessage) -> bool {
        match message {
            PoolMessage::Register {
                control_block,
                record,
                response_tx,
            } => {
                let em_id = record.execution_manager_id;
                let is_known_live = self.state.known_live_execution_managers.contains(&em_id);
                let result = if is_known_live
                    || self
                        .execution_manager_liveness_store
                        .is_execution_manager_alive(em_id)
                        .await
                {
                    if !is_known_live {
                        self.state.known_live_execution_managers.insert(em_id);
                    }
                    self.state.running_task_instances.push(PoolEntry {
                        record,
                        control_block,
                        soft_timeout_handled: false,
                    });
                    Ok(())
                } else {
                    Err(CacheError::StaleState(
                        StaleStateError::ExecutionManagerIsDead,
                    ))
                };
                let _ = response_tx.send(result);
            }
            PoolMessage::RunGcAt {
                gc_started_at,
                response_tx,
            } => {
                let result = self.run_gc_cycle_at(gc_started_at).await;
                let _ = response_tx.send(result);
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
    /// * [`InternalError`] if sending the corresponding ready-queue event fails.
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
    ) -> Result<(), CacheError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(PoolMessage::Register {
                control_block: Tcb::Task(tcb),
                record: registration,
                response_tx,
            })
            .await
            .map_err(|_| {
                InternalError::TaskInstancePoolCorrupted("pool coroutine dropped".into())
            })?;
        response_rx.await.map_err(|_| {
            InternalError::TaskInstancePoolCorrupted("pool coroutine dropped".into())
        })?
    }

    async fn register_termination_task_instance(
        &self,
        termination_tcb: SharedTerminationTaskControlBlock,
        registration: TaskInstanceMetadata,
    ) -> Result<(), CacheError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(PoolMessage::Register {
                control_block: Tcb::Termination(termination_tcb),
                record: registration,
                response_tx,
            })
            .await
            .map_err(|_| {
                InternalError::TaskInstancePoolCorrupted("pool coroutine dropped".into())
            })?;
        response_rx.await.map_err(|_| {
            InternalError::TaskInstancePoolCorrupted("pool coroutine dropped".into())
        })?
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
    use crate::cache::error::{CacheError, StaleStateError};

    /// A [`ExecutionManagerLivenessStore`] that always returns an empty dead-execution-manager
    /// list.
    #[derive(Clone, Default)]
    struct NoopExecutionManagerLivenessStore;

    #[async_trait]
    impl ExecutionManagerLivenessStore for NoopExecutionManagerLivenessStore {
        async fn is_execution_manager_alive(&self, _id: ExecutionManagerId) -> bool {
            true
        }

        async fn get_dead_execution_managers(
            &self,
            _stale_before: SystemTime,
        ) -> Result<Vec<ExecutionManagerId>, DbError> {
            Ok(Vec::new())
        }
    }

    /// A [`ExecutionManagerLivenessStore`] that returns a preconfigured list of dead execution
    /// managers.
    #[derive(Clone, Default)]
    struct MockExecutionManagerLivenessStore {
        dead_execution_managers: Arc<Mutex<Vec<ExecutionManagerId>>>,
    }

    impl MockExecutionManagerLivenessStore {
        async fn set_dead_execution_managers(&self, execution_managers: Vec<ExecutionManagerId>) {
            *self.dead_execution_managers.lock().await = execution_managers;
        }
    }

    #[async_trait]
    impl ExecutionManagerLivenessStore for MockExecutionManagerLivenessStore {
        async fn is_execution_manager_alive(&self, _id: ExecutionManagerId) -> bool {
            true
        }

        async fn get_dead_execution_managers(
            &self,
            _stale_before: SystemTime,
        ) -> Result<Vec<ExecutionManagerId>, DbError> {
            Ok(self.dead_execution_managers.lock().await.clone())
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
        async fn take_messages(&self) -> Vec<ReadyMessage> {
            std::mem::take(&mut *self.sent_messages.lock().await)
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

    #[derive(Clone, Default)]
    struct FlakyReadyQueueSender {
        fail_task_ready_once: Arc<Mutex<bool>>,
        sent_messages: Arc<Mutex<Vec<ReadyMessage>>>,
    }

    impl FlakyReadyQueueSender {
        async fn take_messages(&self) -> Vec<ReadyMessage> {
            std::mem::take(&mut *self.sent_messages.lock().await)
        }
    }

    #[async_trait]
    impl ReadyQueueSender for FlakyReadyQueueSender {
        async fn send_task_ready(
            &self,
            job_id: JobId,
            task_indices: Vec<usize>,
        ) -> Result<(), InternalError> {
            let mut fail_task_ready_once = self.fail_task_ready_once.lock().await;
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

    #[tokio::test]
    async fn soft_timeout_reenqueues_exactly_once_and_keeps_original_instance_live() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let pool = TaskInstancePoolHandle::new(
            ready_queue_sender.clone(),
            NoopExecutionManagerLivenessStore,
            Duration::from_mins(1),
            Duration::from_mins(1),
        );
        let initial_gc_cycle_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let tcb = build_single_task_tcb().await;
        let execution_manager_id = ExecutionManagerId::new();
        let registered_at = initial_gc_cycle_at + Duration::from_millis(50);
        let record = make_record(TaskId::Index(0), 1, execution_manager_id, registered_at);

        tcb.register_task_instance(record.task_instance_id)
            .await
            .expect("task control block registration should succeed");
        pool.register_task_instance(tcb.clone(), record.clone())
            .await
            .expect("registration should succeed");

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(149))
            .await
            .expect("gc should succeed");
        assert!(ready_queue_sender.take_messages().await.is_empty());

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(150))
            .await
            .expect("gc should succeed");
        assert_eq!(
            ready_queue_sender.take_messages().await,
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

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(250))
            .await
            .expect("gc should succeed");
        assert!(ready_queue_sender.take_messages().await.is_empty());
    }

    #[tokio::test]
    async fn soft_timeout_retries_after_ready_queue_send_failure() {
        let ready_queue_sender = FlakyReadyQueueSender {
            fail_task_ready_once: Arc::new(Mutex::new(true)),
            ..FlakyReadyQueueSender::default()
        };
        let pool = TaskInstancePoolHandle::new(
            ready_queue_sender.clone(),
            NoopExecutionManagerLivenessStore,
            Duration::from_mins(1),
            Duration::from_mins(1),
        );
        let initial_gc_cycle_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let tcb = build_single_task_tcb().await;
        let record = make_record(
            TaskId::Index(0),
            1,
            ExecutionManagerId::new(),
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
        assert!(ready_queue_sender.take_messages().await.is_empty());

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(250))
            .await
            .expect("gc should retry a previously failed soft-timeout re-enqueue");
        assert_eq!(
            ready_queue_sender.take_messages().await,
            vec![ReadyMessage::Task(record.job_id, 0)]
        );

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(350))
            .await
            .expect("gc should not re-enqueue the same soft-timeout twice");
        assert!(ready_queue_sender.take_messages().await.is_empty());
    }

    #[tokio::test]
    async fn dead_execution_manager_recovery_removes_entries_and_reenqueues() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let liveness_store = MockExecutionManagerLivenessStore::default();
        let pool = TaskInstancePoolHandle::new(
            ready_queue_sender.clone(),
            liveness_store.clone(),
            Duration::from_mins(1),
            Duration::from_mins(1),
        );
        let initial_gc_cycle_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);

        let tcb_dead = build_single_task_tcb().await;
        let tcb_alive = build_single_task_tcb().await;
        let dead_em = ExecutionManagerId::new();
        let alive_em = ExecutionManagerId::new();

        let dead_record = make_record(
            TaskId::Index(0),
            1,
            dead_em,
            initial_gc_cycle_at + Duration::from_millis(10),
        );
        let alive_record = make_record(
            TaskId::Index(0),
            2,
            alive_em,
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

        liveness_store
            .set_dead_execution_managers(vec![dead_em])
            .await;

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(50))
            .await
            .expect("gc should succeed");

        // Dead execution manager's instance should be re-enqueued.
        assert_eq!(
            ready_queue_sender.take_messages().await,
            vec![ReadyMessage::Task(dead_record.job_id, 0)],
            "dead execution manager's task should be re-enqueued"
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
        let pool = TaskInstancePoolHandle::new(
            ready_queue_sender.clone(),
            NoopExecutionManagerLivenessStore,
            Duration::from_mins(1),
            Duration::from_mins(1),
        );
        let initial_gc_cycle_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);

        let tcb = build_single_task_tcb().await;
        let execution_manager_id = ExecutionManagerId::new();
        let record = make_record(
            TaskId::Index(0),
            1,
            execution_manager_id,
            initial_gc_cycle_at + Duration::from_millis(10),
        );

        tcb.register_task_instance(record.task_instance_id)
            .await
            .expect("task control block registration should succeed");
        pool.register_task_instance(tcb.clone(), record.clone())
            .await
            .expect("registration should succeed");

        // Simulate the task completing via the normal succeed path (TCB removes the instance).
        tcb.succeed_task_instance(record.task_instance_id, vec![vec![0u8; 4]])
            .await
            .expect("task success should succeed");

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(50))
            .await
            .expect("gc should succeed");

        assert!(
            ready_queue_sender.take_messages().await.is_empty(),
            "no re-enqueue for a task that completed normally"
        );
    }

    /// A [`ExecutionManagerLivenessStore`] where all EMs are reported as dead.
    #[derive(Clone, Default)]
    struct RejectAllLivenessStore;

    #[async_trait]
    impl ExecutionManagerLivenessStore for RejectAllLivenessStore {
        async fn is_execution_manager_alive(&self, _id: ExecutionManagerId) -> bool {
            false
        }

        async fn get_dead_execution_managers(
            &self,
            _stale_before: SystemTime,
        ) -> Result<Vec<ExecutionManagerId>, DbError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn registration_rejected_for_dead_execution_manager() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let pool = TaskInstancePoolHandle::new(
            ready_queue_sender,
            RejectAllLivenessStore,
            Duration::from_mins(1),
            Duration::from_mins(1),
        );
        let tcb = build_single_task_tcb().await;
        let record = make_record(
            TaskId::Index(0),
            1,
            ExecutionManagerId::new(),
            SystemTime::now(),
        );

        let result = pool.register_task_instance(tcb, record).await;
        assert!(
            matches!(
                result,
                Err(CacheError::StaleState(
                    StaleStateError::ExecutionManagerIsDead
                ))
            ),
            "registration from unknown dead EM should be rejected, got: {result:?}"
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
        pool.register_task_instance(tcb1, record1)
            .await
            .expect("first registration should succeed");

        // Second registration for the same EM should also succeed via the live-set fast path.
        let tcb2 = build_single_task_tcb().await;
        let record2 = make_record(TaskId::Index(0), 2, execution_manager_id, SystemTime::now());
        pool.register_task_instance(tcb2, record2)
            .await
            .expect("second registration for same EM should succeed via live-set cache");
    }

    #[tokio::test]
    async fn gc_removes_dead_em_from_live_set() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let liveness_store = MockExecutionManagerLivenessStore::default();
        let pool = TaskInstancePoolHandle::new(
            ready_queue_sender.clone(),
            liveness_store.clone(),
            Duration::from_mins(1),
            Duration::from_mins(1),
        );
        let initial_gc_cycle_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let dead_em = ExecutionManagerId::new();

        // Register with the live EM (MockExecutionManagerLivenessStore returns true).
        let tcb = build_single_task_tcb().await;
        let record = make_record(TaskId::Index(0), 1, dead_em, initial_gc_cycle_at);
        tcb.register_task_instance(record.task_instance_id)
            .await
            .expect("task control block registration should succeed");
        pool.register_task_instance(tcb.clone(), record.clone())
            .await
            .expect("registration should succeed");

        // Mark the EM as dead for the next GC cycle.
        liveness_store
            .set_dead_execution_managers(vec![dead_em])
            .await;

        pool.run_gc_cycle_at(initial_gc_cycle_at + Duration::from_millis(50))
            .await
            .expect("gc should succeed");

        // The dead EM should have been removed from the live set, so a new registration
        // will go through verify again. Since MockExecutionManagerLivenessStore still returns
        // true for is_execution_manager_alive, registration succeeds.
        let tcb2 = build_single_task_tcb().await;
        let record2 = make_record(
            TaskId::Index(0),
            2,
            dead_em,
            initial_gc_cycle_at + Duration::from_millis(60),
        );
        pool.register_task_instance(tcb2, record2.clone())
            .await
            .expect("re-registration after GC should succeed");
    }
}
