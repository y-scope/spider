//! Task instance pool for tracking running task instances and re-enqueuing timed-out work.
//!
//! This module provides the [`TaskInstancePoolHandle`] — a non-generic connector that tracks
//! in-flight task instances across execution managers. It serves two purposes:
//!
//! * **Soft-timeout recovery**: When a task instance exceeds its soft timeout, the pool re-enqueues
//!   the task so a new instance can be scheduled, while the original instance remains live until it
//!   completes or is force-removed.
//! * **Dead-execution-manager recovery**: During each GC cycle, the pool queries the
//!   [`ExecutionManagerLivenessManagement`] to detect dead execution managers, force-removes their
//!   instances from the task control blocks, and re-enqueues the corresponding tasks.
//!
//! Internally, the pool runs as a single-owner coroutine: a tokio task owns the mutable state
//! directly (no mutex), processing registration messages and GC timers via `tokio::select!`.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;
use serde::Deserialize;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cache::error::InternalError;
use crate::cache::task::SharedTaskControlBlock;
use crate::cache::task::SharedTerminationTaskControlBlock;
use crate::db::ExecutionManagerLivenessManagement;
use crate::ready_queue::ReadyQueueSender;

/// Configuration for a task instance pool actor.
///
/// Controls GC timing, channel buffering, and execution manager staleness detection.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct TaskInstancePoolConfig {
    /// Seconds without a heartbeat after which an execution manager is considered stale.
    pub execution_manager_stale_cutoff_sec: u64,

    /// Interval in seconds between GC cycles that check for dead execution managers.
    pub gc_interval_sec: u64,

    /// The capacity of the bounded message channel.
    pub message_channel_capacity: usize,
}

impl TaskInstancePoolConfig {
    /// Validates the configuration parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskInstancePoolInvalidConfig`] if `execution_manager_stale_cutoff_sec`,
    ///   `gc_interval_sec`, or `message_channel_capacity` is zero.
    pub const fn validate(&self) -> Result<(), InternalError> {
        if self.execution_manager_stale_cutoff_sec == 0 {
            return Err(InternalError::TaskInstancePoolInvalidConfig(
                "execution_manager_stale_cutoff_sec must be greater than zero",
            ));
        }
        if self.gc_interval_sec == 0 {
            return Err(InternalError::TaskInstancePoolInvalidConfig(
                "gc_interval_sec must be greater than zero",
            ));
        }
        if self.message_channel_capacity == 0 {
            return Err(InternalError::TaskInstancePoolInvalidConfig(
                "message_channel_capacity must be greater than zero",
            ));
        }
        Ok(())
    }
}

impl Default for TaskInstancePoolConfig {
    fn default() -> Self {
        Self {
            execution_manager_stale_cutoff_sec: 60,
            gc_interval_sec: 30,
            message_channel_capacity: 128,
        }
    }
}

/// Metadata for a running task instance tracked by the task instance pool.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskInstanceMetadata {
    pub resource_group_id: ResourceGroupId,
    pub job_id: JobId,
    pub task_id: TaskId,
    pub task_instance_id: TaskInstanceId,
    pub execution_manager_id: ExecutionManagerId,
    pub soft_timeout_ddl: Option<SystemTime>,
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
    /// * [`InternalError::TaskInstancePoolCorrupted`] if the pool coroutine has terminated.
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
    /// * [`InternalError::TaskInstancePoolCorrupted`] if the pool coroutine has terminated.
    async fn register_termination_task_instance(
        &self,
        termination_tcb: SharedTerminationTaskControlBlock,
        registration: TaskInstanceMetadata,
    ) -> Result<(), InternalError>;
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
                tcb: Tcb::Task(tcb),
                metadata: registration,
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
                tcb: Tcb::Termination(termination_tcb),
                metadata: registration,
            })
            .await
            .map_err(|e| {
                InternalError::TaskInstancePoolCorrupted(format!(
                    "task instance pool coroutine is dead: {e}"
                ))
            })
    }
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

    async fn is_terminal(&self) -> bool {
        match self {
            Self::Task(tcb) => tcb.is_terminal().await,
            Self::Termination(tcb) => tcb.is_terminal().await,
        }
    }
}

/// A running task-instance entry tracked by the task instance pool.
///
/// This entry combines the externally visible [`TaskInstanceMetadata`] with the associated control
/// block and the internal soft-timeout bookkeeping flag.
#[derive(Clone)]
struct PoolEntry {
    metadata: TaskInstanceMetadata,
    tcb: Tcb,
    soft_timeout_handled: bool,
}

/// Messages sent to the task instance pool coroutine.
enum PoolMessage {
    /// Register a task instance.
    Register {
        tcb: Tcb,
        metadata: TaskInstanceMetadata,
    },
}

/// The task instance pool, running as a tokio coroutine.
///
/// This struct owns all mutable pool states and processes messages from [`TaskInstancePoolHandle`]
/// instances. It is consumed by [`tokio::spawn`] and never exposed publicly.
///
/// A single `Vec` stores all running task instances. Operations that need to find or remove entries
/// use linear scan, which is sufficient because the pool is small and GC is not speed-sensitive.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready queue sender implementation for re-enqueue operations.
/// * `LivenessStoreType` - The execution manager liveness store implementation.
struct TaskInstancePool<
    ReadyQueueSenderType: ReadyQueueSender,
    LivenessStoreType: ExecutionManagerLivenessManagement,
> {
    ready_queue_sender: ReadyQueueSenderType,
    execution_manager_liveness_store: LivenessStoreType,
    execution_manager_pool: HashSet<ExecutionManagerId>,
    execution_manager_stale_after_sec: u64,
    instances: Vec<PoolEntry>,
    receiver: mpsc::Receiver<PoolMessage>,
}

impl<ReadyQueueSenderType: ReadyQueueSender, LivenessStoreType: ExecutionManagerLivenessManagement>
    TaskInstancePool<ReadyQueueSenderType, LivenessStoreType>
{
    /// Runs the coroutine loop, processing messages and GC timer ticks.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::handle_message`]'s return values on failure.
    /// * Forwards [`Self::run_gc_cycle_at`]'s return values on failure.
    async fn run(
        mut self,
        cancellation_token: CancellationToken,
        gc_interval_sec: u64,
    ) -> Result<(), InternalError> {
        let mut gc_interval = tokio::time::interval(Duration::from_secs(gc_interval_sec));
        // The first tick completes immediately; skip it so we don't GC right at startup.
        gc_interval.tick().await;

        loop {
            tokio::select! {
                () = cancellation_token.cancelled() => {
                    return Ok(());
                }
                message = self.receiver.recv() => {
                    let Some(message) = message else {
                        // TODO: log this exit
                        return Ok(());
                    };
                    self.handle_message(message).await?;
                }
                _ = gc_interval.tick() => {
                    let () = self.run_gc_cycle_at(SystemTime::now()).await?;
                }
            }
        }
    }

    /// Handles a single pool message.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ExecutionManagerLivenessManagement::is_execution_manager_alive`]'s return
    ///   values on failure.
    /// * Forwards [`Self::re_enqueue_task`]'s return values on failure.
    #[allow(clippy::set_contains_or_insert)]
    async fn handle_message(&mut self, message: PoolMessage) -> Result<(), InternalError> {
        match message {
            PoolMessage::Register { tcb, metadata } => {
                let em_id = metadata.execution_manager_id;
                if !self.execution_manager_pool.contains(&em_id) {
                    if !self
                        .execution_manager_liveness_store
                        .is_execution_manager_alive(em_id)
                        .await?
                    {
                        if tcb
                            .force_remove_task_instance(metadata.task_instance_id)
                            .await
                        {
                            // There is a corner case where the force-removed task instance has
                            // already terminated. In this case, the re-enqueue is redundant.
                            // However, checking this termination is expensive and this case should
                            // be rare. Besides, the downstream logic will gracefully handle the
                            // re-enqueued task-ready message. Therefore, this re-enqueue is
                            // unconditionally performed.
                            self.re_enqueue_task(&metadata).await?;
                        }
                        return Ok(());
                    }
                    self.execution_manager_pool.insert(em_id);
                }
                self.instances.push(PoolEntry {
                    metadata,
                    tcb,
                    soft_timeout_handled: false,
                });
                Ok(())
            }
        }
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
    /// 3. **Already-terminated cleanup**: Instances whose TCB has reached a terminal state (task
    //     completed via the normal succeed/fail/cancel path) are simply removed from the pool.
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ExecutionManagerLivenessManagement::get_dead_execution_managers`]'s return
    ///   values on failure.
    /// * Forwards [`Self::re_enqueue_task`]'s return values on failure.
    async fn run_gc_cycle_at(&mut self, gc_started_at: SystemTime) -> Result<(), InternalError> {
        let dead_em_ids: Vec<ExecutionManagerId> = self
            .execution_manager_liveness_store
            .get_dead_execution_managers(self.execution_manager_stale_after_sec)
            .await?;

        for execution_manager_id in &dead_em_ids {
            self.execution_manager_pool.remove(execution_manager_id);
        }

        let mut indices_to_remove: Vec<usize> = Vec::new();
        let mut indices_to_re_enqueue: Vec<usize> = Vec::new();
        for (idx, entry) in self.instances.iter_mut().enumerate() {
            let tcb = &entry.tcb;
            if tcb.is_terminal().await {
                indices_to_remove.push(idx);
                continue;
            }

            if !self
                .execution_manager_pool
                .contains(&entry.metadata.execution_manager_id)
            {
                // EM no longer lives, force-remove from TCB.
                if tcb
                    .force_remove_task_instance(entry.metadata.task_instance_id)
                    .await
                {
                    indices_to_re_enqueue.push(idx);
                }
                indices_to_remove.push(idx);
                continue;
            }

            if !entry.soft_timeout_handled
                && let Some(soft_timeout_ddl) = entry.metadata.soft_timeout_ddl
                && soft_timeout_ddl <= gc_started_at
            {
                entry.soft_timeout_handled = true;
                indices_to_re_enqueue.push(idx);
            }
        }

        for entry_to_re_enqueue in indices_to_re_enqueue
            .into_iter()
            .filter_map(|idx| self.instances.get(idx))
        {
            self.re_enqueue_task(&entry_to_re_enqueue.metadata).await?;
        }

        // Remove entries. `indices_to_remove` is in sorted ascending order. Reversely iterate to
        // apply `swap_remove` is the most efficient way to remove entries in the current
        // implementation.
        for idx_to_remove in indices_to_remove.into_iter().rev() {
            self.instances.swap_remove(idx_to_remove);
        }

        Ok(())
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
    async fn re_enqueue_task(&self, metadata: &TaskInstanceMetadata) -> Result<(), InternalError> {
        match metadata.task_id {
            TaskId::Index(task_index) => {
                self.ready_queue_sender
                    .send_task_ready(
                        metadata.resource_group_id,
                        metadata.job_id,
                        vec![task_index],
                    )
                    .await
            }
            TaskId::Commit => {
                self.ready_queue_sender
                    .send_commit_ready(metadata.resource_group_id, metadata.job_id)
                    .await
            }
            TaskId::Cleanup => {
                self.ready_queue_sender
                    .send_cleanup_ready(metadata.resource_group_id, metadata.job_id)
                    .await
            }
        }
    }
}

/// Creates a task instance pool.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready queue sender implementation for re-enqueue operations.
/// * `LivenessStoreType` - The execution manager liveness store implementation.
///
/// # Returns
///
/// A tuple on success, containing:
///
/// * A [`TaskInstancePoolHandle`] for registering task instances to the pool.
/// * A [`JoinHandle`] for the task instance pool actor.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`TaskInstancePoolConfig::validate`]'s return values on failure.
pub fn create_task_instance_pool<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    LivenessStoreType: ExecutionManagerLivenessManagement + 'static,
>(
    ready_queue_sender: ReadyQueueSenderType,
    execution_manager_liveness_store: LivenessStoreType,
    cancellation_token: CancellationToken,
    config: &TaskInstancePoolConfig,
) -> Result<
    (
        TaskInstancePoolHandle,
        JoinHandle<Result<(), InternalError>>,
    ),
    InternalError,
> {
    config.validate()?;

    let next_task_instance_id = Arc::new(AtomicU64::new(1));
    let (sender, receiver) = mpsc::channel(config.message_channel_capacity);

    let pool = TaskInstancePool {
        ready_queue_sender,
        execution_manager_liveness_store,
        execution_manager_stale_after_sec: config.execution_manager_stale_cutoff_sec,
        instances: Vec::new(),
        execution_manager_pool: HashSet::new(),
        receiver,
    };
    let gc_interval_sec = config.gc_interval_sec;
    let pool_join_handle =
        tokio::spawn(async move { pool.run(cancellation_token, gc_interval_sec).await });
    let handle = TaskInstancePoolHandle {
        next_task_instance_id,
        sender,
    };

    Ok((handle, pool_join_handle))
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::time::Duration;
    use std::time::SystemTime;

    use async_trait::async_trait;
    use spider_core::task::DataTypeDescriptor;
    use spider_core::task::ExecutionPolicy;
    use spider_core::task::TaskDescriptor;
    use spider_core::task::TaskGraph as SubmittedTaskGraph;
    use spider_core::task::TdlContext;
    use spider_core::task::ValueTypeDescriptor;
    use spider_core::types::id::ExecutionManagerId;
    use spider_core::types::id::JobId;
    use spider_core::types::id::ResourceGroupId;
    use spider_core::types::io::TaskInput;
    use tokio::sync::Mutex;

    use super::*;
    use crate::db::DbError;
    use crate::job_submission::create_validated_submission;

    const DEFAULT_CHANNEL_SIZE: usize = 128;

    /// A [`ExecutionManagerLivenessManagement`] that returns a preconfigured list of dead execution
    /// managers and tracks how many times `is_execution_manager_alive` was called.
    #[derive(Clone, Default)]
    struct MockExecutionManagerLivenessManagement {
        dead_execution_managers: Arc<Mutex<Vec<ExecutionManagerId>>>,
        alive_call_count: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl ExecutionManagerLivenessManagement for MockExecutionManagerLivenessManagement {
        async fn register_execution_manager(
            &self,
            _ip_address: IpAddr,
        ) -> Result<ExecutionManagerId, DbError> {
            unimplemented!("not needed by pool tests")
        }

        async fn update_execution_manager_heartbeat(
            &self,
            _execution_manager_id: ExecutionManagerId,
        ) -> Result<(), DbError> {
            unimplemented!("not needed by pool tests")
        }

        async fn is_execution_manager_alive(
            &self,
            _execution_manager_id: ExecutionManagerId,
        ) -> Result<bool, DbError> {
            self.alive_call_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(true)
        }

        async fn get_dead_execution_managers(
            &self,
            _stale_after_sec: u64,
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
            _rg_id: ResourceGroupId,
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

        async fn send_commit_ready(
            &self,
            _rg_id: ResourceGroupId,
            job_id: JobId,
        ) -> Result<(), InternalError> {
            self.sent_messages
                .lock()
                .await
                .push(ReadyMessage::Commit(job_id));
            Ok(())
        }

        async fn send_cleanup_ready(
            &self,
            _rg_id: ResourceGroupId,
            job_id: JobId,
        ) -> Result<(), InternalError> {
            self.sent_messages
                .lock()
                .await
                .push(ReadyMessage::Cleanup(job_id));
            Ok(())
        }
    }

    /// A [`ExecutionManagerLivenessManagement`] where all EMs are reported as dead.
    #[derive(Clone, Default)]
    struct RejectAllLivenessStore;

    #[async_trait]
    impl ExecutionManagerLivenessManagement for RejectAllLivenessStore {
        async fn register_execution_manager(
            &self,
            _ip_address: IpAddr,
        ) -> Result<ExecutionManagerId, DbError> {
            unimplemented!("not needed by pool tests")
        }

        async fn update_execution_manager_heartbeat(
            &self,
            _execution_manager_id: ExecutionManagerId,
        ) -> Result<(), DbError> {
            unimplemented!("not needed by pool tests")
        }

        async fn is_execution_manager_alive(
            &self,
            _execution_manager_id: ExecutionManagerId,
        ) -> Result<bool, DbError> {
            Ok(false)
        }

        async fn get_dead_execution_managers(
            &self,
            _stale_after_sec: u64,
        ) -> Result<Vec<ExecutionManagerId>, DbError> {
            Ok(Vec::new())
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
        let job_submission =
            create_validated_submission(submitted, vec![TaskInput::ValuePayload(vec![0u8; 4])]);
        let task_graph = crate::cache::task::TaskGraph::create(job_submission)
            .await
            .expect("cache task graph creation should succeed");
        task_graph
            .get_task_control_block(0)
            .expect("task control block should exist")
    }

    /// # Returns
    ///
    /// A newly created task instance metadata with a fixed timeout policy (soft timeout 100ms).
    fn make_task_instance_metadata(
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        execution_manager_id: ExecutionManagerId,
        registered_at: SystemTime,
    ) -> TaskInstanceMetadata {
        const SOFT_TIMEOUT_MS: Duration = Duration::from_millis(100);
        TaskInstanceMetadata {
            resource_group_id: ResourceGroupId::random(),
            job_id: JobId::random(),
            task_id,
            task_instance_id,
            execution_manager_id,
            soft_timeout_ddl: registered_at.checked_add(SOFT_TIMEOUT_MS),
        }
    }

    /// Builds a [`TaskInstancePool`] directly (no `tokio::spawn`) so tests can drive
    /// [`TaskInstancePool::handle_message`] and [`TaskInstancePool::run_gc_cycle_at`]
    /// synchronously.
    ///
    /// The `mpsc::Receiver` field is required by the struct but unused by these tests; the matching
    /// sender is dropped immediately.
    fn build_test_pool(
        ready_queue_sender: MockReadyQueueSender,
        liveness_store: MockExecutionManagerLivenessManagement,
        execution_manager_stale_cutoff: Duration,
    ) -> TaskInstancePool<MockReadyQueueSender, MockExecutionManagerLivenessManagement> {
        let (_sender, receiver) = mpsc::channel(1);
        TaskInstancePool {
            ready_queue_sender,
            execution_manager_liveness_store: liveness_store,
            execution_manager_pool: HashSet::new(),
            execution_manager_stale_after_sec: execution_manager_stale_cutoff.as_secs(),
            instances: Vec::new(),
            receiver,
        }
    }

    /// Registers a task instance in both the TCB and the pool by driving
    /// [`TaskInstancePool::handle_message`].
    ///
    /// # Returns
    ///
    /// The job ID assigned to the task, so callers can match it against re-enqueue messages.
    async fn register_task_in_pool(
        pool: &mut TaskInstancePool<MockReadyQueueSender, MockExecutionManagerLivenessManagement>,
        tcb: &SharedTaskControlBlock,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        execution_manager_id: ExecutionManagerId,
        registered_at: SystemTime,
    ) -> JobId {
        let _ = tcb
            .register_task_instance(task_instance_id)
            .await
            .expect("TCB registration should succeed");
        let metadata = make_task_instance_metadata(
            task_id,
            task_instance_id,
            execution_manager_id,
            registered_at,
        );
        let job_id = metadata.job_id;
        pool.handle_message(PoolMessage::Register {
            tcb: Tcb::Task(tcb.clone()),
            metadata,
        })
        .await
        .expect("registration should succeed");
        job_id
    }

    #[tokio::test]
    async fn dead_execution_manager_registration_triggers_recovery() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let cancellation_token = CancellationToken::new();
        let (pool, pool_join_handle) = create_task_instance_pool(
            ready_queue_sender.clone(),
            RejectAllLivenessStore,
            cancellation_token.clone(),
            &TaskInstancePoolConfig {
                execution_manager_stale_cutoff_sec: 60,
                gc_interval_sec: 60,
                message_channel_capacity: DEFAULT_CHANNEL_SIZE,
            },
        )
        .expect("pool creation should succeed");
        let tcb = build_single_task_tcb().await;
        let task_instance_id = 1;
        let _ = tcb
            .register_task_instance(task_instance_id)
            .await
            .expect("TCB registration should succeed");
        let metadata = make_task_instance_metadata(
            TaskId::Index(0),
            task_instance_id,
            ExecutionManagerId::random(),
            SystemTime::now(),
        );
        let job_id = metadata.job_id;

        pool.register_task_instance(tcb.clone(), metadata)
            .await
            .expect("registration should be sent");

        // Give the pool coroutine time to process the message.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let messages = ready_queue_sender.sent_messages.lock().await.clone();
        assert!(
            messages.contains(&ReadyMessage::Task(job_id, 0)),
            "task should be re-enqueued for dead EM, got: {messages:?}"
        );
        cancellation_token.cancel();
        tokio::time::timeout(Duration::from_secs(1), pool_join_handle)
            .await
            .expect("pool task should exit before timeout")
            .expect("pool task should join successfully")
            .expect("pool task should return success");
    }

    #[tokio::test]
    async fn valid_em_is_cached_and_subsequent_registrations_skip_verify() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let liveness_store = MockExecutionManagerLivenessManagement::default();
        let cancellation_token = CancellationToken::new();
        let (pool, pool_join_handle) = create_task_instance_pool(
            ready_queue_sender,
            liveness_store.clone(),
            cancellation_token.clone(),
            &TaskInstancePoolConfig {
                execution_manager_stale_cutoff_sec: 60,
                gc_interval_sec: 60,
                message_channel_capacity: DEFAULT_CHANNEL_SIZE,
            },
        )
        .expect("pool creation should succeed");
        let execution_manager_id = ExecutionManagerId::random();

        let tcb1 = build_single_task_tcb().await;
        let metadata1 = make_task_instance_metadata(
            TaskId::Index(0),
            1,
            execution_manager_id,
            SystemTime::now(),
        );
        pool.register_task_instance(tcb1, metadata1)
            .await
            .expect("first registration should succeed");

        let tcb2 = build_single_task_tcb().await;
        let metadata2 = make_task_instance_metadata(
            TaskId::Index(0),
            2,
            execution_manager_id,
            SystemTime::now(),
        );
        pool.register_task_instance(tcb2, metadata2)
            .await
            .expect("second registration should succeed");

        // Give the pool coroutine time to process both messages.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The liveness store should have been called exactly once (for the first registration).
        // The second registration should have hit the cached live-set fast path.
        assert_eq!(
            liveness_store.alive_call_count.load(Ordering::Relaxed),
            1,
            "liveness store should be called exactly once for two registrations with the same EM"
        );
        cancellation_token.cancel();
        tokio::time::timeout(Duration::from_secs(1), pool_join_handle)
            .await
            .expect("pool task should exit before timeout")
            .expect("pool task should join successfully")
            .expect("pool task should return success");
    }

    #[tokio::test]
    async fn spawned_pool_exits_when_cancelled() {
        let cancellation_token = CancellationToken::new();
        let (_pool, pool_join_handle) = create_task_instance_pool(
            MockReadyQueueSender::default(),
            MockExecutionManagerLivenessManagement::default(),
            cancellation_token.clone(),
            &TaskInstancePoolConfig {
                execution_manager_stale_cutoff_sec: 60,
                gc_interval_sec: 60,
                message_channel_capacity: DEFAULT_CHANNEL_SIZE,
            },
        )
        .expect("pool creation should succeed");

        cancellation_token.cancel();

        tokio::time::timeout(Duration::from_secs(1), pool_join_handle)
            .await
            .expect("pool task should exit before timeout")
            .expect("pool task should join successfully")
            .expect("pool task should return success");
    }

    #[tokio::test]
    async fn spawned_pool_processes_registration_before_shutdown() {
        let ready_queue_sender = MockReadyQueueSender::default();
        let cancellation_token = CancellationToken::new();
        let (pool, pool_join_handle) = create_task_instance_pool(
            ready_queue_sender.clone(),
            RejectAllLivenessStore,
            cancellation_token.clone(),
            &TaskInstancePoolConfig {
                execution_manager_stale_cutoff_sec: 60,
                gc_interval_sec: 60,
                message_channel_capacity: DEFAULT_CHANNEL_SIZE,
            },
        )
        .expect("pool creation should succeed");
        let tcb = build_single_task_tcb().await;
        let task_instance_id = 1;
        let _ = tcb
            .register_task_instance(task_instance_id)
            .await
            .expect("TCB registration should succeed");
        let metadata = make_task_instance_metadata(
            TaskId::Index(0),
            task_instance_id,
            ExecutionManagerId::random(),
            SystemTime::now(),
        );
        let job_id = metadata.job_id;

        pool.register_task_instance(tcb, metadata)
            .await
            .expect("registration should be sent");
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancellation_token.cancel();
        tokio::time::timeout(Duration::from_secs(1), pool_join_handle)
            .await
            .expect("pool task should exit before timeout")
            .expect("pool task should join successfully")
            .expect("pool task should return success");

        let messages = ready_queue_sender.sent_messages.lock().await.clone();
        assert!(
            messages.contains(&ReadyMessage::Task(job_id, 0)),
            "registration should be processed before shutdown, got: {messages:?}"
        );
    }

    #[tokio::test]
    async fn gc_removes_all_terminated_tasks() {
        const NUM_TASKS: usize = 10;

        let ready_queue_sender = MockReadyQueueSender::default();
        let liveness_store = MockExecutionManagerLivenessManagement::default();
        let mut pool = build_test_pool(
            ready_queue_sender.clone(),
            liveness_store,
            Duration::from_mins(1),
        );
        let em_id = ExecutionManagerId::random();

        // Create a few tasks and terminate them immediately.
        for i in 0..NUM_TASKS {
            let tcb = build_single_task_tcb().await;
            register_task_in_pool(
                &mut pool,
                &tcb,
                TaskId::Index(i),
                i as TaskInstanceId,
                em_id,
                SystemTime::now(),
            )
            .await;
            tcb.cancel_non_terminal().await;
        }

        pool.run_gc_cycle_at(SystemTime::now())
            .await
            .expect("GC cycle should succeed");

        assert!(
            pool.instances.is_empty(),
            "all terminated entries should be removed, remaining: {}",
            pool.instances.len()
        );
        let messages = ready_queue_sender.sent_messages.lock().await.clone();
        assert!(
            messages.is_empty(),
            "terminated tasks should not be re-enqueued, got: {messages:?}"
        );
    }

    #[tokio::test]
    async fn gc_re_enqueues_all_soft_timed_out_tasks_and_keeps_them() {
        const NUM_TASKS: usize = 10;

        let ready_queue_sender = MockReadyQueueSender::default();
        let liveness_store = MockExecutionManagerLivenessManagement::default();
        let mut pool = build_test_pool(
            ready_queue_sender.clone(),
            liveness_store,
            Duration::from_mins(1),
        );
        let em_id = ExecutionManagerId::random();
        let gc_starting_time = SystemTime::now();
        // soft_timeout_ddl = registered_at + 100ms
        // deadline = now - 900ms
        let registered_at = gc_starting_time - Duration::from_secs(1);

        let mut expected_messages: Vec<ReadyMessage> = Vec::new();
        for i in 0..NUM_TASKS {
            let tcb = build_single_task_tcb().await;
            let job_id = register_task_in_pool(
                &mut pool,
                &tcb,
                TaskId::Index(i),
                i as TaskInstanceId,
                em_id,
                registered_at,
            )
            .await;
            expected_messages.push(ReadyMessage::Task(job_id, i));
        }

        pool.run_gc_cycle_at(gc_starting_time)
            .await
            .expect("GC cycle should succeed");

        assert_eq!(
            pool.instances.len(),
            NUM_TASKS,
            "soft-timed-out entries should remain in the pool"
        );
        for entry in &pool.instances {
            assert!(
                entry.soft_timeout_handled,
                "soft_timeout_handled should be set for entry {}",
                entry.metadata.task_instance_id
            );
        }
        let messages = ready_queue_sender.sent_messages.lock().await.clone();
        assert_eq!(messages.len(), expected_messages.len(), "got: {messages:?}");
        for expected in &expected_messages {
            assert!(
                messages.contains(expected),
                "missing re-enqueue {expected:?}, got: {messages:?}"
            );
        }
    }

    #[tokio::test]
    async fn gc_re_enqueues_and_removes_all_tasks_for_dead_em() {
        const NUM_TASKS: usize = 10;

        let ready_queue_sender = MockReadyQueueSender::default();
        let liveness_store = MockExecutionManagerLivenessManagement::default();
        let mut pool = build_test_pool(
            ready_queue_sender.clone(),
            liveness_store.clone(),
            Duration::from_mins(1),
        );
        let em_id = ExecutionManagerId::random();
        let now = SystemTime::now();

        let mut expected_messages: Vec<ReadyMessage> = Vec::new();
        for i in 0..NUM_TASKS {
            let tcb = build_single_task_tcb().await;
            let job_id = register_task_in_pool(
                &mut pool,
                &tcb,
                TaskId::Index(i),
                i as TaskInstanceId,
                em_id,
                now,
            )
            .await;
            expected_messages.push(ReadyMessage::Task(job_id, i));
        }

        liveness_store
            .dead_execution_managers
            .lock()
            .await
            .push(em_id);

        pool.run_gc_cycle_at(now)
            .await
            .expect("GC cycle should succeed");

        assert!(
            pool.instances.is_empty(),
            "all dead-EM entries should be removed, remaining: {}",
            pool.instances.len()
        );
        assert!(
            !pool.execution_manager_pool.contains(&em_id),
            "dead EM should be pruned from execution_manager_pool"
        );
        let messages = ready_queue_sender.sent_messages.lock().await.clone();
        assert_eq!(messages.len(), expected_messages.len(), "got: {messages:?}");
        for expected in &expected_messages {
            assert!(
                messages.contains(expected),
                "missing re-enqueue {expected:?}, got: {messages:?}"
            );
        }
    }

    #[tokio::test]
    async fn gc_removes_terminated_tasks_for_dead_em_without_re_enqueue() {
        const NUM_TASKS: usize = 10;

        let ready_queue_sender = MockReadyQueueSender::default();
        let liveness_store = MockExecutionManagerLivenessManagement::default();
        let mut pool = build_test_pool(
            ready_queue_sender.clone(),
            liveness_store.clone(),
            Duration::from_mins(1),
        );
        let em_id = ExecutionManagerId::random();
        let now = SystemTime::now();

        for i in 0..NUM_TASKS {
            let tcb = build_single_task_tcb().await;
            register_task_in_pool(
                &mut pool,
                &tcb,
                TaskId::Index(i),
                i as TaskInstanceId,
                em_id,
                now,
            )
            .await;
            tcb.cancel_non_terminal().await;
        }

        liveness_store
            .dead_execution_managers
            .lock()
            .await
            .push(em_id);

        pool.run_gc_cycle_at(now)
            .await
            .expect("GC cycle should succeed");

        assert!(
            pool.instances.is_empty(),
            "all entries should be removed, remaining: {}",
            pool.instances.len()
        );
        assert!(
            !pool.execution_manager_pool.contains(&em_id),
            "dead EM should be pruned from execution_manager_pool"
        );
        let messages = ready_queue_sender.sent_messages.lock().await.clone();
        assert!(
            messages.is_empty(),
            "terminated tasks should not be re-enqueued even with dead EM, got: {messages:?}"
        );
    }

    #[tokio::test]
    async fn gc_comprehensive() {
        // Layout (5 entries):
        //   index 0: alive EM, terminated -> removed, no re-enqueue
        //   index 1: alive EM, soft timed out -> kept, re-enqueued, flag set
        //   index 2: alive EM, healthy on-going -> kept, no re-enqueue
        //   index 3: dead EM, terminated -> removed, no re-enqueue (terminal wins)
        //   index 4: dead EM, on-going -> removed, re-enqueued
        let ready_queue_sender = MockReadyQueueSender::default();
        let liveness_store = MockExecutionManagerLivenessManagement::default();
        let mut pool = build_test_pool(
            ready_queue_sender.clone(),
            liveness_store.clone(),
            Duration::from_mins(1),
        );
        let alive_em = ExecutionManagerId::random();
        let dead_em = ExecutionManagerId::random();
        let now = SystemTime::now();
        // soft timeout deadline = now - 900ms
        let elapsed_registration = now - Duration::from_secs(1);
        // soft timeout deadline = now + 100ms (not yet elapsed)
        let fresh = now;

        let tcb0 = build_single_task_tcb().await;
        register_task_in_pool(&mut pool, &tcb0, TaskId::Index(0), 0, alive_em, fresh).await;
        tcb0.cancel_non_terminal().await;

        let tcb1 = build_single_task_tcb().await;
        let job_id_1 = register_task_in_pool(
            &mut pool,
            &tcb1,
            TaskId::Index(1),
            1,
            alive_em,
            elapsed_registration,
        )
        .await;

        let tcb2 = build_single_task_tcb().await;
        register_task_in_pool(&mut pool, &tcb2, TaskId::Index(2), 2, alive_em, fresh).await;

        let tcb3 = build_single_task_tcb().await;
        register_task_in_pool(&mut pool, &tcb3, TaskId::Index(3), 3, dead_em, fresh).await;
        tcb3.cancel_non_terminal().await;

        let tcb4 = build_single_task_tcb().await;
        let job_id_4 =
            register_task_in_pool(&mut pool, &tcb4, TaskId::Index(4), 4, dead_em, fresh).await;

        liveness_store
            .dead_execution_managers
            .lock()
            .await
            .push(dead_em);

        pool.run_gc_cycle_at(now)
            .await
            .expect("GC cycle should succeed");

        // Pool should retain entries 1 (soft-timed-out, kept) and 2 (healthy).
        let remaining_ids: HashSet<TaskInstanceId> = pool
            .instances
            .iter()
            .map(|entry| entry.metadata.task_instance_id)
            .collect();
        assert_eq!(remaining_ids, HashSet::from([1, 2]));

        let entry_1 = pool
            .instances
            .iter()
            .find(|entry| entry.metadata.task_instance_id == 1)
            .expect("entry 1 should still be in the pool");
        assert!(
            entry_1.soft_timeout_handled,
            "soft_timeout_handled should be set for entry 1"
        );

        let entry_2 = pool
            .instances
            .iter()
            .find(|entry| entry.metadata.task_instance_id == 2)
            .expect("entry 2 should still be in the pool");
        assert!(
            !entry_2.soft_timeout_handled,
            "soft_timeout_handled should not be set for healthy entry 2"
        );

        assert!(
            !pool.execution_manager_pool.contains(&dead_em),
            "dead EM should be pruned from execution_manager_pool"
        );
        assert!(
            pool.execution_manager_pool.contains(&alive_em),
            "alive EM should remain in execution_manager_pool"
        );

        let messages = ready_queue_sender.sent_messages.lock().await.clone();
        let expected = [
            ReadyMessage::Task(job_id_1, 1),
            ReadyMessage::Task(job_id_4, 4),
        ];
        assert_eq!(messages.len(), expected.len(), "got: {messages:?}");
        for msg in &expected {
            assert!(
                messages.contains(msg),
                "missing re-enqueue {msg:?}, got: {messages:?}"
            );
        }
    }
}
