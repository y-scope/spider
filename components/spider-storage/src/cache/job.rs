use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use spider_core::{
    job::JobState,
    task::{TaskIndex, TaskState},
    types::{
        id::{ExecutionManagerId, JobId, ResourceGroupId, TaskInstanceId},
        io::{ExecutionContext, TaskOutput},
    },
};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    cache::{
        TaskId,
        error::{CacheError, InternalError, InternalError::UnexpectedJobState, StaleStateError},
        job_submission::ValidatedJobSubmission,
        task::TaskGraph,
    },
    db::InternalJobOrchestration,
    ready_queue::ReadyQueueSender,
    task_instance_pool::{TaskInstanceMetadata, TaskInstancePoolConnector},
};

/// A shareable control block for a job.
///
/// All mutable state, including the task graph, connectors, and queue sender, is held inside the
/// underlying [`JobExecutionState`] and protected by [`JobExecutionStateHandle`]'s read-write lock.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The type of the ready queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
#[derive(Clone)]
pub struct SharedJobControlBlock<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    inner:
        Arc<JobControlBlock<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>>,
}

impl<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> SharedJobControlBlock<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The created [`SharedJobControlBlock`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TaskGraph::create`]'s return values on failure.
    pub async fn create(
        id: JobId,
        owner_id: ResourceGroupId,
        job_submission: ValidatedJobSubmission,
        ready_queue_sender: ReadyQueueSenderType,
        db_connector: DbConnectorType,
        task_instance_pool_connector: TaskInstancePoolConnectorType,
    ) -> Result<Self, CacheError> {
        let num_tasks = job_submission.task_graph().get_num_tasks();
        let task_graph = TaskGraph::create(job_submission).await?;
        let job_execution_state = JobExecutionState {
            state: JobState::Ready,
            task_graph,
            num_incomplete_tasks: AtomicUsize::new(num_tasks),
            ready_queue_sender,
            db_connector,
            task_instance_pool_connector,
        };
        Ok(Self {
            inner: Arc::new(JobControlBlock {
                id,
                owner_id,
                job_execution_state: JobExecutionStateHandle {
                    inner: tokio::sync::RwLock::new(job_execution_state),
                },
            }),
        })
    }

    /// Returns the job ID.
    #[must_use]
    pub fn id(&self) -> JobId {
        self.inner.id
    }

    /// Starts the job.
    ///
    /// Any tasks in [`TaskState::Ready`] will be enqueued to the ready queue on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionStateHandle::write_ready`]'s return values on failure.
    /// * Forwards [`InternalJobOrchestration::start`]'s return values on failure.
    /// * Forwards [`ReadyQueueSender::send_task_ready`]'s return values on failure.
    pub async fn start(&self) -> Result<(), CacheError> {
        let jcb = &self.inner;
        let mut job = jcb.job_execution_state.write_ready().await?;
        job.db_connector.start(jcb.id).await?;
        job.state = JobState::Running;
        let ready_task_indices = job.task_graph.get_all_ready_task_indices().await;
        if ready_task_indices.is_empty() {
            return Err(InternalError::TaskGraphCorrupted(
                "initial task graph has no ready tasks".to_owned(),
            )
            .into());
        }

        // NOTE: This enqueue is safe because it happens inside the exclusive (write) lock of the
        // JCB. If it happens to travel fast enough to go into the scheduler and then the executor,
        // the request from the executor for registering task instances will be blocked until this
        // method returns.
        job.ready_queue_sender
            .send_task_ready(jcb.owner_id, jcb.id, ready_task_indices)
            .await?;
        drop(job);
        Ok(())
    }

    /// Resends all ready tasks to the ready queue.
    ///
    /// The method handles the following job states:
    ///
    /// * [`JobState::Running`] — all tasks in [`TaskState::Ready`] via
    ///   [`ReadyQueueSender::send_task_ready`].
    /// * [`JobState::CommitReady`] — the commit task via [`ReadyQueueSender::send_commit_ready`].
    /// * [`JobState::CleanupReady`] — the cleanup task via
    ///   [`ReadyQueueSender::send_cleanup_ready`].
    ///
    /// For other job states, this method is a no-op and returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ReadyQueueSender::send_task_ready`]'s return values on failure.
    /// * Forwards [`ReadyQueueSender::send_commit_ready`]'s return values on failure.
    /// * Forwards [`ReadyQueueSender::send_cleanup_ready`]'s return values on failure.
    pub async fn resend_ready_tasks(&self) -> Result<(), CacheError> {
        let jcb = &self.inner;

        if let Ok(job) = jcb.job_execution_state.read_running().await {
            let ready_task_indices = job.task_graph.get_all_ready_task_indices().await;
            if !ready_task_indices.is_empty() {
                job.ready_queue_sender
                    .send_task_ready(jcb.owner_id, jcb.id, ready_task_indices)
                    .await?;
            }
            return Ok(());
        }

        if let Ok(job) = jcb.job_execution_state.read_commit_ready().await {
            job.ready_queue_sender
                .send_commit_ready(jcb.owner_id, jcb.id)
                .await?;
            return Ok(());
        }

        if let Ok(job) = jcb.job_execution_state.read_cleanup_ready().await {
            job.ready_queue_sender
                .send_cleanup_ready(jcb.owner_id, jcb.id)
                .await?;
            return Ok(());
        }

        Ok(())
    }

    /// Creates a task instance for the given task and registers it in the task instance pool.
    ///
    /// # Returns
    ///
    /// The execution context for the created task instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::create_regular_task_instance`]'s return values on failure.
    /// * Forwards [`Self::create_commit_task_instance`]'s return values on failure.
    /// * Forwards [`Self::create_cleanup_task_instance`]'s return values on failure.
    pub async fn create_task_instance(
        &self,
        task_id: TaskId,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<ExecutionContext, CacheError> {
        let jcb = &self.inner;
        match task_id {
            TaskId::Index(task_index) => {
                Self::create_regular_task_instance(jcb, task_index, execution_manager_id).await
            }
            TaskId::Commit => Self::create_commit_task_instance(jcb, execution_manager_id).await,
            TaskId::Cleanup => Self::create_cleanup_task_instance(jcb, execution_manager_id).await,
        }
    }

    /// Marks the task instance as succeeded.
    ///
    /// If all tasks have succeeded, commits the job outputs, transitions the job state, and
    /// enqueues the commit task (if any) to the ready queue. Otherwise, if the completed task
    /// unblocks any child tasks, those child tasks are enqueued to the ready queue.
    ///
    /// # Returns
    ///
    /// The current [`JobState`] after the operation on success. Must be one of:
    ///
    /// * [`JobState::Running`]
    /// * [`JobState::CommitReady`]
    /// * [`JobState::Succeeded`]
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskIndexOutOfBound`] if the task index is out of range.
    /// * [`InternalError::TaskGraphCorrupted`] if no incomplete tasks remain while new ready tasks
    ///   are generated.
    /// * Forwards [`JobExecutionStateHandle::read_running`]'s return values on failure.
    /// * Forwards [`JobExecutionStateHandle::write_running`]'s return values on failure.
    /// * Forwards [`SharedTaskControlBlock::succeed_task_instance`]'s return values on failure.
    /// * Forwards [`ReadyQueueSender::send_task_ready`]'s return values on failure.
    /// * Forwards [`ReadyQueueSender::send_commit_ready`]'s return values on failure.
    /// * Forwards [`SharedJobControlBlock::commit_outputs`]'s return values on failure.
    /// * Forwards [`OutputReader::read_as_task_output`]'s return values on failure.
    /// * Forwards [`InternalJobOrchestration::commit_outputs`]'s return values on failure.
    pub async fn succeed_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_index: TaskIndex,
        task_outputs: Vec<TaskOutput>,
    ) -> Result<JobState, CacheError> {
        let jcb = &self.inner;
        let job = jcb.job_execution_state.read_running().await?;
        let tcb = job
            .task_graph
            .get_task_control_block(task_index)
            .ok_or(InternalError::TaskIndexOutOfBound)?;
        let ready_task_indices = tcb
            .succeed_task_instance(task_instance_id, task_outputs)
            .await?;
        // NOTE: `fetch_sub` returns the previous value, so the new count is the returned value
        // minus 1.
        let num_incomplete_tasks = job.num_incomplete_tasks.fetch_sub(1, Ordering::Relaxed) - 1;

        if !ready_task_indices.is_empty() {
            if num_incomplete_tasks == 0 {
                return Err(InternalError::TaskGraphCorrupted(
                    "no incomplete tasks while new ready task indices are generated".to_owned(),
                )
                .into());
            }
            job.ready_queue_sender
                .send_task_ready(jcb.owner_id, jcb.id, ready_task_indices)
                .await?;
            return Ok(job.state);
        }

        if num_incomplete_tasks != 0 {
            return Ok(job.state);
        }

        // Release the read lock prior to acquiring a write lock for committing job outputs.
        drop(job);
        let mut job = jcb.job_execution_state.write_running().await?;
        let mut job_outputs = Vec::new();
        for output_reader in job.task_graph.get_outputs() {
            let payload = output_reader
                .read()
                .await
                .as_ref()
                .ok_or(InternalError::TaskInputNotReady)?
                .clone();
            job_outputs.push(payload);
        }
        let has_commit_task = job.task_graph.has_commit_task();
        job.db_connector
            .commit_outputs(jcb.id, job_outputs, has_commit_task)
            .await?;
        job.state = if has_commit_task {
            JobState::CommitReady
        } else {
            JobState::Succeeded
        };
        if has_commit_task {
            job.ready_queue_sender
                .send_commit_ready(jcb.owner_id, jcb.id)
                .await?;
        }
        Ok(job.state)
    }

    /// Marks the commit task instance as succeeded and transitions the job to
    /// [`JobState::Succeeded`].
    ///
    /// # Returns
    ///
    /// [`JobState::Succeeded`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UndefinedCommitTask`] if the job has no commit task.
    /// * Forwards [`JobExecutionStateHandle::write_commit_ready`]'s return values on failure.
    /// * Forwards [`SharedTerminationTaskControlBlock::succeed_task_instance`]'s return values on
    ///   failure.
    /// * Forwards [`InternalJobOrchestration::set_state`]'s return values on failure.
    pub async fn succeed_commit_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, CacheError> {
        let jcb = &self.inner;
        let mut job = jcb.job_execution_state.write_commit_ready().await?;
        job.task_graph
            .get_commit_task_control_block()
            .ok_or(InternalError::UndefinedCommitTask)?
            .succeed_task_instance(task_instance_id)
            .await?;
        job.db_connector
            .set_state(jcb.id, JobState::Succeeded)
            .await?;
        job.state = JobState::Succeeded;
        drop(job);
        Ok(JobState::Succeeded)
    }

    /// Marks the cleanup task instance as succeeded and transitions the job to
    /// [`JobState::Cancelled`].
    ///
    /// # Returns
    ///
    /// [`JobState::Cancelled`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UndefinedCleanupTask`] if the job has no cleanup task.
    /// * Forwards [`JobExecutionStateHandle::write_cleanup_ready`]'s return values on failure.
    /// * Forwards [`SharedTerminationTaskControlBlock::succeed_task_instance`]'s return values on
    ///   failure.
    /// * Forwards [`InternalJobOrchestration::set_state`]'s return values on failure.
    pub async fn succeed_cleanup_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, CacheError> {
        let jcb = &self.inner;
        let mut job = jcb.job_execution_state.write_cleanup_ready().await?;
        job.task_graph
            .get_cleanup_task_control_block()
            .ok_or(InternalError::UndefinedCleanupTask)?
            .succeed_task_instance(task_instance_id)
            .await?;
        job.db_connector
            .set_state(jcb.id, JobState::Cancelled)
            .await?;
        job.state = JobState::Cancelled;
        drop(job);
        Ok(JobState::Cancelled)
    }

    /// Marks a task instance as failed.
    ///
    /// If the task has remaining retries, it is re-enqueued to the ready queue. Otherwise, the job
    /// transitions to [`JobState::Failed`].
    ///
    /// # Returns
    ///
    /// The current [`JobState`] after the operation on success. Must be one of:
    ///
    /// * [`JobState::Running`]
    /// * [`JobState::Failed`]
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskIndexOutOfBound`] if the task index is out of range.
    /// * [`InternalError::UndefinedCommitTask`] if the job has no commit task.
    /// * [`InternalError::UndefinedCleanupTask`] if the job has no cleanup task.
    /// * [`InternalError::UnexpectedJobTermination`] if the job terminated in an unexpected state.
    /// * Forwards [`JobExecutionStateHandle::read_running`]'s return values on failure.
    /// * Forwards [`JobExecutionStateHandle::read_commit_ready`]'s return values on failure.
    /// * Forwards [`JobExecutionStateHandle::read_cleanup_ready`]'s return values on failure.
    /// * Forwards [`JobExecutionStateHandle::write_non_terminated`]'s return values on failure.
    /// * Forwards [`SharedTaskControlBlock::fail_task_instance`]'s return values on failure.
    /// * Forwards [`SharedTerminationTaskControlBlock::fail_task_instance`]'s return values on
    ///   failure.
    /// * Forwards [`ReadyQueueSender::send_task_ready`]'s return values on failure.
    /// * Forwards [`ReadyQueueSender::send_commit_ready`]'s return values on failure.
    /// * Forwards [`ReadyQueueSender::send_cleanup_ready`]'s return values on failure.
    /// * Forwards [`InternalJobOrchestration::fail`]'s return values on failure.
    pub async fn fail_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_id: TaskId,
        error_message: String,
    ) -> Result<JobState, CacheError> {
        let jcb = &self.inner;
        match task_id {
            TaskId::Index(task_index) => {
                let job = jcb.job_execution_state.read_running().await?;
                let task_state = job
                    .task_graph
                    .get_task_control_block(task_index)
                    .ok_or(InternalError::TaskIndexOutOfBound)?
                    .fail_task_instance(task_instance_id, error_message.clone())
                    .await?;
                if matches!(task_state, TaskState::Ready | TaskState::Running) {
                    job.ready_queue_sender
                        .send_task_ready(jcb.owner_id, jcb.id, vec![task_index])
                        .await?;
                    return Ok(job.state);
                }
            }
            TaskId::Commit => {
                let job = jcb.job_execution_state.read_commit_ready().await?;
                let task_state = job
                    .task_graph
                    .get_commit_task_control_block()
                    .ok_or(InternalError::UndefinedCommitTask)?
                    .fail_task_instance(task_instance_id, error_message.clone())
                    .await?;
                if matches!(task_state, TaskState::Ready | TaskState::Running) {
                    job.ready_queue_sender
                        .send_commit_ready(jcb.owner_id, jcb.id)
                        .await?;
                    return Ok(job.state);
                }
            }
            TaskId::Cleanup => {
                let job = jcb.job_execution_state.read_cleanup_ready().await?;
                let task_state = job
                    .task_graph
                    .get_cleanup_task_control_block()
                    .ok_or(InternalError::UndefinedCleanupTask)?
                    .fail_task_instance(task_instance_id, error_message.clone())
                    .await?;
                if matches!(task_state, TaskState::Ready | TaskState::Running) {
                    job.ready_queue_sender
                        .send_cleanup_ready(jcb.owner_id, jcb.id)
                        .await?;
                    return Ok(job.state);
                }
            }
        }

        let mut job = jcb
            .job_execution_state
            .write_non_terminated()
            .await
            .map_err(|e| match &e {
                CacheError::StaleState(StaleStateError::JobAlreadyTerminated(state)) => {
                    if *state == JobState::Failed {
                        return e;
                    }
                    InternalError::UnexpectedJobTermination.into()
                }
                _ => InternalError::UnexpectedJobTermination.into(),
            })?;
        job.db_connector.fail(jcb.id, error_message).await?;
        job.state = JobState::Failed;
        drop(job);
        Ok(JobState::Failed)
    }

    /// Cancels the job and enqueues the cleanup task (if any).
    ///
    /// # Returns
    ///
    /// The current [`JobState`] after the cancellation operation on success. Must be one of:
    ///
    /// * [`JobState::CleanupReady`]
    /// * [`JobState::Cancelled`]
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionStateHandle::write_cancellable`]'s return values on failure.
    /// * Forwards [`InternalJobOrchestration::cancel`]'s return values on failure.
    /// * Forwards [`ReadyQueueSender::send_cleanup_ready`]'s return values on failure.
    pub async fn cancel(&self) -> Result<JobState, CacheError> {
        let jcb = &self.inner;
        let mut job = jcb.job_execution_state.write_cancellable().await?;
        let has_cleanup_task = job.task_graph.has_cleanup_task();
        job.db_connector.cancel(jcb.id, has_cleanup_task).await?;
        job.state = if has_cleanup_task {
            JobState::CleanupReady
        } else {
            JobState::Cancelled
        };

        job.task_graph.cancel_non_terminal().await;
        if has_cleanup_task {
            job.ready_queue_sender
                .send_cleanup_ready(jcb.owner_id, jcb.id)
                .await?;
        }
        Ok(job.state)
    }

    /// Creates a task instance for a regular task in the job and registers it in the task instance
    /// pool.
    ///
    /// # Returns
    ///
    /// The execution context for the created task instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskIndexOutOfBound`] if the task index is out of range.
    /// * Forwards [`JobExecutionStateHandle::read_running`]'s return values on failure.
    /// * Forwards [`SharedTaskControlBlock::register_task_instance`]'s return values on failure.
    /// * Forwards [`TaskInstancePoolConnector::register_task_instance`]'s return values on failure.
    async fn create_regular_task_instance(
        jcb: &JobControlBlock<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
        task_index: TaskIndex,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<ExecutionContext, CacheError> {
        let job = jcb.job_execution_state.read_running().await?;
        let tcb = job
            .task_graph
            .get_task_control_block(task_index)
            .ok_or(InternalError::TaskIndexOutOfBound)?;
        let task_instance_id = job
            .task_instance_pool_connector
            .get_next_available_task_instance_id();
        let execution_context = tcb.register_task_instance(task_instance_id).await?;
        let registration = TaskInstanceMetadata {
            resource_group_id: jcb.owner_id,
            job_id: jcb.id,
            task_id: TaskId::Index(task_index),
            task_instance_id,
            execution_manager_id,
            soft_timeout_ddl: SystemTime::now().checked_add(Duration::from_millis(
                execution_context.timeout_policy.soft_timeout_ms,
            )),
        };
        job.task_instance_pool_connector
            .register_task_instance(tcb.clone(), registration)
            .await?;

        // The lock is intentionally held until just before return so all TCB accesses
        // observe a consistent state within the lock's scope.
        drop(job);
        Ok(execution_context)
    }

    /// Creates a task instance for the commit task and registers it in the task instance pool.
    ///
    /// # Returns
    ///
    /// The execution context for the created task instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UndefinedCommitTask`] if the job has no commit task.
    /// * Forwards [`JobExecutionStateHandle::read_commit_ready`]'s return values on failure.
    /// * Forwards [`SharedTerminationTaskControlBlock::register_task_instance`]'s return values on
    ///   failure.
    /// * Forwards [`TaskInstancePoolConnector::register_termination_task_instance`]'s return values
    ///   on failure.
    async fn create_commit_task_instance(
        jcb: &JobControlBlock<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<ExecutionContext, CacheError> {
        let job = jcb.job_execution_state.read_commit_ready().await?;
        let commit_tcb = job
            .task_graph
            .get_commit_task_control_block()
            .ok_or(InternalError::UndefinedCommitTask)?;
        let task_instance_id = job
            .task_instance_pool_connector
            .get_next_available_task_instance_id();
        let (tdl_context, timeout_policy) =
            commit_tcb.register_task_instance(task_instance_id).await?;
        let registration = TaskInstanceMetadata {
            resource_group_id: jcb.owner_id,
            job_id: jcb.id,
            task_id: TaskId::Commit,
            task_instance_id,
            execution_manager_id,
            soft_timeout_ddl: SystemTime::now()
                .checked_add(Duration::from_millis(timeout_policy.soft_timeout_ms)),
        };
        job.task_instance_pool_connector
            .register_termination_task_instance(commit_tcb.clone(), registration)
            .await?;

        drop(job);
        Ok(ExecutionContext {
            task_instance_id,
            tdl_context,
            timeout_policy,
            inputs: Vec::new(),
        })
    }

    /// Creates a task instance for the cleanup task and registers it in the task instance pool.
    ///
    /// # Returns
    ///
    /// The execution context for the created task instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UndefinedCleanupTask`] if the job has no cleanup task.
    /// * Forwards [`JobExecutionStateHandle::read_cleanup_ready`]'s return values on failure.
    /// * Forwards [`SharedTerminationTaskControlBlock::register_task_instance`]'s return values on
    ///   failure.
    /// * Forwards [`TaskInstancePoolConnector::register_termination_task_instance`]'s return values
    ///   on failure.
    async fn create_cleanup_task_instance(
        jcb: &JobControlBlock<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<ExecutionContext, CacheError> {
        let job = jcb.job_execution_state.read_cleanup_ready().await?;
        let cleanup_tcb = job
            .task_graph
            .get_cleanup_task_control_block()
            .ok_or(InternalError::UndefinedCleanupTask)?;
        let task_instance_id = job
            .task_instance_pool_connector
            .get_next_available_task_instance_id();
        let (tdl_context, timeout_policy) =
            cleanup_tcb.register_task_instance(task_instance_id).await?;
        let registration = TaskInstanceMetadata {
            resource_group_id: jcb.owner_id,
            job_id: jcb.id,
            task_id: TaskId::Cleanup,
            task_instance_id,
            execution_manager_id,
            soft_timeout_ddl: SystemTime::now()
                .checked_add(Duration::from_millis(timeout_policy.soft_timeout_ms)),
        };
        job.task_instance_pool_connector
            .register_termination_task_instance(cleanup_tcb.clone(), registration)
            .await?;

        drop(job);
        Ok(ExecutionContext {
            task_instance_id,
            tdl_context,
            timeout_policy,
            inputs: Vec::new(),
        })
    }
}

/// The control block for a job.
///
/// This struct holds the immutable identity of a job and a handle to its execution state. All
/// mutable state and connectors live inside [`JobExecutionState`] and are protected by the
/// read-write lock in [`JobExecutionStateHandle`].
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The type of the ready queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
struct JobControlBlock<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    id: JobId,
    owner_id: ResourceGroupId,
    job_execution_state: JobExecutionStateHandle<
        ReadyQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    >,
}

/// A concurrency-safe handle to a job's execution state.
///
/// This type wraps [`JobExecutionState`] in a read-write lock and provides controlled access to it.
/// All accessors enforce state invariants by validating the underlying job state before returning a
/// read or write guard.
///
/// # Type Parameters
///
/// The type parameters are forwarded directly to [`JobExecutionState`] in the same declaration
/// order. Single-character names are used to:
///
/// * Reduce verbosity while preserving consistency with the underlying type.
/// * Avoid formatting issues, as `rustfmt` does not handle line wrapping well when using more
///   descriptive type parameter names in this particular struct.
struct JobExecutionStateHandle<
    R: ReadyQueueSender,
    D: InternalJobOrchestration,
    T: TaskInstancePoolConnector,
> {
    inner: tokio::sync::RwLock<JobExecutionState<R, D, T>>,
}

impl<R: ReadyQueueSender, D: InternalJobOrchestration, T: TaskInstancePoolConnector>
    JobExecutionStateHandle<R, D, T>
{
    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_running`]'s return values on failure.
    async fn read_running(
        &self,
    ) -> Result<RwLockReadGuard<'_, JobExecutionState<R, D, T>>, CacheError> {
        self.validate_and_read(JobExecutionState::ensure_running)
            .await
    }

    /// # Returns
    ///
    /// A writer guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_running`]'s return values on failure.
    async fn write_running(
        &self,
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState<R, D, T>>, CacheError> {
        self.validate_and_write(JobExecutionState::ensure_running)
            .await
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_commit_ready`]'s return values on failure.
    async fn read_commit_ready(
        &self,
    ) -> Result<RwLockReadGuard<'_, JobExecutionState<R, D, T>>, CacheError> {
        self.validate_and_read(JobExecutionState::ensure_commit_ready)
            .await
    }

    /// # Returns
    ///
    /// A writer guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_commit_ready`]'s return values on failure.
    async fn write_commit_ready(
        &self,
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState<R, D, T>>, CacheError> {
        self.validate_and_write(JobExecutionState::ensure_commit_ready)
            .await
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_cleanup_ready`]'s return values on failure.
    async fn read_cleanup_ready(
        &self,
    ) -> Result<RwLockReadGuard<'_, JobExecutionState<R, D, T>>, CacheError> {
        self.validate_and_read(JobExecutionState::ensure_cleanup_ready)
            .await
    }

    /// # Returns
    ///
    /// A writer guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_cleanup_ready`]'s return values on failure.
    async fn write_cleanup_ready(
        &self,
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState<R, D, T>>, CacheError> {
        self.validate_and_write(JobExecutionState::ensure_cleanup_ready)
            .await
    }

    /// # Returns
    ///
    /// A writer guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_non_terminated`]'s return values on failure.
    async fn write_non_terminated(
        &self,
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState<R, D, T>>, CacheError> {
        self.validate_and_write(JobExecutionState::ensure_non_terminated)
            .await
    }

    /// # Returns
    ///
    /// A writer guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_ready`]'s return values on failure.
    async fn write_ready(
        &self,
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState<R, D, T>>, CacheError> {
        self.validate_and_write(JobExecutionState::ensure_ready)
            .await
    }

    /// # Returns
    ///
    /// A writer guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_cancellable`]'s return values on failure.
    async fn write_cancellable(
        &self,
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState<R, D, T>>, CacheError> {
        self.validate_and_write(JobExecutionState::ensure_cancellable)
            .await
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards `validator`'s return values on failure.
    async fn validate_and_read(
        &self,
        validator: fn(&JobExecutionState<R, D, T>) -> Result<(), CacheError>,
    ) -> Result<RwLockReadGuard<'_, JobExecutionState<R, D, T>>, CacheError> {
        let guard = self.inner.read().await;
        validator(&guard)?;
        Ok(guard)
    }

    /// # Returns
    ///
    /// A writer guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards `validator`'s return values on failure.
    async fn validate_and_write(
        &self,
        validator: fn(&JobExecutionState<R, D, T>) -> Result<(), CacheError>,
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState<R, D, T>>, CacheError> {
        let guard = self.inner.write().await;
        validator(&guard)?;
        Ok(guard)
    }
}

/// Represents the execution state of a job.
///
/// This struct holds all mutable job state, including the task graph, connectors, and queue sender,
/// so that concurrent access is synchronized through [`JobExecutionStateHandle`]'s read-write lock.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The type of the ready queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
struct JobExecutionState<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    state: JobState,
    task_graph: TaskGraph,
    num_incomplete_tasks: AtomicUsize,
    ready_queue_sender: ReadyQueueSenderType,
    db_connector: DbConnectorType,
    task_instance_pool_connector: TaskInstancePoolConnectorType,
}

impl<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> JobExecutionState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    /// Ensures that the job is currently in the [`JobState::Running`] state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::JobNotStarted`] if the job hasn't been started yet.
    /// * [`StaleStateError::JobNoLongerRunning`] if the job is no longer running.
    fn ensure_running(&self) -> Result<(), CacheError> {
        if !self.state.is_running() {
            if matches!(self.state, JobState::Ready) {
                return Err(InternalError::JobNotStarted.into());
            }
            return Err(StaleStateError::JobNoLongerRunning.into());
        }
        Ok(())
    }

    /// Ensures that the job is currently in the [`JobState::CommitReady`] state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UnexpectedJobState`] if the job is in an unexpected state.
    /// * [`StaleStateError::JobNoLongerCommitReady`] if the job is no longer commit-ready.
    fn ensure_commit_ready(&self) -> Result<(), CacheError> {
        if !matches!(self.state, JobState::CommitReady) {
            if self.state.is_terminal() || matches!(self.state, JobState::CleanupReady) {
                return Err(StaleStateError::JobNoLongerCommitReady.into());
            }
            return Err(UnexpectedJobState {
                current: self.state,
                expected: JobState::CommitReady,
            }
            .into());
        }
        Ok(())
    }

    /// Ensures that the job is currently in the [`JobState::CleanupReady`] state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UnexpectedJobState`] if the job is in an unexpected state.
    /// * [`StaleStateError::JobNoLongerCommitReady`] if the job is no longer cleanup-ready.
    fn ensure_cleanup_ready(&self) -> Result<(), CacheError> {
        if !matches!(self.state, JobState::CleanupReady) {
            if self.state.is_terminal() {
                return Err(StaleStateError::JobNoLongerCleanupReady.into());
            }
            return Err(UnexpectedJobState {
                current: self.state,
                expected: JobState::CleanupReady,
            }
            .into());
        }
        Ok(())
    }

    /// Ensures that the job is currently in a cancellable state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::JobCancellationAlreadyRequested`] if job cancellation has already been
    ///   requested.
    /// * [`StaleStateError::JobAlreadyCancelled`] if the job is already been cancelled.
    /// * [`StaleStateError::JobAlreadyTerminated`] if the job has already terminated.
    fn ensure_cancellable(&self) -> Result<(), CacheError> {
        if matches!(self.state, JobState::CleanupReady) {
            return Err(StaleStateError::JobCancellationAlreadyRequested.into());
        }
        if matches!(self.state, JobState::Cancelled) {
            return Err(StaleStateError::JobAlreadyCancelled.into());
        }
        if self.state.is_terminal() {
            return Err(StaleStateError::JobAlreadyTerminated(self.state).into());
        }
        Ok(())
    }

    /// Ensures that the job is currently in a non-terminated state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::JobAlreadyTerminated`] if the job is already terminated.
    fn ensure_non_terminated(&self) -> Result<(), CacheError> {
        if self.state.is_terminal() {
            return Err(StaleStateError::JobAlreadyTerminated(self.state).into());
        }
        Ok(())
    }

    /// Ensures that the job is currently in [`JobState::Ready`] state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::JobAlreadyStarted`] if the job has already started.
    fn ensure_ready(&self) -> Result<(), CacheError> {
        if !matches!(self.state, JobState::Ready) {
            return Err(StaleStateError::JobAlreadyStarted.into());
        }
        Ok(())
    }
}
