use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use spider_core::{
    job::JobState,
    task::{TaskIndex, TaskState},
    types::{
        id::{JobId, ResourceGroupId, TaskInstanceId},
        io::{ExecutionContext, TaskOutput},
    },
};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    cache::{
        TaskId,
        error::{CacheError, InternalError, InternalError::UnexpectedJobState, StaleStateError},
        task::TaskGraph,
    },
    db::InternalJobOrchestration,
    ready_queue::ReadyQueueSender,
    task_instance_pool::TaskInstancePoolConnector,
};

/// A shareable control block for a job.
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
    /// * [`InternalError::TaskIndexOutOfBound`] if the task index is out of range.
    /// * [`InternalError::UndefinedCommitTask`] if the job has no commit task when requested for.
    /// * [`InternalError::UndefinedCleanupTask`] if the job has no cleanup task when requested for.
    /// * Forwards [`JobExecutionStateHandle::read_running`]'s return values on failure.
    /// * Forwards [`JobExecutionStateHandle::read_commit_ready`]'s return values on failure.
    /// * Forwards [`JobExecutionStateHandle::read_cleanup_ready`]'s return values on failure.
    /// * Forwards [`SharedTaskControlBlock::register_task_instance`]'s return values on failure.
    /// * Forwards [`SharedTerminationTaskControlBlock::register_task_instance`]'s return values on
    ///   failure.
    /// * Forwards [`TaskInstancePoolConnector::register_task_instance`]'s return values on failure.
    /// * Forwards [`TaskInstancePoolConnector::register_termination_task_instance`]'s return values
    ///   on failure.
    pub async fn create_task_instance(
        &self,
        task_id: TaskId,
    ) -> Result<ExecutionContext, CacheError> {
        let jcb = &self.inner;
        let execution_context = match task_id {
            TaskId::Index(task_index) => {
                let job = jcb.job_execution_state.read_running().await?;
                let tcb = job
                    .task_graph
                    .get_task_control_block(task_index)
                    .ok_or(InternalError::TaskIndexOutOfBound)?;
                drop(job);
                let task_instance_id = jcb
                    .task_instance_pool_connector
                    .get_next_available_task_instance_id();
                let execution_context = tcb.register_task_instance(task_instance_id).await?;
                jcb.task_instance_pool_connector
                    .register_task_instance(task_instance_id, tcb)
                    .await?;
                execution_context
            }

            TaskId::Commit => {
                let job = jcb.job_execution_state.read_commit_ready().await?;
                let commit_tcb = job
                    .task_graph
                    .get_commit_task_control_block()
                    .ok_or(InternalError::UndefinedCommitTask)?;
                drop(job);
                let task_instance_id = jcb
                    .task_instance_pool_connector
                    .get_next_available_task_instance_id();
                let (tdl_context, timeout_policy) =
                    commit_tcb.register_task_instance(task_instance_id).await?;
                jcb.task_instance_pool_connector
                    .register_termination_task_instance(task_instance_id, commit_tcb)
                    .await?;
                ExecutionContext {
                    task_instance_id,
                    tdl_context,
                    timeout_policy,
                    inputs: Vec::new(),
                }
            }

            TaskId::Cleanup => {
                let job = jcb.job_execution_state.read_cleanup_ready().await?;
                let cleanup_tcb = job
                    .task_graph
                    .get_cleanup_task_control_block()
                    .ok_or(InternalError::UndefinedCleanupTask)?;
                drop(job);
                let task_instance_id = jcb
                    .task_instance_pool_connector
                    .get_next_available_task_instance_id();
                let (tdl_context, timeout_policy) =
                    cleanup_tcb.register_task_instance(task_instance_id).await?;
                jcb.task_instance_pool_connector
                    .register_termination_task_instance(task_instance_id, cleanup_tcb)
                    .await?;
                ExecutionContext {
                    task_instance_id,
                    tdl_context,
                    timeout_policy,
                    inputs: Vec::new(),
                }
            }
        };

        Ok(execution_context)
    }

    /// Marks the task instance as succeeded.
    ///
    /// If all tasks succeed, commits the job outputs and transitions the job state.
    ///
    /// # Returns
    ///
    /// The current [`JobState`] after the operation on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskIndexOutOfBound`] if the task index is out of range.
    /// * [`InternalError::TaskGraphCorrupted`] if no incomplete tasks remain while new ready tasks
    ///   are generated.
    /// * [`InternalError::UndefinedCommitTask`] if the job should transition to commit-ready but
    ///   has no commit task.
    /// * Forwards [`JobExecutionStateHandle::read_running`]'s return values on failure.
    /// * Forwards [`SharedTaskControlBlock::succeed_task_instance`]'s return values on failure.
    /// * Forwards [`ReadyQueueSender::send_task_ready`]'s return values on failure.
    /// * Forwards [`ReadyQueueSender::send_commit_ready`]'s return values on failure.
    /// * Forwards [`SharedJobControlBlock::commit_outputs`]'s return values on failure.
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
            jcb.ready_queue_sender
                .send_task_ready(jcb.id, ready_task_indices)
                .await?;
            return Ok(job.state);
        }

        if num_incomplete_tasks != 0 {
            return Ok(job.state);
        }

        drop(job);
        let job_state = self.commit_outputs().await?;
        match job_state {
            JobState::CommitReady => {
                if !jcb.job_execution_state.has_commit_task().await {
                    return Err(InternalError::UndefinedCommitTask.into());
                }
                jcb.ready_queue_sender.send_commit_ready(jcb.id).await?;
            }
            JobState::Succeeded => {}
            other => unreachable!(
                "unexpected job state after committing job outputs: {:?}",
                other
            ),
        }
        Ok(job_state)
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
        jcb.db_connector
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
        jcb.db_connector
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
    /// The current [`JobState`] after the operation on success.
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
                    jcb.ready_queue_sender
                        .send_task_ready(jcb.id, vec![task_index])
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
                    jcb.ready_queue_sender.send_commit_ready(jcb.id).await?;
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
                    jcb.ready_queue_sender.send_cleanup_ready(jcb.id).await?;
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
        jcb.db_connector.fail(jcb.id, error_message).await?;
        job.state = JobState::Failed;
        drop(job);
        Ok(JobState::Failed)
    }

    /// Commits the job outputs to the database.
    ///
    /// Collects all task graph outputs and persists them via the DB connector.
    ///
    /// # Returns
    ///
    /// The new [`JobState`] after committing on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionStateHandle::write_running`]'s return values on failure.
    /// * Forwards [`OutputReader::read_as_task_output`]'s return values on failure.
    /// * Forwards [`InternalJobOrchestration::commit_outputs`]'s return values on failure.
    async fn commit_outputs(&self) -> Result<JobState, CacheError> {
        let mut job = self.inner.job_execution_state.write_running().await?;
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

        let job_state = self
            .inner
            .db_connector
            .commit_outputs(self.inner.id, job_outputs)
            .await?;
        job.state = job_state;
        drop(job);
        Ok(job_state)
    }
}

/// The control block for a job.
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
    _owner_id: ResourceGroupId,
    job_execution_state: JobExecutionStateHandle,
    ready_queue_sender: ReadyQueueSenderType,
    db_connector: DbConnectorType,
    task_instance_pool_connector: TaskInstancePoolConnectorType,
}

/// A concurrency-safe handle to a job’s execution state.
///
/// This type wraps [`JobExecutionState`] in a read-write lock and provides controlled access to it.
/// All accessors enforce state invariants by validating the underlying job state before returning a
/// read or write guard.
///
/// This ensures that callers can only observe or mutate the execution state when the job is in a
/// valid state for the requested operation.
struct JobExecutionStateHandle {
    inner: tokio::sync::RwLock<JobExecutionState>,
}

impl JobExecutionStateHandle {
    pub async fn has_commit_task(&self) -> bool {
        self.inner.read().await.task_graph.has_commit_task()
    }

    pub async fn _has_cleanup_task(&self) -> bool {
        self.inner.read().await.task_graph.has_cleanup_task()
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_running`]'s return values on failure.
    async fn read_running(&self) -> Result<RwLockReadGuard<'_, JobExecutionState>, CacheError> {
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
    async fn write_running(&self) -> Result<RwLockWriteGuard<'_, JobExecutionState>, CacheError> {
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
    ) -> Result<RwLockReadGuard<'_, JobExecutionState>, CacheError> {
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
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState>, CacheError> {
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
    ) -> Result<RwLockReadGuard<'_, JobExecutionState>, CacheError> {
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
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState>, CacheError> {
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
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState>, CacheError> {
        self.validate_and_write(JobExecutionState::ensure_non_terminated)
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
        validator: fn(&JobExecutionState) -> Result<(), CacheError>,
    ) -> Result<RwLockReadGuard<'_, JobExecutionState>, CacheError> {
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
        validator: fn(&JobExecutionState) -> Result<(), CacheError>,
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState>, CacheError> {
        let guard = self.inner.write().await;
        validator(&guard)?;
        Ok(guard)
    }
}

/// Represents the execution state of a job.
///
/// # Note
///
/// This struct doesn't provide synchronization for concurrent access to the underlying task graph.
struct JobExecutionState {
    state: JobState,
    task_graph: TaskGraph,
    num_incomplete_tasks: AtomicUsize,
}

impl JobExecutionState {
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
                expected: JobState::CommitReady,
            }
            .into());
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
}
