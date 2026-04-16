use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::SystemTime,
};

use spider_core::{
    job::JobState,
    task::{TaskGraph as SubmittedTaskGraph, TaskIndex, TaskState},
    types::{
        id::{JobId, ResourceGroupId, TaskInstanceId, WorkerId},
        io::{ExecutionContext, TaskInput, TaskOutput},
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
    task_instance_pool::{TaskInstancePoolConnector, TaskInstanceRecord},
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
    /// * [`InternalError::TaskGraphCorrupted`] if the given task graph doesn't contain any tasks.
    ///   The current version of JCB requires the job contains at least one task.
    /// * Forwards [`TaskGraph::create`]'s return values on failure.
    pub async fn create(
        id: JobId,
        owner_id: ResourceGroupId,
        submitted_task_graph: &SubmittedTaskGraph,
        inputs: Vec<TaskInput>,
        ready_queue_sender: ReadyQueueSenderType,
        db_connector: DbConnectorType,
        task_instance_pool_connector: TaskInstancePoolConnectorType,
    ) -> Result<Self, CacheError> {
        let num_tasks = submitted_task_graph.get_num_tasks();
        if 0 == num_tasks {
            return Err(InternalError::TaskGraphCorrupted(
                "task graph with no task is unsupported".to_owned(),
            )
            .into());
        }

        let task_graph = TaskGraph::create(submitted_task_graph, inputs).await?;
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
                _owner_id: owner_id,
                job_execution_state: JobExecutionStateHandle {
                    inner: tokio::sync::RwLock::new(job_execution_state),
                },
            }),
        })
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
            .send_task_ready(jcb.id, ready_task_indices)
            .await?;
        drop(job);
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
        worker_id: WorkerId,
    ) -> Result<ExecutionContext, CacheError> {
        let jcb = &self.inner;
        match task_id {
            TaskId::Index(task_index) => {
                let job = jcb.job_execution_state.read_running().await?;
                let tcb = job
                    .task_graph
                    .get_task_control_block(task_index)
                    .ok_or(InternalError::TaskIndexOutOfBound)?;
                let task_instance_id = job
                    .task_instance_pool_connector
                    .get_next_available_task_instance_id();
                let execution_context = tcb.register_task_instance(task_instance_id).await?;
                let registration = TaskInstanceRecord {
                    job_id: jcb.id,
                    task_id: TaskId::Index(task_index),
                    task_instance_id,
                    worker_id,
                    registered_at: SystemTime::now(),
                    timeout_policy: execution_context.timeout_policy.clone(),
                };
                if let Err(error) = job
                    .task_instance_pool_connector
                    .register_task_instance(tcb.clone(), registration)
                    .await
                {
                    let _ = tcb.force_remove_task_instance(task_instance_id).await;
                    return Err(error.into());
                }

                // The lock is intentionally held until just before return so all TCB accesses
                // observe a consistent state within the lock's scope.
                drop(job);
                Ok(execution_context)
            }

            TaskId::Commit => {
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
                let registration = TaskInstanceRecord {
                    job_id: jcb.id,
                    task_id: TaskId::Commit,
                    task_instance_id,
                    worker_id,
                    registered_at: SystemTime::now(),
                    timeout_policy: timeout_policy.clone(),
                };
                if let Err(error) = job
                    .task_instance_pool_connector
                    .register_termination_task_instance(commit_tcb.clone(), registration)
                    .await
                {
                    let _ = commit_tcb
                        .force_remove_task_instance(task_instance_id)
                        .await;
                    return Err(error.into());
                }

                // The lock is intentionally held until just before return so all TCB accesses
                // observe a consistent state within the lock's scope.
                drop(job);
                Ok(ExecutionContext {
                    task_instance_id,
                    tdl_context,
                    timeout_policy,
                    inputs: Vec::new(),
                })
            }

            TaskId::Cleanup => {
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
                let registration = TaskInstanceRecord {
                    job_id: jcb.id,
                    task_id: TaskId::Cleanup,
                    task_instance_id,
                    worker_id,
                    registered_at: SystemTime::now(),
                    timeout_policy: timeout_policy.clone(),
                };
                if let Err(error) = job
                    .task_instance_pool_connector
                    .register_termination_task_instance(cleanup_tcb.clone(), registration)
                    .await
                {
                    let _ = cleanup_tcb
                        .force_remove_task_instance(task_instance_id)
                        .await;
                    return Err(error.into());
                }

                // The lock is intentionally held until just before return so all TCB accesses
                // observe a consistent state within the lock's scope.
                drop(job);
                Ok(ExecutionContext {
                    task_instance_id,
                    tdl_context,
                    timeout_policy,
                    inputs: Vec::new(),
                })
            }
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
        job.task_instance_pool_connector
            .unregister_running_task_instance(task_instance_id)
            .await?;
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
                .send_task_ready(jcb.id, ready_task_indices)
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
            job.ready_queue_sender.send_commit_ready(jcb.id).await?;
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
        job.task_instance_pool_connector
            .unregister_running_task_instance(task_instance_id)
            .await?;
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
        job.task_instance_pool_connector
            .unregister_running_task_instance(task_instance_id)
            .await?;
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
                job.task_instance_pool_connector
                    .unregister_running_task_instance(task_instance_id)
                    .await?;
                let task_state = job
                    .task_graph
                    .get_task_control_block(task_index)
                    .ok_or(InternalError::TaskIndexOutOfBound)?
                    .fail_task_instance(task_instance_id, error_message.clone())
                    .await?;
                if matches!(task_state, TaskState::Ready | TaskState::Running) {
                    job.ready_queue_sender
                        .send_task_ready(jcb.id, vec![task_index])
                        .await?;
                    return Ok(job.state);
                }
            }
            TaskId::Commit => {
                let job = jcb.job_execution_state.read_commit_ready().await?;
                job.task_instance_pool_connector
                    .unregister_running_task_instance(task_instance_id)
                    .await?;
                let task_state = job
                    .task_graph
                    .get_commit_task_control_block()
                    .ok_or(InternalError::UndefinedCommitTask)?
                    .fail_task_instance(task_instance_id, error_message.clone())
                    .await?;
                if matches!(task_state, TaskState::Ready | TaskState::Running) {
                    job.ready_queue_sender.send_commit_ready(jcb.id).await?;
                    return Ok(job.state);
                }
            }
            TaskId::Cleanup => {
                let job = jcb.job_execution_state.read_cleanup_ready().await?;
                job.task_instance_pool_connector
                    .unregister_running_task_instance(task_instance_id)
                    .await?;
                let task_state = job
                    .task_graph
                    .get_cleanup_task_control_block()
                    .ok_or(InternalError::UndefinedCleanupTask)?
                    .fail_task_instance(task_instance_id, error_message.clone())
                    .await?;
                if matches!(task_state, TaskState::Ready | TaskState::Running) {
                    job.ready_queue_sender.send_cleanup_ready(jcb.id).await?;
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

    /// Forcefully removes a task instance from the matching task control block.
    ///
    /// # Returns
    ///
    /// `true` if the task instance was live and removed from the matching TCB, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskIndexOutOfBound`] if the task index is out of range.
    /// * [`InternalError::UndefinedCommitTask`] if the job has no commit task.
    /// * [`InternalError::UndefinedCleanupTask`] if the job has no cleanup task.
    /// * Forwards the relevant job-state guard errors on failure.
    pub async fn force_remove_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_id: TaskId,
    ) -> Result<bool, CacheError> {
        let jcb = &self.inner;
        match task_id {
            TaskId::Index(task_index) => {
                let job = jcb.job_execution_state.read_running().await?;
                let removed = job
                    .task_graph
                    .get_task_control_block(task_index)
                    .ok_or(InternalError::TaskIndexOutOfBound)?
                    .force_remove_task_instance(task_instance_id)
                    .await;
                Ok(removed)
            }
            TaskId::Commit => {
                let job = jcb.job_execution_state.read_commit_ready().await?;
                let removed = job
                    .task_graph
                    .get_commit_task_control_block()
                    .ok_or(InternalError::UndefinedCommitTask)?
                    .force_remove_task_instance(task_instance_id)
                    .await;
                Ok(removed)
            }
            TaskId::Cleanup => {
                let job = jcb.job_execution_state.read_cleanup_ready().await?;
                let removed = job
                    .task_graph
                    .get_cleanup_task_control_block()
                    .ok_or(InternalError::UndefinedCleanupTask)?
                    .force_remove_task_instance(task_instance_id)
                    .await;
                Ok(removed)
            }
        }
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
            job.ready_queue_sender.send_cleanup_ready(jcb.id).await?;
        }
        Ok(job.state)
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
    _owner_id: ResourceGroupId,
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

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };

    use async_trait::async_trait;
    use spider_core::{
        task::{
            DataTypeDescriptor, ExecutionPolicy, TaskDescriptor, TaskGraph as SubmittedTaskGraph,
            TdlContext, TerminationTaskDescriptor, ValueTypeDescriptor,
        },
        types::{
            id::{ResourceGroupId, WorkerId},
            io::TaskInput,
        },
    };

    use super::*;
    use crate::{db::DbError, task_instance_pool::TaskInstanceRecord};

    #[derive(Clone, Default)]
    struct NoopReadyQueueSender {}

    #[async_trait]
    impl ReadyQueueSender for NoopReadyQueueSender {
        async fn send_task_ready(
            &self,
            _job_id: JobId,
            _task_indices: Vec<TaskIndex>,
        ) -> Result<(), InternalError> {
            Ok(())
        }

        async fn send_commit_ready(&self, _job_id: JobId) -> Result<(), InternalError> {
            Ok(())
        }

        async fn send_cleanup_ready(&self, _job_id: JobId) -> Result<(), InternalError> {
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct NoopDbConnector {}

    #[async_trait]
    impl InternalJobOrchestration for NoopDbConnector {
        async fn start(&self, _job_id: JobId) -> Result<(), DbError> {
            Ok(())
        }

        async fn set_state(&self, _job_id: JobId, _state: JobState) -> Result<(), DbError> {
            Ok(())
        }

        async fn commit_outputs(
            &self,
            _job_id: JobId,
            _job_outputs: Vec<TaskOutput>,
            _has_commit_task: bool,
        ) -> Result<(), DbError> {
            Ok(())
        }

        async fn cancel(&self, _job_id: JobId, _has_cleanup_task: bool) -> Result<(), DbError> {
            Ok(())
        }

        async fn fail(&self, _job_id: JobId, _error_message: String) -> Result<(), DbError> {
            Ok(())
        }

        async fn delete_expired_terminated_jobs(
            &self,
            _expire_after_sec: u64,
        ) -> Result<Vec<JobId>, DbError> {
            Ok(Vec::new())
        }
    }

    #[derive(Clone, Default)]
    struct RecordingTaskInstancePool {
        next_id: Arc<AtomicU64>,
        fail_next_registration: Arc<AtomicBool>,
        registered: Arc<Mutex<Vec<TaskInstanceRecord>>>,
        unregistered: Arc<Mutex<Vec<TaskInstanceId>>>,
    }

    impl RecordingTaskInstancePool {
        fn fail_next_registration(&self) {
            self.fail_next_registration.store(true, Ordering::Relaxed);
        }

        fn take_registered(&self) -> Vec<TaskInstanceRecord> {
            std::mem::take(
                &mut *self
                    .registered
                    .lock()
                    .expect("registered mutex should not be poisoned"),
            )
        }

        fn take_unregistered(&self) -> Vec<TaskInstanceId> {
            std::mem::take(
                &mut *self
                    .unregistered
                    .lock()
                    .expect("unregistered mutex should not be poisoned"),
            )
        }
    }

    #[async_trait]
    impl TaskInstancePoolConnector for RecordingTaskInstancePool {
        fn get_next_available_task_instance_id(&self) -> TaskInstanceId {
            self.next_id.fetch_add(1, Ordering::Relaxed) + 1
        }

        async fn register_task_instance(
            &self,
            _tcb: crate::cache::task::SharedTaskControlBlock,
            registration: TaskInstanceRecord,
        ) -> Result<(), InternalError> {
            if self.fail_next_registration.swap(false, Ordering::Relaxed) {
                return Err(InternalError::TaskInstancePoolCorrupted(
                    "injected registration failure".to_owned(),
                ));
            }
            self.registered
                .lock()
                .expect("registered mutex should not be poisoned")
                .push(registration);
            Ok(())
        }

        async fn register_termination_task_instance(
            &self,
            _termination_tcb: crate::cache::task::SharedTerminationTaskControlBlock,
            registration: TaskInstanceRecord,
        ) -> Result<(), InternalError> {
            if self.fail_next_registration.swap(false, Ordering::Relaxed) {
                return Err(InternalError::TaskInstancePoolCorrupted(
                    "injected registration failure".to_owned(),
                ));
            }
            self.registered
                .lock()
                .expect("registered mutex should not be poisoned")
                .push(registration);
            Ok(())
        }

        async fn unregister_running_task_instance(
            &self,
            task_instance_id: TaskInstanceId,
        ) -> Result<(), InternalError> {
            self.unregistered
                .lock()
                .expect("unregistered mutex should not be poisoned")
                .push(task_instance_id);
            Ok(())
        }

        async fn drain_worker_task_instances(
            &self,
            _worker_id: WorkerId,
        ) -> Result<Vec<TaskInstanceRecord>, InternalError> {
            Ok(Vec::new())
        }
    }

    type TestJcb =
        SharedJobControlBlock<NoopReadyQueueSender, NoopDbConnector, RecordingTaskInstancePool>;

    fn build_task_graph(
        with_commit: bool,
        with_cleanup: bool,
    ) -> (SubmittedTaskGraph, Vec<TaskInput>) {
        let termination_execution_policy = Some(ExecutionPolicy::default());
        let commit_task = with_commit.then(|| TerminationTaskDescriptor {
            tdl_context: TdlContext {
                package: "test_pkg".to_owned(),
                task_func: "commit_fn".to_owned(),
            },
            execution_policy: termination_execution_policy.clone(),
        });
        let cleanup_task = with_cleanup.then(|| TerminationTaskDescriptor {
            tdl_context: TdlContext {
                package: "test_pkg".to_owned(),
                task_func: "cleanup_fn".to_owned(),
            },
            execution_policy: termination_execution_policy,
        });

        let mut submitted = SubmittedTaskGraph::new(commit_task, cleanup_task)
            .expect("task graph creation should succeed");
        let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
        submitted
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: "test_pkg".to_owned(),
                    task_func: "task_fn".to_owned(),
                },
                execution_policy: Some(ExecutionPolicy::default()),
                inputs: vec![bytes_type.clone()],
                outputs: vec![bytes_type],
                input_sources: None,
            })
            .expect("task insertion should succeed");

        (submitted, vec![TaskInput::ValuePayload(vec![0u8; 4])])
    }

    async fn create_started_jcb(
        with_commit: bool,
        with_cleanup: bool,
    ) -> (TestJcb, RecordingTaskInstancePool) {
        let (submitted_task_graph, inputs) = build_task_graph(with_commit, with_cleanup);
        let pool = RecordingTaskInstancePool::default();
        let jcb = SharedJobControlBlock::create(
            JobId::default(),
            ResourceGroupId::default(),
            &submitted_task_graph,
            inputs,
            NoopReadyQueueSender::default(),
            NoopDbConnector::default(),
            pool.clone(),
        )
        .await
        .expect("jcb creation should succeed");
        jcb.start().await.expect("jcb start should succeed");
        (jcb, pool)
    }

    #[tokio::test]
    async fn create_task_instance_rolls_back_tcb_when_pool_registration_fails() {
        let (submitted_task_graph, inputs) = build_task_graph(false, false);
        let pool = RecordingTaskInstancePool::default();
        pool.fail_next_registration();
        let jcb = SharedJobControlBlock::create(
            JobId::default(),
            ResourceGroupId::default(),
            &submitted_task_graph,
            inputs,
            NoopReadyQueueSender::default(),
            NoopDbConnector::default(),
            pool.clone(),
        )
        .await
        .expect("jcb creation should succeed");
        jcb.start().await.expect("jcb start should succeed");

        let first_worker = WorkerId::new();
        let error = jcb
            .create_task_instance(TaskId::Index(0), first_worker)
            .await
            .expect_err("first registration should fail");
        assert!(matches!(
            error,
            CacheError::Internal(InternalError::TaskInstancePoolCorrupted(_))
        ));

        let second_worker = WorkerId::new();
        let execution_context = jcb
            .create_task_instance(TaskId::Index(0), second_worker)
            .await
            .expect("second registration should succeed after rollback");
        let registered = pool.take_registered();
        assert_eq!(registered.len(), 1);
        assert_eq!(registered[0].worker_id, second_worker);
        assert_eq!(
            registered[0].task_instance_id,
            execution_context.task_instance_id
        );
    }

    #[tokio::test]
    async fn regular_success_unregisters_running_record() {
        let (jcb, pool) = create_started_jcb(false, false).await;
        let worker_id = WorkerId::new();
        let execution_context = jcb
            .create_task_instance(TaskId::Index(0), worker_id)
            .await
            .expect("task instance creation should succeed");

        let state = jcb
            .succeed_task_instance(execution_context.task_instance_id, 0, vec![vec![1u8; 4]])
            .await
            .expect("task success should succeed");

        assert_eq!(state, JobState::Succeeded);
        assert_eq!(
            pool.take_unregistered(),
            vec![execution_context.task_instance_id]
        );
    }

    #[tokio::test]
    async fn regular_failure_unregisters_running_record() {
        let (jcb, pool) = create_started_jcb(false, false).await;
        let worker_id = WorkerId::new();
        let execution_context = jcb
            .create_task_instance(TaskId::Index(0), worker_id)
            .await
            .expect("task instance creation should succeed");

        let state = jcb
            .fail_task_instance(
                execution_context.task_instance_id,
                TaskId::Index(0),
                "failure".to_owned(),
            )
            .await
            .expect("task failure should succeed");

        assert_eq!(state, JobState::Failed);
        assert_eq!(
            pool.take_unregistered(),
            vec![execution_context.task_instance_id]
        );
    }

    #[tokio::test]
    async fn termination_success_unregisters_running_record() {
        let (commit_jcb, commit_pool) = create_started_jcb(true, false).await;
        let worker_id = WorkerId::new();
        let task_execution_context = commit_jcb
            .create_task_instance(TaskId::Index(0), worker_id)
            .await
            .expect("task instance creation should succeed");
        let state = commit_jcb
            .succeed_task_instance(
                task_execution_context.task_instance_id,
                0,
                vec![vec![1u8; 4]],
            )
            .await
            .expect("task success should succeed");
        assert_eq!(state, JobState::CommitReady);
        let _ = commit_pool.take_unregistered();

        let commit_execution_context = commit_jcb
            .create_task_instance(TaskId::Commit, worker_id)
            .await
            .expect("commit task instance creation should succeed");
        let state = commit_jcb
            .succeed_commit_task_instance(commit_execution_context.task_instance_id)
            .await
            .expect("commit task success should succeed");
        assert_eq!(state, JobState::Succeeded);
        assert_eq!(
            commit_pool.take_unregistered(),
            vec![commit_execution_context.task_instance_id]
        );

        let (cleanup_jcb, cleanup_pool) = create_started_jcb(false, true).await;
        let state = cleanup_jcb.cancel().await.expect("cancel should succeed");
        assert_eq!(state, JobState::CleanupReady);

        let cleanup_execution_context = cleanup_jcb
            .create_task_instance(TaskId::Cleanup, worker_id)
            .await
            .expect("cleanup task instance creation should succeed");
        let state = cleanup_jcb
            .succeed_cleanup_task_instance(cleanup_execution_context.task_instance_id)
            .await
            .expect("cleanup task success should succeed");
        assert_eq!(state, JobState::Cancelled);
        assert_eq!(
            cleanup_pool.take_unregistered(),
            vec![cleanup_execution_context.task_instance_id]
        );
    }

    #[tokio::test]
    async fn force_remove_task_instance_rejects_stale_completion() {
        let (jcb, _) = create_started_jcb(false, false).await;
        let worker_id = WorkerId::new();
        let execution_context = jcb
            .create_task_instance(TaskId::Index(0), worker_id)
            .await
            .expect("task instance creation should succeed");

        assert!(
            jcb.force_remove_task_instance(execution_context.task_instance_id, TaskId::Index(0))
                .await
                .expect("force remove should succeed")
        );

        let error = jcb
            .succeed_task_instance(execution_context.task_instance_id, 0, vec![vec![1u8; 4]])
            .await
            .expect_err("stale completion should be rejected");
        assert!(matches!(
            error,
            CacheError::StaleState(StaleStateError::InvalidTaskInstanceId)
        ));
    }
}
