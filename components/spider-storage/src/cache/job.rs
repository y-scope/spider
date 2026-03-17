use std::sync::atomic::{AtomicUsize, Ordering};

use spider_core::{
    job::JobState,
    task::{TaskIndex, TaskState},
    types::{
        id::{JobId, ResourceGroupId, TaskInstanceId},
        io::TaskOutput,
    },
};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    cache::{
        error::{
            CacheError,
            CacheError::Internal,
            InternalError,
            RejectionError,
            RejectionError::{JobNoLongerCleanupReady, JobNoLongerCommitReady},
        },
        task::{SharedTaskControlBlock, SharedTerminationTaskControlBlock, TaskGraph},
        types::{ExecutionContext, TaskId},
    },
    db::InternalJobOrchestration,
};

pub struct JobControlBlock<
    ReadyQueueSenderType: ReadyQueueConnector,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    id: JobId,
    owner_id: ResourceGroupId,
    job: RwJob,
    ready_queue_connector: ReadyQueueSenderType,
    db_connector: DbConnectorType,
    task_instance_pool_connector: TaskInstancePoolConnectorType,
}

impl<
    ReadyQueueSenderType: ReadyQueueConnector,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> JobControlBlock<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    pub async fn create_task_instance(
        &self,
        task_id: TaskId,
    ) -> Result<ExecutionContext, CacheError> {
        let execution_context = match task_id {
            TaskId::TaskIndex(task_index) => {
                let job = self.job.read_if_running().await?;
                let tcb = job
                    .task_graph
                    .get_task(task_index)
                    .ok_or(InternalError::TaskIndexOutOfBound)?;
                let task_instance_id = self
                    .task_instance_pool_connector
                    .get_next_available_task_instance_id();
                let execution_context = tcb.register_task_instance(task_instance_id).await?;
                self.task_instance_pool_connector
                    .register_task_instance(task_instance_id, tcb)
                    .await?;
                execution_context
            }

            TaskId::Commit => {
                let job = self.job.read_if_commit_ready().await?;
                let commit_tcb = job
                    .task_graph
                    .get_commit_task()
                    .ok_or(InternalError::JobNoCommit)?;
                let task_instance_id = self
                    .task_instance_pool_connector
                    .get_next_available_task_instance_id();
                let tdl_context = commit_tcb
                    .register_termination_task_instance(task_instance_id)
                    .await?;
                self.task_instance_pool_connector
                    .register_termination_task_instance(task_instance_id, commit_tcb)
                    .await?;
                ExecutionContext {
                    task_instance_id,
                    tdl_context,
                    // TODO: Question, what's the input for the commit task?
                    inputs: None,
                }
            }

            TaskId::Cleanup => {
                let job = self.job.read_if_cleanup_ready().await?;
                let commit_tcb = job
                    .task_graph
                    .get_commit_task()
                    .ok_or(InternalError::JobNoCommit)?;
                let task_instance_id = self
                    .task_instance_pool_connector
                    .get_next_available_task_instance_id();
                let tdl_context = commit_tcb
                    .register_termination_task_instance(task_instance_id)
                    .await?;
                self.task_instance_pool_connector
                    .register_termination_task_instance(task_instance_id, commit_tcb)
                    .await?;
                ExecutionContext {
                    task_instance_id,
                    tdl_context,
                    inputs: None,
                }
            }
        };

        Ok(execution_context)
    }

    pub async fn complete_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_index: TaskIndex,
        task_outputs: Vec<TaskOutput>,
    ) -> Result<JobState, CacheError> {
        let job = self.job.read_if_running().await?;
        let tcb = job
            .task_graph
            .get_task(task_index)
            .ok_or(InternalError::TaskIndexOutOfBound)?;
        let ready_task_ids = tcb
            .complete_task_instance(task_instance_id, task_outputs)
            .await?;
        let num_incompleted_task = job.num_incompleted_tasks.fetch_sub(1, Ordering::Relaxed);

        if !ready_task_ids.is_empty() {
            if num_incompleted_task == 0 {
                return Err(InternalError::TaskGraphCorrupted(
                    "no incompleted tasks while new ready task IDs are generated".to_owned(),
                )
                .into());
            }
            self.ready_queue_connector
                .send_task_ready(self.id.clone(), ready_task_ids)
                .await?;
            return Ok(job.state);
        }

        if num_incompleted_task != 0 {
            return Ok(job.state);
        }

        drop(job);
        let job_state = self.commit_outputs().await?;
        match job_state {
            JobState::CommitReady => {
                if !self.job.has_commit_task().await {
                    return Err(InternalError::JobNoCommit.into());
                }
                self.ready_queue_connector
                    .send_commit_ready(self.id.clone())
                    .await?;
            }
            JobState::Succeeded => {}
            other => unreachable!(
                "unexpected job state after committing job outputs: {:?}",
                other
            ),
        }
        Ok(job_state)
    }

    pub async fn complete_commit_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, CacheError> {
        let mut job = self.job.write_if_commit_ready().await?;
        job.task_graph
            .get_commit_task()
            .ok_or(InternalError::JobNoCommit)?
            .complete_termination_task_instance(task_instance_id)
            .await?;
        self.db_connector
            .set_state(self.id.clone(), JobState::Succeeded)
            .await?;
        job.state = JobState::Succeeded;
        Ok(JobState::Succeeded)
    }

    pub async fn complete_cleanup_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, CacheError> {
        let mut job = self.job.write_if_cleanup_ready().await?;
        job.task_graph
            .get_cleanup_task()
            .ok_or(InternalError::JobNoCleanup)?
            .complete_termination_task_instance(task_instance_id)
            .await?;
        self.db_connector
            .set_state(self.id.clone(), JobState::Cancelled)
            .await?;
        job.state = JobState::Cancelled;
        Ok(JobState::Cancelled)
    }

    pub async fn fail_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_id: TaskId,
        error_message: String,
    ) -> Result<JobState, CacheError> {
        match task_id {
            TaskId::TaskIndex(task_index) => {
                let job = self.job.read_if_running().await?;
                let task_state = job
                    .task_graph
                    .get_task(task_index)
                    .ok_or(InternalError::TaskIndexOutOfBound)?
                    .fail_task_instance(task_instance_id, error_message.clone())
                    .await?;
                if matches!(task_state, TaskState::Ready | TaskState::Running) {
                    self.ready_queue_connector
                        .send_task_ready(self.id.clone(), vec![task_index])
                        .await?;
                    return Ok(job.state);
                }
            }
            TaskId::Commit => {
                let job = self.job.read_if_commit_ready().await?;
                let task_state = job
                    .task_graph
                    .get_commit_task()
                    .ok_or(InternalError::JobNoCommit)?
                    .fail_termination_task_instance(task_instance_id, error_message.clone())
                    .await?;
                if matches!(task_state, TaskState::Ready | TaskState::Running) {
                    self.ready_queue_connector
                        .send_commit_ready(self.id.clone())
                        .await?;
                    return Ok(job.state);
                }
            }
            TaskId::Cleanup => {
                let job = self.job.read_if_cleanup_ready().await?;
                let task_state = job
                    .task_graph
                    .get_cleanup_task()
                    .ok_or(InternalError::JobNoCleanup)?
                    .fail_termination_task_instance(task_instance_id, error_message.clone())
                    .await?;
                if matches!(task_state, TaskState::Ready | TaskState::Running) {
                    self.ready_queue_connector
                        .send_cleanup_ready(self.id.clone())
                        .await?;
                    return Ok(job.state);
                }
            }
        };

        let mut job = self.job.write_if_non_terminated().await.map_err(|e| {
            match &e {
                CacheError::Rejection(RejectionError::JobAlreadyTerminated(state)) => {
                    if *state == JobState::Failed {
                        // Already failed by others
                        return e;
                    }
                    InternalError::JobTerminatedUnexpectedly.into()
                }
                _ => InternalError::JobTerminatedUnexpectedly.into(),
            }
        })?;
        self.db_connector
            .fail(self.id.clone(), error_message)
            .await?;
        job.state = JobState::Failed;
        Ok(JobState::Failed)
    }

    async fn commit_outputs(&self) -> Result<JobState, CacheError> {
        let mut job = self.job.write_if_running().await?;
        let outputs = job
            .task_graph
            .get_outputs()
            .await
            .map_err(|_| InternalError::JobOutputsNotReady)?;
        job.state = self
            .db_connector
            .commit_outputs(self.id.clone(), outputs)
            .await?;
        Ok(job.state)
    }

    async fn cancel(&self) -> Result<JobState, CacheError> {
        todo!(
            "Implement this. The job table must be locked for write, and the state of all tasks \
             must be checked to ensure if any of them are failed already, the cancellation \
             shouldn't go through."
        )
    }
}

struct Job {
    state: JobState,
    task_graph: TaskGraph,
    num_incompleted_tasks: AtomicUsize,
}

struct RwJob {
    inner: RwLock<Job>,
}

impl RwJob {
    async fn read_checked(
        &self,
        check: fn(&Job) -> Result<(), CacheError>,
    ) -> Result<RwLockReadGuard<'_, Job>, CacheError> {
        let guard = self.inner.read().await;
        check(&*guard)?;
        Ok(guard)
    }

    async fn write_checked(
        &self,
        check: fn(&Job) -> Result<(), CacheError>,
    ) -> Result<RwLockWriteGuard<'_, Job>, CacheError> {
        let guard = self.inner.write().await;
        check(&*guard)?;
        Ok(guard)
    }

    pub async fn read_if_running(&self) -> Result<RwLockReadGuard<'_, Job>, CacheError> {
        self.read_checked(Job::assumed_running).await
    }

    pub async fn write_if_running(&self) -> Result<RwLockWriteGuard<'_, Job>, CacheError> {
        self.write_checked(Job::assumed_running).await
    }

    pub async fn read_if_commit_ready(&self) -> Result<RwLockReadGuard<'_, Job>, CacheError> {
        self.read_checked(Job::assumed_commit_ready).await
    }

    pub async fn write_if_commit_ready(&self) -> Result<RwLockWriteGuard<'_, Job>, CacheError> {
        self.write_checked(Job::assumed_commit_ready).await
    }

    pub async fn read_if_cleanup_ready(&self) -> Result<RwLockReadGuard<'_, Job>, CacheError> {
        self.read_checked(Job::assumed_cleanup_ready).await
    }

    pub async fn write_if_cleanup_ready(&self) -> Result<RwLockWriteGuard<'_, Job>, CacheError> {
        self.write_checked(Job::assumed_cleanup_ready).await
    }

    pub async fn write_if_non_terminated(&self) -> Result<RwLockWriteGuard<'_, Job>, CacheError> {
        self.write_checked(Job::assumed_non_terminated).await
    }

    pub async fn has_commit_task(&self) -> bool {
        self.inner.read().await.task_graph.has_commit_task()
    }

    pub async fn has_cleanup_task(&self) -> bool {
        self.inner.read().await.task_graph.has_cleanup_task()
    }
}

impl Job {
    fn assumed_running(&self) -> Result<(), CacheError> {
        if !self.state.is_running() {
            if self.state == JobState::Ready {
                return Err(InternalError::JobNotStarted.into());
            }
            return Err(RejectionError::JobNoLongerRunning(self.state).into());
        }
        Ok(())
    }

    fn assumed_commit_ready(&self) -> Result<(), CacheError> {
        if self.state != JobState::CommitReady {
            if self.state.is_terminal() || self.state == JobState::CleanupReady {
                return Err(JobNoLongerCommitReady(self.state).into());
            }
            return Err(InternalError::UnexpectedJobState {
                expected: JobState::CommitReady,
                current: self.state,
            }
            .into());
        }
        Ok(())
    }

    fn assumed_cleanup_ready(&self) -> Result<(), CacheError> {
        if self.state != JobState::CleanupReady {
            if self.state.is_terminal() {
                return Err(JobNoLongerCleanupReady(self.state).into());
            }
            return Err(InternalError::UnexpectedJobState {
                expected: JobState::CleanupReady,
                current: self.state,
            }
            .into());
        }
        Ok(())
    }

    fn assumed_non_terminated(&self) -> Result<(), CacheError> {
        if self.state.is_terminal() {
            return Err(RejectionError::JobNoLongerRunning(self.state).into());
        }
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait ReadyQueueConnector {
    async fn send_task_ready(
        &self,
        job_id: JobId,
        task_ids: Vec<TaskIndex>,
    ) -> Result<(), InternalError>;

    async fn send_commit_ready(&self, job_id: JobId) -> Result<(), InternalError>;

    async fn send_cleanup_ready(&self, job_id: JobId) -> Result<(), InternalError>;
}

#[async_trait::async_trait]
pub trait TaskInstancePoolConnector {
    fn get_next_available_task_instance_id(&self) -> TaskInstanceId;

    async fn register_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task: SharedTaskControlBlock,
    ) -> Result<(), InternalError>;

    async fn register_termination_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        termination_task: SharedTerminationTaskControlBlock,
    ) -> Result<(), InternalError>;
}
