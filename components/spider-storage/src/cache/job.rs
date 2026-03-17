use std::sync::atomic::{AtomicUsize, Ordering};

use spider_core::{
    job::JobState,
    task::TaskIndex,
    types::{
        id::{JobId, ResourceGroupId, TaskInstanceId},
        io::TaskOutput,
    },
};

use crate::{
    cache::{
        error::{
            CacheError,
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
    job: tokio::sync::RwLock<Job>,
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
        let job = self.job.read().await;

        let execution_context = match task_id {
            TaskId::TaskIndex(task_index) => {
                if job.state == JobState::Ready {
                    return Err(InternalError::JobNotStarted.into());
                }
                if !job.state.is_running() {
                    return Err(RejectionError::JobNoLongerRunning(job.state).into());
                }
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
                if job.state.is_terminal() || job.state == JobState::CleanupReady {
                    return Err(JobNoLongerCommitReady(job.state).into());
                }
                if job.state != JobState::CommitReady {
                    return Err(InternalError::UnexpectedJobState {
                        expected: JobState::CommitReady,
                        current: job.state,
                    }
                    .into());
                }
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
                if job.state.is_terminal() {
                    return Err(JobNoLongerCleanupReady(job.state).into());
                }
                if job.state != JobState::CleanupReady {
                    return Err(InternalError::UnexpectedJobState {
                        expected: JobState::CleanupReady,
                        current: job.state,
                    }
                    .into());
                }
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
        let job = self.job.read().await;
        if job.state == JobState::Ready {
            return Err(InternalError::JobNotStarted.into());
        }
        if !job.state.is_running() {
            return Err(RejectionError::JobNoLongerRunning(job.state).into());
        }
        let tcb = job
            .task_graph
            .get_task(task_index)
            .ok_or(InternalError::TaskIndexOutOfBound)?;
        let ready_task_ids = tcb.complete_task_instance(task_instance_id, task_outputs).await?;
        let num_incompleted_task = job.num_incompleted_tasks.fetch_sub(1, Ordering::Relaxed);
        if !ready_task_ids.is_empty() {
            if num_incompleted_task == 0 {
                return Err(
                    InternalError::TaskGraphCorrupted(
                        "no incompleted tasks while new ready task IDs are generated".to_owned()
                    ).into());
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

        let job_state = self.commit_job_outputs().await?;
        if matches!(job_state, JobState::CommitReady) {
            self.ready_queue_connector.send_commit_ready(self.id.clone()).await?;
        }
        Ok(job_state)
    }

    pub async fn commit_job_outputs(&self) -> Result<JobState, CacheError> {
        let mut job = self.job.write().await;
        if !job.state.is_running() {
            return Err(RejectionError::JobNoLongerRunning(job.state).into());
        }
        let outputs = job.task_graph.get_outputs().await.map_err(|_| InternalError::JobOutputsNotReady)?;

        Ok(job.state)
    }

    pub async fn complete_commit_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, CacheError> {
        todo!("Implement this!")
    }

    pub async fn complete_cleanup_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, CacheError> {
        todo!("Implement this!")
    }

    pub async fn fail_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_id: TaskId,
    ) -> Result<JobState, CacheError> {
        todo!("Implement this!")
    }
}

struct Job {
    state: JobState,
    task_graph: TaskGraph,
    num_incompleted_tasks: AtomicUsize,
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
