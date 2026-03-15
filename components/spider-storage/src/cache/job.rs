use std::sync::atomic::AtomicUsize;

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
        error::CacheError,
        task::{SharedTaskControlBlock, SharedTerminationTaskControlBlock, TaskGraph},
        types::{ExecutionContext, TaskId},
    },
    db::DbStorage,
};

pub struct JobControlBlock<
    ReadyQueueSenderType: ReadyQueueConnector,
    DbConnectorType: DbStorage,
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
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> JobControlBlock<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    pub async fn create_task_instance(
        &self,
        task_id: TaskId,
    ) -> Result<ExecutionContext, CacheError> {
        todo!("Implement this!")
    }

    pub async fn complete_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_id: TaskId,
        task_outputs: Vec<TaskOutput>,
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
    num_unfinished_tasks: AtomicUsize,
}

#[async_trait::async_trait]
pub trait ReadyQueueConnector {
    async fn send_task_ready(&self, job_id: JobId, task_ids: Vec<TaskId>)
    -> Result<(), CacheError>;

    async fn send_commit_ready(&self, job_id: JobId) -> Result<(), CacheError>;

    async fn send_cleanup_ready(&self, job_id: JobId) -> Result<(), CacheError>;
}

#[async_trait::async_trait]
pub trait TaskInstancePoolConnector {
    fn get_next_available_task_instance_id(&self) -> TaskInstanceId;

    async fn register_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task: SharedTaskControlBlock,
    ) -> Result<(), CacheError>;

    async fn register_termination_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        termination_task: SharedTerminationTaskControlBlock,
    ) -> Result<(), CacheError>;
}
