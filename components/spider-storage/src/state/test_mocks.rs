use std::sync::Arc;

use dashmap::DashMap;
use spider_core::{
    job::JobState,
    types::{
        id::{JobId, ResourceGroupId, TaskInstanceId},
        io::{TaskInput, TaskOutput},
    },
};

use crate::{
    cache::{
        error::InternalError,
        task::{SharedTaskControlBlock, SharedTerminationTaskControlBlock},
    },
    db::{DbError, ExternalJobOrchestration, InternalJobOrchestration},
    ready_queue::ReadyQueueSender,
    task_instance_pool::{TaskInstanceMetadata, TaskInstancePoolConnector},
};

/// A mock ready queue sender for testing.
#[derive(Clone, Default)]
pub struct MockReadyQueueSender;

#[async_trait::async_trait]
impl ReadyQueueSender for MockReadyQueueSender {
    async fn send_task_ready(
        &self,
        _rg_id: ResourceGroupId,
        _job_id: JobId,
        _task_indices: Vec<usize>,
    ) -> Result<(), InternalError> {
        Ok(())
    }

    async fn send_commit_ready(
        &self,
        _rg_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<(), InternalError> {
        Ok(())
    }

    async fn send_cleanup_ready(
        &self,
        _rg_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<(), InternalError> {
        Ok(())
    }
}

/// A mock DB connector for testing that implements both external and internal orchestration.
#[derive(Clone)]
pub struct MockDbConnector {
    pub states: Arc<DashMap<JobId, JobState>>,
    pub errors: Arc<DashMap<JobId, String>>,
    pub outputs: Arc<DashMap<JobId, Vec<TaskOutput>>>,
}

impl Default for MockDbConnector {
    fn default() -> Self {
        Self {
            states: Arc::new(DashMap::new()),
            errors: Arc::new(DashMap::new()),
            outputs: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl ExternalJobOrchestration for MockDbConnector {
    async fn register(
        &self,
        _resource_group_id: ResourceGroupId,
        _task_graph: &spider_core::task::TaskGraph,
        _job_inputs: &[TaskInput],
    ) -> Result<JobId, DbError> {
        let job_id = JobId::new();
        self.states.insert(job_id, JobState::Ready);
        Ok(job_id)
    }

    async fn get_state(&self, job_id: JobId) -> Result<JobState, DbError> {
        self.states
            .get(&job_id)
            .map(|v| *v)
            .ok_or(DbError::JobNotFound(job_id))
    }

    async fn get_outputs(&self, job_id: JobId) -> Result<Vec<TaskOutput>, DbError> {
        self.outputs
            .get(&job_id)
            .map(|v| v.clone())
            .ok_or(DbError::JobNotFound(job_id))
    }

    async fn get_error(&self, job_id: JobId) -> Result<String, DbError> {
        self.errors
            .get(&job_id)
            .map(|v| v.clone())
            .ok_or(DbError::JobNotFound(job_id))
    }
}

#[async_trait::async_trait]
impl InternalJobOrchestration for MockDbConnector {
    async fn start(&self, job_id: JobId) -> Result<(), DbError> {
        self.states.insert(job_id, JobState::Running);
        Ok(())
    }

    async fn set_state(&self, job_id: JobId, state: JobState) -> Result<(), DbError> {
        self.states.insert(job_id, state);
        Ok(())
    }

    async fn commit_outputs(
        &self,
        job_id: JobId,
        _job_outputs: Vec<TaskOutput>,
        _has_commit_task: bool,
    ) -> Result<(), DbError> {
        self.states.insert(job_id, JobState::Succeeded);
        Ok(())
    }

    async fn cancel(&self, job_id: JobId, _has_cleanup_task: bool) -> Result<(), DbError> {
        self.states.insert(job_id, JobState::Cancelled);
        Ok(())
    }

    async fn fail(&self, job_id: JobId, _error_message: String) -> Result<(), DbError> {
        self.states.insert(job_id, JobState::Failed);
        Ok(())
    }

    async fn delete_expired_terminated_jobs(
        &self,
        _expire_after_sec: u64,
    ) -> Result<Vec<JobId>, DbError> {
        Ok(Vec::new())
    }
}

/// A mock task instance pool connector for testing.
#[derive(Clone, Default)]
pub struct MockTaskInstancePoolConnector;

#[async_trait::async_trait]
impl TaskInstancePoolConnector for MockTaskInstancePoolConnector {
    fn get_next_available_task_instance_id(&self) -> TaskInstanceId {
        1
    }

    async fn register_task_instance(
        &self,
        _tcb: SharedTaskControlBlock,
        _registration: TaskInstanceMetadata,
    ) -> Result<(), InternalError> {
        Ok(())
    }

    async fn register_termination_task_instance(
        &self,
        _termination_tcb: SharedTerminationTaskControlBlock,
        _registration: TaskInstanceMetadata,
    ) -> Result<(), InternalError> {
        Ok(())
    }
}
