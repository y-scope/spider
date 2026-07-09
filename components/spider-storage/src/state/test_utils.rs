use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use dashmap::DashMap;
use spider_core::job::JobState;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SchedulerId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::TaskOutput;
use spider_core::types::scheduler::RegisteredScheduler;

use crate::cache::error::InternalError;
use crate::cache::task::SharedTaskControlBlock;
use crate::cache::task::SharedTerminationTaskControlBlock;
use crate::db::DbError;
use crate::db::DbStorage;
use crate::db::ExecutionManagerLivenessManagement;
use crate::db::ExternalJobOrchestration;
use crate::db::InternalJobOrchestration;
use crate::db::RecoverableJobContext;
use crate::db::ResourceGroupManagement;
use crate::db::SchedulerRegistrationManagement;
use crate::db::SessionManagement;
use crate::job_submission::ValidatedJobSubmission;
use crate::ready_queue::ReadyQueueSender;
use crate::task_instance_pool::TaskInstanceMetadata;
use crate::task_instance_pool::TaskInstancePoolConnector;

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

/// A mock DB connector for testing that implements [`DbStorage`].
#[derive(Clone)]
pub struct MockDbConnector {
    pub states: Arc<DashMap<JobId, JobState>>,
    pub errors: Arc<DashMap<JobId, String>>,
    pub outputs: Arc<DashMap<JobId, Vec<TaskOutput>>>,
    pub resource_groups: Arc<DashMap<ResourceGroupId, Vec<u8>>>,
    pub next_resource_group_id: Arc<AtomicUsize>,
    pub execution_managers: Arc<DashMap<ExecutionManagerId, IpAddr>>,
    pub next_execution_manager_id: Arc<AtomicUsize>,
    pub next_scheduler_id: Arc<AtomicUsize>,
    pub session_id: SessionId,
}

impl Default for MockDbConnector {
    fn default() -> Self {
        Self {
            states: Arc::new(DashMap::new()),
            errors: Arc::new(DashMap::new()),
            outputs: Arc::new(DashMap::new()),
            resource_groups: Arc::new(DashMap::new()),
            next_resource_group_id: Arc::new(AtomicUsize::new(1)),
            execution_managers: Arc::new(DashMap::new()),
            next_execution_manager_id: Arc::new(AtomicUsize::new(1)),
            next_scheduler_id: Arc::new(AtomicUsize::new(1)),
            session_id: 0,
        }
    }
}

#[async_trait::async_trait]
impl ExternalJobOrchestration for MockDbConnector {
    async fn register(
        &self,
        _resource_group_id: ResourceGroupId,
        _job_submission: &ValidatedJobSubmission,
    ) -> Result<JobId, DbError> {
        let job_id = JobId::random();
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

    async fn get_recoverable_jobs(&self) -> Result<Vec<RecoverableJobContext>, DbError> {
        Ok(Vec::new())
    }
}

#[async_trait::async_trait]
impl ResourceGroupManagement for MockDbConnector {
    async fn add(
        &self,
        _external_resource_group_id: String,
        password: Vec<u8>,
    ) -> Result<ResourceGroupId, DbError> {
        let counter = self.next_resource_group_id.fetch_add(1, Ordering::Relaxed);
        let id = ResourceGroupId::from(counter as u64);
        self.resource_groups.insert(id, password);
        Ok(id)
    }

    async fn verify(
        &self,
        resource_group_id: ResourceGroupId,
        password: &[u8],
    ) -> Result<(), DbError> {
        let stored = self
            .resource_groups
            .get(&resource_group_id)
            .ok_or(DbError::ResourceGroupNotFound(resource_group_id))?;
        let matches = stored.as_slice() == password;
        drop(stored);
        if !matches {
            return Err(DbError::InvalidPassword(resource_group_id));
        }
        Ok(())
    }

    async fn delete(&self, resource_group_id: ResourceGroupId) -> Result<(), DbError> {
        self.resource_groups
            .remove(&resource_group_id)
            .ok_or(DbError::ResourceGroupNotFound(resource_group_id))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ExecutionManagerLivenessManagement for MockDbConnector {
    async fn register_execution_manager(
        &self,
        ip_address: IpAddr,
    ) -> Result<ExecutionManagerId, DbError> {
        let counter = self
            .next_execution_manager_id
            .fetch_add(1, Ordering::Relaxed);
        let id = ExecutionManagerId::from(counter as u64);
        self.execution_managers.insert(id, ip_address);
        Ok(id)
    }

    async fn update_execution_manager_heartbeat(
        &self,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<(), DbError> {
        if self.execution_managers.contains_key(&execution_manager_id) {
            Ok(())
        } else {
            Err(DbError::IllegalExecutionManagerId(execution_manager_id))
        }
    }

    async fn is_execution_manager_alive(
        &self,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<bool, DbError> {
        Ok(self.execution_managers.contains_key(&execution_manager_id))
    }

    async fn get_dead_execution_managers(
        &self,
        _stale_after_sec: u64,
    ) -> Result<Vec<ExecutionManagerId>, DbError> {
        Ok(Vec::new())
    }
}

#[async_trait::async_trait]
impl SchedulerRegistrationManagement for MockDbConnector {
    async fn register_scheduler(
        &self,
        _ip_address: IpAddr,
        _port: u16,
    ) -> Result<SchedulerId, DbError> {
        let counter = self.next_scheduler_id.fetch_add(1, Ordering::Relaxed);
        Ok(SchedulerId::from(counter as u64))
    }

    async fn get_schedulers(&self) -> Result<Vec<RegisteredScheduler>, DbError> {
        unreachable!("not implemented for mock connector")
    }

    async fn is_scheduler_registered(&self, _scheduler_id: SchedulerId) -> Result<bool, DbError> {
        unreachable!("not implemented for mock connector")
    }
}

impl SessionManagement for MockDbConnector {
    fn session_id(&self) -> SessionId {
        self.session_id
    }
}

impl DbStorage for MockDbConnector {}

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
