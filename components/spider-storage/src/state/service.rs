use std::sync::Arc;

use spider_core::{
    job::JobState,
    task::TaskIndex,
    types::{
        id::{ExecutionManagerId, JobId, ResourceGroupId, TaskInstanceId},
        io::{ExecutionContext, TaskInput, TaskOutput},
    },
};

use crate::{
    cache::TaskId,
    db::{ExternalJobOrchestration, InternalJobOrchestration},
    ready_queue::{ReadyQueueReceiverHandle, ReadyQueueSender},
    state::{JobCache, StorageServerError},
    task_instance_pool::TaskInstancePoolConnector,
};

/// Per-request service state providing access to the storage layer.
///
/// Holds a DB connector, session ID, job cache, and ready queue handles. Request handlers call
/// methods on `ServiceState` directly.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The type of the ready queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
#[allow(dead_code)]
#[derive(Clone)]
pub struct ServiceState<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration + ExternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    db: DbConnectorType,
    session_id: spider_core::types::id::SessionId,
    job_cache: Arc<JobCache<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>>,
    ready_queue_sender: ReadyQueueSenderType,
    ready_queue_receiver: ReadyQueueReceiverHandle,
    task_instance_pool_connector: TaskInstancePoolConnectorType,
}

impl<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration + ExternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    /// Creates a new `ServiceState` from its constituent parts.
    pub fn new(
        db: DbConnectorType,
        session_id: spider_core::types::id::SessionId,
        job_cache: JobCache<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
        ready_queue_sender: ReadyQueueSenderType,
        ready_queue_receiver: ReadyQueueReceiverHandle,
        task_instance_pool_connector: TaskInstancePoolConnectorType,
    ) -> Self {
        Self {
            db,
            session_id,
            job_cache: Arc::new(job_cache),
            ready_queue_sender,
            ready_queue_receiver,
            task_instance_pool_connector,
        }
    }

    /// # Returns
    ///
    /// The current session ID.
    #[must_use]
    pub const fn session_id(&self) -> spider_core::types::id::SessionId {
        self.session_id
    }

    // ── Job operations ───────────────────────────────────────────────

    /// Registers a job in the database.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The owner of the created job.
    /// * `task_graph` - The task graph representing the job's tasks and their dependencies.
    /// * `job_inputs` - A slice of job inputs required for the job.
    ///
    /// # Returns
    ///
    /// The ID of the submitted job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ExternalJobOrchestration::register`]'s return values on failure.
    pub async fn register_job(
        &self,
        resource_group_id: ResourceGroupId,
        task_graph: &spider_core::task::TaskGraph,
        job_inputs: &[TaskInput],
    ) -> Result<JobId, StorageServerError> {
        Ok(self
            .db
            .register(resource_group_id, task_graph, job_inputs)
            .await?)
    }

    /// Submits a job for execution.
    ///
    /// Calls [`InternalJobOrchestration::start`] on the database to transition the job to
    /// [`JobState::Running`].
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job to submit.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`InternalJobOrchestration::start`]'s return values on failure.
    pub async fn submit_job(&self, job_id: JobId) -> Result<(), StorageServerError> {
        self.db.start(job_id).await?;
        Ok(())
    }

    /// Cancels a job.
    ///
    /// If the job is in the cache, delegates to [`SharedJobControlBlock::cancel`]. When the
    /// resulting state is terminal, the job is removed from the cache.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job to cancel.
    ///
    /// # Returns
    ///
    /// The job state after the cancellation operation on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::BadRequest`] if the job is not in the cache.
    /// * Forwards [`SharedJobControlBlock::cancel`]'s return values on failure.
    pub async fn cancel_job(&self, job_id: JobId) -> Result<JobState, StorageServerError> {
        let jcb = self
            .job_cache
            .get(job_id)
            .ok_or(StorageServerError::BadRequest("job not found in cache"))?;
        let state = jcb.cancel().await.map_err(StorageServerError::from)?;
        if state.is_terminal() {
            let _ = self.job_cache.remove(job_id);
        }
        Ok(state)
    }

    /// Gets the state of a job.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    ///
    /// # Returns
    ///
    /// The state of the job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ExternalJobOrchestration::get_state`]'s return values on failure.
    pub async fn get_job_state(&self, job_id: JobId) -> Result<JobState, StorageServerError> {
        Ok(self.db.get_state(job_id).await?)
    }

    /// Gets the outputs of a job.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    ///
    /// # Returns
    ///
    /// The outputs of the job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ExternalJobOrchestration::get_outputs`]'s return values on failure.
    pub async fn get_job_outputs(
        &self,
        job_id: JobId,
    ) -> Result<Vec<TaskOutput>, StorageServerError> {
        Ok(self.db.get_outputs(job_id).await?)
    }

    /// Gets the error message of a job.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    ///
    /// # Returns
    ///
    /// The error message of the job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ExternalJobOrchestration::get_error`]'s return values on failure.
    pub async fn get_job_error(&self, job_id: JobId) -> Result<String, StorageServerError> {
        Ok(self.db.get_error(job_id).await?)
    }

    // ── Execution-manager task-instance reporting ────────────────────

    /// Creates a task instance for the given task and registers it in the task instance pool.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    /// * `task_id` - The identifier of the task within the job.
    /// * `execution_manager_id` - The ID of the execution manager creating this instance.
    ///
    /// # Returns
    ///
    /// The execution context for the created task instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::BadRequest`] if the job is not in the cache.
    /// * Forwards [`SharedJobControlBlock::create_task_instance`]'s return values on failure.
    pub async fn create_task_instance(
        &self,
        job_id: JobId,
        task_id: TaskId,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<ExecutionContext, StorageServerError> {
        let jcb = self
            .job_cache
            .get(job_id)
            .ok_or(StorageServerError::BadRequest("job not found in cache"))?;
        jcb.create_task_instance(task_id, execution_manager_id)
            .await
            .map_err(StorageServerError::from)
    }

    /// Marks a task instance as succeeded.
    ///
    /// If all tasks have succeeded, commits the job outputs and transitions the job state. When the
    /// resulting state is terminal, the job is removed from the cache.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    /// * `task_instance_id` - The ID of the succeeded task instance.
    /// * `task_index` - The index of the task in the job's task graph.
    /// * `task_outputs` - The outputs produced by the task instance.
    ///
    /// # Returns
    ///
    /// The current job state after the operation on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::BadRequest`] if the job is not in the cache.
    /// * Forwards [`SharedJobControlBlock::succeed_task_instance`]'s return values on failure.
    pub async fn succeed_task_instance(
        &self,
        job_id: JobId,
        task_instance_id: TaskInstanceId,
        task_index: TaskIndex,
        task_outputs: Vec<TaskOutput>,
    ) -> Result<JobState, StorageServerError> {
        let jcb = self
            .job_cache
            .get(job_id)
            .ok_or(StorageServerError::BadRequest("job not found in cache"))?;
        let state = jcb
            .succeed_task_instance(task_instance_id, task_index, task_outputs)
            .await
            .map_err(StorageServerError::from)?;
        if state.is_terminal() {
            let _ = self.job_cache.remove(job_id);
        }
        Ok(state)
    }

    /// Marks a task instance as failed.
    ///
    /// When the resulting state is terminal, the job is removed from the cache.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    /// * `task_instance_id` - The ID of the failed task instance.
    /// * `task_id` - The identifier of the task within the job.
    /// * `error` - The error message explaining the failure.
    ///
    /// # Returns
    ///
    /// The current job state after the operation on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::BadRequest`] if the job is not in the cache.
    /// * Forwards [`SharedJobControlBlock::fail_task_instance`]'s return values on failure.
    pub async fn fail_task_instance(
        &self,
        job_id: JobId,
        task_instance_id: TaskInstanceId,
        task_id: TaskId,
        error: String,
    ) -> Result<JobState, StorageServerError> {
        let jcb = self
            .job_cache
            .get(job_id)
            .ok_or(StorageServerError::BadRequest("job not found in cache"))?;
        let state = jcb
            .fail_task_instance(task_instance_id, task_id, error)
            .await
            .map_err(StorageServerError::from)?;
        if state.is_terminal() {
            let _ = self.job_cache.remove(job_id);
        }
        Ok(state)
    }
}

impl From<crate::cache::error::CacheError> for StorageServerError {
    fn from(err: crate::cache::error::CacheError) -> Self {
        match err {
            crate::cache::error::CacheError::Internal(e) => Self::Internal(e),
            crate::cache::error::CacheError::StaleState(e) => Self::StaleState(e),
            crate::cache::error::CacheError::Db(e) => Self::Db(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use spider_core::{
        job::JobState,
        task::{
            DataTypeDescriptor,
            ExecutionPolicy,
            TaskDescriptor,
            TaskGraph as SubmittedTaskGraph,
            TaskIndex,
            TdlContext,
            ValueTypeDescriptor,
        },
        types::{
            id::{ExecutionManagerId, JobId, ResourceGroupId, TaskInstanceId},
            io::{TaskInput, TaskOutput},
        },
    };

    use super::*;
    use crate::{
        cache::{error::InternalError, job::SharedJobControlBlock},
        ready_queue::ReadyQueueSender,
        state::StorageServerError,
        task_instance_pool::{TaskInstanceMetadata, TaskInstancePoolConnector},
    };

    /// A mock ready queue sender for testing.
    #[derive(Clone, Default)]
    struct MockReadyQueueSender;

    #[async_trait::async_trait]
    impl ReadyQueueSender for MockReadyQueueSender {
        async fn send_task_ready(
            &self,
            _rg_id: ResourceGroupId,
            _job_id: JobId,
            _task_indices: Vec<TaskIndex>,
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
    #[derive(Clone, Default)]
    struct MockDbConnector {
        registered_jobs: std::sync::Arc<tokio::sync::Mutex<Vec<JobId>>>,
        job_states: std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<JobId, JobState>>>,
        job_errors: std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<JobId, String>>>,
        job_outputs:
            std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<JobId, Vec<TaskOutput>>>>,
    }

    #[async_trait::async_trait]
    impl crate::db::ExternalJobOrchestration for MockDbConnector {
        async fn register(
            &self,
            _resource_group_id: ResourceGroupId,
            _task_graph: &SubmittedTaskGraph,
            _job_inputs: &[TaskInput],
        ) -> Result<JobId, crate::db::DbError> {
            let job_id = JobId::new();
            self.registered_jobs.lock().await.push(job_id);
            self.job_states.lock().await.insert(job_id, JobState::Ready);
            Ok(job_id)
        }

        async fn get_state(&self, job_id: JobId) -> Result<JobState, crate::db::DbError> {
            self.job_states
                .lock()
                .await
                .get(&job_id)
                .copied()
                .ok_or(crate::db::DbError::JobNotFound(job_id))
        }

        async fn get_outputs(&self, job_id: JobId) -> Result<Vec<TaskOutput>, crate::db::DbError> {
            self.job_outputs
                .lock()
                .await
                .get(&job_id)
                .cloned()
                .ok_or(crate::db::DbError::JobNotFound(job_id))
        }

        async fn get_error(&self, job_id: JobId) -> Result<String, crate::db::DbError> {
            self.job_errors
                .lock()
                .await
                .get(&job_id)
                .cloned()
                .ok_or(crate::db::DbError::JobNotFound(job_id))
        }
    }

    #[async_trait::async_trait]
    impl crate::db::InternalJobOrchestration for MockDbConnector {
        async fn start(&self, job_id: JobId) -> Result<(), crate::db::DbError> {
            self.job_states
                .lock()
                .await
                .insert(job_id, JobState::Running);
            Ok(())
        }

        async fn set_state(
            &self,
            job_id: JobId,
            state: JobState,
        ) -> Result<(), crate::db::DbError> {
            self.job_states.lock().await.insert(job_id, state);
            Ok(())
        }

        async fn commit_outputs(
            &self,
            job_id: JobId,
            _job_outputs: Vec<TaskOutput>,
            _has_commit_task: bool,
        ) -> Result<(), crate::db::DbError> {
            self.job_states
                .lock()
                .await
                .insert(job_id, JobState::Succeeded);
            Ok(())
        }

        async fn cancel(
            &self,
            job_id: JobId,
            _has_cleanup_task: bool,
        ) -> Result<(), crate::db::DbError> {
            self.job_states
                .lock()
                .await
                .insert(job_id, JobState::Cancelled);
            Ok(())
        }

        async fn fail(
            &self,
            job_id: JobId,
            _error_message: String,
        ) -> Result<(), crate::db::DbError> {
            self.job_states
                .lock()
                .await
                .insert(job_id, JobState::Failed);
            Ok(())
        }

        async fn delete_expired_terminated_jobs(
            &self,
            _expire_after_sec: u64,
        ) -> Result<Vec<JobId>, crate::db::DbError> {
            Ok(Vec::new())
        }
    }

    /// A mock task instance pool connector for testing.
    #[derive(Clone, Default)]
    struct MockTaskInstancePoolConnector;

    #[async_trait::async_trait]
    impl TaskInstancePoolConnector for MockTaskInstancePoolConnector {
        fn get_next_available_task_instance_id(&self) -> TaskInstanceId {
            1
        }

        async fn register_task_instance(
            &self,
            _tcb: crate::cache::task::SharedTaskControlBlock,
            _registration: TaskInstanceMetadata,
        ) -> Result<(), InternalError> {
            Ok(())
        }

        async fn register_termination_task_instance(
            &self,
            _termination_tcb: crate::cache::task::SharedTerminationTaskControlBlock,
            _registration: TaskInstanceMetadata,
        ) -> Result<(), InternalError> {
            Ok(())
        }
    }

    type TestServiceState =
        ServiceState<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector>;

    fn create_test_service() -> TestServiceState {
        use crate::ready_queue::{ReadyQueueConfig, create_ready_queue};
        let (_sender, receiver) =
            create_ready_queue(ReadyQueueConfig::default()).expect("ready queue creation");
        TestServiceState::new(
            MockDbConnector::default(),
            0,
            JobCache::new(),
            MockReadyQueueSender,
            receiver,
            MockTaskInstancePoolConnector,
        )
    }

    async fn create_test_jcb(
        job_id: JobId,
    ) -> SharedJobControlBlock<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector>
    {
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

        SharedJobControlBlock::create(
            job_id,
            ResourceGroupId::new(),
            &submitted,
            vec![TaskInput::ValuePayload(vec![0u8; 4])],
            MockReadyQueueSender,
            MockDbConnector::default(),
            MockTaskInstancePoolConnector,
        )
        .await
        .expect("JCB creation should succeed")
    }

    #[tokio::test]
    async fn register_job_returns_job_id() {
        let service = create_test_service();
        let job_id = service
            .register_job(
                ResourceGroupId::new(),
                &SubmittedTaskGraph::new(None, None).unwrap(),
                &[],
            )
            .await
            .expect("register_job should succeed");
        assert_ne!(job_id, JobId::default(), "job ID should be assigned");
    }

    #[tokio::test]
    async fn submit_job_delegates_to_db() {
        let service = create_test_service();
        let job_id = service
            .register_job(
                ResourceGroupId::new(),
                &SubmittedTaskGraph::new(None, None).unwrap(),
                &[],
            )
            .await
            .expect("register_job should succeed");

        service
            .submit_job(job_id)
            .await
            .expect("submit_job should succeed");

        let state = service
            .get_job_state(job_id)
            .await
            .expect("get_job_state should succeed");
        assert_eq!(state, JobState::Running);
    }

    #[tokio::test]
    async fn cancel_job_returns_bad_request_when_not_in_cache() {
        let service = create_test_service();
        let result = service.cancel_job(JobId::new()).await;
        assert!(
            matches!(result, Err(StorageServerError::BadRequest(_))),
            "cancel_job should return BadRequest when job is not in cache"
        );
    }

    #[tokio::test]
    async fn cancel_job_with_cached_jcb_removes_on_terminal() {
        let service = create_test_service();
        let job_id = JobId::new();
        let jcb = create_test_jcb(job_id).await;
        service
            .job_cache
            .insert(job_id, jcb)
            .expect("insert should succeed");

        let state = service
            .cancel_job(job_id)
            .await
            .expect("cancel_job should succeed for cached job");
        assert!(
            state.is_terminal(),
            "cancel should result in terminal state"
        );
        assert!(
            service.job_cache.get(job_id).is_none(),
            "job should be removed from cache after terminal cancel"
        );
    }

    #[tokio::test]
    async fn get_job_state_delegates_to_db() {
        let service = create_test_service();
        let job_id = service
            .register_job(
                ResourceGroupId::new(),
                &SubmittedTaskGraph::new(None, None).unwrap(),
                &[],
            )
            .await
            .expect("register_job should succeed");

        let state = service
            .get_job_state(job_id)
            .await
            .expect("get_job_state should succeed");
        assert_eq!(state, JobState::Ready);
    }

    #[tokio::test]
    async fn get_job_state_returns_error_for_unknown_job() {
        let service = create_test_service();
        let result = service.get_job_state(JobId::new()).await;
        assert!(result.is_err(), "get_job_state should fail for unknown job");
    }

    #[tokio::test]
    async fn get_job_error_returns_error_for_unknown_job() {
        let service = create_test_service();
        let result = service.get_job_error(JobId::new()).await;
        assert!(result.is_err(), "get_job_error should fail for unknown job");
    }

    #[tokio::test]
    async fn create_task_instance_returns_bad_request_when_not_in_cache() {
        let service = create_test_service();
        let result = service
            .create_task_instance(JobId::new(), TaskId::Index(0), ExecutionManagerId::new())
            .await;
        assert!(
            matches!(result, Err(StorageServerError::BadRequest(_))),
            "create_task_instance should return BadRequest when job is not in cache"
        );
    }

    #[tokio::test]
    async fn succeed_task_instance_returns_bad_request_when_not_in_cache() {
        let service = create_test_service();
        let result = service
            .succeed_task_instance(JobId::new(), 1, 0, vec![])
            .await;
        assert!(
            matches!(result, Err(StorageServerError::BadRequest(_))),
            "succeed_task_instance should return BadRequest when job is not in cache"
        );
    }

    #[tokio::test]
    async fn fail_task_instance_returns_bad_request_when_not_in_cache() {
        let service = create_test_service();
        let result = service
            .fail_task_instance(JobId::new(), 1, TaskId::Index(0), "error".to_owned())
            .await;
        assert!(
            matches!(result, Err(StorageServerError::BadRequest(_))),
            "fail_task_instance should return BadRequest when job is not in cache"
        );
    }
}
