use std::sync::Arc;

use spider_core::{
    job::JobState,
    task::TaskIndex,
    types::{
        id::{ExecutionManagerId, JobId, ResourceGroupId, SessionId, TaskInstanceId},
        io::{ExecutionContext, TaskInput, TaskOutput},
    },
};

use crate::{
    cache::{TaskId, job::SharedJobControlBlock},
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
#[derive(Clone)]
pub struct ServiceState<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration + ExternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    db: DbConnectorType,
    session_id: SessionId,
    job_cache: Arc<JobCache<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>>,
    ready_queue_sender: ReadyQueueSenderType,
    _ready_queue_receiver: ReadyQueueReceiverHandle,
    task_instance_pool_connector: TaskInstancePoolConnectorType,
}

impl<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration + ExternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    /// Creates a new `ServiceState` from its constituent parts.
    ///
    /// # Returns
    ///
    /// A newly created `ServiceState`.
    pub fn new(
        db: DbConnectorType,
        session_id: SessionId,
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
            _ready_queue_receiver: ready_queue_receiver,
            task_instance_pool_connector,
        }
    }

    /// # Returns
    ///
    /// The current session ID.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Registers a job in the database and inserts its control block into the cache.
    ///
    /// If [`SharedJobControlBlock::create`] or [`JobCache::insert`] fails after the DB record has
    /// been created, the DB record is **not** deleted.
    ///
    /// # Returns
    ///
    /// The ID of the submitted job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobCache::insert`]'s return values on failure.
    /// * Forwards [`ExternalJobOrchestration::register`]'s return values on failure.
    /// * Forwards [`SharedJobControlBlock::create`]'s return values on failure.
    pub async fn register_job(
        &self,
        resource_group_id: ResourceGroupId,
        task_graph: &spider_core::task::TaskGraph,
        job_inputs: Vec<TaskInput>,
    ) -> Result<JobId, StorageServerError> {
        let job_id = self
            .db
            .register(resource_group_id, task_graph, &job_inputs)
            .await?;

        let jcb = SharedJobControlBlock::create(
            job_id,
            resource_group_id,
            task_graph,
            job_inputs,
            self.ready_queue_sender.clone(),
            self.db.clone(),
            self.task_instance_pool_connector.clone(),
        )
        .await?;

        self.job_cache.insert(jcb)?;

        Ok(job_id)
    }

    /// Submits a job for execution.
    ///
    /// Gets the job control block from the cache and starts it by calling
    /// [`SharedJobControlBlock::start`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::JobNotFound`] if the job is not in the cache.
    /// * Forwards [`SharedJobControlBlock::start`]'s return values on failure.
    pub async fn submit_job(&self, job_id: JobId) -> Result<(), StorageServerError> {
        let jcb = self
            .job_cache
            .get(job_id)
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        jcb.start().await?;
        Ok(())
    }

    /// Cancels a job.
    ///
    /// If the job is in the cache, delegates to [`SharedJobControlBlock::cancel`]. When the
    /// resulting state is terminal, the job is removed from the cache.
    ///
    /// # Returns
    ///
    /// The job state after the cancellation operation on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::JobNotFound`] if the job is not in the cache.
    /// * Forwards [`SharedJobControlBlock::cancel`]'s return values on failure.
    pub async fn cancel_job(&self, job_id: JobId) -> Result<JobState, StorageServerError> {
        let jcb = self
            .job_cache
            .get(job_id)
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        let state = jcb.cancel().await?;
        if state.is_terminal() {
            // OK if already removed by a concurrent operation.
            drop(self.job_cache.remove(job_id));
        }
        Ok(state)
    }

    /// Gets the state of a job.
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
    /// * [`StorageServerError::JobNotFound`] if the job is not in the cache.
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
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        Ok(jcb
            .create_task_instance(task_id, execution_manager_id)
            .await?)
    }

    /// Marks a task instance as succeeded.
    ///
    /// If all tasks have succeeded, commits the job outputs and transitions the job state. When the
    /// resulting state is terminal, the job is removed from the cache.
    ///
    /// # Returns
    ///
    /// The current job state after the operation on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::JobNotFound`] if the job is not in the cache.
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
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        let state = jcb
            .succeed_task_instance(task_instance_id, task_index, task_outputs)
            .await?;
        if state.is_terminal() {
            // OK if already removed by a concurrent operation.
            drop(self.job_cache.remove(job_id));
        }
        Ok(state)
    }

    /// Marks a task instance as failed.
    ///
    /// When the resulting state is terminal, the job is removed from the cache.
    ///
    /// # Returns
    ///
    /// The current job state after the operation on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::JobNotFound`] if the job is not in the cache.
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
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        let state = jcb
            .fail_task_instance(task_instance_id, task_id, error)
            .await?;
        if state.is_terminal() {
            // OK if already removed by a concurrent operation.
            drop(self.job_cache.remove(job_id));
        }
        Ok(state)
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
            TdlContext,
            ValueTypeDescriptor,
        },
        types::{
            id::{ExecutionManagerId, JobId, ResourceGroupId},
            io::{TaskInput, TaskOutput},
        },
    };

    use super::*;
    use crate::{
        cache::job::SharedJobControlBlock,
        state::{
            StorageServerError,
            test_mocks::{MockDbConnector, MockReadyQueueSender, MockTaskInstancePoolConnector},
        },
    };

    type TestServiceState =
        ServiceState<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector>;

    fn create_test_service() -> TestServiceState {
        create_test_service_with_db(MockDbConnector::default())
    }

    fn create_test_service_with_db(db: MockDbConnector) -> TestServiceState {
        use crate::ready_queue::{ReadyQueueConfig, create_ready_queue};
        let (_sender, receiver) =
            create_ready_queue(ReadyQueueConfig::default()).expect("ready queue creation");
        TestServiceState::new(
            db,
            0,
            JobCache::new(),
            MockReadyQueueSender,
            receiver,
            MockTaskInstancePoolConnector,
        )
    }

    fn create_test_task_graph() -> SubmittedTaskGraph {
        let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
        let mut task_graph =
            SubmittedTaskGraph::new(None, None).expect("task graph creation should succeed");
        task_graph
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
        task_graph
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
    async fn register_job_returns_job_id_and_inserts_into_cache() -> anyhow::Result<()> {
        let service = create_test_service();
        let job_id = service
            .register_job(
                ResourceGroupId::new(),
                &create_test_task_graph(),
                vec![TaskInput::ValuePayload(vec![0u8; 4])],
            )
            .await?;
        assert_ne!(job_id, JobId::default(), "job ID should be assigned");
        assert!(
            service.job_cache.get(job_id).is_some(),
            "JCB should be in cache after register_job"
        );
        Ok(())
    }

    #[tokio::test]
    async fn submit_job_starts_cached_job() -> anyhow::Result<()> {
        let service = create_test_service();
        let job_id = service
            .register_job(
                ResourceGroupId::new(),
                &create_test_task_graph(),
                vec![TaskInput::ValuePayload(vec![0u8; 4])],
            )
            .await?;

        service.submit_job(job_id).await?;

        let state = service.get_job_state(job_id).await?;
        assert_eq!(state, JobState::Running);
        Ok(())
    }

    #[tokio::test]
    async fn submit_job_returns_job_not_found_when_not_in_cache() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service.submit_job(JobId::new()).await;
        assert!(
            matches!(result, Err(StorageServerError::JobNotFound(_))),
            "submit_job should return JobNotFound when job is not in cache"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancel_job_returns_job_not_found_when_not_in_cache() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service.cancel_job(JobId::new()).await;
        assert!(
            matches!(result, Err(StorageServerError::JobNotFound(_))),
            "cancel_job should return JobNotFound when job is not in cache"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancel_job_with_cached_jcb_removes_on_terminal() -> anyhow::Result<()> {
        let service = create_test_service();
        let job_id = JobId::new();
        let jcb = create_test_jcb(job_id).await;
        service.job_cache.insert(jcb)?;

        let state = service.cancel_job(job_id).await?;
        assert!(
            state.is_terminal(),
            "cancel should result in terminal state"
        );
        assert!(
            service.job_cache.get(job_id).is_none(),
            "job should be removed from cache after terminal cancel"
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_job_state_delegates_to_db() -> anyhow::Result<()> {
        let service = create_test_service();
        let job_id = service
            .register_job(
                ResourceGroupId::new(),
                &create_test_task_graph(),
                vec![TaskInput::ValuePayload(vec![0u8; 4])],
            )
            .await?;

        let state = service.get_job_state(job_id).await?;
        assert_eq!(state, JobState::Ready);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_state_returns_error_for_unknown_job() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service.get_job_state(JobId::new()).await;
        assert!(result.is_err(), "get_job_state should fail for unknown job");
        Ok(())
    }

    #[tokio::test]
    async fn get_job_outputs_returns_outputs_for_registered_job() -> anyhow::Result<()> {
        let db = MockDbConnector::default();
        let job_id = JobId::new();
        let outputs: Vec<TaskOutput> = vec![vec![1, 2, 3]];
        db.outputs.insert(job_id, outputs.clone());

        let service = create_test_service_with_db(db);
        let result = service.get_job_outputs(job_id).await?;
        assert_eq!(result, outputs);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_outputs_returns_error_for_unknown_job() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service.get_job_outputs(JobId::new()).await;
        assert!(
            result.is_err(),
            "get_job_outputs should fail for unknown job"
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_job_error_returns_error_message_for_failed_job() -> anyhow::Result<()> {
        let db = MockDbConnector::default();
        let job_id = JobId::new();
        let error_msg = "something went wrong".to_owned();
        db.errors.insert(job_id, error_msg.clone());

        let service = create_test_service_with_db(db);
        let result = service.get_job_error(job_id).await?;
        assert_eq!(result, error_msg);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_error_returns_error_for_unknown_job() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service.get_job_error(JobId::new()).await;
        assert!(result.is_err(), "get_job_error should fail for unknown job");
        Ok(())
    }

    #[tokio::test]
    async fn create_task_instance_returns_execution_context() -> anyhow::Result<()> {
        let service = create_test_service();
        let job_id = service
            .register_job(
                ResourceGroupId::new(),
                &create_test_task_graph(),
                vec![TaskInput::ValuePayload(vec![0u8; 4])],
            )
            .await?;
        service.submit_job(job_id).await?;

        let context = service
            .create_task_instance(job_id, TaskId::Index(0), ExecutionManagerId::new())
            .await?;
        assert_eq!(
            context.task_instance_id, 1,
            "task instance ID should match mock pool counter"
        );
        Ok(())
    }

    #[tokio::test]
    async fn create_task_instance_returns_job_not_found_when_not_in_cache() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service
            .create_task_instance(JobId::new(), TaskId::Index(0), ExecutionManagerId::new())
            .await;
        assert!(
            matches!(result, Err(StorageServerError::JobNotFound(_))),
            "create_task_instance should return JobNotFound when job is not in cache"
        );
        Ok(())
    }

    #[tokio::test]
    async fn succeed_task_instance_transitions_job_to_succeeded() -> anyhow::Result<()> {
        let service = create_test_service();
        let job_id = service
            .register_job(
                ResourceGroupId::new(),
                &create_test_task_graph(),
                vec![TaskInput::ValuePayload(vec![0u8; 4])],
            )
            .await?;
        service.submit_job(job_id).await?;

        let context = service
            .create_task_instance(job_id, TaskId::Index(0), ExecutionManagerId::new())
            .await?;
        let state = service
            .succeed_task_instance(job_id, context.task_instance_id, 0, vec![vec![0u8; 4]])
            .await?;
        assert_eq!(state, JobState::Succeeded);
        assert!(
            service.job_cache.get(job_id).is_none(),
            "job should be removed from cache after terminal succeed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn succeed_task_instance_returns_job_not_found_when_not_in_cache() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service
            .succeed_task_instance(JobId::new(), 1, 0, vec![])
            .await;
        assert!(
            matches!(result, Err(StorageServerError::JobNotFound(_))),
            "succeed_task_instance should return JobNotFound when job is not in cache"
        );
        Ok(())
    }

    #[tokio::test]
    async fn fail_task_instance_transitions_job_to_failed() -> anyhow::Result<()> {
        let service = create_test_service();
        let job_id = service
            .register_job(
                ResourceGroupId::new(),
                &create_test_task_graph(),
                vec![TaskInput::ValuePayload(vec![0u8; 4])],
            )
            .await?;
        service.submit_job(job_id).await?;

        let context = service
            .create_task_instance(job_id, TaskId::Index(0), ExecutionManagerId::new())
            .await?;
        let state = service
            .fail_task_instance(
                job_id,
                context.task_instance_id,
                TaskId::Index(0),
                "test failure".to_owned(),
            )
            .await?;
        assert_eq!(state, JobState::Failed);
        assert!(
            service.job_cache.get(job_id).is_none(),
            "job should be removed from cache after terminal fail"
        );
        Ok(())
    }

    #[tokio::test]
    async fn fail_task_instance_returns_job_not_found_when_not_in_cache() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service
            .fail_task_instance(JobId::new(), 1, TaskId::Index(0), "error".to_owned())
            .await;
        assert!(
            matches!(result, Err(StorageServerError::JobNotFound(_))),
            "fail_task_instance should return JobNotFound when job is not in cache"
        );
        Ok(())
    }
}
