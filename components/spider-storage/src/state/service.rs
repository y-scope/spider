use std::sync::Arc;

use spider_core::{
    job::JobState,
    task::TaskIndex,
    types::{
        id::{ExecutionManagerId, JobId, ResourceGroupId, SessionId, TaskInstanceId},
        io::{ExecutionContext, TaskOutput},
    },
};
use tracing::{debug, instrument};

use crate::{
    cache::{TaskId, job::SharedJobControlBlock, job_submission::ValidatedJobSubmission},
    db::DbStorage,
    ready_queue::{ReadyQueueReceiverHandle, ReadyQueueSender},
    state::{JobCache, StorageServerError},
    task_instance_pool::TaskInstancePoolConnector,
};

/// Inner data for [`ServiceState`], holding all storage services.
///
/// The job cache is stored directly (not in an Arc) so that cloning the outer `ServiceState`
/// only clones a single `Arc`.
struct ServiceStateInner<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    db: DbConnectorType,
    session_id: SessionId,
    job_cache: JobCache<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    ready_queue_sender: ReadyQueueSenderType,
    _ready_queue_receiver: ReadyQueueReceiverHandle,
    task_instance_pool_connector: TaskInstancePoolConnectorType,
}

/// Per-request service state providing access to the storage layer.
///
/// Holds a DB connector, session ID, job cache, and ready queue handles. Request handlers call
/// methods on `ServiceState` directly.
///
/// Internally wraps a single [`Arc`] around [`ServiceStateInner`] so that cloning is cheap (one
/// Arc clone instead of cloning each field).
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The type of the ready queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
#[derive(Clone)]
pub struct ServiceState<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    inner: Arc<
        ServiceStateInner<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    >,
}

impl<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
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
            inner: Arc::new(ServiceStateInner {
                db,
                session_id,
                job_cache,
                ready_queue_sender,
                _ready_queue_receiver: ready_queue_receiver,
                task_instance_pool_connector,
            }),
        }
    }

    /// Registers a job in the database and inserts its control block into the cache.
    ///
    /// Accepts a [`ValidatedJobSubmission`] which guarantees that the task graph and inputs have
    /// already been validated for consistency.
    ///
    /// If [`SharedJobControlBlock::create`] or [`JobCache::insert`] fails after the DB record has
    /// been created, the DB record is **not** deleted.
    ///
    /// # Returns
    ///
    /// The ID of the registered job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ExternalJobOrchestration::register`]'s return values on failure.
    /// * Forwards [`SharedJobControlBlock::create`]'s return values on failure.
    /// * Forwards [`JobCache::insert`]'s return values on failure.
    #[instrument(skip(self, job_submission), fields(job_id))]
    pub async fn register_job(
        &self,
        resource_group_id: ResourceGroupId,
        job_submission: ValidatedJobSubmission,
    ) -> Result<JobId, StorageServerError> {
        let job_id = self
            .inner
            .db
            .register(resource_group_id, &job_submission)
            .await?;

        tracing::Span::current().record("job_id", tracing::field::debug(&job_id));

        let jcb = SharedJobControlBlock::create(
            job_id,
            resource_group_id,
            job_submission,
            self.inner.ready_queue_sender.clone(),
            self.inner.db.clone(),
            self.inner.task_instance_pool_connector.clone(),
        )
        .await?;

        self.inner.job_cache.insert(jcb)?;
        debug!("Inserted JCB into job cache");

        Ok(job_id)
    }

    /// Starts a job for execution.
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
    #[instrument(skip(self), fields(job_id = ?job_id))]
    pub async fn start_job(&self, job_id: JobId) -> Result<(), StorageServerError> {
        let jcb = self
            .inner
            .job_cache
            .get(job_id)
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        debug!("JCB found in cache, starting job");
        jcb.start().await?;
        Ok(())
    }

    /// Cancels a job.
    ///
    /// If the job is in the cache, delegates to [`SharedJobControlBlock::cancel`].
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
    #[instrument(skip(self), fields(job_id = ?job_id))]
    pub async fn cancel_job(&self, job_id: JobId) -> Result<JobState, StorageServerError> {
        let jcb = self
            .inner
            .job_cache
            .get(job_id)
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        debug!("JCB found in cache, cancelling job");
        let state = jcb.cancel().await?;
        Ok(state)
    }

    /// Gets the state of a job.
    ///
    /// Checks the job cache first; if the JCB is present, returns its in-memory state. Otherwise
    /// falls back to the database.
    ///
    /// # Returns
    ///
    /// The state of the job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ExternalJobOrchestration::get_state`]'s return values on failure (DB fallback).
    #[instrument(skip(self), fields(job_id = ?job_id))]
    pub async fn get_job_state(&self, job_id: JobId) -> Result<JobState, StorageServerError> {
        if let Some(jcb) = self.inner.job_cache.get(job_id) {
            debug!("JCB found in cache, returning in-memory state");
            return Ok(jcb.state().await);
        }
        debug!("JCB not in cache, falling back to database");
        Ok(self.inner.db.get_state(job_id).await?)
    }

    /// Gets the outputs of a job from the database.
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
    #[instrument(skip(self), fields(job_id = ?job_id))]
    pub async fn get_job_outputs(
        &self,
        job_id: JobId,
    ) -> Result<Vec<TaskOutput>, StorageServerError> {
        Ok(self.inner.db.get_outputs(job_id).await?)
    }

    /// Gets the error message of a job from the database.
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
    #[instrument(skip(self), fields(job_id = ?job_id))]
    pub async fn get_job_error(&self, job_id: JobId) -> Result<String, StorageServerError> {
        Ok(self.inner.db.get_error(job_id).await?)
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
    /// * [`StorageServerError::StaleSession`] if the session has changed.
    /// * [`StorageServerError::JobNotFound`] if the job is not in the cache.
    /// * Forwards [`SharedJobControlBlock::create_task_instance`]'s return values on failure.
    #[instrument(skip(self, session_id), fields(job_id = ?job_id, task_id = ?task_id))]
    pub async fn create_task_instance(
        &self,
        session_id: SessionId,
        job_id: JobId,
        task_id: TaskId,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<ExecutionContext, StorageServerError> {
        self.validate_session(session_id)?;
        let jcb = self
            .inner
            .job_cache
            .get(job_id)
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        debug!("JCB found in cache, creating task instance");
        Ok(jcb
            .create_task_instance(task_id, execution_manager_id)
            .await?)
    }

    /// Marks a task instance as succeeded.
    ///
    /// If all tasks have succeeded, commits the job outputs and transitions the job state.
    ///
    /// # Returns
    ///
    /// The current job state after the operation on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::StaleSession`] if the session has changed.
    /// * [`StorageServerError::JobNotFound`] if the job is not in the cache.
    /// * Forwards [`SharedJobControlBlock::succeed_task_instance`]'s return values on failure.
    #[instrument(
        skip(self, session_id, task_outputs),
        fields(job_id = ?job_id, task_instance_id = ?task_instance_id)
    )]
    pub async fn succeed_task_instance(
        &self,
        session_id: SessionId,
        job_id: JobId,
        task_instance_id: TaskInstanceId,
        task_index: TaskIndex,
        task_outputs: Vec<TaskOutput>,
    ) -> Result<JobState, StorageServerError> {
        self.validate_session(session_id)?;
        let jcb = self
            .inner
            .job_cache
            .get(job_id)
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        debug!("JCB found in cache, succeeding task instance");
        let state = jcb
            .succeed_task_instance(task_instance_id, task_index, task_outputs)
            .await?;
        Ok(state)
    }

    /// Marks a task instance as failed.
    ///
    /// # Returns
    ///
    /// The current job state after the operation on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::StaleSession`] if the session has changed.
    /// * [`StorageServerError::JobNotFound`] if the job is not in the cache.
    /// * Forwards [`SharedJobControlBlock::fail_task_instance`]'s return values on failure.
    #[instrument(
        skip(self, session_id, error),
        fields(job_id = ?job_id, task_instance_id = ?task_instance_id, task_id = ?task_id)
    )]
    pub async fn fail_task_instance(
        &self,
        session_id: SessionId,
        job_id: JobId,
        task_instance_id: TaskInstanceId,
        task_id: TaskId,
        error: String,
    ) -> Result<JobState, StorageServerError> {
        self.validate_session(session_id)?;
        let jcb = self
            .inner
            .job_cache
            .get(job_id)
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        debug!("JCB found in cache, failing task instance");
        let state = jcb
            .fail_task_instance(task_instance_id, task_id, error)
            .await?;
        Ok(state)
    }

    /// Validates that the given `session_id` matches the session ID captured at service creation
    /// time.
    ///
    /// # Errors
    ///
    /// Returns [`StorageServerError::StaleSession`] if the session IDs don't match.
    fn validate_session(&self, session_id: SessionId) -> Result<(), StorageServerError> {
        if session_id != self.inner.session_id {
            debug!("Session ID mismatch");
            return Err(StorageServerError::StaleSession);
        }
        Ok(())
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
        cache::{job::SharedJobControlBlock, job_submission::ValidatedJobSubmission},
        state::{
            StorageServerError,
            test_mocks::{MockDbConnector, MockReadyQueueSender, MockTaskInstancePoolConnector},
        },
    };

    type TestServiceState =
        ServiceState<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector>;

    const TEST_SESSION_ID: SessionId = 0;

    fn create_test_service() -> TestServiceState {
        create_test_service_with_db(MockDbConnector::default())
    }

    fn create_test_service_with_db(db: MockDbConnector) -> TestServiceState {
        create_test_service_with_db_and_session(db, TEST_SESSION_ID)
    }

    fn create_test_service_with_db_and_session(
        db: MockDbConnector,
        session_id: SessionId,
    ) -> TestServiceState {
        use crate::ready_queue::{ReadyQueueConfig, create_ready_queue};
        let (_sender, receiver) =
            create_ready_queue(ReadyQueueConfig::default()).expect("ready queue creation");
        TestServiceState::new(
            db,
            session_id,
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

    fn create_test_job_submission() -> ValidatedJobSubmission {
        ValidatedJobSubmission::create(
            create_test_task_graph(),
            vec![TaskInput::ValuePayload(vec![0u8; 4])],
        )
        .expect("job submission should be valid")
    }

    async fn create_test_jcb(
        job_id: JobId,
    ) -> SharedJobControlBlock<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector>
    {
        let task_graph = create_test_task_graph();
        let job_submission =
            ValidatedJobSubmission::create(task_graph, vec![TaskInput::ValuePayload(vec![0u8; 4])])
                .expect("job submission should be valid");

        SharedJobControlBlock::create(
            job_id,
            ResourceGroupId::new(),
            job_submission,
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
            .register_job(ResourceGroupId::new(), create_test_job_submission())
            .await?;
        assert_ne!(job_id, JobId::default(), "job ID should be assigned");
        assert!(
            service.inner.job_cache.get(job_id).is_some(),
            "JCB should be in cache after register_job"
        );
        Ok(())
    }

    #[tokio::test]
    async fn start_job_starts_cached_job() -> anyhow::Result<()> {
        let service = create_test_service();
        let job_id = service
            .register_job(ResourceGroupId::new(), create_test_job_submission())
            .await?;

        service.start_job(job_id).await?;

        let state = service.get_job_state(job_id).await?;
        assert_eq!(state, JobState::Running);
        Ok(())
    }

    #[tokio::test]
    async fn start_job_returns_job_not_found_when_not_in_cache() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service.start_job(JobId::new()).await;
        assert!(
            matches!(result, Err(StorageServerError::JobNotFound(_))),
            "start_job should return JobNotFound when job is not in cache"
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
    async fn cancel_job_transitions_to_terminal_state() -> anyhow::Result<()> {
        let service = create_test_service();
        let job_id = JobId::new();
        let jcb = create_test_jcb(job_id).await;
        service.inner.job_cache.insert(jcb)?;

        let state = service.cancel_job(job_id).await?;
        assert!(
            state.is_terminal(),
            "cancel should result in terminal state"
        );
        assert!(
            service.inner.job_cache.get(job_id).is_some(),
            "JCB should remain in cache after terminal cancel"
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_job_state_serves_from_cache_when_jcb_present() -> anyhow::Result<()> {
        let service = create_test_service();
        let job_id = service
            .register_job(ResourceGroupId::new(), create_test_job_submission())
            .await?;

        let state = service.get_job_state(job_id).await?;
        assert_eq!(state, JobState::Ready);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_state_falls_back_to_db_when_not_in_cache() -> anyhow::Result<()> {
        let db = MockDbConnector::default();
        let job_id = JobId::new();
        db.states.insert(job_id, JobState::Failed);

        let service = create_test_service_with_db(db);
        let state = service.get_job_state(job_id).await?;
        assert_eq!(state, JobState::Failed);
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
    async fn get_job_outputs_returns_outputs_from_db() -> anyhow::Result<()> {
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
    async fn get_job_error_returns_error_message_from_db() -> anyhow::Result<()> {
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
            .register_job(ResourceGroupId::new(), create_test_job_submission())
            .await?;
        service.start_job(job_id).await?;

        let context = service
            .create_task_instance(
                TEST_SESSION_ID,
                job_id,
                TaskId::Index(0),
                ExecutionManagerId::new(),
            )
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
            .create_task_instance(
                TEST_SESSION_ID,
                JobId::new(),
                TaskId::Index(0),
                ExecutionManagerId::new(),
            )
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
            .register_job(ResourceGroupId::new(), create_test_job_submission())
            .await?;
        service.start_job(job_id).await?;

        let context = service
            .create_task_instance(
                TEST_SESSION_ID,
                job_id,
                TaskId::Index(0),
                ExecutionManagerId::new(),
            )
            .await?;
        let state = service
            .succeed_task_instance(
                TEST_SESSION_ID,
                job_id,
                context.task_instance_id,
                0,
                vec![vec![0u8; 4]],
            )
            .await?;
        assert_eq!(state, JobState::Succeeded);
        assert!(
            service.inner.job_cache.get(job_id).is_some(),
            "JCB should remain in cache after terminal succeed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn succeed_task_instance_returns_job_not_found_when_not_in_cache() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service
            .succeed_task_instance(TEST_SESSION_ID, JobId::new(), 1, 0, vec![])
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
            .register_job(ResourceGroupId::new(), create_test_job_submission())
            .await?;
        service.start_job(job_id).await?;

        let context = service
            .create_task_instance(
                TEST_SESSION_ID,
                job_id,
                TaskId::Index(0),
                ExecutionManagerId::new(),
            )
            .await?;
        let state = service
            .fail_task_instance(
                TEST_SESSION_ID,
                job_id,
                context.task_instance_id,
                TaskId::Index(0),
                "test failure".to_owned(),
            )
            .await?;
        assert_eq!(state, JobState::Failed);
        assert!(
            service.inner.job_cache.get(job_id).is_some(),
            "JCB should remain in cache after terminal fail"
        );
        Ok(())
    }

    #[tokio::test]
    async fn fail_task_instance_returns_job_not_found_when_not_in_cache() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service
            .fail_task_instance(
                TEST_SESSION_ID,
                JobId::new(),
                1,
                TaskId::Index(0),
                "error".to_owned(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageServerError::JobNotFound(_))),
            "fail_task_instance should return JobNotFound when job is not in cache"
        );
        Ok(())
    }

    #[tokio::test]
    async fn task_instance_apis_return_stale_session_on_mismatch() -> anyhow::Result<()> {
        // Create a service with a higher session ID to simulate a server restart.
        let current_session_id: SessionId = 10;
        let db = MockDbConnector::default();
        let service = create_test_service_with_db_and_session(db, current_session_id);

        // Register a job so the JCB is in cache.
        let job_id = service
            .register_job(ResourceGroupId::new(), create_test_job_submission())
            .await?;

        let stale_session_id = current_session_id - 1;
        let result = service
            .create_task_instance(
                stale_session_id,
                job_id,
                TaskId::Index(0),
                ExecutionManagerId::new(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageServerError::StaleSession)),
            "create_task_instance should return StaleSession on session mismatch"
        );
        Ok(())
    }

    #[tokio::test]
    async fn succeed_task_instance_returns_stale_session_on_mismatch() -> anyhow::Result<()> {
        let current_session_id: SessionId = 10;
        let db = MockDbConnector::default();
        let service = create_test_service_with_db_and_session(db, current_session_id);

        let job_id = service
            .register_job(ResourceGroupId::new(), create_test_job_submission())
            .await?;
        service.start_job(job_id).await?;

        let context = service
            .create_task_instance(
                current_session_id,
                job_id,
                TaskId::Index(0),
                ExecutionManagerId::new(),
            )
            .await?;

        let stale_session_id = current_session_id - 1;
        let result = service
            .succeed_task_instance(
                stale_session_id,
                job_id,
                context.task_instance_id,
                0,
                vec![vec![0u8; 4]],
            )
            .await;
        assert!(
            matches!(result, Err(StorageServerError::StaleSession)),
            "succeed_task_instance should return StaleSession on session mismatch"
        );
        Ok(())
    }

    #[tokio::test]
    async fn fail_task_instance_returns_stale_session_on_mismatch() -> anyhow::Result<()> {
        let current_session_id: SessionId = 10;
        let db = MockDbConnector::default();
        let service = create_test_service_with_db_and_session(db, current_session_id);

        let job_id = service
            .register_job(ResourceGroupId::new(), create_test_job_submission())
            .await?;
        service.start_job(job_id).await?;

        let context = service
            .create_task_instance(
                current_session_id,
                job_id,
                TaskId::Index(0),
                ExecutionManagerId::new(),
            )
            .await?;

        let stale_session_id = current_session_id - 1;
        let result = service
            .fail_task_instance(
                stale_session_id,
                job_id,
                context.task_instance_id,
                TaskId::Index(0),
                "error".to_owned(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageServerError::StaleSession)),
            "fail_task_instance should return StaleSession on session mismatch"
        );
        Ok(())
    }
}
