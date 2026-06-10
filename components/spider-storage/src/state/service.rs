use std::{net::IpAddr, sync::Arc, time::Duration};

use spider_core::{
    job::JobState,
    task::{TaskGraph, TaskIndex},
    types::{
        id::{ExecutionManagerId, JobId, ResourceGroupId, SessionId, TaskId, TaskInstanceId},
        io::{ExecutionContext, TaskInput, TaskOutput},
    },
};
use spider_tdl::{
    error::TdlError,
    wire::{TaskOutputsSerializer, unframe},
};

use crate::{
    cache::{
        error::{CacheError, InternalError},
        job::SharedJobControlBlock,
        job_submission::ValidatedJobSubmission,
    },
    db::DbStorage,
    ready_queue::{
        CleanupTaskMarker,
        CommitTaskMarker,
        ReadyQueueEntry,
        ReadyQueueReceiverHandle,
        ReadyQueueSender,
    },
    state::{JobCache, StorageServerError},
    task_instance_pool::TaskInstancePoolConnector,
};

/// Per-request service state providing access to the storage layer.
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
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created [`ServiceState`] from its constituent parts.
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
                ready_queue_receiver,
                task_instance_pool_connector,
            }),
        }
    }

    /// # Returns
    ///
    /// The storage session ID owned by this service state.
    #[must_use]
    pub fn session_id(&self) -> SessionId {
        self.inner.session_id
    }

    /// Registers a job in the database and inserts its control block into the cache.
    ///
    /// # Returns
    ///
    /// The ID of the registered job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TaskGraph::from_json`]'s return values on failure.
    /// * Forwards [`spider_tdl::wire::unframe`]'s return values on failure.
    /// * Forwards [`ValidatedJobSubmission::create`]'s return values on failure.
    /// * Forwards [`ExternalJobOrchestration::register`]'s return values on failure.
    /// * Forwards [`SharedJobControlBlock::create`]'s return values on failure.
    /// * Forwards [`JobCache::insert`]'s return values on failure.
    pub async fn register_job(
        &self,
        resource_group_id: ResourceGroupId,
        serialized_task_graph: String,
        serialized_inputs: Vec<u8>,
    ) -> Result<JobId, StorageServerError> {
        let task_graph =
            TaskGraph::from_json(&serialized_task_graph).map_err(StorageServerError::Task)?;
        let inputs = unframe(&serialized_inputs)
            .map_err(|e| StorageServerError::Tdl(TdlError::DeserializationError(e.to_string())))?
            .into_iter()
            .map(TaskInput::ValuePayload)
            .collect();
        let job_submission =
            ValidatedJobSubmission::create(task_graph, inputs).map_err(CacheError::from)?;

        let job_id = self
            .inner
            .db
            .register(resource_group_id, &job_submission)
            .await?;
        tracing::info!(
            job_id = ? job_id,
            rg_id = ? resource_group_id,
            "Job registered in DB."
        );

        let jcb = SharedJobControlBlock::create(
            job_id,
            resource_group_id,
            job_submission,
            self.inner.ready_queue_sender.clone(),
            self.inner.db.clone(),
            self.inner.task_instance_pool_connector.clone(),
        )
        .await?;

        self.inner.job_cache.insert(jcb).await?;
        tracing::info!(
            job_id = ? job_id,
            rg_id = ? resource_group_id,
            "Job inserted in cache.",
        );

        Ok(job_id)
    }

    /// Starts a job for execution.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::JobNotFound`] if the job is not in the cache.
    /// * Forwards [`SharedJobControlBlock::start`]'s return values on failure.
    pub async fn start_job(&self, job_id: JobId) -> Result<(), StorageServerError> {
        let jcb = self
            .inner
            .job_cache
            .get(job_id)
            .await
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        jcb.start().await?;
        tracing::info!(
            job_id = ? job_id,
            "Job started.",
        );
        Ok(())
    }

    /// Cancels a job.
    ///
    /// # Returns
    ///
    /// The job state after the cancellation operation on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::JobNotFound`] if the job is not in a terminal state, but it is not in the
    ///   cache.
    /// * [`StorageServerError::JobNotFound`] if the job is not in the cache and not in a terminal
    ///   state in the database.
    /// * Forwards [`SharedJobControlBlock::cancel`]'s return values on failure.
    pub async fn cancel_job(&self, job_id: JobId) -> Result<JobState, StorageServerError> {
        if let Some(jcb) = self.inner.job_cache.get(job_id).await {
            let state = jcb.cancel().await?;
            tracing::info!(
                job_id = ? job_id,
                "Job cancelled.",
            );
            return Ok(state);
        }
        match self.inner.db.get_state(job_id).await {
            Ok(state) => {
                if !state.is_terminal() {
                    // If the job is not terminated, it should always exist in the cache.
                    return Err(StorageServerError::Cache(CacheError::Internal(
                        InternalError::JobNotFound(job_id),
                    )));
                }
                Ok(state)
            }
            _ => Err(StorageServerError::JobNotFound(job_id)),
        }
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
    /// * Forwards [`ExternalJobOrchestration::get_state`]'s return values on failure (DB fallback).
    pub async fn get_job_state(&self, job_id: JobId) -> Result<JobState, StorageServerError> {
        if let Some(jcb) = self.inner.job_cache.get(job_id).await {
            return Ok(jcb.state().await);
        }
        Ok(self.inner.db.get_state(job_id).await?)
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
    /// * Forwards [`SharedJobControlBlock::get_outputs`]'s return values on failure (cache path).
    /// * Forwards [`ExternalJobOrchestration::get_outputs`]'s return values on failure (DB
    ///   fallback).
    pub async fn get_job_outputs(
        &self,
        job_id: JobId,
    ) -> Result<Vec<TaskOutput>, StorageServerError> {
        if let Some(jcb) = self.inner.job_cache.get(job_id).await {
            return Ok(jcb.get_outputs().await?);
        }
        Ok(self.inner.db.get_outputs(job_id).await?)
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
        Ok(self.inner.db.get_error(job_id).await?)
    }

    /// Resends ready tasks for all jobs in the cache to the ready queue.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobCache::resend_ready_tasks`]'s return values on failure.
    pub async fn resend_ready_tasks(&self) -> Result<(), StorageServerError> {
        self.inner.job_cache.resend_ready_tasks().await
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
            .await
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        let exe_context = jcb
            .create_task_instance(task_id, execution_manager_id)
            .await?;
        tracing::info!(
            job_id = ? job_id,
            task_id = ? task_id,
            execution_manager_id = ? execution_manager_id,
            task_instance_id = ? exe_context.task_instance_id,
            "Task instance created.",
        );
        Ok(exe_context)
    }

    /// Marks a task instance as succeeded.
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
    /// * Forwards [`spider_tdl::wire::TaskOutputsSerializer::deserialize`]'s return values on
    ///   failure.
    /// * Forwards [`SharedJobControlBlock::succeed_task_instance`]'s return values on failure.
    pub async fn succeed_task_instance(
        &self,
        session_id: SessionId,
        job_id: JobId,
        task_instance_id: TaskInstanceId,
        task_index: TaskIndex,
        serialized_outputs: Vec<u8>,
    ) -> Result<JobState, StorageServerError> {
        self.validate_session(session_id)?;
        let task_outputs = TaskOutputsSerializer::deserialize(&serialized_outputs)
            .map_err(|e| StorageServerError::Tdl(TdlError::DeserializationError(e.to_string())))?;
        let jcb = self
            .inner
            .job_cache
            .get(job_id)
            .await
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        let state = jcb
            .succeed_task_instance(task_instance_id, task_index, task_outputs)
            .await?;
        tracing::info!(
            job_id = ? job_id,
            task_id = ? TaskId::Index(task_index),
            task_instance_id = ? task_instance_id,
            job_state = ? state,
            "Task instance succeeded.",
        );
        Ok(state)
    }

    /// Marks a commit task instance as succeeded.
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
    /// * Forwards [`SharedJobControlBlock::succeed_commit_task_instance`]'s return values on
    ///   failure.
    pub async fn succeed_commit_task_instance(
        &self,
        session_id: SessionId,
        job_id: JobId,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, StorageServerError> {
        self.validate_session(session_id)?;
        let jcb = self
            .inner
            .job_cache
            .get(job_id)
            .await
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        let state = jcb.succeed_commit_task_instance(task_instance_id).await?;
        tracing::info!(
            job_id = ? job_id,
            task_id = ? TaskId::Commit,
            task_instance_id = ? task_instance_id,
            job_state = ? state,
            "Task instance succeeded.",
        );
        Ok(state)
    }

    /// Marks a cleanup task instance as succeeded.
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
    /// * Forwards [`SharedJobControlBlock::succeed_cleanup_task_instance`]'s return values on
    ///   failure.
    pub async fn succeed_cleanup_task_instance(
        &self,
        session_id: SessionId,
        job_id: JobId,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, StorageServerError> {
        self.validate_session(session_id)?;
        let jcb = self
            .inner
            .job_cache
            .get(job_id)
            .await
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        let state = jcb.succeed_cleanup_task_instance(task_instance_id).await?;
        tracing::info!(
            job_id = ? job_id,
            task_id = ? TaskId::Cleanup,
            task_instance_id = ? task_instance_id,
            job_state = ? state,
            "Task instance succeeded.",
        );
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
            .await
            .ok_or(StorageServerError::JobNotFound(job_id))?;
        let state = jcb
            .fail_task_instance(task_instance_id, task_id, error)
            .await?;
        tracing::info!(
            job_id = ? job_id,
            task_id = ? task_id,
            task_instance_id = ? task_instance_id,
            job_state = ? state,
            "Task instance failed.",
        );
        Ok(state)
    }

    /// Adds a resource group with the given external ID and password.
    ///
    /// # Returns
    ///
    /// The ID of the created resource group on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ResourceGroupManagement::add`]'s return values on failure.
    pub async fn add_resource_group(
        &self,
        external_id: String,
        password: Vec<u8>,
    ) -> Result<ResourceGroupId, StorageServerError> {
        let rg_id = self.inner.db.add(external_id, password).await?;
        tracing::info!(
            rg_id = ? rg_id,
            "Resource group added.",
        );
        Ok(rg_id)
    }

    /// Verifies the password of a resource group.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ResourceGroupManagement::verify`]'s return values on failure.
    pub async fn verify_resource_group(
        &self,
        resource_group_id: ResourceGroupId,
        password: &[u8],
    ) -> Result<(), StorageServerError> {
        self.inner
            .db
            .verify(resource_group_id, password)
            .await
            .map_err(StorageServerError::from)
    }

    /// Polls the ready queue for task entries.
    ///
    /// # Returns
    ///
    /// Up to `max_tasks` ready queue entries received within the `wait` duration on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ReadyQueueReceiverHandle::recv_tasks`]'s return values on failure.
    pub async fn poll_ready_tasks(
        &self,
        max_tasks: usize,
        wait: Duration,
    ) -> Result<Vec<ReadyQueueEntry<TaskIndex>>, StorageServerError> {
        self.inner
            .ready_queue_receiver
            .recv_tasks(max_tasks, wait)
            .await
            .map_err(|e| CacheError::Internal(e).into())
    }

    /// Polls the ready queue for commit-ready task entries.
    ///
    /// # Returns
    ///
    /// Up to `max_tasks` commit-ready queue entries received within the `wait` duration on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ReadyQueueReceiverHandle::recv_commits`]'s return values on failure.
    pub async fn poll_commit_ready_tasks(
        &self,
        max_tasks: usize,
        wait: Duration,
    ) -> Result<Vec<ReadyQueueEntry<CommitTaskMarker>>, StorageServerError> {
        self.inner
            .ready_queue_receiver
            .recv_commits(max_tasks, wait)
            .await
            .map_err(|e| CacheError::Internal(e).into())
    }

    /// Polls the ready queue for cleanup-ready task entries.
    ///
    /// # Returns
    ///
    /// Up to `max_tasks` cleanup-ready queue entries received within the `wait` duration on
    /// success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ReadyQueueReceiverHandle::recv_cleanups`]'s return values on failure.
    pub async fn poll_cleanup_ready_tasks(
        &self,
        max_tasks: usize,
        wait: Duration,
    ) -> Result<Vec<ReadyQueueEntry<CleanupTaskMarker>>, StorageServerError> {
        self.inner
            .ready_queue_receiver
            .recv_cleanups(max_tasks, wait)
            .await
            .map_err(|e| CacheError::Internal(e).into())
    }

    /// Registers an execution manager.
    ///
    /// # Returns
    ///
    /// The ID of the registered execution manager on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ExecutionManagerLivenessManagement::register_execution_manager`]'s return
    ///   values on failure.
    pub async fn register_execution_manager(
        &self,
        ip_address: IpAddr,
    ) -> Result<ExecutionManagerId, StorageServerError> {
        let em_id = self.inner.db.register_execution_manager(ip_address).await?;
        tracing::info!(
            em_id = ? em_id,
            ip = ? ip_address,
            "Execution manager registered.",
        );
        Ok(em_id)
    }

    /// Updates the heartbeat of an execution manager.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ExecutionManagerLivenessManagement::update_execution_manager_heartbeat`]'s
    ///   return values on failure.
    pub async fn update_execution_manager_heartbeat(
        &self,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<(), StorageServerError> {
        self.inner
            .db
            .update_execution_manager_heartbeat(execution_manager_id)
            .await?;
        tracing::info!(
            em_id = ? execution_manager_id,
            "Execution manager heartbeat updated.",
        );
        Ok(())
    }

    /// Validates that the given `session_id` matches the session ID captured at service creation
    /// time.
    ///
    /// # Errors
    ///
    /// Returns [`StorageServerError::StaleSession`] if the session IDs don't match.
    fn validate_session(&self, session_id: SessionId) -> Result<(), StorageServerError> {
        if session_id != self.inner.session_id {
            return Err(StorageServerError::StaleSession);
        }
        Ok(())
    }
}

/// Inner data for [`ServiceState`], holding all storage services.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The type of the ready queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
struct ServiceStateInner<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    db: DbConnectorType,
    session_id: SessionId,
    job_cache: JobCache<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    ready_queue_sender: ReadyQueueSenderType,
    ready_queue_receiver: ReadyQueueReceiverHandle,
    task_instance_pool_connector: TaskInstancePoolConnectorType,
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
        db::DbError,
        ready_queue::ReadyQueueSenderHandle,
        state::{
            StorageServerError,
            test_utils::{MockDbConnector, MockReadyQueueSender, MockTaskInstancePoolConnector},
        },
    };

    type TestServiceState =
        ServiceState<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector>;

    type TestServiceStateWithReadyQueue =
        ServiceState<ReadyQueueSenderHandle, MockDbConnector, MockTaskInstancePoolConnector>;

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
            create_ready_queue(&ReadyQueueConfig::default()).expect("ready queue creation");
        TestServiceState::new(
            db,
            session_id,
            JobCache::new(),
            MockReadyQueueSender,
            receiver,
            MockTaskInstancePoolConnector,
        )
    }

    /// Creates a [`ServiceState`] backed by [`ReadyQueueReceiverHandle`].
    ///
    /// # Returns
    ///
    /// A tuple of the service state and the ready queue sender handle on success.
    fn create_test_service_with_ready_queue(
        db: MockDbConnector,
    ) -> (TestServiceStateWithReadyQueue, ReadyQueueSenderHandle) {
        use crate::ready_queue::{ReadyQueueConfig, create_ready_queue};
        let (sender, receiver) =
            create_ready_queue(&ReadyQueueConfig::default()).expect("ready queue creation");
        let service = TestServiceStateWithReadyQueue::new(
            db,
            0,
            JobCache::new(),
            sender.clone(),
            receiver,
            MockTaskInstancePoolConnector,
        );
        (service, sender)
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

    fn create_test_job_submission() -> (String, Vec<u8>) {
        let task_graph = create_test_task_graph()
            .to_json()
            .expect("task graph serialization should succeed");
        let mut serializer = spider_tdl::wire::TaskInputsSerializer::new();
        serializer
            .append(TaskInput::ValuePayload(vec![0u8; 4]))
            .expect("input serialization should succeed");
        (task_graph, serializer.release())
    }

    fn create_test_serialized_outputs() -> Vec<u8> {
        let output_tuple = (1,);
        TaskOutputsSerializer::from_tuple(&output_tuple)
            .expect("output serialization should succeed")
    }

    fn create_empty_serialized_inputs() -> Vec<u8> {
        spider_tdl::wire::TaskInputsSerializer::new().release()
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
            ResourceGroupId::random(),
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
        let (serialized_task_graph, serialized_inputs) = create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                serialized_task_graph,
                serialized_inputs,
            )
            .await?;
        assert!(
            service.inner.job_cache.get(job_id).await.is_some(),
            "JCB should be in cache after register_job"
        );
        Ok(())
    }

    #[tokio::test]
    async fn register_job_returns_error_on_invalid_task_graph() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service
            .register_job(
                ResourceGroupId::random(),
                "invalid json".to_owned(),
                create_empty_serialized_inputs(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageServerError::Task(_))),
            "register_job should return Task error on invalid task graph JSON"
        );
        Ok(())
    }

    #[tokio::test]
    async fn register_job_returns_error_on_input_size_mismatch() -> anyhow::Result<()> {
        let service = create_test_service();
        let task_graph = create_test_task_graph()
            .to_json()
            .expect("task graph serialization should succeed");
        let result = service
            .register_job(
                ResourceGroupId::random(),
                task_graph,
                create_empty_serialized_inputs(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageServerError::Cache(_))),
            "register_job should return Cache error on input size mismatch"
        );
        Ok(())
    }

    #[tokio::test]
    async fn register_job_returns_error_on_empty_task_graph() -> anyhow::Result<()> {
        let service = create_test_service();
        let task_graph = TaskGraph::new(None, None)
            .expect("empty task graph creation should succeed")
            .to_json()
            .expect("task graph serialization should succeed");
        let result = service
            .register_job(
                ResourceGroupId::random(),
                task_graph,
                create_empty_serialized_inputs(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageServerError::Cache(_))),
            "register_job should return Cache error on empty task graph"
        );
        Ok(())
    }

    #[tokio::test]
    async fn start_job_starts_cached_job() -> anyhow::Result<()> {
        let service = create_test_service();
        let (serialized_task_graph, serialized_inputs) = create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                serialized_task_graph,
                serialized_inputs,
            )
            .await?;

        service.start_job(job_id).await?;

        let state = service.get_job_state(job_id).await?;
        assert_eq!(state, JobState::Running);
        Ok(())
    }

    #[tokio::test]
    async fn start_job_returns_job_not_found_when_not_in_cache() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service.start_job(JobId::random()).await;
        assert!(
            matches!(result, Err(StorageServerError::JobNotFound(_))),
            "start_job should return JobNotFound when job is not in cache"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancel_job_returns_job_not_found_if_not_exist() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service.cancel_job(JobId::random()).await;
        assert!(
            matches!(result, Err(StorageServerError::JobNotFound(_))),
            "cancel_job should return JobNotFound when job is not in cache or DB"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancel_job_returns_terminal_state_from_db_when_not_in_cache() -> anyhow::Result<()> {
        let db = MockDbConnector::default();
        let job_id = JobId::random();
        db.states.insert(job_id, JobState::Cancelled);

        let service = create_test_service_with_db(db);
        let state = service.cancel_job(job_id).await?;
        assert_eq!(
            state,
            JobState::Cancelled,
            "cancel_job should return Cancelled when job is already cancelled in DB"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancel_job_transitions_to_terminal_state() -> anyhow::Result<()> {
        let service = create_test_service();
        let job_id = JobId::random();
        let jcb = create_test_jcb(job_id).await;
        service.inner.job_cache.insert(jcb).await?;

        let state = service.cancel_job(job_id).await?;
        assert!(
            matches!(state, JobState::Cancelled),
            "cancel should result in terminal state"
        );
        assert!(
            service.inner.job_cache.get(job_id).await.is_some(),
            "JCB should remain in cache after terminal cancel"
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_job_state_serves_from_cache_when_jcb_present() -> anyhow::Result<()> {
        let service = create_test_service();
        let (serialized_task_graph, serialized_inputs) = create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                serialized_task_graph,
                serialized_inputs,
            )
            .await?;

        let state = service.get_job_state(job_id).await?;
        assert_eq!(state, JobState::Ready);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_state_falls_back_to_db_when_not_in_cache() -> anyhow::Result<()> {
        let db = MockDbConnector::default();
        let job_id = JobId::random();
        db.states.insert(job_id, JobState::Failed);

        let service = create_test_service_with_db(db);
        let state = service.get_job_state(job_id).await?;
        assert_eq!(state, JobState::Failed);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_state_returns_error_for_unknown_job() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service.get_job_state(JobId::random()).await;
        assert!(result.is_err(), "get_job_state should fail for unknown job");
        Ok(())
    }

    #[tokio::test]
    async fn get_job_outputs_returns_outputs_from_db() -> anyhow::Result<()> {
        let db = MockDbConnector::default();
        let job_id = JobId::random();
        let outputs: Vec<TaskOutput> = vec![vec![1, 2, 3]];
        db.outputs.insert(job_id, outputs.clone());

        let service = create_test_service_with_db(db);
        let result = service.get_job_outputs(job_id).await?;
        assert_eq!(result, outputs);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_outputs_returns_outputs_from_cache_when_jcb_present() -> anyhow::Result<()> {
        let service = create_test_service();
        let (serialized_task_graph, serialized_inputs) = create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                serialized_task_graph,
                serialized_inputs,
            )
            .await?;
        service.start_job(job_id).await?;

        let context = service
            .create_task_instance(
                TEST_SESSION_ID,
                job_id,
                TaskId::Index(0),
                ExecutionManagerId::random(),
            )
            .await?;
        let serialized_outputs = create_test_serialized_outputs();
        let expected = TaskOutputsSerializer::deserialize(&serialized_outputs)
            .expect("test serialized outputs should deserialize successfully");
        service
            .succeed_task_instance(
                TEST_SESSION_ID,
                job_id,
                context.task_instance_id,
                0,
                serialized_outputs,
            )
            .await?;

        let actual = service.get_job_outputs(job_id).await?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_outputs_returns_error_for_unknown_job() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service.get_job_outputs(JobId::random()).await;
        assert!(
            result.is_err(),
            "get_job_outputs should fail for unknown job"
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_job_outputs_returns_error_when_job_not_succeeded() -> anyhow::Result<()> {
        let service = create_test_service();
        let (serialized_task_graph, serialized_inputs) = create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                serialized_task_graph,
                serialized_inputs,
            )
            .await?;
        // JCB is in cache but job is still Ready (not Succeeded).
        let result = service.get_job_outputs(job_id).await;
        assert!(
            result.is_err(),
            "get_job_outputs should fail when job is not Succeeded"
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_job_error_returns_error_message_from_db() -> anyhow::Result<()> {
        let db = MockDbConnector::default();
        let job_id = JobId::random();
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
        let result = service.get_job_error(JobId::random()).await;
        assert!(
            matches!(result, Err(StorageServerError::Db(DbError::JobNotFound(_)))),
            "get_job_error should fail for unknown job"
        );
        Ok(())
    }

    #[tokio::test]
    async fn create_task_instance_returns_execution_context() -> anyhow::Result<()> {
        let service = create_test_service();
        let (serialized_task_graph, serialized_inputs) = create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                serialized_task_graph,
                serialized_inputs,
            )
            .await?;
        service.start_job(job_id).await?;

        let context = service
            .create_task_instance(
                TEST_SESSION_ID,
                job_id,
                TaskId::Index(0),
                ExecutionManagerId::random(),
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
                JobId::random(),
                TaskId::Index(0),
                ExecutionManagerId::random(),
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
        let (serialized_task_graph, serialized_inputs) = create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                serialized_task_graph,
                serialized_inputs,
            )
            .await?;
        service.start_job(job_id).await?;

        let context = service
            .create_task_instance(
                TEST_SESSION_ID,
                job_id,
                TaskId::Index(0),
                ExecutionManagerId::random(),
            )
            .await?;
        let state = service
            .succeed_task_instance(
                TEST_SESSION_ID,
                job_id,
                context.task_instance_id,
                0,
                create_test_serialized_outputs(),
            )
            .await?;
        assert_eq!(state, JobState::Succeeded);
        assert!(
            service.inner.job_cache.get(job_id).await.is_some(),
            "JCB should remain in cache after terminal succeed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn succeed_task_instance_returns_job_not_found_when_not_in_cache() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service
            .succeed_task_instance(
                TEST_SESSION_ID,
                JobId::random(),
                1,
                0,
                create_test_serialized_outputs(),
            )
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
        let (serialized_task_graph, serialized_inputs) = create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                serialized_task_graph,
                serialized_inputs,
            )
            .await?;
        service.start_job(job_id).await?;

        let context = service
            .create_task_instance(
                TEST_SESSION_ID,
                job_id,
                TaskId::Index(0),
                ExecutionManagerId::random(),
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
            service.inner.job_cache.get(job_id).await.is_some(),
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
                JobId::random(),
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
    async fn task_instance_orchestration_return_stale_session_on_mismatch() -> anyhow::Result<()> {
        // Create a service with a higher session ID to simulate a server restart.
        const CURRENT_SESSION_ID: SessionId = 10;
        const STALE_SESSION_ID: SessionId = CURRENT_SESSION_ID - 1;
        const TASK_INDEX: TaskIndex = 0;
        const TASK_INSTANCE_ID: TaskInstanceId = 1;

        let db = MockDbConnector::default();
        let service = create_test_service_with_db_and_session(db, CURRENT_SESSION_ID);

        // Register a job so the JCB is in cache.
        let (serialized_task_graph, serialized_inputs) = create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                serialized_task_graph,
                serialized_inputs,
            )
            .await?;

        {
            let result = service
                .create_task_instance(
                    STALE_SESSION_ID,
                    job_id,
                    TaskId::Index(TASK_INDEX),
                    ExecutionManagerId::random(),
                )
                .await;
            assert!(
                matches!(result, Err(StorageServerError::StaleSession)),
                "create_task_instance should return StaleSession on session mismatch"
            );
        }

        {
            let result = service
                .succeed_task_instance(
                    STALE_SESSION_ID,
                    job_id,
                    TASK_INSTANCE_ID,
                    TASK_INDEX,
                    create_test_serialized_outputs(),
                )
                .await;
            assert!(
                matches!(result, Err(StorageServerError::StaleSession)),
                "succeed_task_instance should return StaleSession on session mismatch"
            );
        }

        {
            let result = service
                .fail_task_instance(
                    STALE_SESSION_ID,
                    job_id,
                    TASK_INSTANCE_ID,
                    TaskId::Index(TASK_INDEX),
                    "error".to_owned(),
                )
                .await;
            assert!(
                matches!(result, Err(StorageServerError::StaleSession)),
                "fail_task_instance should return StaleSession on session mismatch"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn add_resource_group_returns_id() -> anyhow::Result<()> {
        let service = create_test_service();
        assert!(
            service
                .add_resource_group("external_123".to_owned(), vec![1, 2, 3])
                .await
                .is_ok()
        );
        Ok(())
    }

    #[tokio::test]
    async fn verify_resource_group_succeeds_for_correct_password() -> anyhow::Result<()> {
        let service = create_test_service();
        let password = vec![1, 2, 3];
        let rg_id = service
            .add_resource_group("external_123".to_owned(), password.clone())
            .await?;
        service.verify_resource_group(rg_id, &password).await?;
        Ok(())
    }

    #[tokio::test]
    async fn verify_resource_group_fails_for_wrong_password() -> anyhow::Result<()> {
        let service = create_test_service();
        let rg_id = service
            .add_resource_group("external_123".to_owned(), vec![1, 2, 3])
            .await?;
        let result = service.verify_resource_group(rg_id, &[4, 5, 6]).await;
        assert!(result.is_err(), "verify should fail for wrong password");
        Ok(())
    }

    #[tokio::test]
    async fn poll_ready_tasks_returns_entries_from_ready_queue() -> anyhow::Result<()> {
        const TASK_INDEX: TaskIndex = 0;
        let (service, sender) = create_test_service_with_ready_queue(MockDbConnector::default());
        let rg_id = ResourceGroupId::random();
        let job_id = JobId::random();
        sender
            .send_task_ready(rg_id, job_id, vec![TASK_INDEX])
            .await
            .expect("send_task_ready should succeed");

        let entries = service
            .poll_ready_tasks(10, Duration::from_millis(100))
            .await?;
        assert_eq!(entries.len(), 1, "should receive one ready queue entry");
        assert_eq!(entries[0].job_id, job_id);
        assert_eq!(entries[0].task_kind, TASK_INDEX);
        assert_eq!(entries[0].resource_group_id, rg_id);
        Ok(())
    }

    #[tokio::test]
    async fn poll_ready_tasks_returns_empty_when_no_tasks() -> anyhow::Result<()> {
        let (service, _sender) = create_test_service_with_ready_queue(MockDbConnector::default());
        let entries = service
            .poll_ready_tasks(10, Duration::from_millis(10))
            .await?;
        assert!(
            entries.is_empty(),
            "should receive no entries from empty ready queue"
        );
        Ok(())
    }

    #[tokio::test]
    async fn poll_commit_ready_tasks_returns_entries_from_ready_queue() -> anyhow::Result<()> {
        let (service, sender) = create_test_service_with_ready_queue(MockDbConnector::default());
        let rg_id = ResourceGroupId::random();
        let job_id = JobId::random();
        sender
            .send_commit_ready(rg_id, job_id)
            .await
            .expect("send_commit_ready should succeed");

        let entries = service
            .poll_commit_ready_tasks(10, Duration::from_millis(100))
            .await?;
        assert_eq!(entries.len(), 1, "should receive one commit-ready entry");
        assert_eq!(entries[0].job_id, job_id);
        assert_eq!(entries[0].task_kind, CommitTaskMarker);
        assert_eq!(entries[0].resource_group_id, rg_id);
        Ok(())
    }

    #[tokio::test]
    async fn poll_cleanup_ready_tasks_returns_entries_from_ready_queue() -> anyhow::Result<()> {
        let (service, sender) = create_test_service_with_ready_queue(MockDbConnector::default());
        let rg_id = ResourceGroupId::random();
        let job_id = JobId::random();
        sender
            .send_cleanup_ready(rg_id, job_id)
            .await
            .expect("send_cleanup_ready should succeed");

        let entries = service
            .poll_cleanup_ready_tasks(10, Duration::from_millis(100))
            .await?;
        assert_eq!(entries.len(), 1, "should receive one cleanup-ready entry");
        assert_eq!(entries[0].job_id, job_id);
        assert_eq!(entries[0].task_kind, CleanupTaskMarker);
        assert_eq!(entries[0].resource_group_id, rg_id);
        Ok(())
    }

    #[tokio::test]
    async fn register_execution_manager_returns_id() -> anyhow::Result<()> {
        let service = create_test_service();
        assert!(
            service
                .register_execution_manager("127.0.0.1".parse()?)
                .await
                .is_ok()
        );
        Ok(())
    }

    #[tokio::test]
    async fn update_execution_manager_heartbeat_succeeds_for_registered_em() -> anyhow::Result<()> {
        let service = create_test_service();
        let em_id = service
            .register_execution_manager("127.0.0.1".parse()?)
            .await?;
        service.update_execution_manager_heartbeat(em_id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn update_execution_manager_heartbeat_fails_for_unknown_em() -> anyhow::Result<()> {
        let service = create_test_service();
        let result = service
            .update_execution_manager_heartbeat(ExecutionManagerId::random())
            .await;

        assert!(
            matches!(
                result,
                Err(StorageServerError::Db(DbError::IllegalExecutionManagerId(
                    _
                )))
            ),
            "update heartbeat should fail for unregistered execution manager"
        );
        Ok(())
    }
}
