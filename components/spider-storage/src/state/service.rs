use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use spider_core::job::JobState;
use spider_core::task::TaskIndex;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SchedulerId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::ExecutionContext;
use spider_core::types::io::TaskOutput;
use spider_core::types::io::TaskOutputsSerializer;
use spider_core::types::resource_group::ExternalResourceGroupCredentials;
use spider_core::types::scheduler::RegisteredScheduler;
use spider_tdl::error::TdlError;
use spider_utils::config::Host;
use spider_utils::shutdown::ShutdownHandle;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;

use crate::cache::error::CacheError;
use crate::cache::error::InternalError;
use crate::cache::error::StaleStateError;
use crate::cache::job::SharedJobControlBlock;
use crate::db::DbStorage;
use crate::inbound_queue::CleanupTaskMarker;
use crate::inbound_queue::CommitTaskMarker;
use crate::inbound_queue::InboundQueueEntry;
use crate::inbound_queue::InboundQueueReceiverHandle;
use crate::inbound_queue::InboundQueueSender;
use crate::job_submission::ValidatedJobSubmission;
use crate::state::JobCache;
use crate::state::JobCacheGcHandle;
use crate::state::StorageServerError;
use crate::task_instance_pool::TaskInstancePoolConnector;
use crate::test_hooks::TestHooks;

/// Bundle of constructor parameters for [`ServiceState::new`].
///
/// This is a work-around for silencing the ` clippy::too_many_arguments ` warning.
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
pub struct ServiceStateParams<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> {
    pub db: DbConnectorType,
    pub session_id: SessionId,
    pub job_cache: JobCache<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    pub inbound_queue_sender: InboundQueueSenderType,
    pub inbound_queue_receiver: InboundQueueReceiverHandle,
    pub task_instance_pool_connector: TaskInstancePoolConnectorType,
    pub job_cache_gc_handle: JobCacheGcHandle,
    pub shutdown_handle: ShutdownHandle,
    pub hooks: TestHooks,
}

/// Per-request service state providing access to the storage layer.
///
/// Internally wraps a single [`Arc`] around [`ServiceStateInner`] so that cloning is cheap (one
/// Arc clone instead of cloning each field).
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
#[derive(Clone)]
pub struct ServiceState<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> {
    inner: Arc<
        ServiceStateInner<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    >,
}

impl<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> ServiceState<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created [`ServiceState`] that notifies the GC actor when cached jobs terminate.
    #[must_use]
    pub fn new(
        params: ServiceStateParams<
            InboundQueueSenderType,
            DbConnectorType,
            TaskInstancePoolConnectorType,
        >,
    ) -> Self {
        let ServiceStateParams {
            db,
            session_id,
            job_cache,
            inbound_queue_sender,
            inbound_queue_receiver,
            task_instance_pool_connector,
            job_cache_gc_handle,
            shutdown_handle,
            hooks,
        } = params;
        hooks.register_runtime(&shutdown_handle);
        Self {
            inner: Arc::new(ServiceStateInner {
                db,
                session_id,
                job_cache,
                inbound_queue_sender,
                inbound_queue_receiver,
                task_instance_pool_connector,
                job_cache_gc_handle,
                has_previous_scheduler_connection: tokio::sync::Mutex::new(false),
                shutdown_handle,
                task_tracker: TaskTracker::new(),
                hooks,
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

    /// # Returns
    ///
    /// The tracker of the service's spawned handler bodies.
    #[must_use]
    pub(crate) fn task_tracker(&self) -> &TaskTracker {
        &self.inner.task_tracker
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
    /// * Forwards [`ValidatedJobSubmission::create`]'s return values as
    ///   [`StorageServerError::BadRequest`] on failure.
    /// * Forwards [`ExternalJobOrchestration::register`]'s return values on failure.
    /// * Forwards [`SharedJobControlBlock::create`]'s return values on failure.
    /// * Forwards [`JobCache::insert`]'s return values on failure.
    /// * Forwards [`Self::spawn_body`]'s return values on failure.
    pub async fn register_job(
        &self,
        resource_group_id: ResourceGroupId,
        compressed_serialized_task_graph: Vec<u8>,
        compressed_serialized_task_graph_input: Vec<u8>,
    ) -> Result<JobId, StorageServerError> {
        let job_submission = ValidatedJobSubmission::create(
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .map_err(|e| StorageServerError::BadRequest(e.to_string()))?;

        let this = self.clone();
        self.spawn_body(async move {
            let job_id = this
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
                this.inner.inbound_queue_sender.clone(),
                this.inner.db.clone(),
                this.inner.task_instance_pool_connector.clone(),
                this.inner.hooks.clone(),
            )
            .await?;

            this.inner.job_cache.insert(jcb).await?;
            tracing::info!(
                job_id = ? job_id,
                rg_id = ? resource_group_id,
                "Job inserted in cache.",
            );

            Ok(job_id)
        })
        .await
    }

    /// Starts a job for execution.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::JobNotFound`] if the job is not in a terminal state, but it is not in the
    ///   cache.
    /// * [`StaleStateError::JobAlreadyTerminated`] if the job is not in the cache and is in a
    ///   terminal state in the database.
    /// * Forwards [`SharedJobControlBlock::start`]'s return values on failure.
    /// * Forwards [`ExternalJobOrchestration::get_state`]'s return values on failure.
    /// * Forwards [`Self::spawn_body`]'s return values on failure.
    pub async fn start_job(&self, job_id: JobId) -> Result<(), StorageServerError> {
        let this = self.clone();
        self.spawn_body(async move {
            if let Some(jcb) = this.inner.job_cache.get(job_id).await {
                jcb.start().await?;
                tracing::info!(
                    job_id = ? job_id,
                    "Job started.",
                );
                return Ok(());
            }
            let state = this.inner.db.get_state(job_id).await?;
            if !state.is_terminal() {
                return Err(StorageServerError::Cache(CacheError::Internal(
                    InternalError::JobNotFound(job_id),
                )));
            }
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::JobAlreadyTerminated(state),
            )))
        })
        .await
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
    /// * Forwards [`Self::spawn_body`]'s return values on failure.
    pub async fn cancel_job(&self, job_id: JobId) -> Result<JobState, StorageServerError> {
        let this = self.clone();
        self.spawn_body(async move {
            if let Some(jcb) = this.inner.job_cache.get(job_id).await {
                let state = jcb.cancel().await?;
                this.enqueue_for_gc_if_terminal(job_id, state);
                tracing::info!(
                    job_id = ? job_id,
                    "Job cancelled.",
                );
                return Ok(state);
            }
            match this.inner.db.get_state(job_id).await {
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
        })
        .await
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

    /// Resends ready tasks for all jobs in the cache to the inbound queue.
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
    /// * Forwards [`Self::spawn_body`]'s return values on failure.
    pub async fn create_task_instance(
        &self,
        session_id: SessionId,
        job_id: JobId,
        task_id: TaskId,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<ExecutionContext, StorageServerError> {
        self.validate_session(session_id)?;
        let this = self.clone();
        self.spawn_body(async move {
            let jcb = this
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
        })
        .await
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
    /// * Forwards [`spider_core::types::io::TaskOutputsSerializer::deserialize`]'s return values on
    ///   failure.
    /// * Forwards [`SharedJobControlBlock::succeed_task_instance`]'s return values on failure.
    /// * Forwards [`Self::spawn_body`]'s return values on failure.
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
        let this = self.clone();
        self.spawn_body(async move {
            let jcb = this
                .inner
                .job_cache
                .get(job_id)
                .await
                .ok_or(StorageServerError::JobNotFound(job_id))?;
            let state = jcb
                .succeed_task_instance(task_instance_id, task_index, task_outputs)
                .await?;
            this.enqueue_for_gc_if_terminal(job_id, state);
            tracing::info!(
                job_id = ? job_id,
                task_id = ? TaskId::Index(task_index),
                task_instance_id = ? task_instance_id,
                job_state = ? state,
                "Task instance succeeded.",
            );
            Ok(state)
        })
        .await
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
    /// * Forwards [`Self::spawn_body`]'s return values on failure.
    pub async fn succeed_commit_task_instance(
        &self,
        session_id: SessionId,
        job_id: JobId,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, StorageServerError> {
        self.validate_session(session_id)?;
        let this = self.clone();
        self.spawn_body(async move {
            let jcb = this
                .inner
                .job_cache
                .get(job_id)
                .await
                .ok_or(StorageServerError::JobNotFound(job_id))?;
            let state = jcb.succeed_commit_task_instance(task_instance_id).await?;
            this.enqueue_for_gc_if_terminal(job_id, state);
            tracing::info!(
                job_id = ? job_id,
                task_id = ? TaskId::Commit,
                task_instance_id = ? task_instance_id,
                job_state = ? state,
                "Task instance succeeded.",
            );
            Ok(state)
        })
        .await
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
    /// * Forwards [`Self::spawn_body`]'s return values on failure.
    pub async fn succeed_cleanup_task_instance(
        &self,
        session_id: SessionId,
        job_id: JobId,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, StorageServerError> {
        self.validate_session(session_id)?;
        let this = self.clone();
        self.spawn_body(async move {
            let jcb = this
                .inner
                .job_cache
                .get(job_id)
                .await
                .ok_or(StorageServerError::JobNotFound(job_id))?;
            let state = jcb.succeed_cleanup_task_instance(task_instance_id).await?;
            this.enqueue_for_gc_if_terminal(job_id, state);
            tracing::info!(
                job_id = ? job_id,
                task_id = ? TaskId::Cleanup,
                task_instance_id = ? task_instance_id,
                job_state = ? state,
                "Task instance succeeded.",
            );
            Ok(state)
        })
        .await
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
    /// * Forwards [`Self::spawn_body`]'s return values on failure.
    pub async fn fail_task_instance(
        &self,
        session_id: SessionId,
        job_id: JobId,
        task_instance_id: TaskInstanceId,
        task_id: TaskId,
        error: String,
    ) -> Result<JobState, StorageServerError> {
        self.validate_session(session_id)?;
        let this = self.clone();
        self.spawn_body(async move {
            let jcb = this
                .inner
                .job_cache
                .get(job_id)
                .await
                .ok_or(StorageServerError::JobNotFound(job_id))?;
            let state = jcb
                .fail_task_instance(task_instance_id, task_id, error)
                .await?;
            this.enqueue_for_gc_if_terminal(job_id, state);
            tracing::info!(
                job_id = ? job_id,
                task_id = ? task_id,
                task_instance_id = ? task_instance_id,
                job_state = ? state,
                "Task instance failed.",
            );
            Ok(state)
        })
        .await
    }

    /// Adds a resource group with the given credentials.
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
        credentials: ExternalResourceGroupCredentials,
    ) -> Result<ResourceGroupId, StorageServerError> {
        let rg_id = self.inner.db.add(credentials).await?;
        tracing::info!(
            rg_id = ? rg_id,
            "Resource group added.",
        );
        Ok(rg_id)
    }

    /// Adds a resource group or verifies the password of an existing resource group.
    ///
    /// # Returns
    ///
    /// The ID of the created or verified resource group on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ResourceGroupManagement::add_or_verify`]'s return values on failure.
    pub async fn add_or_verify_resource_group(
        &self,
        credentials: ExternalResourceGroupCredentials,
    ) -> Result<ResourceGroupId, StorageServerError> {
        let rg_id = self.inner.db.add_or_verify(credentials).await?;
        tracing::info!(
            rg_id = ? rg_id,
            "Resource group added or verified.",
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

    /// Polls the inbound queue for task entries.
    ///
    /// # Returns
    ///
    /// Up to `max_tasks` inbound queue entries received within the `wait` duration on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`InboundQueueReceiverHandle::recv_tasks`]'s return values on failure.
    pub async fn poll_ready_tasks(
        &self,
        max_tasks: usize,
        wait: Duration,
    ) -> Result<Vec<InboundQueueEntry<TaskIndex>>, StorageServerError> {
        self.inner
            .inbound_queue_receiver
            .recv_tasks(max_tasks, wait)
            .await
            .map_err(|e| CacheError::Internal(e).into())
    }

    /// Polls the inbound queue for commit-ready task entries.
    ///
    /// # Returns
    ///
    /// Up to `max_tasks` commit-ready task entries received within the `wait` duration on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`InboundQueueReceiverHandle::recv_commits`]'s return values on failure.
    pub async fn poll_commit_ready_tasks(
        &self,
        max_tasks: usize,
        wait: Duration,
    ) -> Result<Vec<InboundQueueEntry<CommitTaskMarker>>, StorageServerError> {
        self.inner
            .inbound_queue_receiver
            .recv_commits(max_tasks, wait)
            .await
            .map_err(|e| CacheError::Internal(e).into())
    }

    /// Polls the inbound queue for cleanup-ready task entries.
    ///
    /// # Returns
    ///
    /// Up to `max_tasks` cleanup-ready task entries received within the `wait` duration on
    /// success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`InboundQueueReceiverHandle::recv_cleanups`]'s return values on failure.
    pub async fn poll_cleanup_ready_tasks(
        &self,
        max_tasks: usize,
        wait: Duration,
    ) -> Result<Vec<InboundQueueEntry<CleanupTaskMarker>>, StorageServerError> {
        self.inner
            .inbound_queue_receiver
            .recv_cleanups(max_tasks, wait)
            .await
            .map_err(|e| CacheError::Internal(e).into())
    }

    /// Registers an execution manager.
    ///
    /// # Returns
    ///
    /// A tuple on success, containing:
    ///
    /// * The ID of the registered execution manager.
    /// * The ID of the resource group to which the execution manager is dedicated, if any.
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
        resource_group_credentials: Option<ExternalResourceGroupCredentials>,
    ) -> Result<(ExecutionManagerId, Option<ResourceGroupId>), StorageServerError> {
        let registration = self
            .inner
            .db
            .register_execution_manager(ip_address, resource_group_credentials)
            .await?;
        tracing::info!(
            em_id = ? registration.0,
            rg_id = ? registration.1,
            ip = ? ip_address,
            "Execution manager registered.",
        );
        Ok(registration)
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

    /// Registers a scheduler.
    ///
    /// Scheduler registration is mutually exclusive: only one registration request can be processed
    /// at a time. Registering a scheduler invalidates any scheduler that was previously registered.
    ///
    /// If this replaces an existing scheduler, all ready tasks are re-enqueued in a background task
    /// so they become visible in the inbound queue for the newly registered scheduler.
    ///
    /// # Returns
    ///
    /// The ID of the registered scheduler on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`SchedulerRegistrationManagement::register_scheduler`]'s return values on
    ///   failure.
    pub async fn register_scheduler(
        &self,
        host: Host,
        port: u16,
    ) -> Result<SchedulerId, StorageServerError> {
        let mut has_previous_scheduler_connection =
            self.inner.has_previous_scheduler_connection.lock().await;
        let scheduler_id = self
            .inner
            .db
            .register_scheduler(host.as_str(), port)
            .await?;
        tracing::info!(
            scheduler_id = ? scheduler_id,
            host = % host,
            port,
            "Scheduler registered.",
        );
        if *has_previous_scheduler_connection {
            tracing::info!(
                "Previous scheduler connection has been invalidated. Resending all ready-tasks in \
                 a background task."
            );
            let job_cache = self.inner.job_cache.clone();
            let shutdown_handle = self.inner.shutdown_handle.clone();
            tokio::spawn(async move {
                if let Err(e) = job_cache.resend_ready_tasks().await {
                    tracing::error!(
                        error = % e,
                        "Failed to resend ready-tasks after scheduler registration. Cancelling the \
                         service."
                    );
                    shutdown_handle.shut_down_fatally();
                }
            });
        }
        *has_previous_scheduler_connection = true;
        drop(has_previous_scheduler_connection);
        Ok(scheduler_id)
    }

    /// Gets registered schedulers.
    ///
    /// # Returns
    ///
    /// The registered schedulers on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`SchedulerRegistrationManagement::get_schedulers`]'s return values on failure.
    pub async fn get_schedulers(&self) -> Result<Vec<RegisteredScheduler>, StorageServerError> {
        self.inner
            .db
            .get_schedulers()
            .await
            .map_err(StorageServerError::from)
    }

    /// Validates that the given `session_id` matches the session ID captured at service creation
    /// time.
    ///
    /// # Errors
    ///
    /// Returns [`StorageServerError::StaleSession`] if the session IDs don't match.
    fn validate_session(&self, session_id: SessionId) -> Result<(), StorageServerError> {
        if session_id != self.inner.session_id {
            return Err(StorageServerError::StaleSession(self.inner.session_id));
        }
        Ok(())
    }

    /// Enqueues a job for delayed cache GC if it has reached a terminal state.
    fn enqueue_for_gc_if_terminal(&self, job_id: JobId, state: JobState) {
        if state.is_terminal() {
            self.inner
                .job_cache_gc_handle
                .enqueue_terminated_job(job_id);
        }
    }

    /// Runs a handler body as a task in the service's task tracker, so that dropping the caller's
    /// future does not cut the body short, and waits for its result, raced against the service's
    /// shutdown.
    ///
    /// If the caller stops waiting for the result, a panic of the body still shuts the service down
    /// fatally, and so does a fatal error (see [`StorageServerError::is_fatal`]) unless the service
    /// is already shutting down gracefully.
    ///
    /// # Type Parameters
    ///
    /// * `ReturnType` - The type of the value that the body returns on success.
    ///
    /// # Returns
    ///
    /// The body's result on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::Stopping`] if:
    ///   * The service is already shutting down when called, in which case the body is not run.
    ///   * The service is shutting down before the body finishes.
    ///   * The body fails while the service is shutting down without a fatal error.
    /// * [`InternalError::HandlerBodyFailed`] if the body panicked.
    /// * Forwards `body`'s return values on failure.
    async fn spawn_body<ReturnType: Send + 'static>(
        &self,
        body: impl Future<Output = Result<ReturnType, StorageServerError>> + Send + 'static,
    ) -> Result<ReturnType, StorageServerError> {
        let shutdown_handle = &self.inner.shutdown_handle;
        // Keeps the tracker non-empty between the shutdown check and the spawn, so that
        // `Runtime::stop`'s `wait()` cannot return in that gap and miss the body.
        let task_tracker_token = self.inner.task_tracker.token();
        if shutdown_handle.is_cancelled() {
            return Err(Self::stopping_error());
        }
        let join_handle = self.inner.task_tracker.spawn(body);
        drop(task_tracker_token);
        let mut handler_body_join_guard = HandlerBodyJoinGuard {
            join_handle: Some(join_handle),
            task_tracker: self.inner.task_tracker.clone(),
            shutdown_handle: shutdown_handle.clone(),
        };
        tokio::select! {
            biased;
            result = &mut handler_body_join_guard => match result {
                Ok(Err(e)) if shutdown_handle.is_cancelled() && !shutdown_handle.is_fatal() => {
                    tracing::warn!(
                        error = % e,
                        "Handler body failed while the service is shutting down."
                    );
                    Err(Self::stopping_error())
                }
                Ok(result) => result,
                Err(e) => Err(
                    CacheError::Internal(InternalError::HandlerBodyFailed(e.to_string())).into()
                ),
            },
            () = shutdown_handle.cancelled() => Err(Self::stopping_error()),
        }
    }

    /// # Returns
    ///
    /// The error of a handler that the service's shutdown stopped.
    fn stopping_error() -> StorageServerError {
        StorageServerError::Stopping("storage is shutting down".to_owned())
    }
}

/// Inner data for [`ServiceState`], holding all storage services.
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
struct ServiceStateInner<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> {
    db: DbConnectorType,
    session_id: SessionId,
    job_cache: JobCache<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    inbound_queue_sender: InboundQueueSenderType,
    inbound_queue_receiver: InboundQueueReceiverHandle,
    task_instance_pool_connector: TaskInstancePoolConnectorType,
    job_cache_gc_handle: JobCacheGcHandle,
    has_previous_scheduler_connection: tokio::sync::Mutex<bool>,
    shutdown_handle: ShutdownHandle,
    task_tracker: TaskTracker,
    hooks: TestHooks,
}

/// The [`JoinHandle`] of a handler body spawned by [`ServiceState::spawn_body`], which resolves to
/// the body's result.
///
/// If dropped before the result is read, it spawns a task into the task tracker that waits for the
/// body and shuts the service down fatally on a panic, or on a fatal error unless the service is
/// already shutting down gracefully.
///
/// # Type Parameters
///
/// * `ReturnType` - The type of the value that the body returns on success.
struct HandlerBodyJoinGuard<ReturnType: Send + 'static> {
    join_handle: Option<JoinHandle<Result<ReturnType, StorageServerError>>>,
    task_tracker: TaskTracker,
    shutdown_handle: ShutdownHandle,
}

impl<ReturnType: Send + 'static> Future for HandlerBodyJoinGuard<ReturnType> {
    type Output = Result<Result<ReturnType, StorageServerError>, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let join_handle = self
            .join_handle
            .as_mut()
            .expect("the handler body's result should not be read twice");
        let output = std::task::ready!(Pin::new(join_handle).poll(cx));
        self.join_handle = None;
        Poll::Ready(output)
    }
}

impl<ReturnType: Send + 'static> Drop for HandlerBodyJoinGuard<ReturnType> {
    fn drop(&mut self) {
        let Some(join_handle) = self.join_handle.take() else {
            return;
        };
        let shutdown_handle = self.shutdown_handle.clone();
        self.task_tracker.spawn(async move {
            match join_handle.await {
                Ok(Err(e)) if e.is_fatal() => {
                    if shutdown_handle.is_cancelled() && !shutdown_handle.is_fatal() {
                        tracing::warn!(
                            error = % e,
                            "Abandoned handler body failed while the service is shutting down."
                        );
                    } else {
                        tracing::error!(
                            error = % e,
                            "Abandoned handler body failed with a fatal error. Cancelling service."
                        );
                        shutdown_handle.shut_down_fatally();
                    }
                }
                Err(e) if e.is_panic() => {
                    tracing::error!(
                        error = % e,
                        "Abandoned handler body panicked. Cancelling service."
                    );
                    shutdown_handle.shut_down_fatally();
                }
                _ => {}
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use spider_core::compression::encode_zstd_bytes;
    use spider_core::job::JobState;
    use spider_core::task::ExecutionPolicy;
    use spider_core::task::TaskGraph as SubmittedTaskGraph;
    use spider_core::task::TdlContext;
    use spider_core::task::TerminationTaskDescriptor;
    use spider_core::types::id::ExecutionManagerId;
    use spider_core::types::id::JobId;
    use spider_core::types::id::ResourceGroupId;
    use spider_core::types::io::TaskGraphInputBuilder;
    use spider_core::types::io::TaskOutput;

    use super::*;
    use crate::cache::job::SharedJobControlBlock;
    use crate::db::DbError;
    use crate::db::RecoverableJobContext;
    use crate::inbound_queue::InboundQueueSenderHandle;
    use crate::job_submission::compress_job_inputs;
    use crate::job_submission::compress_task_graph;
    use crate::job_submission::create_validated_submission;
    use crate::state::JobCacheGcHandle;
    use crate::state::StorageServerError;
    use crate::state::test_utils::MockDbConnector;
    use crate::state::test_utils::MockInboundQueueSender;
    use crate::state::test_utils::MockTaskInstancePoolConnector;
    use crate::state::test_utils::create_test_jcb;
    use crate::state::test_utils::create_test_task_graph;
    use crate::state::test_utils::create_test_task_graph_input;
    use crate::state::test_utils::create_test_task_graph_with_termination_tasks;

    type TestServiceState =
        ServiceState<MockInboundQueueSender, MockDbConnector, MockTaskInstancePoolConnector>;

    type TestServiceStateWithInboundQueue =
        ServiceState<InboundQueueSenderHandle, MockDbConnector, MockTaskInstancePoolConnector>;

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
        TestServiceState::new(ServiceStateParams {
            db,
            session_id,
            job_cache: JobCache::new(),
            inbound_queue_sender: MockInboundQueueSender,
            inbound_queue_receiver: create_inbound_queue_receiver(),
            task_instance_pool_connector: MockTaskInstancePoolConnector,
            job_cache_gc_handle: JobCacheGcHandle::new(tokio::sync::mpsc::unbounded_channel().0),
            shutdown_handle: ShutdownHandle::new(),
            hooks: TestHooks::default(),
        })
    }

    fn create_inbound_queue_receiver() -> InboundQueueReceiverHandle {
        use crate::inbound_queue::create_inbound_queue;
        let (_sender, receiver) = create_inbound_queue();
        receiver
    }

    /// Creates a [`ServiceState`] backed by [`InboundQueueReceiverHandle`].
    ///
    /// # Returns
    ///
    /// A tuple of the service state and the inbound queue sender handle on success.
    fn create_test_service_with_inbound_queue(
        db: MockDbConnector,
    ) -> (TestServiceStateWithInboundQueue, InboundQueueSenderHandle) {
        use crate::inbound_queue::create_inbound_queue;
        let (sender, receiver) = create_inbound_queue();
        let service = TestServiceStateWithInboundQueue::new(ServiceStateParams {
            db,
            session_id: 0,
            job_cache: JobCache::new(),
            inbound_queue_sender: sender.clone(),
            inbound_queue_receiver: receiver,
            task_instance_pool_connector: MockTaskInstancePoolConnector,
            job_cache_gc_handle: JobCacheGcHandle::new(tokio::sync::mpsc::unbounded_channel().0),
            shutdown_handle: ShutdownHandle::new(),
            hooks: TestHooks::default(),
        });
        (service, sender)
    }

    fn create_test_job_submission() -> (Vec<u8>, Vec<u8>) {
        let task_graph = create_test_task_graph();
        (
            compress_task_graph(&task_graph),
            compress_job_inputs(&create_test_task_graph_input()),
        )
    }

    fn create_test_serialized_outputs() -> Vec<u8> {
        let output_tuple = (1,);
        TaskOutputsSerializer::from_tuple(&output_tuple)
            .expect("output serialization should succeed")
    }

    fn create_empty_compressed_serialized_task_graph_input() -> Vec<u8> {
        compress_job_inputs(&TaskGraphInputBuilder::new().build())
    }

    #[tokio::test]
    async fn register_job_returns_job_id_and_inserts_into_cache() -> anyhow::Result<()> {
        let service = create_test_service();
        let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
            create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                compressed_serialized_task_graph,
                compressed_serialized_task_graph_input,
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
                encode_zstd_bytes(b"invalid json")
                    .expect("invalid task graph compression should succeed"),
                create_empty_compressed_serialized_task_graph_input(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageServerError::BadRequest(_))),
            "register_job should return a bad-request error on invalid task graph JSON"
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
                encode_zstd_bytes(task_graph.as_bytes())
                    .expect("task graph compression should succeed"),
                create_empty_compressed_serialized_task_graph_input(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageServerError::BadRequest(_))),
            "register_job should return a bad-request error on input size mismatch"
        );
        Ok(())
    }

    #[tokio::test]
    async fn register_job_returns_error_on_empty_task_graph() -> anyhow::Result<()> {
        let service = create_test_service();
        let task_graph = SubmittedTaskGraph::new(None, None)
            .expect("empty task graph creation should succeed")
            .to_json()
            .expect("task graph serialization should succeed");
        let result = service
            .register_job(
                ResourceGroupId::random(),
                encode_zstd_bytes(task_graph.as_bytes())
                    .expect("task graph compression should succeed"),
                create_empty_compressed_serialized_task_graph_input(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageServerError::BadRequest(_))),
            "register_job should return a bad-request error on empty task graph"
        );
        Ok(())
    }

    #[tokio::test]
    async fn start_job_starts_cached_job() -> anyhow::Result<()> {
        let service = create_test_service();
        let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
            create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                compressed_serialized_task_graph,
                compressed_serialized_task_graph_input,
            )
            .await?;

        service.start_job(job_id).await?;

        let state = service.get_job_state(job_id).await?;
        assert_eq!(state, JobState::Running);
        Ok(())
    }

    #[tokio::test]
    async fn start_job_returns_job_not_found_if_not_exist() -> anyhow::Result<()> {
        let service = create_test_service();
        let job_id = JobId::random();
        let result = service.start_job(job_id).await;
        assert!(
            matches!(result, Err(StorageServerError::Db(DbError::JobNotFound(id))) if id == job_id),
            "unexpected result: {result:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn start_job_rejects_terminal_jobs_not_in_cache() -> anyhow::Result<()> {
        for state in [JobState::Succeeded, JobState::Failed, JobState::Cancelled] {
            let db = MockDbConnector::default();
            let job_id = JobId::random();
            db.states.insert(job_id, state);
            let service = create_test_service_with_db(db);

            let result = service.start_job(job_id).await;
            assert!(
                matches!(
                    result,
                    Err(StorageServerError::Cache(CacheError::StaleState(
                        StaleStateError::JobAlreadyTerminated(actual)
                    ))) if actual == state
                ),
                "unexpected result for {state:?}: {result:?}"
            );
        }
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
    async fn cancel_job_enqueues_cleanup_task_of_recovered_commit_ready_job() -> anyhow::Result<()>
    {
        let db = MockDbConnector::default();
        let (service, sender) = create_test_service_with_inbound_queue(db.clone());

        let create_termination_task_descriptor = |task_func: &str| TerminationTaskDescriptor {
            tdl_context: TdlContext {
                package: "test_pkg".to_owned(),
                task_func: task_func.to_owned(),
            },
            execution_policy: Some(ExecutionPolicy::default()),
        };
        let task_graph = create_test_task_graph_with_termination_tasks(
            Some(create_termination_task_descriptor("commit_fn")),
            Some(create_termination_task_descriptor("cleanup_fn")),
        );

        let job_id = JobId::random();
        let jcb = SharedJobControlBlock::recover(
            RecoverableJobContext {
                id: job_id,
                resource_group_id: ResourceGroupId::random(),
                state: JobState::CommitReady,
                submission: create_validated_submission(task_graph, create_test_task_graph_input()),
                outputs: Some(TaskOutputsSerializer::deserialize(
                    &create_test_serialized_outputs(),
                )?),
            },
            sender,
            db,
            MockTaskInstancePoolConnector,
            TestHooks::default(),
        )
        .await?;
        service.inner.job_cache.insert(jcb).await?;

        let state = service.cancel_job(job_id).await?;
        assert_eq!(state, JobState::CleanupReady);

        let entries = service
            .poll_cleanup_ready_tasks(10, Duration::from_millis(100))
            .await?;
        assert_eq!(entries.len(), 1, "should receive one cleanup-ready entry");
        assert_eq!(entries[0].job_id, job_id);
        assert_eq!(entries[0].task_kind, CleanupTaskMarker);

        let context = service
            .create_task_instance(
                TEST_SESSION_ID,
                job_id,
                TaskId::Cleanup,
                ExecutionManagerId::random(),
            )
            .await?;
        let state = service
            .succeed_cleanup_task_instance(TEST_SESSION_ID, job_id, context.task_instance_id)
            .await?;
        assert_eq!(state, JobState::Cancelled);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_state_serves_from_cache_when_jcb_present() -> anyhow::Result<()> {
        let service = create_test_service();
        let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
            create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                compressed_serialized_task_graph,
                compressed_serialized_task_graph_input,
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
        let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
            create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                compressed_serialized_task_graph,
                compressed_serialized_task_graph_input,
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
        let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
            create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                compressed_serialized_task_graph,
                compressed_serialized_task_graph_input,
            )
            .await?;
        // JCB is in cache but job is still Ready (not Succeeded).
        let result = service.get_job_outputs(job_id).await;
        assert!(
            matches!(
                result,
                Err(StorageServerError::Cache(CacheError::StaleState(
                    StaleStateError::JobNotSucceeded(JobState::Ready)
                )))
            ),
            "get_job_outputs should fail with a stale-state error when job is not Succeeded"
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
        let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
            create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                compressed_serialized_task_graph,
                compressed_serialized_task_graph_input,
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
        let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
            create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                compressed_serialized_task_graph,
                compressed_serialized_task_graph_input,
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
        let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
            create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                compressed_serialized_task_graph,
                compressed_serialized_task_graph_input,
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
    async fn cancel_job_enqueues_terminal_job_for_cache_gc() -> anyhow::Result<()> {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let service = TestServiceState::new(ServiceStateParams {
            db: MockDbConnector::default(),
            session_id: TEST_SESSION_ID,
            job_cache: JobCache::new(),
            inbound_queue_sender: MockInboundQueueSender,
            inbound_queue_receiver: create_inbound_queue_receiver(),
            task_instance_pool_connector: MockTaskInstancePoolConnector,
            job_cache_gc_handle: JobCacheGcHandle::new(sender),
            shutdown_handle: ShutdownHandle::new(),
            hooks: TestHooks::default(),
        });
        let job_id = JobId::random();
        let jcb = create_test_jcb(job_id).await;
        service.inner.job_cache.insert(jcb).await?;

        service.cancel_job(job_id).await?;

        assert_eq!(
            receiver.try_recv(),
            Ok(job_id),
            "cancelled job should be enqueued for cache GC"
        );
        Ok(())
    }

    #[tokio::test]
    async fn succeed_task_instance_enqueues_terminal_job_for_cache_gc() -> anyhow::Result<()> {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let service = TestServiceState::new(ServiceStateParams {
            db: MockDbConnector::default(),
            session_id: TEST_SESSION_ID,
            job_cache: JobCache::new(),
            inbound_queue_sender: MockInboundQueueSender,
            inbound_queue_receiver: create_inbound_queue_receiver(),
            task_instance_pool_connector: MockTaskInstancePoolConnector,
            job_cache_gc_handle: JobCacheGcHandle::new(sender),
            shutdown_handle: ShutdownHandle::new(),
            hooks: TestHooks::default(),
        });
        let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
            create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                compressed_serialized_task_graph,
                compressed_serialized_task_graph_input,
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

        service
            .succeed_task_instance(
                TEST_SESSION_ID,
                job_id,
                context.task_instance_id,
                0,
                create_test_serialized_outputs(),
            )
            .await?;

        assert_eq!(
            receiver.try_recv(),
            Ok(job_id),
            "succeeded job should be enqueued for cache GC"
        );
        Ok(())
    }

    #[tokio::test]
    async fn fail_task_instance_enqueues_terminal_job_for_cache_gc() -> anyhow::Result<()> {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let service = TestServiceState::new(ServiceStateParams {
            db: MockDbConnector::default(),
            session_id: TEST_SESSION_ID,
            job_cache: JobCache::new(),
            inbound_queue_sender: MockInboundQueueSender,
            inbound_queue_receiver: create_inbound_queue_receiver(),
            task_instance_pool_connector: MockTaskInstancePoolConnector,
            job_cache_gc_handle: JobCacheGcHandle::new(sender),
            shutdown_handle: ShutdownHandle::new(),
            hooks: TestHooks::default(),
        });
        let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
            create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                compressed_serialized_task_graph,
                compressed_serialized_task_graph_input,
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

        service
            .fail_task_instance(
                TEST_SESSION_ID,
                job_id,
                context.task_instance_id,
                TaskId::Index(0),
                "test failure".to_owned(),
            )
            .await?;

        assert_eq!(
            receiver.recv().await,
            Some(job_id),
            "failed job should be enqueued for cache GC"
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
        let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
            create_test_job_submission();
        let job_id = service
            .register_job(
                ResourceGroupId::random(),
                compressed_serialized_task_graph,
                compressed_serialized_task_graph_input,
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
                matches!(result, Err(StorageServerError::StaleSession(_))),
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
                matches!(result, Err(StorageServerError::StaleSession(_))),
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
                matches!(result, Err(StorageServerError::StaleSession(_))),
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
                .add_resource_group(ExternalResourceGroupCredentials::new(
                    "external_123".to_owned(),
                    vec![1, 2, 3],
                ))
                .await
                .is_ok()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_add_or_verify_resource_group_creates_and_reuses_id() -> anyhow::Result<()> {
        let db = MockDbConnector::default();
        let service = create_test_service_with_db(db.clone());
        let credentials =
            ExternalResourceGroupCredentials::new("external_123".to_owned(), vec![1, 2, 3]);
        let rg_id = service
            .add_or_verify_resource_group(credentials.clone())
            .await?;
        service
            .verify_resource_group(rg_id, credentials.get_password())
            .await?;
        assert_eq!(
            service.add_or_verify_resource_group(credentials).await?,
            rg_id
        );
        assert_eq!(db.resource_groups.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_add_or_verify_resource_group_rejects_wrong_password() -> anyhow::Result<()> {
        let db = MockDbConnector::default();
        let service = create_test_service_with_db(db.clone());
        let credentials =
            ExternalResourceGroupCredentials::new("external_123".to_owned(), vec![1, 2, 3]);
        let rg_id = service.add_resource_group(credentials.clone()).await?;
        let result = service
            .add_or_verify_resource_group(ExternalResourceGroupCredentials::new(
                credentials.get_external_resource_group_id().to_owned(),
                vec![4, 5, 6],
            ))
            .await;
        assert!(matches!(
            result,
            Err(StorageServerError::Db(DbError::InvalidPassword(id))) if id == rg_id
        ));
        assert_eq!(
            service.add_or_verify_resource_group(credentials).await?,
            rg_id
        );
        assert_eq!(db.resource_groups.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn verify_resource_group_succeeds_for_correct_password() -> anyhow::Result<()> {
        let service = create_test_service();
        let password = vec![1, 2, 3];
        let rg_id = service
            .add_resource_group(ExternalResourceGroupCredentials::new(
                "external_123".to_owned(),
                password.clone(),
            ))
            .await?;
        service.verify_resource_group(rg_id, &password).await?;
        Ok(())
    }

    #[tokio::test]
    async fn verify_resource_group_fails_for_wrong_password() -> anyhow::Result<()> {
        let service = create_test_service();
        let rg_id = service
            .add_resource_group(ExternalResourceGroupCredentials::new(
                "external_123".to_owned(),
                vec![1, 2, 3],
            ))
            .await?;
        let result = service.verify_resource_group(rg_id, &[4, 5, 6]).await;
        assert!(result.is_err(), "verify should fail for wrong password");
        Ok(())
    }

    #[tokio::test]
    async fn poll_ready_tasks_returns_entries_from_inbound_queue() -> anyhow::Result<()> {
        const TASK_INDEX: TaskIndex = 0;
        let (service, sender) = create_test_service_with_inbound_queue(MockDbConnector::default());
        let rg_id = ResourceGroupId::random();
        let job_id = JobId::random();
        sender
            .send_task_ready(rg_id, job_id, vec![TASK_INDEX])
            .await
            .expect("send_task_ready should succeed");

        let entries = service
            .poll_ready_tasks(10, Duration::from_millis(100))
            .await?;
        assert_eq!(entries.len(), 1, "should receive one inbound queue entry");
        assert_eq!(entries[0].job_id, job_id);
        assert_eq!(entries[0].task_kind, TASK_INDEX);
        assert_eq!(entries[0].resource_group_id, rg_id);
        Ok(())
    }

    #[tokio::test]
    async fn poll_ready_tasks_returns_empty_when_no_tasks() -> anyhow::Result<()> {
        let (service, _sender) = create_test_service_with_inbound_queue(MockDbConnector::default());
        let entries = service
            .poll_ready_tasks(10, Duration::from_millis(10))
            .await?;
        assert!(
            entries.is_empty(),
            "should receive no entries from empty inbound queue"
        );
        Ok(())
    }

    #[tokio::test]
    async fn poll_commit_ready_tasks_returns_entries_from_inbound_queue() -> anyhow::Result<()> {
        let (service, sender) = create_test_service_with_inbound_queue(MockDbConnector::default());
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
    async fn poll_cleanup_ready_tasks_returns_entries_from_inbound_queue() -> anyhow::Result<()> {
        let (service, sender) = create_test_service_with_inbound_queue(MockDbConnector::default());
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
        let (_, resource_group_id) = service
            .register_execution_manager("127.0.0.1".parse()?, None)
            .await?;
        assert_eq!(resource_group_id, None);
        Ok(())
    }

    #[tokio::test]
    async fn register_execution_manager_resolves_resource_group() -> anyhow::Result<()> {
        let service = create_test_service();
        let password = b"password";
        let resource_group_id = service
            .add_resource_group(ExternalResourceGroupCredentials::new(
                "external_123".to_owned(),
                password.to_vec(),
            ))
            .await?;

        let (_, registered_resource_group_id) = service
            .register_execution_manager(
                "127.0.0.1".parse()?,
                Some(ExternalResourceGroupCredentials::new(
                    "external_123".to_owned(),
                    password.to_vec(),
                )),
            )
            .await?;

        assert_eq!(registered_resource_group_id, Some(resource_group_id));
        Ok(())
    }

    #[tokio::test]
    async fn register_execution_manager_rejects_unknown_resource_group() -> anyhow::Result<()> {
        let service = create_test_service();

        let result = service
            .register_execution_manager(
                "127.0.0.1".parse()?,
                Some(ExternalResourceGroupCredentials::new(
                    "unknown".to_owned(),
                    b"password".to_vec(),
                )),
            )
            .await;

        assert!(matches!(
            result,
            Err(StorageServerError::Db(
                DbError::ExternalResourceGroupNotFound(_)
            ))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn register_execution_manager_rejects_invalid_password() -> anyhow::Result<()> {
        let service = create_test_service();
        service
            .add_resource_group(ExternalResourceGroupCredentials::new(
                "external_123".to_owned(),
                b"password".to_vec(),
            ))
            .await?;

        let result = service
            .register_execution_manager(
                "127.0.0.1".parse()?,
                Some(ExternalResourceGroupCredentials::new(
                    "external_123".to_owned(),
                    b"wrong-password".to_vec(),
                )),
            )
            .await;

        assert!(matches!(
            result,
            Err(StorageServerError::Db(DbError::InvalidPassword(_)))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn register_scheduler_resends_ready_tasks_only_when_replacing_previous_scheduler()
    -> anyhow::Result<()> {
        let (service, _sender) = create_test_service_with_inbound_queue(MockDbConnector::default());

        let (task_graph, inputs) = create_test_job_submission();
        let job_id = service
            .register_job(ResourceGroupId::random(), task_graph, inputs)
            .await?;
        service.start_job(job_id).await?;

        // Starting the job enqueues its initial ready task; drain it so the queue is empty before
        // probing whether a registration triggers a resend.
        let initial = service
            .poll_ready_tasks(10, Duration::from_millis(100))
            .await?;
        assert_eq!(
            initial.len(),
            1,
            "starting the job should enqueue its initial ready task"
        );

        // The first registration has no previous scheduler to replace, so it must not resend.
        service
            .register_scheduler(
                Host::new("scheduler-a.example.com".to_owned())
                    .expect("scheduler host should not be empty"),
                8080,
            )
            .await?;
        tokio::task::yield_now().await;
        let after_first = service
            .poll_ready_tasks(10, Duration::from_millis(100))
            .await?;
        assert!(
            after_first.is_empty(),
            "the first scheduler registration must not resend ready tasks"
        );

        // The second registration replaces the first scheduler, so it must resend ready tasks. The
        // resend runs in a spawned background task, so yield to let it run before polling.
        service
            .register_scheduler(
                Host::new("scheduler-b.example.com".to_owned())
                    .expect("scheduler host should not be empty"),
                8081,
            )
            .await?;
        tokio::task::yield_now().await;
        let after_second = service
            .poll_ready_tasks(10, Duration::from_millis(100))
            .await?;
        assert_eq!(
            after_second.len(),
            1,
            "the second scheduler registration must resend the job's ready tasks"
        );
        assert_eq!(after_second[0].job_id, job_id);
        Ok(())
    }

    #[tokio::test]
    async fn update_execution_manager_heartbeat_succeeds_for_registered_em() -> anyhow::Result<()> {
        let service = create_test_service();
        let em_id = service
            .register_execution_manager("127.0.0.1".parse()?, None)
            .await?
            .0;
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

    #[tokio::test]
    async fn a_body_failing_during_a_graceful_shutdown_is_stopped() {
        let service = create_test_service();
        let shutdown_handle = service.inner.shutdown_handle.clone();

        let result = service
            .spawn_body(async move {
                shutdown_handle.shut_down_gracefully();
                Err::<(), _>(StorageServerError::from(CacheError::Internal(
                    InternalError::InboundQueueChannelClosed,
                )))
            })
            .await;

        assert!(
            matches!(result, Err(StorageServerError::Stopping(_))),
            "a body failing during a graceful shutdown should be stopped, got {result:?}"
        );
        assert!(!service.inner.shutdown_handle.is_fatal());
    }

    #[tokio::test]
    async fn a_body_failing_with_a_fatal_shutdown_keeps_its_error() {
        let service = create_test_service();
        let shutdown_handle = service.inner.shutdown_handle.clone();

        let result = service
            .spawn_body(async move {
                shutdown_handle.shut_down_fatally();
                Err::<(), _>(StorageServerError::from(CacheError::Internal(
                    InternalError::InboundQueueChannelClosed,
                )))
            })
            .await;

        assert!(
            matches!(
                result,
                Err(StorageServerError::Cache(CacheError::Internal(
                    InternalError::InboundQueueChannelClosed
                )))
            ),
            "a body failing with a fatal shutdown should keep its error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn a_body_failing_fatally_after_its_caller_is_dropped_shuts_down_fatally() {
        let service = create_test_service();
        let (release_sender, release_receiver) = tokio::sync::oneshot::channel();

        let mut caller = Box::pin(service.spawn_body(async move {
            release_receiver
                .await
                .expect("the release sender should not be dropped");
            Err::<(), _>(StorageServerError::from(CacheError::Internal(
                InternalError::InboundQueueChannelClosed,
            )))
        }));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(caller.as_mut().poll(cx).is_pending())).await
        );
        drop(caller);
        release_sender
            .send(())
            .expect("the body should still wait for the release");
        service.inner.task_tracker.close();
        service.inner.task_tracker.wait().await;

        assert!(service.inner.shutdown_handle.is_fatal());
    }

    #[tokio::test]
    async fn a_body_panicking_after_its_caller_is_dropped_shuts_down_fatally() {
        let service = create_test_service();
        let (release_sender, release_receiver) = tokio::sync::oneshot::channel();

        let mut caller = Box::pin(service.spawn_body::<()>(async move {
            release_receiver
                .await
                .expect("the release sender should not be dropped");
            panic!("injected handler body panic")
        }));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(caller.as_mut().poll(cx).is_pending())).await
        );
        drop(caller);
        release_sender
            .send(())
            .expect("the body should still wait for the release");
        service.inner.task_tracker.close();
        service.inner.task_tracker.wait().await;

        assert!(service.inner.shutdown_handle.is_fatal());
    }

    #[tokio::test]
    async fn a_body_failing_fatally_without_a_caller_during_a_graceful_shutdown_is_not_fatal() {
        let service = create_test_service();
        let shutdown_handle = service.inner.shutdown_handle.clone();
        let (release_sender, release_receiver) = tokio::sync::oneshot::channel();

        let mut caller = Box::pin(service.spawn_body(async move {
            release_receiver
                .await
                .expect("the release sender should not be dropped");
            shutdown_handle.shut_down_gracefully();
            Err::<(), _>(StorageServerError::from(CacheError::Internal(
                InternalError::InboundQueueChannelClosed,
            )))
        }));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(caller.as_mut().poll(cx).is_pending())).await
        );
        drop(caller);
        release_sender
            .send(())
            .expect("the body should still wait for the release");
        service.inner.task_tracker.close();
        service.inner.task_tracker.wait().await;

        assert!(service.inner.shutdown_handle.is_cancelled());
        assert!(!service.inner.shutdown_handle.is_fatal());
    }

    #[tokio::test]
    async fn a_body_requested_after_shutdown_starts_is_not_run() {
        let service = create_test_service();
        service.inner.shutdown_handle.shut_down_gracefully();
        let body_ran = Arc::new(AtomicBool::new(false));
        let body_ran_in_body = Arc::clone(&body_ran);

        let result = service
            .spawn_body(async move {
                body_ran_in_body.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await;

        assert!(
            matches!(result, Err(StorageServerError::Stopping(_))),
            "a body requested after shutdown starts should be stopped, got {result:?}"
        );
        assert_eq!(service.inner.task_tracker.len(), 0);
        service.inner.task_tracker.close();
        service.inner.task_tracker.wait().await;
        assert!(!body_ran.load(Ordering::SeqCst));
    }
}
