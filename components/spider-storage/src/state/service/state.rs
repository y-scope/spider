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
use crate::cache::tree::EventReceivers;
use crate::cache::tree::TreeControlBlock;
use crate::db::DbStorage;
use crate::inbound_queue::CleanupTaskMarker;
use crate::inbound_queue::CommitTaskMarker;
use crate::inbound_queue::InboundQueueEntry;
use crate::inbound_queue::InboundQueueReceiverHandle;
use crate::inbound_queue::InboundQueueSender;
use crate::job_submission::ValidatedJobSubmission;
use crate::partial_mutation::PartialMutationGuard;
use crate::state::CommitterEnv;
use crate::state::JobCache;
use crate::state::JobCacheGcHandle;
use crate::state::StorageServerError;
use crate::state::spawn_committer;
use crate::task_instance_pool::TaskInstancePoolConnector;
use crate::test_hooks::HookContext;
use crate::test_hooks::HookPoint;
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
    pub job_cache: JobCache<InboundQueueSenderType, TaskInstancePoolConnectorType>,
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
    pub(super) inner: Arc<
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
    /// A newly created [`ServiceState`] whose tree committers notify the GC actor when cached jobs
    /// terminate.
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
        let committer_env = Arc::new(CommitterEnv {
            db: db.clone(),
            job_cache: job_cache.clone(),
            job_cache_gc_handle,
            inbound_queue_sender: inbound_queue_sender.clone(),
            shutdown_handle: shutdown_handle.clone(),
            hooks: hooks.clone(),
        });
        Self {
            inner: Arc::new(ServiceStateInner {
                db,
                session_id,
                job_cache,
                inbound_queue_sender,
                inbound_queue_receiver,
                task_instance_pool_connector,
                committer_env,
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

    /// Inserts `root`, the root job of a new one-job tree, into the cache, and spawns into the
    /// service's task tracker the tree's committer, which receives the tree's events from
    /// `receivers`.
    ///
    /// The caller must run in a task of the service's task tracker, or before the service serves
    /// requests, so that [`crate::state::Runtime::stop`] cannot miss the committer.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobCache::insert`]'s return values on failure.
    pub(crate) async fn insert_tree(
        &self,
        root: SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
        receivers: EventReceivers,
    ) -> Result<(), StorageServerError> {
        self.inner.job_cache.insert(root.clone()).await?;
        spawn_committer(
            &self.inner.task_tracker,
            Arc::clone(&self.inner.committer_env),
            root,
            receivers,
        );
        Ok(())
    }

    /// Registers a job in the database, inserts its control block into the cache and spawns its
    /// tree's committer.
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
    /// * Forwards [`TestHooks::hit`]'s return values on failure.
    /// * Forwards [`SharedJobControlBlock::create`]'s return values on failure.
    /// * Forwards [`Self::insert_tree`]'s return values on failure.
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

            let partial_mutation_guard =
                PartialMutationGuard::new(&this.inner.shutdown_handle, job_id);
            tracing::info!(
                job_id = ? job_id,
                rg_id = ? resource_group_id,
                "Job registered in DB."
            );

            this.inner
                .hooks
                .hit(HookPoint::RegisterJobAfterInsert, || {
                    HookContext::job(job_id)
                })
                .await
                .map_err(CacheError::from)?;

            let (tree, receivers) = TreeControlBlock::new(this.inner.shutdown_handle.clone());
            let jcb = SharedJobControlBlock::create(
                job_id,
                resource_group_id,
                job_submission,
                this.inner.inbound_queue_sender.clone(),
                this.inner.task_instance_pool_connector.clone(),
                this.inner.hooks.clone(),
                tree,
            )
            .await?;

            this.insert_tree(jcb, receivers).await?;
            partial_mutation_guard.disarm();
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
    /// * Forwards [`SharedJobControlBlock::fail_task_instance`]'s return values on failure.
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
            let state = match jcb
                .succeed_task_instance(task_instance_id, task_index, task_outputs)
                .await
            {
                Err(rejection @ CacheError::TaskOutputsLengthMismatch { .. }) => {
                    tracing::warn!(
                        job_id = ? job_id,
                        task_id = ? TaskId::Index(task_index),
                        task_instance_id = ? task_instance_id,
                        error = % rejection,
                        "Task success report rejected. Failing the task instance."
                    );
                    let state = jcb
                        .fail_task_instance(
                            task_instance_id,
                            TaskId::Index(task_index),
                            rejection.to_string(),
                        )
                        .await?;
                    tracing::info!(
                        job_id = ? job_id,
                        task_id = ? TaskId::Index(task_index),
                        task_instance_id = ? task_instance_id,
                        job_state = ? state,
                        "Task instance failed.",
                    );
                    return Ok(state);
                }
                result => result?,
            };
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
    pub(super) async fn spawn_body<ReturnType: Send + 'static>(
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
pub(super) struct ServiceStateInner<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> {
    db: DbConnectorType,
    session_id: SessionId,
    pub(super) job_cache: JobCache<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    inbound_queue_sender: InboundQueueSenderType,
    inbound_queue_receiver: InboundQueueReceiverHandle,
    task_instance_pool_connector: TaskInstancePoolConnectorType,
    committer_env:
        Arc<CommitterEnv<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>>,
    has_previous_scheduler_connection: tokio::sync::Mutex<bool>,
    pub(super) shutdown_handle: ShutdownHandle,
    pub(super) task_tracker: TaskTracker,
    pub(super) hooks: TestHooks,
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
