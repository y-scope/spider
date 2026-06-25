//! gRPC service adapters for the storage runtime.

use async_trait::async_trait;
use spider_core::types::id::{SessionId, TaskId};
use spider_proto_rust::{
    storage::{
        self,
        execution_manager_liveness_service_server::ExecutionManagerLivenessService,
        inbound_queue_service_server::InboundQueueService,
        job_orchestration_service_server::JobOrchestrationService,
        resource_group_management_service_server::ResourceGroupManagementService,
        scheduler_registration_service_server::SchedulerRegistrationService,
        session_management_service_server::SessionManagementService,
        task_instance_management_service_server::TaskInstanceManagementService,
    },
    unpack::RequestUnpack,
};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::{
    cache::error::{CacheError, InternalError},
    db::{DbError, DbStorage},
    ready_queue::{CleanupTaskMarker, CommitTaskMarker, ReadyQueueEntry, ReadyQueueSender},
    state::{ServiceState, StorageServerError},
    task_instance_pool::TaskInstancePoolConnector,
};

/// gRPC adapter over a storage [`ServiceState`].
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready queue sender type.
/// * `DbConnectorType` - The database connector type.
/// * `TaskInstancePoolConnectorType` - The task instance pool connector type.
#[derive(Clone)]
pub struct GrpcServiceState<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    inner: ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    cancellation_token: CancellationToken,
}

impl<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> GrpcServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A new [`GrpcServiceState`] wrapping [`ServiceState`].
    #[must_use]
    pub const fn new(
        inner: ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            inner,
            cancellation_token,
        }
    }

    /// Logs a fatal cache-internal error, cancels the service, and returns an `INTERNAL` status.
    ///
    /// Shared by every service error handler for the `Cache(CacheError::Internal)` arm. The error
    /// is unrecoverable, so the whole storage service is cancelled to avoid cache corruption.
    ///
    /// # Returns
    ///
    /// `Status::internal("storage service unavailable")`.
    fn fatal_internal_status(
        &self,
        service_name: &'static str,
        tag: &'static str,
        error: &InternalError,
    ) -> Status {
        tracing::error!(
            error = % error,
            service = service_name,
            tag,
            "Internal error in the cache layer. Cancelling service."
        );
        self.cancellation_token.cancel();
        Status::internal("storage service unavailable")
    }

    /// Logs an unexpected error, cancels the service, and returns an `INTERNAL` status.
    ///
    /// Shared by every service error handler for the catch-all fallback arm. An unmapped error is
    /// treated as unrecoverable, so the whole storage service is cancelled to avoid cache
    /// corruption.
    ///
    /// # Returns
    ///
    /// `Status::internal("internal error")`.
    fn unexpected_internal_status(
        &self,
        service_name: &'static str,
        tag: &'static str,
        error: &StorageServerError,
    ) -> Status {
        tracing::error!(
            error = % error,
            service = service_name,
            tag,
            "Unexpected internal error. Cancelling service to avoid cache corruption."
        );
        self.cancellation_token.cancel();
        Status::internal("internal error")
    }

    /// Error handler for job orchestration service errors.
    ///
    /// This function maps the given [`StorageServerError`] to a [`Status`] that can be sent to the
    /// client. The errors are logged for observability.
    ///
    /// # Returns
    ///
    /// The [`Status`] to send to the client:
    ///
    /// * `INTERNAL` for a fatal cache-internal error (the service will restart).
    /// * `UNAUTHENTICATED` for a wrong resource-group password.
    /// * `NOT_FOUND` for an unknown resource group or a missing job.
    /// * `FAILED_PRECONDITION` for operations on an invalid job state.
    /// * `INVALID_ARGUMENT` for a malformed task graph, inputs, or request.
    /// * `INTERNAL` for any other (database or otherwise unexpected) error.
    pub fn job_orchestration_service_error_handler(
        &self,
        error: StorageServerError,
        tag: &'static str,
    ) -> Status {
        const SERVICE_NAME: &str = "JobOrchestration";
        match error {
            StorageServerError::Cache(CacheError::Internal(e)) => {
                self.fatal_internal_status(SERVICE_NAME, tag, &e)
            }

            StorageServerError::Db(db_error) => match &db_error {
                DbError::ResourceGroupNotFound(_) => {
                    tracing::warn!(
                        error = % db_error,
                        service = SERVICE_NAME,
                        tag,
                        "Resource group not found."
                    );
                    Status::not_found(db_error.to_string())
                }
                DbError::InvalidPassword(_) => {
                    tracing::warn!(
                        error = % db_error,
                        service = SERVICE_NAME,
                        tag,
                        "Invalid resource group password."
                    );
                    Status::unauthenticated(db_error.to_string())
                }
                DbError::JobNotFound(_) => {
                    tracing::warn!(
                        error = % db_error,
                        service = SERVICE_NAME,
                        tag,
                        "Job not found."
                    );
                    Status::not_found("job not found")
                }
                DbError::InvalidJobStateTransition { .. } | DbError::UnexpectedJobState { .. } => {
                    tracing::warn!(
                        error = % db_error,
                        service = SERVICE_NAME,
                        tag,
                        "Invalid job state."
                    );
                    Status::failed_precondition(db_error.to_string())
                }
                _ => {
                    tracing::error!(
                        error = % db_error,
                        service = SERVICE_NAME,
                        tag,
                        "DB operation failed."
                    );
                    Status::internal("internal error")
                }
            },

            StorageServerError::JobNotFound(_) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Job not found."
                );
                Status::not_found("job not found")
            }

            error @ (StorageServerError::Task(_)
            | StorageServerError::Tdl(_)
            | StorageServerError::BadRequest(_)) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Invalid argument."
                );
                Status::invalid_argument(error.to_string())
            }

            _ => self.unexpected_internal_status(SERVICE_NAME, tag, &error),
        }
    }

    /// Error handler for task instance management service errors.
    ///
    /// This function maps the given [`StorageServerError`] to a [`Status`] that can be sent to the
    /// client. The errors are logged for observability.
    ///
    /// # Returns
    ///
    /// The [`Status`] to send to the client:
    ///
    /// * `INTERNAL` for:
    ///   * A fatal cache-internal error (the service will restart).
    ///   * Any other unexpected error (the service will restart).
    /// * `UNAVAILABLE` for a request issued from a stale session.
    /// * `NOT_FOUND` for a request targeting a job that no longer exists in the cache (e.g. its
    ///   resource group was deleted). Clients treat this as a benign no-op and drop the request.
    /// * `FAILED_PRECONDITION` for a request issued against a stale cache state.
    /// * `INVALID_ARGUMENT` for malformed inputs or a malformed request.
    pub fn task_instance_management_service_error_handler(
        &self,
        error: StorageServerError,
        tag: &'static str,
    ) -> Status {
        const SERVICE_NAME: &str = "TaskInstanceManagement";
        match error {
            StorageServerError::Cache(CacheError::Internal(e)) => {
                self.fatal_internal_status(SERVICE_NAME, tag, &e)
            }

            StorageServerError::StaleSession(storage_session) => {
                tracing::warn!(
                    storage_session,
                    service = SERVICE_NAME,
                    tag,
                    "The request was issued from a stale session."
                );
                Status::unavailable(format!(
                    "stale session; current storage session is {storage_session}"
                ))
            }

            StorageServerError::Cache(CacheError::StaleState(_)) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "The request was issued from a stale cache state."
                );
                Status::failed_precondition(format!("cache stale: {error}"))
            }

            StorageServerError::JobNotFound(job_id) => {
                tracing::warn!(
                    job_id = job_id.get(),
                    service = SERVICE_NAME,
                    tag,
                    "The request targets a job that no longer exists in the cache."
                );
                // The job is gone (deleted or evicted), not transiently stale. Report it as
                // NOT_FOUND so clients can drop the request as a benign no-op instead of retrying
                // against a permanently missing job.
                Status::not_found(error.to_string())
            }

            error @ (StorageServerError::Tdl(_) | StorageServerError::BadRequest(_)) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Invalid argument."
                );
                Status::invalid_argument(error.to_string())
            }

            _ => self.unexpected_internal_status(SERVICE_NAME, tag, &error),
        }
    }

    /// Error handler for inbound queue service errors.
    ///
    /// This function maps the given [`StorageServerError`] to a [`Status`] that can be sent to the
    /// client. The errors are logged for observability.
    ///
    /// # Returns
    ///
    /// The [`Status`] to send to the client:
    ///
    /// * `UNAVAILABLE` when the ready-queue channel is closed (the inbound queue can no longer
    ///   yield entries).
    /// * `INTERNAL` for a fatal cache-internal error (the service will restart) or any other
    ///   unexpected failure.
    pub fn inbound_queue_service_error_handler(
        &self,
        error: StorageServerError,
        tag: &'static str,
    ) -> Status {
        const SERVICE_NAME: &str = "InboundQueue";
        match error {
            StorageServerError::Cache(CacheError::Internal(
                InternalError::ReadyQueueChannelClosed,
            )) => {
                tracing::warn!(
                    service = SERVICE_NAME,
                    tag,
                    "Inbound queue channel is closed."
                );
                Status::unavailable("inbound queue is closed")
            }

            StorageServerError::Cache(CacheError::Internal(e)) => {
                self.fatal_internal_status(SERVICE_NAME, tag, &e)
            }

            error => self.unexpected_internal_status(SERVICE_NAME, tag, &error),
        }
    }

    /// Error handler for resource group management service errors.
    ///
    /// This function maps the given [`StorageServerError`] to a [`Status`] that can be sent to the
    /// client. The errors are logged for observability.
    ///
    /// # Returns
    ///
    /// The [`Status`] to send to the client:
    ///
    /// * `NOT_FOUND` for an unknown resource group.
    /// * `UNAUTHENTICATED` for a wrong password.
    /// * `ALREADY_EXISTS` for a duplicate external resource group ID.
    /// * `INTERNAL` for a fatal cache-internal error (the service will restart) or any other
    ///   unexpected failure.
    pub fn resource_group_management_service_error_handler(
        &self,
        error: StorageServerError,
        tag: &'static str,
    ) -> Status {
        const SERVICE_NAME: &str = "ResourceGroupManagement";
        match error {
            error @ StorageServerError::Db(DbError::ResourceGroupNotFound(_)) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Resource group not found."
                );
                Status::not_found(error.to_string())
            }

            error @ StorageServerError::Db(DbError::InvalidPassword(_)) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Invalid resource group password."
                );
                Status::unauthenticated(error.to_string())
            }

            error @ StorageServerError::Db(DbError::ResourceGroupAlreadyExists(_)) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Resource group already exists."
                );
                Status::already_exists(error.to_string())
            }

            StorageServerError::Cache(CacheError::Internal(e)) => {
                self.fatal_internal_status(SERVICE_NAME, tag, &e)
            }

            error => self.unexpected_internal_status(SERVICE_NAME, tag, &error),
        }
    }

    /// Error handler for execution manager liveness service errors.
    ///
    /// This function maps the given [`StorageServerError`] to a [`Status`] that can be sent to the
    /// client. The errors are logged for observability.
    ///
    /// # Returns
    ///
    /// The [`Status`] to send to the client:
    ///
    /// * `FAILED_PRECONDITION` when the execution manager has already been reaped.
    /// * `INVALID_ARGUMENT` for an illegal execution manager ID.
    /// * `INTERNAL` for a fatal cache-internal error (the service will restart) or any other
    ///   unexpected failure.
    pub fn execution_manager_liveness_service_error_handler(
        &self,
        error: StorageServerError,
        tag: &'static str,
    ) -> Status {
        const SERVICE_NAME: &str = "ExecutionManagerLiveness";
        match error {
            error @ StorageServerError::Db(DbError::ExecutionManagerAlreadyDead(_)) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Execution manager already marked dead."
                );
                Status::failed_precondition(error.to_string())
            }

            error @ StorageServerError::Db(DbError::IllegalExecutionManagerId(_)) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Illegal execution manager id."
                );
                Status::invalid_argument(error.to_string())
            }

            StorageServerError::Cache(CacheError::Internal(e)) => {
                self.fatal_internal_status(SERVICE_NAME, tag, &e)
            }

            error => self.unexpected_internal_status(SERVICE_NAME, tag, &error),
        }
    }

    /// Error handler for scheduler registration service errors.
    ///
    /// This function maps the given [`StorageServerError`] to a [`Status`] that can be sent to the
    /// client. The errors are logged for observability.
    ///
    /// # Returns
    ///
    /// The [`Status`] to send to the client:
    ///
    /// * `INTERNAL` for a fatal cache-internal error (the service will restart) or any other
    ///   failure; scheduler registration currently has no caller-visible error classification
    ///   beyond a generic server error.
    #[must_use]
    pub fn scheduler_registration_service_error_handler(
        &self,
        error: StorageServerError,
        tag: &'static str,
    ) -> Status {
        const SERVICE_NAME: &str = "SchedulerRegistration";
        match error {
            StorageServerError::Cache(CacheError::Internal(e)) => {
                self.fatal_internal_status(SERVICE_NAME, tag, &e)
            }

            error => self.unexpected_internal_status(SERVICE_NAME, tag, &error),
        }
    }
}

/// Implementation of [`JobOrchestrationService`].
///
/// All possible errors that can occur during job orchestration can be found in
/// [`GrpcServiceState::job_orchestration_service_error_handler`].
#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> JobOrchestrationService
    for GrpcServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn register_job(
        &self,
        request: Request<storage::RegisterJobRequest>,
    ) -> Result<Response<storage::RegisterJobResponse>, Status> {
        let (rg_id, serialized_task_graph, serialized_inputs) = request.into_inner().unpack()?;
        tracing::info!(rg_id = rg_id.get(), "Job submission request received.");

        match self
            .inner
            .register_job(rg_id, serialized_task_graph, serialized_inputs)
            .await
        {
            Ok(job_id) => Ok(Response::new(storage::RegisterJobResponse {
                job_id: job_id.get(),
            })),
            Err(error) => Err(self.job_orchestration_service_error_handler(error, "register_job")),
        }
    }

    async fn start_job(
        &self,
        request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobStateResponse>, Status> {
        let job_id = request.into_inner().unpack()?;
        tracing::info!(job_id = job_id.get(), "Job start request received.");

        match self.inner.start_job(job_id).await {
            Ok(()) => Ok(make_job_state_response(spider_core::job::JobState::Running)),
            Err(error) => Err(self.job_orchestration_service_error_handler(error, "start_job")),
        }
    }

    async fn cancel_job(
        &self,
        request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobStateResponse>, Status> {
        let job_id = request.into_inner().unpack()?;
        tracing::info!(job_id = job_id.get(), "Job cancellation request received.");

        match self.inner.cancel_job(job_id).await {
            Ok(state) => Ok(make_job_state_response(state)),
            Err(error) => Err(self.job_orchestration_service_error_handler(error, "cancel_job")),
        }
    }

    async fn get_job_state(
        &self,
        request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobStateResponse>, Status> {
        let job_id = request.into_inner().unpack()?;
        tracing::info!(job_id = job_id.get(), "Job state request received.");

        match self.inner.get_job_state(job_id).await {
            Ok(state) => Ok(make_job_state_response(state)),
            Err(error) => Err(self.job_orchestration_service_error_handler(error, "get_job_state")),
        }
    }

    async fn get_job_outputs(
        &self,
        request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobOutputsResponse>, Status> {
        let job_id = request.into_inner().unpack()?;
        tracing::info!(job_id = job_id.get(), "Job outputs request received.");

        match self.inner.get_job_outputs(job_id).await {
            Ok(outputs) => Ok(Response::new(storage::JobOutputsResponse {
                outputs: Some(storage::JobOutputs { outputs }),
            })),
            Err(error) => {
                Err(self.job_orchestration_service_error_handler(error, "get_job_outputs"))
            }
        }
    }

    async fn get_job_error(
        &self,
        request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobErrorResponse>, Status> {
        let job_id = request.into_inner().unpack()?;
        tracing::info!(job_id = job_id.get(), "Job error request received.");

        match self.inner.get_job_error(job_id).await {
            Ok(error_message) => Ok(Response::new(storage::JobErrorResponse { error_message })),
            Err(error) => Err(self.job_orchestration_service_error_handler(error, "get_job_error")),
        }
    }
}

#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> TaskInstanceManagementService
    for GrpcServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn register_task_instance(
        &self,
        request: Request<storage::RegisterTaskInstanceRequest>,
    ) -> Result<Response<storage::RegisterTaskInstanceResponse>, Status> {
        let (session_id, job_id, task_id, em_id) = request.into_inner().unpack()?;
        tracing::info!(
            session_id = session_id,
            job_id = job_id.get(),
            task_id = % task_id,
            em_id = em_id.get(),
            "Task instance registration request received."
        );

        let execution_context = self
            .inner
            .create_task_instance(session_id, job_id, task_id, em_id)
            .await
            .map_err(|error| {
                self.task_instance_management_service_error_handler(error, "register_task_instance")
            })?;

        Ok(Response::new(storage::RegisterTaskInstanceResponse {
            execution_context: Some(storage::ExecutionContext {
                task_instance_id: execution_context.task_instance_id,
                tdl_context: Some(storage::TdlContext {
                    package: execution_context.tdl_context.package,
                    task_func: execution_context.tdl_context.task_func,
                }),
                timeout_policy: Some(storage::TimeoutPolicy {
                    soft_timeout_ms: execution_context.timeout_policy.soft_timeout_ms,
                    hard_timeout_ms: execution_context.timeout_policy.hard_timeout_ms,
                }),
                serialized_inputs: execution_context.serialized_inputs,
            }),
        }))
    }

    async fn report_task_success(
        &self,
        request: Request<storage::ReportTaskSuccessRequest>,
    ) -> Result<Response<storage::TaskInstanceOperationResponse>, Status> {
        let (session_id, job_id, task_id, task_instance_id, serialized_outputs) =
            request.into_inner().unpack()?;
        tracing::info!(
            session_id = session_id,
            job_id = job_id.get(),
            task_id = % task_id,
            task_instance_id,
            "Task instance completion request (success) received."
        );

        let _job_state = match task_id {
            TaskId::Index(task_index) => {
                self.inner
                    .succeed_task_instance(
                        session_id,
                        job_id,
                        task_instance_id,
                        task_index,
                        serialized_outputs,
                    )
                    .await
            }
            TaskId::Commit => {
                self.inner
                    .succeed_commit_task_instance(session_id, job_id, task_instance_id)
                    .await
            }
            TaskId::Cleanup => {
                self.inner
                    .succeed_cleanup_task_instance(session_id, job_id, task_instance_id)
                    .await
            }
        }
        .map_err(|error| {
            self.task_instance_management_service_error_handler(error, "report_task_success")
        })?;

        Ok(Response::new(storage::TaskInstanceOperationResponse {}))
    }

    async fn report_task_failure(
        &self,
        request: Request<storage::ReportTaskFailureRequest>,
    ) -> Result<Response<storage::TaskInstanceOperationResponse>, Status> {
        let (session_id, job_id, task_id, task_instance_id, error_message) =
            request.into_inner().unpack()?;
        tracing::info!(
            session_id = session_id,
            job_id = job_id.get(),
            task_id = % task_id,
            task_instance_id,
            "Task instance completion request (failure) received."
        );

        let _job_state = self
            .inner
            .fail_task_instance(session_id, job_id, task_instance_id, task_id, error_message)
            .await
            .map_err(|error| {
                self.task_instance_management_service_error_handler(error, "report_task_failure")
            })?;
        Ok(Response::new(storage::TaskInstanceOperationResponse {}))
    }
}

#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> InboundQueueService
    for GrpcServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn poll_ready_tasks(
        &self,
        request: Request<storage::PollReadyTasksRequest>,
    ) -> Result<Response<storage::PollReadyTasksResponse>, Status> {
        let (max_items, wait) = request.into_inner().unpack()?;
        tracing::debug!(max_items, ?wait, "Poll ready tasks request received.");
        let entries = self
            .inner
            .poll_ready_tasks(max_items, wait)
            .await
            .map_err(|error| self.inbound_queue_service_error_handler(error, "poll_ready_tasks"))?;
        Ok(Response::new(storage::PollReadyTasksResponse {
            tasks: Some(ready_tasks(self.inner.session_id(), entries)),
        }))
    }

    async fn poll_ready_commit_tasks(
        &self,
        request: Request<storage::PollReadyTasksRequest>,
    ) -> Result<Response<storage::PollReadyTasksResponse>, Status> {
        let (max_items, wait) = request.into_inner().unpack()?;
        tracing::debug!(
            max_items,
            ?wait,
            "Poll ready commit tasks request received."
        );
        let entries = self
            .inner
            .poll_commit_ready_tasks(max_items, wait)
            .await
            .map_err(|error| {
                self.inbound_queue_service_error_handler(error, "poll_ready_commit_tasks")
            })?;
        Ok(Response::new(storage::PollReadyTasksResponse {
            tasks: Some(ready_tasks(self.inner.session_id(), entries)),
        }))
    }

    async fn poll_ready_cleanup_tasks(
        &self,
        request: Request<storage::PollReadyTasksRequest>,
    ) -> Result<Response<storage::PollReadyTasksResponse>, Status> {
        let (max_items, wait) = request.into_inner().unpack()?;
        tracing::debug!(
            max_items,
            ?wait,
            "Poll ready cleanup tasks request received."
        );
        let entries = self
            .inner
            .poll_cleanup_ready_tasks(max_items, wait)
            .await
            .map_err(|error| {
                self.inbound_queue_service_error_handler(error, "poll_ready_cleanup_tasks")
            })?;
        Ok(Response::new(storage::PollReadyTasksResponse {
            tasks: Some(ready_tasks(self.inner.session_id(), entries)),
        }))
    }

    async fn resend_ready_tasks(
        &self,
        _request: Request<storage::ResendReadyTasksRequest>,
    ) -> Result<Response<storage::ResendReadyTasksResponse>, Status> {
        tracing::info!("Resend ready tasks request received.");
        self.inner.resend_ready_tasks().await.map_err(|error| {
            self.inbound_queue_service_error_handler(error, "resend_ready_tasks")
        })?;
        Ok(Response::new(storage::ResendReadyTasksResponse {}))
    }
}

#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> ResourceGroupManagementService
    for GrpcServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn add_resource_group(
        &self,
        request: Request<storage::AddResourceGroupRequest>,
    ) -> Result<Response<storage::ResourceGroupIdResponse>, Status> {
        let (external_id, password) = request.into_inner().unpack()?;
        tracing::info!(external_id = % external_id, "Add resource group request received.");
        let rg_id = self
            .inner
            .add_resource_group(external_id, password)
            .await
            .map_err(|error| {
                self.resource_group_management_service_error_handler(error, "add_resource_group")
            })?;
        Ok(Response::new(storage::ResourceGroupIdResponse {
            resource_group_id: rg_id.get(),
        }))
    }

    async fn verify_resource_group(
        &self,
        request: Request<storage::VerifyResourceGroupRequest>,
    ) -> Result<Response<storage::ResourceGroupOperationResponse>, Status> {
        let (rg_id, password) = request.into_inner().unpack()?;
        tracing::info!(
            rg_id = rg_id.get(),
            "Verify resource group request received."
        );
        self.inner
            .verify_resource_group(rg_id, &password)
            .await
            .map_err(|error| {
                self.resource_group_management_service_error_handler(error, "verify_resource_group")
            })?;
        Ok(Response::new(storage::ResourceGroupOperationResponse {}))
    }

    async fn delete_resource_group(
        &self,
        request: Request<storage::DeleteResourceGroupRequest>,
    ) -> Result<Response<storage::ResourceGroupOperationResponse>, Status> {
        let (rg_id, password) = request.into_inner().unpack()?;
        tracing::info!(
            rg_id = rg_id.get(),
            "Delete resource group request received."
        );
        self.inner
            .delete_resource_group(rg_id, &password)
            .await
            .map_err(|error| {
                self.resource_group_management_service_error_handler(error, "delete_resource_group")
            })?;
        Ok(Response::new(storage::ResourceGroupOperationResponse {}))
    }
}

#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> ExecutionManagerLivenessService
    for GrpcServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn register_execution_manager(
        &self,
        request: Request<storage::RegisterExecutionManagerRequest>,
    ) -> Result<Response<storage::RegisterExecutionManagerResponse>, Status> {
        let ip_address = request.into_inner().unpack()?;
        tracing::info!(% ip_address, "Execution manager registration request received.");
        let em_id = self
            .inner
            .register_execution_manager(ip_address)
            .await
            .map_err(|error| {
                self.execution_manager_liveness_service_error_handler(
                    error,
                    "register_execution_manager",
                )
            })?;
        Ok(Response::new(storage::RegisterExecutionManagerResponse {
            registration: Some(storage::ExecutionManagerRegistration {
                execution_manager_id: em_id.get(),
                session_id: self.inner.session_id(),
            }),
        }))
    }

    async fn update_execution_manager_heartbeat(
        &self,
        request: Request<storage::ExecutionManagerIdRequest>,
    ) -> Result<Response<storage::UpdateExecutionManagerHeartbeatResponse>, Status> {
        let em_id = request.into_inner().unpack()?;
        tracing::info!(
            em_id = em_id.get(),
            "Execution manager heartbeat request received."
        );
        self.inner
            .update_execution_manager_heartbeat(em_id)
            .await
            .map_err(|error| {
                self.execution_manager_liveness_service_error_handler(
                    error,
                    "update_execution_manager_heartbeat",
                )
            })?;
        Ok(Response::new(
            storage::UpdateExecutionManagerHeartbeatResponse {
                session_id: self.inner.session_id(),
            },
        ))
    }
}

#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> SchedulerRegistrationService
    for GrpcServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn register_scheduler(
        &self,
        request: Request<storage::RegisterSchedulerRequest>,
    ) -> Result<Response<storage::RegisterSchedulerResponse>, Status> {
        let (ip_address, port) = request.into_inner().unpack()?;
        tracing::info!(% ip_address, port, "Scheduler registration request received.");
        let scheduler_id = self
            .inner
            .register_scheduler(ip_address, port)
            .await
            .map_err(|error| {
                self.scheduler_registration_service_error_handler(error, "register_scheduler")
            })?;
        Ok(Response::new(storage::RegisterSchedulerResponse {
            registration: Some(storage::SchedulerRegistration {
                scheduler_id: scheduler_id.get(),
                session_id: self.inner.session_id(),
            }),
        }))
    }

    async fn get_schedulers(
        &self,
        _request: Request<storage::Void>,
    ) -> Result<Response<storage::GetSchedulersResponse>, Status> {
        tracing::info!("Get schedulers request received.");
        let schedulers = self.inner.get_schedulers().await.map_err(|error| {
            self.scheduler_registration_service_error_handler(error, "get_schedulers")
        })?;
        Ok(Response::new(storage::GetSchedulersResponse {
            schedulers: Some(storage::SchedulerRegistrations {
                schedulers: schedulers
                    .into_iter()
                    .map(storage::Scheduler::from)
                    .collect(),
            }),
        }))
    }
}

#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> SessionManagementService
    for GrpcServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn get_session(
        &self,
        _request: Request<storage::Void>,
    ) -> Result<Response<storage::GetSessionResponse>, Status> {
        let session_id = self.inner.session_id();
        tracing::info!(session_id, "Get session request received.");
        Ok(Response::new(storage::GetSessionResponse { session_id }))
    }
}

/// Converts a ready-queue task kind into its protobuf [`storage::TaskId`] form.
trait ToProtoTaskId {
    /// # Returns
    ///
    /// The protobuf task ID for this ready-queue lane marker.
    fn to_proto_task_id(self) -> storage::TaskId;
}

impl ToProtoTaskId for spider_core::task::TaskIndex {
    fn to_proto_task_id(self) -> storage::TaskId {
        storage::TaskId::from(TaskId::Index(self))
    }
}

impl ToProtoTaskId for CommitTaskMarker {
    fn to_proto_task_id(self) -> storage::TaskId {
        storage::TaskId::from(TaskId::Commit)
    }
}

impl ToProtoTaskId for CleanupTaskMarker {
    fn to_proto_task_id(self) -> storage::TaskId {
        storage::TaskId::from(TaskId::Cleanup)
    }
}

/// Builds a [`storage::ReadyTasks`] message from a batch of ready-queue entries.
///
/// # Type Parameters
///
/// * `TaskKindType` - The kind of ready-queue task (`ReadyTask`, `CommitTask`, or `CleanupTask`)
///   carried by each entry; must be convertible to a protobuf task ID.
///
/// # Returns
///
/// A [`storage::ReadyTasks`] carrying the storage session and the flattened ready tasks.
fn ready_tasks<TaskKindType: ToProtoTaskId>(
    session_id: SessionId,
    entries: Vec<ReadyQueueEntry<TaskKindType>>,
) -> storage::ReadyTasks {
    let tasks = entries
        .into_iter()
        .map(|entry| {
            let resource_group_id = entry.resource_group_id.get();
            let job_id = entry.job_id.get();
            let task_id = entry.task_kind.to_proto_task_id();
            storage::ReadyTask {
                resource_group_id,
                job_id,
                task_id: Some(task_id),
            }
        })
        .collect();
    storage::ReadyTasks { session_id, tasks }
}

/// # Returns
///
/// A [`storage::JobStateResponse`] carrying the given job state.
fn make_job_state_response(
    state: spider_core::job::JobState,
) -> Response<storage::JobStateResponse> {
    Response::new(storage::JobStateResponse {
        state: storage::JobState::from(state).into(),
    })
}

#[cfg(test)]
mod tests {
    use spider_core::types::id::{ExecutionManagerId, JobId, ResourceGroupId, SessionId};
    use tokio_util::sync::CancellationToken;
    use tonic::{Code, Request};

    use super::*;
    use crate::{
        ready_queue::{ReadyQueueConfig, ReadyQueueSenderHandle, create_ready_queue},
        state::{
            JobCache,
            JobCacheGcHandle,
            test_utils::{MockDbConnector, MockReadyQueueSender, MockTaskInstancePoolConnector},
        },
    };

    type TestGrpcState =
        GrpcServiceState<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector>;

    type TestGrpcStateWithReadyQueue =
        GrpcServiceState<ReadyQueueSenderHandle, MockDbConnector, MockTaskInstancePoolConnector>;

    const TEST_SESSION_ID: SessionId = 0;

    /// # Returns
    ///
    /// A [`TestGrpcState`] backed by a default mock DB connector.
    fn create_grpc_service() -> TestGrpcState {
        create_grpc_service_with_db(MockDbConnector::default())
    }

    /// # Returns
    ///
    /// A [`TestGrpcState`] backed by `db` and a [`MockReadyQueueSender`].
    fn create_grpc_service_with_db(db: MockDbConnector) -> TestGrpcState {
        let (_sender, receiver) =
            create_ready_queue(&ReadyQueueConfig::default()).expect("ready queue creation");
        let service = ServiceState::new(
            db,
            TEST_SESSION_ID,
            JobCache::new(),
            MockReadyQueueSender,
            receiver,
            MockTaskInstancePoolConnector,
            JobCacheGcHandle::new(tokio::sync::mpsc::unbounded_channel().0),
        );
        GrpcServiceState::new(service, CancellationToken::new())
    }

    /// # Returns
    ///
    /// A [`TestGrpcStateWithReadyQueue`] wired to a real ready queue, plus the queue's sender
    /// handle so tests can enqueue entries.
    fn create_grpc_service_with_ready_queue(
        db: MockDbConnector,
    ) -> (TestGrpcStateWithReadyQueue, ReadyQueueSenderHandle) {
        let (sender, receiver) =
            create_ready_queue(&ReadyQueueConfig::default()).expect("ready queue creation");
        let service = ServiceState::new(
            db,
            TEST_SESSION_ID,
            JobCache::new(),
            sender.clone(),
            receiver,
            MockTaskInstancePoolConnector,
            JobCacheGcHandle::new(tokio::sync::mpsc::unbounded_channel().0),
        );
        (
            GrpcServiceState::new(service, CancellationToken::new()),
            sender,
        )
    }

    #[tokio::test]
    async fn get_session_returns_service_session_id() -> anyhow::Result<()> {
        let service = create_grpc_service();
        let response = service
            .get_session(Request::new(storage::Void {}))
            .await?
            .into_inner();
        assert_eq!(response.session_id, TEST_SESSION_ID);
        Ok(())
    }

    #[tokio::test]
    async fn add_verify_delete_resource_group_round_trip() -> anyhow::Result<()> {
        let service = create_grpc_service();
        let password = b"secret".to_vec();

        let add_response = service
            .add_resource_group(Request::new(storage::AddResourceGroupRequest {
                external_resource_group_id: "external-rg".to_owned(),
                password: password.clone(),
            }))
            .await?
            .into_inner();
        let rg_id = add_response.resource_group_id;

        service
            .verify_resource_group(Request::new(storage::VerifyResourceGroupRequest {
                resource_group_id: rg_id,
                password: password.clone(),
            }))
            .await?;

        service
            .delete_resource_group(Request::new(storage::DeleteResourceGroupRequest {
                resource_group_id: rg_id,
                password: password.clone(),
            }))
            .await?;

        let verify_after_delete = service
            .verify_resource_group(Request::new(storage::VerifyResourceGroupRequest {
                resource_group_id: rg_id,
                password,
            }))
            .await;
        let status = verify_after_delete.expect_err("verify should fail after delete");
        assert_eq!(status.code(), Code::NotFound);
        Ok(())
    }

    #[tokio::test]
    async fn delete_resource_group_rejects_wrong_password_as_unauthenticated() -> anyhow::Result<()>
    {
        let service = create_grpc_service();
        let password = b"secret".to_vec();
        let rg_id = service
            .add_resource_group(Request::new(storage::AddResourceGroupRequest {
                external_resource_group_id: "external-rg".to_owned(),
                password: password.clone(),
            }))
            .await?
            .into_inner()
            .resource_group_id;

        let result = service
            .delete_resource_group(Request::new(storage::DeleteResourceGroupRequest {
                resource_group_id: rg_id,
                password: b"wrong".to_vec(),
            }))
            .await;
        let status = result.expect_err("a wrong password should be rejected");
        assert_eq!(status.code(), Code::Unauthenticated);
        Ok(())
    }

    #[tokio::test]
    async fn verify_resource_group_rejects_wrong_password_as_unauthenticated() -> anyhow::Result<()>
    {
        let service = create_grpc_service();
        let password = b"secret".to_vec();
        let rg_id = service
            .add_resource_group(Request::new(storage::AddResourceGroupRequest {
                external_resource_group_id: "external-rg".to_owned(),
                password: password.clone(),
            }))
            .await?
            .into_inner()
            .resource_group_id;

        let result = service
            .verify_resource_group(Request::new(storage::VerifyResourceGroupRequest {
                resource_group_id: rg_id,
                password: b"wrong".to_vec(),
            }))
            .await;
        let status = result.expect_err("a wrong password should be rejected");
        assert_eq!(status.code(), Code::Unauthenticated);
        Ok(())
    }

    #[tokio::test]
    async fn register_task_instance_reports_missing_job_as_not_found() -> anyhow::Result<()> {
        let service = create_grpc_service();
        let result = service
            .register_task_instance(Request::new(storage::RegisterTaskInstanceRequest {
                job_id: JobId::random().get(),
                task_id: Some(storage::TaskId::from(TaskId::Index(0))),
                execution_manager_id: ExecutionManagerId::from(1).get(),
                session_id: TEST_SESSION_ID,
            }))
            .await;
        let status = result.expect_err("an unknown job should be rejected");
        assert_eq!(status.code(), Code::NotFound);
        Ok(())
    }

    #[tokio::test]
    async fn register_execution_manager_returns_id_and_session() -> anyhow::Result<()> {
        let service = create_grpc_service();
        let response = service
            .register_execution_manager(Request::new(storage::RegisterExecutionManagerRequest {
                ip_address: "127.0.0.1".to_owned(),
            }))
            .await?
            .into_inner();
        let registration = response
            .registration
            .expect("registration should be present");
        assert_eq!(registration.session_id, TEST_SESSION_ID);
        assert_ne!(registration.execution_manager_id, 0);
        Ok(())
    }

    #[tokio::test]
    async fn heartbeat_returns_session_for_registered_em() -> anyhow::Result<()> {
        let service = create_grpc_service();
        let em_id = service
            .register_execution_manager(Request::new(storage::RegisterExecutionManagerRequest {
                ip_address: "127.0.0.1".to_owned(),
            }))
            .await?
            .into_inner()
            .registration
            .expect("registration should be present")
            .execution_manager_id;

        let response = service
            .update_execution_manager_heartbeat(Request::new(storage::ExecutionManagerIdRequest {
                execution_manager_id: em_id,
            }))
            .await?
            .into_inner();
        assert_eq!(response.session_id, TEST_SESSION_ID);
        Ok(())
    }

    #[tokio::test]
    async fn heartbeat_rejects_unknown_em() -> anyhow::Result<()> {
        let service = create_grpc_service();
        let result = service
            .update_execution_manager_heartbeat(Request::new(storage::ExecutionManagerIdRequest {
                execution_manager_id: ExecutionManagerId::from(999).get(),
            }))
            .await;
        let status = result.expect_err("an unknown em id should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        Ok(())
    }

    #[tokio::test]
    async fn register_and_get_schedulers() -> anyhow::Result<()> {
        let service = create_grpc_service();
        let register_response = service
            .register_scheduler(Request::new(storage::RegisterSchedulerRequest {
                ip_address: "127.0.0.1".to_owned(),
                port: 5678,
            }))
            .await?
            .into_inner();
        let registration = register_response
            .registration
            .expect("registration should be present");
        let scheduler_id = registration.scheduler_id;
        assert_eq!(registration.session_id, TEST_SESSION_ID);

        let get_response = service
            .get_schedulers(Request::new(storage::Void {}))
            .await?
            .into_inner();
        let schedulers = get_response
            .schedulers
            .expect("schedulers should be present");
        assert_eq!(schedulers.schedulers.len(), 1);
        assert_eq!(schedulers.schedulers[0].scheduler_id, scheduler_id);
        Ok(())
    }

    #[tokio::test]
    async fn resend_ready_tasks_succeeds() -> anyhow::Result<()> {
        let service = create_grpc_service();
        service
            .resend_ready_tasks(Request::new(storage::ResendReadyTasksRequest {}))
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn poll_ready_tasks_returns_entries() -> anyhow::Result<()> {
        const TASK_INDEX: usize = 3;
        let (service, sender) = create_grpc_service_with_ready_queue(MockDbConnector::default());
        let rg_id = ResourceGroupId::from(7);
        let job_id = JobId::from(11);
        sender
            .send_task_ready(rg_id, job_id, vec![TASK_INDEX])
            .await
            .expect("send_task_ready should succeed");

        let response = service
            .poll_ready_tasks(Request::new(storage::PollReadyTasksRequest {
                max_items: 10,
                wait_ms: 100,
            }))
            .await?
            .into_inner();
        let tasks = response.tasks.expect("ready tasks should be present");
        assert_eq!(tasks.session_id, TEST_SESSION_ID);
        assert_eq!(tasks.tasks.len(), 1);
        assert_eq!(tasks.tasks[0].resource_group_id, rg_id.get());
        assert_eq!(tasks.tasks[0].job_id, job_id.get());
        Ok(())
    }
}
