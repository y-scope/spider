//! gRPC service adapters for the storage service.

use async_trait::async_trait;
use spider_core::types::id::TaskId;
use spider_core::types::io::SerializedTaskOutputs;
use spider_proto_rust::common;
use spider_proto_rust::storage::ExecutionManagerLivenessService;
use spider_proto_rust::storage::InboundQueueService;
use spider_proto_rust::storage::JobOrchestrationService;
use spider_proto_rust::storage::ResourceGroupManagementService;
use spider_proto_rust::storage::SchedulerRegistration;
use spider_proto_rust::storage::SchedulerRegistrationService;
use spider_proto_rust::storage::SessionManagementService;
use spider_proto_rust::storage::TaskInstanceManagementService;
use spider_proto_rust::storage::{self};
use spider_proto_rust::unpack::RequestUnpack;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::cache::error::CacheError;
use crate::db::DbError;
use crate::db::DbStorage;
use crate::ready_queue::ReadyQueueEntry;
use crate::ready_queue::ReadyQueueSender;
use crate::state::ServiceState;
use crate::state::StorageServerError;
use crate::task_instance_pool::TaskInstancePoolConnector;

/// gRPC adapter over a storage [`ServiceState`].
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready queue sender type.
/// * `DbConnectorType` - The database connector type.
/// * `TaskInstancePoolConnectorType` - The task instance pool connector type.
#[derive(Clone)]
pub struct GrpcServiceState<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> {
    inner: ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    cancellation_token: CancellationToken,
}

impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
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

    /// Error handler for job orchestration service errors.
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
    ///   * Any other (database or otherwise unexpected) error.
    /// * `UNAUTHENTICATED` for an unknown or unauthorized resource group.
    /// * `NOT_FOUND` for a missing job.
    /// * `FAILED_PRECONDITION` for operations on an invalid job state.
    /// * `INVALID_ARGUMENT` for a malformed task graph, inputs, or request.
    pub fn job_orchestration_service_error_handler(
        &self,
        error: StorageServerError,
        tag: &'static str,
    ) -> Status {
        const SERVICE_NAME: &str = "JobOrchestration";
        match error {
            error @ StorageServerError::Db(
                DbError::ResourceGroupNotFound(_) | DbError::InvalidPassword(_),
            ) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Invalid resource group."
                );
                Status::unauthenticated("invalid resource group")
            }

            error @ StorageServerError::Db(DbError::JobNotFound(_)) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Job not found."
                );
                Status::not_found("job not found")
            }

            error @ StorageServerError::Db(
                DbError::InvalidJobStateTransition { .. } | DbError::UnexpectedJobState { .. },
            ) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Invalid job state."
                );
                Status::failed_precondition(error.to_string())
            }

            StorageServerError::JobNotFound(_) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Job not found."
                );
                Status::not_found("job not found")
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

            _ => self.default_error_handler(SERVICE_NAME, tag, &error, false),
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
    /// * `NOT_FOUND` for a request issued from a stale session.
    /// * `FAILED_PRECONDITION` for a request issued against a stale cache state.
    /// * `INVALID_ARGUMENT` for malformed inputs or a malformed request.
    pub fn task_instance_management_service_error_handler(
        &self,
        error: StorageServerError,
        tag: &'static str,
    ) -> Status {
        const SERVICE_NAME: &str = "TaskInstanceManagement";
        match error {
            StorageServerError::StaleSession(storage_session) => {
                tracing::warn!(
                    storage_session,
                    service = SERVICE_NAME,
                    tag,
                    "The request was issued from a stale session."
                );
                Status::not_found(format!(
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
                    "The request attempts to access a job that does not exist in the cache."
                );
                // The absence of the job is considered a stale cache state.
                Status::failed_precondition(format!("cache stale: {error}"))
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

            _ => self.default_error_handler(SERVICE_NAME, tag, &error, true),
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
    /// * `INTERNAL` for any failure happened on the server side. This method should never fail
    ///   under the service's assumption.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn inbound_queue_service_error_handler(
        &self,
        error: StorageServerError,
        tag: &'static str,
    ) -> Status {
        const SERVICE_NAME: &str = "InboundQueue";
        self.default_error_handler(SERVICE_NAME, tag, &error, false)
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
    /// * `UNAUTHENTICATED` for an unknown or unauthorized resource group.
    /// * `ALREADY_EXISTS` for a duplicate external resource group ID.
    /// * `INTERNAL` for:
    ///   * A fatal cache-internal error (the service will restart).
    ///   * Any other unexpected failure.
    pub fn resource_group_management_service_error_handler(
        &self,
        error: StorageServerError,
        tag: &'static str,
    ) -> Status {
        const SERVICE_NAME: &str = "ResourceGroupManagement";
        match error {
            error @ StorageServerError::Db(
                DbError::ResourceGroupNotFound(_) | DbError::InvalidPassword(_),
            ) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Invalid resource group."
                );
                Status::unauthenticated("invalid resource group")
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

            error => self.default_error_handler(SERVICE_NAME, tag, &error, false),
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
    /// * `INTERNAL` for:
    ///   * A fatal cache-internal error (the service will restart).
    ///   * Any other unexpected failure.
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
                    "Illegal execution manager ID."
                );
                Status::invalid_argument(error.to_string())
            }

            error => self.default_error_handler(SERVICE_NAME, tag, &error, false),
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
    /// * `INTERNAL` for any failure happened on the server side.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    fn scheduler_registration_service_error_handler(
        &self,
        error: StorageServerError,
        tag: &'static str,
    ) -> Status {
        const SERVICE_NAME: &str = "SchedulerRegistration";
        self.default_error_handler(SERVICE_NAME, tag, &error, false)
    }

    /// Handles generic storage server errors.
    ///
    /// This handler maps every [`StorageServerError`] to an `INTERNAL` [`Status`] with a generic
    /// error message. Errors are treated as fatal, and the storage service is cancelled in any of
    /// the following cases:
    ///
    /// * The error is [`CacheError::Internal`].
    /// * The error is [`DbError::CorruptedDbState`].
    /// * `strict_mode` is enabled.
    ///
    /// Non-fatal errors are logged as warnings. Fatal errors are logged as errors before
    /// cancellation.
    ///
    /// # Returns
    ///
    /// An `INTERNAL` [`Status`] with a generic storage service error message.
    #[must_use]
    fn default_error_handler(
        &self,
        service: &'static str,
        tag: &'static str,
        error: &StorageServerError,
        strict_mode: bool,
    ) -> Status {
        match error {
            StorageServerError::Cache(CacheError::Internal(e)) => {
                tracing::error!(
                    error = % e,
                    service,
                    tag,
                    "Internal error in the cache layer. Cancelling service."
                );
                self.cancellation_token.cancel();
            }

            StorageServerError::Db(DbError::CorruptedDbState(e)) => {
                tracing::error!(
                    error = % e,
                    service,
                    tag,
                    "Internal error in the database layer. Cancelling service."
                );
                self.cancellation_token.cancel();
            }

            e => {
                if strict_mode {
                    tracing::error!(
                        error = % e,
                        service,
                        tag,
                        "Unexpected internal error. Cancelling service."
                    );
                    self.cancellation_token.cancel();
                } else {
                    tracing::warn!(
                        error = % e,
                        service,
                        tag,
                        "Unexpected internal error."
                    );
                }
            }
        }

        Status::internal("storage service internal error")
    }

    /// Builds a [`storage::ReadyTasks`] message from a batch of ready-queue entries.
    ///
    /// # Type Parameters
    ///
    /// * `TaskKindType` - The kind of ready-queue task carried by each entry:
    ///   * [`spider_core::task::TaskIndex`] for the regular lane.
    ///   * [`crate::ready_queue::CommitTaskMarker`] for the commit lane.
    ///   * [`crate::ready_queue::CleanupTaskMarker`] for the cleanup lane.
    ///
    /// # Returns
    ///
    /// A [`storage::ReadyTasks`] carrying the storage session and the flattened ready tasks.
    fn build_ready_tasks<TaskKindType>(
        &self,
        entries: Vec<ReadyQueueEntry<TaskKindType>>,
        to_task_id: impl Fn(TaskKindType) -> common::TaskId,
    ) -> storage::ReadyTasks {
        let tasks = entries
            .into_iter()
            .map(|entry| {
                let resource_group_id = entry.resource_group_id.get();
                let job_id = entry.job_id.get();
                let task_id = to_task_id(entry.task_kind);
                storage::ReadyTask {
                    resource_group_id,
                    job_id,
                    task_id: Some(task_id),
                }
            })
            .collect();
        storage::ReadyTasks {
            session_id: self.inner.session_id(),
            tasks,
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
        const TAG: &str = "get_job_outputs";

        let job_id = request.into_inner().unpack()?;
        tracing::info!(job_id = job_id.get(), "Job outputs request received.");

        let job_outputs = self
            .inner
            .get_job_outputs(job_id)
            .await
            .map_err(|error| self.job_orchestration_service_error_handler(error, TAG))?;

        let serialized_job_outputs = SerializedTaskOutputs::serialize_with_size_hint(&job_outputs)
            .map_err(|error| {
                let storage_error = StorageServerError::Serde(Box::new(error));
                self.job_orchestration_service_error_handler(storage_error, TAG)
            })?;

        Ok(Response::new(storage::JobOutputsResponse {
            serialized_outputs: serialized_job_outputs.to_raw(),
        }))
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
    ) -> Result<Response<common::Void>, Status> {
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

        Ok(Response::new(common::Void {}))
    }

    async fn report_task_failure(
        &self,
        request: Request<storage::ReportTaskFailureRequest>,
    ) -> Result<Response<common::Void>, Status> {
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
        Ok(Response::new(common::Void {}))
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
        tracing::info!(max_items, "Poll ready tasks request received.");
        let entries = self
            .inner
            .poll_ready_tasks(max_items, wait)
            .await
            .map_err(|error| self.inbound_queue_service_error_handler(error, "poll_ready_tasks"))?;
        Ok(Response::new(storage::PollReadyTasksResponse {
            tasks: Some(self.build_ready_tasks(entries, |task_index| {
                common::TaskId::from(TaskId::Index(task_index))
            })),
        }))
    }

    async fn poll_ready_commit_tasks(
        &self,
        request: Request<storage::PollReadyTasksRequest>,
    ) -> Result<Response<storage::PollReadyTasksResponse>, Status> {
        let (max_items, wait) = request.into_inner().unpack()?;
        tracing::info!(max_items, "Poll ready commit tasks request received.");
        let entries = self
            .inner
            .poll_commit_ready_tasks(max_items, wait)
            .await
            .map_err(|error| {
                self.inbound_queue_service_error_handler(error, "poll_ready_commit_tasks")
            })?;
        Ok(Response::new(storage::PollReadyTasksResponse {
            tasks: Some(self.build_ready_tasks(entries, |_| common::TaskId::from(TaskId::Commit))),
        }))
    }

    async fn poll_ready_cleanup_tasks(
        &self,
        request: Request<storage::PollReadyTasksRequest>,
    ) -> Result<Response<storage::PollReadyTasksResponse>, Status> {
        let (max_items, wait) = request.into_inner().unpack()?;
        tracing::info!(max_items, "Poll ready cleanup tasks request received.");
        let entries = self
            .inner
            .poll_cleanup_ready_tasks(max_items, wait)
            .await
            .map_err(|error| {
                self.inbound_queue_service_error_handler(error, "poll_ready_cleanup_tasks")
            })?;
        Ok(Response::new(storage::PollReadyTasksResponse {
            tasks: Some(self.build_ready_tasks(entries, |_| common::TaskId::from(TaskId::Cleanup))),
        }))
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
    ) -> Result<Response<common::Void>, Status> {
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
        Ok(Response::new(common::Void {}))
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
        let (ip_addr, port) = request.into_inner().unpack()?;
        tracing::info!(% ip_addr, port, "Scheduler registration request received.");
        let scheduler_id = self
            .inner
            .register_scheduler(ip_addr, port)
            .await
            .map_err(|error| {
                self.scheduler_registration_service_error_handler(error, "register_scheduler")
            })?;
        Ok(Response::new(storage::RegisterSchedulerResponse {
            registration: Some(SchedulerRegistration {
                scheduler_id: scheduler_id.get(),
                session_id: self.inner.session_id(),
            }),
        }))
    }

    async fn get_schedulers(
        &self,
        _request: Request<common::Void>,
    ) -> Result<Response<storage::GetSchedulersResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
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
        _request: Request<common::Void>,
    ) -> Result<Response<storage::GetSessionResponse>, Status> {
        let session_id = self.inner.session_id();
        tracing::info!(session_id, "Get session request received.");
        Ok(Response::new(storage::GetSessionResponse { session_id }))
    }
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
