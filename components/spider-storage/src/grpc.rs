//! gRPC service adapters for the storage runtime.

use async_trait::async_trait;
use spider_core::types::{id::TaskId, io::SerializedTaskOutputs};
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
    cache::error::CacheError,
    db::{DbError, DbStorage},
    ready_queue::ReadyQueueSender,
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

    /// Error handler for job orchestration service errors.
    ///
    /// This function maps the given [`StorageServerError`] to a [`Status`] that can be sent to the
    /// client. The errors are logged for observability.
    ///
    /// # Returns
    ///
    /// The [`Status`] to send to the client:
    ///
    /// * `UNAVAILABLE` for a fatal cache-internal error (the service will restart).
    /// * `UNAUTHENTICATED` for an unknown or unauthorized resource group.
    /// * `NOT_FOUND` for a missing job.
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
                tracing::error!(
                    error = % e,
                    service = SERVICE_NAME,
                    tag,
                    "Internal error in the cache layer. Cancelling service."
                );
                self.cancellation_token.cancel();
                Status::unavailable("storage service unavailable")
            }

            StorageServerError::Db(db_error) => match &db_error {
                DbError::ResourceGroupNotFound(_) | DbError::InvalidPassword(_) => {
                    tracing::warn!(
                        error = % db_error,
                        service = SERVICE_NAME,
                        tag,
                        "Invalid resource group."
                    );
                    Status::unauthenticated("invalid resource group")
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

            error @ (StorageServerError::Tdl(_) | StorageServerError::BadRequest(_)) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Invalid argument."
                );
                Status::invalid_argument(error.to_string())
            }

            _ => {
                tracing::error!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Unexpected internal error."
                );
                Status::internal("internal error")
            }
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
                tracing::error!(
                    error = % e,
                    service = SERVICE_NAME,
                    tag,
                    "Internal error in the cache layer. Cancelling service."
                );
                self.cancellation_token.cancel();
                Status::internal("storage service unavailable")
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

            _ => {
                tracing::error!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Unexpected internal error. Cancelling service to avoid cache corruption."
                );
                self.cancellation_token.cancel();
                Status::internal("internal error")
            }
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
        _request: Request<storage::PollReadyTasksRequest>,
    ) -> Result<Response<storage::PollReadyTasksResponse>, Status> {
        todo!("Not implemented")
    }

    async fn poll_ready_commit_tasks(
        &self,
        _request: Request<storage::PollReadyTasksRequest>,
    ) -> Result<Response<storage::PollReadyTasksResponse>, Status> {
        todo!("Not implemented")
    }

    async fn poll_ready_cleanup_tasks(
        &self,
        _request: Request<storage::PollReadyTasksRequest>,
    ) -> Result<Response<storage::PollReadyTasksResponse>, Status> {
        todo!("Not implemented")
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
        _request: Request<storage::AddResourceGroupRequest>,
    ) -> Result<Response<storage::ResourceGroupIdResponse>, Status> {
        todo!("Not implemented")
    }

    async fn verify_resource_group(
        &self,
        _request: Request<storage::VerifyResourceGroupRequest>,
    ) -> Result<Response<storage::ResourceGroupOperationResponse>, Status> {
        todo!("Not implemented")
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
        _request: Request<storage::RegisterExecutionManagerRequest>,
    ) -> Result<Response<storage::RegisterExecutionManagerResponse>, Status> {
        todo!("Not implemented")
    }

    async fn update_execution_manager_heartbeat(
        &self,
        _request: Request<storage::ExecutionManagerIdRequest>,
    ) -> Result<Response<storage::UpdateExecutionManagerHeartbeatResponse>, Status> {
        todo!("Not implemented")
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
        _request: Request<storage::RegisterSchedulerRequest>,
    ) -> Result<Response<storage::RegisterSchedulerResponse>, Status> {
        todo!("Not implemented")
    }

    async fn get_schedulers(
        &self,
        _request: Request<storage::Void>,
    ) -> Result<Response<storage::GetSchedulersResponse>, Status> {
        todo!("Not implemented")
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
        todo!("Not implemented")
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
