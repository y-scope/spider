//! gRPC service adapters for the storage runtime.

use async_trait::async_trait;
use spider_core::types::id::{SessionId, TaskId};
use spider_proto_rust::{
    storage::{
        self,
        execution_manager_liveness_service_server::ExecutionManagerLivenessService,
        get_schedulers_response,
        inbound_queue_service_server::InboundQueueService,
        job_orchestration_service_server::JobOrchestrationService,
        poll_ready_tasks_response,
        register_execution_manager_response,
        register_scheduler_response,
        resend_ready_tasks_response,
        resource_group_id_response,
        resource_group_management_service_server::ResourceGroupManagementService,
        resource_group_operation_response,
        scheduler_registration_service_server::SchedulerRegistrationService,
        session_management_service_server::SessionManagementService,
        task_instance_management_service_server::TaskInstanceManagementService,
        update_execution_manager_heartbeat_response,
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
        let result = match self.inner.poll_ready_tasks(max_items, wait).await {
            Ok(entries) => poll_ready_tasks_response::Result::Tasks(ready_tasks(
                self.inner.session_id(),
                entries,
            )),
            Err(error) => poll_ready_tasks_response::Result::Error(to_inbound_error(&error)),
        };
        Ok(Response::new(storage::PollReadyTasksResponse {
            result: Some(result),
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
        let result = match self.inner.poll_commit_ready_tasks(max_items, wait).await {
            Ok(entries) => poll_ready_tasks_response::Result::Tasks(ready_tasks(
                self.inner.session_id(),
                entries,
            )),
            Err(error) => poll_ready_tasks_response::Result::Error(to_inbound_error(&error)),
        };
        Ok(Response::new(storage::PollReadyTasksResponse {
            result: Some(result),
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
        let result = match self.inner.poll_cleanup_ready_tasks(max_items, wait).await {
            Ok(entries) => poll_ready_tasks_response::Result::Tasks(ready_tasks(
                self.inner.session_id(),
                entries,
            )),
            Err(error) => poll_ready_tasks_response::Result::Error(to_inbound_error(&error)),
        };
        Ok(Response::new(storage::PollReadyTasksResponse {
            result: Some(result),
        }))
    }

    async fn resend_ready_tasks(
        &self,
        _request: Request<storage::ResendReadyTasksRequest>,
    ) -> Result<Response<storage::ResendReadyTasksResponse>, Status> {
        tracing::info!("Resend ready tasks request received.");
        let result = match self.inner.resend_ready_tasks().await {
            Ok(()) => resend_ready_tasks_response::Result::Ok(storage::Void {}),
            Err(error) => resend_ready_tasks_response::Result::Error(to_inbound_error(&error)),
        };
        Ok(Response::new(storage::ResendReadyTasksResponse {
            result: Some(result),
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
        let (external_id, password, request_session_id) = request.into_inner().unpack()?;
        tracing::info!(external_id = % external_id, "Add resource group request received.");
        let current_session = self.inner.session_id();
        let result = if request_session_id == current_session {
            match self.inner.add_resource_group(external_id, password).await {
                Ok(rg_id) => resource_group_id_response::Result::ResourceGroupId(rg_id.get()),
                Err(error) => resource_group_id_response::Result::Error(to_resource_group_error(
                    &error,
                    current_session,
                )),
            }
        } else {
            resource_group_id_response::Result::Error(to_resource_group_error(
                &StorageServerError::StaleSession(current_session),
                current_session,
            ))
        };
        Ok(Response::new(storage::ResourceGroupIdResponse {
            result: Some(result),
        }))
    }

    async fn verify_resource_group(
        &self,
        request: Request<storage::VerifyResourceGroupRequest>,
    ) -> Result<Response<storage::ResourceGroupOperationResponse>, Status> {
        let (rg_id, password, request_session_id) = request.into_inner().unpack()?;
        tracing::info!(
            rg_id = rg_id.get(),
            "Verify resource group request received."
        );
        let current_session = self.inner.session_id();
        let result = if request_session_id == current_session {
            match self.inner.verify_resource_group(rg_id, &password).await {
                Ok(()) => resource_group_operation_response::Result::Ok(storage::Void {}),
                Err(error) => resource_group_operation_response::Result::Error(
                    to_resource_group_error(&error, current_session),
                ),
            }
        } else {
            resource_group_operation_response::Result::Error(to_resource_group_error(
                &StorageServerError::StaleSession(current_session),
                current_session,
            ))
        };
        Ok(Response::new(storage::ResourceGroupOperationResponse {
            result: Some(result),
        }))
    }

    async fn delete_resource_group(
        &self,
        request: Request<storage::ResourceGroupIdRequest>,
    ) -> Result<Response<storage::ResourceGroupOperationResponse>, Status> {
        let (rg_id, request_session_id) = request.into_inner().unpack()?;
        tracing::info!(
            rg_id = rg_id.get(),
            "Delete resource group request received."
        );
        let current_session = self.inner.session_id();
        let result = if request_session_id == current_session {
            match self.inner.delete_resource_group(rg_id).await {
                Ok(()) => resource_group_operation_response::Result::Ok(storage::Void {}),
                Err(error) => resource_group_operation_response::Result::Error(
                    to_resource_group_error(&error, current_session),
                ),
            }
        } else {
            resource_group_operation_response::Result::Error(to_resource_group_error(
                &StorageServerError::StaleSession(current_session),
                current_session,
            ))
        };
        Ok(Response::new(storage::ResourceGroupOperationResponse {
            result: Some(result),
        }))
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
        let result = match self.inner.register_execution_manager(ip_address).await {
            Ok(em_id) => register_execution_manager_response::Result::Registration(
                storage::ExecutionManagerRegistration {
                    execution_manager_id: em_id.get(),
                    session_id: self.inner.session_id(),
                },
            ),
            Err(error) => {
                register_execution_manager_response::Result::Error(to_liveness_error(&error))
            }
        };
        Ok(Response::new(storage::RegisterExecutionManagerResponse {
            result: Some(result),
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
        let result = match self.inner.update_execution_manager_heartbeat(em_id).await {
            Ok(()) => update_execution_manager_heartbeat_response::Result::SessionId(
                self.inner.session_id(),
            ),
            Err(error) => update_execution_manager_heartbeat_response::Result::Error(
                to_liveness_error(&error),
            ),
        };
        Ok(Response::new(
            storage::UpdateExecutionManagerHeartbeatResponse {
                result: Some(result),
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
        let result = match self.inner.register_scheduler(ip_address, port).await {
            Ok(scheduler_id) => {
                register_scheduler_response::Result::Registration(storage::SchedulerRegistration {
                    scheduler_id: scheduler_id.get(),
                    session_id: self.inner.session_id(),
                })
            }
            Err(error) => register_scheduler_response::Result::Error(to_scheduler_error(&error)),
        };
        Ok(Response::new(storage::RegisterSchedulerResponse {
            result: Some(result),
        }))
    }

    async fn get_schedulers(
        &self,
        _request: Request<storage::Void>,
    ) -> Result<Response<storage::GetSchedulersResponse>, Status> {
        tracing::info!("Get schedulers request received.");
        let result = match self.inner.get_schedulers().await {
            Ok(schedulers) => {
                get_schedulers_response::Result::Schedulers(storage::SchedulerRegistrations {
                    schedulers: schedulers
                        .into_iter()
                        .map(storage::Scheduler::from)
                        .collect(),
                })
            }
            Err(error) => get_schedulers_response::Result::Error(to_scheduler_error(&error)),
        };
        Ok(Response::new(storage::GetSchedulersResponse {
            result: Some(result),
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
/// # Returns
///
/// A [`storage::ReadyTasks`] carrying the storage session and the flattened ready tasks.
fn ready_tasks<TaskKind: ToProtoTaskId>(
    session_id: SessionId,
    entries: Vec<ReadyQueueEntry<TaskKind>>,
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

/// Maps an inbound-queue [`StorageServerError`] to a protobuf
/// [`storage::InboundQueueResponseError`].
///
/// # Returns
///
/// * `INBOUND_CLOSED` when the ready-queue channel is closed.
/// * `SERVER` for any other failure.
fn to_inbound_error(error: &StorageServerError) -> storage::InboundQueueResponseError {
    use storage::inbound_queue_response_error::ErrCode;
    let err_code = match error {
        StorageServerError::Cache(CacheError::Internal(InternalError::ReadyQueueChannelClosed)) => {
            ErrCode::InboundClosed
        }
        _ => ErrCode::Server,
    };
    storage::InboundQueueResponseError {
        err_code: err_code.into(),
        message: error.to_string(),
    }
}

/// Maps a resource-group [`StorageServerError`] to a protobuf
/// [`storage::ResourceGroupManagementError`].
///
/// # Returns
///
/// * `STALE_SESSION` when the request was issued against a stale storage session.
/// * `INVALID_INPUT` for an unknown resource group, a wrong password, or a duplicate external ID.
/// * `SERVER` for any other failure.
fn to_resource_group_error(
    error: &StorageServerError,
    current_session: SessionId,
) -> storage::ResourceGroupManagementError {
    use storage::resource_group_management_error::ErrCode;
    let err_code = match error {
        StorageServerError::StaleSession(_) => ErrCode::StaleSession,
        StorageServerError::Db(
            DbError::ResourceGroupNotFound(_)
            | DbError::InvalidPassword(_)
            | DbError::ResourceGroupAlreadyExists(_),
        ) => ErrCode::InvalidInput,
        _ => ErrCode::Server,
    };
    storage::ResourceGroupManagementError {
        err_code: err_code.into(),
        message: error.to_string(),
        storage_session: current_session,
    }
}

/// Maps an execution-manager-liveness [`StorageServerError`] to a protobuf
/// [`storage::ExecutionManagerLivenessError`].
///
/// # Returns
///
/// * `MARKED_DEAD` when the execution manager has already been reaped.
/// * `INVALID_INPUT` for an illegal execution manager ID.
/// * `SERVER` for any other failure.
fn to_liveness_error(error: &StorageServerError) -> storage::ExecutionManagerLivenessError {
    use storage::execution_manager_liveness_error::ErrCode;
    let err_code = match error {
        StorageServerError::Db(DbError::ExecutionManagerAlreadyDead(_)) => ErrCode::MarkedDead,
        StorageServerError::Db(DbError::IllegalExecutionManagerId(_)) => ErrCode::InvalidInput,
        _ => ErrCode::Server,
    };
    storage::ExecutionManagerLivenessError {
        err_code: err_code.into(),
        message: error.to_string(),
    }
}

/// Maps a scheduler-registration [`StorageServerError`] to a protobuf
/// [`storage::SchedulerRegistrationError`].
///
/// # Returns
///
/// `SERVER` for any failure; scheduler registration currently has no caller-visible error
/// classification beyond a generic server error.
fn to_scheduler_error(error: &StorageServerError) -> storage::SchedulerRegistrationError {
    storage::SchedulerRegistrationError {
        err_code: storage::scheduler_registration_error::ErrCode::Server.into(),
        message: error.to_string(),
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

#[cfg(test)]
mod tests {
    use spider_core::types::id::{ExecutionManagerId, JobId, ResourceGroupId, SessionId};
    use tokio_util::sync::CancellationToken;
    use tonic::Request;

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

    fn create_grpc_service() -> TestGrpcState {
        create_grpc_service_with_db(MockDbConnector::default())
    }

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
                session_id: TEST_SESSION_ID,
            }))
            .await?
            .into_inner();
        let rg_id = match add_response.result {
            Some(resource_group_id_response::Result::ResourceGroupId(rg_id)) => rg_id,
            other => panic!("expected a resource group id, got {other:?}"),
        };

        let verify_response = service
            .verify_resource_group(Request::new(storage::VerifyResourceGroupRequest {
                resource_group_id: rg_id,
                password: password.clone(),
                session_id: TEST_SESSION_ID,
            }))
            .await?
            .into_inner();
        assert!(matches!(
            verify_response.result,
            Some(resource_group_operation_response::Result::Ok(_))
        ));

        let delete_response = service
            .delete_resource_group(Request::new(storage::ResourceGroupIdRequest {
                resource_group_id: rg_id,
                session_id: TEST_SESSION_ID,
            }))
            .await?
            .into_inner();
        assert!(matches!(
            delete_response.result,
            Some(resource_group_operation_response::Result::Ok(_))
        ));

        let verify_after_delete = service
            .verify_resource_group(Request::new(storage::VerifyResourceGroupRequest {
                resource_group_id: rg_id,
                password,
                session_id: TEST_SESSION_ID,
            }))
            .await?
            .into_inner();
        assert!(matches!(
            verify_after_delete.result,
            Some(resource_group_operation_response::Result::Error(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn add_resource_group_rejects_stale_session() -> anyhow::Result<()> {
        let service = create_grpc_service();
        let response = service
            .add_resource_group(Request::new(storage::AddResourceGroupRequest {
                external_resource_group_id: "external-rg".to_owned(),
                password: b"secret".to_vec(),
                session_id: TEST_SESSION_ID + 1,
            }))
            .await?
            .into_inner();
        match response.result {
            Some(resource_group_id_response::Result::Error(error)) => {
                assert_eq!(
                    storage::resource_group_management_error::ErrCode::try_from(error.err_code)
                        .expect("valid err code"),
                    storage::resource_group_management_error::ErrCode::StaleSession
                );
                assert_eq!(error.storage_session, TEST_SESSION_ID);
            }
            other => panic!("expected a stale-session error, got {other:?}"),
        }
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
        let registration = match response.result {
            Some(register_execution_manager_response::Result::Registration(registration)) => {
                registration
            }
            other => panic!("expected a registration, got {other:?}"),
        };
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
            .result
            .and_then(|result| match result {
                register_execution_manager_response::Result::Registration(registration) => {
                    Some(registration.execution_manager_id)
                }
                register_execution_manager_response::Result::Error(_) => None,
            })
            .expect("registration should succeed");

        let response = service
            .update_execution_manager_heartbeat(Request::new(storage::ExecutionManagerIdRequest {
                execution_manager_id: em_id,
            }))
            .await?
            .into_inner();
        assert!(matches!(
            response.result,
            Some(update_execution_manager_heartbeat_response::Result::SessionId(TEST_SESSION_ID))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn heartbeat_rejects_unknown_em() -> anyhow::Result<()> {
        let service = create_grpc_service();
        let response = service
            .update_execution_manager_heartbeat(Request::new(storage::ExecutionManagerIdRequest {
                execution_manager_id: ExecutionManagerId::from(999).get(),
            }))
            .await?
            .into_inner();
        match response.result {
            Some(update_execution_manager_heartbeat_response::Result::Error(error)) => {
                assert_eq!(
                    storage::execution_manager_liveness_error::ErrCode::try_from(error.err_code)
                        .expect("valid err code"),
                    storage::execution_manager_liveness_error::ErrCode::InvalidInput
                );
            }
            other => panic!("expected an invalid-input error, got {other:?}"),
        }
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
        let scheduler_id = match register_response.result {
            Some(register_scheduler_response::Result::Registration(registration)) => {
                registration.scheduler_id
            }
            other => panic!("expected a registration, got {other:?}"),
        };
        assert_eq!(
            register_response
                .result
                .and_then(|result| match result {
                    register_scheduler_response::Result::Registration(registration) => {
                        Some(registration.session_id)
                    }
                    register_scheduler_response::Result::Error(_) => None,
                })
                .expect("session id should be present"),
            TEST_SESSION_ID
        );

        let get_response = service
            .get_schedulers(Request::new(storage::Void {}))
            .await?
            .into_inner();
        let schedulers = match get_response.result {
            Some(get_schedulers_response::Result::Schedulers(registrations)) => registrations,
            other => panic!("expected schedulers, got {other:?}"),
        };
        assert_eq!(schedulers.schedulers.len(), 1);
        assert_eq!(schedulers.schedulers[0].scheduler_id, scheduler_id);
        Ok(())
    }

    #[tokio::test]
    async fn resend_ready_tasks_succeeds() -> anyhow::Result<()> {
        let service = create_grpc_service();
        let response = service
            .resend_ready_tasks(Request::new(storage::ResendReadyTasksRequest {}))
            .await?
            .into_inner();
        assert!(matches!(
            response.result,
            Some(resend_ready_tasks_response::Result::Ok(_))
        ));
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
        let tasks = match response.result {
            Some(poll_ready_tasks_response::Result::Tasks(tasks)) => tasks,
            other => panic!("expected ready tasks, got {other:?}"),
        };
        assert_eq!(tasks.session_id, TEST_SESSION_ID);
        assert_eq!(tasks.tasks.len(), 1);
        assert_eq!(tasks.tasks[0].resource_group_id, rg_id.get());
        assert_eq!(tasks.tasks[0].job_id, job_id.get());
        Ok(())
    }
}
