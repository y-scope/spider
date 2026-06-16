//! gRPC service adapters for the storage runtime.

use std::{net::IpAddr, time::Duration};

use async_trait::async_trait;
use spider_core::{
    job::JobState,
    types::id::{ExecutionManagerId, JobId, ResourceGroupId, TaskId},
};
use spider_proto_rust::{
    error::Error as ProtoError,
    storage::{
        self,
        execution_manager_liveness_error,
        execution_manager_liveness_service_server::ExecutionManagerLivenessService,
        inbound_queue_response_error,
        inbound_queue_service_server::InboundQueueService,
        job_error_response,
        job_orchestration_error,
        job_orchestration_service_server::JobOrchestrationService,
        job_outputs_response,
        job_state_response,
        poll_ready_tasks_response,
        register_execution_manager_response,
        register_task_instance_response,
        resource_group_id_response,
        resource_group_management_error,
        resource_group_management_service_server::ResourceGroupManagementService,
        resource_group_operation_response,
        session_management_service_server::SessionManagementService,
        submit_job_response,
        task_instance_management_error,
        task_instance_management_service_server::TaskInstanceManagementService,
        task_instance_operation_response,
        update_execution_manager_heartbeat_response,
    },
};
use tonic::{Request, Response, Status};

use crate::{
    cache::error::{CacheError, InternalError},
    db::{DbError, DbStorage},
    ready_queue::{CleanupTaskMarker, CommitTaskMarker, ReadyQueueEntry, ReadyQueueSender},
    state::{ServiceState, StorageServerError},
    task_instance_pool::TaskInstancePoolConnector,
};

/// gRPC adapter over a storage [`ServiceState`].
#[derive(Clone)]
pub struct StorageGrpcService<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    service_state:
        ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
}

impl<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> StorageGrpcService<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A new [`StorageGrpcService`] wrapping `service_state`.
    #[must_use]
    pub const fn new(
        service_state: ServiceState<
            ReadyQueueSenderType,
            DbConnectorType,
            TaskInstancePoolConnectorType,
        >,
    ) -> Self {
        Self { service_state }
    }

    /// Validates a request session against the current runtime session.
    ///
    /// # Errors
    ///
    /// Returns [`StorageServerError::StaleSession`] if the request session is stale.
    fn validate_session(&self, session_id: u64) -> Result<(), StorageServerError> {
        if session_id != self.service_state.session_id() {
            return Err(StorageServerError::StaleSession);
        }
        Ok(())
    }
}

#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> JobOrchestrationService
    for StorageGrpcService<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn submit_job(
        &self,
        request: Request<storage::SubmitJobRequest>,
    ) -> Result<Response<storage::SubmitJobResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(
            session_id = request.session_id,
            resource_group_id = request.resource_group_id,
            task_graph_size = request.serialized_task_graph.len(),
            input_count = request.serialized_inputs.len(),
            "Received SubmitJob request."
        );
        let result = self.validate_session(request.session_id).and_then(|()| {
            String::from_utf8(request.serialized_task_graph)
                .map_err(|e| StorageServerError::BadRequest(e.to_string()))
        });
        let result = match result {
            Ok(serialized_task_graph) => {
                self.service_state
                    .register_job(
                        ResourceGroupId::from(request.resource_group_id),
                        serialized_task_graph,
                        request.serialized_inputs,
                    )
                    .await
            }
            Err(error) => Err(error),
        };
        Ok(Response::new(storage::SubmitJobResponse {
            result: Some(match result {
                Ok(job_id) => submit_job_response::Result::JobId(job_id.get()),
                Err(error) => {
                    submit_job_response::Result::Error(job_orchestration_error(&error, self))
                }
            }),
        }))
    }

    async fn start_job(
        &self,
        request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobStateResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(job_id = request.job_id, "Received StartJob request.");
        let job_id = JobId::from(request.job_id);
        let result = self
            .service_state
            .start_job(job_id)
            .await
            .map(|()| JobState::Running);
        Ok(Response::new(job_state_response_from_result(result, self)))
    }

    async fn cancel_job(
        &self,
        request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobStateResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(job_id = request.job_id, "Received CancelJob request.");
        let result = self
            .service_state
            .cancel_job(JobId::from(request.job_id))
            .await;
        Ok(Response::new(job_state_response_from_result(result, self)))
    }

    async fn get_job_state(
        &self,
        request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobStateResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(job_id = request.job_id, "Received GetJobState request.");
        let result = self
            .service_state
            .get_job_state(JobId::from(request.job_id))
            .await;
        Ok(Response::new(job_state_response_from_result(result, self)))
    }

    async fn get_job_outputs(
        &self,
        request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobOutputsResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(job_id = request.job_id, "Received GetJobOutputs request.");
        let result = self
            .service_state
            .get_job_outputs(JobId::from(request.job_id))
            .await;
        Ok(Response::new(storage::JobOutputsResponse {
            result: Some(match result {
                Ok(outputs) => {
                    job_outputs_response::Result::Outputs(storage::JobOutputs { outputs })
                }
                Err(error) => {
                    job_outputs_response::Result::Error(job_orchestration_error(&error, self))
                }
            }),
        }))
    }

    async fn get_job_error(
        &self,
        request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobErrorResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(job_id = request.job_id, "Received GetJobError request.");
        let result = self
            .service_state
            .get_job_error(JobId::from(request.job_id))
            .await;
        Ok(Response::new(storage::JobErrorResponse {
            result: Some(match result {
                Ok(error_message) => job_error_response::Result::ErrorMessage(error_message),
                Err(error) => {
                    job_error_response::Result::Error(job_orchestration_error(&error, self))
                }
            }),
        }))
    }
}

#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> TaskInstanceManagementService
    for StorageGrpcService<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn register_task_instance(
        &self,
        request: Request<storage::RegisterTaskInstanceRequest>,
    ) -> Result<Response<storage::RegisterTaskInstanceResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(
            session_id = request.session_id,
            job_id = request.job_id,
            execution_manager_id = request.execution_manager_id,
            "Received RegisterTaskInstance request."
        );
        let result = request_task_id(request.task_id).map(|task_id| {
            (
                request.session_id,
                JobId::from(request.job_id),
                task_id,
                ExecutionManagerId::from(request.execution_manager_id),
            )
        });
        let result = match result {
            Ok((session_id, job_id, task_id, execution_manager_id)) => {
                self.service_state
                    .create_task_instance(session_id, job_id, task_id, execution_manager_id)
                    .await
            }
            Err(error) => Err(error),
        };
        Ok(Response::new(storage::RegisterTaskInstanceResponse {
            result: Some(match result {
                Ok(execution_context) => match bincode::serialize(&execution_context) {
                    Ok(bytes) => register_task_instance_response::Result::ExecutionContext(bytes),
                    Err(error) => register_task_instance_response::Result::Error(
                        task_instance_management_error_response(
                            &StorageServerError::BadRequest(error.to_string()),
                            self,
                        ),
                    ),
                },
                Err(error) => register_task_instance_response::Result::Error(
                    task_instance_management_error_response(&error, self),
                ),
            }),
        }))
    }

    async fn report_task_success(
        &self,
        request: Request<storage::ReportTaskSuccessRequest>,
    ) -> Result<Response<storage::TaskInstanceOperationResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(
            session_id = request.session_id,
            job_id = request.job_id,
            task_instance_id = request.task_instance_id,
            output_size = request.serialized_outputs.len(),
            "Received ReportTaskSuccess request."
        );
        let result = request_task_id(request.task_id).and_then(|task_id| {
            validate_report_outputs(&task_id, &request.serialized_outputs)?;
            Ok(task_id)
        });
        let result = match result {
            Ok(TaskId::Index(task_index)) => {
                self.service_state
                    .succeed_task_instance(
                        request.session_id,
                        JobId::from(request.job_id),
                        request.task_instance_id,
                        task_index,
                        request.serialized_outputs,
                    )
                    .await
            }
            Ok(TaskId::Commit) => {
                self.service_state
                    .succeed_commit_task_instance(
                        request.session_id,
                        JobId::from(request.job_id),
                        request.task_instance_id,
                    )
                    .await
            }
            Ok(TaskId::Cleanup) => {
                self.service_state
                    .succeed_cleanup_task_instance(
                        request.session_id,
                        JobId::from(request.job_id),
                        request.task_instance_id,
                    )
                    .await
            }
            Err(error) => Err(error),
        };
        Ok(Response::new(task_instance_operation_response_from_result(
            result.map(|_| ()),
            self,
        )))
    }

    async fn report_task_failure(
        &self,
        request: Request<storage::ReportTaskFailureRequest>,
    ) -> Result<Response<storage::TaskInstanceOperationResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(
            session_id = request.session_id,
            job_id = request.job_id,
            task_instance_id = request.task_instance_id,
            "Received ReportTaskFailure request."
        );
        let result = request_task_id(request.task_id);
        let result = match result {
            Ok(task_id) => {
                self.service_state
                    .fail_task_instance(
                        request.session_id,
                        JobId::from(request.job_id),
                        request.task_instance_id,
                        task_id,
                        request.error_message,
                    )
                    .await
            }
            Err(error) => Err(error),
        };
        Ok(Response::new(task_instance_operation_response_from_result(
            result.map(|_| ()),
            self,
        )))
    }
}

#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> InboundQueueService
    for StorageGrpcService<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn poll_ready_tasks(
        &self,
        request: Request<storage::PollReadyTasksRequest>,
    ) -> Result<Response<storage::PollReadyTasksResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(
            max_items = request.max_items,
            wait_ms = request.wait_ms,
            "Received PollReadyTasks request."
        );
        let result = poll_request(request);
        let result = match result {
            Ok((max_tasks, wait)) => self.service_state.poll_ready_tasks(max_tasks, wait).await,
            Err(error) => Err(error),
        };
        Ok(Response::new(poll_response(result.map(|entries| {
            task_entries_to_ready_tasks(self, entries)
        }))))
    }

    async fn poll_ready_commit_tasks(
        &self,
        request: Request<storage::PollReadyTasksRequest>,
    ) -> Result<Response<storage::PollReadyTasksResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(
            max_items = request.max_items,
            wait_ms = request.wait_ms,
            "Received PollReadyCommitTasks request."
        );
        let result = poll_request(request);
        let result = match result {
            Ok((max_tasks, wait)) => {
                self.service_state
                    .poll_commit_ready_tasks(max_tasks, wait)
                    .await
            }
            Err(error) => Err(error),
        };
        Ok(Response::new(poll_response(result.map(|entries| {
            commit_entries_to_ready_tasks(self, entries)
        }))))
    }

    async fn poll_ready_cleanup_tasks(
        &self,
        request: Request<storage::PollReadyTasksRequest>,
    ) -> Result<Response<storage::PollReadyTasksResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(
            max_items = request.max_items,
            wait_ms = request.wait_ms,
            "Received PollReadyCleanupTasks request."
        );
        let result = poll_request(request);
        let result = match result {
            Ok((max_tasks, wait)) => {
                self.service_state
                    .poll_cleanup_ready_tasks(max_tasks, wait)
                    .await
            }
            Err(error) => Err(error),
        };
        Ok(Response::new(poll_response(result.map(|entries| {
            cleanup_entries_to_ready_tasks(self, entries)
        }))))
    }
}

#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> ResourceGroupManagementService
    for StorageGrpcService<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn add_resource_group(
        &self,
        request: Request<storage::AddResourceGroupRequest>,
    ) -> Result<Response<storage::ResourceGroupIdResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(
            session_id = request.session_id,
            external_resource_group_id = %request.external_resource_group_id,
            "Received AddResourceGroup request."
        );
        let result = match self.validate_session(request.session_id) {
            Ok(()) => {
                self.service_state
                    .add_resource_group(request.external_resource_group_id, request.password)
                    .await
            }
            Err(error) => Err(error),
        };
        Ok(Response::new(storage::ResourceGroupIdResponse {
            result: Some(match result {
                Ok(rg_id) => resource_group_id_response::Result::ResourceGroupId(rg_id.get()),
                Err(error) => {
                    resource_group_id_response::Result::Error(resource_group_error(&error, self))
                }
            }),
        }))
    }

    async fn verify_resource_group(
        &self,
        request: Request<storage::VerifyResourceGroupRequest>,
    ) -> Result<Response<storage::ResourceGroupOperationResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(
            session_id = request.session_id,
            resource_group_id = request.resource_group_id,
            "Received VerifyResourceGroup request."
        );
        let result = match self.validate_session(request.session_id) {
            Ok(()) => {
                self.service_state
                    .verify_resource_group(
                        ResourceGroupId::from(request.resource_group_id),
                        &request.password,
                    )
                    .await
            }
            Err(error) => Err(error),
        };
        Ok(Response::new(
            resource_group_operation_response_from_result(result, self),
        ))
    }

    async fn delete_resource_group(
        &self,
        request: Request<storage::ResourceGroupIdRequest>,
    ) -> Result<Response<storage::ResourceGroupOperationResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(
            session_id = request.session_id,
            resource_group_id = request.resource_group_id,
            "Received DeleteResourceGroup request."
        );
        let result = match self.validate_session(request.session_id) {
            Ok(()) => {
                self.service_state
                    .delete_resource_group(ResourceGroupId::from(request.resource_group_id))
                    .await
            }
            Err(error) => Err(error),
        };
        Ok(Response::new(
            resource_group_operation_response_from_result(result, self),
        ))
    }
}

#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> ExecutionManagerLivenessService
    for StorageGrpcService<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn register_execution_manager(
        &self,
        request: Request<storage::RegisterExecutionManagerRequest>,
    ) -> Result<Response<storage::RegisterExecutionManagerResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(
            ip_address = %request.ip_address,
            "Received RegisterExecutionManager request."
        );
        let ip = request.ip_address.parse::<IpAddr>();
        let result = match ip {
            Ok(ip) => self.service_state.register_execution_manager(ip).await,
            Err(error) => Err(StorageServerError::BadRequest(error.to_string())),
        };
        Ok(Response::new(storage::RegisterExecutionManagerResponse {
            result: Some(match result {
                Ok(em_id) => register_execution_manager_response::Result::Registration(
                    storage::ExecutionManagerRegistration {
                        execution_manager_id: em_id.get(),
                        session_id: self.service_state.session_id(),
                    },
                ),
                Err(error) => {
                    register_execution_manager_response::Result::Error(liveness_error(&error))
                }
            }),
        }))
    }

    async fn update_execution_manager_heartbeat(
        &self,
        request: Request<storage::ExecutionManagerIdRequest>,
    ) -> Result<Response<storage::UpdateExecutionManagerHeartbeatResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(
            execution_manager_id = request.execution_manager_id,
            "Received UpdateExecutionManagerHeartbeat request."
        );
        let result = self
            .service_state
            .update_execution_manager_heartbeat(ExecutionManagerId::from(
                request.execution_manager_id,
            ))
            .await;
        Ok(Response::new(
            storage::UpdateExecutionManagerHeartbeatResponse {
                result: Some(match result {
                    Ok(()) => update_execution_manager_heartbeat_response::Result::SessionId(
                        self.service_state.session_id(),
                    ),
                    Err(error) => update_execution_manager_heartbeat_response::Result::Error(
                        liveness_error(&error),
                    ),
                }),
            },
        ))
    }
}

#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> SessionManagementService
    for StorageGrpcService<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn get_session(
        &self,
        _request: Request<storage::Void>,
    ) -> Result<Response<storage::GetSessionResponse>, Status> {
        tracing::debug!("Received GetSession request.");
        Ok(Response::new(storage::GetSessionResponse {
            session_id: self.service_state.session_id(),
        }))
    }
}

/// Converts a runtime job-state result into a protobuf response.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready-queue sender implementation used by the service state.
/// * `DbConnectorType` - The database connector implementation used by the service state.
/// * `TaskInstancePoolConnectorType` - The task-instance pool connector implementation used by the
///   service state.
///
/// # Returns
///
/// A [`storage::JobStateResponse`] from the runtime result.
fn job_state_response_from_result<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    result: Result<JobState, StorageServerError>,
    service: &StorageGrpcService<
        ReadyQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    >,
) -> storage::JobStateResponse {
    storage::JobStateResponse {
        result: Some(match result {
            Ok(state) => job_state_response::Result::State(storage::JobState::from(state) as i32),
            Err(error) => {
                job_state_response::Result::Error(job_orchestration_error(&error, service))
            }
        }),
    }
}

/// Converts a task-instance runtime result into a protobuf response.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready-queue sender implementation used by the service state.
/// * `DbConnectorType` - The database connector implementation used by the service state.
/// * `TaskInstancePoolConnectorType` - The task-instance pool connector implementation used by the
///   service state.
///
/// # Returns
///
/// A [`storage::TaskInstanceOperationResponse`] from the runtime result.
fn task_instance_operation_response_from_result<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    result: Result<(), StorageServerError>,
    service: &StorageGrpcService<
        ReadyQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    >,
) -> storage::TaskInstanceOperationResponse {
    storage::TaskInstanceOperationResponse {
        result: Some(match result {
            Ok(()) => task_instance_operation_response::Result::Ok(storage::Void {}),
            Err(error) => task_instance_operation_response::Result::Error(
                task_instance_management_error_response(&error, service),
            ),
        }),
    }
}

/// Converts a resource-group runtime result into a protobuf response.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready-queue sender implementation used by the service state.
/// * `DbConnectorType` - The database connector implementation used by the service state.
/// * `TaskInstancePoolConnectorType` - The task-instance pool connector implementation used by the
///   service state.
///
/// # Returns
///
/// A [`storage::ResourceGroupOperationResponse`] from the runtime result.
fn resource_group_operation_response_from_result<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    result: Result<(), StorageServerError>,
    service: &StorageGrpcService<
        ReadyQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    >,
) -> storage::ResourceGroupOperationResponse {
    storage::ResourceGroupOperationResponse {
        result: Some(match result {
            Ok(()) => resource_group_operation_response::Result::Ok(storage::Void {}),
            Err(error) => resource_group_operation_response::Result::Error(resource_group_error(
                &error, service,
            )),
        }),
    }
}

/// Converts a protobuf task ID into a core task ID.
///
/// # Returns
///
/// A protobuf task ID converted into a core [`TaskId`] on success.
///
/// # Errors
///
/// Returns [`StorageServerError::BadRequest`] if the request does not carry a task ID or if the
/// task ID is invalid.
fn request_task_id(task_id: Option<storage::TaskId>) -> Result<TaskId, StorageServerError> {
    let task_id = task_id
        .ok_or_else(|| StorageServerError::BadRequest(ProtoError::TaskIdKindMissing.to_string()))?;
    TaskId::try_from(task_id).map_err(|error| StorageServerError::BadRequest(error.to_string()))
}

/// Validates that success-report outputs match the task kind.
///
/// # Errors
///
/// Returns [`StorageServerError::BadRequest`] if a termination task carries outputs.
fn validate_report_outputs(
    task_id: &TaskId,
    serialized_outputs: &[u8],
) -> Result<(), StorageServerError> {
    if !matches!(task_id, TaskId::Index(_)) && !serialized_outputs.is_empty() {
        return Err(StorageServerError::BadRequest(
            "termination task success report must not carry outputs".to_owned(),
        ));
    }
    Ok(())
}

/// Validates a poll-ready request.
///
/// # Returns
///
/// A validated poll request.
///
/// # Errors
///
/// Returns [`StorageServerError::BadRequest`] if the requested item count is invalid.
fn poll_request(
    request: storage::PollReadyTasksRequest,
) -> Result<(usize, Duration), StorageServerError> {
    let max_items = usize::try_from(request.max_items)
        .map_err(|error| StorageServerError::BadRequest(error.to_string()))?;
    Ok((max_items, Duration::from_millis(request.wait_ms)))
}

/// Converts a poll-ready runtime result into a protobuf response.
///
/// # Returns
///
/// A [`storage::PollReadyTasksResponse`] from the runtime result.
fn poll_response(
    result: Result<storage::ReadyTasks, StorageServerError>,
) -> storage::PollReadyTasksResponse {
    storage::PollReadyTasksResponse {
        result: Some(match result {
            Ok(tasks) => poll_ready_tasks_response::Result::Tasks(tasks),
            Err(error) => poll_ready_tasks_response::Result::Error(inbound_queue_error(&error)),
        }),
    }
}

/// Converts index-task ready-queue entries into protobuf ready tasks.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready-queue sender implementation used by the service state.
/// * `DbConnectorType` - The database connector implementation used by the service state.
/// * `TaskInstancePoolConnectorType` - The task-instance pool connector implementation used by the
///   service state.
///
/// # Returns
///
/// A [`storage::ReadyTasks`] response body carrying index tasks.
fn task_entries_to_ready_tasks<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    service: &StorageGrpcService<
        ReadyQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    >,
    entries: Vec<ReadyQueueEntry<usize>>,
) -> storage::ReadyTasks {
    storage::ReadyTasks {
        session_id: service.service_state.session_id(),
        tasks: entries
            .into_iter()
            .map(|entry| {
                ready_task(
                    entry.resource_group_id,
                    entry.job_id,
                    TaskId::Index(entry.task_kind),
                )
            })
            .collect(),
    }
}

/// Converts commit-task ready-queue entries into protobuf ready tasks.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready-queue sender implementation used by the service state.
/// * `DbConnectorType` - The database connector implementation used by the service state.
/// * `TaskInstancePoolConnectorType` - The task-instance pool connector implementation used by the
///   service state.
///
/// # Returns
///
/// A [`storage::ReadyTasks`] response body carrying commit tasks.
fn commit_entries_to_ready_tasks<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    service: &StorageGrpcService<
        ReadyQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    >,
    entries: Vec<ReadyQueueEntry<CommitTaskMarker>>,
) -> storage::ReadyTasks {
    storage::ReadyTasks {
        session_id: service.service_state.session_id(),
        tasks: entries
            .into_iter()
            .map(|entry| ready_task(entry.resource_group_id, entry.job_id, TaskId::Commit))
            .collect(),
    }
}

/// Converts cleanup-task ready-queue entries into protobuf ready tasks.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready-queue sender implementation used by the service state.
/// * `DbConnectorType` - The database connector implementation used by the service state.
/// * `TaskInstancePoolConnectorType` - The task-instance pool connector implementation used by the
///   service state.
///
/// # Returns
///
/// A [`storage::ReadyTasks`] response body carrying cleanup tasks.
fn cleanup_entries_to_ready_tasks<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    service: &StorageGrpcService<
        ReadyQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    >,
    entries: Vec<ReadyQueueEntry<CleanupTaskMarker>>,
) -> storage::ReadyTasks {
    storage::ReadyTasks {
        session_id: service.service_state.session_id(),
        tasks: entries
            .into_iter()
            .map(|entry| ready_task(entry.resource_group_id, entry.job_id, TaskId::Cleanup))
            .collect(),
    }
}

/// Converts core task identifiers into a protobuf ready task.
///
/// # Returns
///
/// A [`storage::ReadyTask`] carrying the given identifiers.
fn ready_task(
    resource_group_id: ResourceGroupId,
    job_id: JobId,
    task_id: TaskId,
) -> storage::ReadyTask {
    storage::ReadyTask {
        resource_group_id: resource_group_id.get(),
        job_id: job_id.get(),
        task_id: Some(storage::TaskId::from(task_id)),
    }
}

/// Converts a runtime error into a job-orchestration protobuf error.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready-queue sender implementation used by the service state.
/// * `DbConnectorType` - The database connector implementation used by the service state.
/// * `TaskInstancePoolConnectorType` - The task-instance pool connector implementation used by the
///   service state.
///
/// # Returns
///
/// A [`storage::JobOrchestrationError`] with the service session ID.
fn job_orchestration_error<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    error: &StorageServerError,
    service: &StorageGrpcService<
        ReadyQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    >,
) -> storage::JobOrchestrationError {
    storage::JobOrchestrationError {
        err_code: job_orchestration_error_code(error) as i32,
        message: error.to_string(),
        storage_session: service.service_state.session_id(),
    }
}

/// Maps a runtime error to a job-orchestration protobuf error code.
///
/// # Returns
///
/// The protobuf error code matching the runtime error category.
const fn job_orchestration_error_code(
    error: &StorageServerError,
) -> job_orchestration_error::ErrCode {
    match error {
        StorageServerError::StaleSession => job_orchestration_error::ErrCode::StaleSession,
        StorageServerError::JobNotFound(_) | StorageServerError::Db(DbError::JobNotFound(_)) => {
            job_orchestration_error::ErrCode::JobNotFound
        }
        StorageServerError::BadRequest(_)
        | StorageServerError::Task(_)
        | StorageServerError::Tdl(_)
        | StorageServerError::Db(DbError::ResourceGroupNotFound(_)) => {
            job_orchestration_error::ErrCode::InvalidInput
        }
        _ => job_orchestration_error::ErrCode::Server,
    }
}

/// Converts a runtime error into a task-instance protobuf error.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready-queue sender implementation used by the service state.
/// * `DbConnectorType` - The database connector implementation used by the service state.
/// * `TaskInstancePoolConnectorType` - The task-instance pool connector implementation used by the
///   service state.
///
/// # Returns
///
/// A [`storage::TaskInstanceManagementError`] with the service session ID.
fn task_instance_management_error_response<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    error: &StorageServerError,
    service: &StorageGrpcService<
        ReadyQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    >,
) -> storage::TaskInstanceManagementError {
    storage::TaskInstanceManagementError {
        err_code: task_instance_error_code(error) as i32,
        message: error.to_string(),
        storage_session: service.service_state.session_id(),
    }
}

/// Maps a runtime error to a task-instance protobuf error code.
///
/// # Returns
///
/// The protobuf error code matching the runtime error category.
const fn task_instance_error_code(
    error: &StorageServerError,
) -> task_instance_management_error::ErrCode {
    match error {
        StorageServerError::StaleSession => task_instance_management_error::ErrCode::StaleSession,
        StorageServerError::Cache(CacheError::StaleState(_))
        | StorageServerError::JobNotFound(_) => task_instance_management_error::ErrCode::CacheStale,
        StorageServerError::BadRequest(_)
        | StorageServerError::Task(_)
        | StorageServerError::Tdl(_)
        | StorageServerError::Db(DbError::IllegalExecutionManagerId(_)) => {
            task_instance_management_error::ErrCode::InvalidInput
        }
        _ => task_instance_management_error::ErrCode::Server,
    }
}

/// Converts a runtime error into an inbound-queue protobuf error.
///
/// # Returns
///
/// A [`storage::InboundQueueResponseError`] for the runtime error.
fn inbound_queue_error(error: &StorageServerError) -> storage::InboundQueueResponseError {
    storage::InboundQueueResponseError {
        err_code: inbound_queue_error_code(error) as i32,
        message: error.to_string(),
    }
}

/// Maps a runtime error to an inbound-queue protobuf error code.
///
/// # Returns
///
/// The protobuf error code matching the runtime error category.
const fn inbound_queue_error_code(
    error: &StorageServerError,
) -> inbound_queue_response_error::ErrCode {
    match error {
        StorageServerError::Cache(CacheError::Internal(InternalError::ReadyQueueChannelClosed)) => {
            inbound_queue_response_error::ErrCode::InboundClosed
        }
        StorageServerError::BadRequest(_) => inbound_queue_response_error::ErrCode::InvalidInput,
        _ => inbound_queue_response_error::ErrCode::Server,
    }
}

/// Converts a runtime error into a resource-group protobuf error.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready-queue sender implementation used by the service state.
/// * `DbConnectorType` - The database connector implementation used by the service state.
/// * `TaskInstancePoolConnectorType` - The task-instance pool connector implementation used by the
///   service state.
///
/// # Returns
///
/// A [`storage::ResourceGroupManagementError`] with the service session ID.
fn resource_group_error<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    error: &StorageServerError,
    service: &StorageGrpcService<
        ReadyQueueSenderType,
        DbConnectorType,
        TaskInstancePoolConnectorType,
    >,
) -> storage::ResourceGroupManagementError {
    storage::ResourceGroupManagementError {
        err_code: resource_group_error_code(error) as i32,
        message: error.to_string(),
        storage_session: service.service_state.session_id(),
    }
}

/// Maps a runtime error to a resource-group protobuf error code.
///
/// # Returns
///
/// The protobuf error code matching the runtime error category.
const fn resource_group_error_code(
    error: &StorageServerError,
) -> resource_group_management_error::ErrCode {
    match error {
        StorageServerError::StaleSession => resource_group_management_error::ErrCode::StaleSession,
        StorageServerError::BadRequest(_)
        | StorageServerError::Db(
            DbError::ResourceGroupNotFound(_)
            | DbError::ResourceGroupAlreadyExists(_)
            | DbError::InvalidPassword(_),
        ) => resource_group_management_error::ErrCode::InvalidInput,
        _ => resource_group_management_error::ErrCode::Server,
    }
}

/// Converts a runtime error into an execution-manager liveness protobuf error.
///
/// # Returns
///
/// A [`storage::ExecutionManagerLivenessError`] for the runtime error.
fn liveness_error(error: &StorageServerError) -> storage::ExecutionManagerLivenessError {
    storage::ExecutionManagerLivenessError {
        err_code: liveness_error_code(error) as i32,
        message: error.to_string(),
    }
}

/// Maps a runtime error to an execution-manager liveness protobuf error code.
///
/// # Returns
///
/// The protobuf error code matching the runtime error category.
const fn liveness_error_code(
    error: &StorageServerError,
) -> execution_manager_liveness_error::ErrCode {
    match error {
        StorageServerError::Db(DbError::ExecutionManagerAlreadyDead(_)) => {
            execution_manager_liveness_error::ErrCode::MarkedDead
        }
        StorageServerError::BadRequest(_)
        | StorageServerError::Db(DbError::IllegalExecutionManagerId(_)) => {
            execution_manager_liveness_error::ErrCode::InvalidInput
        }
        _ => execution_manager_liveness_error::ErrCode::Server,
    }
}
