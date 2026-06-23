//! gRPC service adapters for the storage runtime.

use async_trait::async_trait;
use spider_proto_rust::{
    common,
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
};
use tonic::{Request, Response, Status};

use crate::{
    db::DbStorage,
    ready_queue::ReadyQueueSender,
    state::ServiceState,
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
    _inner: ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
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
    ) -> Self {
        Self { _inner: inner }
    }
}

#[async_trait]
impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> JobOrchestrationService
    for GrpcServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    async fn submit_job(
        &self,
        _request: Request<storage::SubmitJobRequest>,
    ) -> Result<Response<storage::SubmitJobResponse>, Status> {
        todo!("Not implemented")
    }

    async fn start_job(
        &self,
        _request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobStateResponse>, Status> {
        todo!("Not implemented")
    }

    async fn cancel_job(
        &self,
        _request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobStateResponse>, Status> {
        todo!("Not implemented")
    }

    async fn get_job_state(
        &self,
        _request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobStateResponse>, Status> {
        todo!("Not implemented")
    }

    async fn get_job_outputs(
        &self,
        _request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobOutputsResponse>, Status> {
        todo!("Not implemented")
    }

    async fn get_job_error(
        &self,
        _request: Request<storage::JobIdRequest>,
    ) -> Result<Response<storage::JobErrorResponse>, Status> {
        todo!("Not implemented")
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
        _request: Request<storage::RegisterTaskInstanceRequest>,
    ) -> Result<Response<storage::RegisterTaskInstanceResponse>, Status> {
        todo!("Not implemented")
    }

    async fn report_task_success(
        &self,
        _request: Request<storage::ReportTaskSuccessRequest>,
    ) -> Result<Response<storage::TaskInstanceOperationResponse>, Status> {
        todo!("Not implemented")
    }

    async fn report_task_failure(
        &self,
        _request: Request<storage::ReportTaskFailureRequest>,
    ) -> Result<Response<storage::TaskInstanceOperationResponse>, Status> {
        todo!("Not implemented")
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
        _request: Request<common::Void>,
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
        _request: Request<common::Void>,
    ) -> Result<Response<storage::GetSessionResponse>, Status> {
        todo!("Not implemented")
    }
}
