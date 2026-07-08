//! gRPC service adapter for the scheduler service.

use async_trait::async_trait;
use spider_core::types::id::SchedulerId;
use spider_core::types::id::SessionId;
use spider_core::types::scheduler::TaskAssignment;
use spider_proto_rust::common;
use spider_proto_rust::scheduler::NextTaskResponse;
use spider_proto_rust::scheduler::SchedulerAssignment;
use spider_proto_rust::scheduler::SchedulerService;
use spider_proto_rust::scheduler::next_task_response;
use spider_proto_rust::scheduler::{self};
use spider_proto_rust::unpack::RequestUnpack;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::dispatch_queue::DispatchQueueSource;
use crate::error::SchedulerError;
use crate::error::SchedulerServiceError;
use crate::execution_manager_registry::ExecutionManagerRegistryError;
use crate::service::SchedulerServiceState;

/// gRPC adapter over a [`SchedulerServiceState`].
///
/// # Type Parameters
///
/// * `DispatchQueueSourceType` - The reader side of the dispatching queue the underlying service
///   drains.
#[derive(Clone)]
pub struct GrpcSchedulerService<DispatchQueueSourceType: DispatchQueueSource + 'static> {
    inner: SchedulerServiceState<DispatchQueueSourceType>,
    cancellation_token: CancellationToken,
}

impl<DispatchQueueSourceType: DispatchQueueSource + 'static>
    GrpcSchedulerService<DispatchQueueSourceType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A new [`GrpcSchedulerService`] wrapping [`SchedulerServiceState`].
    #[must_use]
    pub const fn new(
        inner: SchedulerServiceState<DispatchQueueSourceType>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            inner,
            cancellation_token,
        }
    }

    /// Error handler for scheduler service errors.
    ///
    /// This function maps the given [`SchedulerServiceError`] to a [`Status`] that can be sent to
    /// the client. The errors are logged for observability.
    ///
    /// # Returns
    ///
    /// The [`Status`] to send to the client:
    ///
    /// * `NOT_FOUND` for an unknown execution manager.
    /// * `INTERNAL` for any other failure happened on the server side.
    #[must_use]
    pub fn service_error_handler(&self, error: SchedulerServiceError, tag: &'static str) -> Status {
        const SERVICE_NAME: &str = "Scheduler";
        match error {
            SchedulerServiceError::Scheduler(SchedulerError::DispatchQueueClosed) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    "Dispatch queue is closed."
                );
                Status::internal("scheduler is shutting down")
            }

            SchedulerServiceError::EMRegistry(ExecutionManagerRegistryError::EmNotFound(em_id)) => {
                tracing::warn!(
                    error = % error,
                    service = SERVICE_NAME,
                    tag,
                    em_id = % em_id,
                    "Execution manager not found."
                );
                Status::not_found("execution manager not found")
            }

            SchedulerServiceError::Scheduler(SchedulerError::Internal(e)) => {
                tracing::error!(
                    error = % e,
                    service = SERVICE_NAME,
                    tag,
                    "Internal error. Cancelling service."
                );
                self.cancellation_token.cancel();
                Status::internal("scheduler service unavailable")
            }

            error => {
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
}

/// Implementation of [`SchedulerService`].
///
/// All possible errors that can occur during scheduling can be found in
/// [`GrpcSchedulerService::service_error_handler`].
#[async_trait]
impl<DispatchQueueSourceType: DispatchQueueSource + 'static> SchedulerService
    for GrpcSchedulerService<DispatchQueueSourceType>
{
    async fn next_task(
        &self,
        request: Request<scheduler::NextTaskRequest>,
    ) -> Result<Response<NextTaskResponse>, Status> {
        let (em_id, prev_assignment, wait_time) = request.into_inner().unpack()?;
        tracing::info!(em_id = em_id.get(), "Task dispatching request received.");

        let dispatched = self
            .inner
            .next_task(em_id, prev_assignment, wait_time)
            .await
            .map_err(|error| self.service_error_handler(error, "next_task"))?;

        let response = match dispatched {
            Some((session_id, assignment)) => {
                make_next_task_response(assignment, self.inner.scheduler_id(), session_id)
            }
            None => NextTaskResponse {
                result: Some(next_task_response::Result::NoTask(common::Void {})),
            },
        };
        Ok(Response::new(response))
    }

    async fn heartbeat(
        &self,
        request: Request<scheduler::HeartbeatRequest>,
    ) -> Result<Response<common::Void>, Status> {
        let em_id = request.into_inner().unpack()?;
        tracing::info!(
            em_id = em_id.get(),
            "Execution manager heartbeat request received."
        );

        self.inner
            .heartbeat(em_id)
            .await
            .map_err(|error| self.service_error_handler(error, "heartbeat"))?;
        Ok(Response::new(common::Void {}))
    }

    async fn shutdown(
        &self,
        request: Request<scheduler::ShutdownRequest>,
    ) -> Result<Response<common::Void>, Status> {
        let (em_id, prev_assignments) = request.into_inner().unpack()?;
        tracing::info!(
            em_id = em_id.get(),
            "Execution manager shutdown request received."
        );

        self.inner
            .shutdown(em_id, prev_assignments)
            .await
            .map_err(|error| self.service_error_handler(error, "shutdown"))?;
        Ok(Response::new(common::Void {}))
    }
}

/// # Returns
///
/// A [`NextTaskResponse`] carrying the given assignment, stamped with `scheduler_id` and paired
/// with `session_id`.
fn make_next_task_response(
    assignment: TaskAssignment,
    scheduler_id: SchedulerId,
    session_id: SessionId,
) -> NextTaskResponse {
    NextTaskResponse {
        result: Some(next_task_response::Result::Assignment(
            SchedulerAssignment {
                id: assignment.id.get(),
                resource_group_id: assignment.resource_group_id.get(),
                job_id: assignment.job_id.get(),
                task_id: Some(common::TaskId::from(assignment.task_id)),
                scheduler_id: scheduler_id.get(),
                session_id,
            },
        )),
    }
}
