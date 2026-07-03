//! gRPC service adapter for the scheduler service.
//!
//! [`GrpcSchedulerService`] wraps a [`SchedulerServiceState`] and implements the generated
//! [`SchedulerService`] trait, translating inbound protobuf requests into domain calls and mapping
//! [`SchedulerServiceError`]s back to [`tonic::Status`]. It owns the runtime [`CancellationToken`]
//! so a fatal internal error can cancel the scheduler runtime, mirroring the split in
//! `spider-storage` between the domain [`SchedulerServiceState`] and its gRPC adapter.

use async_trait::async_trait;
use spider_core::types::{
    id::{SchedulerId, SessionId},
    scheduler::TaskAssignment,
};
use spider_proto_rust::{
    common,
    scheduler::{
        self,
        NextTaskResponse,
        SchedulerAssignment,
        next_task_response,
        scheduler_service_server::SchedulerService,
    },
    unpack::RequestUnpack,
};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::{
    dispatch_queue::DispatchQueueSource,
    error::{SchedulerError, SchedulerServiceError},
    execution_manager_registry::ExecutionManagerRegistryError,
    service::SchedulerServiceState,
};

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
    /// * `NOT_FOUND` for an unknown execution manager or task assignment.
    /// * `FAILED_PRECONDITION` for an invalid storage session.
    /// * `INTERNAL` when the dispatching queue is closed (the scheduler is shutting down), for a
    ///   fatal internal error (the service will be cancelled), and any other otherwise unexpected
    ///   error.
    pub fn service_error_handler(&self, error: SchedulerServiceError, tag: &'static str) -> Status {
        const SERVICE_NAME: &str = "Scheduler";
        match error {
            SchedulerServiceError::Scheduler(SchedulerError::DispatchQueueClosed) => {
                tracing::warn!(
                    error = %error,
                    service = SERVICE_NAME,
                    tag,
                    "Dispatch queue is closed."
                );
                Status::internal("scheduler is shutting down")
            }

            SchedulerServiceError::Scheduler(SchedulerError::InvalidSessionId(session_id)) => {
                tracing::warn!(
                    error = %error,
                    service = SERVICE_NAME,
                    tag,
                    session_id,
                    "Invalid session ID."
                );
                Status::failed_precondition(error.to_string())
            }

            SchedulerServiceError::EMRegistry(ExecutionManagerRegistryError::EmNotFound(em_id)) => {
                tracing::warn!(
                    error = %error,
                    service = SERVICE_NAME,
                    tag,
                    em_id = %em_id,
                    "Execution manager not found."
                );
                Status::not_found("execution manager not found")
            }

            SchedulerServiceError::EMRegistry(
                ExecutionManagerRegistryError::TaskAssignmentNotFound(em_id, assignment_id),
            ) => {
                tracing::warn!(
                    error = %error,
                    service = SERVICE_NAME,
                    tag,
                    em_id = %em_id,
                    assignment_id = %assignment_id,
                    "Task assignment not found."
                );
                Status::not_found("task assignment not found")
            }

            SchedulerServiceError::Scheduler(SchedulerError::Internal(e)) => {
                tracing::error!(
                    error = %e,
                    service = SERVICE_NAME,
                    tag,
                    "Internal error. Cancelling service."
                );
                self.cancellation_token.cancel();
                Status::internal("scheduler service unavailable")
            }

            error => {
                tracing::error!(
                    error = %error,
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
        const TAG: &str = "next_task";

        let (em_id, prev_assignment, wait_time) = request.into_inner().unpack()?;
        tracing::info!(em_id = em_id.get(), "NextTask request received.");

        match self
            .inner
            .next_task(em_id, prev_assignment, wait_time)
            .await
        {
            Ok(Some((session_id, assignment))) => {
                // Benchmark instrumentation: the log timestamp marks when the scheduler dispatches
                // this task assignment to an execution manager.
                tracing::info!(
                    job_id = assignment.job_id.get(),
                    task_id = ? assignment.task_id,
                    "Dispatched a task assignment to an execution manager."
                );
                Ok(Response::new(make_next_task_response(
                    assignment,
                    self.inner.scheduler_id(),
                    session_id,
                )))
            }
            Ok(None) => Ok(Response::new(NextTaskResponse {
                result: Some(next_task_response::Result::NoTask(common::Void {})),
            })),
            Err(error) => Err(self.service_error_handler(error, TAG)),
        }
    }

    async fn heartbeat(
        &self,
        request: Request<scheduler::HeartbeatRequest>,
    ) -> Result<Response<common::Void>, Status> {
        let em_id = request.into_inner().unpack()?;
        tracing::info!(em_id = em_id.get(), "Heartbeat request received.");

        match self.inner.heartbeat(em_id).await {
            Ok(()) => Ok(Response::new(common::Void {})),
            Err(error) => Err(self.service_error_handler(error, "heartbeat")),
        }
    }

    async fn shutdown(
        &self,
        request: Request<scheduler::ShutdownRequest>,
    ) -> Result<Response<common::Void>, Status> {
        let (em_id, prev_assignments) = request.into_inner().unpack()?;
        tracing::info!(em_id = em_id.get(), "Shutdown request received.");

        match self.inner.shutdown(em_id, prev_assignments).await {
            Ok(()) => Ok(Response::new(common::Void {})),
            Err(error) => Err(self.service_error_handler(error, "shutdown")),
        }
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
