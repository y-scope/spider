//! gRPC-backed [`SchedulerClient`] implementation.

use async_trait::async_trait;
use spider_core::types::{id::ExecutionManagerId, scheduler::TaskAssignmentRecord};
use spider_proto_rust::scheduler::{self, scheduler_service_client::SchedulerServiceClient};
use tonic::transport::{Channel, Endpoint};

use crate::client::{SchedulerClient, SchedulerError, SchedulerResponse};

/// gRPC-backed [`SchedulerClient`] implementation.
#[derive(Debug, Clone)]
pub struct GrpcSchedulerClient {
    client: SchedulerServiceClient<Channel>,
}

impl GrpcSchedulerClient {
    /// Connects to the scheduler gRPC endpoint.
    ///
    /// # Returns
    ///
    /// A new [`GrpcSchedulerClient`] connected to `endpoint` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::Transport`] if tonic cannot create or connect to the endpoint.
    pub async fn connect(endpoint: Endpoint) -> Result<Self, SchedulerError> {
        SchedulerServiceClient::connect(endpoint)
            .await
            .map(|client| Self { client })
            .map_err(to_transport_error)
    }
}

#[async_trait]
impl SchedulerClient for GrpcSchedulerClient {
    async fn next_task(
        &self,
        em_id: ExecutionManagerId,
        prev_assignment: Option<TaskAssignmentRecord>,
        wait_time_ms: u64,
    ) -> Result<SchedulerResponse, SchedulerError> {
        loop {
            let response = self
                .client
                .clone()
                .next_task(scheduler::NextTaskRequest {
                    execution_manager_id: em_id.get(),
                    prev_assignment: prev_assignment.map(Into::into),
                    wait_time_ms,
                })
                .await
                .map_err(to_transport_error)?
                .into_inner();

            let assignment: Option<SchedulerResponse> =
                response.try_into().map_err(to_protocol_error)?;
            if let Some(assignment) = assignment {
                return Ok(assignment);
            }
        }
    }

    async fn heartbeat(&self, em_id: ExecutionManagerId) -> Result<(), SchedulerError> {
        self.client
            .clone()
            .heartbeat(scheduler::HeartbeatRequest {
                execution_manager_id: em_id.get(),
            })
            .await
            .map_err(to_transport_error)?;
        Ok(())
    }

    async fn shutdown(
        &self,
        em_id: ExecutionManagerId,
        prev_assignments: Vec<TaskAssignmentRecord>,
    ) {
        if let Err(error) = self
            .client
            .clone()
            .shutdown(scheduler::ShutdownRequest {
                execution_manager_id: em_id.get(),
                prev_assignments: prev_assignments.into_iter().map(Into::into).collect(),
            })
            .await
        {
            tracing::warn!(
                em_id = ? em_id,
                error = ? error,
                "Failed to notify scheduler shutdown."
            );
        }
    }
}

/// Converts a displayable transport-layer error into [`SchedulerError::Transport`].
///
/// # Returns
///
/// A [`SchedulerError::Transport`] containing `error`'s display string.
fn to_transport_error(error: impl std::fmt::Display) -> SchedulerError {
    SchedulerError::Transport(error.to_string())
}

/// Converts a displayable protocol-layer error into [`SchedulerError::Protocol`].
///
/// # Returns
///
/// A [`SchedulerError::Protocol`] containing `error`'s display string.
fn to_protocol_error(error: impl std::fmt::Display) -> SchedulerError {
    SchedulerError::Protocol(error.to_string())
}
