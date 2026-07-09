//! gRPC-backed [`SchedulerClient`] implementation.

use std::num::NonZeroUsize;

use async_trait::async_trait;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::scheduler::TaskAssignmentRecord;
use spider_proto_rust::scheduler::SchedulerServiceClient;
use spider_proto_rust::scheduler::{self};
use spider_utils::grpc::client::ConnectionPool;
use tonic::Code;
use tonic::Status;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use crate::client::SchedulerClient;
use crate::client::SchedulerError;
use crate::client::SchedulerResponse;

/// gRPC-backed [`SchedulerClient`] implementation.
#[derive(Debug, Clone)]
pub struct GrpcSchedulerClient {
    connection_pool: ConnectionPool<SchedulerServiceClient<Channel>>,
}

impl GrpcSchedulerClient {
    /// Connects a pool of `pool_size` connections to the scheduler gRPC endpoint.
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
    pub async fn connect(
        endpoint: Endpoint,
        pool_size: NonZeroUsize,
    ) -> Result<Self, SchedulerError> {
        let connection_pool = ConnectionPool::connect(endpoint, pool_size, |channel| {
            SchedulerServiceClient::new(channel)
        })
        .await
        .map_err(to_transport_error)?;

        Ok(Self { connection_pool })
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
                .connection_pool
                .get_client()
                .next_task(scheduler::NextTaskRequest {
                    execution_manager_id: em_id.get(),
                    prev_assignment: prev_assignment.map(Into::into),
                    wait_time_ms,
                })
                .await
                .map_err(|status| status_to_error(&status))?
                .into_inner();

            let assignment: Option<SchedulerResponse> =
                response.try_into().map_err(to_protocol_error)?;
            if let Some(assignment) = assignment {
                return Ok(assignment);
            }
        }
    }

    async fn heartbeat(&self, em_id: ExecutionManagerId) -> Result<(), SchedulerError> {
        self.connection_pool
            .get_client()
            .heartbeat(scheduler::HeartbeatRequest {
                execution_manager_id: em_id.get(),
            })
            .await
            .map_err(|status| status_to_error(&status))?;
        Ok(())
    }

    async fn shutdown(
        &self,
        em_id: ExecutionManagerId,
        prev_assignments: Vec<TaskAssignmentRecord>,
    ) {
        if let Err(error) = self
            .connection_pool
            .get_client()
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

/// Maps a scheduler gRPC [`Status`] to a [`SchedulerError`].
///
/// # Returns
///
/// The [`SchedulerError`] for `status`'s code:
///
/// * [`SchedulerError::Transport`] for `UNAVAILABLE` (a lost or unestablished connection).
/// * [`SchedulerError::Server`] for any other code (the scheduler returned an error response).
fn status_to_error(status: &Status) -> SchedulerError {
    match status.code() {
        Code::Unavailable => to_transport_error(status.message()),
        _ => SchedulerError::Server(status.message().to_owned()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_maps_unavailable_to_transport() {
        const MESSAGE: &str = "connection lost";
        match status_to_error(&Status::unavailable(MESSAGE)) {
            SchedulerError::Transport(message) => assert!(message.contains(MESSAGE)),
            error => panic!("unexpected error: {error:?}"),
        }
    }

    #[test]
    fn status_maps_internal_to_server() {
        const MESSAGE: &str = "boom";
        match status_to_error(&Status::internal(MESSAGE)) {
            SchedulerError::Server(message) => assert!(message.contains(MESSAGE)),
            error => panic!("unexpected error: {error:?}"),
        }
    }

    #[test]
    fn status_maps_not_found_to_server() {
        match status_to_error(&Status::not_found("execution manager not found")) {
            SchedulerError::Server(_) => {}
            error => panic!("unexpected error: {error:?}"),
        }
    }
}
