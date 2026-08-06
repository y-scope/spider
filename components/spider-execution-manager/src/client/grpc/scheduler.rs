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
        mut prev_assignment: Option<TaskAssignmentRecord>,
        wait_time_ms: u64,
    ) -> Result<SchedulerResponse, SchedulerError> {
        // The scheduler completes `prev_assignment` by removing it from its registry, so it must be
        // sent at most once; every later poll iteration sends `None`.
        loop {
            let response = self
                .connection_pool
                .get_client()
                .next_task(scheduler::NextTaskRequest {
                    execution_manager_id: em_id.get(),
                    prev_assignment: prev_assignment.take().map(Into::into),
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
    use std::net::TcpListener as StdTcpListener;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use spider_core::types::id::SchedulerId;
    use spider_core::types::id::TaskAssignmentId;
    use spider_proto_rust::common;
    use spider_proto_rust::common::TaskId as ProtoTaskId;
    use spider_proto_rust::common::task_id::Kind as ProtoTaskIdKind;
    use spider_proto_rust::scheduler::SchedulerAssignment;
    use spider_proto_rust::scheduler::SchedulerService;
    use spider_proto_rust::scheduler::SchedulerServiceServer;
    use spider_proto_rust::scheduler::next_task_response;
    use tonic::Request;
    use tonic::Response;
    use tonic::transport::Server;

    use super::*;

    /// The number of `NoTask` replies the fake scheduler sends before handing out an assignment.
    const NUM_NO_TASK_REPLIES: usize = 3;

    /// The maximum time spent waiting for the test server to accept connections.
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

    /// The delay between connection attempts while the test server is still binding.
    const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(50);

    /// Connects to `endpoint`, retrying for up to [`CONNECT_TIMEOUT`] while the test server binds
    /// its listener.
    ///
    /// # Returns
    ///
    /// A connected [`GrpcSchedulerClient`] on success.
    ///
    /// # Errors
    ///
    /// Forwards [`GrpcSchedulerClient::connect`]'s failure once [`CONNECT_TIMEOUT`] elapses.
    async fn connect_with_retries(endpoint: &Endpoint) -> anyhow::Result<GrpcSchedulerClient> {
        let pool_size = NonZeroUsize::new(1).expect("1 is non-zero");
        let deadline = tokio::time::Instant::now() + CONNECT_TIMEOUT;
        loop {
            match GrpcSchedulerClient::connect(endpoint.clone(), pool_size).await {
                Ok(client) => return Ok(client),
                Err(error) if tokio::time::Instant::now() >= deadline => return Err(error.into()),
                Err(_) => tokio::time::sleep(CONNECT_RETRY_DELAY).await,
            }
        }
    }

    /// A fake scheduler that records every request's `prev_assignment`, replying `NoTask`
    /// [`NUM_NO_TASK_REPLIES`] times before handing out an assignment.
    struct FakeScheduler {
        observed: Arc<Mutex<Vec<Option<scheduler::TaskAssignmentRecord>>>>,
    }

    #[async_trait]
    impl SchedulerService for FakeScheduler {
        async fn next_task(
            &self,
            request: Request<scheduler::NextTaskRequest>,
        ) -> Result<Response<scheduler::NextTaskResponse>, Status> {
            let num_requests = {
                let mut observed = self.observed.lock().expect("lock shouldn't be poisoned");
                observed.push(request.into_inner().prev_assignment);
                observed.len()
            };
            let result = if num_requests > NUM_NO_TASK_REPLIES {
                next_task_response::Result::Assignment(SchedulerAssignment {
                    id: 1,
                    resource_group_id: 2,
                    job_id: 3,
                    task_id: Some(ProtoTaskId {
                        kind: Some(ProtoTaskIdKind::Index(0)),
                    }),
                    scheduler_id: 4,
                    session_id: 5,
                })
            } else {
                next_task_response::Result::NoTask(common::Void {})
            };
            Ok(Response::new(scheduler::NextTaskResponse {
                result: Some(result),
            }))
        }

        async fn heartbeat(
            &self,
            _request: Request<scheduler::HeartbeatRequest>,
        ) -> Result<Response<common::Void>, Status> {
            Ok(Response::new(common::Void {}))
        }

        async fn shutdown(
            &self,
            _request: Request<scheduler::ShutdownRequest>,
        ) -> Result<Response<common::Void>, Status> {
            Ok(Response::new(common::Void {}))
        }
    }

    /// Tests that `prev_assignment` is sent exactly once even when the client long-polls several
    /// times before a task becomes available. The scheduler completes it by removing it from its
    /// registry, so re-sending it makes the scheduler fail to complete an already-dropped
    /// assignment.
    #[tokio::test]
    async fn prev_assignment_is_sent_once_across_long_poll_iterations() -> anyhow::Result<()> {
        let observed = Arc::new(Mutex::new(Vec::new()));
        // Reserve an ephemeral port, then release it so the server can bind it.
        let address = StdTcpListener::bind("127.0.0.1:0")?.local_addr()?;
        let service = SchedulerServiceServer::new(FakeScheduler {
            observed: Arc::clone(&observed),
        });
        let server = tokio::spawn(Server::builder().add_service(service).serve(address));

        let endpoint = Endpoint::from_shared(format!("http://{address}"))?;
        let client = connect_with_retries(&endpoint).await?;
        let prev_assignment =
            TaskAssignmentRecord::new(TaskAssignmentId::from(7), SchedulerId::from(4));
        client
            .next_task(ExecutionManagerId::from(2), Some(prev_assignment), 0)
            .await?;
        server.abort();

        let observed = observed.lock().expect("lock shouldn't be poisoned").clone();
        assert_eq!(observed.len(), NUM_NO_TASK_REPLIES + 1);
        assert_eq!(
            observed[0],
            Some(scheduler::TaskAssignmentRecord { id: 7, from: 4 })
        );
        assert!(
            observed[1..].iter().all(Option::is_none),
            "later poll iterations resent the assignment: {observed:?}"
        );
        Ok(())
    }

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
