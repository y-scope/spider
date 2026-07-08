//! gRPC-backed [`LivenessClient`] implementation.

use std::net::IpAddr;
use std::num::NonZeroUsize;

use async_trait::async_trait;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::SessionId;
use spider_proto_rust::storage::ExecutionManagerLivenessServiceClient;
use spider_proto_rust::storage::{self};
use spider_utils::grpc::client::ConnectionPool;
use tonic::Code;
use tonic::Status;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use crate::client::liveness::LivenessClient;
use crate::client::liveness::LivenessResponseError;
use crate::client::liveness::RegistrationResponse;

/// gRPC-backed [`LivenessClient`] implementation.
#[derive(Debug, Clone)]
pub struct GrpcLivenessClient {
    connection_pool: ConnectionPool<ExecutionManagerLivenessServiceClient<Channel>>,
}

impl GrpcLivenessClient {
    /// Connects a pool of `pool_size` connections to the liveness gRPC endpoint.
    ///
    /// # Returns
    ///
    /// A new [`GrpcLivenessClient`] connected to `endpoint` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`LivenessResponseError::Transport`] if tonic cannot create or connect to the endpoint.
    pub async fn connect(
        endpoint: Endpoint,
        pool_size: NonZeroUsize,
    ) -> Result<Self, LivenessResponseError> {
        let connection_pool = ConnectionPool::connect(endpoint, pool_size, |channel| {
            ExecutionManagerLivenessServiceClient::new(channel)
        })
        .await
        .map_err(to_transport_error)?;

        Ok(Self { connection_pool })
    }
}

#[async_trait]
impl LivenessClient for GrpcLivenessClient {
    async fn register(&self, ip: IpAddr) -> Result<RegistrationResponse, LivenessResponseError> {
        let request = storage::RegisterExecutionManagerRequest {
            ip_address: ip.to_string(),
        };
        let response = self
            .connection_pool
            .get_client()
            .register_execution_manager(request)
            .await
            .map_err(|status| status_to_error(&status))?
            .into_inner();

        register_response_to_result(response)
    }

    async fn heartbeat(
        &self,
        em_id: ExecutionManagerId,
    ) -> Result<SessionId, LivenessResponseError> {
        let request = storage::ExecutionManagerIdRequest {
            execution_manager_id: em_id.get(),
        };
        let response = self
            .connection_pool
            .get_client()
            .update_execution_manager_heartbeat(request)
            .await
            .map_err(|status| status_to_error(&status))?
            .into_inner();

        Ok(heartbeat_response_to_result(response))
    }
}

/// Maps an execution-manager-liveness gRPC [`Status`] to a [`LivenessResponseError`].
///
/// # Returns
///
/// The [`LivenessResponseError`] for `status`'s code:
///
/// * [`LivenessResponseError::MarkedDead`] for `FAILED_PRECONDITION`.
/// * [`LivenessResponseError::IllegalId`] for `INVALID_ARGUMENT`.
/// * [`LivenessResponseError::Transport`] for any other code.
fn status_to_error(status: &Status) -> LivenessResponseError {
    match status.code() {
        Code::FailedPrecondition => LivenessResponseError::MarkedDead,
        Code::InvalidArgument => LivenessResponseError::IllegalId(status.message().to_owned()),
        _ => LivenessResponseError::Transport(status.message().to_owned()),
    }
}

/// # Returns
///
/// [`storage::RegisterExecutionManagerResponse`] converted into
/// [`Result<RegistrationResponse, LivenessResponseError>`].
fn register_response_to_result(
    response: storage::RegisterExecutionManagerResponse,
) -> Result<RegistrationResponse, LivenessResponseError> {
    let registration = response.registration.ok_or_else(|| {
        LivenessResponseError::Transport(
            "register execution manager response missing registration".to_owned(),
        )
    })?;
    Ok(RegistrationResponse {
        em_id: ExecutionManagerId::from(registration.execution_manager_id),
        session_id: registration.session_id,
    })
}

/// # Returns
///
/// The [`SessionId`] carried by `response`.
const fn heartbeat_response_to_result(
    response: storage::UpdateExecutionManagerHeartbeatResponse,
) -> SessionId {
    response.session_id
}

/// Converts a displayable transport-layer error into [`LivenessResponseError::Transport`].
///
/// # Returns
///
/// A [`LivenessResponseError::Transport`] containing `error`'s display string.
fn to_transport_error(error: impl std::fmt::Display) -> LivenessResponseError {
    LivenessResponseError::Transport(error.to_string())
}

#[cfg(test)]
mod tests {
    use spider_core::types::id::ExecutionManagerId;

    use super::*;
    use crate::client::LivenessResponseError;
    use crate::client::RegistrationResponse;

    #[test]
    fn register_response_to_result_returns_registration() {
        const SESSION_ID: SessionId = 7;
        const EM_ID: ExecutionManagerId = ExecutionManagerId::from(5);

        let response = storage::RegisterExecutionManagerResponse {
            registration: Some(storage::ExecutionManagerRegistration {
                execution_manager_id: EM_ID.get(),
                session_id: SESSION_ID,
            }),
        };

        let registration = register_response_to_result(response)
            .expect("registration response conversion should succeed");

        assert_eq!(
            registration,
            RegistrationResponse {
                em_id: EM_ID,
                session_id: SESSION_ID,
            }
        );
    }

    #[test]
    fn register_response_to_result_rejects_missing_registration() {
        let response = storage::RegisterExecutionManagerResponse { registration: None };

        assert!(matches!(
            register_response_to_result(response),
            Err(LivenessResponseError::Transport(_))
        ));
    }

    #[test]
    fn register_response_to_result_accepts_zero_session_id() {
        let response = storage::RegisterExecutionManagerResponse {
            registration: Some(storage::ExecutionManagerRegistration {
                execution_manager_id: 5,
                session_id: 0,
            }),
        };

        let registration = register_response_to_result(response)
            .expect("registration response conversion should succeed");

        assert_eq!(registration.session_id, 0);
    }

    #[test]
    fn heartbeat_response_to_result_returns_session_id() {
        const SESSION_ID: SessionId = 9;

        let response = storage::UpdateExecutionManagerHeartbeatResponse {
            session_id: SESSION_ID,
        };

        let session_id = heartbeat_response_to_result(response);

        assert_eq!(session_id, SESSION_ID);
    }

    #[test]
    fn heartbeat_response_to_result_accepts_zero_session_id() {
        let response = storage::UpdateExecutionManagerHeartbeatResponse { session_id: 0 };

        assert_eq!(heartbeat_response_to_result(response), 0);
    }

    #[test]
    fn status_maps_failed_precondition_to_marked_dead() {
        let status = tonic::Status::failed_precondition("already dead");

        assert!(matches!(
            status_to_error(&status),
            LivenessResponseError::MarkedDead
        ));
    }

    #[test]
    fn status_maps_invalid_argument_to_illegal_id() {
        const ERROR_MSG: &str = "bad em id";
        let status = tonic::Status::invalid_argument(ERROR_MSG);

        match status_to_error(&status) {
            LivenessResponseError::IllegalId(message) => assert_eq!(message, ERROR_MSG),
            error => panic!("unexpected liveness status mapping: {error:?}"),
        }
    }

    #[test]
    fn status_maps_other_codes_to_transport() {
        let status = tonic::Status::internal("boom");

        assert!(matches!(
            status_to_error(&status),
            LivenessResponseError::Transport(_)
        ));
    }
}
