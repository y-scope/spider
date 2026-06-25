//! gRPC-backed [`LivenessClient`] implementation.

use std::net::IpAddr;

use async_trait::async_trait;
use spider_core::types::id::{ExecutionManagerId, SessionId};
use spider_proto_rust::storage::{
    self,
    execution_manager_liveness_service_client::ExecutionManagerLivenessServiceClient,
};
use tonic::{
    Code,
    transport::{Channel, Endpoint},
};

use crate::client::liveness::{LivenessClient, LivenessResponseError, RegistrationResponse};

/// gRPC-backed [`LivenessClient`] implementation.
#[derive(Debug, Clone)]
pub struct GrpcLivenessClient {
    client: ExecutionManagerLivenessServiceClient<Channel>,
}

impl GrpcLivenessClient {
    /// Connects to the storage gRPC endpoint.
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
    pub async fn connect(endpoint: Endpoint) -> Result<Self, LivenessResponseError> {
        ExecutionManagerLivenessServiceClient::connect(endpoint)
            .await
            .map(|client| Self { client })
            .map_err(to_transport_error)
    }
}

#[async_trait]
impl LivenessClient for GrpcLivenessClient {
    async fn register(&self, ip: IpAddr) -> Result<RegistrationResponse, LivenessResponseError> {
        let request = storage::RegisterExecutionManagerRequest {
            ip_address: ip.to_string(),
        };
        let response = self
            .client
            .clone()
            .register_execution_manager(request)
            .await
            .map_err(|status| map_liveness_status(&status))?
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
            .client
            .clone()
            .update_execution_manager_heartbeat(request)
            .await
            .map_err(|status| map_liveness_status(&status))?
            .into_inner();

        heartbeat_response_to_result(response)
    }
}

/// Maps a [`tonic::Status`] returned by an execution-manager-liveness RPC into a
/// [`LivenessResponseError`].
///
/// # Returns
///
/// * [`LivenessResponseError::MarkedDead`] when storage has already reaped the execution manager,
///   signalled by `FAILED_PRECONDITION`.
/// * [`LivenessResponseError::IllegalId`] when storage rejects the execution manager id, signalled
///   by `INVALID_ARGUMENT`.
/// * [`LivenessResponseError::Transport`] for any other failure.
fn map_liveness_status(status: &tonic::Status) -> LivenessResponseError {
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
///
/// # Errors
///
/// Returns an error if:
///
/// * [`LivenessResponseError::Transport`] if the response omits the registration payload or carries
///   a zero session ID. Storage assigns session IDs from a database auto-increment column, so a
///   live session is always nonzero; a zero value indicates a malformed response.
fn register_response_to_result(
    response: storage::RegisterExecutionManagerResponse,
) -> Result<RegistrationResponse, LivenessResponseError> {
    let registration = response.registration.ok_or_else(|| {
        LivenessResponseError::Transport(
            "register execution manager response missing registration".to_owned(),
        )
    })?;
    if registration.session_id == 0 {
        return Err(LivenessResponseError::Transport(
            "register execution manager response carried a zero session id".to_owned(),
        ));
    }
    Ok(RegistrationResponse {
        em_id: ExecutionManagerId::from(registration.execution_manager_id),
        session_id: registration.session_id,
    })
}

/// Converts an [`storage::UpdateExecutionManagerHeartbeatResponse`] into the storage session ID
/// it carries.
///
/// # Returns
///
/// The session ID carried by `response` on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`LivenessResponseError::Transport`] if `response` carries a zero session ID. Storage assigns
///   session IDs from a database auto-increment column, so a live session is always nonzero; a zero
///   value indicates a malformed response.
fn heartbeat_response_to_result(
    response: storage::UpdateExecutionManagerHeartbeatResponse,
) -> Result<SessionId, LivenessResponseError> {
    let session_id = response.session_id;
    if session_id == 0 {
        return Err(LivenessResponseError::Transport(
            "update execution manager heartbeat response carried a zero session id".to_owned(),
        ));
    }
    Ok(session_id)
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
    use crate::client::{LivenessResponseError, RegistrationResponse};

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
    fn register_response_to_result_rejects_zero_session_id() {
        let response = storage::RegisterExecutionManagerResponse {
            registration: Some(storage::ExecutionManagerRegistration {
                execution_manager_id: 5,
                session_id: 0,
            }),
        };

        assert!(matches!(
            register_response_to_result(response),
            Err(LivenessResponseError::Transport(_))
        ));
    }

    #[test]
    fn heartbeat_response_to_result_returns_session_id() {
        const SESSION_ID: SessionId = 9;

        let response = storage::UpdateExecutionManagerHeartbeatResponse {
            session_id: SESSION_ID,
        };

        let session_id = heartbeat_response_to_result(response)
            .expect("heartbeat response with a nonzero session id should convert");

        assert_eq!(session_id, SESSION_ID);
    }

    #[test]
    fn heartbeat_response_to_result_rejects_zero_session_id() {
        let response = storage::UpdateExecutionManagerHeartbeatResponse { session_id: 0 };

        assert!(matches!(
            heartbeat_response_to_result(response),
            Err(LivenessResponseError::Transport(_))
        ));
    }

    #[test]
    fn map_liveness_status_maps_failed_precondition_to_marked_dead() {
        let status = tonic::Status::failed_precondition("already dead");

        assert!(matches!(
            map_liveness_status(&status),
            LivenessResponseError::MarkedDead
        ));
    }

    #[test]
    fn map_liveness_status_maps_invalid_argument_to_illegal_id() {
        const ERROR_MSG: &str = "bad em id";
        let status = tonic::Status::invalid_argument(ERROR_MSG);

        match map_liveness_status(&status) {
            LivenessResponseError::IllegalId(message) => assert_eq!(message, ERROR_MSG),
            error => panic!("unexpected liveness status mapping: {error:?}"),
        }
    }

    #[test]
    fn map_liveness_status_maps_other_codes_to_transport() {
        let status = tonic::Status::internal("boom");

        assert!(matches!(
            map_liveness_status(&status),
            LivenessResponseError::Transport(_)
        ));
    }
}
