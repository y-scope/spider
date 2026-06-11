//! gRPC-backed [`LivenessClient`] implementation.

use std::net::IpAddr;

use async_trait::async_trait;
use spider_core::types::id::{ExecutionManagerId, SessionId};
use spider_proto_rust::storage::{
    self,
    execution_manager_liveness_error,
    execution_manager_liveness_service_client::ExecutionManagerLivenessServiceClient,
    register_execution_manager_response,
    update_execution_manager_heartbeat_response,
};
use tonic::transport::{Channel, Endpoint};

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
            .map(|inner| Self { client: inner })
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
            .map_err(to_transport_error)?
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
            .map_err(to_transport_error)?
            .into_inner();

        heartbeat_response_to_result(response)
    }
}

/// # Returns
///
/// [`storage::RegisterExecutionManagerResponse`] converted into
/// [`Result<RegistrationResponse, LivenessResponseError>`].
fn register_response_to_result(
    response: storage::RegisterExecutionManagerResponse,
) -> Result<RegistrationResponse, LivenessResponseError> {
    match response.result {
        Some(register_execution_manager_response::Result::Registration(registration)) => {
            Ok(RegistrationResponse {
                em_id: ExecutionManagerId::from(registration.execution_manager_id),
                session_id: registration.session_id,
            })
        }
        Some(register_execution_manager_response::Result::Error(error)) => {
            Err(storage_error_to_liveness_error(error))
        }
        None => Err(LivenessResponseError::Transport(
            "register execution manager response missing result".to_owned(),
        )),
    }
}

/// # Returns
///
/// [`storage::UpdateExecutionManagerHeartbeatResponse`] converted into
/// [`Result<SessionId, LivenessResponseError>`].
fn heartbeat_response_to_result(
    response: storage::UpdateExecutionManagerHeartbeatResponse,
) -> Result<SessionId, LivenessResponseError> {
    match response.result {
        Some(update_execution_manager_heartbeat_response::Result::SessionId(session_id)) => {
            Ok(session_id)
        }
        Some(update_execution_manager_heartbeat_response::Result::Error(error)) => {
            Err(storage_error_to_liveness_error(error))
        }
        None => Err(LivenessResponseError::Transport(
            "update execution manager heartbeat response missing result".to_owned(),
        )),
    }
}

/// Converts a protobuf storage error into a liveness client error.
///
/// # Returns
///
/// The corresponding [`LivenessResponseError`].
fn storage_error_to_liveness_error(
    error: storage::ExecutionManagerLivenessError,
) -> LivenessResponseError {
    match execution_manager_liveness_error::ErrCode::try_from(error.err_code) {
        Ok(execution_manager_liveness_error::ErrCode::MarkedDead) => {
            LivenessResponseError::MarkedDead
        }
        Ok(execution_manager_liveness_error::ErrCode::InvalidInput) => {
            LivenessResponseError::IllegalId(error.message)
        }
        Ok(
            execution_manager_liveness_error::ErrCode::Server
            | execution_manager_liveness_error::ErrCode::Unspecified,
        ) => LivenessResponseError::Transport(error.message),
        Err(error) => LivenessResponseError::Transport(format!(
            "unknown execution manager liveness error kind: {error}"
        )),
    }
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
        let response = storage::RegisterExecutionManagerResponse {
            result: Some(register_execution_manager_response::Result::Registration(
                storage::ExecutionManagerRegistration {
                    execution_manager_id: 5,
                    session_id: 7,
                },
            )),
        };

        let registration = register_response_to_result(response)
            .expect("registration response conversion should succeed");

        assert_eq!(
            registration,
            RegistrationResponse {
                em_id: ExecutionManagerId::from(5),
                session_id: 7,
            }
        );
    }

    #[test]
    fn heartbeat_response_to_result_returns_session_id() {
        let response = storage::UpdateExecutionManagerHeartbeatResponse {
            result: Some(update_execution_manager_heartbeat_response::Result::SessionId(9)),
        };

        let session_id = heartbeat_response_to_result(response)
            .expect("heartbeat response conversion should succeed");

        assert_eq!(session_id, 9);
    }

    #[test]
    fn liveness_storage_error_maps_invalid_input_to_illegal_id() {
        let error = storage::ExecutionManagerLivenessError {
            err_code: execution_manager_liveness_error::ErrCode::InvalidInput.into(),
            message: "bad em id".to_owned(),
        };

        match storage_error_to_liveness_error(error) {
            LivenessResponseError::IllegalId(message) => {
                assert_eq!(message, "bad em id");
            }
            error => panic!("unexpected liveness response error: {error:?}"),
        }
    }
}
