//! gRPC-backed [`LivenessClient`] implementation.

use std::{net::IpAddr, num::NonZeroUsize};

use async_trait::async_trait;
use spider_core::types::id::{ExecutionManagerId, SessionId};
use spider_proto_rust::storage::{
    self,
    execution_manager_liveness_error,
    execution_manager_liveness_service_client::ExecutionManagerLivenessServiceClient,
    register_execution_manager_response,
    update_execution_manager_heartbeat_response,
};
use spider_utils::grpc::client::ConnectionPool;
use tonic::transport::{Channel, Endpoint};

use crate::client::liveness::{LivenessClient, LivenessResponseError, RegistrationResponse};

/// gRPC-backed [`LivenessClient`] implementation.
#[derive(Debug, Clone)]
pub struct GrpcLivenessClient {
    connection_pool: ConnectionPool<ExecutionManagerLivenessServiceClient<Channel>>,
}

impl GrpcLivenessClient {
    /// Connects a pool of `pool_size` connections to the storage gRPC endpoint.
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
            Ok(ExecutionManagerLivenessServiceClient::new(channel))
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
            .connection_pool
            .get_client()
            .update_execution_manager_heartbeat(request)
            .await
            .map_err(to_transport_error)?
            .into_inner();

        heartbeat_response_to_result(response)
    }
}

impl From<storage::ExecutionManagerLivenessError> for LivenessResponseError {
    fn from(error: storage::ExecutionManagerLivenessError) -> Self {
        match execution_manager_liveness_error::ErrCode::try_from(error.err_code) {
            Ok(execution_manager_liveness_error::ErrCode::MarkedDead) => Self::MarkedDead,
            Ok(execution_manager_liveness_error::ErrCode::InvalidInput) => {
                Self::IllegalId(error.message)
            }
            Ok(
                execution_manager_liveness_error::ErrCode::Server
                | execution_manager_liveness_error::ErrCode::Unspecified,
            ) => Self::Transport(error.message),
            Err(error) => Self::Transport(format!(
                "unknown execution manager liveness error kind: {error}"
            )),
        }
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
        Some(register_execution_manager_response::Result::Error(error)) => Err(error.into()),
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
            Err(error.into())
        }
        None => Err(LivenessResponseError::Transport(
            "update execution manager heartbeat response missing result".to_owned(),
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
        const SESSION_ID: SessionId = 7;
        const EM_ID: ExecutionManagerId = ExecutionManagerId::from(5);

        let response = storage::RegisterExecutionManagerResponse {
            result: Some(register_execution_manager_response::Result::Registration(
                storage::ExecutionManagerRegistration {
                    execution_manager_id: EM_ID.get(),
                    session_id: SESSION_ID,
                },
            )),
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
    fn heartbeat_response_to_result_returns_session_id() {
        const SESSION_ID: SessionId = 9;

        let response = storage::UpdateExecutionManagerHeartbeatResponse {
            result: Some(
                update_execution_manager_heartbeat_response::Result::SessionId(SESSION_ID),
            ),
        };

        let session_id = heartbeat_response_to_result(response)
            .expect("heartbeat response conversion should succeed");

        assert_eq!(session_id, SESSION_ID);
    }

    #[test]
    fn liveness_storage_error_maps_invalid_input_to_illegal_id() {
        const ERROR_MSG: &str = "bad em id";

        let error = storage::ExecutionManagerLivenessError {
            err_code: execution_manager_liveness_error::ErrCode::InvalidInput.into(),
            message: ERROR_MSG.to_owned(),
        };

        match LivenessResponseError::from(error) {
            LivenessResponseError::IllegalId(message) => {
                assert_eq!(message, ERROR_MSG);
            }
            error => panic!("unexpected liveness response error: {error:?}"),
        }
    }
}
