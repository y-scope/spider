//! gRPC-related utilities.

pub mod client;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),

    #[error(transparent)]
    TonicTransport(tonic::transport::Error),
}
