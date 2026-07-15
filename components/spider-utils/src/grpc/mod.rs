//! gRPC-related utilities.

pub mod client;
pub mod retry;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    TonicTransport(tonic::transport::Error),
}
