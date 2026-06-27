//! gRPC-related utilities.

pub mod client;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    TonicTransport(tonic::transport::Error),
}
