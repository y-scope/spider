//! gRPC-backed implementations of the execution manager's client traits.

pub mod liveness;
pub mod storage;

pub use liveness::GrpcLivenessClient;
pub use storage::GrpcStorageClient;
