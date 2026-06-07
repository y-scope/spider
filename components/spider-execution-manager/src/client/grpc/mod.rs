//! gRPC-backed implementations of the execution manager's client traits.

pub mod storage;

pub use storage::GrpcStorageClient;
