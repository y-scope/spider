//! gRPC-backed implementations of the execution manager's client traits.

pub mod liveness;
pub mod scheduler;
pub mod storage;

pub use liveness::GrpcLivenessClient;
pub use scheduler::{GrpcSchedulerClient, next_task_request, scheduler_response_to_result};
pub use storage::GrpcStorageClient;
