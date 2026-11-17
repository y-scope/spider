//! Network client traits used by the execution manager.
//!
//! Three traits cover the EM's outbound traffic:
//!
//! * [`scheduler::SchedulerClient`] — pulls task assignments from the scheduler.
//! * [`storage::StorageClient`] — registers task instances and reports their outcome.
//! * [`liveness::LivenessClient`] — registers the EM at boot and ticks the heartbeat thereafter.

pub mod grpc;
pub mod liveness;
pub mod scheduler;
pub mod storage;

pub use grpc::GrpcLivenessClient;
pub use grpc::GrpcStorageClient;
pub use liveness::LivenessClient;
pub use liveness::LivenessResponseError;
pub use liveness::RegistrationResponse;
pub use scheduler::SchedulerClient;
pub use scheduler::SchedulerError;
pub use spider_core::types::scheduler::SchedulerResponse;
pub use storage::StorageClient;
pub use storage::StorageResponseError;
