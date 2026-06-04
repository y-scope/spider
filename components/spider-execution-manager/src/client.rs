//! Network client traits used by the execution manager.
//!
//! Three traits cover the EM's outbound traffic:
//!
//! * [`scheduler::SchedulerClient`] — pulls task assignments from the scheduler.
//! * [`storage::StorageClient`] — registers task instances and reports their outcome.
//! * [`liveness::LivenessClient`] — registers the EM at boot and ticks the heartbeat thereafter.

pub mod liveness;
pub mod scheduler;
pub mod storage;

pub use liveness::{LivenessClient, LivenessResponseError, RegistrationResponse};
pub use scheduler::{SchedulerClient, SchedulerError, SchedulerResponse};
pub use storage::{GrpcStorageClient, StorageClient, StorageResponseError};
