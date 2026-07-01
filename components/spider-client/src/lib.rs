//! User-facing client library for the Spider storage gRPC services.
//!
//! [`SpiderClient`] wraps the storage server's job-orchestration and resource-group-management
//! gRPC services and exposes high-level typed methods that operate on `spider-core` types. It
//! hides proto-level concerns — task-graph and input serialization, zstd compression, the task
//! output wire format, and `tonic::Status` mapping — behind an ergonomic async API:
//!
//! * Job lifecycle: [`SpiderClient::submit_job`], [`SpiderClient::start_job`],
//!   [`SpiderClient::cancel_job`], [`SpiderClient::get_job_state`],
//!   [`SpiderClient::get_job_outputs`], [`SpiderClient::get_job_error`].
//! * Resource group operations: [`SpiderClient::add_resource_group`],
//!   [`SpiderClient::verify_resource_group`].
//!
//! Each service is also available as a standalone client — [`JobOrchestrationClient`] and
//! [`ResourceGroupManagementClient`] — for callers who need only one of the two.

pub mod client;
pub mod error;
pub(crate) mod grpc;

pub use client::SpiderClient;
