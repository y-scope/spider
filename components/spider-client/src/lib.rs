//! User-facing client library for the Spider services.
//!
//! This library provides the Rust API for interacting with the Spider services to:
//!
//! * Managing Spider resource groups.
//! * Orchestrating Spider jobs.
//!
//! See [`SpiderClient`] for the main client API.

pub mod client;
pub mod error;
pub(crate) mod grpc;

pub use client::SpiderClient;
pub use client::SpiderClientBuilder;
pub use spider_utils::grpc::retry::RetryConfig;
