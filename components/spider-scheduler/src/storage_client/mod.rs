//! The scheduler's view of the storage layer, abstracting inbound polling and placement-time reads.

use std::time::Duration;

use async_trait::async_trait;
use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::id::SchedulerId;
use spider_core::types::id::SessionId;

use crate::error::StorageClientError;
use crate::types::InboundEntry;

pub mod grpc;

pub use grpc::GrpcSchedulerStorageClient;

/// The scheduler's view of the storage layer.
///
/// Abstracts the storage-owned inbound queue and the read-only queries a scheduling algorithm
/// needs to make placement decisions. Modeled as a trait so the scheduler runtime can be driven by
/// a real storage client in production or a mock in tests.
#[async_trait]
pub trait SchedulerStorageClient: Send + Sync + Clone {
    /// Registers this scheduler with the storage service, advertising the endpoint execution
    /// managers should reach it on.
    ///
    /// # Parameters
    ///
    /// * `ip_address` - The IP address this scheduler is reachable on.
    /// * `port` - The port this scheduler is reachable on.
    ///
    /// # Returns
    ///
    /// The scheduler identifier assigned by the storage service on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageClientError::Server`] if the storage service returns an error.
    /// * [`StorageClientError::Transport`] if the storage transport fails or returns malformed
    ///   data.
    async fn register(
        &self,
        ip_address: std::net::IpAddr,
        port: u16,
    ) -> Result<SchedulerId, StorageClientError>;

    /// Polls the regular-task lane of the storage-owned inbound queue for ready tasks.
    ///
    /// # Parameters
    ///
    /// * `max_items` - The maximum number of entries to return from a single poll.
    /// * `wait` - The maximum duration to block waiting for ready entries on the storage side.
    ///
    /// # Returns
    ///
    /// A tuple on success, containing:
    ///
    /// * The storage session the poll was served under.
    /// * The ready regular tasks drained from the lane.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageClientError::Server`] if the storage service returns an error.
    /// * [`StorageClientError::Transport`] if the storage transport fails or returns malformed
    ///   data.
    async fn poll_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError>;

    /// Polls the commit-task lane of the storage-owned inbound queue for ready tasks.
    ///
    /// # Parameters
    ///
    /// * `max_items` - The maximum number of entries to return from a single poll.
    /// * `wait` - The maximum duration to block waiting for ready entries on the storage side.
    ///
    /// # Returns
    ///
    /// A tuple on success, containing:
    ///
    /// * The storage session the poll was served under.
    /// * The ready commit tasks drained from the lane.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageClientError::Server`] if the storage service returns an error.
    /// * [`StorageClientError::Transport`] if the storage transport fails or returns malformed
    ///   data.
    async fn poll_commit_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError>;

    /// Polls the cleanup-task lane of the storage-owned inbound queue for ready tasks.
    ///
    /// # Parameters
    ///
    /// * `max_items` - The maximum number of entries to return from a single poll.
    /// * `wait` - The maximum duration to block waiting for ready entries on the storage side.
    ///
    /// # Returns
    ///
    /// A tuple on success, containing:
    ///
    /// * The storage session the poll was served under.
    /// * The ready cleanup tasks drained from the lane.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageClientError::Server`] if the storage service returns an error.
    /// * [`StorageClientError::Transport`] if the storage transport fails or returns malformed
    ///   data.
    async fn poll_cleanup_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError>;

    /// Reads the current state of a job.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The identifier of the job to query.
    ///
    /// # Returns
    ///
    /// The job's current [`JobState`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageClientError::JobNotFound`] if no job with the given identifier exists.
    /// * [`StorageClientError::Server`] if the storage server returns an error.
    /// * [`StorageClientError::Transport`] if the storage server returns malformed data.
    async fn job_state(&self, job_id: JobId) -> Result<JobState, StorageClientError>;
}
