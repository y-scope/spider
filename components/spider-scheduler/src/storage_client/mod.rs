//! The scheduler's view of the storage layer, abstracting inbound polling and placement-time reads.

use std::time::Duration;

use async_trait::async_trait;
use spider_core::{
    job::JobState,
    types::id::{JobId, SessionId},
};

use crate::{error::StorageClientError, types::InboundEntry};

pub mod grpc;

pub use grpc::GrpcSchedulerStorageClient;

/// The scheduler's view of the storage layer.
///
/// Abstracts the storage-owned inbound queue and the read-only queries a scheduling algorithm
/// needs to make placement decisions. Modeled as a trait so the scheduler runtime can be driven by
/// a real storage client in production or a mock in tests.
#[async_trait]
pub trait SchedulerStorageClient: Send + Sync + Clone {
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

    /// Asks storage to re-enqueue the ready tasks of every cached job back onto the inbound queue.
    ///
    /// Used after a storage session change (e.g. a scheduler reconnect) to recover tasks that were
    /// drained but not yet placed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageClientError::Server`] if the inbound queue is closed and can no longer yield
    ///   entries, or the storage server returns another error.
    /// * [`StorageClientError::Transport`] if the storage transport fails or returns malformed
    ///   data.
    async fn resend_ready_tasks(&self) -> Result<(), StorageClientError>;
}
