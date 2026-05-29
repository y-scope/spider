use std::time::Duration;

use async_trait::async_trait;
use spider_core::{job::JobState, types::id::JobId};

use crate::{error::StorageClientError, types::InboundEntry};

/// The scheduler's view of the storage layer.
///
/// Abstracts the storage-owned inbound queue and the read-only queries a scheduling algorithm
/// needs to make placement decisions. Modeled as a trait so the scheduler runtime can be driven by
/// a real storage client in production or a mock in tests.
#[async_trait]
pub trait SchedulerStorageClient: Send + Sync {
    /// Polls the storage-owned inbound (ready) queue for newly-ready tasks.
    ///
    /// Drains up to `max_items` ready entries across all storage lanes (regular, commit, and
    /// cleanup tasks), blocking for at most `wait`. Returns an empty vector if no entry becomes
    /// ready within `wait`.
    ///
    /// # Parameters
    ///
    /// * `max_items` - The maximum number of entries to return from a single poll.
    /// * `wait` - The maximum duration to block waiting for ready entries.
    ///
    /// # Returns
    ///
    /// The ready entries drained from the inbound queue on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageClientError::InboundClosed`] if the inbound queue is closed and can no longer
    ///   yield entries.
    async fn poll_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<Vec<InboundEntry>, StorageClientError>;

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
    async fn job_state(&self, job_id: JobId) -> Result<JobState, StorageClientError>;
}
