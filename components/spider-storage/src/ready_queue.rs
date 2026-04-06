use async_trait::async_trait;
use spider_core::{task::TaskIndex, types::id::JobId};

use crate::cache::error::InternalError;

/// Connector for publishing task execution events to the ready queue.
///
/// This trait is invoked by the cache layer to enqueue tasks that are ready for scheduling.
#[async_trait]
pub trait ReadyQueueSender: Clone + Send + Sync {
    /// Enqueues a batch of tasks for the specified job which are ready to be scheduled.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The job ID.
    /// * `task_indices` - The indices of the tasks that are ready.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the tasks fail to be sent to the ready queue.
    async fn send_task_ready(
        &self,
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError>;

    /// Enqueues a signal indicating that the commit task of the given job is ready to be scheduled.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The job ID.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the message fails to be sent to the ready queue.
    async fn send_commit_ready(&self, job_id: JobId) -> Result<(), InternalError>;

    /// Enqueues a signal indicating that the cleanup task of the given job is ready to be
    /// scheduled.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The job ID.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the message fails to be sent to the ready queue.
    async fn send_cleanup_ready(&self, job_id: JobId) -> Result<(), InternalError>;
}
