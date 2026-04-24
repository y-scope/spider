//! In-memory ready queue for schedulable tasks.
//!
//! The queue is a set of three independent MPMC async channels — one for regular tasks, one for
//! commit tasks, and one for cleanup tasks. Each channel carries a [`ReadyQueueEntry`]
//! parameterized by the lane-specific task kind: [`TaskIndex`] for the regular lane,
//! [`CommitTaskMarker`] for the commit lane, and [`CleanupTaskMarker`] for the cleanup lane.
//!
//! [`ReadyQueueSender`] routes each send to the matching channel, and [`ReadyQueueReceiverHandle`]
//! exposes three `recv_*` methods that each read from one channel with a
//! `(max_items, wait_duration)` signature.

use std::time::Duration;

use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use spider_core::{
    task::TaskIndex,
    types::id::{JobId, ResourceGroupId},
};

use crate::cache::error::InternalError;

/// Marker type for commit-task ready entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommitTaskMarker;

/// Marker type for cleanup-task ready entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CleanupTaskMarker;

/// A ready queue entry.
///
/// # Type Parameters
///
/// * `TaskKind` - The per-lane task specifier:
///   * [`TaskIndex`] for regular tasks,
///   * [`CommitTaskMarker`] for commit tasks, and
///   * [`CleanupTaskMarker`] for cleanup tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReadyQueueEntry<TaskKind> {
    /// The owning resource group for the ready job.
    pub resource_group_id: ResourceGroupId,
    /// The job that became schedulable.
    pub job_id: JobId,
    /// The per-lane task specifier.
    pub task_kind: TaskKind,
}

/// Configuration of a ready queue.
#[derive(Debug, Clone, Copy)]
pub struct ReadyQueueConfig {
    /// The capacity of the task lane. Must be greater than zero.
    pub task_capacity: usize,
    /// The capacity of the commit lane. Must be greater than zero.
    pub commit_capacity: usize,
    /// The capacity of the cleanup lane. Must be greater than zero.
    pub cleanup_capacity: usize,
}

impl Default for ReadyQueueConfig {
    fn default() -> Self {
        Self {
            task_capacity: DEFAULT_TASK_CAPACITY,
            commit_capacity: DEFAULT_COMMIT_CAPACITY,
            cleanup_capacity: DEFAULT_CLEANUP_CAPACITY,
        }
    }
}

impl ReadyQueueConfig {
    /// Validates the config.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::ReadyQueueInvalidConfig`] if any of the configured capacity is 0.
    const fn validate(self) -> Result<(), InternalError> {
        const ERROR_MESSAGE: &str = "capacity must be greater than 0";
        if self.task_capacity == 0 || self.commit_capacity == 0 || self.cleanup_capacity == 0 {
            return Err(InternalError::ReadyQueueInvalidConfig(ERROR_MESSAGE));
        }
        Ok(())
    }
}

/// Connector for publishing task execution events to the ready queue.
///
/// This trait is invoked by the cache layer to enqueue tasks that are ready for scheduling.
#[async_trait]
pub trait ReadyQueueSender: Clone + Send + Sync {
    /// Enqueues a batch of tasks for the specified job which are ready to be scheduled. Each task
    /// index becomes one entry on the task lane.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The owning resource group ID.
    /// * `job_id` - The job ID.
    /// * `task_indices` - The indices of the tasks that are ready.
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] on failure.
    async fn send_task_ready(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError>;

    /// Enqueues a signal indicating that the commit task of the given job is ready to be
    /// scheduled.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The owning resource group ID.
    /// * `job_id` - The job ID.
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] on failure.
    async fn send_commit_ready(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), InternalError>;

    /// Enqueues a signal indicating that the cleanup task of the given job is ready to be
    /// scheduled.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The owning resource group ID.
    /// * `job_id` - The job ID.
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] on failure.
    async fn send_cleanup_ready(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), InternalError>;
}

/// A shareable ready-queue sender backed by three MPMC channels.
#[derive(Clone)]
pub struct ReadyQueueSenderHandle {
    task: Sender<ReadyQueueEntry<TaskIndex>>,
    commit: Sender<ReadyQueueEntry<CommitTaskMarker>>,
    cleanup: Sender<ReadyQueueEntry<CleanupTaskMarker>>,
}

#[async_trait]
impl ReadyQueueSender for ReadyQueueSenderHandle {
    async fn send_task_ready(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError> {
        for task_index in task_indices {
            let entry = ReadyQueueEntry {
                resource_group_id,
                job_id,
                task_kind: task_index,
            };
            self.task
                .send(entry)
                .await
                .map_err(|_| InternalError::ReadyQueueChannelClosed)?;
        }
        Ok(())
    }

    async fn send_commit_ready(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), InternalError> {
        let entry = ReadyQueueEntry {
            resource_group_id,
            job_id,
            task_kind: CommitTaskMarker,
        };
        self.commit
            .send(entry)
            .await
            .map_err(|_| InternalError::ReadyQueueChannelClosed)
    }

    async fn send_cleanup_ready(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), InternalError> {
        let entry = ReadyQueueEntry {
            resource_group_id,
            job_id,
            task_kind: CleanupTaskMarker,
        };
        self.cleanup
            .send(entry)
            .await
            .map_err(|_| InternalError::ReadyQueueChannelClosed)
    }
}

/// A cloneable ready-queue receiver that reads from all three lanes.
///
/// Multiple consumers can clone this handle and concurrently receive from any lane; each entry is
/// delivered to exactly one consumer.
#[derive(Clone)]
pub struct ReadyQueueReceiverHandle {
    task: Receiver<ReadyQueueEntry<TaskIndex>>,
    commit: Receiver<ReadyQueueEntry<CommitTaskMarker>>,
    cleanup: Receiver<ReadyQueueEntry<CleanupTaskMarker>>,
}

impl ReadyQueueReceiverHandle {
    /// Receives up to `max_items` regular task entries within a total time interval specified by
    /// `wait`.
    ///
    /// # Returns
    ///
    /// The ready queue entries received from the ready queue, up to `max_items`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`recv_batch`]'s return values on failure.
    pub async fn recv_tasks(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<Vec<ReadyQueueEntry<TaskIndex>>, InternalError> {
        recv_batch(&self.task, max_items, wait).await
    }

    /// Receives up to `max_items` commit task entries within a total time interval specified by
    /// `wait`.
    ///
    /// # Returns
    ///
    /// The ready queue entries received from the ready queue, up to `max_items`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`recv_batch`]'s return values on failure.
    pub async fn recv_commits(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<Vec<ReadyQueueEntry<CommitTaskMarker>>, InternalError> {
        recv_batch(&self.commit, max_items, wait).await
    }

    /// Receives up to `max_items` cleanup task entries within a total time interval specified by
    /// `wait`.
    ///
    /// # Returns
    ///
    /// The ready queue entries received from the ready queue, up to `max_items`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`recv_batch`]'s return values on failure.
    pub async fn recv_cleanups(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<Vec<ReadyQueueEntry<CleanupTaskMarker>>, InternalError> {
        recv_batch(&self.cleanup, max_items, wait).await
    }
}

/// Factory function.
///
/// Creates a ready queue and returns its paired sender and receiver handles.
///
/// # Returns
///
/// A pair on success, containing:
///
/// * The sender handle of the ready queue.
/// * The receiver handle of the ready queue.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`ReadyQueueConfig::validate`]'s return values on failure.
pub fn create_ready_queue(
    config: ReadyQueueConfig,
) -> Result<(ReadyQueueSenderHandle, ReadyQueueReceiverHandle), InternalError> {
    config.validate()?;

    let (task_tx, task_rx) = async_channel::bounded(config.task_capacity);
    let (commit_tx, commit_rx) = async_channel::bounded(config.commit_capacity);
    let (cleanup_tx, cleanup_rx) = async_channel::bounded(config.cleanup_capacity);

    let sender = ReadyQueueSenderHandle {
        task: task_tx,
        commit: commit_tx,
        cleanup: cleanup_tx,
    };
    let receiver = ReadyQueueReceiverHandle {
        task: task_rx,
        commit: commit_rx,
        cleanup: cleanup_rx,
    };
    Ok((sender, receiver))
}

/// Default capacity for the task lane.
const DEFAULT_TASK_CAPACITY: usize = 65_536;

/// Default capacity for the commit lane.
const DEFAULT_COMMIT_CAPACITY: usize = 1024;

/// Default capacity for the cleanup lane.
const DEFAULT_CLEANUP_CAPACITY: usize = 1024;

/// Receives up to `max_items` entries from a lane within a total time interval specified by `wait`.
///
/// The call collects entries until either `max_items` have been received or the `wait` budget
/// elapses — whichever happens first. The budget is shared across all receives, not reset per
/// entry.
///
/// # Type Parameters
///
/// * `TaskKind` - The task type specifier.
///
/// # Returns
///
/// The ready queue entries received from the lane, up to `max_items`.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`InternalError::ReadyQueueChannelClosed`] if the channel is closed. In a healthy storage
///   service, the channel should only be closed when the service is shutting down.
async fn recv_batch<TaskKind>(
    receiver: &Receiver<ReadyQueueEntry<TaskKind>>,
    max_items: usize,
    wait: Duration,
) -> Result<Vec<ReadyQueueEntry<TaskKind>>, InternalError> {
    if max_items == 0 {
        return Ok(Vec::new());
    }

    let deadline = tokio::time::Instant::now() + wait;
    let mut entries = Vec::with_capacity(max_items);
    while entries.len() < max_items {
        if let Ok(result) = tokio::time::timeout_at(deadline, receiver.recv()).await {
            match result {
                Ok(entry) => entries.push(entry),
                Err(_) => {
                    return Err(InternalError::ReadyQueueChannelClosed);
                }
            }
        } else {
            // Timeout
            break;
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_returns_early_when_max_items_reached() -> anyhow::Result<()> {
        let (sender, receiver) = create_ready_queue(ReadyQueueConfig {
            task_capacity: 1,
            commit_capacity: 1,
            cleanup_capacity: 1,
        })?;

        let resource_group_id = ResourceGroupId::default();
        let job_id = JobId::default();
        let task_indices: Vec<TaskIndex> = vec![1, 2, 3, 4];

        let send_handle = tokio::spawn({
            let task_indices = task_indices.clone();
            async move {
                sender
                    .send_task_ready(resource_group_id, job_id, task_indices)
                    .await
            }
        });

        let long_wait = Duration::from_secs(10);
        let start = std::time::Instant::now();
        let entries = receiver.recv_tasks(task_indices.len(), long_wait).await?;
        let elapsed = start.elapsed();

        assert_eq!(entries.len(), task_indices.len());
        assert!(
            elapsed < long_wait,
            "recv should return as soon as max_items is reached",
        );

        send_handle.await??;

        let received_indices: Vec<TaskIndex> = entries
            .iter()
            .map(|entry| {
                assert_eq!(entry.resource_group_id, resource_group_id);
                assert_eq!(entry.job_id, job_id);
                entry.task_kind
            })
            .collect();
        assert_eq!(received_indices, task_indices);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_returns_empty_when_wait_elapses() -> anyhow::Result<()> {
        let (_sender, receiver) = create_ready_queue(ReadyQueueConfig {
            task_capacity: 1,
            commit_capacity: 1,
            cleanup_capacity: 1,
        })?;

        let wait = Duration::from_millis(50);
        let start = std::time::Instant::now();
        let entries = receiver.recv_tasks(5, wait).await?;
        let elapsed = start.elapsed();

        assert!(entries.is_empty());
        assert!(
            elapsed >= wait,
            "recv should block for the full wait duration when no entries arrive",
        );
        Ok(())
    }
}
