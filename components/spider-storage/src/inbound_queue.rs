//! In-memory inbound queue for schedulable tasks.
//!
//! The queue is a set of three independent, unbounded MPMC async channels — one for regular tasks,
//! one for commit tasks, and one for cleanup tasks. Each channel carries an [`InboundQueueEntry`]
//! parameterized by the lane-specific task kind: [`TaskIndex`] for the regular lane,
//! [`CommitTaskMarker`] for the commit lane, and [`CleanupTaskMarker`] for the cleanup lane.
//!
//! [`InboundQueueSender`] routes each send to the matching channel, and
//! [`InboundQueueReceiverHandle`] exposes three `recv_*` methods that each read from one channel
//! with a `(max_items, wait_duration)` signature.

use std::time::Duration;

use async_channel::Receiver;
use async_channel::Sender;
use async_trait::async_trait;
use spider_core::task::TaskIndex;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;

use crate::cache::error::InternalError;

/// Marker type for commit-task inbound-queue entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommitTaskMarker;

/// Marker type for cleanup-task inbound-queue entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CleanupTaskMarker;

/// An inbound queue entry.
///
/// # Type Parameters
///
/// * `TaskKind` - The per-lane task specifier:
///   * [`TaskIndex`] for regular tasks,
///   * [`CommitTaskMarker`] for commit tasks, and
///   * [`CleanupTaskMarker`] for cleanup tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InboundQueueEntry<TaskKind> {
    /// The owning resource group for the ready job.
    pub resource_group_id: ResourceGroupId,
    /// The job that became schedulable.
    pub job_id: JobId,
    /// The per-lane task specifier.
    pub task_kind: TaskKind,
}

/// Connector for publishing task execution events to the inbound queue.
///
/// This trait is invoked by the cache layer to enqueue tasks that are ready for scheduling.
#[async_trait]
pub trait InboundQueueSender: Clone + Send + Sync {
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

/// A shareable inbound-queue sender backed by three MPMC channels.
#[derive(Clone)]
pub struct InboundQueueSenderHandle {
    task: Sender<InboundQueueEntry<TaskIndex>>,
    commit: Sender<InboundQueueEntry<CommitTaskMarker>>,
    cleanup: Sender<InboundQueueEntry<CleanupTaskMarker>>,
}

#[async_trait]
impl InboundQueueSender for InboundQueueSenderHandle {
    async fn send_task_ready(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError> {
        for task_index in task_indices {
            let entry = InboundQueueEntry {
                resource_group_id,
                job_id,
                task_kind: task_index,
            };
            self.task
                .try_send(entry)
                .map_err(|_| InternalError::InboundQueueChannelClosed)?;
        }
        Ok(())
    }

    async fn send_commit_ready(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), InternalError> {
        let entry = InboundQueueEntry {
            resource_group_id,
            job_id,
            task_kind: CommitTaskMarker,
        };
        self.commit
            .try_send(entry)
            .map_err(|_| InternalError::InboundQueueChannelClosed)
    }

    async fn send_cleanup_ready(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), InternalError> {
        let entry = InboundQueueEntry {
            resource_group_id,
            job_id,
            task_kind: CleanupTaskMarker,
        };
        self.cleanup
            .try_send(entry)
            .map_err(|_| InternalError::InboundQueueChannelClosed)
    }
}

/// A cloneable inbound-queue receiver that reads from all three lanes.
///
/// Multiple consumers can clone this handle and concurrently receive from any lane; each entry is
/// delivered to exactly one consumer.
#[derive(Clone)]
pub struct InboundQueueReceiverHandle {
    task: Receiver<InboundQueueEntry<TaskIndex>>,
    commit: Receiver<InboundQueueEntry<CommitTaskMarker>>,
    cleanup: Receiver<InboundQueueEntry<CleanupTaskMarker>>,
}

impl InboundQueueReceiverHandle {
    /// Receives up to `max_items` regular task entries within a total time interval specified by
    /// `wait`.
    ///
    /// # Returns
    ///
    /// The inbound queue entries received from the inbound queue, up to `max_items`.
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
    ) -> Result<Vec<InboundQueueEntry<TaskIndex>>, InternalError> {
        recv_batch(&self.task, max_items, wait).await
    }

    /// Receives up to `max_items` commit task entries within a total time interval specified by
    /// `wait`.
    ///
    /// # Returns
    ///
    /// The inbound queue entries received from the inbound queue, up to `max_items`.
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
    ) -> Result<Vec<InboundQueueEntry<CommitTaskMarker>>, InternalError> {
        recv_batch(&self.commit, max_items, wait).await
    }

    /// Receives up to `max_items` cleanup task entries within a total time interval specified by
    /// `wait`.
    ///
    /// # Returns
    ///
    /// The inbound queue entries received from the inbound queue, up to `max_items`.
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
    ) -> Result<Vec<InboundQueueEntry<CleanupTaskMarker>>, InternalError> {
        recv_batch(&self.cleanup, max_items, wait).await
    }
}

/// Factory function.
///
/// Creates an inbound queue and returns its paired sender and receiver handles.
///
/// # Returns
///
/// A pair, containing:
///
/// * The sender handle of the inbound queue.
/// * The receiver handle of the inbound queue.
#[must_use]
pub fn create_inbound_queue() -> (InboundQueueSenderHandle, InboundQueueReceiverHandle) {
    let (task_tx, task_rx) = async_channel::unbounded();
    let (commit_tx, commit_rx) = async_channel::unbounded();
    let (cleanup_tx, cleanup_rx) = async_channel::unbounded();

    let sender = InboundQueueSenderHandle {
        task: task_tx,
        commit: commit_tx,
        cleanup: cleanup_tx,
    };
    let receiver = InboundQueueReceiverHandle {
        task: task_rx,
        commit: commit_rx,
        cleanup: cleanup_rx,
    };
    (sender, receiver)
}

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
/// The inbound queue entries received from the lane, up to `max_items`.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`InternalError::InboundQueueChannelClosed`] if the channel is closed. In a healthy storage
///   service, the channel should only be closed when the service is shutting down.
async fn recv_batch<TaskKind>(
    receiver: &Receiver<InboundQueueEntry<TaskKind>>,
    max_items: usize,
    wait: Duration,
) -> Result<Vec<InboundQueueEntry<TaskKind>>, InternalError> {
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
                    return Err(InternalError::InboundQueueChannelClosed);
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
        let (sender, receiver) = create_inbound_queue();

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
        let (_sender, receiver) = create_inbound_queue();

        let wait = Duration::from_millis(50);
        let start = std::time::Instant::now();
        let entries = receiver.recv_tasks(5, wait).await?;
        let elapsed = start.elapsed();

        assert_eq!(entries, [] as [InboundQueueEntry<TaskIndex>; 0]);
        assert!(
            elapsed >= wait,
            "recv should block for the full wait duration when no entries arrive",
        );
        Ok(())
    }

    #[tokio::test]
    async fn sends_never_wait_for_a_lane_that_nobody_polls() -> anyhow::Result<()> {
        const NUM_ENTRIES_PER_LANE: usize = 100_000;

        let (sender, receiver) = create_inbound_queue();
        let resource_group_id = ResourceGroupId::default();
        let job_id = JobId::default();
        tokio::time::timeout(Duration::from_secs(10), async {
            sender
                .send_task_ready(
                    resource_group_id,
                    job_id,
                    (0..NUM_ENTRIES_PER_LANE).collect(),
                )
                .await?;
            for _ in 0..NUM_ENTRIES_PER_LANE {
                sender.send_commit_ready(resource_group_id, job_id).await?;
                sender.send_cleanup_ready(resource_group_id, job_id).await?;
            }
            anyhow::Ok(())
        })
        .await??;

        let wait = Duration::from_secs(1);
        let task_indices: Vec<TaskIndex> = receiver
            .recv_tasks(NUM_ENTRIES_PER_LANE, wait)
            .await?
            .into_iter()
            .map(|entry| entry.task_kind)
            .collect();
        assert_eq!(task_indices, (0..NUM_ENTRIES_PER_LANE).collect::<Vec<_>>());
        assert_eq!(
            receiver
                .recv_commits(NUM_ENTRIES_PER_LANE, wait)
                .await?
                .len(),
            NUM_ENTRIES_PER_LANE
        );
        assert_eq!(
            receiver
                .recv_cleanups(NUM_ENTRIES_PER_LANE, wait)
                .await?
                .len(),
            NUM_ENTRIES_PER_LANE
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_send_after_the_receiver_is_dropped_fails_with_a_closed_lane() -> anyhow::Result<()> {
        let (sender, receiver) = create_inbound_queue();
        drop(receiver);
        let resource_group_id = ResourceGroupId::default();
        let job_id = JobId::default();
        for result in [
            sender
                .send_task_ready(resource_group_id, job_id, vec![0])
                .await,
            sender.send_commit_ready(resource_group_id, job_id).await,
            sender.send_cleanup_ready(resource_group_id, job_id).await,
        ] {
            assert!(
                matches!(result, Err(InternalError::InboundQueueChannelClosed)),
                "a send into a closed lane should fail, got {result:?}"
            );
        }
        Ok(())
    }
}
