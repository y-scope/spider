use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use spider_core::{
    task::TaskIndex,
    types::id::{JobId, ResourceGroupId},
};

use crate::cache::{TaskId, error::InternalError};

/// A single entry in the ready queue.
///
/// Each entry represents one schedulable unit of work (a regular task, commit task, or cleanup
/// task) and carries a monotonically increasing [`queue_id`](ReadyQueueEntry::queue_id) for
/// cursor-based pagination.
#[derive(Clone, Debug)]
pub struct ReadyQueueEntry {
    /// Monotonically increasing ID assigned when the entry is enqueued.
    pub queue_id: u64,
    /// The job this task belongs to.
    pub job_id: JobId,
    /// The resource group that owns the job.
    pub resource_group_id: ResourceGroupId,
    /// Identifies the task within the job.
    pub task_id: TaskId,
}

/// Connector for publishing task execution events to the ready queue.
///
/// This trait is invoked by the cache layer to enqueue tasks that are ready for scheduling.
#[async_trait]
pub trait ReadyQueueSender: Clone + Send + Sync {
    /// Enqueues regular tasks that have become ready for scheduling.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The job ID.
    /// * `resource_group_id` - The resource group that owns the job.
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
        resource_group_id: ResourceGroupId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError>;

    /// Enqueues a signal indicating that the commit task of the given job is ready to be scheduled.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The job ID.
    /// * `resource_group_id` - The resource group that owns the job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the message fails to be sent to the ready queue.
    async fn send_commit_ready(
        &self,
        job_id: JobId,
        resource_group_id: ResourceGroupId,
    ) -> Result<(), InternalError>;

    /// Enqueues a signal indicating that the cleanup task of the given job is ready to be
    /// scheduled.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The job ID.
    /// * `resource_group_id` - The resource group that owns the job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the message fails to be sent to the ready queue.
    async fn send_cleanup_ready(
        &self,
        job_id: JobId,
        resource_group_id: ResourceGroupId,
    ) -> Result<(), InternalError>;

    /// Removes all entries matching the given job and task.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The job ID.
    /// * `task_id` - The task ID to remove.
    fn remove_task_entries(&self, job_id: JobId, task_id: TaskId);

    /// Removes all entries for the given job.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The job ID.
    fn remove_job_entries(&self, job_id: JobId);
}

/// Connector for consuming task execution events from the ready queue.
///
/// This trait is invoked by the scheduler to dequeue tasks that are ready for dispatch.
pub trait ReadyQueueReceiver: Clone + Send + Sync {
    /// Returns up to `limit` entries with `queue_id > start_after`.
    ///
    /// Returns immediately with 0 or more entries. Idempotent — repeated calls with the same cursor
    /// return the same entries as long as no entries have been removed in between.
    ///
    /// # Parameters
    ///
    /// * `start_after` - If `Some(id)`, only returns entries with `queue_id > id`. If `None`,
    ///   returns from the beginning.
    /// * `limit` - Maximum number of entries to return.
    fn recv_batch(&self, start_after: Option<u64>, limit: usize) -> Vec<ReadyQueueEntry>;

    /// Returns the `queue_id` of the last entry in the queue, or `None` if empty.
    fn latest_id(&self) -> Option<u64>;
}

/// Shared state for the ready queue.
struct ReadyQueueInner {
    entries: VecDeque<ReadyQueueEntry>,
    next_id: u64,
}

struct ReadyQueueShared {
    inner: std::sync::Mutex<ReadyQueueInner>,
}

/// Creates a new ready queue.
///
/// # Returns
///
/// A tuple of (sender, receiver) backed by an append-only log with cursor-based pagination.
#[must_use]
pub fn channel() -> (ReadyQueueSenderImpl, ReadyQueueReceiverImpl) {
    let shared = Arc::new(ReadyQueueShared {
        inner: std::sync::Mutex::new(ReadyQueueInner {
            entries: VecDeque::new(),
            next_id: 1,
        }),
    });
    (
        ReadyQueueSenderImpl {
            shared: shared.clone(),
        },
        ReadyQueueReceiverImpl { shared },
    )
}

#[derive(Clone)]
pub struct ReadyQueueSenderImpl {
    shared: Arc<ReadyQueueShared>,
}

#[async_trait]
impl ReadyQueueSender for ReadyQueueSenderImpl {
    async fn send_task_ready(
        &self,
        job_id: JobId,
        resource_group_id: ResourceGroupId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError> {
        let mut inner = self.shared.inner.lock().unwrap();
        for task_index in task_indices {
            let queue_id = inner.next_id;
            inner.next_id += 1;
            inner.entries.push_back(ReadyQueueEntry {
                queue_id,
                job_id,
                resource_group_id,
                task_id: TaskId::Index(task_index),
            });
        }
        drop(inner);
        Ok(())
    }

    async fn send_commit_ready(
        &self,
        job_id: JobId,
        resource_group_id: ResourceGroupId,
    ) -> Result<(), InternalError> {
        let mut inner = self.shared.inner.lock().unwrap();
        let queue_id = inner.next_id;
        inner.next_id += 1;
        inner.entries.push_back(ReadyQueueEntry {
            queue_id,
            job_id,
            resource_group_id,
            task_id: TaskId::Commit,
        });
        drop(inner);
        Ok(())
    }

    async fn send_cleanup_ready(
        &self,
        job_id: JobId,
        resource_group_id: ResourceGroupId,
    ) -> Result<(), InternalError> {
        let mut inner = self.shared.inner.lock().unwrap();
        let queue_id = inner.next_id;
        inner.next_id += 1;
        inner.entries.push_back(ReadyQueueEntry {
            queue_id,
            job_id,
            resource_group_id,
            task_id: TaskId::Cleanup,
        });
        drop(inner);
        Ok(())
    }

    fn remove_task_entries(&self, job_id: JobId, task_id: TaskId) {
        let mut inner = self.shared.inner.lock().unwrap();
        inner
            .entries
            .retain(|e| !(e.job_id == job_id && e.task_id == task_id));
    }

    fn remove_job_entries(&self, job_id: JobId) {
        let mut inner = self.shared.inner.lock().unwrap();
        inner.entries.retain(|e| e.job_id != job_id);
    }
}

#[derive(Clone)]
pub struct ReadyQueueReceiverImpl {
    shared: Arc<ReadyQueueShared>,
}

impl ReadyQueueReceiver for ReadyQueueReceiverImpl {
    fn recv_batch(&self, start_after: Option<u64>, limit: usize) -> Vec<ReadyQueueEntry> {
        let inner = self.shared.inner.lock().unwrap();
        inner
            .entries
            .iter()
            .filter(|e| start_after.is_none_or(|id| e.queue_id > id))
            .take(limit)
            .cloned()
            .collect()
    }

    fn latest_id(&self) -> Option<u64> {
        let inner = self.shared.inner.lock().unwrap();
        inner.entries.back().map(|e| e.queue_id)
    }
}

#[cfg(test)]
mod tests {
    use spider_core::types::id::{JobId, ResourceGroupId};

    use super::*;

    fn default_ids() -> (JobId, ResourceGroupId) {
        (JobId::default(), ResourceGroupId::default())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_and_recv_single_task() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();
        sender
            .send_task_ready(job_id, rg_id, vec![0])
            .await
            .expect("send should succeed");

        let batch = receiver.recv_batch(None, 1);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].queue_id, 1);
        assert_eq!(batch[0].job_id, job_id);
        assert_eq!(batch[0].resource_group_id, rg_id);
        assert!(matches!(batch[0].task_id, TaskId::Index(0)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_and_recv_batch() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1, 2])
            .await
            .expect("send should succeed");

        let batch = receiver.recv_batch(None, 10);
        assert_eq!(batch.len(), 3, "should receive all three entries");
        assert_eq!(batch[0].queue_id, 1);
        assert_eq!(batch[1].queue_id, 2);
        assert_eq!(batch[2].queue_id, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_with_start_after() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1, 2, 3, 4])
            .await
            .expect("send should succeed");

        let batch = receiver.recv_batch(Some(2), 10);
        assert_eq!(batch.len(), 3, "should skip entries with queue_id <= 2");
        assert_eq!(batch[0].queue_id, 3);
        assert_eq!(batch[1].queue_id, 4);
        assert_eq!(batch[2].queue_id, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_limit() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1, 2, 3, 4])
            .await
            .expect("send should succeed");

        let batch = receiver.recv_batch(None, 3);
        assert_eq!(batch.len(), 3, "should respect limit");
        assert_eq!(batch[0].queue_id, 1);
        assert_eq!(batch[2].queue_id, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_empty() {
        let (_sender, receiver) = channel();
        let batch = receiver.recv_batch(None, 10);
        assert!(batch.is_empty(), "should return empty when no messages");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn latest_id_tracks_newest() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();

        assert!(receiver.latest_id().is_none(), "should be None when empty");

        sender
            .send_task_ready(job_id, rg_id, vec![0])
            .await
            .expect("send should succeed");
        assert_eq!(receiver.latest_id(), Some(1));

        sender
            .send_task_ready(job_id, rg_id, vec![1, 2])
            .await
            .expect("send should succeed");
        assert_eq!(receiver.latest_id(), Some(3));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn auto_increment_ids() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0])
            .await
            .expect("send should succeed");
        sender
            .send_commit_ready(job_id, rg_id)
            .await
            .expect("send should succeed");
        sender
            .send_cleanup_ready(job_id, rg_id)
            .await
            .expect("send should succeed");

        let batch = receiver.recv_batch(None, 10);
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].queue_id, 1);
        assert_eq!(batch[1].queue_id, 2);
        assert_eq!(batch[2].queue_id, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_and_recv_commit() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();

        sender
            .send_commit_ready(job_id, rg_id)
            .await
            .expect("send should succeed");

        let batch = receiver.recv_batch(None, 1);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].queue_id, 1);
        assert_eq!(batch[0].job_id, job_id);
        assert!(matches!(batch[0].task_id, TaskId::Commit));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_and_recv_cleanup() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();

        sender
            .send_cleanup_ready(job_id, rg_id)
            .await
            .expect("send should succeed");

        let batch = receiver.recv_batch(None, 1);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].queue_id, 1);
        assert_eq!(batch[0].job_id, job_id);
        assert!(matches!(batch[0].task_id, TaskId::Cleanup));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_preserves_order() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0])
            .await
            .expect("send should succeed");
        sender
            .send_commit_ready(job_id, rg_id)
            .await
            .expect("send should succeed");
        sender
            .send_cleanup_ready(job_id, rg_id)
            .await
            .expect("send should succeed");

        let batch = receiver.recv_batch(None, 10);
        assert_eq!(batch.len(), 3);
        assert!(matches!(batch[0].task_id, TaskId::Index(0)));
        assert!(matches!(batch[1].task_id, TaskId::Commit));
        assert!(matches!(batch[2].task_id, TaskId::Cleanup));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cloned_sender_sends_to_same_queue() {
        let (sender, receiver) = channel();
        let sender2 = sender.clone();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![1])
            .await
            .expect("send from original should succeed");
        sender2
            .send_task_ready(job_id, rg_id, vec![2])
            .await
            .expect("send from clone should succeed");

        let batch = receiver.recv_batch(None, 10);
        assert_eq!(batch.len(), 2);
        assert!(matches!(batch[0].task_id, TaskId::Index(1)));
        assert!(matches!(batch[1].task_id, TaskId::Index(2)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_is_idempotent() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1])
            .await
            .expect("send should succeed");

        let batch1 = receiver.recv_batch(None, 10);
        assert_eq!(batch1.len(), 2);

        // Second call with same cursor returns the same entries.
        let batch2 = receiver.recv_batch(None, 10);
        assert_eq!(batch2.len(), 2);
        assert_eq!(batch1[0].queue_id, batch2[0].queue_id);
        assert_eq!(batch1[1].queue_id, batch2[1].queue_id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_task_entries_removes_matching() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();
        let other_job_id = JobId::new();

        sender
            .send_task_ready(job_id, rg_id, vec![0])
            .await
            .expect("send should succeed");
        sender
            .send_task_ready(other_job_id, rg_id, vec![1])
            .await
            .expect("send should succeed");
        sender
            .send_task_ready(job_id, rg_id, vec![2])
            .await
            .expect("send should succeed");

        sender.remove_task_entries(job_id, TaskId::Index(0));

        let remaining = receiver.recv_batch(None, 10);
        assert_eq!(remaining.len(), 2);
        assert!(
            remaining
                .iter()
                .all(|e| e.task_id != TaskId::Index(0) || e.job_id != job_id)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_job_entries_removes_all_for_job() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();
        let other_job_id = JobId::new();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1])
            .await
            .expect("send should succeed");
        sender
            .send_task_ready(other_job_id, rg_id, vec![2])
            .await
            .expect("send should succeed");

        sender.remove_job_entries(job_id);

        let remaining = receiver.recv_batch(None, 10);
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].job_id, other_job_id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_skips_removed_entries() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1, 2])
            .await
            .expect("send should succeed");
        sender.remove_task_entries(job_id, TaskId::Index(1));

        let batch = receiver.recv_batch(None, 10);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].queue_id, 1);
        assert_eq!(batch[1].queue_id, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_after_removal_filters_correctly() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1, 2, 3])
            .await
            .expect("send should succeed");
        sender.remove_task_entries(job_id, TaskId::Index(1));
        sender.remove_task_entries(job_id, TaskId::Index(2));

        let batch = receiver.recv_batch(Some(1), 10);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].queue_id, 4);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn latest_id_after_removal_reflects_remaining() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();
        let other_job_id = JobId::new();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1])
            .await
            .expect("send should succeed");
        sender
            .send_task_ready(other_job_id, rg_id, vec![2])
            .await
            .expect("send should succeed");
        assert_eq!(receiver.latest_id(), Some(3));

        sender.remove_job_entries(job_id);
        assert_eq!(
            receiver.latest_id(),
            Some(3),
            "latest_id should return the last remaining entry's queue_id"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_start_after_zero_behaves_like_none() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();
        sender
            .send_task_ready(job_id, rg_id, vec![0, 1])
            .await
            .expect("send should succeed");

        let batch_none = receiver.recv_batch(None, 10);
        let batch_zero = receiver.recv_batch(Some(0), 10);
        assert_eq!(batch_none.len(), batch_zero.len());
        assert_eq!(batch_none[0].queue_id, batch_zero[0].queue_id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_start_after_u64_max_returns_empty() {
        let (sender, receiver) = channel();
        let (job_id, rg_id) = default_ids();
        sender
            .send_task_ready(job_id, rg_id, vec![0])
            .await
            .expect("send should succeed");

        let batch = receiver.recv_batch(Some(u64::MAX), 10);
        assert!(batch.is_empty());
    }
}
