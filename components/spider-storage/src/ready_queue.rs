use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

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
    /// Enqueues a batch of tasks for the specified job which are ready to be scheduled.
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
    ///
    /// # Returns
    ///
    /// The removed entries.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the entries fail to be removed from the ready queue.
    fn remove_task_entries(
        &self,
        job_id: JobId,
        task_id: TaskId,
    ) -> Result<Vec<ReadyQueueEntry>, InternalError>;

    /// Removes all entries for the given job across all priority lanes.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The job ID.
    ///
    /// # Returns
    ///
    /// All removed entries, across all three priority lanes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the entries fail to be removed from the ready queue.
    fn remove_job_entries(&self, job_id: JobId) -> Result<Vec<ReadyQueueEntry>, InternalError>;
}

/// Connector for getting task execution events from the ready queue.
///
/// This trait is invoked by the scheduler to dequeue tasks that are ready for dispatch. Each
/// priority lane (task, commit, cleanup) has its own cursor and `queue_id` sequence.
pub trait ReadyQueueReceiver: Clone + Send + Sync {
    /// Fetches up to `limit` task entries with `queue_id` greater than `start_after`.
    ///
    /// Returns immediately with zero or more entries. Repeated calls with the same cursor return
    /// the same entries as long as no entries have been removed in between.
    ///
    /// # Parameters
    ///
    /// * `start_after` - Only returns entries with `queue_id > start_after`. Use `0` to return from
    ///   the beginning.
    /// * `limit` - Maximum number of entries to return.
    ///
    /// # Returns
    ///
    /// A vector of matching [`ReadyQueueEntry`] values, up to `limit` in length.
    fn recv_task_batch(&self, start_after: u64, limit: usize) -> Vec<ReadyQueueEntry>;

    /// Returns the highest `queue_id` in the task lane, or `None` if the lane is empty.
    ///
    /// # Returns
    ///
    /// `Some(queue_id)` of the most recently enqueued task entry, or `None` if no entries exist.
    fn latest_task_id(&self) -> Option<u64>;

    /// Fetches up to `limit` commit entries with `queue_id` greater than `start_after`.
    ///
    /// Returns immediately with zero or more entries. Repeated calls with the same cursor return
    /// the same entries as long as no entries have been removed in between.
    ///
    /// # Parameters
    ///
    /// * `start_after` - Only returns entries with `queue_id > start_after`. Use `0` to return from
    ///   the beginning.
    /// * `limit` - Maximum number of entries to return.
    ///
    /// # Returns
    ///
    /// A vector of matching [`ReadyQueueEntry`] values, up to `limit` in length.
    fn recv_commit_batch(&self, start_after: u64, limit: usize) -> Vec<ReadyQueueEntry>;

    /// Returns the highest `queue_id` in the commit lane, or `None` if the lane is empty.
    ///
    /// # Returns
    ///
    /// `Some(queue_id)` of the most recently enqueued commit entry, or `None` if no entries
    /// exist.
    fn latest_commit_id(&self) -> Option<u64>;

    /// Fetches up to `limit` cleanup entries with `queue_id` greater than `start_after`.
    ///
    /// Returns immediately with zero or more entries. Repeated calls with the same cursor return
    /// the same entries as long as no entries have been removed in between.
    ///
    /// # Parameters
    ///
    /// * `start_after` - Only returns entries with `queue_id > start_after`. Use `0` to return from
    ///   the beginning.
    /// * `limit` - Maximum number of entries to return.
    ///
    /// # Returns
    ///
    /// A vector of matching [`ReadyQueueEntry`] values, up to `limit` in length.
    fn recv_cleanup_batch(&self, start_after: u64, limit: usize) -> Vec<ReadyQueueEntry>;

    /// Returns the highest `queue_id` in the cleanup lane, or `None` if the lane is empty.
    ///
    /// # Returns
    ///
    /// `Some(queue_id)` of the most recently enqueued cleanup entry, or `None` if no entries
    /// exist.
    fn latest_cleanup_id(&self) -> Option<u64>;
}

/// A shareable ready queue for scheduling tasks.
///
/// The queue maintains three priority queues (task, commit, cleanup), each with its own
/// monotonically increasing `queue_id` sequence for cursor-based pagination.
///
/// Use [`SharedReadyQueue::sender`] to obtain a write handle for enqueuing tasks and
/// [`SharedReadyQueue::receiver`] to obtain a read handle for dequeuing.
pub struct SharedReadyQueue {
    inner: Arc<std::sync::Mutex<ReadyQueue>>,
}

impl Default for SharedReadyQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedReadyQueue {
    /// Creates a new empty ready queue.
    ///
    /// # Returns
    ///
    /// A new [`SharedReadyQueue`] instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(ReadyQueue {
                task: TaskQueue::new(),
                commit: TaskQueue::new(),
                cleanup: TaskQueue::new(),
            })),
        }
    }

    /// Creates a sender handle for enqueueing tasks.
    ///
    /// # Returns
    ///
    /// A [`ReadyQueueSenderHandle`] backed by this queue.
    #[must_use]
    pub fn sender(&self) -> ReadyQueueSenderHandle {
        ReadyQueueSenderHandle::new(self.inner.clone())
    }

    /// Creates a receiver handle for dequeuing tasks.
    ///
    /// # Returns
    ///
    /// A [`ReadyQueueReceiverHandle`] backed by this queue.
    #[must_use]
    pub fn receiver(&self) -> ReadyQueueReceiverHandle {
        ReadyQueueReceiverHandle::new(self.inner.clone())
    }
}

#[derive(Clone)]
pub struct ReadyQueueSenderHandle {
    inner: Arc<std::sync::Mutex<ReadyQueue>>,
}

impl ReadyQueueSenderHandle {
    const fn new(inner: Arc<std::sync::Mutex<ReadyQueue>>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl ReadyQueueSender for ReadyQueueSenderHandle {
    async fn send_task_ready(
        &self,
        job_id: JobId,
        resource_group_id: ResourceGroupId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError> {
        let mut queue = self.inner.lock().unwrap();
        for task_index in task_indices {
            let queue_id = queue.task.next_id;
            queue.task.next_id += 1;
            queue.task.push(ReadyQueueEntry {
                queue_id,
                job_id,
                resource_group_id,
                task_id: TaskId::Index(task_index),
            });
        }
        drop(queue);
        Ok(())
    }

    async fn send_commit_ready(
        &self,
        job_id: JobId,
        resource_group_id: ResourceGroupId,
    ) -> Result<(), InternalError> {
        let mut queue = self.inner.lock().unwrap();
        let queue_id = queue.commit.next_id;
        queue.commit.next_id += 1;
        queue.commit.push(ReadyQueueEntry {
            queue_id,
            job_id,
            resource_group_id,
            task_id: TaskId::Commit,
        });
        drop(queue);
        Ok(())
    }

    async fn send_cleanup_ready(
        &self,
        job_id: JobId,
        resource_group_id: ResourceGroupId,
    ) -> Result<(), InternalError> {
        let mut queue = self.inner.lock().unwrap();
        let queue_id = queue.cleanup.next_id;
        queue.cleanup.next_id += 1;
        queue.cleanup.push(ReadyQueueEntry {
            queue_id,
            job_id,
            resource_group_id,
            task_id: TaskId::Cleanup,
        });
        drop(queue);
        Ok(())
    }

    fn remove_task_entries(
        &self,
        job_id: JobId,
        task_id: TaskId,
    ) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        let mut queue = self.inner.lock().unwrap();
        Ok(match task_id {
            TaskId::Index(_) => queue.task.remove_task_entries(job_id, task_id),
            TaskId::Commit => queue.commit.remove_task_entries(job_id, task_id),
            TaskId::Cleanup => queue.cleanup.remove_task_entries(job_id, task_id),
        })
    }

    fn remove_job_entries(&self, job_id: JobId) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        let mut queue = self.inner.lock().unwrap();
        let mut removed = Vec::new();
        removed.extend(queue.task.remove_job_entries(job_id));
        removed.extend(queue.commit.remove_job_entries(job_id));
        removed.extend(queue.cleanup.remove_job_entries(job_id));
        drop(queue);
        Ok(removed)
    }
}

#[derive(Clone)]
pub struct ReadyQueueReceiverHandle {
    inner: Arc<std::sync::Mutex<ReadyQueue>>,
}

impl ReadyQueueReceiverHandle {
    const fn new(inner: Arc<std::sync::Mutex<ReadyQueue>>) -> Self {
        Self { inner }
    }
}

impl ReadyQueueReceiver for ReadyQueueReceiverHandle {
    fn recv_task_batch(&self, start_after: u64, limit: usize) -> Vec<ReadyQueueEntry> {
        let queue = self.inner.lock().unwrap();
        queue.task.recv_batch(start_after, limit)
    }

    fn latest_task_id(&self) -> Option<u64> {
        let queue = self.inner.lock().unwrap();
        queue.task.latest_id()
    }

    fn recv_commit_batch(&self, start_after: u64, limit: usize) -> Vec<ReadyQueueEntry> {
        let queue = self.inner.lock().unwrap();
        queue.commit.recv_batch(start_after, limit)
    }

    fn latest_commit_id(&self) -> Option<u64> {
        let queue = self.inner.lock().unwrap();
        queue.commit.latest_id()
    }

    fn recv_cleanup_batch(&self, start_after: u64, limit: usize) -> Vec<ReadyQueueEntry> {
        let queue = self.inner.lock().unwrap();
        queue.cleanup.recv_batch(start_after, limit)
    }

    fn latest_cleanup_id(&self) -> Option<u64> {
        let queue = self.inner.lock().unwrap();
        queue.cleanup.latest_id()
    }
}

/// A queue of [`ReadyQueueEntry`] values backed by a [`BTreeMap`] for O(log n) paginated reads.
///
/// Each queue maintains its own monotonically increasing `queue_id` sequence and a secondary
/// index mapping `(job_id, task_id)` to the set of `queue_id`s for efficient removal by job or
/// task.
struct TaskQueue {
    /// Primary store: `queue_id` -> entry. Ordered by `queue_id` for O(log n) range queries.
    entries: BTreeMap<u64, ReadyQueueEntry>,
    /// Secondary index: `(job_id, task_id)` -> set of `queue_id`s. For removal lookups.
    job_task_index: HashMap<(JobId, TaskId), BTreeSet<u64>>,
    /// Monotonically increasing ID counter for this lane.
    next_id: u64,
}

impl TaskQueue {
    /// Creates a new empty lane with IDs starting at 1.
    fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            job_task_index: HashMap::new(),
            next_id: 1,
        }
    }

    /// Appends an entry to the lane, updating both the primary store and secondary index.
    fn push(&mut self, entry: ReadyQueueEntry) {
        let key = (entry.job_id, entry.task_id.clone());
        self.job_task_index
            .entry(key)
            .or_default()
            .insert(entry.queue_id);
        self.entries.insert(entry.queue_id, entry);
    }

    /// Returns up to `limit` entries with `queue_id > start_after` using O(log n) seek.
    fn recv_batch(&self, start_after: u64, limit: usize) -> Vec<ReadyQueueEntry> {
        let Some(start) = start_after.checked_add(1) else {
            return Vec::new();
        };
        self.entries
            .range(start..)
            .map(|(_, v)| v)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Returns the highest `queue_id` in this lane, or `None` if empty.
    fn latest_id(&self) -> Option<u64> {
        self.entries.last_key_value().map(|(id, _)| *id)
    }

    /// Removes all entries matching the given `(job_id, task_id)` pair.
    ///
    /// Uses the secondary index to look up the set of matching `queue_id`s, then removes each
    /// from the primary store.
    fn remove_task_entries(&mut self, job_id: JobId, task_id: TaskId) -> Vec<ReadyQueueEntry> {
        let Some(ids) = self.job_task_index.remove(&(job_id, task_id)) else {
            return Vec::new();
        };
        ids.into_iter()
            .filter_map(|id| self.entries.remove(&id))
            .collect()
    }

    /// Removes all entries for the given `job_id`.
    ///
    /// Scans the secondary index for all `(job_id, task_id)` pairs matching the job, then removes
    /// each from the primary store.
    fn remove_job_entries(&mut self, job_id: JobId) -> Vec<ReadyQueueEntry> {
        let keys_to_remove: Vec<(JobId, TaskId)> = self
            .job_task_index
            .keys()
            .filter(|(jid, _)| *jid == job_id)
            .cloned()
            .collect();
        let mut removed = Vec::new();
        for key in keys_to_remove {
            if let Some(ids) = self.job_task_index.remove(&key) {
                removed.extend(ids.into_iter().filter_map(|id| self.entries.remove(&id)));
            }
        }
        removed
    }
}

/// The ready queue data, containing three task queues.
struct ReadyQueue {
    task: TaskQueue,
    commit: TaskQueue,
    cleanup: TaskQueue,
}

#[cfg(test)]
mod tests {
    use spider_core::types::id::{JobId, ResourceGroupId};

    use super::*;

    fn default_ids() -> (JobId, ResourceGroupId) {
        (JobId::default(), ResourceGroupId::default())
    }

    fn create_queue() -> (ReadyQueueSenderHandle, ReadyQueueReceiverHandle) {
        let queue = SharedReadyQueue::new();
        (queue.sender(), queue.receiver())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_and_recv_single_task() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();
        sender
            .send_task_ready(job_id, rg_id, vec![0])
            .await
            .expect("send should succeed");

        let batch = receiver.recv_task_batch(0, 1);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].queue_id, 1);
        assert_eq!(batch[0].job_id, job_id);
        assert_eq!(batch[0].resource_group_id, rg_id);
        assert!(matches!(batch[0].task_id, TaskId::Index(0)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_and_recv_batch() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1, 2])
            .await
            .expect("send should succeed");

        let batch = receiver.recv_task_batch(0, 10);
        assert_eq!(batch.len(), 3, "should receive all three entries");
        assert_eq!(batch[0].queue_id, 1);
        assert_eq!(batch[1].queue_id, 2);
        assert_eq!(batch[2].queue_id, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_with_start_after() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1, 2, 3, 4])
            .await
            .expect("send should succeed");

        let batch = receiver.recv_task_batch(2, 10);
        assert_eq!(batch.len(), 3, "should skip entries with queue_id <= 2");
        assert_eq!(batch[0].queue_id, 3);
        assert_eq!(batch[1].queue_id, 4);
        assert_eq!(batch[2].queue_id, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_limit() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1, 2, 3, 4])
            .await
            .expect("send should succeed");

        let batch = receiver.recv_task_batch(0, 3);
        assert_eq!(batch.len(), 3, "should respect limit");
        assert_eq!(batch[0].queue_id, 1);
        assert_eq!(batch[2].queue_id, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_empty() {
        let (_sender, receiver) = create_queue();
        assert!(
            receiver.recv_task_batch(0, 10).is_empty(),
            "should return empty when no messages"
        );
        assert!(
            receiver.recv_commit_batch(0, 10).is_empty(),
            "should return empty when no messages"
        );
        assert!(
            receiver.recv_cleanup_batch(0, 10).is_empty(),
            "should return empty when no messages"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn latest_id_tracks_newest() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();

        assert!(
            receiver.latest_task_id().is_none(),
            "should be None when empty"
        );

        sender
            .send_task_ready(job_id, rg_id, vec![0])
            .await
            .expect("send should succeed");
        assert_eq!(receiver.latest_task_id(), Some(1));

        sender
            .send_task_ready(job_id, rg_id, vec![1, 2])
            .await
            .expect("send should succeed");
        assert_eq!(receiver.latest_task_id(), Some(3));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn per_queue_ids() {
        let (sender, receiver) = create_queue();
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

        // Each lane has its own ID sequence starting at 1.
        assert_eq!(receiver.latest_task_id(), Some(1));
        assert_eq!(receiver.latest_commit_id(), Some(1));
        assert_eq!(receiver.latest_cleanup_id(), Some(1));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_and_recv_commit() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();

        sender
            .send_commit_ready(job_id, rg_id)
            .await
            .expect("send should succeed");

        let batch = receiver.recv_commit_batch(0, 1);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].queue_id, 1);
        assert_eq!(batch[0].job_id, job_id);
        assert!(matches!(batch[0].task_id, TaskId::Commit));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_and_recv_cleanup() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();

        sender
            .send_cleanup_ready(job_id, rg_id)
            .await
            .expect("send should succeed");

        let batch = receiver.recv_cleanup_batch(0, 1);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].queue_id, 1);
        assert_eq!(batch[0].job_id, job_id);
        assert!(matches!(batch[0].task_id, TaskId::Cleanup));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cloned_sender_sends_to_same_queue() {
        let (sender, receiver) = create_queue();
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

        let batch = receiver.recv_task_batch(0, 10);
        assert_eq!(batch.len(), 2);
        assert!(matches!(batch[0].task_id, TaskId::Index(1)));
        assert!(matches!(batch[1].task_id, TaskId::Index(2)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_task_batch_is_idempotent() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1])
            .await
            .expect("send should succeed");

        let batch1 = receiver.recv_task_batch(0, 10);
        assert_eq!(batch1.len(), 2);

        // Second call with same cursor returns the same entries.
        let batch2 = receiver.recv_task_batch(0, 10);
        assert_eq!(batch2.len(), 2);
        assert_eq!(batch1[0].queue_id, batch2[0].queue_id);
        assert_eq!(batch1[1].queue_id, batch2[1].queue_id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_task_entries_removes_matching() {
        let (sender, receiver) = create_queue();
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

        let removed = sender
            .remove_task_entries(job_id, TaskId::Index(0))
            .expect("remove should succeed");
        assert_eq!(removed.len(), 1);
        assert!(matches!(removed[0].task_id, TaskId::Index(0)));

        let remaining = receiver.recv_task_batch(0, 10);
        assert_eq!(remaining.len(), 2);
        assert!(
            remaining
                .iter()
                .all(|e| e.task_id != TaskId::Index(0) || e.job_id != job_id)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_job_entries_removes_all_for_job() {
        let (sender, receiver) = create_queue();
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

        let removed = sender
            .remove_job_entries(job_id)
            .expect("remove should succeed");
        assert_eq!(removed.len(), 2);
        assert!(removed.iter().all(|e| e.job_id == job_id));

        let remaining = receiver.recv_task_batch(0, 10);
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].job_id, other_job_id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_skips_removed_entries() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1, 2])
            .await
            .expect("send should succeed");
        sender
            .remove_task_entries(job_id, TaskId::Index(1))
            .expect("remove should succeed");

        let batch = receiver.recv_task_batch(0, 10);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].queue_id, 1);
        assert_eq!(batch[1].queue_id, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_after_removal_filters_correctly() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();

        sender
            .send_task_ready(job_id, rg_id, vec![0, 1, 2, 3])
            .await
            .expect("send should succeed");
        sender
            .remove_task_entries(job_id, TaskId::Index(1))
            .expect("remove should succeed");
        sender
            .remove_task_entries(job_id, TaskId::Index(2))
            .expect("remove should succeed");

        let batch = receiver.recv_task_batch(1, 10);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].queue_id, 4);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn latest_id_after_removal_reflects_remaining() {
        let (sender, receiver) = create_queue();
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
        assert_eq!(receiver.latest_task_id(), Some(3));

        sender
            .remove_job_entries(job_id)
            .expect("remove should succeed");
        assert_eq!(
            receiver.latest_task_id(),
            Some(3),
            "latest_task_id should return the last remaining entry's queue_id"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_batch_start_after_u64_max_returns_empty() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();
        sender
            .send_task_ready(job_id, rg_id, vec![0])
            .await
            .expect("send should succeed");

        let batch = receiver.recv_task_batch(u64::MAX, 10);
        assert!(batch.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_task_entries_routes_commit_to_commit_lane() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();

        sender
            .send_commit_ready(job_id, rg_id)
            .await
            .expect("send commit should succeed");
        sender
            .send_cleanup_ready(job_id, rg_id)
            .await
            .expect("send cleanup should succeed");

        let removed = sender
            .remove_task_entries(job_id, TaskId::Commit)
            .expect("remove should succeed");
        assert_eq!(removed.len(), 1);
        assert!(matches!(removed[0].task_id, TaskId::Commit));

        assert!(
            receiver.recv_commit_batch(0, 10).is_empty(),
            "commit lane should be empty after removal"
        );
        assert_eq!(
            receiver.recv_cleanup_batch(0, 10).len(),
            1,
            "cleanup lane should still have its entry"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_task_entries_routes_cleanup_to_cleanup_lane() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();

        sender
            .send_commit_ready(job_id, rg_id)
            .await
            .expect("send commit should succeed");
        sender
            .send_cleanup_ready(job_id, rg_id)
            .await
            .expect("send cleanup should succeed");

        let removed = sender
            .remove_task_entries(job_id, TaskId::Cleanup)
            .expect("remove should succeed");
        assert_eq!(removed.len(), 1);
        assert!(matches!(removed[0].task_id, TaskId::Cleanup));

        assert_eq!(
            receiver.recv_commit_batch(0, 10).len(),
            1,
            "commit lane should still have its entry"
        );
        assert!(
            receiver.recv_cleanup_batch(0, 10).is_empty(),
            "cleanup lane should be empty after removal"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_job_entries_removes_from_all_lanes() {
        let (sender, receiver) = create_queue();
        let (job_id, rg_id) = default_ids();
        let other_job_id = JobId::new();

        sender
            .send_task_ready(job_id, rg_id, vec![0])
            .await
            .expect("send task should succeed");
        sender
            .send_commit_ready(job_id, rg_id)
            .await
            .expect("send commit should succeed");
        sender
            .send_cleanup_ready(job_id, rg_id)
            .await
            .expect("send cleanup should succeed");
        sender
            .send_task_ready(other_job_id, rg_id, vec![1])
            .await
            .expect("send other task should succeed");

        let removed = sender
            .remove_job_entries(job_id)
            .expect("remove should succeed");
        assert_eq!(
            removed.len(),
            3,
            "should remove entries from all three lanes"
        );

        assert_eq!(
            receiver.recv_task_batch(0, 10).len(),
            1,
            "only other_job entry should remain in task lane"
        );
        assert!(
            receiver.recv_commit_batch(0, 10).is_empty(),
            "commit lane should be empty"
        );
        assert!(
            receiver.recv_cleanup_batch(0, 10).is_empty(),
            "cleanup lane should be empty"
        );
    }
}
