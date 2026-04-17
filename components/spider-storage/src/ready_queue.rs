//! In-memory ready queue for schedulable tasks.
//!
//! The queue is split into three independent lanes: regular tasks, commit tasks, and cleanup
//! tasks. Each lane has a bounded ingress channel plus a resident deduplicating queue. A
//! dedicated flusher task forwards channel messages into resident state, while the resident queue
//! exposes writer, resetter, and receiver interfaces for blocking writes, snapshot rebuilds, and
//! blocking receives.

use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use spider_core::{
    task::TaskIndex,
    types::id::{JobId, ResourceGroupId},
};
use tokio::sync::{Mutex, Notify, mpsc};

use crate::cache::{TaskId, error::InternalError};

/// A ready queue entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReadyQueueEntry {
    /// The owning resource group for the ready job.
    pub resource_group_id: ResourceGroupId,
    /// The job that became schedulable.
    pub job_id: JobId,
    /// The specific ready task within the job.
    pub task_id: TaskId,
}

/// Configuration of a ready queue.
#[derive(Debug, Clone, Copy)]
pub struct ReadyQueueConfig {
    /// The capacity of each ingress channel. Must be greater than zero.
    pub ingress_capacity: usize,
    /// The resident capacity of the task lane. Must be greater than zero.
    pub task_ready_capacity: usize,
    /// The resident capacity of the commit lane. Must be greater than zero.
    pub commit_ready_capacity: usize,
    /// The resident capacity of the cleanup lane. Must be greater than zero.
    pub cleanup_ready_capacity: usize,
}

impl Default for ReadyQueueConfig {
    fn default() -> Self {
        Self {
            ingress_capacity: DEFAULT_INGRESS_CAPACITY,
            task_ready_capacity: DEFAULT_TASK_READY_CAPACITY,
            commit_ready_capacity: DEFAULT_COMMIT_READY_CAPACITY,
            cleanup_ready_capacity: DEFAULT_CLEANUP_READY_CAPACITY,
        }
    }
}

impl ReadyQueueConfig {
    /// Panics if any configured capacity is zero.
    fn assert_valid(self) {
        assert!(
            self.ingress_capacity > 0,
            "ready queue ingress_capacity must be > 0"
        );
        assert!(
            self.task_ready_capacity > 0,
            "ready queue task_ready_capacity must be > 0"
        );
        assert!(
            self.commit_ready_capacity > 0,
            "ready queue commit_ready_capacity must be > 0"
        );
        assert!(
            self.cleanup_ready_capacity > 0,
            "ready queue cleanup_ready_capacity must be > 0"
        );
    }
}

/// A shareable ready-queue sender.
#[derive(Clone)]
pub struct ReadyQueueSenderHandle {
    inner: Arc<ReadyQueueInner>,
}

/// A shareable ready-queue receiver.
#[derive(Clone)]
pub struct ReadyQueueReceiverHandle {
    inner: Arc<ReadyQueueInner>,
}

/// A shareable ready-queue resetter.
#[derive(Clone)]
pub struct ReadyQueueResetterHandle {
    inner: Arc<ReadyQueueInner>,
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
    /// * `resource_group_id` - The owning resource group ID.
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
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the message fails to be sent to the ready queue.
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
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the message fails to be sent to the ready queue.
    async fn send_cleanup_ready(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), InternalError>;
}

/// Connector for consuming ready tasks from the ready queue.
#[async_trait]
pub trait ReadyQueueReceiver: Clone + Send + Sync {
    /// Receives a batch of regular task entries.
    ///
    /// # Parameters
    ///
    /// * `max_items` - The maximum number of entries to receive.
    ///
    /// # Returns
    ///
    /// A batch of up to `max_items` regular task entries. This function blocks until at least one
    /// resident entry is available unless `max_items == 0`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the ready queue is corrupted or unavailable.
    async fn recv_tasks(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError>;

    /// Receives a batch of commit task entries.
    ///
    /// # Parameters
    ///
    /// * `max_items` - The maximum number of entries to receive.
    ///
    /// # Returns
    ///
    /// A batch of up to `max_items` commit entries. This function blocks until at least one
    /// resident entry is available unless `max_items == 0`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the ready queue is corrupted or unavailable.
    async fn recv_commits(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError>;

    /// Receives a batch of cleanup task entries.
    ///
    /// # Parameters
    ///
    /// * `max_items` - The maximum number of entries to receive.
    ///
    /// # Returns
    ///
    /// A batch of up to `max_items` cleanup entries. This function blocks until at least one
    /// resident entry is available unless `max_items == 0`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the ready queue is corrupted or unavailable.
    async fn recv_cleanups(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError>;

    /// # Returns
    ///
    /// The number of resident regular task entries.
    async fn num_tasks(&self) -> usize;

    /// # Returns
    ///
    /// The number of resident commit entries.
    async fn num_commits(&self) -> usize;

    /// # Returns
    ///
    /// The number of resident cleanup entries.
    async fn num_cleanups(&self) -> usize;

    /// # Returns
    ///
    /// The total number of resident entries across all queues.
    async fn num_all(&self) -> usize;
}

/// Connector for rebuilding resident ready-queue state from a snapshot.
#[async_trait]
pub trait ReadyQueueResetter: Clone + Send + Sync {
    /// Rebuilds the resident queues from a fresh snapshot of ready entries.
    ///
    /// This operation clears and repopulates resident contents without dropping already-buffered
    /// channel backlog.
    ///
    /// # Errors
    ///
    /// Returns [`InternalError::ReadyQueueSendFailure`] if the rebuilt snapshot exceeds resident
    /// queue capacity.
    async fn rebuild<I>(&self, entries: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = ReadyQueueEntry> + Send;
}

/// An in-memory ready queue backed by simple ingress channels and resident queues.
#[derive(Clone)]
pub struct ReadyQueue {
    inner: Arc<ReadyQueueInner>,
}

/// Default resident ready-task queue capacity.
const DEFAULT_TASK_READY_CAPACITY: usize = 65_536;

/// Default resident commit-task queue capacity.
const DEFAULT_COMMIT_READY_CAPACITY: usize = 1024;

/// Default resident cleanup-task queue capacity.
const DEFAULT_CLEANUP_READY_CAPACITY: usize = 1024;

/// Default ingress queue capacity.
const DEFAULT_INGRESS_CAPACITY: usize = DEFAULT_TASK_READY_CAPACITY;

#[async_trait]
impl ReadyQueueSender for ReadyQueueSenderHandle {
    async fn send_task_ready(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError> {
        if task_indices.is_empty() {
            return Ok(());
        }

        self.inner
            .send_task_ready_batch(resource_group_id, job_id, task_indices)
            .await
    }

    async fn send_commit_ready(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), InternalError> {
        self.inner
            .send_termination_ready(ReadyQueueEntry {
                resource_group_id,
                job_id,
                task_id: TaskId::Commit,
            })
            .await
    }

    async fn send_cleanup_ready(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), InternalError> {
        self.inner
            .send_termination_ready(ReadyQueueEntry {
                resource_group_id,
                job_id,
                task_id: TaskId::Cleanup,
            })
            .await
    }
}

impl ReadyQueue {
    /// Factory function.
    ///
    /// Creates a ready queue with the given configuration and eagerly starts the background
    /// flusher tasks on the given Tokio runtime.
    ///
    /// # Parameters
    ///
    /// * `config` - The ready queue configuration.
    /// * `runtime_handle` - The Tokio runtime handle used to spawn the background flusher tasks.
    ///
    /// # Returns
    ///
    /// The created ready queue.
    #[must_use]
    pub fn create(config: ReadyQueueConfig, runtime_handle: &tokio::runtime::Handle) -> Self {
        config.assert_valid();
        let task_queue = ResidentQueue::new(config.task_ready_capacity);
        let commit_queue = ResidentQueue::new(config.commit_ready_capacity);
        let cleanup_queue = ResidentQueue::new(config.cleanup_ready_capacity);

        let (task_sender, task_receiver) = mpsc::channel(config.ingress_capacity);
        let (commit_sender, commit_receiver) = mpsc::channel(config.ingress_capacity);
        let (cleanup_sender, cleanup_receiver) = mpsc::channel(config.ingress_capacity);

        ReadyQueueInner::spawn_task_flusher(
            runtime_handle,
            task_receiver,
            task_queue.writer.clone(),
        );
        ReadyQueueInner::spawn_entry_flusher(
            runtime_handle,
            commit_receiver,
            commit_queue.writer.clone(),
        );
        ReadyQueueInner::spawn_entry_flusher(
            runtime_handle,
            cleanup_receiver,
            cleanup_queue.writer.clone(),
        );

        let inner = Arc::new(ReadyQueueInner {
            task_sender,
            commit_sender,
            cleanup_sender,
            task_queue,
            commit_queue,
            cleanup_queue,
        });
        Self { inner }
    }

    /// # Returns
    ///
    /// A sender handle for enqueuing ready tasks.
    #[must_use]
    pub fn sender(&self) -> ReadyQueueSenderHandle {
        ReadyQueueSenderHandle {
            inner: self.inner.clone(),
        }
    }

    /// # Returns
    ///
    /// A receiver handle for consuming ready tasks.
    #[must_use]
    pub fn receiver(&self) -> ReadyQueueReceiverHandle {
        ReadyQueueReceiverHandle {
            inner: self.inner.clone(),
        }
    }

    /// # Returns
    ///
    /// A resetter handle for rebuilding resident ready-queue state.
    #[must_use]
    pub fn resetter(&self) -> ReadyQueueResetterHandle {
        ReadyQueueResetterHandle {
            inner: self.inner.clone(),
        }
    }

    /// Rebuilds the resident queues from a fresh snapshot of ready entries.
    ///
    /// This operation clears and repopulates the resident queues without dropping already-buffered
    /// channel backlog.
    ///
    /// # Errors
    ///
    /// Returns [`InternalError::ReadyQueueSendFailure`] if the rebuilt snapshot exceeds resident
    /// queue capacity.
    pub async fn rebuild<I>(&self, entries: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = ReadyQueueEntry> + Send, {
        self.resetter().rebuild(entries).await
    }
}

#[async_trait]
impl ReadyQueueReceiver for ReadyQueueReceiverHandle {
    async fn recv_tasks(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        self.inner.recv_tasks(max_items).await
    }

    async fn recv_commits(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        self.inner.recv_commits(max_items).await
    }

    async fn recv_cleanups(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        self.inner.recv_cleanups(max_items).await
    }

    async fn num_tasks(&self) -> usize {
        self.inner.num_tasks().await
    }

    async fn num_commits(&self) -> usize {
        self.inner.num_commits().await
    }

    async fn num_cleanups(&self) -> usize {
        self.inner.num_cleanups().await
    }

    async fn num_all(&self) -> usize {
        self.inner.num_all().await
    }
}

#[async_trait]
impl ReadyQueueResetter for ReadyQueueResetterHandle {
    async fn rebuild<I>(&self, entries: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = ReadyQueueEntry> + Send, {
        self.inner.rebuild(entries).await
    }
}

/// Shared state behind sender and receiver handles.
struct ReadyQueueInner {
    task_sender: mpsc::Sender<TaskReadyBatch>,
    commit_sender: mpsc::Sender<ReadyQueueEntry>,
    cleanup_sender: mpsc::Sender<ReadyQueueEntry>,
    task_queue: ResidentQueue,
    commit_queue: ResidentQueue,
    cleanup_queue: ResidentQueue,
}

/// Channel payload for regular task-ready batches.
struct TaskReadyBatch {
    resource_group_id: ResourceGroupId,
    job_id: JobId,
    task_indices: Vec<TaskIndex>,
}

/// A resident queue split into writer, resetter, and receiver interfaces.
#[derive(Clone)]
struct ResidentQueue {
    writer: ResidentQueueWriter,
    resetter: ResidentQueueResetter,
    receiver: ResidentQueueReceiverHandleInner,
}

#[derive(Clone)]
struct ResidentQueueWriter {
    inner: Arc<ResidentQueueInner>,
}

#[derive(Clone)]
struct ResidentQueueResetter {
    inner: Arc<ResidentQueueInner>,
}

#[derive(Clone)]
struct ResidentQueueReceiverHandleInner {
    inner: Arc<ResidentQueueInner>,
}

struct ResidentQueueInner {
    state: Mutex<ResidentQueueState>,
    receiver_notify: Notify,
    writer_notify: Notify,
}

/// Deduplicating resident queue state protected by a mutex.
struct ResidentQueueState {
    capacity: usize,
    entries: VecDeque<ReadyQueueEntry>,
    entry_set: HashSet<ReadyQueueEntry>,
}

/// Result of attempting to enqueue into a resident queue.
enum EnqueueResult {
    Enqueued,
    Duplicate,
    Full,
}

impl ReadyQueueInner {
    /// Starts the task-batch flusher for one ingress lane.
    fn spawn_task_flusher(
        runtime_handle: &tokio::runtime::Handle,
        mut receiver: mpsc::Receiver<TaskReadyBatch>,
        writer: ResidentQueueWriter,
    ) {
        runtime_handle.spawn(async move {
            while let Some(task_batch) = receiver.recv().await {
                writer.write_task_batch(task_batch).await;
            }
        });
    }

    /// Starts the entry flusher for one ingress lane.
    fn spawn_entry_flusher(
        runtime_handle: &tokio::runtime::Handle,
        mut receiver: mpsc::Receiver<ReadyQueueEntry>,
        writer: ResidentQueueWriter,
    ) {
        runtime_handle.spawn(async move {
            while let Some(ready_entry) = receiver.recv().await {
                writer.write_entry(ready_entry).await;
            }
        });
    }

    /// Enqueues one task-ready batch without unpacking it at the channel boundary.
    async fn send_task_ready_batch(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError> {
        self.task_sender
            .send(TaskReadyBatch {
                resource_group_id,
                job_id,
                task_indices,
            })
            .await
            .map_err(|_| {
                InternalError::ReadyQueueSendFailure(
                    "task ready queue ingress is closed".to_owned(),
                )
            })
    }

    /// Enqueues a commit-ready or cleanup-ready signal.
    async fn send_termination_ready(
        &self,
        ready_entry: ReadyQueueEntry,
    ) -> Result<(), InternalError> {
        let (ingress_queue_name, ingress_sender) = match ready_entry.task_id {
            TaskId::Index(_) => {
                return Err(InternalError::ReadyQueueSendFailure(
                    "regular tasks must be sent through send_task_ready".to_owned(),
                ));
            }
            TaskId::Commit => ("commit", &self.commit_sender),
            TaskId::Cleanup => ("cleanup", &self.cleanup_sender),
        };

        ingress_sender.send(ready_entry).await.map_err(|_| {
            InternalError::ReadyQueueSendFailure(format!(
                "{ingress_queue_name} ready queue ingress is closed"
            ))
        })
    }

    /// Receives regular task entries from the resident queue.
    async fn recv_tasks(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        self.task_queue.receiver.recv(max_items).await
    }

    /// Receives commit entries from the resident queue.
    async fn recv_commits(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        self.commit_queue.receiver.recv(max_items).await
    }

    /// Receives cleanup entries from the resident queue.
    async fn recv_cleanups(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        self.cleanup_queue.receiver.recv(max_items).await
    }

    /// Returns the resident task-queue length.
    async fn num_tasks(&self) -> usize {
        self.task_queue.receiver.len().await
    }

    /// Returns the resident commit-queue length.
    async fn num_commits(&self) -> usize {
        self.commit_queue.receiver.len().await
    }

    /// Returns the resident cleanup-queue length.
    async fn num_cleanups(&self) -> usize {
        self.cleanup_queue.receiver.len().await
    }

    /// Returns the combined resident length across all queues.
    async fn num_all(&self) -> usize {
        self.num_tasks().await + self.num_commits().await + self.num_cleanups().await
    }

    /// Replaces resident queue contents from a rebuilt snapshot while preserving channel backlog.
    async fn rebuild<I>(&self, entries: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = ReadyQueueEntry>, {
        let mut task_entries = Vec::new();
        let mut commit_entries = Vec::new();
        let mut cleanup_entries = Vec::new();

        for ready_entry in entries {
            match ready_entry.task_id {
                TaskId::Index(_) => task_entries.push(ready_entry),
                TaskId::Commit => commit_entries.push(ready_entry),
                TaskId::Cleanup => cleanup_entries.push(ready_entry),
            }
        }

        self.task_queue.resetter.reset(task_entries).await?;
        self.commit_queue.resetter.reset(commit_entries).await?;
        self.cleanup_queue.resetter.reset(cleanup_entries).await?;
        Ok(())
    }
}

impl ResidentQueue {
    /// Creates a resident queue and its three interfaces.
    fn new(capacity: usize) -> Self {
        let inner = Arc::new(ResidentQueueInner {
            state: Mutex::new(ResidentQueueState::new(capacity)),
            receiver_notify: Notify::new(),
            writer_notify: Notify::new(),
        });
        Self {
            writer: ResidentQueueWriter {
                inner: inner.clone(),
            },
            resetter: ResidentQueueResetter {
                inner: inner.clone(),
            },
            receiver: ResidentQueueReceiverHandleInner { inner },
        }
    }
}

impl ResidentQueueWriter {
    /// Writes one task-ready batch into the resident queue in input order.
    async fn write_task_batch(&self, task_batch: TaskReadyBatch) {
        for task_index in task_batch.task_indices {
            self.write_entry(ReadyQueueEntry {
                resource_group_id: task_batch.resource_group_id,
                job_id: task_batch.job_id,
                task_id: TaskId::Index(task_index),
            })
            .await;
        }
    }

    /// Writes one ready entry into the resident queue, waiting for space when necessary.
    async fn write_entry(&self, ready_entry: ReadyQueueEntry) {
        loop {
            let writer_notified = self.inner.writer_notify.notified();
            let enqueue_result = {
                let mut state = self.inner.state.lock().await;
                state.try_push(ready_entry)
            };

            match enqueue_result {
                EnqueueResult::Enqueued => {
                    self.inner.receiver_notify.notify_one();
                    return;
                }
                EnqueueResult::Duplicate => {
                    return;
                }
                EnqueueResult::Full => {
                    writer_notified.await;
                }
            }
        }
    }
}

impl ResidentQueueResetter {
    /// Replaces resident contents from a rebuilt snapshot.
    async fn reset<I>(&self, entries: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = ReadyQueueEntry>, {
        let has_entries = {
            let mut state = self.inner.state.lock().await;
            state.clear();
            for ready_entry in entries {
                state.push_for_rebuild(ready_entry)?;
            }
            !state.is_empty()
        };

        if has_entries {
            self.inner.receiver_notify.notify_waiters();
        }
        self.inner.writer_notify.notify_waiters();
        Ok(())
    }
}

impl ResidentQueueReceiverHandleInner {
    /// Receives up to `max_items` entries, blocking until resident data is available.
    async fn recv(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        if max_items == 0 {
            return Ok(Vec::new());
        }

        loop {
            let receiver_notified = self.inner.receiver_notify.notified();
            let ready_entries = {
                let mut state = self.inner.state.lock().await;
                if state.is_empty() {
                    None
                } else {
                    Some(state.pop_bulk(max_items)?)
                }
            };

            if let Some(ready_entries) = ready_entries {
                self.inner.writer_notify.notify_one();
                return Ok(ready_entries);
            }

            receiver_notified.await;
        }
    }

    /// Returns the number of resident entries.
    async fn len(&self) -> usize {
        self.inner.state.lock().await.len()
    }
}

impl ResidentQueueState {
    /// Creates an empty resident deduplicating queue.
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: VecDeque::with_capacity(capacity),
            entry_set: HashSet::with_capacity(capacity),
        }
    }

    /// Attempts to append an entry to the resident queue.
    fn try_push(&mut self, ready_entry: ReadyQueueEntry) -> EnqueueResult {
        if self.entry_set.contains(&ready_entry) {
            return EnqueueResult::Duplicate;
        }
        if self.entries.len() >= self.capacity {
            return EnqueueResult::Full;
        }

        self.entry_set.insert(ready_entry);
        self.entries.push_back(ready_entry);
        EnqueueResult::Enqueued
    }

    /// Appends an entry during rebuild.
    fn push_for_rebuild(&mut self, ready_entry: ReadyQueueEntry) -> Result<(), InternalError> {
        match self.try_push(ready_entry) {
            EnqueueResult::Enqueued | EnqueueResult::Duplicate => Ok(()),
            EnqueueResult::Full => Err(InternalError::ReadyQueueSendFailure(format!(
                "ready queue rebuild exceeds capacity {}",
                self.capacity
            ))),
        }
    }

    /// Pops up to `max_items` entries from the resident queue.
    fn pop_bulk(&mut self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        let num_entries = max_items.min(self.entries.len());
        let mut ready_entries = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            let Some(ready_entry) = self.entries.pop_front() else {
                return Err(InternalError::ReadyQueueSendFailure(
                    "resident queue is corrupted".to_owned(),
                ));
            };
            self.entry_set.remove(&ready_entry);
            ready_entries.push(ready_entry);
        }
        Ok(ready_entries)
    }

    /// Clears the resident queue and its dedup bookkeeping.
    fn clear(&mut self) {
        self.entries.clear();
        self.entry_set.clear();
    }

    /// Returns the number of resident entries.
    fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the resident queue is empty.
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    fn test_queue() -> (
        ResourceGroupId,
        ReadyQueueSenderHandle,
        ReadyQueueReceiverHandle,
        ReadyQueue,
    ) {
        let resource_group_id = ResourceGroupId::default();
        let ready_queue = ReadyQueue::create(
            ReadyQueueConfig {
                ingress_capacity: 8,
                task_ready_capacity: 8,
                commit_ready_capacity: 4,
                cleanup_ready_capacity: 4,
            },
            &tokio::runtime::Handle::current(),
        );
        let sender = ready_queue.sender();
        let receiver = ready_queue.receiver();
        (resource_group_id, sender, receiver, ready_queue)
    }

    fn ready_entry(
        resource_group_id: ResourceGroupId,
        job_id: JobId,
        task_id: TaskId,
    ) -> ReadyQueueEntry {
        ReadyQueueEntry {
            resource_group_id,
            job_id,
            task_id,
        }
    }

    async fn wait_for_task_resident_len(receiver: &ReadyQueueReceiverHandle, expected_len: usize) {
        timeout(Duration::from_millis(100), async {
            loop {
                if receiver.num_tasks().await == expected_len {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("resident task queue should reach expected length");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn task_ready_batches_are_flattened_by_queue() {
        let (resource_group_id, sender, receiver, _) = test_queue();
        let job_id = JobId::default();

        sender
            .send_task_ready(resource_group_id, job_id, vec![1, 2, 3])
            .await
            .expect("send should succeed");
        wait_for_task_resident_len(&receiver, 3).await;

        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![
                ready_entry(resource_group_id, job_id, TaskId::Index(1)),
                ready_entry(resource_group_id, job_id, TaskId::Index(2)),
                ready_entry(resource_group_id, job_id, TaskId::Index(3)),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[should_panic(expected = "ready queue ingress_capacity must be > 0")]
    async fn create_rejects_zero_capacity_config() {
        let _ = ReadyQueue::create(
            ReadyQueueConfig {
                ingress_capacity: 0,
                task_ready_capacity: 1,
                commit_ready_capacity: 1,
                cleanup_ready_capacity: 1,
            },
            &tokio::runtime::Handle::current(),
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn receiver_blocks_until_flusher_writes_task_batch() {
        let (resource_group_id, sender, receiver, _) = test_queue();
        let job_id = JobId::default();
        let receiver_clone = receiver.clone();

        let mut recv_handle = tokio::spawn(async move {
            receiver_clone
                .recv_tasks(1)
                .await
                .expect("recv should work")
        });
        assert!(
            timeout(Duration::from_millis(50), &mut recv_handle)
                .await
                .is_err(),
            "empty resident queue should block"
        );

        sender
            .send_task_ready(resource_group_id, job_id, vec![7])
            .await
            .expect("send should succeed");

        assert_eq!(
            recv_handle.await.expect("recv task should not panic"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Index(7))]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_and_cleanup_use_separate_queues() {
        let (resource_group_id, sender, receiver, _) = test_queue();
        let job_id = JobId::default();

        sender
            .send_commit_ready(resource_group_id, job_id)
            .await
            .expect("commit send should succeed");
        sender
            .send_cleanup_ready(resource_group_id, job_id)
            .await
            .expect("cleanup send should succeed");

        assert_eq!(
            receiver
                .recv_commits(1)
                .await
                .expect("commit recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Commit)]
        );
        assert_eq!(
            receiver
                .recv_cleanups(1)
                .await
                .expect("cleanup recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Cleanup)]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resident_duplicates_are_suppressed_until_pop() {
        let (resource_group_id, sender, receiver, _) = test_queue();
        let job_id = JobId::default();

        sender
            .send_task_ready(resource_group_id, job_id, vec![7, 7])
            .await
            .expect("duplicate task send should succeed");

        assert_eq!(
            receiver
                .recv_tasks(10)
                .await
                .expect("recv should succeed")
                .len(),
            1
        );

        sender
            .send_task_ready(resource_group_id, job_id, vec![7])
            .await
            .expect("re-enqueue after pop should succeed");
        assert_eq!(
            receiver
                .recv_tasks(10)
                .await
                .expect("recv should succeed")
                .len(),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn len_tracks_resident_entries() {
        let (resource_group_id, _, receiver, ready_queue) = test_queue();
        let job_id = JobId::default();

        ready_queue
            .rebuild([
                ready_entry(resource_group_id, job_id, TaskId::Index(1)),
                ready_entry(resource_group_id, job_id, TaskId::Index(2)),
                ready_entry(resource_group_id, job_id, TaskId::Commit),
            ])
            .await
            .expect("rebuild should succeed");

        let task_batch = receiver.recv_tasks(1).await.expect("recv should succeed");
        assert_eq!(task_batch.len(), 1);
        assert_eq!(receiver.num_tasks().await, 1);
        assert_eq!(receiver.num_commits().await, 1);
        assert_eq!(receiver.num_all().await, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resetter_handle_rebuilds_resident_state() {
        let (resource_group_id, _, receiver, ready_queue) = test_queue();
        let job_id = JobId::default();

        ready_queue
            .resetter()
            .rebuild([
                ready_entry(resource_group_id, job_id, TaskId::Index(5)),
                ready_entry(resource_group_id, job_id, TaskId::Cleanup),
            ])
            .await
            .expect("rebuild should succeed");

        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Index(5))]
        );
        assert_eq!(
            receiver
                .recv_cleanups(1)
                .await
                .expect("recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Cleanup)]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rebuild_preserves_buffered_ingress_backlog() {
        let ready_queue = ReadyQueue::create(
            ReadyQueueConfig {
                ingress_capacity: 8,
                task_ready_capacity: 1,
                commit_ready_capacity: 4,
                cleanup_ready_capacity: 4,
            },
            &tokio::runtime::Handle::current(),
        );
        let sender = ready_queue.sender();
        let receiver = ready_queue.receiver();
        let resource_group_id = ResourceGroupId::default();
        let buffered_job_id = JobId::default();
        let rebuilt_job_id = JobId::default();

        sender
            .send_task_ready(resource_group_id, buffered_job_id, vec![1, 2])
            .await
            .expect("buffered send should succeed");
        wait_for_task_resident_len(&receiver, 1).await;

        ready_queue
            .rebuild([
                ready_entry(resource_group_id, rebuilt_job_id, TaskId::Index(9)),
                ready_entry(resource_group_id, rebuilt_job_id, TaskId::Cleanup),
            ])
            .await
            .expect("rebuild should succeed");

        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ready_entry(
                resource_group_id,
                rebuilt_job_id,
                TaskId::Index(9)
            )]
        );
        assert_eq!(
            receiver
                .recv_cleanups(10)
                .await
                .expect("recv should succeed"),
            vec![ready_entry(
                resource_group_id,
                rebuilt_job_id,
                TaskId::Cleanup
            )]
        );
        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ready_entry(
                resource_group_id,
                buffered_job_id,
                TaskId::Index(2)
            )]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sender_handles_survive_rebuild() {
        let (resource_group_id, sender, receiver, ready_queue) = test_queue();
        let job_id = JobId::default();

        ready_queue
            .rebuild([])
            .await
            .expect("empty rebuild should succeed");
        sender
            .send_task_ready(resource_group_id, job_id, vec![4])
            .await
            .expect("send after rebuild should succeed");

        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Index(4))]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn oversized_task_batch_completes_without_dropping_suffix() {
        let ready_queue = ReadyQueue::create(
            ReadyQueueConfig {
                ingress_capacity: 2,
                task_ready_capacity: 1,
                commit_ready_capacity: 4,
                cleanup_ready_capacity: 4,
            },
            &tokio::runtime::Handle::current(),
        );
        let sender = ready_queue.sender();
        let receiver = ready_queue.receiver();
        let resource_group_id = ResourceGroupId::default();
        let job_id = JobId::default();

        sender
            .send_task_ready(resource_group_id, job_id, vec![1, 2, 3])
            .await
            .expect("batch send should complete");

        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Index(1))]
        );
        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Index(2))]
        );
        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Index(3))]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rebuild_dedups_duplicate_snapshot_entries() {
        let (resource_group_id, _, receiver, ready_queue) = test_queue();
        let job_id = JobId::default();

        ready_queue
            .rebuild([
                ready_entry(resource_group_id, job_id, TaskId::Index(1)),
                ready_entry(resource_group_id, job_id, TaskId::Index(1)),
                ready_entry(resource_group_id, job_id, TaskId::Commit),
                ready_entry(resource_group_id, job_id, TaskId::Commit),
            ])
            .await
            .expect("rebuild should succeed");

        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Index(1))]
        );
        assert_eq!(
            receiver
                .recv_commits(10)
                .await
                .expect("recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Commit)]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn full_task_ingress_channel_blocks_until_recv_frees_space() {
        let ready_queue = ReadyQueue::create(
            ReadyQueueConfig {
                ingress_capacity: 1,
                task_ready_capacity: 1,
                commit_ready_capacity: 1,
                cleanup_ready_capacity: 1,
            },
            &tokio::runtime::Handle::current(),
        );
        let sender = ready_queue.sender();
        let receiver = ready_queue.receiver();
        let resource_group_id = ResourceGroupId::default();
        let job_id = JobId::default();

        sender
            .send_task_ready(resource_group_id, job_id, vec![1])
            .await
            .expect("first send should succeed");
        wait_for_task_resident_len(&receiver, 1).await;

        sender
            .send_task_ready(resource_group_id, job_id, vec![2])
            .await
            .expect("second send should be accepted");
        sender
            .send_task_ready(resource_group_id, job_id, vec![3])
            .await
            .expect("third send should fill ingress");

        let sender_clone = sender.clone();
        let mut send_handle = tokio::spawn(async move {
            sender_clone
                .send_task_ready(resource_group_id, job_id, vec![4])
                .await
        });
        assert!(
            timeout(Duration::from_millis(50), &mut send_handle)
                .await
                .is_err(),
            "fourth send should block while ingress is full"
        );

        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Index(1))]
        );

        send_handle
            .await
            .expect("send task should not panic")
            .expect("blocked send should complete");

        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Index(2))]
        );
        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Index(3))]
        );
        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ready_entry(resource_group_id, job_id, TaskId::Index(4))]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn full_commit_ingress_channel_blocks_until_recv_frees_space() {
        let ready_queue = ReadyQueue::create(
            ReadyQueueConfig {
                ingress_capacity: 1,
                task_ready_capacity: 1,
                commit_ready_capacity: 1,
                cleanup_ready_capacity: 1,
            },
            &tokio::runtime::Handle::current(),
        );
        let sender = ready_queue.sender();
        let receiver = ready_queue.receiver();
        let resource_group_id = ResourceGroupId::default();
        let first_job_id = JobId::default();
        let second_job_id = JobId::default();
        let third_job_id = JobId::default();
        let fourth_job_id = JobId::default();

        sender
            .send_commit_ready(resource_group_id, first_job_id)
            .await
            .expect("first commit send should succeed");
        timeout(Duration::from_millis(100), async {
            loop {
                if receiver.num_commits().await == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("resident commit queue should fill");

        sender
            .send_commit_ready(resource_group_id, second_job_id)
            .await
            .expect("second commit send should be accepted");
        sender
            .send_commit_ready(resource_group_id, third_job_id)
            .await
            .expect("third commit send should fill ingress");

        let sender_clone = sender.clone();
        let mut send_handle = tokio::spawn(async move {
            sender_clone
                .send_commit_ready(resource_group_id, fourth_job_id)
                .await
        });
        assert!(
            timeout(Duration::from_millis(50), &mut send_handle)
                .await
                .is_err(),
            "fourth commit send should block while ingress is full"
        );

        assert_eq!(
            receiver.recv_commits(1).await.expect("recv should succeed"),
            vec![ready_entry(resource_group_id, first_job_id, TaskId::Commit)]
        );

        send_handle
            .await
            .expect("send task should not panic")
            .expect("blocked send should complete");

        assert_eq!(
            receiver.recv_commits(1).await.expect("recv should succeed"),
            vec![ready_entry(
                resource_group_id,
                second_job_id,
                TaskId::Commit
            )]
        );
        assert_eq!(
            receiver.recv_commits(1).await.expect("recv should succeed"),
            vec![ready_entry(resource_group_id, third_job_id, TaskId::Commit)]
        );
        assert_eq!(
            receiver.recv_commits(1).await.expect("recv should succeed"),
            vec![ready_entry(
                resource_group_id,
                fourth_job_id,
                TaskId::Commit
            )]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn full_cleanup_ingress_channel_blocks_until_recv_frees_space() {
        let ready_queue = ReadyQueue::create(
            ReadyQueueConfig {
                ingress_capacity: 1,
                task_ready_capacity: 1,
                commit_ready_capacity: 1,
                cleanup_ready_capacity: 1,
            },
            &tokio::runtime::Handle::current(),
        );
        let sender = ready_queue.sender();
        let receiver = ready_queue.receiver();
        let resource_group_id = ResourceGroupId::default();
        let first_job_id = JobId::default();
        let second_job_id = JobId::default();
        let third_job_id = JobId::default();
        let fourth_job_id = JobId::default();

        sender
            .send_cleanup_ready(resource_group_id, first_job_id)
            .await
            .expect("first cleanup send should succeed");
        timeout(Duration::from_millis(100), async {
            loop {
                if receiver.num_cleanups().await == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("resident cleanup queue should fill");

        sender
            .send_cleanup_ready(resource_group_id, second_job_id)
            .await
            .expect("second cleanup send should be accepted");
        sender
            .send_cleanup_ready(resource_group_id, third_job_id)
            .await
            .expect("third cleanup send should fill ingress");

        let sender_clone = sender.clone();
        let mut send_handle = tokio::spawn(async move {
            sender_clone
                .send_cleanup_ready(resource_group_id, fourth_job_id)
                .await
        });
        assert!(
            timeout(Duration::from_millis(50), &mut send_handle)
                .await
                .is_err(),
            "fourth cleanup send should block while ingress is full"
        );

        assert_eq!(
            receiver
                .recv_cleanups(1)
                .await
                .expect("recv should succeed"),
            vec![ready_entry(
                resource_group_id,
                first_job_id,
                TaskId::Cleanup
            )]
        );

        send_handle
            .await
            .expect("send task should not panic")
            .expect("blocked send should complete");

        assert_eq!(
            receiver
                .recv_cleanups(1)
                .await
                .expect("recv should succeed"),
            vec![ready_entry(
                resource_group_id,
                second_job_id,
                TaskId::Cleanup
            )]
        );
        assert_eq!(
            receiver
                .recv_cleanups(1)
                .await
                .expect("recv should succeed"),
            vec![ready_entry(
                resource_group_id,
                third_job_id,
                TaskId::Cleanup
            )]
        );
        assert_eq!(
            receiver
                .recv_cleanups(1)
                .await
                .expect("recv should succeed"),
            vec![ready_entry(
                resource_group_id,
                fourth_job_id,
                TaskId::Cleanup
            )]
        );
    }
}
