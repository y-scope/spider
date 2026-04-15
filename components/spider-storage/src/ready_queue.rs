use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use spider_core::{task::TaskIndex, types::id::JobId};
use tokio::sync::{Mutex, mpsc};

use crate::cache::{TaskId, error::InternalError};

/// Default resident ready-task queue capacity.
const DEFAULT_TASK_READY_CAPACITY: usize = 65_536;

/// Default resident commit-task queue capacity.
const DEFAULT_COMMIT_READY_CAPACITY: usize = 1024;

/// Default resident cleanup-task queue capacity.
const DEFAULT_CLEANUP_READY_CAPACITY: usize = 1024;

/// Default ingress queue capacity.
const DEFAULT_INGRESS_CAPACITY: usize = DEFAULT_TASK_READY_CAPACITY;

/// A ready queue entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReadyQueueEntry {
    pub job_id: JobId,
    pub task_id: TaskId,
}

/// Configuration of a ready queue.
#[derive(Debug, Clone, Copy)]
pub struct ReadyQueueConfig {
    pub ingress_capacity: usize,
    pub task_ready_capacity: usize,
    pub commit_ready_capacity: usize,
    pub cleanup_ready_capacity: usize,
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

    /// Enqueues a signal indicating that the commit task of the given job is ready to be
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
    /// A batch of up to `max_items` regular task entries. This function does not block.
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
    /// A batch of up to `max_items` commit entries. This function does not block.
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
    /// A batch of up to `max_items` cleanup entries. This function does not block.
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
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the ready queue is corrupted or unavailable.
    async fn num_tasks(&self) -> Result<usize, InternalError>;

    /// # Returns
    ///
    /// The number of resident commit entries.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the ready queue is corrupted or unavailable.
    async fn num_commits(&self) -> Result<usize, InternalError>;

    /// # Returns
    ///
    /// The number of resident cleanup entries.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the ready queue is corrupted or unavailable.
    async fn num_cleanups(&self) -> Result<usize, InternalError>;

    /// # Returns
    ///
    /// The total number of resident entries across all queues.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the ready queue is corrupted or unavailable.
    async fn total_len(&self) -> Result<usize, InternalError>;
}

/// An in-memory ready queue with explicit periodic flattening.
#[derive(Clone)]
pub struct ReadyQueue {
    inner: Arc<ReadyQueueInner>,
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

#[async_trait]
impl ReadyQueueSender for ReadyQueueSenderHandle {
    async fn send_task_ready(
        &self,
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError> {
        if task_indices.is_empty() {
            return Ok(());
        }

        self.inner.send_task_ready_batch(job_id, task_indices)
    }

    async fn send_commit_ready(&self, job_id: JobId) -> Result<(), InternalError> {
        self.inner.send_termination_ready(ReadyQueueEntry {
            job_id,
            task_id: TaskId::Commit,
        })
    }

    async fn send_cleanup_ready(&self, job_id: JobId) -> Result<(), InternalError> {
        self.inner.send_termination_ready(ReadyQueueEntry {
            job_id,
            task_id: TaskId::Cleanup,
        })
    }
}

impl ReadyQueue {
    /// Creates a ready queue with the given configuration.
    ///
    /// # Returns
    ///
    /// The created ready queue.
    #[must_use]
    pub fn create(config: ReadyQueueConfig) -> Self {
        let (ingress_senders, ingress_receivers) =
            IngressSenderSet::create(config.ingress_capacity);
        Self {
            inner: Arc::new(ReadyQueueInner {
                ingress_senders,
                state: Mutex::new(ReadyQueueState::new(config, ingress_receivers)),
            }),
        }
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

    /// Drains all buffered ingress messages into the resident queues.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the ready queue is corrupted or unavailable.
    pub async fn flatten(&self) -> Result<(), InternalError> {
        self.inner.flatten().await
    }

    /// Rebuilds the resident queues from a fresh snapshot of ready entries.
    ///
    /// This operation clears and repopulates the resident queues.
    ///
    /// # Errors
    ///
    /// Returns [`InternalError::ReadyQueueSendFailure`] if the rebuilt snapshot exceeds resident
    /// queue capacity.
    pub async fn rebuild<I>(&self, entries: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = ReadyQueueEntry>, {
        self.inner.rebuild(entries).await
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

    async fn num_tasks(&self) -> Result<usize, InternalError> {
        self.inner.num_tasks().await
    }

    async fn num_commits(&self) -> Result<usize, InternalError> {
        self.inner.num_commits().await
    }

    async fn num_cleanups(&self) -> Result<usize, InternalError> {
        self.inner.num_cleanups().await
    }

    async fn total_len(&self) -> Result<usize, InternalError> {
        Ok(self.num_tasks().await? + self.num_commits().await? + self.num_cleanups().await?)
    }
}

struct ReadyQueueInner {
    ingress_senders: IngressSenderSet,
    state: Mutex<ReadyQueueState>,
}

struct ReadyQueueState {
    ingress_receivers: IngressReceiverSet,
    task_queue: ResidentQueue,
    commit_queue: ResidentQueue,
    cleanup_queue: ResidentQueue,
}

#[derive(Clone)]
struct IngressSenderSet {
    tasks: mpsc::Sender<ReadyQueueEntry>,
    commits: mpsc::Sender<ReadyQueueEntry>,
    cleanups: mpsc::Sender<ReadyQueueEntry>,
}

struct IngressReceiverSet {
    tasks: mpsc::Receiver<ReadyQueueEntry>,
    commits: mpsc::Receiver<ReadyQueueEntry>,
    cleanups: mpsc::Receiver<ReadyQueueEntry>,
}

struct ResidentQueue {
    capacity: usize,
    entries: VecDeque<ReadyQueueEntry>,
    entry_set: HashSet<ReadyQueueEntry>,
}

enum EnqueueResult {
    Enqueued,
    Duplicate,
    Full,
}

#[derive(Clone, Copy, strum::Display)]
enum IngressQueue {
    Commit,
    Cleanup,
}

impl ReadyQueueInner {
    async fn lock_state(&self) -> tokio::sync::MutexGuard<'_, ReadyQueueState> {
        self.state.lock().await
    }

    fn send_task_ready_batch(
        &self,
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError> {
        let num_tasks = task_indices.len();
        let mut num_accepted_tasks = 0usize;

        for task_index in task_indices {
            let ready_entry = ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(task_index),
            };

            match self.ingress_senders.tasks.try_send(ready_entry) {
                Ok(()) => {
                    num_accepted_tasks += 1;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    return Err(InternalError::ReadyQueueSendFailure(format!(
                        "task ready queue ingress is full after accepting {num_accepted_tasks} of \
                         {num_tasks} tasks"
                    )));
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(InternalError::ReadyQueueSendFailure(
                        "task ready queue ingress is closed".to_owned(),
                    ));
                }
            }
        }

        Ok(())
    }

    fn send_termination_ready(&self, ready_entry: ReadyQueueEntry) -> Result<(), InternalError> {
        let (ingress_queue, ingress_sender) = match ready_entry.task_id {
            TaskId::Index(_) => {
                return Err(InternalError::ReadyQueueSendFailure(
                    "regular tasks must be sent through send_task_ready".to_owned(),
                ));
            }
            TaskId::Commit => (IngressQueue::Commit, &self.ingress_senders.commits),
            TaskId::Cleanup => (IngressQueue::Cleanup, &self.ingress_senders.cleanups),
        };

        Self::send_ingress_message(ingress_queue, ready_entry, ingress_sender)
    }

    fn send_ingress_message<Message>(
        ingress_queue: IngressQueue,
        message: Message,
        ingress_sender: &mpsc::Sender<Message>,
    ) -> Result<(), InternalError>
    where
        Message: Send, {
        match ingress_sender.try_send(message) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(InternalError::ReadyQueueSendFailure(
                format!("{ingress_queue} ready queue ingress is full"),
            )),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(InternalError::ReadyQueueSendFailure(
                format!("{ingress_queue} ready queue ingress is closed"),
            )),
        }
    }

    async fn flatten(&self) -> Result<(), InternalError> {
        self.lock_state().await.flatten();
        Ok(())
    }

    async fn recv_tasks(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        if max_items == 0 {
            return Ok(Vec::new());
        }

        self.lock_state().await.task_queue.pop_bulk(max_items)
    }

    async fn recv_commits(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        if max_items == 0 {
            return Ok(Vec::new());
        }

        self.lock_state().await.commit_queue.pop_bulk(max_items)
    }

    async fn recv_cleanups(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        if max_items == 0 {
            return Ok(Vec::new());
        }

        self.lock_state().await.cleanup_queue.pop_bulk(max_items)
    }

    async fn num_tasks(&self) -> Result<usize, InternalError> {
        Ok(self.lock_state().await.task_queue.len())
    }

    async fn num_commits(&self) -> Result<usize, InternalError> {
        Ok(self.lock_state().await.commit_queue.len())
    }

    async fn num_cleanups(&self) -> Result<usize, InternalError> {
        Ok(self.lock_state().await.cleanup_queue.len())
    }

    async fn rebuild<I>(&self, entries: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = ReadyQueueEntry>, {
        self.lock_state().await.rebuild(entries)?;
        Ok(())
    }
}

impl ReadyQueueState {
    fn new(config: ReadyQueueConfig, ingress_receivers: IngressReceiverSet) -> Self {
        Self {
            ingress_receivers,
            task_queue: ResidentQueue::new(config.task_ready_capacity),
            commit_queue: ResidentQueue::new(config.commit_ready_capacity),
            cleanup_queue: ResidentQueue::new(config.cleanup_ready_capacity),
        }
    }

    fn rebuild<I>(&mut self, entries: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = ReadyQueueEntry>, {
        self.task_queue.clear();
        self.commit_queue.clear();
        self.cleanup_queue.clear();

        for ready_entry in entries {
            let queue = match ready_entry.task_id {
                TaskId::Index(_) => &mut self.task_queue,
                TaskId::Commit => &mut self.commit_queue,
                TaskId::Cleanup => &mut self.cleanup_queue,
            };
            queue.push_for_rebuild(ready_entry)?;
        }

        Ok(())
    }

    fn flatten(&mut self) {
        self.drain_task_ingress();
        self.commit_queue
            .drain_ingress(&mut self.ingress_receivers.commits);
        self.cleanup_queue
            .drain_ingress(&mut self.ingress_receivers.cleanups);
    }

    fn drain_task_ingress(&mut self) {
        while !self.task_queue.is_full() {
            match self.ingress_receivers.tasks.try_recv() {
                Ok(ready_entry) => {
                    let _ = self.task_queue.try_push(ready_entry);
                }
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }
    }
}

impl IngressSenderSet {
    fn create(ingress_capacity: usize) -> (Self, IngressReceiverSet) {
        let (task_sender, task_receiver) = mpsc::channel(ingress_capacity);
        let (commit_sender, commit_receiver) = mpsc::channel(ingress_capacity);
        let (cleanup_sender, cleanup_receiver) = mpsc::channel(ingress_capacity);
        (
            Self {
                tasks: task_sender,
                commits: commit_sender,
                cleanups: cleanup_sender,
            },
            IngressReceiverSet {
                tasks: task_receiver,
                commits: commit_receiver,
                cleanups: cleanup_receiver,
            },
        )
    }
}

impl ResidentQueue {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: VecDeque::new(),
            entry_set: HashSet::new(),
        }
    }

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

    fn push_for_rebuild(&mut self, ready_entry: ReadyQueueEntry) -> Result<(), InternalError> {
        match self.try_push(ready_entry) {
            EnqueueResult::Enqueued | EnqueueResult::Duplicate => Ok(()),
            EnqueueResult::Full => Err(InternalError::ReadyQueueSendFailure(format!(
                "ready queue rebuild exceeds capacity {}",
                self.capacity
            ))),
        }
    }

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

    fn clear(&mut self) {
        self.entries.clear();
        self.entry_set.clear();
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_full(&self) -> bool {
        self.entries.len() >= self.capacity
    }

    fn drain_ingress(&mut self, ingress_receiver: &mut mpsc::Receiver<ReadyQueueEntry>) {
        while !self.is_full() {
            match ingress_receiver.try_recv() {
                Ok(ready_entry) => {
                    let _ = self.try_push(ready_entry);
                }
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_queue() -> (ReadyQueueSenderHandle, ReadyQueueReceiverHandle, ReadyQueue) {
        let ready_queue = ReadyQueue::create(ReadyQueueConfig {
            ingress_capacity: 8,
            task_ready_capacity: 8,
            commit_ready_capacity: 4,
            cleanup_ready_capacity: 4,
        });
        let sender = ready_queue.sender();
        let receiver = ready_queue.receiver();
        (sender, receiver, ready_queue)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn task_ready_batches_are_flattened_by_queue() {
        let (sender, receiver, ready_queue) = test_queue();
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![1, 2, 3])
            .await
            .expect("send should succeed");

        ready_queue.flatten().await.expect("flatten should succeed");

        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![
                ReadyQueueEntry {
                    job_id,
                    task_id: TaskId::Index(1),
                },
                ReadyQueueEntry {
                    job_id,
                    task_id: TaskId::Index(2),
                },
                ReadyQueueEntry {
                    job_id,
                    task_id: TaskId::Index(3),
                },
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_and_cleanup_use_separate_queues() {
        let (sender, receiver, ready_queue) = test_queue();
        let job_id = JobId::default();

        sender
            .send_commit_ready(job_id)
            .await
            .expect("commit send should succeed");
        sender
            .send_cleanup_ready(job_id)
            .await
            .expect("cleanup send should succeed");

        ready_queue.flatten().await.expect("flatten should succeed");

        assert_eq!(
            receiver
                .recv_commits(1)
                .await
                .expect("commit recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Commit,
            }]
        );
        assert_eq!(
            receiver
                .recv_cleanups(1)
                .await
                .expect("cleanup recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Cleanup,
            }]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resident_duplicates_are_suppressed_until_pop() {
        let (sender, receiver, ready_queue) = test_queue();
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![7, 7])
            .await
            .expect("duplicate task send should succeed");
        ready_queue.flatten().await.expect("flatten should succeed");

        assert_eq!(
            receiver
                .recv_tasks(10)
                .await
                .expect("recv should succeed")
                .len(),
            1
        );
        assert!(
            receiver
                .recv_tasks(10)
                .await
                .expect("recv should succeed")
                .is_empty()
        );

        sender
            .send_task_ready(job_id, vec![7])
            .await
            .expect("re-enqueue after pop should succeed");
        ready_queue.flatten().await.expect("flatten should succeed");
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
        let (_, receiver, ready_queue) = test_queue();
        let job_id = JobId::default();

        ready_queue
            .rebuild([
                ReadyQueueEntry {
                    job_id,
                    task_id: TaskId::Index(1),
                },
                ReadyQueueEntry {
                    job_id,
                    task_id: TaskId::Index(2),
                },
                ReadyQueueEntry {
                    job_id,
                    task_id: TaskId::Commit,
                },
            ])
            .await
            .expect("rebuild should succeed");

        let task_batch = receiver.recv_tasks(1).await.expect("recv should succeed");
        assert_eq!(task_batch.len(), 1);
        assert_eq!(receiver.num_tasks().await.expect("count should succeed"), 1);
        assert_eq!(
            receiver.num_commits().await.expect("count should succeed"),
            1
        );
        assert_eq!(receiver.total_len().await.expect("count should succeed"), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rebuild_preserves_buffered_ingress_backlog() {
        let (sender, receiver, ready_queue) = test_queue();
        let buffered_job_id = JobId::default();
        let rebuilt_job_id = JobId::default();

        sender
            .send_task_ready(buffered_job_id, vec![1, 2])
            .await
            .expect("buffered send should succeed");

        ready_queue
            .rebuild([
                ReadyQueueEntry {
                    job_id: rebuilt_job_id,
                    task_id: TaskId::Index(9),
                },
                ReadyQueueEntry {
                    job_id: rebuilt_job_id,
                    task_id: TaskId::Cleanup,
                },
            ])
            .await
            .expect("rebuild should succeed");

        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id: rebuilt_job_id,
                task_id: TaskId::Index(9),
            }]
        );
        assert_eq!(
            receiver
                .recv_cleanups(10)
                .await
                .expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id: rebuilt_job_id,
                task_id: TaskId::Cleanup,
            }]
        );

        ready_queue.flatten().await.expect("flatten should succeed");

        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![
                ReadyQueueEntry {
                    job_id: buffered_job_id,
                    task_id: TaskId::Index(1),
                },
                ReadyQueueEntry {
                    job_id: buffered_job_id,
                    task_id: TaskId::Index(2),
                },
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sender_handles_survive_rebuild() {
        let (sender, receiver, ready_queue) = test_queue();
        let job_id = JobId::default();

        ready_queue
            .rebuild([])
            .await
            .expect("empty rebuild should succeed");
        sender
            .send_task_ready(job_id, vec![4])
            .await
            .expect("send after rebuild should succeed");

        ready_queue.flatten().await.expect("flatten should succeed");

        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(4),
            }]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flatten_leaves_task_channel_backlog_when_resident_queue_is_full() {
        let ready_queue = ReadyQueue::create(ReadyQueueConfig {
            ingress_capacity: 8,
            task_ready_capacity: 1,
            commit_ready_capacity: 4,
            cleanup_ready_capacity: 4,
        });
        let sender = ready_queue.sender();
        let receiver = ready_queue.receiver();
        let buffered_job_id = JobId::default();

        sender
            .send_task_ready(buffered_job_id, vec![1, 2, 3])
            .await
            .expect("buffered send should succeed");
        ready_queue.flatten().await.expect("flatten should succeed");

        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id: buffered_job_id,
                task_id: TaskId::Index(1),
            }]
        );

        ready_queue.flatten().await.expect("flatten should succeed");
        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id: buffered_job_id,
                task_id: TaskId::Index(2),
            }]
        );

        ready_queue.flatten().await.expect("flatten should succeed");
        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id: buffered_job_id,
                task_id: TaskId::Index(3),
            }]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn task_batch_send_accepts_prefix_and_drops_overflow_suffix() {
        let ready_queue = ReadyQueue::create(ReadyQueueConfig {
            ingress_capacity: 2,
            task_ready_capacity: 8,
            commit_ready_capacity: 4,
            cleanup_ready_capacity: 4,
        });
        let sender = ready_queue.sender();
        let receiver = ready_queue.receiver();
        let job_id = JobId::default();

        let send_error = sender
            .send_task_ready(job_id, vec![1, 2, 3])
            .await
            .expect_err("overflowing task batch should fail");
        assert!(matches!(
            send_error,
            InternalError::ReadyQueueSendFailure(message)
                if message.contains("accepting 2 of 3 tasks")
        ));

        ready_queue.flatten().await.expect("flatten should succeed");
        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![
                ReadyQueueEntry {
                    job_id,
                    task_id: TaskId::Index(1),
                },
                ReadyQueueEntry {
                    job_id,
                    task_id: TaskId::Index(2),
                },
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rebuild_dedups_duplicate_snapshot_entries() {
        let (_, receiver, ready_queue) = test_queue();
        let job_id = JobId::default();

        ready_queue
            .rebuild([
                ReadyQueueEntry {
                    job_id,
                    task_id: TaskId::Index(1),
                },
                ReadyQueueEntry {
                    job_id,
                    task_id: TaskId::Index(1),
                },
                ReadyQueueEntry {
                    job_id,
                    task_id: TaskId::Commit,
                },
                ReadyQueueEntry {
                    job_id,
                    task_id: TaskId::Commit,
                },
            ])
            .await
            .expect("rebuild should succeed");

        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(1),
            }]
        );
        assert_eq!(
            receiver
                .recv_commits(10)
                .await
                .expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Commit,
            }]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn full_ingress_channel_fails_send_immediately() {
        let ready_queue = ReadyQueue::create(ReadyQueueConfig {
            ingress_capacity: 1,
            task_ready_capacity: 1,
            commit_ready_capacity: 1,
            cleanup_ready_capacity: 1,
        });
        let sender = ready_queue.sender();
        let receiver = ready_queue.receiver();
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![1])
            .await
            .expect("first send should succeed");
        ready_queue.flatten().await.expect("flatten should succeed");

        sender
            .send_task_ready(job_id, vec![2])
            .await
            .expect("second send should fill ingress");

        let send_error = sender
            .send_task_ready(job_id, vec![3])
            .await
            .expect_err("third send should fail immediately when ingress is full");
        assert!(matches!(
            send_error,
            InternalError::ReadyQueueSendFailure(message)
                if message.contains("ingress is full")
        ));

        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(1),
            }]
        );
        ready_queue.flatten().await.expect("flatten should succeed");
        assert_eq!(receiver.num_tasks().await.expect("count should succeed"), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn full_commit_ingress_channel_fails_send_immediately() {
        let ready_queue = ReadyQueue::create(ReadyQueueConfig {
            ingress_capacity: 1,
            task_ready_capacity: 1,
            commit_ready_capacity: 1,
            cleanup_ready_capacity: 1,
        });
        let sender = ready_queue.sender();
        let receiver = ready_queue.receiver();
        let job_id = JobId::default();

        sender
            .send_commit_ready(job_id)
            .await
            .expect("first commit send should succeed");
        ready_queue.flatten().await.expect("flatten should succeed");

        sender
            .send_commit_ready(job_id)
            .await
            .expect("second commit send should fill ingress");

        let send_error = sender
            .send_commit_ready(job_id)
            .await
            .expect_err("third commit send should fail immediately when ingress is full");
        assert!(matches!(
            send_error,
            InternalError::ReadyQueueSendFailure(message)
                if message.contains("ingress is full")
        ));

        assert_eq!(
            receiver.recv_commits(1).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Commit,
            }]
        );
        ready_queue.flatten().await.expect("flatten should succeed");
        assert_eq!(
            receiver.num_commits().await.expect("count should succeed"),
            1
        );
    }
}
