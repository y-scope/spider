use std::{
    collections::{HashSet, VecDeque},
    future::ready,
    sync::{Arc, Mutex, RwLock},
};

use async_trait::async_trait;
use spider_core::{task::TaskIndex, types::id::JobId};
use tokio::sync::mpsc;

use crate::cache::{TaskId, error::InternalError};

/// Default ingress queue capacity.
const DEFAULT_INGRESS_CAPACITY: usize = 1024;

/// Default resident ready-task queue capacity.
const DEFAULT_TASK_READY_CAPACITY: usize = 65_536;

/// Default resident commit-task queue capacity.
const DEFAULT_COMMIT_READY_CAPACITY: usize = 1024;

/// Default resident cleanup-task queue capacity.
const DEFAULT_CLEANUP_READY_CAPACITY: usize = 1024;

/// An entry yielded by the ready queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReadyQueueEntry {
    pub job_id: JobId,
    pub task_id: TaskId,
}

/// Configuration for the ready queue.
#[derive(Debug, Clone, Copy)]
pub struct ReadyQueueConfig {
    pub ingress_capacity: usize,
    pub task_ready_capacity: usize,
    pub commit_ready_capacity: usize,
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

/// Production ready queue with explicit periodic flattening.
#[derive(Clone)]
pub struct ReadyQueue {
    shared: Arc<ReadyQueueShared>,
}

impl ReadyQueue {
    /// Creates a new ready queue.
    #[must_use]
    pub fn create(config: ReadyQueueConfig) -> Self {
        let (senders, receivers) = create_ingress_channels(config.ingress_capacity);
        Self {
            shared: Arc::new(ReadyQueueShared {
                config,
                ingress_senders: RwLock::new(senders),
                state: Mutex::new(ReadyQueueState {
                    ingress_receivers: receivers,
                    pending_task_entries: VecDeque::new(),
                    tasks: OrderedReadyQueue::new(config.task_ready_capacity),
                    commits: OrderedReadyQueue::new(config.commit_ready_capacity),
                    cleanups: OrderedReadyQueue::new(config.cleanup_ready_capacity),
                }),
            }),
        }
    }

    #[must_use]
    pub fn sender(&self) -> ReadyQueueSenderHandle {
        ReadyQueueSenderHandle {
            shared: self.shared.clone(),
        }
    }

    /// Drains all currently buffered ingress messages into the resident queues.
    ///
    /// Each resident queue drains as many entries as possible from its corresponding ingress
    /// channel while preserving insertion order and resident-only deduplication.
    pub fn flatten(&self) {
        self.shared.flatten();
    }

    /// Receives up to `max_items` regular task entries without blocking.
    #[must_use]
    pub fn recv_tasks(&self, max_items: usize) -> Vec<ReadyQueueEntry> {
        self.shared.recv_tasks(max_items)
    }

    /// Receives up to `max_items` commit entries without blocking.
    #[must_use]
    pub fn recv_commits(&self, max_items: usize) -> Vec<ReadyQueueEntry> {
        self.shared.recv_commits(max_items)
    }

    /// Receives up to `max_items` cleanup entries without blocking.
    #[must_use]
    pub fn recv_cleanups(&self, max_items: usize) -> Vec<ReadyQueueEntry> {
        self.shared.recv_cleanups(max_items)
    }

    /// Returns the number of resident regular task entries.
    #[must_use]
    pub fn num_tasks(&self) -> usize {
        self.shared.num_tasks()
    }

    /// Returns the number of resident commit entries.
    #[must_use]
    pub fn num_commits(&self) -> usize {
        self.shared.num_commits()
    }

    /// Returns the number of resident cleanup entries.
    #[must_use]
    pub fn num_cleanups(&self) -> usize {
        self.shared.num_cleanups()
    }

    /// Returns the total number of resident entries across all three queues.
    #[must_use]
    pub fn total_len(&self) -> usize {
        self.num_tasks() + self.num_commits() + self.num_cleanups()
    }

    /// Rebuilds the resident queues from a fresh snapshot of ready entries.
    ///
    /// The caller is responsible for coordinating with higher-level state so the snapshot is
    /// authoritative for the rebuild window. Rebuild swaps ingress channels before replacing the
    /// resident state, which drops all stale buffered ingress from the previous channels.
    ///
    /// # Errors
    ///
    /// Returns [`InternalError::ReadyQueueSendFailure`] if the rebuilt snapshot exceeds resident
    /// queue capacity.
    pub fn rebuild<I>(&self, entries: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = ReadyQueueEntry>, {
        self.shared.rebuild(entries)
    }
}

/// Cloneable sender handle used by the cache/JCB layer.
#[derive(Clone)]
pub struct ReadyQueueSenderHandle {
    shared: Arc<ReadyQueueShared>,
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
        ready(self.shared.send_task_batch(TaskIngressMessage {
            job_id,
            task_indices,
        }))
        .await
    }

    async fn send_commit_ready(&self, job_id: JobId) -> Result<(), InternalError> {
        ready(self.shared.send_entry(ReadyQueueEntry {
            job_id,
            task_id: TaskId::Commit,
        }))
        .await
    }

    async fn send_cleanup_ready(&self, job_id: JobId) -> Result<(), InternalError> {
        ready(self.shared.send_entry(ReadyQueueEntry {
            job_id,
            task_id: TaskId::Cleanup,
        }))
        .await
    }
}

struct ReadyQueueShared {
    config: ReadyQueueConfig,
    ingress_senders: RwLock<IngressSenders>,
    state: Mutex<ReadyQueueState>,
}

impl ReadyQueueShared {
    fn send_task_batch(&self, message: TaskIngressMessage) -> Result<(), InternalError> {
        self.send_with_retry("task", message, |senders| senders.tasks.clone())
    }

    fn send_entry(&self, entry: ReadyQueueEntry) -> Result<(), InternalError> {
        let queue_name = match entry.task_id {
            TaskId::Index(_) => unreachable!("task entries are sent through send_task_batch"),
            TaskId::Commit => "commit",
            TaskId::Cleanup => "cleanup",
        };
        self.send_with_retry(queue_name, entry, |senders| match entry.task_id {
            TaskId::Index(_) => unreachable!("task entries are sent through send_task_batch"),
            TaskId::Commit => senders.commits.clone(),
            TaskId::Cleanup => senders.cleanups.clone(),
        })
    }

    fn send_with_retry<Message, Select>(
        &self,
        queue_name: &'static str,
        mut message: Message,
        select_sender: Select,
    ) -> Result<(), InternalError>
    where
        Message: Send,
        Select: Fn(&IngressSenders) -> mpsc::Sender<Message>, {
        loop {
            let sender = {
                let senders = self
                    .ingress_senders
                    .read()
                    .expect("ready queue sender lock should not poison");
                select_sender(&senders)
            };
            match sender.try_send(message) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(_returned_message)) => {
                    return Err(InternalError::ReadyQueueSendFailure(format!(
                        "{queue_name} ready queue ingress is full"
                    )));
                }
                Err(mpsc::error::TrySendError::Closed(returned_message)) => {
                    message = returned_message;
                }
            }
        }
    }

    fn flatten(&self) {
        self.state
            .lock()
            .expect("ready queue mutex should not poison")
            .flatten();
    }

    fn recv_tasks(&self, max_items: usize) -> Vec<ReadyQueueEntry> {
        if max_items == 0 {
            return Vec::new();
        }
        self.state
            .lock()
            .expect("ready queue mutex should not poison")
            .tasks
            .pop_bulk(max_items)
    }

    fn recv_commits(&self, max_items: usize) -> Vec<ReadyQueueEntry> {
        if max_items == 0 {
            return Vec::new();
        }
        self.state
            .lock()
            .expect("ready queue mutex should not poison")
            .commits
            .pop_bulk(max_items)
    }

    fn recv_cleanups(&self, max_items: usize) -> Vec<ReadyQueueEntry> {
        if max_items == 0 {
            return Vec::new();
        }
        self.state
            .lock()
            .expect("ready queue mutex should not poison")
            .cleanups
            .pop_bulk(max_items)
    }

    fn num_tasks(&self) -> usize {
        self.state
            .lock()
            .expect("ready queue mutex should not poison")
            .tasks
            .len()
    }

    fn num_commits(&self) -> usize {
        self.state
            .lock()
            .expect("ready queue mutex should not poison")
            .commits
            .len()
    }

    fn num_cleanups(&self) -> usize {
        self.state
            .lock()
            .expect("ready queue mutex should not poison")
            .cleanups
            .len()
    }

    fn rebuild<I>(&self, entries: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = ReadyQueueEntry>, {
        let (senders, receivers) = create_ingress_channels(self.config.ingress_capacity);
        let rebuilt_state = ReadyQueueState::from_entries(self.config, receivers, entries)?;

        {
            let mut ingress_senders = self
                .ingress_senders
                .write()
                .expect("ready queue sender lock should not poison");
            *ingress_senders = senders;
        }

        *self
            .state
            .lock()
            .expect("ready queue mutex should not poison") = rebuilt_state;
        Ok(())
    }
}

struct ReadyQueueState {
    ingress_receivers: IngressReceivers,
    pending_task_entries: VecDeque<ReadyQueueEntry>,
    tasks: OrderedReadyQueue,
    commits: OrderedReadyQueue,
    cleanups: OrderedReadyQueue,
}

impl ReadyQueueState {
    fn from_entries<I>(
        config: ReadyQueueConfig,
        ingress_receivers: IngressReceivers,
        entries: I,
    ) -> Result<Self, InternalError>
    where
        I: IntoIterator<Item = ReadyQueueEntry>, {
        let mut state = Self {
            ingress_receivers,
            pending_task_entries: VecDeque::new(),
            tasks: OrderedReadyQueue::new(config.task_ready_capacity),
            commits: OrderedReadyQueue::new(config.commit_ready_capacity),
            cleanups: OrderedReadyQueue::new(config.cleanup_ready_capacity),
        };

        for entry in entries {
            let queue = match entry.task_id {
                TaskId::Index(_) => &mut state.tasks,
                TaskId::Commit => &mut state.commits,
                TaskId::Cleanup => &mut state.cleanups,
            };
            queue.push_for_rebuild(entry)?;
        }

        Ok(state)
    }

    fn flatten(&mut self) {
        self.flatten_tasks();
        {
            let commit_receiver = &mut self.ingress_receivers.commits;
            let commits = &mut self.commits;
            drain_ingress_queue(commit_receiver, commits);
        }
        {
            let cleanup_receiver = &mut self.ingress_receivers.cleanups;
            let cleanups = &mut self.cleanups;
            drain_ingress_queue(cleanup_receiver, cleanups);
        }
    }

    fn flatten_tasks(&mut self) {
        self.drain_pending_tasks();
        while !self.tasks.is_full() {
            match self.ingress_receivers.tasks.try_recv() {
                Ok(message) => self.push_task_batch(message),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }
    }

    fn drain_pending_tasks(&mut self) {
        while !self.tasks.is_full() {
            let Some(entry) = self.pending_task_entries.pop_front() else {
                break;
            };
            match self.tasks.try_push(entry) {
                PushOutcome::Enqueued | PushOutcome::Duplicate => {}
                PushOutcome::Full => {
                    self.pending_task_entries.push_front(entry);
                    break;
                }
            }
        }
    }

    fn push_task_batch(&mut self, message: TaskIngressMessage) {
        let mut task_indices = message.task_indices.into_iter();
        while let Some(task_index) = task_indices.next() {
            let entry = ReadyQueueEntry {
                job_id: message.job_id,
                task_id: TaskId::Index(task_index),
            };
            match self.tasks.try_push(entry) {
                PushOutcome::Enqueued | PushOutcome::Duplicate => {}
                PushOutcome::Full => {
                    self.pending_task_entries.push_back(entry);
                    self.pending_task_entries
                        .extend(task_indices.map(|task_index| ReadyQueueEntry {
                            job_id: message.job_id,
                            task_id: TaskId::Index(task_index),
                        }));
                    break;
                }
            }
        }
    }
}

#[derive(Clone)]
struct IngressSenders {
    tasks: mpsc::Sender<TaskIngressMessage>,
    commits: mpsc::Sender<ReadyQueueEntry>,
    cleanups: mpsc::Sender<ReadyQueueEntry>,
}

struct IngressReceivers {
    tasks: mpsc::Receiver<TaskIngressMessage>,
    commits: mpsc::Receiver<ReadyQueueEntry>,
    cleanups: mpsc::Receiver<ReadyQueueEntry>,
}

struct TaskIngressMessage {
    job_id: JobId,
    task_indices: Vec<TaskIndex>,
}

fn create_ingress_channels(ingress_capacity: usize) -> (IngressSenders, IngressReceivers) {
    let (task_sender, task_receiver) = mpsc::channel(ingress_capacity);
    let (commit_sender, commit_receiver) = mpsc::channel(ingress_capacity);
    let (cleanup_sender, cleanup_receiver) = mpsc::channel(ingress_capacity);
    (
        IngressSenders {
            tasks: task_sender,
            commits: commit_sender,
            cleanups: cleanup_sender,
        },
        IngressReceivers {
            tasks: task_receiver,
            commits: commit_receiver,
            cleanups: cleanup_receiver,
        },
    )
}

fn drain_ingress_queue(
    receiver: &mut mpsc::Receiver<ReadyQueueEntry>,
    resident_queue: &mut OrderedReadyQueue,
) {
    while !resident_queue.is_full() {
        match receiver.try_recv() {
            Ok(entry) => {
                let _ = resident_queue.try_push(entry);
            }
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
}

struct OrderedReadyQueue {
    capacity: usize,
    queue: VecDeque<ReadyQueueEntry>,
    dedup: HashSet<ReadyQueueEntry>,
}

impl OrderedReadyQueue {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            queue: VecDeque::new(),
            dedup: HashSet::new(),
        }
    }

    fn try_push(&mut self, entry: ReadyQueueEntry) -> PushOutcome {
        if self.dedup.contains(&entry) {
            return PushOutcome::Duplicate;
        }
        if self.queue.len() >= self.capacity {
            return PushOutcome::Full;
        }

        self.dedup.insert(entry);
        self.queue.push_back(entry);
        PushOutcome::Enqueued
    }

    fn push_for_rebuild(&mut self, entry: ReadyQueueEntry) -> Result<(), InternalError> {
        match self.try_push(entry) {
            PushOutcome::Enqueued | PushOutcome::Duplicate => Ok(()),
            PushOutcome::Full => Err(InternalError::ReadyQueueSendFailure(format!(
                "ready queue rebuild exceeds capacity {}",
                self.capacity
            ))),
        }
    }

    fn pop_bulk(&mut self, max_items: usize) -> Vec<ReadyQueueEntry> {
        let num_items = max_items.min(self.queue.len());
        let mut batch = Vec::with_capacity(num_items);
        for _ in 0..num_items {
            let entry = self
                .queue
                .pop_front()
                .expect("resident queue length was checked before pop");
            self.dedup.remove(&entry);
            batch.push(entry);
        }
        batch
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn is_full(&self) -> bool {
        self.queue.len() >= self.capacity
    }
}

enum PushOutcome {
    Enqueued,
    Duplicate,
    Full,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_queue() -> (ReadyQueueSenderHandle, ReadyQueue) {
        let ready_queue = ReadyQueue::create(ReadyQueueConfig {
            ingress_capacity: 8,
            task_ready_capacity: 8,
            commit_ready_capacity: 4,
            cleanup_ready_capacity: 4,
        });
        let sender = ready_queue.sender();
        (sender, ready_queue)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn task_ready_batches_are_flattened_by_queue() {
        let (sender, ready_queue) = test_queue();
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![1, 2, 3])
            .await
            .expect("send should succeed");

        ready_queue.flatten();

        assert_eq!(
            ready_queue.recv_tasks(10),
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
        let (sender, ready_queue) = test_queue();
        let job_id = JobId::default();

        sender
            .send_commit_ready(job_id)
            .await
            .expect("commit send should succeed");
        sender
            .send_cleanup_ready(job_id)
            .await
            .expect("cleanup send should succeed");

        ready_queue.flatten();

        assert_eq!(
            ready_queue.recv_commits(1),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Commit,
            }]
        );
        assert_eq!(
            ready_queue.recv_cleanups(1),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Cleanup,
            }]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resident_duplicates_are_suppressed_until_pop() {
        let (sender, ready_queue) = test_queue();
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![7, 7])
            .await
            .expect("duplicate task send should succeed");
        ready_queue.flatten();

        assert_eq!(ready_queue.recv_tasks(10).len(), 1);
        assert!(ready_queue.recv_tasks(10).is_empty());

        sender
            .send_task_ready(job_id, vec![7])
            .await
            .expect("re-enqueue after pop should succeed");
        ready_queue.flatten();
        assert_eq!(ready_queue.recv_tasks(10).len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn len_tracks_resident_entries() {
        let (_, ready_queue) = test_queue();
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
            .expect("rebuild should succeed");

        let task_batch = ready_queue.recv_tasks(1);
        assert_eq!(task_batch.len(), 1);
        assert_eq!(ready_queue.num_tasks(), 1);
        assert_eq!(ready_queue.num_commits(), 1);
        assert_eq!(ready_queue.total_len(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rebuild_replaces_resident_state_and_drops_stale_buffer() {
        let (sender, ready_queue) = test_queue();
        let stale_job_id = JobId::default();
        let fresh_job_id = JobId::default();

        sender
            .send_task_ready(stale_job_id, vec![1, 2])
            .await
            .expect("stale send should succeed");

        ready_queue
            .rebuild([
                ReadyQueueEntry {
                    job_id: fresh_job_id,
                    task_id: TaskId::Index(9),
                },
                ReadyQueueEntry {
                    job_id: fresh_job_id,
                    task_id: TaskId::Cleanup,
                },
            ])
            .expect("rebuild should succeed");

        ready_queue.flatten();

        assert_eq!(
            ready_queue.recv_tasks(10),
            vec![ReadyQueueEntry {
                job_id: fresh_job_id,
                task_id: TaskId::Index(9),
            }]
        );
        assert_eq!(
            ready_queue.recv_cleanups(10),
            vec![ReadyQueueEntry {
                job_id: fresh_job_id,
                task_id: TaskId::Cleanup,
            }]
        );
        assert!(ready_queue.recv_tasks(10).is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sender_handles_survive_rebuild_channel_swap() {
        let (sender, ready_queue) = test_queue();
        let job_id = JobId::default();

        ready_queue
            .rebuild([])
            .expect("empty rebuild should succeed");
        sender
            .send_task_ready(job_id, vec![4])
            .await
            .expect("send after rebuild should succeed");

        ready_queue.flatten();

        assert_eq!(
            ready_queue.recv_tasks(10),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(4),
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
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![1])
            .await
            .expect("first send should succeed");
        ready_queue.flatten();

        sender
            .send_task_ready(job_id, vec![2])
            .await
            .expect("second send should fill ingress");

        let send_error = sender
            .send_task_ready(job_id, vec![3])
            .await
            .expect_err("third send should fail immediately when ingress is full");
        assert!(
            matches!(send_error, InternalError::ReadyQueueSendFailure(message) if message.contains("ingress is full"))
        );

        assert_eq!(
            ready_queue.recv_tasks(1),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(1),
            }]
        );
        ready_queue.flatten();
        assert_eq!(ready_queue.num_tasks(), 1);
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
        let job_id = JobId::default();

        sender
            .send_commit_ready(job_id)
            .await
            .expect("first commit send should succeed");
        ready_queue.flatten();

        sender
            .send_commit_ready(job_id)
            .await
            .expect("second commit send should fill ingress");

        let send_error = sender
            .send_commit_ready(job_id)
            .await
            .expect_err("third commit send should fail immediately when ingress is full");
        assert!(
            matches!(send_error, InternalError::ReadyQueueSendFailure(message) if message.contains("ingress is full"))
        );

        assert_eq!(
            ready_queue.recv_commits(1),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Commit,
            }]
        );
        ready_queue.flatten();
        assert_eq!(ready_queue.num_commits(), 1);
    }
}
