use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use spider_core::{task::TaskIndex, types::id::JobId};
use tokio::sync::{Mutex, Notify, mpsc};

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

/// An in-memory ready queue with demand-driven ingress draining.
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

        self.inner.send_task_ready_batch(job_id, task_indices).await
    }

    async fn send_commit_ready(&self, job_id: JobId) -> Result<(), InternalError> {
        self.inner
            .send_termination_ready(ReadyQueueEntry {
                job_id,
                task_id: TaskId::Commit,
            })
            .await
    }

    async fn send_cleanup_ready(&self, job_id: JobId) -> Result<(), InternalError> {
        self.inner
            .send_termination_ready(ReadyQueueEntry {
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
    /// flusher task on the given Tokio runtime.
    ///
    /// # Parameters
    ///
    /// * `config` - The ready queue configuration.
    /// * `runtime_handle` - The Tokio runtime handle used to spawn the background flusher task.
    ///
    /// # Returns
    ///
    /// The created ready queue.
    #[must_use]
    pub fn create(config: ReadyQueueConfig, runtime_handle: &tokio::runtime::Handle) -> Self {
        let (ingress_senders, ingress_receivers) =
            IngressSenderSet::create(config.ingress_capacity);
        let flusher_notify = Arc::new(Notify::new());
        let inner = Arc::new(ReadyQueueInner {
            flusher_notify: flusher_notify.clone(),
            ingress_senders,
            state: Mutex::new(ReadyQueueState::new(config, ingress_receivers)),
        });
        ReadyQueueInner::spawn_flusher(runtime_handle, Arc::downgrade(&inner), flusher_notify);
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
        self.inner.total_len().await
    }
}

/// Shared state behind sender and receiver handles.
struct ReadyQueueInner {
    /// Wake primitive for the single background flusher task.
    flusher_notify: Arc<Notify>,
    /// Bounded ingress queues used by producers.
    ingress_senders: IngressSenderSet,
    /// Resident queues plus ingress receivers.
    state: Mutex<ReadyQueueState>,
}

/// Resident queue state plus ingress receivers for all queue families.
struct ReadyQueueState {
    ingress_receivers: IngressReceiverSet,
    task_queue: ResidentQueue,
    commit_queue: ResidentQueue,
    cleanup_queue: ResidentQueue,
}

/// Producer-side handles for the three ingress channels.
#[derive(Clone)]
struct IngressSenderSet {
    tasks: mpsc::Sender<ReadyQueueEntry>,
    commits: mpsc::Sender<ReadyQueueEntry>,
    cleanups: mpsc::Sender<ReadyQueueEntry>,
}

/// Consumer-side handles for the three ingress channels.
struct IngressReceiverSet {
    tasks: mpsc::Receiver<ReadyQueueEntry>,
    commits: mpsc::Receiver<ReadyQueueEntry>,
    cleanups: mpsc::Receiver<ReadyQueueEntry>,
}

/// Deduplicating resident queue used after ingress draining.
struct ResidentQueue {
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

/// Identifies which resident and ingress queues should be drained together.
#[derive(Clone, Copy)]
enum DrainScope {
    All,
    Tasks,
    Commits,
    Cleanups,
}

impl ReadyQueueInner {
    /// Starts the background flusher task.
    ///
    /// The flusher wakes on [`Notify`] signals, drains ingress until no further progress is
    /// possible, and then goes back to sleep. The task only holds a weak reference to the queue
    /// state so dropping the queue stops the loop naturally without extra shutdown plumbing.
    ///
    /// # Parameters
    ///
    /// * `runtime_handle` - The Tokio runtime handle used to spawn the flusher task.
    /// * `weak_inner` - Weak reference to the queue state shared by senders and receivers.
    /// * `flusher_notify` - Wake primitive used to notify the flusher of new ingress work.
    fn spawn_flusher(
        runtime_handle: &tokio::runtime::Handle,
        weak_inner: std::sync::Weak<Self>,
        flusher_notify: Arc<Notify>,
    ) {
        runtime_handle.spawn(async move {
            loop {
                flusher_notify.notified().await;

                loop {
                    let Some(inner) = weak_inner.upgrade() else {
                        return;
                    };
                    if !inner.poll_inline(DrainScope::All).await {
                        break;
                    }
                }
            }
        });
    }

    /// Acquires the ready queue state lock.
    ///
    /// # Returns
    ///
    /// The locked [`ReadyQueueState`].
    async fn lock_state(&self) -> tokio::sync::MutexGuard<'_, ReadyQueueState> {
        self.state.lock().await
    }

    /// Enqueues a batch of regular task-ready entries.
    ///
    /// Each task is forwarded independently in input order. The function does not make the batch
    /// atomic; if the ingress channel closes mid-batch, earlier tasks may already have been
    /// accepted.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The job ID.
    /// * `task_indices` - The regular task indices that became ready.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::ReadyQueueSendFailure`] if the task ingress channel closes before the
    ///   full batch is accepted.
    async fn send_task_ready_batch(
        self: &Arc<Self>,
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError> {
        let num_tasks = task_indices.len();
        for (num_accepted_tasks, task_index) in task_indices.into_iter().enumerate() {
            let ready_entry = ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(task_index),
            };

            if self
                .send_ingress_message(
                    ready_entry,
                    &self.ingress_senders.tasks,
                    DrainScope::Tasks,
                    "task",
                )
                .await
                .is_err()
            {
                return Err(InternalError::ReadyQueueSendFailure(format!(
                    "task ready queue ingress is closed after accepting {num_accepted_tasks} of \
                     {num_tasks} tasks"
                )));
            }
        }

        Ok(())
    }

    /// Enqueues a commit-ready or cleanup-ready signal.
    ///
    /// # Parameters
    ///
    /// * `ready_entry` - The termination entry to enqueue.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::ReadyQueueSendFailure`] if the caller passes a regular task entry.
    /// * [`InternalError::ReadyQueueSendFailure`] if the selected ingress channel is closed.
    async fn send_termination_ready(
        self: &Arc<Self>,
        ready_entry: ReadyQueueEntry,
    ) -> Result<(), InternalError> {
        let (ingress_queue_name, drain_scope, ingress_sender) = match ready_entry.task_id {
            TaskId::Index(_) => {
                return Err(InternalError::ReadyQueueSendFailure(
                    "regular tasks must be sent through send_task_ready".to_owned(),
                ));
            }
            TaskId::Commit => ("commit", DrainScope::Commits, &self.ingress_senders.commits),
            TaskId::Cleanup => (
                "cleanup",
                DrainScope::Cleanups,
                &self.ingress_senders.cleanups,
            ),
        };

        self.send_ingress_message(ready_entry, ingress_sender, drain_scope, ingress_queue_name)
            .await
    }

    /// Sends one message to an ingress queue.
    ///
    /// The fast path is a plain `try_send` followed by a background wakeup, which avoids touching
    /// resident state when ingress still has capacity. When ingress is full, the function performs
    /// one inline drain for the relevant lane, retries `try_send`, and only then awaits on
    /// `send`.
    ///
    /// # Parameters
    ///
    /// * `message` - The ingress message to enqueue.
    /// * `ingress_sender` - The bounded ingress channel used by the selected queue family.
    /// * `drain_scope` - The resident queue family that may be drained inline when ingress is full.
    ///
    /// # Errors
    ///
    /// Returns [`InternalError::ReadyQueueSendFailure`] if the ingress channel is closed.
    async fn send_ingress_message<Message>(
        self: &Arc<Self>,
        message: Message,
        ingress_sender: &mpsc::Sender<Message>,
        drain_scope: DrainScope,
        ingress_queue_name: &str,
    ) -> Result<(), InternalError>
    where
        Message: Send, {
        let Some(message) =
            Self::try_send_ingress_message(message, ingress_sender, ingress_queue_name)?
        else {
            self.after_successful_enqueue(ingress_sender, drain_scope)
                .await;
            return Ok(());
        };

        let _ = self.poll_inline(drain_scope).await;

        let Some(message) =
            Self::try_send_ingress_message(message, ingress_sender, ingress_queue_name)?
        else {
            self.after_successful_enqueue(ingress_sender, drain_scope)
                .await;
            return Ok(());
        };

        ingress_sender.send(message).await.map_err(|_| {
            InternalError::ReadyQueueSendFailure(format!(
                "{ingress_queue_name} ready queue ingress is closed"
            ))
        })?;
        self.after_successful_enqueue(ingress_sender, drain_scope)
            .await;
        Ok(())
    }

    /// Attempts one non-blocking ingress enqueue.
    ///
    /// # Returns
    ///
    /// * `Ok(None)` if the enqueue succeeds immediately.
    /// * `Ok(Some(message))` if the ingress channel is full and the caller should retry or await.
    ///
    /// # Errors
    ///
    /// Returns [`InternalError::ReadyQueueSendFailure`] if the ingress channel is closed.
    fn try_send_ingress_message<Message>(
        message: Message,
        ingress_sender: &mpsc::Sender<Message>,
        ingress_queue_name: &str,
    ) -> Result<Option<Message>, InternalError> {
        match ingress_sender.try_send(message) {
            Ok(()) => Ok(None),
            Err(mpsc::error::TrySendError::Full(message)) => Ok(Some(message)),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(InternalError::ReadyQueueSendFailure(
                format!("{ingress_queue_name} ready queue ingress is closed"),
            )),
        }
    }

    /// Completes post-enqueue work after a successful ingress send.
    ///
    /// If the successful enqueue leaves ingress full, the sender performs one inline drain before
    /// returning so the just-enqueued item can be surfaced promptly. Otherwise it only wakes the
    /// background flusher, keeping the normal enqueue fast path lock-free with respect to resident
    /// state.
    ///
    /// # Parameters
    ///
    /// * `ingress_sender` - The sender that accepted the message.
    /// * `drain_scope` - The resident queue family that may be drained inline when ingress is now
    ///   full.
    async fn after_successful_enqueue<Message>(
        self: &Arc<Self>,
        ingress_sender: &mpsc::Sender<Message>,
        drain_scope: DrainScope,
    ) where
        Message: Send, {
        if ingress_sender.capacity() == 0 {
            let _ = self.poll_inline(drain_scope).await;
        } else {
            self.flusher_notify.notify_one();
        }
    }

    /// Drains ingress inline.
    ///
    /// Callers use this only when they are already on a slow path such as a full-ingress enqueue
    /// or an explicit receive/count operation.
    ///
    /// # Parameters
    ///
    /// * `drain_scope` - The queue families to drain.
    ///
    /// # Returns
    ///
    /// `true` if at least one ingress entry was removed from a channel, `false` otherwise.
    async fn poll_inline(self: &Arc<Self>, drain_scope: DrainScope) -> bool {
        self.lock_state().await.drain_ingress(drain_scope)
    }

    /// Receives regular task entries after draining task ingress on demand.
    async fn recv_tasks(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        if max_items == 0 {
            return Ok(Vec::new());
        }

        self.recv_from_queue(max_items, DrainScope::Tasks, |state| &mut state.task_queue)
            .await
    }

    /// Receives commit entries after draining commit ingress on demand.
    async fn recv_commits(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        if max_items == 0 {
            return Ok(Vec::new());
        }

        self.recv_from_queue(max_items, DrainScope::Commits, |state| {
            &mut state.commit_queue
        })
        .await
    }

    /// Receives cleanup entries after draining cleanup ingress on demand.
    async fn recv_cleanups(&self, max_items: usize) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        if max_items == 0 {
            return Ok(Vec::new());
        }

        self.recv_from_queue(max_items, DrainScope::Cleanups, |state| {
            &mut state.cleanup_queue
        })
        .await
    }

    /// Returns the resident task-queue length after one drain pass.
    async fn num_tasks(&self) -> Result<usize, InternalError> {
        Ok(self
            .len_after_drain(DrainScope::Tasks, |state| &state.task_queue)
            .await)
    }

    /// Returns the resident commit-queue length after one drain pass.
    async fn num_commits(&self) -> Result<usize, InternalError> {
        Ok(self
            .len_after_drain(DrainScope::Commits, |state| &state.commit_queue)
            .await)
    }

    /// Returns the resident cleanup-queue length after one drain pass.
    async fn num_cleanups(&self) -> Result<usize, InternalError> {
        Ok(self
            .len_after_drain(DrainScope::Cleanups, |state| &state.cleanup_queue)
            .await)
    }

    /// Returns the combined resident length after draining all ingress queues once.
    async fn total_len(&self) -> Result<usize, InternalError> {
        let mut state = self.lock_state().await;
        state.drain_ingress(DrainScope::All);
        Ok(state.task_queue.len() + state.commit_queue.len() + state.cleanup_queue.len())
    }

    /// Replaces resident queue contents from a rebuilt snapshot.
    ///
    /// # Parameters
    ///
    /// * `entries` - The snapshot entries used to rebuild resident state.
    ///
    /// # Errors
    ///
    /// Forwards [`ReadyQueueState::rebuild`]'s return values on failure.
    async fn rebuild<I>(&self, entries: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = ReadyQueueEntry>, {
        self.lock_state().await.rebuild(entries)?;
        Ok(())
    }

    /// Receives entries from one resident queue.
    ///
    /// The function drains the selected lane before popping so buffered ingress becomes visible to
    /// the caller, then drains again after popping so freed resident capacity is refilled
    /// immediately.
    ///
    /// # Parameters
    ///
    /// * `max_items` - The maximum number of resident entries to pop.
    /// * `drain_scope` - The queue families to drain before and after popping.
    /// * `select_queue` - Selector for the resident queue to pop from.
    ///
    /// # Errors
    ///
    /// Forwards [`ResidentQueue::pop_bulk`]'s return values on failure.
    async fn recv_from_queue(
        &self,
        max_items: usize,
        drain_scope: DrainScope,
        select_queue: impl Fn(&mut ReadyQueueState) -> &mut ResidentQueue,
    ) -> Result<Vec<ReadyQueueEntry>, InternalError> {
        let mut state = self.lock_state().await;
        state.drain_ingress(drain_scope);
        let ready_entries = select_queue(&mut state).pop_bulk(max_items)?;
        state.drain_ingress(drain_scope);
        drop(state);
        Ok(ready_entries)
    }

    /// Returns one resident queue length after a drain pass.
    ///
    /// # Parameters
    ///
    /// * `drain_scope` - The queue families to drain before reading the length.
    /// * `select_queue` - Selector for the resident queue whose length will be returned.
    ///
    /// # Returns
    ///
    /// The selected resident queue length after draining the requested ingress lanes.
    async fn len_after_drain(
        &self,
        drain_scope: DrainScope,
        select_queue: impl Fn(&ReadyQueueState) -> &ResidentQueue,
    ) -> usize {
        let mut state = self.lock_state().await;
        state.drain_ingress(drain_scope);
        let len = select_queue(&state).len();
        drop(state);
        len
    }
}

impl ReadyQueueState {
    /// Factory function.
    ///
    /// Creates empty resident queues and stores the ingress receivers they drain from.
    ///
    /// # Parameters
    ///
    /// * `config` - The ready queue configuration.
    /// * `ingress_receivers` - The ingress receivers drained into the resident queues.
    ///
    /// # Returns
    ///
    /// The created [`ReadyQueueState`].
    fn new(config: ReadyQueueConfig, ingress_receivers: IngressReceiverSet) -> Self {
        Self {
            ingress_receivers,
            task_queue: ResidentQueue::new(config.task_ready_capacity),
            commit_queue: ResidentQueue::new(config.commit_ready_capacity),
            cleanup_queue: ResidentQueue::new(config.cleanup_ready_capacity),
        }
    }

    /// Rebuilds the resident queues from a snapshot.
    ///
    /// Buffered ingress backlog is preserved and will be reconciled through subsequent drain
    /// operations.
    ///
    /// # Parameters
    ///
    /// * `entries` - The snapshot of ready entries used to repopulate resident state.
    ///
    /// # Errors
    ///
    /// Forwards [`ResidentQueue::push_for_rebuild`]'s return values on failure.
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

    /// Drains ingress into resident queues for the requested scope.
    ///
    /// Draining stops when each targeted resident queue is either full or its ingress receiver is
    /// empty.
    ///
    /// # Parameters
    ///
    /// * `drain_scope` - The queue families to drain.
    ///
    /// # Returns
    ///
    /// `true` if at least one ingress entry was removed from a channel, `false` otherwise.
    fn drain_ingress(&mut self, drain_scope: DrainScope) -> bool {
        let mut made_progress = false;
        match drain_scope {
            DrainScope::All => {
                made_progress |= self.drain_task_ingress();
                made_progress |= self
                    .commit_queue
                    .drain_ingress(&mut self.ingress_receivers.commits);
                made_progress |= self
                    .cleanup_queue
                    .drain_ingress(&mut self.ingress_receivers.cleanups);
            }
            DrainScope::Tasks => {
                made_progress |= self.drain_task_ingress();
            }
            DrainScope::Commits => {
                made_progress |= self
                    .commit_queue
                    .drain_ingress(&mut self.ingress_receivers.commits);
            }
            DrainScope::Cleanups => {
                made_progress |= self
                    .cleanup_queue
                    .drain_ingress(&mut self.ingress_receivers.cleanups);
            }
        }
        made_progress
    }

    /// Drains task ingress into the resident task queue.
    ///
    /// Task entries use a dedicated helper because task resident capacity is much larger than the
    /// termination queues and callers often want to target this lane independently.
    ///
    /// # Returns
    ///
    /// `true` if at least one task ingress entry was removed from the channel, `false` otherwise.
    fn drain_task_ingress(&mut self) -> bool {
        let mut made_progress = false;
        while !self.task_queue.is_full() {
            match self.ingress_receivers.tasks.try_recv() {
                Ok(ready_entry) => {
                    made_progress = true;
                    let _ = self.task_queue.try_push(ready_entry);
                }
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }
        made_progress
    }
}

impl IngressSenderSet {
    /// Factory function.
    ///
    /// Creates bounded ingress channels for task, commit, and cleanup notifications.
    ///
    /// # Parameters
    ///
    /// * `ingress_capacity` - The capacity of each ingress queue.
    ///
    /// # Returns
    ///
    /// The created ingress senders and receivers.
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
    /// Factory function.
    ///
    /// Creates an empty resident deduplicating queue.
    ///
    /// # Parameters
    ///
    /// * `capacity` - The maximum number of resident entries the queue can hold.
    ///
    /// # Returns
    ///
    /// The created resident queue.
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: VecDeque::with_capacity(capacity),
            entry_set: HashSet::with_capacity(capacity),
        }
    }

    /// Attempts to append an entry to the resident queue.
    ///
    /// Entries already resident in the queue are treated as duplicates and are not appended.
    ///
    /// # Parameters
    ///
    /// * `ready_entry` - The entry to enqueue.
    ///
    /// # Returns
    ///
    /// The enqueue result.
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
    ///
    /// Duplicate entries are ignored. Resident-capacity overflow is reported as a rebuild error.
    ///
    /// # Parameters
    ///
    /// * `ready_entry` - The snapshot entry to insert.
    ///
    /// # Errors
    ///
    /// Returns [`InternalError::ReadyQueueSendFailure`] if the resident queue would exceed
    /// capacity.
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
    ///
    /// Popped entries are also removed from the dedup set so they may be re-enqueued later.
    ///
    /// # Parameters
    ///
    /// * `max_items` - The maximum number of entries to pop.
    ///
    /// # Returns
    ///
    /// The popped entries.
    ///
    /// # Errors
    ///
    /// Returns [`InternalError::ReadyQueueSendFailure`] if the resident queue bookkeeping is
    /// corrupted.
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

    /// Returns whether the resident queue has reached capacity.
    fn is_full(&self) -> bool {
        self.entries.len() >= self.capacity
    }

    /// Drains one ingress receiver into this resident queue.
    ///
    /// Draining stops when the resident queue becomes full or the ingress receiver is empty.
    ///
    /// # Parameters
    ///
    /// * `ingress_receiver` - The ingress receiver drained into this resident queue.
    ///
    /// # Returns
    ///
    /// `true` if at least one ingress entry was removed from the channel, `false` otherwise.
    fn drain_ingress(&mut self, ingress_receiver: &mut mpsc::Receiver<ReadyQueueEntry>) -> bool {
        let mut made_progress = false;
        while !self.is_full() {
            match ingress_receiver.try_recv() {
                Ok(ready_entry) => {
                    made_progress = true;
                    let _ = self.try_push(ready_entry);
                }
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }
        made_progress
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::{sleep, timeout};

    use super::*;

    fn test_queue() -> (ReadyQueueSenderHandle, ReadyQueueReceiverHandle, ReadyQueue) {
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
        (sender, receiver, ready_queue)
    }

    async fn wait_for_task_resident_len(ready_queue: &ReadyQueue, expected_len: usize) {
        timeout(Duration::from_millis(100), async {
            loop {
                let len = {
                    let state = ready_queue.inner.lock_state().await;
                    state.task_queue.len()
                };
                if len == expected_len {
                    break;
                }
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("resident task queue should reach expected length");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn task_ready_batches_are_flattened_by_queue() {
        let (sender, receiver, _) = test_queue();
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![1, 2, 3])
            .await
            .expect("send should succeed");

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
    async fn non_full_enqueue_schedules_async_poll() {
        let ready_queue = ReadyQueue::create(
            ReadyQueueConfig {
                ingress_capacity: 4,
                task_ready_capacity: 4,
                commit_ready_capacity: 4,
                cleanup_ready_capacity: 4,
            },
            &tokio::runtime::Handle::current(),
        );
        let sender = ready_queue.sender();
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![1])
            .await
            .expect("send should succeed");

        wait_for_task_resident_len(&ready_queue, 1).await;

        let state = ready_queue.inner.lock_state().await;
        assert_eq!(state.task_queue.len(), 1);
        drop(state);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_and_cleanup_use_separate_queues() {
        let (sender, receiver, _) = test_queue();
        let job_id = JobId::default();

        sender
            .send_commit_ready(job_id)
            .await
            .expect("commit send should succeed");
        sender
            .send_cleanup_ready(job_id)
            .await
            .expect("cleanup send should succeed");

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
        let (sender, receiver, _) = test_queue();
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![7, 7])
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
            receiver.recv_tasks(1).await.expect("recv should succeed"),
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

        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(4),
            }]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_drains_task_channel_backlog_when_resident_queue_is_full() {
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
        let buffered_job_id = JobId::default();

        sender
            .send_task_ready(buffered_job_id, vec![1, 2, 3])
            .await
            .expect("buffered send should succeed");

        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id: buffered_job_id,
                task_id: TaskId::Index(1),
            }]
        );

        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id: buffered_job_id,
                task_id: TaskId::Index(2),
            }]
        );

        assert_eq!(
            receiver.recv_tasks(10).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id: buffered_job_id,
                task_id: TaskId::Index(3),
            }]
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
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![1, 2, 3])
            .await
            .expect("batch send should complete");

        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(1),
            }]
        );
        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(2),
            }]
        );
        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(3),
            }]
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
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![1])
            .await
            .expect("first send should succeed");
        assert_eq!(receiver.num_tasks().await.expect("count should succeed"), 1);

        sender
            .send_task_ready(job_id, vec![2])
            .await
            .expect("second send should fill ingress");

        let sender_clone = sender.clone();
        let mut send_handle =
            tokio::spawn(async move { sender_clone.send_task_ready(job_id, vec![3]).await });
        assert!(
            timeout(Duration::from_millis(50), &mut send_handle)
                .await
                .is_err(),
            "third send should block while ingress is full"
        );

        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(1),
            }]
        );

        send_handle
            .await
            .expect("send task should not panic")
            .expect("blocked send should complete");

        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(2),
            }]
        );
        assert_eq!(
            receiver.recv_tasks(1).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Index(3),
            }]
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
        let job_id = JobId::default();

        sender
            .send_commit_ready(job_id)
            .await
            .expect("first commit send should succeed");
        assert_eq!(
            receiver.num_commits().await.expect("count should succeed"),
            1
        );

        sender
            .send_commit_ready(job_id)
            .await
            .expect("second commit send should fill ingress");

        let sender_clone = sender.clone();
        let mut send_handle =
            tokio::spawn(async move { sender_clone.send_commit_ready(job_id).await });
        assert!(
            timeout(Duration::from_millis(50), &mut send_handle)
                .await
                .is_err(),
            "third commit send should block while ingress is full"
        );

        assert_eq!(
            receiver.recv_commits(1).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Commit,
            }]
        );

        send_handle
            .await
            .expect("send task should not panic")
            .expect("blocked send should complete");

        assert_eq!(
            receiver.recv_commits(1).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Commit,
            }]
        );
        assert_eq!(
            receiver.recv_commits(1).await.expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Commit,
            }]
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
        let job_id = JobId::default();

        sender
            .send_cleanup_ready(job_id)
            .await
            .expect("first cleanup send should succeed");
        assert_eq!(
            receiver.num_cleanups().await.expect("count should succeed"),
            1
        );

        sender
            .send_cleanup_ready(job_id)
            .await
            .expect("second cleanup send should fill ingress");

        let sender_clone = sender.clone();
        let mut send_handle =
            tokio::spawn(async move { sender_clone.send_cleanup_ready(job_id).await });
        assert!(
            timeout(Duration::from_millis(50), &mut send_handle)
                .await
                .is_err(),
            "third cleanup send should block while ingress is full"
        );

        assert_eq!(
            receiver
                .recv_cleanups(1)
                .await
                .expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Cleanup,
            }]
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
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Cleanup,
            }]
        );
        assert_eq!(
            receiver
                .recv_cleanups(1)
                .await
                .expect("recv should succeed"),
            vec![ReadyQueueEntry {
                job_id,
                task_id: TaskId::Cleanup,
            }]
        );
    }
}
