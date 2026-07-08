//! The implementation of the round-robin scheduler core. See the parent module's documentation for
//! the scheduling policy and configuration.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use serde::Deserialize;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskId;
use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::DispatchQueueSink;
use crate::InboundEntry;
use crate::SchedulerCore;
use crate::SchedulerError;
use crate::SchedulerStorageClient;
use crate::StorageClientError;
use crate::TaskAssignment;
use crate::core::TaskAssignmentIdIssuer;

/// The configuration of the round-robin scheduler core.
#[derive(Clone, Debug, Deserialize)]
pub struct RoundRobinConfig {
    /// The capacity of the active job queue. The scheduler will make task assignments from these
    /// jobs in a round-robin manner.
    pub active_job_queue_capacity: NonZeroUsize,

    /// The capacity of the dispatch queue.
    pub dispatch_queue_capacity: NonZeroUsize,

    /// The capacity of the total pending ready tasks buffered in the scheduler.
    pub ready_task_capacity: NonZeroUsize,

    /// The capacity of the total pending commit-ready tasks buffered in the scheduler.
    pub commit_ready_task_capacity: NonZeroUsize,

    /// The capacity of the total pending cleanup-ready tasks buffered in the scheduler.
    pub cleanup_ready_task_capacity: NonZeroUsize,

    /// The maximum time (in milliseconds) that the scheduler will wait for the storage server to
    /// fill the inbound-queue reading request.
    pub storage_poll_timeout_ms: u64,

    /// The time (in milliseconds) that the scheduler will spend on each tick.
    pub tick_interval_ms: NonZeroU64,

    /// The time (in seconds) that a job may remain in the finalizing state before the scheduler
    /// retires it.
    pub finalizing_job_expiration_timeout_sec: u64,
}

impl RoundRobinConfig {
    /// Creates a ready-to-run scheduler core from the configuration.
    ///
    /// # Type Parameters
    ///
    /// * `SchedulerStorageClientType` - The storage client used to poll the inbound queue.
    /// * `DispatchQueueSinkType` - The dispatch sink that task assignments are written to.
    ///
    /// # Returns
    ///
    /// A newly created round-robin scheduler core.
    #[must_use]
    pub const fn make_core<
        SchedulerStorageClientType: SchedulerStorageClient + 'static,
        DispatchQueueSinkType: DispatchQueueSink,
    >(
        self,
    ) -> RoundRobinCore<SchedulerStorageClientType, DispatchQueueSinkType> {
        RoundRobinCore {
            config: self,
            _marker: std::marker::PhantomData,
        }
    }
}

/// The round-robin implementation of [`SchedulerCore`], created from
/// [`RoundRobinConfig::make_core`].
///
/// Holding an instance of this type guarantees the wrapped configuration has passed validation, so
/// the scheduling loop can trust its invariants without re-validating.
///
/// # Type Parameters
///
/// * `SchedulerStorageClientType` - The storage client used to poll the inbound queue.
/// * `DispatchQueueSinkType` - The dispatch sink that task assignments are written to.
pub struct RoundRobinCore<
    SchedulerStorageClientType: SchedulerStorageClient + 'static,
    DispatchQueueSinkType: DispatchQueueSink,
> {
    config: RoundRobinConfig,
    _marker: std::marker::PhantomData<(SchedulerStorageClientType, DispatchQueueSinkType)>,
}

#[async_trait]
impl<
    SchedulerStorageClientType: SchedulerStorageClient + 'static,
    DispatchQueueSinkType: DispatchQueueSink,
> SchedulerCore for RoundRobinCore<SchedulerStorageClientType, DispatchQueueSinkType>
{
    type Sink = DispatchQueueSinkType;
    type StorageClient = SchedulerStorageClientType;

    async fn run(
        self: Box<Self>,
        storage_client: Self::StorageClient,
        sink: Self::Sink,
        id_issuer: TaskAssignmentIdIssuer,
        cancellation_token: CancellationToken,
    ) -> Result<(), SchedulerError> {
        RoundRobin::new(
            SessionId::default(),
            storage_client,
            sink,
            id_issuer,
            cancellation_token,
            self.config,
        )
        .run()
        .await
    }
}

/// A FIFO queue of a job's buffered ready tasks.
#[derive(Eq, PartialEq, Debug)]
pub(super) struct JobTaskQueue {
    job_id: JobId,
    resource_group_id: ResourceGroupId,
    task_ids: VecDeque<TaskId>,
}

impl JobTaskQueue {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A new task queue for the given job, seeded with `init_task_id`.
    fn new(job_id: JobId, resource_group_id: ResourceGroupId, init_task_id: TaskId) -> Self {
        Self {
            job_id,
            resource_group_id,
            task_ids: VecDeque::from([init_task_id]),
        }
    }

    fn enqueue(&mut self, task_id: TaskId) {
        self.task_ids.push_back(task_id);
    }

    /// # Returns
    ///
    /// * The next ready task ID in FIFO order.
    /// * [`None`] if the queue is empty.
    fn dequeue(&mut self) -> Option<TaskId> {
        self.task_ids.pop_front()
    }
}

/// The round-robin scheduler core created from a [`RoundRobinConfig`].
///
/// # Type Parameters
///
/// * `SchedulerStorageClientType` - The storage client used to poll the inbound queue.
/// * `DispatchQueueSinkType` - The dispatch sink that task assignments are written to.
///
/// # Note
///
/// All member variables are marked `pub(super)` to allow the test module to inspect the internal
/// states.
pub(super) struct RoundRobin<
    SchedulerStorageClientType: SchedulerStorageClient + 'static,
    DispatchQueueSinkType: DispatchQueueSink,
> {
    pub(super) sink: DispatchQueueSinkType,
    pub(super) cancellation_token: CancellationToken,
    pub(super) id_issuer: TaskAssignmentIdIssuer,
    pub(super) config: RoundRobinConfig,
    pub(super) storage_session_id: SessionId,

    pub(super) buffered_tasks: HashSet<(JobId, TaskId)>,

    pub(super) active_jobs: HashMap<JobId, JobTaskQueue>,
    pub(super) rr_queue: Vec<RoundRobinSlot>,
    pub(super) rr_cursor: usize,

    pub(super) pending_jobs: HashMap<JobId, JobTaskQueue>,
    pub(super) pending_job_queue: VecDeque<JobId>,

    pub(super) commit_ready_jobs: VecDeque<(JobId, ResourceGroupId)>,
    pub(super) cleanup_ready_jobs: VecDeque<(JobId, ResourceGroupId)>,

    pub(super) finalizing_jobs: HashSet<JobId>,
    pub(super) finalizing_job_queue: VecDeque<(JobId, Instant)>,

    pub(super) inbound_queue_reader: AsyncInboundQueueReader<SchedulerStorageClientType>,
}

impl<
    SchedulerStorageClientType: SchedulerStorageClient + 'static,
    DispatchQueueSinkType: DispatchQueueSink,
> RoundRobin<SchedulerStorageClientType, DispatchQueueSinkType>
{
    /// Factory function.
    ///
    /// Creates a [`RoundRobin`] scheduler from the given config.
    ///
    /// # Returns
    ///
    /// The constructed [`RoundRobin`] scheduler.
    pub(super) fn new(
        storage_session_id: SessionId,
        storage_client: SchedulerStorageClientType,
        sink: DispatchQueueSinkType,
        id_issuer: TaskAssignmentIdIssuer,
        cancellation_token: CancellationToken,
        config: RoundRobinConfig,
    ) -> Self {
        let buffered_tasks = HashSet::with_capacity(config.ready_task_capacity.get());
        let active_jobs = HashMap::with_capacity(config.active_job_queue_capacity.get());
        let rr_queue = Self::new_round_robin_queue(config.active_job_queue_capacity.get());
        let rr_cursor = 0;
        let pending_jobs = HashMap::with_capacity(config.active_job_queue_capacity.get());
        let pending_job_queue = VecDeque::with_capacity(config.active_job_queue_capacity.get());
        let commit_ready_jobs = VecDeque::with_capacity(config.commit_ready_task_capacity.get());
        let cleanup_ready_jobs = VecDeque::with_capacity(config.cleanup_ready_task_capacity.get());
        let finalizing_jobs = HashSet::with_capacity(
            config.commit_ready_task_capacity.get() + config.cleanup_ready_task_capacity.get(),
        );
        let finalizing_job_queue = VecDeque::new();
        let inbound_queue_reader = AsyncInboundQueueReader::new(storage_client);
        Self {
            sink,
            cancellation_token,
            id_issuer,
            config,
            storage_session_id,
            buffered_tasks,
            active_jobs,
            rr_queue,
            rr_cursor,
            pending_jobs,
            pending_job_queue,
            commit_ready_jobs,
            cleanup_ready_jobs,
            finalizing_jobs,
            finalizing_job_queue,
            inbound_queue_reader,
        }
    }

    /// Executes a single scheduling tick: consumes any completed inbound poll, then makes
    /// scheduling decisions to fill the dispatch queue.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::consume_inbound_poll_result`]'s return values on failure.
    /// * Forwards [`Self::make_schedule_decisions`]'s return values on failure.
    pub(super) async fn tick(&mut self) -> Result<(), SchedulerError> {
        tracing::info!("Starting scheduling tick.");
        self.consume_inbound_poll_result().await?;
        self.make_schedule_decisions().await?;
        self.retire_expired_finalizing_jobs();
        Ok(())
    }

    /// # Returns
    ///
    /// A new round-robin queue containing only the commit-ready and cleanup-ready slots.
    fn new_round_robin_queue(active_job_pool_capacity: usize) -> Vec<RoundRobinSlot> {
        let mut round_robin_queue = Vec::with_capacity(active_job_pool_capacity + 2);
        round_robin_queue.push(RoundRobinSlot::CommitReady);
        round_robin_queue.push(RoundRobinSlot::CleanupReady);
        round_robin_queue
    }

    /// Runs the scheduling loop until the cancellation token is triggered.
    ///
    /// Each iteration executes one [`Self::tick`] and then sleeps for the remainder of the
    /// configured tick interval.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::tick`]'s return values on failure.
    async fn run(mut self) -> Result<(), SchedulerError> {
        tracing::info!(
            config = ? self.config,
            init_session_id = self.storage_session_id,
            "Round-robin scheduler started."
        );
        let tick_interval = Duration::from_millis(self.config.tick_interval_ms.get());
        loop {
            let now = tokio::time::Instant::now();
            let cancellation_token = self.cancellation_token.clone();
            select! {
                () = cancellation_token.cancelled() => {
                    tracing::info!("Round-robin scheduler cancelled. Shutting down.");
                    return Ok(());
                }
                result = self.tick() => {
                    result.inspect_err(|err| tracing::error!(
                        err = % err,
                        "Round-robin scheduler exits on error."
                    ))?;
                }
            }
            let elapsed = now.elapsed();
            let sleep_time = tick_interval.saturating_sub(elapsed);
            if sleep_time.is_zero() {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(sleep_time).await;
            }
        }
    }

    /// Clears all buffered jobs and tasks, resetting the scheduler to its initial placement state.
    fn clear(&mut self) {
        self.buffered_tasks.clear();
        self.active_jobs.clear();
        self.pending_jobs.clear();
        self.pending_job_queue.clear();
        self.commit_ready_jobs.clear();
        self.cleanup_ready_jobs.clear();
        self.finalizing_jobs.clear();
        self.finalizing_job_queue.clear();

        self.rr_queue = Self::new_round_robin_queue(self.config.active_job_queue_capacity.get());
        self.rr_cursor = 0;
    }

    /// Removes the given job from the active set, discards its buffered tasks, and backfills the
    /// freed slot with the next pending job, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::Internal`] if the given job is not currently active.
    fn retire_active_job(&mut self, job_id: JobId) -> Result<(), SchedulerError> {
        tracing::info!(job_id = ? job_id, "Retiring active job.");
        if let Some(index) = self.rr_queue.iter().position(|entry| match entry {
            RoundRobinSlot::Job(id) => *id == job_id,
            _ => false,
        }) {
            self.rr_queue.swap_remove(index);
        } else {
            return Err(SchedulerError::Internal(format!(
                "attempt to remove a non-existing active job: {job_id:?}"
            )));
        }

        if let Some(removed_entry) = self.active_jobs.remove(&job_id) {
            self.discard_job_tasks(removed_entry);
        } else {
            return Err(SchedulerError::Internal(format!(
                "attempt to destroy a non-existing active job: {job_id:?}"
            )));
        }

        if let Some(next_pending_job) = self.pop_next_pending_job() {
            tracing::info!(
                job_id = ? next_pending_job.job_id,
                "Pending job promoted to active job."
            );
            self.rr_queue
                .push(RoundRobinSlot::Job(next_pending_job.job_id));
            self.active_jobs
                .insert(next_pending_job.job_id, next_pending_job);
        }
        Ok(())
    }

    /// # Returns
    ///
    /// The next pending job in FIFO order, or [`None`] if there is no pending job left.
    fn pop_next_pending_job(&mut self) -> Option<JobTaskQueue> {
        loop {
            let job_id = self.pending_job_queue.pop_front()?;
            // NOTE: The job may have been cancelled and removed from `pending_jobs`, so the ID in
            // the queue may not necessarily exist in `pending_jobs`.
            if let Some(pending_job) = self.pending_jobs.remove(&job_id) {
                return Some(pending_job);
            }
        }
    }

    /// Removes all of the given job's queued tasks from the buffered-task set.
    fn discard_job_tasks(&mut self, job_entry: JobTaskQueue) {
        tracing::info!(
            job_id = ? job_entry.job_id,
            num_tasks = job_entry.task_ids.len(),
            "Discarding job tasks."
        );
        for task_id in job_entry.task_ids {
            self.buffered_tasks.remove(&(job_entry.job_id, task_id));
        }
    }

    /// Inserts a job as it is considered finalizing (commit-ready or cleanup-ready). Once inserted,
    /// any further tasks for the job will be ignored until this queue is reset.
    fn mark_job_finalizing(&mut self, job_id: JobId) {
        if self.finalizing_jobs.insert(job_id) {
            self.finalizing_job_queue
                .push_back((job_id, Instant::now()));
        }
    }

    /// Retires expired finalizing jobs.
    ///
    /// A finalizing job is considered expired once it has remained in the finalizing state for
    /// longer than [`RoundRobinConfig::finalizing_job_expiration_timeout_sec`].
    fn retire_expired_finalizing_jobs(&mut self) {
        let expiration_time =
            Duration::from_secs(self.config.finalizing_job_expiration_timeout_sec);
        while let Some((job_id, insertion_time)) = self.finalizing_job_queue.front() {
            if insertion_time.elapsed() > expiration_time {
                tracing::info!(job_id = ? job_id, "Finalizing job retired.");
                self.finalizing_jobs.remove(job_id);
                self.finalizing_job_queue.pop_front();
            } else {
                break;
            }
        }
    }

    /// Loads polled inbound entries into the scheduler's internal buffers.
    ///
    /// If the polled session is newer than the current session, all existing placement states are
    /// cleared and the dispatch queue's session is bumped before loading. Entries whose tasks are
    /// already buffered are ignored.
    ///
    /// A commit-ready or cleanup-ready entry marks its job as finalizing. A finalizing job no
    /// longer participates in regular-task scheduling: the job is removed from the active or
    /// pending set, its buffered ready tasks are discarded, and its incoming ready entries are
    /// ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::InvalidSessionId`] if the polled session is older than the current
    ///   session.
    /// * Forwards [`DispatchQueueSink::bump_session_id`]'s return values on failure.
    /// * Forwards [`Self::enqueue_commit_ready_entries`]'s return values on failure.
    /// * Forwards [`Self::enqueue_cleanup_ready_entries`]'s return values on failure.
    async fn ingest_inbound_entries(
        &mut self,
        curr_session_id: SessionId,
        storage_session_id: SessionId,
        ready_entries: Vec<InboundEntry>,
        commit_ready_entries: Vec<InboundEntry>,
        cleanup_ready_entries: Vec<InboundEntry>,
    ) -> Result<(), SchedulerError> {
        if storage_session_id < curr_session_id {
            return Err(SchedulerError::InvalidSessionId(storage_session_id));
        }
        if storage_session_id > curr_session_id {
            tracing::info!(
                curr_session_id = ? curr_session_id,
                storage_session_id = ? storage_session_id,
                "New session detected. Clearing existing placement state and bumping dispatch \
                 queue session."
            );
            self.storage_session_id = storage_session_id;
            self.clear();
            self.sink.bump_session_id(storage_session_id).await?;
        }

        // Load commit-ready tasks and cleanup-ready tasks first to avoid loading a job that is
        // already finalizing.
        self.enqueue_commit_ready_entries(commit_ready_entries)?;
        self.enqueue_cleanup_ready_entries(cleanup_ready_entries)?;
        self.enqueue_ready_entries(ready_entries);

        Ok(())
    }

    /// Enqueues polled commit-ready entries: each entry's job is marked finalizing, queued for a
    /// commit-task assignment, and removed from the active or pending set.
    ///
    /// Entries whose tasks are already buffered are ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::retire_active_job`]'s return values on failure.
    fn enqueue_commit_ready_entries(
        &mut self,
        commit_ready_entries: Vec<InboundEntry>,
    ) -> Result<(), SchedulerError> {
        for inbound_entry in commit_ready_entries {
            if !self
                .buffered_tasks
                .insert((inbound_entry.job_id, inbound_entry.task_id))
            {
                continue;
            }

            tracing::info!(
                job_id = ? inbound_entry.job_id,
                "Commit-ready task received. Finalizing job."
            );

            self.mark_job_finalizing(inbound_entry.job_id);
            self.commit_ready_jobs
                .push_back((inbound_entry.job_id, inbound_entry.resource_group_id));

            if self.active_jobs.contains_key(&inbound_entry.job_id) {
                self.retire_active_job(inbound_entry.job_id)?;
                continue;
            }

            if let Some(job_entry) = self.pending_jobs.remove(&inbound_entry.job_id) {
                self.discard_job_tasks(job_entry);
            }
        }

        Ok(())
    }

    /// Enqueues polled cleanup-ready entries: each entry's job is marked finalizing, queued for a
    /// cleanup-task assignment, and removed from the active or pending set.
    ///
    /// Entries whose tasks are already buffered are ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::retire_active_job`]'s return values on failure.
    fn enqueue_cleanup_ready_entries(
        &mut self,
        cleanup_ready_entries: Vec<InboundEntry>,
    ) -> Result<(), SchedulerError> {
        for inbound_entry in cleanup_ready_entries {
            if !self
                .buffered_tasks
                .insert((inbound_entry.job_id, inbound_entry.task_id))
            {
                continue;
            }

            tracing::info!(
                job_id = ? inbound_entry.job_id,
                "Cleanup-ready task received. Finalizing job."
            );

            self.mark_job_finalizing(inbound_entry.job_id);
            self.cleanup_ready_jobs
                .push_back((inbound_entry.job_id, inbound_entry.resource_group_id));

            if self.active_jobs.contains_key(&inbound_entry.job_id) {
                self.retire_active_job(inbound_entry.job_id)?;
                continue;
            }

            if let Some(job_entry) = self.pending_jobs.remove(&inbound_entry.job_id) {
                self.discard_job_tasks(job_entry);
            }
        }

        Ok(())
    }

    /// Enqueues polled regular ready entries into their jobs' task queues
    ///
    /// Entries of finalizing jobs and entries whose tasks are already buffered are ignored.
    fn enqueue_ready_entries(&mut self, ready_entries: Vec<InboundEntry>) {
        for inbound_entry in ready_entries {
            if self.finalizing_jobs.contains(&inbound_entry.job_id) {
                tracing::info!(
                    job_id = ? inbound_entry.job_id,
                    "Ready task received for a finalizing job. Ignored."
                );
                continue;
            }
            if !self
                .buffered_tasks
                .insert((inbound_entry.job_id, inbound_entry.task_id))
            {
                continue;
            }

            tracing::debug!(
                job_id = ? inbound_entry.job_id,
                task_id = ? inbound_entry.task_id,
                "Inbound task received."
            );

            if let Some(active_job) = self.active_jobs.get_mut(&inbound_entry.job_id) {
                active_job.enqueue(inbound_entry.task_id);
                continue;
            }
            if let Some(pending_job) = self.pending_jobs.get_mut(&inbound_entry.job_id) {
                pending_job.enqueue(inbound_entry.task_id);
                continue;
            }

            if self.active_jobs.len() < self.config.active_job_queue_capacity.get() {
                tracing::info!(
                    job_id = ? inbound_entry.job_id,
                    "New job received. Placing in active job queue."
                );
                self.active_jobs.insert(
                    inbound_entry.job_id,
                    JobTaskQueue::new(
                        inbound_entry.job_id,
                        inbound_entry.resource_group_id,
                        inbound_entry.task_id,
                    ),
                );
                self.rr_queue
                    .push(RoundRobinSlot::Job(inbound_entry.job_id));
                continue;
            }

            tracing::info!(
                job_id = ? inbound_entry.job_id,
                "New job received. Placing in pending job queue."
            );
            self.pending_jobs.insert(
                inbound_entry.job_id,
                JobTaskQueue::new(
                    inbound_entry.job_id,
                    inbound_entry.resource_group_id,
                    inbound_entry.task_id,
                ),
            );
            self.pending_job_queue.push_back(inbound_entry.job_id);
        }
    }

    /// Consumes the in-flight inbound poll if it has completed, ingesting its entries and starting
    /// the next poll; starts the initial poll if none is in flight.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`AsyncInboundQueueReader::try_collect_result`]'s return values on failure.
    /// * Forwards [`Self::ingest_inbound_entries`]'s return values on failure.
    /// * Forwards [`Self::start_inbound_poll`]'s return values on failure.
    async fn consume_inbound_poll_result(&mut self) -> Result<(), SchedulerError> {
        let curr_session_id = self.storage_session_id;
        let inbound_poll_state = self
            .inbound_queue_reader
            .try_collect_result(curr_session_id)
            .await?;
        match inbound_poll_state {
            InboundPollState::Ready {
                session_id: storage_session_id,
                ready_entries,
                commit_ready_entries,
                cleanup_ready_entries,
            } => {
                tracing::info!("Inbound poll completed.");
                self.ingest_inbound_entries(
                    curr_session_id,
                    storage_session_id,
                    ready_entries,
                    commit_ready_entries,
                    cleanup_ready_entries,
                )
                .await?;
                self.start_inbound_poll()?;
            }
            InboundPollState::Pending => {}
            InboundPollState::NotStarted => {
                self.start_inbound_poll()?;
            }
        }

        Ok(())
    }

    /// Makes scheduling decisions in round-robin order, writing task assignments to the dispatch
    /// queue until it reaches capacity or no buffered task is left.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::Internal`] if the round-robin queue is inconsistent with the scheduler's
    ///   job bookkeeping.
    /// * Forwards [`DispatchQueueSink::enqueue`]'s return values on failure.
    /// * Forwards [`Self::retire_active_job`]'s return values on failure.
    async fn make_schedule_decisions(&mut self) -> Result<(), SchedulerError> {
        let dispatch_slots = self
            .config
            .dispatch_queue_capacity
            .get()
            .saturating_sub(self.sink.size());
        let mut remaining_dispatch_slots = dispatch_slots;
        'fill_dispatch_queue: while remaining_dispatch_slots > 0 && !self.buffered_tasks.is_empty()
        {
            if self.rr_cursor >= self.rr_queue.len() {
                self.rr_cursor = 0;
            }
            let round_robin_queue_entry = match self.rr_queue.get(self.rr_cursor) {
                Some(entry) => entry.clone(),
                None => {
                    return Err(SchedulerError::Internal(
                        "round-robin cursor is corrupted".to_string(),
                    ));
                }
            };
            self.rr_cursor += 1;

            match round_robin_queue_entry {
                RoundRobinSlot::CleanupReady => {
                    let Some((job_id, resource_group_id)) = self.cleanup_ready_jobs.pop_front()
                    else {
                        continue;
                    };
                    self.sink
                        .enqueue(TaskAssignment {
                            id: self.id_issuer.next(),
                            job_id,
                            resource_group_id,
                            task_id: TaskId::Cleanup,
                        })
                        .await?;
                    self.buffered_tasks.remove(&(job_id, TaskId::Cleanup));
                    remaining_dispatch_slots -= 1;
                }
                RoundRobinSlot::CommitReady => {
                    for _ in 0..self.config.active_job_queue_capacity.get() {
                        if remaining_dispatch_slots == 0 {
                            break 'fill_dispatch_queue;
                        }
                        let Some((job_id, resource_group_id)) = self.commit_ready_jobs.pop_front()
                        else {
                            break;
                        };
                        self.sink
                            .enqueue(TaskAssignment {
                                id: self.id_issuer.next(),
                                job_id,
                                resource_group_id,
                                task_id: TaskId::Commit,
                            })
                            .await?;
                        self.buffered_tasks.remove(&(job_id, TaskId::Commit));
                        remaining_dispatch_slots -= 1;
                    }
                }
                RoundRobinSlot::Job(job_id) => {
                    let Some(job_entry) = self.active_jobs.get_mut(&job_id) else {
                        return Err(SchedulerError::Internal(format!(
                            "attempt to remove a non-existing active job: {job_id:?}"
                        )));
                    };
                    if let Some(task_id) = job_entry.dequeue() {
                        self.sink
                            .enqueue(TaskAssignment {
                                id: self.id_issuer.next(),
                                job_id,
                                resource_group_id: job_entry.resource_group_id,
                                task_id,
                            })
                            .await?;
                        self.buffered_tasks.remove(&(job_id, task_id));
                        remaining_dispatch_slots -= 1;
                    } else {
                        self.retire_active_job(job_id)?;
                    }
                }
            }
        }

        tracing::info!(
            dispatch_slots = dispatch_slots,
            num_task_assignments_enqueued = dispatch_slots - remaining_dispatch_slots,
            "Decision-making loop completed."
        );

        Ok(())
    }

    /// Starts a new asynchronous inbound poll, with per-lane entry limits derived from the
    /// remaining buffer capacities.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`AsyncInboundQueueReader::start`]'s return values on failure.
    fn start_inbound_poll(&mut self) -> Result<(), SchedulerError> {
        let num_commit_ready_tasks = self.commit_ready_jobs.len();
        let num_cleanup_ready_tasks = self.cleanup_ready_jobs.len();
        let max_commit_ready_entries = self
            .config
            .commit_ready_task_capacity
            .get()
            .saturating_sub(num_commit_ready_tasks);
        let max_cleanup_ready_entries = self
            .config
            .cleanup_ready_task_capacity
            .get()
            .saturating_sub(num_cleanup_ready_tasks);
        let max_ready_entries = self.config.ready_task_capacity.get().saturating_sub(
            self.buffered_tasks.len() - num_commit_ready_tasks - num_cleanup_ready_tasks,
        );

        self.inbound_queue_reader.start(
            Duration::from_millis(self.config.storage_poll_timeout_ms),
            max_ready_entries,
            max_commit_ready_entries,
            max_cleanup_ready_entries,
        )
    }
}

/// A slot in the round-robin rotation that the scheduler draws task assignments from.
#[derive(Clone)]
pub(super) enum RoundRobinSlot {
    /// An active job: assignments are drawn from the job's buffered ready tasks.
    Job(JobId),

    /// The commit lane: assignments are drawn from the buffered commit-ready jobs.
    CommitReady,

    /// The cleanup lane: assignments are drawn from the buffered cleanup-ready jobs.
    CleanupReady,
}

/// The state of an asynchronous inbound-queue poll.
enum InboundPollState {
    /// The poll has completed, carrying the polled session and the entries drained from each
    /// inbound-queue lane.
    Ready {
        session_id: SessionId,
        ready_entries: Vec<InboundEntry>,
        commit_ready_entries: Vec<InboundEntry>,
        cleanup_ready_entries: Vec<InboundEntry>,
    },

    /// The poll is still in flight.
    Pending,

    /// No poll has been started.
    NotStarted,
}

/// The join handles of one in-flight inbound poll, one per inbound-queue lane.
#[allow(clippy::struct_field_names)]
struct InboundPollHandles {
    ready_handle:
        tokio::task::JoinHandle<Result<(SessionId, Vec<InboundEntry>), StorageClientError>>,
    commit_ready_handle:
        tokio::task::JoinHandle<Result<(SessionId, Vec<InboundEntry>), StorageClientError>>,
    cleanup_ready_handle:
        tokio::task::JoinHandle<Result<(SessionId, Vec<InboundEntry>), StorageClientError>>,
}

impl InboundPollHandles {
    /// Tries to collect the results of all lane polls without blocking.
    ///
    /// Entries from lanes that report an older session than the latest observed session are
    /// dropped.
    ///
    /// # Returns
    ///
    /// On success:
    ///
    /// * [`InboundPollState::Pending`] if any lane poll is still in flight.
    /// * [`InboundPollState::Ready`] with the latest observed session and its entries otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::Internal`] if any lane's polling task fails to join.
    /// * Forwards [`SchedulerStorageClient::poll_ready`]'s return values on failure.
    /// * Forwards [`SchedulerStorageClient::poll_commit_ready`]'s return values on failure.
    /// * Forwards [`SchedulerStorageClient::poll_cleanup_ready`]'s return values on failure.
    async fn try_collect_result(
        &mut self,
        curr_session_id: SessionId,
    ) -> Result<InboundPollState, SchedulerError> {
        if !self.ready_handle.is_finished()
            || !self.commit_ready_handle.is_finished()
            || !self.cleanup_ready_handle.is_finished()
        {
            return Ok(InboundPollState::Pending);
        }

        let (ready_session_id, ready_entries) = (&mut self.ready_handle)
            .await
            .map_err(|e| SchedulerError::Internal(e.to_string()))??;
        let (commit_session_id, commit_ready_entries) = (&mut self.commit_ready_handle)
            .await
            .map_err(|e| SchedulerError::Internal(e.to_string()))??;
        let (cleanup_session_id, cleanup_ready_entries) =
            (&mut self.cleanup_ready_handle)
                .await
                .map_err(|e| SchedulerError::Internal(e.to_string()))??;

        let latest_session_id = curr_session_id
            .max(ready_session_id)
            .max(commit_session_id)
            .max(cleanup_session_id);

        Ok(InboundPollState::Ready {
            session_id: latest_session_id,
            ready_entries: Self::drop_if_stale(ready_session_id, latest_session_id, ready_entries),
            commit_ready_entries: Self::drop_if_stale(
                commit_session_id,
                latest_session_id,
                commit_ready_entries,
            ),
            cleanup_ready_entries: Self::drop_if_stale(
                cleanup_session_id,
                latest_session_id,
                cleanup_ready_entries,
            ),
        })
    }

    /// # Returns
    ///
    /// `entries` if `session_id` matches `latest_session_id`, or an empty vector otherwise.
    fn drop_if_stale(
        session_id: SessionId,
        latest_session_id: SessionId,
        entries: Vec<InboundEntry>,
    ) -> Vec<InboundEntry> {
        if session_id == latest_session_id {
            entries
        } else {
            Vec::new()
        }
    }
}

/// A reader that runs inbound-queue polls as background tasks, with at most one polling request
/// (from all three lanes) in flight at a time.
///
/// # Type Parameters
///
/// * `StorageClientType` - The storage client used to poll the inbound queue.
pub(super) struct AsyncInboundQueueReader<StorageClientType: SchedulerStorageClient + 'static> {
    storage_client: StorageClientType,
    handle: Option<InboundPollHandles>,
}

impl<StorageClientType: SchedulerStorageClient + 'static>
    AsyncInboundQueueReader<StorageClientType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A new reader with no poll in flight.
    const fn new(storage_client: StorageClientType) -> Self {
        Self {
            storage_client,
            handle: None,
        }
    }

    /// Tries to collect the result of the in-flight poll without blocking, releasing the poll
    /// handles once a result is produced.
    ///
    /// # Returns
    ///
    /// On success:
    ///
    /// * [`InboundPollState::NotStarted`] if no poll is in flight.
    /// * Forwards [`InboundPollHandles::try_collect_result`]'s return values otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`InboundPollHandles::try_collect_result`]'s return values on failure.
    async fn try_collect_result(
        &mut self,
        curr_session_id: SessionId,
    ) -> Result<InboundPollState, SchedulerError> {
        match &mut self.handle {
            None => Ok(InboundPollState::NotStarted),
            Some(handle) => {
                let inbound_poll_state = handle.try_collect_result(curr_session_id).await?;
                if !matches!(inbound_poll_state, InboundPollState::Pending) {
                    self.handle = None;
                }
                Ok(inbound_poll_state)
            }
        }
    }

    /// Starts a new inbound poll, polling each inbound-queue lane as a background task.
    ///
    /// Lanes whose entry limit is 0 are not polled; if all limits are 0, no poll is started.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::Internal`] if a poll is already in flight.
    fn start(
        &mut self,
        storage_poll_timeout: Duration,
        max_ready_entries: usize,
        max_commit_ready_entries: usize,
        max_cleanup_ready_entries: usize,
    ) -> Result<(), SchedulerError> {
        if self.handle.is_some() {
            return Err(SchedulerError::Internal(
                "inbound poll handle already exists".to_string(),
            ));
        }

        if max_ready_entries == 0 && max_commit_ready_entries == 0 && max_cleanup_ready_entries == 0
        {
            tracing::info!("Inbound poll skipped: all entry limits are 0.");
            return Ok(());
        }

        let ready_storage_client = self.storage_client.clone();
        let ready_handle = tokio::task::spawn(async move {
            if max_ready_entries == 0 {
                return Ok((0, Vec::new()));
            }
            ready_storage_client
                .poll_ready(max_ready_entries, storage_poll_timeout)
                .await
        });

        let commit_ready_storage_client = self.storage_client.clone();
        let commit_ready_handle = tokio::task::spawn(async move {
            if max_commit_ready_entries == 0 {
                return Ok((0, Vec::new()));
            }
            commit_ready_storage_client
                .poll_commit_ready(max_commit_ready_entries, storage_poll_timeout)
                .await
        });

        let cleanup_ready_storage_client = self.storage_client.clone();
        let cleanup_ready_handle = tokio::task::spawn(async move {
            if max_cleanup_ready_entries == 0 {
                return Ok((0, Vec::new()));
            }
            cleanup_ready_storage_client
                .poll_cleanup_ready(max_cleanup_ready_entries, storage_poll_timeout)
                .await
        });

        self.handle = Some(InboundPollHandles {
            ready_handle,
            commit_ready_handle,
            cleanup_ready_handle,
        });

        tracing::info!(
            max_ready_entries = ? max_ready_entries,
            max_commit_ready_entries = ? max_commit_ready_entries,
            max_cleanup_ready_entries = ? max_cleanup_ready_entries,
            "Inbound poll initiated."
        );

        Ok(())
    }
}
