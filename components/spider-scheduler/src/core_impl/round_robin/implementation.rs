//! The implementation of the round-robin scheduler core. See the parent module's documentation for
//! the scheduling policy and configuration.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use async_trait::async_trait;
use serde::Deserialize;
use spider_core::types::id::{JobId, ResourceGroupId, SessionId, TaskId};
use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::{
    DispatchQueueSink,
    InboundEntry,
    SchedulerCore,
    SchedulerError,
    SchedulerStorageClient,
    StorageClientError,
    TaskAssignment,
};

/// The configuration of the round-robin scheduler core.
///
/// The configuration itself implements [`SchedulerCore`]: consuming it through
/// [`SchedulerCore::run`] creates the underlying scheduler and drives its scheduling loop.
///
/// # Type Parameters
///
/// * `SchedulerStorageClientType` - The storage client used to poll the inbound queue.
/// * `DispatchQueueSinkType` - The dispatch sink that task assignments are written to.
#[derive(Deserialize)]
pub struct RoundRobinConfig<
    SchedulerStorageClientType: SchedulerStorageClient + 'static,
    DispatchQueueSinkType: DispatchQueueSink,
> {
    /// The capacity of the active job queue. The scheduler will make task assignments from these
    /// jobs in a round-robin manner.
    ///
    /// Must be greater than 0.
    pub active_job_queue_capacity: usize,

    /// The capacity of the dispatch queue.
    ///
    /// Must be greater than 0.
    pub dispatch_queue_capacity: usize,

    /// The capacity of the total pending ready tasks buffered in the scheduler.
    ///
    /// Must be greater than 0.
    pub ready_task_capacity: usize,

    /// The capacity of the total pending commit-ready tasks buffered in the scheduler.
    ///
    /// Must be greater than 0.
    pub commit_ready_task_capacity: usize,

    /// The capacity of the total pending cleanup-ready tasks buffered in the scheduler.
    ///
    /// Must be greater than 0.
    pub cleanup_ready_task_capacity: usize,

    /// The maximum time (in milliseconds) that the scheduler will wait for the storage server to
    /// fill the inbound-queue reading request.
    pub storage_poll_timeout_ms: u64,

    /// The time (in milliseconds) that the scheduler will spend on each tick.
    pub tick_interval_ms: u64,

    #[serde(skip)]
    _marker: std::marker::PhantomData<(SchedulerStorageClientType, DispatchQueueSinkType)>,
}

#[async_trait]
impl<
    SchedulerStorageClientType: SchedulerStorageClient + 'static,
    DispatchQueueSinkType: DispatchQueueSink,
> SchedulerCore for RoundRobinConfig<SchedulerStorageClientType, DispatchQueueSinkType>
{
    type Sink = DispatchQueueSinkType;
    type StorageClient = SchedulerStorageClientType;

    async fn run(
        self,
        storage_client: Self::StorageClient,
        sink: Self::Sink,
        cancellation_token: CancellationToken,
    ) -> Result<(), SchedulerError> {
        RoundRobin::new(
            SessionId::default(),
            storage_client,
            sink,
            cancellation_token,
            self,
        )?
        .run()
        .await
    }
}

/// A FIFO queue of a job's buffered ready tasks.
struct JobTaskQueue {
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

/// A slot in the round-robin rotation that the scheduler draws task assignments from.
#[derive(Clone)]
enum RoundRobinSlot {
    /// An active job: assignments are drawn from the job's buffered ready tasks.
    Job(JobId),

    /// The commit lane: assignments are drawn from the buffered commit-ready jobs.
    CommitReady,

    /// The cleanup lane: assignments are drawn from the buffered cleanup-ready jobs.
    CleanupReady,
}

/// The round-robin scheduler core created from a [`RoundRobinConfig`].
///
/// # Type Parameters
///
/// * `SchedulerStorageClientType` - The storage client used to poll the inbound queue.
/// * `DispatchQueueSinkType` - The dispatch sink that task assignments are written to.
struct RoundRobin<
    SchedulerStorageClientType: SchedulerStorageClient + 'static,
    DispatchQueueSinkType: DispatchQueueSink,
> {
    sink: DispatchQueueSinkType,
    cancellation_token: CancellationToken,
    config: RoundRobinConfig<SchedulerStorageClientType, DispatchQueueSinkType>,
    storage_session_id: SessionId,
    buffered_tasks: HashSet<(JobId, TaskId)>,

    active_jobs: HashMap<JobId, JobTaskQueue>,
    active_job_queue: Vec<RoundRobinSlot>,
    active_job_queue_round_robin_cursor: usize,

    pending_jobs: HashMap<JobId, JobTaskQueue>,
    pending_job_queue: VecDeque<JobId>,

    commit_ready_jobs: VecDeque<(JobId, ResourceGroupId)>,
    cleanup_ready_jobs: VecDeque<(JobId, ResourceGroupId)>,

    finalizing_jobs: HashSet<JobId>,

    inbound_queue_reader: AsyncInboundQueueReader<SchedulerStorageClientType>,
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
    /// The constructed [`RoundRobin`] scheduler on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::InvalidConfig`] if the config contains invalid values. Check
    ///   [`RoundRobinConfig`]'s docstring for details.
    fn new(
        storage_session_id: SessionId,
        storage_client: SchedulerStorageClientType,
        sink: DispatchQueueSinkType,
        cancellation_token: CancellationToken,
        config: RoundRobinConfig<SchedulerStorageClientType, DispatchQueueSinkType>,
    ) -> Result<Self, SchedulerError> {
        if config.active_job_queue_capacity == 0 {
            return Err(SchedulerError::InvalidConfig(
                "`active_job_queue_capacity` must be greater than 0".to_string(),
            ));
        }

        if config.dispatch_queue_capacity == 0 {
            return Err(SchedulerError::InvalidConfig(
                "`dispatch_queue_capacity` must be greater than 0".to_string(),
            ));
        }

        if config.ready_task_capacity == 0 {
            return Err(SchedulerError::InvalidConfig(
                "`ready_task_capacity` must be greater than 0".to_string(),
            ));
        }

        if config.commit_ready_task_capacity == 0 {
            return Err(SchedulerError::InvalidConfig(
                "`commit_ready_task_capacity` must be greater than 0".to_string(),
            ));
        }

        if config.cleanup_ready_task_capacity == 0 {
            return Err(SchedulerError::InvalidConfig(
                "`cleanup_ready_task_capacity` must be greater than 0".to_string(),
            ));
        }

        let buffered_tasks = HashSet::with_capacity(config.ready_task_capacity);
        let active_jobs = HashMap::with_capacity(config.active_job_queue_capacity);
        let active_job_queue = Self::new_active_job_queue(config.active_job_queue_capacity);
        let round_robin_cursor = 0;
        let pending_jobs = HashMap::with_capacity(config.active_job_queue_capacity);
        let pending_job_queue = VecDeque::with_capacity(config.active_job_queue_capacity);
        let commit_ready_jobs = VecDeque::with_capacity(config.commit_ready_task_capacity);
        let cleanup_ready_jobs = VecDeque::with_capacity(config.cleanup_ready_task_capacity);
        let finalizing_jobs = HashSet::with_capacity(
            config.commit_ready_task_capacity + config.cleanup_ready_task_capacity,
        );
        let inbound_queue_reader = AsyncInboundQueueReader::new(storage_client);
        Ok(Self {
            sink,
            cancellation_token,
            config,
            storage_session_id,
            buffered_tasks,
            active_jobs,
            active_job_queue,
            active_job_queue_round_robin_cursor: round_robin_cursor,
            pending_jobs,
            pending_job_queue,
            commit_ready_jobs,
            cleanup_ready_jobs,
            finalizing_jobs,
            inbound_queue_reader,
        })
    }

    /// # Returns
    ///
    /// A new active job queue containing only the commit-ready and cleanup-ready slots.
    fn new_active_job_queue(active_job_pool_capacity: usize) -> Vec<RoundRobinSlot> {
        let mut active_job_queue = Vec::with_capacity(active_job_pool_capacity + 2);
        active_job_queue.push(RoundRobinSlot::CommitReady);
        active_job_queue.push(RoundRobinSlot::CleanupReady);
        active_job_queue
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
        let tick_interval = Duration::from_millis(self.config.tick_interval_ms);
        loop {
            let now = tokio::time::Instant::now();
            let cancellation_token = self.cancellation_token.clone();
            select! {
                () = cancellation_token.cancelled() => {
                    return Ok(());
                }
                result = self.tick() => {
                    let () = result?;
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

        self.active_job_queue = Self::new_active_job_queue(self.config.active_job_queue_capacity);
        self.active_job_queue_round_robin_cursor = 0;
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
        if let Some(index) = self.active_job_queue.iter().position(|entry| match entry {
            RoundRobinSlot::Job(id) => *id == job_id,
            _ => false,
        }) {
            self.active_job_queue.swap_remove(index);
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
            self.active_job_queue
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
        for task_id in job_entry.task_ids {
            self.buffered_tasks.remove(&(job_entry.job_id, task_id));
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
    async fn tick(&mut self) -> Result<(), SchedulerError> {
        self.consume_inbound_poll_result().await?;
        self.make_schedule_decisions().await?;
        Ok(())
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
    /// * Forwards [`Self::retire_active_job`]'s return values on failure.
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
            self.storage_session_id = storage_session_id;
            self.clear();
            self.sink.bump_session_id(storage_session_id).await?;
        }

        // Load commit ready tasks and cleanup ready tasks first to avoid loading a job that
        // is already cancelled or commit-ready.
        for inbound_entry in commit_ready_entries {
            if !self
                .buffered_tasks
                .insert((inbound_entry.job_id, inbound_entry.task_id))
            {
                continue;
            }
            self.finalizing_jobs.insert(inbound_entry.job_id);
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

        for inbound_entry in cleanup_ready_entries {
            if !self
                .buffered_tasks
                .insert((inbound_entry.job_id, inbound_entry.task_id))
            {
                continue;
            }
            self.finalizing_jobs.insert(inbound_entry.job_id);
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

        for inbound_entry in ready_entries {
            if self.finalizing_jobs.contains(&inbound_entry.job_id) {
                continue;
            }
            if !self
                .buffered_tasks
                .insert((inbound_entry.job_id, inbound_entry.task_id))
            {
                continue;
            }
            if let Some(active_job) = self.active_jobs.get_mut(&inbound_entry.job_id) {
                active_job.enqueue(inbound_entry.task_id);
                continue;
            }
            if let Some(pending_job) = self.pending_jobs.get_mut(&inbound_entry.job_id) {
                pending_job.enqueue(inbound_entry.task_id);
                continue;
            }
            if self.active_jobs.len() < self.config.active_job_queue_capacity {
                self.active_jobs.insert(
                    inbound_entry.job_id,
                    JobTaskQueue::new(
                        inbound_entry.job_id,
                        inbound_entry.resource_group_id,
                        inbound_entry.task_id,
                    ),
                );
                self.active_job_queue
                    .push(RoundRobinSlot::Job(inbound_entry.job_id));
                continue;
            }
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

        Ok(())
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
        let mut remaining_dispatch_slots = self
            .config
            .dispatch_queue_capacity
            .saturating_sub(self.sink.size());
        while remaining_dispatch_slots > 0 && !self.buffered_tasks.is_empty() {
            if self.active_job_queue_round_robin_cursor >= self.active_job_queue.len() {
                self.active_job_queue_round_robin_cursor = 0;
            }
            let active_job_queue_entry = match self
                .active_job_queue
                .get(self.active_job_queue_round_robin_cursor)
            {
                Some(entry) => entry.clone(),
                None => {
                    return Err(SchedulerError::Internal(
                        "round-robin cursor is corrupted".to_string(),
                    ));
                }
            };
            self.active_job_queue_round_robin_cursor += 1;

            match active_job_queue_entry {
                RoundRobinSlot::CleanupReady => {
                    let Some((job_id, resource_group_id)) = self.cleanup_ready_jobs.pop_front()
                    else {
                        continue;
                    };
                    self.sink
                        .enqueue(TaskAssignment {
                            job_id,
                            resource_group_id,
                            task_id: TaskId::Cleanup,
                        })
                        .await?;
                    self.buffered_tasks.remove(&(job_id, TaskId::Cleanup));
                    self.finalizing_jobs.remove(&job_id);
                    remaining_dispatch_slots -= 1;
                }
                RoundRobinSlot::CommitReady => {
                    let Some((job_id, resource_group_id)) = self.commit_ready_jobs.pop_front()
                    else {
                        continue;
                    };
                    self.sink
                        .enqueue(TaskAssignment {
                            job_id,
                            resource_group_id,
                            task_id: TaskId::Commit,
                        })
                        .await?;
                    self.buffered_tasks.remove(&(job_id, TaskId::Commit));
                    self.finalizing_jobs.remove(&job_id);
                    remaining_dispatch_slots -= 1;
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
            .saturating_sub(num_commit_ready_tasks);
        let max_cleanup_ready_entries = self
            .config
            .cleanup_ready_task_capacity
            .saturating_sub(num_cleanup_ready_tasks);
        let max_ready_entries = self.config.ready_task_capacity.saturating_sub(
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
struct AsyncInboundQueueReader<StorageClientType: SchedulerStorageClient + 'static> {
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

        Ok(())
    }
}
