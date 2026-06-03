use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use spider_core::types::id::{JobId, ResourceGroupId, SessionId, TaskId};
use tokio::select;
use tokio_util::sync::CancellationToken;
use serde::Deserialize;
use crate::{
    DispatchQueueSink,
    InboundEntry,
    SchedulerCore,
    SchedulerError,
    SchedulerStorageClient,
    StorageClientError,
    TaskAssignment,
};

#[derive(Deserialize)]
pub struct RoundRobinConfig<
    SchedulerStorageClientType: SchedulerStorageClient + 'static,
    DispatchQueueSinkType: DispatchQueueSink,
> {
    /// The capacity of the active jobs pool. The scheduler will make task assignments from these
    /// jobs in a round-robin manner.
    pub active_job_pool_capacity: usize,

    /// The capacity of the dispatch queue.
    pub dispatch_queue_capacity: usize,

    /// The capacity of the total pending ready tasks buffered in the scheduler.
    pub ready_task_capacity: usize,

    /// The capacity of the total pending commit-ready tasks buffered in the scheduler.
    pub commit_ready_task_capacity: usize,

    /// The capacity of the total pending cleanup-ready tasks buffered in the scheduler.
    pub cleanup_ready_task_capacity: usize,

    pub storage_polling_wait_time_ms: u64,

    #[serde(skip)]
    metrics: Arc<RoundRobinMetrics>,

    #[serde(skip)]
    _marker: std::marker::PhantomData<(SchedulerStorageClientType, DispatchQueueSinkType)>,
}

/// Instrumentation counters for the round-robin scheduling loop.
///
/// Durations are accumulated in nanoseconds; an average is a `*_ns` total divided by its matching
/// `*_count`. All counters use [`Ordering::Relaxed`] and are meant for coarse profiling only, not
/// for establishing happens-before relationships.
#[derive(Debug, Default)]
pub struct RoundRobinMetrics {
    /// Number of completed scheduling-loop iterations (`loop_once` calls).
    pub loop_count: AtomicU64,

    /// Total wall-clock time spent across all scheduling-loop iterations.
    pub total_loop_ns: AtomicU64,

    /// Number of iterations that processed a fresh inbound polling result.
    pub buffer_enrich_count: AtomicU64,

    /// Total time spent draining inbound polling results into the scheduler's buffers ("enrich the
    /// buffer", stage 1).
    pub buffer_enrich_ns: AtomicU64,

    /// Number of iterations that dispatched at least one assignment.
    pub dispatch_enrich_count: AtomicU64,

    /// Total time spent making scheduling decisions and filling the dispatch queue ("enrich the
    /// dispatch queue", stage 2).
    pub dispatch_enrich_ns: AtomicU64,

    /// When set, the scheduling loop stops accumulating any of the counters above. Used to exclude
    /// the idle tail (after all work has drained) from the averages.
    stopped: AtomicBool,
}

impl RoundRobinMetrics {
    /// Freezes all counters: subsequent scheduling-loop iterations are not recorded.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }

    /// # Returns
    ///
    /// Whether the counters are still being recorded.
    fn is_recording(&self) -> bool {
        !self.stopped.load(Ordering::Relaxed)
    }
}

impl<
    SchedulerStorageClientType: SchedulerStorageClient + 'static,
    DispatchQueueSinkType: DispatchQueueSink,
> RoundRobinConfig<SchedulerStorageClientType, DispatchQueueSinkType>
{
    /// Creates a new round-robin configuration with a fresh, empty set of metrics.
    #[must_use]
    pub fn new(
        active_job_pool_capacity: usize,
        dispatch_queue_capacity: usize,
        ready_task_capacity: usize,
        commit_ready_task_capacity: usize,
        cleanup_ready_task_capacity: usize,
        storage_polling_wait_time_ms: u64,
    ) -> Self {
        Self {
            active_job_pool_capacity,
            dispatch_queue_capacity,
            ready_task_capacity,
            commit_ready_task_capacity,
            cleanup_ready_task_capacity,
            storage_polling_wait_time_ms,
            metrics: Arc::new(RoundRobinMetrics::default()),
            _marker: std::marker::PhantomData,
        }
    }

    /// # Returns
    ///
    /// A shared handle to the loop instrumentation counters, so callers can read them while (or
    /// after) the scheduler runs.
    #[must_use]
    pub fn metrics(&self) -> Arc<RoundRobinMetrics> {
        Arc::clone(&self.metrics)
    }
}

/// # Returns
///
/// The time elapsed since `start` in nanoseconds, saturating at [`u64::MAX`].
fn elapsed_nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[async_trait]
impl<
    SchedulerStorageClientType: SchedulerStorageClient + 'static,
    DispatchQueueSinkType: DispatchQueueSink,
> SchedulerCore for RoundRobinConfig<SchedulerStorageClientType, DispatchQueueSinkType>
{
    type StorageClient = SchedulerStorageClientType;
    type Sink = DispatchQueueSinkType;

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
        )
        .run()
        .await
    }
}

struct JobEntry {
    job_id: JobId,
    resource_group_id: ResourceGroupId,
    task_ids: VecDeque<TaskId>,
}

impl JobEntry {
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

    fn dequeue(&mut self) -> Option<TaskId> {
        self.task_ids.pop_front()
    }
}

#[derive(Clone)]
enum ActiveJobQueueEntry {
    Ready(JobId),
    CommitReady,
    CleanupReady,
}

struct RoundRobin<
    SchedulerStorageClientType: SchedulerStorageClient + 'static,
    DispatchQueueSinkType: DispatchQueueSink,
> {
    storage_client: SchedulerStorageClientType,
    sink: DispatchQueueSinkType,
    cancellation_token: CancellationToken,
    config: RoundRobinConfig<SchedulerStorageClientType, DispatchQueueSinkType>,
    storage_session_id: SessionId,
    ready_set: HashSet<(JobId, TaskId)>,

    active_jobs: HashMap<JobId, JobEntry>,
    active_job_queue: Vec<ActiveJobQueueEntry>,
    active_job_queue_cursor: usize,

    pending_jobs: HashMap<JobId, JobEntry>,
    pending_job_queue: VecDeque<JobId>,

    commit_ready_queue: VecDeque<(JobId, ResourceGroupId)>,
    cleanup_ready_queue: VecDeque<(JobId, ResourceGroupId)>,

    commit_ready_or_cleanup_ready_tasks: HashSet<JobId>,

    inbound_queue_reader: AsyncInboundQueueReader<SchedulerStorageClientType>,
}

impl<
    SchedulerStorageClientType: SchedulerStorageClient + 'static,
    DispatchQueueSinkType: DispatchQueueSink,
> RoundRobin<SchedulerStorageClientType, DispatchQueueSinkType>
{
    fn new(
        storage_session_id: SessionId,
        storage_client: SchedulerStorageClientType,
        sink: DispatchQueueSinkType,
        cancellation_token: CancellationToken,
        config: RoundRobinConfig<SchedulerStorageClientType, DispatchQueueSinkType>,
    ) -> Self {
        let ready_set = HashSet::with_capacity(config.ready_task_capacity);
        let active_jobs = HashMap::with_capacity(config.active_job_pool_capacity);
        let active_job_queue = Self::new_active_job_queue(config.active_job_pool_capacity);
        let active_job_queue_cursor = 0;
        let pending_jobs = HashMap::with_capacity(config.active_job_pool_capacity);
        let pending_job_queue = VecDeque::with_capacity(config.active_job_pool_capacity);
        let commit_ready_queue = VecDeque::with_capacity(config.commit_ready_task_capacity);
        let cleanup_ready_queue = VecDeque::with_capacity(config.cleanup_ready_task_capacity);
        let commit_ready_or_cleanup_ready_tasks = HashSet::with_capacity(
            config.commit_ready_task_capacity + config.cleanup_ready_task_capacity,
        );
        let inbound_queue_reader = AsyncInboundQueueReader::new(storage_client.clone());
        Self {
            storage_client,
            sink,
            cancellation_token,
            config,
            storage_session_id,
            ready_set,
            active_jobs,
            active_job_queue,
            active_job_queue_cursor,
            pending_jobs,
            pending_job_queue,
            commit_ready_queue,
            cleanup_ready_queue,
            commit_ready_or_cleanup_ready_tasks,
            inbound_queue_reader,
        }
    }

    fn new_active_job_queue(active_job_pool_capacity: usize) -> Vec<ActiveJobQueueEntry> {
        let mut active_job_queue = Vec::with_capacity(active_job_pool_capacity + 2);
        active_job_queue.push(ActiveJobQueueEntry::CommitReady);
        active_job_queue.push(ActiveJobQueueEntry::CleanupReady);
        active_job_queue
    }

    async fn run(mut self) -> Result<(), SchedulerError> {
        loop {
            let cancellation_token = self.cancellation_token.clone();
            select! {
                () = cancellation_token.cancelled() => {
                    return Ok(());
                }
                result = self.loop_once() => {
                    let () = result?;
                }
            }
        }
    }

    fn clear_all_placement(&mut self) {
        self.ready_set.clear();
        self.active_jobs.clear();
        self.pending_jobs.clear();
        self.pending_job_queue.clear();
        self.commit_ready_queue.clear();
        self.cleanup_ready_queue.clear();
        self.commit_ready_or_cleanup_ready_tasks.clear();

        self.active_job_queue = Self::new_active_job_queue(self.config.active_job_pool_capacity);
        self.active_job_queue_cursor = 0;
    }

    fn remove_active_job_and_dequeue_next_pending_job(
        &mut self,
        job_id: JobId,
    ) -> Result<(), SchedulerError> {
        if let Some(index) = self.active_job_queue.iter().position(|entry| match entry {
            ActiveJobQueueEntry::Ready(id) => *id == job_id,
            _ => false,
        }) {
            self.active_job_queue.swap_remove(index);
        } else {
            return Err(SchedulerError::Internal(
                "attempt to remove a non-existing active job: {job_id:?}".to_string(),
            ));
        }

        if let Some(entry_to_remove) = self.active_jobs.remove(&job_id) {
            self.destroy_job_entry(entry_to_remove);
        } else {
            return Err(SchedulerError::Internal(
                "attempt to destroy a non-existing active job: {job_id:?}".to_string(),
            ));
        }

        if let Some(next_pending_job) = self.next_pending_job() {
            self.active_job_queue
                .push(ActiveJobQueueEntry::Ready(next_pending_job.job_id));
            self.active_jobs
                .insert(next_pending_job.job_id, next_pending_job);
        }
        Ok(())
    }

    fn next_pending_job(&mut self) -> Option<JobEntry> {
        loop {
            let job_id = self.pending_job_queue.pop_front()?;
            // NOTE: The job may have been cancelled and removed from `pending_jobs`, so the ID in
            // the queue may not necessarily exist in `pending_jobs`.
            if let Some(pending_job) = self.pending_jobs.remove(&job_id) {
                return Some(pending_job);
            }
        }
    }

    fn destroy_job_entry(&mut self, job_entry: JobEntry) {
        for task_id in job_entry.task_ids {
            self.ready_set.remove(&(job_entry.job_id, task_id));
        }
    }

    async fn loop_once(&mut self) -> Result<(), SchedulerError> {
        let loop_start = Instant::now();
        let recording = self.config.metrics.is_recording();

        // Stage 1: Retrieve inbound queue results
        let curr_session_id = self.storage_session_id;
        let inbound_queue_result = self
            .inbound_queue_reader
            .poll_ready(curr_session_id)
            .await?;
        match inbound_queue_result {
            InboundQueueResult::Result {
                session_id,
                ready_entries,
                commit_ready_entries,
                cleanup_ready_entries,
            } => {
                let buffer_start = Instant::now();
                let inbound_entry_count =
                    ready_entries.len() + commit_ready_entries.len() + cleanup_ready_entries.len();
                if session_id < curr_session_id {
                    return Err(SchedulerError::InvalidSessionId(session_id));
                }
                if session_id > curr_session_id {
                    self.storage_session_id = session_id;
                    self.clear_all_placement();
                    self.sink.bump_session_id(session_id).await?;
                }

                // Load commit ready tasks and cleanup ready tasks first to avoid loading a job that
                // is already cancelled or commit-ready.
                for inbound_entry in commit_ready_entries {
                    if !self
                        .ready_set
                        .insert((inbound_entry.job_id, inbound_entry.task_id))
                    {
                        continue;
                    }
                    self.commit_ready_or_cleanup_ready_tasks
                        .insert(inbound_entry.job_id);
                    self.commit_ready_queue
                        .push_back((inbound_entry.job_id, inbound_entry.resource_group_id));

                    if self.active_jobs.contains_key(&inbound_entry.job_id) {
                        self.remove_active_job_and_dequeue_next_pending_job(inbound_entry.job_id)?;
                        continue;
                    }

                    if let Some(job_entry) = self.pending_jobs.remove(&inbound_entry.job_id) {
                        self.destroy_job_entry(job_entry);
                    }
                }

                for inbound_entry in cleanup_ready_entries {
                    if !self
                        .ready_set
                        .insert((inbound_entry.job_id, inbound_entry.task_id))
                    {
                        continue;
                    }
                    self.commit_ready_or_cleanup_ready_tasks
                        .insert(inbound_entry.job_id);
                    self.cleanup_ready_queue
                        .push_back((inbound_entry.job_id, inbound_entry.resource_group_id));

                    if self.active_jobs.contains_key(&inbound_entry.job_id) {
                        self.remove_active_job_and_dequeue_next_pending_job(inbound_entry.job_id)?;
                        continue;
                    }

                    if let Some(job_entry) = self.pending_jobs.remove(&inbound_entry.job_id) {
                        self.destroy_job_entry(job_entry);
                    }
                }

                for inbound_entry in ready_entries {
                    if self
                        .commit_ready_or_cleanup_ready_tasks
                        .contains(&inbound_entry.job_id)
                    {
                        continue;
                    }
                    if !self
                        .ready_set
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
                    if self.active_jobs.len() < self.config.active_job_pool_capacity {
                        self.active_jobs.insert(
                            inbound_entry.job_id,
                            JobEntry::new(
                                inbound_entry.job_id,
                                inbound_entry.resource_group_id,
                                inbound_entry.task_id,
                            ),
                        );
                        self.active_job_queue
                            .push(ActiveJobQueueEntry::Ready(inbound_entry.job_id));
                        continue;
                    }
                    self.pending_jobs.insert(
                        inbound_entry.job_id,
                        JobEntry::new(
                            inbound_entry.job_id,
                            inbound_entry.resource_group_id,
                            inbound_entry.task_id,
                        ),
                    );
                    self.pending_job_queue.push_back(inbound_entry.job_id);
                }

                // Only record iterations that actually had entries to enrich, so the average
                // reflects real work rather than empty polls when the scheduler is idle.
                if recording && inbound_entry_count > 0 {
                    self.config
                        .metrics
                        .buffer_enrich_ns
                        .fetch_add(elapsed_nanos(buffer_start), Ordering::Relaxed);
                    self.config
                        .metrics
                        .buffer_enrich_count
                        .fetch_add(1, Ordering::Relaxed);
                }

                self.spawn_inbound_queue_reader();
            }
            InboundQueueResult::ResultNotReady => {}
            InboundQueueResult::HandleNotSpawned => {
                self.spawn_inbound_queue_reader();
            }
        }

        // Stage 2: Make scheduling decisions to fill the dispatch queue
        let dispatch_start = Instant::now();
        let mut dispatch_queue_slots = self
            .config
            .dispatch_queue_capacity
            .saturating_sub(self.sink.size());
        let initial_dispatch_queue_slots = dispatch_queue_slots;
        loop {
            if dispatch_queue_slots == 0 || self.ready_set.is_empty() {
                break;
            }
            if self.active_job_queue_cursor >= self.active_job_queue.len() {
                self.active_job_queue_cursor = 0;
            }
            let active_job_queue_entry =
                match self.active_job_queue.get(self.active_job_queue_cursor) {
                    Some(entry) => entry.clone(),
                    None => {
                        return Err(SchedulerError::Internal(
                            "active job queue cursor is corrupted".to_string(),
                        ));
                    }
                };
            self.active_job_queue_cursor += 1;
            match active_job_queue_entry {
                ActiveJobQueueEntry::CleanupReady => {
                    let Some((job_id, resource_group_id)) = self.cleanup_ready_queue.pop_front()
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
                    self.ready_set.remove(&(job_id, TaskId::Cleanup));
                    self.commit_ready_or_cleanup_ready_tasks.remove(&job_id);
                    dispatch_queue_slots -= 1;
                }
                ActiveJobQueueEntry::CommitReady => {
                    let Some((job_id, resource_group_id)) = self.commit_ready_queue.pop_front()
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
                    self.ready_set.remove(&(job_id, TaskId::Commit));
                    self.commit_ready_or_cleanup_ready_tasks.remove(&job_id);
                    dispatch_queue_slots -= 1;
                }
                ActiveJobQueueEntry::Ready(job_id) => {
                    let Some(job_entry) = self.active_jobs.get_mut(&job_id) else {
                        return Err(SchedulerError::Internal(
                            "attempt to remove a non-existing active job: {job_id:?}".to_string(),
                        ));
                    };
                    if let Some(task_id) = job_entry.dequeue() {
                        self.sink
                            .enqueue(TaskAssignment {
                                job_id,
                                resource_group_id: job_entry.resource_group_id,
                                task_id,
                            })
                            .await?;
                        self.ready_set.remove(&(job_id, task_id));
                        dispatch_queue_slots -= 1;
                    } else {
                        self.remove_active_job_and_dequeue_next_pending_job(job_id)?;
                    }
                }
            }
        }

        let dispatched = initial_dispatch_queue_slots - dispatch_queue_slots;
        if recording && dispatched > 0 {
            self.config
                .metrics
                .dispatch_enrich_ns
                .fetch_add(elapsed_nanos(dispatch_start), Ordering::Relaxed);
            self.config
                .metrics
                .dispatch_enrich_count
                .fetch_add(1, Ordering::Relaxed);
        }

        if recording {
            self.config
                .metrics
                .total_loop_ns
                .fetch_add(elapsed_nanos(loop_start), Ordering::Relaxed);
            self.config
                .metrics
                .loop_count
                .fetch_add(1, Ordering::Relaxed);
        }

        // When the iteration dispatched nothing, the loop is either waiting on an in-flight poll or
        // back-pressured by a full dispatch queue. In both cases it would otherwise spin without an
        // await point; because the inbound polls run on tasks this same runtime must schedule, a
        // non-yielding spin livelocks them and the scheduler never makes progress. Yield to let the
        // poll tasks and dispatch-queue readers run.
        if dispatched == 0 {
            tokio::task::yield_now().await;
        }

        Ok(())
    }

    fn spawn_inbound_queue_reader(&mut self) {
        let num_commit_ready_tasks = self.commit_ready_queue.len();
        let num_cleanup_ready_tasks = self.cleanup_ready_queue.len();
        let max_commit_ready_to_poll = self
            .config
            .commit_ready_task_capacity
            .saturating_sub(num_commit_ready_tasks);
        let max_cleanup_ready_to_poll = self
            .config
            .cleanup_ready_task_capacity
            .saturating_sub(num_cleanup_ready_tasks);
        let max_ready_to_poll = self.config.ready_task_capacity.saturating_sub(
            self.ready_set.len() - num_commit_ready_tasks - num_cleanup_ready_tasks,
        );
        self.inbound_queue_reader.spawn(
            Duration::from_millis(self.config.storage_polling_wait_time_ms),
            max_ready_to_poll,
            max_commit_ready_to_poll,
            max_cleanup_ready_to_poll,
        );
    }
}

enum InboundQueueResult {
    Result {
        session_id: SessionId,
        ready_entries: Vec<InboundEntry>,
        commit_ready_entries: Vec<InboundEntry>,
        cleanup_ready_entries: Vec<InboundEntry>,
    },
    ResultNotReady,
    HandleNotSpawned,
}

struct InboundQueuePollingHandle {
    ready_handle:
        tokio::task::JoinHandle<Result<(SessionId, Vec<InboundEntry>), StorageClientError>>,
    commit_ready_handle:
        tokio::task::JoinHandle<Result<(SessionId, Vec<InboundEntry>), StorageClientError>>,
    cleanup_ready_handle:
        tokio::task::JoinHandle<Result<(SessionId, Vec<InboundEntry>), StorageClientError>>,
}

impl InboundQueuePollingHandle {
    async fn poll_ready(
        &mut self,
        curr_session_id: SessionId,
    ) -> Result<InboundQueueResult, SchedulerError> {
        if !self.ready_handle.is_finished()
            || !self.commit_ready_handle.is_finished()
            || !self.cleanup_ready_handle.is_finished()
        {
            return Ok(InboundQueueResult::ResultNotReady);
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

        Ok(InboundQueueResult::Result {
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

struct AsyncInboundQueueReader<StorageClientType: SchedulerStorageClient + 'static> {
    storage_client: StorageClientType,
    handle: Option<InboundQueuePollingHandle>,
}

impl<StorageClientType: SchedulerStorageClient + 'static>
    AsyncInboundQueueReader<StorageClientType>
{
    const fn new(storage_client: StorageClientType) -> Self {
        Self {
            storage_client,
            handle: None,
        }
    }

    async fn poll_ready(
        &mut self,
        curr_session_id: SessionId,
    ) -> Result<InboundQueueResult, SchedulerError> {
        match &mut self.handle {
            None => Ok(InboundQueueResult::HandleNotSpawned),
            Some(handle) => {
                let inbound_queue_result = handle.poll_ready(curr_session_id).await?;
                if !matches!(inbound_queue_result, InboundQueueResult::ResultNotReady) {
                    self.handle = None;
                }
                Ok(inbound_queue_result)
            }
        }
    }

    fn spawn(
        &mut self,
        storage_polling_wait_time: Duration,
        max_ready_entries: usize,
        max_commit_ready_entries: usize,
        max_cleanup_ready_entries: usize,
    ) {
        let ready_storage_client = self.storage_client.clone();
        let ready_handle = tokio::task::spawn(async move {
            ready_storage_client
                .poll_ready(max_ready_entries, storage_polling_wait_time)
                .await
        });

        let commit_ready_storage_client = self.storage_client.clone();
        let commit_ready_handle = tokio::task::spawn(async move {
            commit_ready_storage_client
                .poll_commit_ready(max_commit_ready_entries, storage_polling_wait_time)
                .await
        });

        let cleanup_ready_storage_client = self.storage_client.clone();
        let cleanup_ready_handle = tokio::task::spawn(async move {
            cleanup_ready_storage_client
                .poll_cleanup_ready(max_cleanup_ready_entries, storage_polling_wait_time)
                .await
        });

        self.handle = Some(InboundQueuePollingHandle {
            ready_handle,
            commit_ready_handle,
            cleanup_ready_handle,
        });
    }
}
