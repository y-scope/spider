//! Round-robin scheduler.
//!
//! This scheduler provides basic fairness across jobs using a round-robin scheduling policy. It
//! polls tasks from the inbound queue (maintained by the storage service) and organizes jobs into
//! two sets:
//!
//! * Active jobs: jobs that participate in round-robin scheduling.
//! * Pending jobs: jobs that are buffered but not yet scheduled. When an active job has no
//!   remaining schedulable tasks, it is replaced by the next pending job in FIFO order.
//!
//! The scheduler operates in discrete ticks. During each tick, it attempts to consume the results
//! of an asynchronous inbound-queue polling operation and loads any newly available tasks into its
//! internal buffers. It then makes scheduling decisions until the dispatch queue reaches capacity.
//!
//! # Properties
//!
//! * Each round-robin cycle may schedule at most one additional commit task and one additional
//!   cleanup task, if available.
//! * All buffered tasks are unique. Tasks loaded from the inbound queue are deduplicated before
//!   entering the scheduler's internal buffers.
//!
//! # Configuration
//!
//! * `active_job_pool_capacity`: Maximum number of active jobs maintained by the scheduler.
//! * `dispatch_queue_capacity`: Maximum number of task assignments in the dispatch queue.
//! * `ready_task_capacity`: Maximum number of ready tasks buffered by the scheduler.
//! * `commit_ready_task_capacity`: Maximum number of buffered commit-ready tasks.
//! * `cleanup_ready_task_capacity`: Maximum number of buffered cleanup-ready tasks.
//! * `storage_polling_wait_time_ms`: Maximum time, in milliseconds, that inbound-queue polling may
//!   block on the storage-service side.
//! * `tick_interval_ms`: Interval, in milliseconds, between scheduler ticks (tick execution time
//!   included).

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

    /// The maximum time (in milliseconds) that the scheduler will wait for the storage server to
    /// fill the inbound-queue reading request.
    pub storage_polling_wait_time_ms: u64,

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
            if !sleep_time.is_zero() {
                tokio::time::sleep(sleep_time).await;
            } else {
                tokio::task::yield_now().await;
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

    async fn tick(&mut self) -> Result<(), SchedulerError> {
        self.poll_inbound_queue_result().await?;
        self.make_schedule_decision().await?;
        Ok(())
    }

    async fn load_inbound_queue_result(
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
            self.clear_all_placement();
            self.sink.bump_session_id(storage_session_id).await?;
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

        Ok(())
    }

    async fn poll_inbound_queue_result(&mut self) -> Result<(), SchedulerError> {
        let curr_session_id = self.storage_session_id;
        let inbound_queue_result = self
            .inbound_queue_reader
            .poll_ready(curr_session_id)
            .await?;
        match inbound_queue_result {
            InboundQueueResult::Result {
                session_id: storage_session_id,
                ready_entries,
                commit_ready_entries,
                cleanup_ready_entries,
            } => {
                self.load_inbound_queue_result(
                    curr_session_id,
                    storage_session_id,
                    ready_entries,
                    commit_ready_entries,
                    cleanup_ready_entries,
                )
                .await?;
                self.spawn_inbound_queue_reader();
            }
            InboundQueueResult::ResultNotReady => {}
            InboundQueueResult::HandleNotSpawned => {
                self.spawn_inbound_queue_reader();
            }
        }

        Ok(())
    }

    async fn make_schedule_decision(&mut self) -> Result<(), SchedulerError> {
        let mut dispatch_queue_slots = self
            .config
            .dispatch_queue_capacity
            .saturating_sub(self.sink.size());
        while dispatch_queue_slots > 0 && !self.ready_set.is_empty() {
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
