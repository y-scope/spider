//! Unit tests for the round-robin scheduler core.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc,
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::bail;
use async_trait::async_trait;
use spider_core::{
    job::JobState,
    types::id::{JobId, ResourceGroupId, SessionId, TaskId},
};
use tokio_util::sync::CancellationToken;

use super::RoundRobinConfig;
use crate::{
    DispatchQueueSource,
    InboundEntry,
    SchedulerCore,
    SchedulerError,
    SchedulerStorageClient,
    StorageClientError,
    TaskAssignment,
    dispatch_queue::{DispatchQueueReader, DispatchQueueWriter, create_dispatch_queue},
};

/// The session used by tests that never bump the session.
const DEFAULT_SESSION_ID: SessionId = 0;

/// The maximum time to wait for expected assignments before failing a test.
const DRAIN_DEADLINE: Duration = Duration::from_secs(5);

struct MockStorageInner {
    session_id: AtomicU64,
    ready_batches: Mutex<VecDeque<(SessionId, Vec<InboundEntry>)>>,
}

/// A mock [`SchedulerStorageClient`] backed by scripted poll batches.
///
/// Each lane serves its scripted batches in FIFO order, one batch per poll; when a lane's script
/// is empty, polls return an empty batch under the mock's current session immediately (the `wait`
/// parameter is ignored to keep tests fast).
#[derive(Clone)]
struct MockStorageClient {
    inner: Arc<MockStorageInner>,
}

impl MockStorageClient {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A new mock storage client with no scripted batches, reporting `session_id` on empty polls.
    fn new(session_id: SessionId) -> Self {
        Self {
            inner: Arc::new(MockStorageInner {
                session_id: AtomicU64::new(session_id),
                ready_batches: Mutex::new(VecDeque::new()),
            }),
        }
    }

    /// Scripts a batch to be served by the next unserved [`SchedulerStorageClient::poll_ready`]
    /// call.
    fn push_ready_batch(&self, session_id: SessionId, entries: Vec<InboundEntry>) {
        self.inner
            .ready_batches
            .lock()
            .expect("ready-batch lock poisoned")
            .push_back((session_id, entries));
    }

    /// # Returns
    ///
    /// The session reported on polls that have no scripted batch.
    fn current_session(&self) -> SessionId {
        self.inner.session_id.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl SchedulerStorageClient for MockStorageClient {
    async fn poll_ready(
        &self,
        max_items: usize,
        _wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
        let scripted_batch = self
            .inner
            .ready_batches
            .lock()
            .expect("ready-batch lock poisoned")
            .pop_front();
        let Some((session_id, entries)) = scripted_batch else {
            return Ok((self.current_session(), Vec::new()));
        };
        assert!(
            entries.len() <= max_items,
            "scripted batch of {} entries exceeds the scheduler's poll limit of {max_items}",
            entries.len(),
        );
        Ok((session_id, entries))
    }

    async fn poll_commit_ready(
        &self,
        _max_items: usize,
        _wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
        Ok((self.current_session(), Vec::new()))
    }

    async fn poll_cleanup_ready(
        &self,
        _max_items: usize,
        _wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
        Ok((self.current_session(), Vec::new()))
    }

    async fn job_state(&self, _job_id: JobId) -> Result<JobState, StorageClientError> {
        Ok(JobState::Running)
    }
}

/// # Returns
///
/// A config with the given pool and dispatch capacities, and defaults large enough that the other
/// capacities never throttle the tests.
fn make_config(
    active_job_queue_capacity: usize,
    dispatch_queue_capacity: usize,
) -> RoundRobinConfig {
    RoundRobinConfig {
        active_job_queue_capacity,
        dispatch_queue_capacity,
        ready_task_capacity: 16_384,
        commit_ready_task_capacity: 16,
        cleanup_ready_task_capacity: 16,
        storage_poll_timeout_ms: 10,
        tick_interval_ms: 1,
    }
}

/// # Returns
///
/// `n` jobs with freshly generated job and resource-group IDs.
fn make_jobs(n: usize) -> Vec<(JobId, ResourceGroupId)> {
    (0..n)
        .map(|_| (JobId::new(), ResourceGroupId::new()))
        .collect()
}

/// Builds one inbound ready batch containing `tasks_per_job` tasks per job, interleaved across
/// jobs in per-job FIFO order (task 0 of every job, then task 1 of every job, and so on).
///
/// When `dup_every` is non-zero, every `dup_every`-th entry is duplicated adjacently within the
/// batch, emulating the duplicate task assignments a real storage may return.
///
/// # Returns
///
/// The inbound entries of the batch.
fn make_ready_batch(
    jobs: &[(JobId, ResourceGroupId)],
    tasks_per_job: usize,
    dup_every: usize,
) -> Vec<InboundEntry> {
    let mut entries = Vec::new();
    let mut num_emitted = 0_usize;
    for task_index in 0..tasks_per_job {
        for &(job_id, resource_group_id) in jobs {
            let entry = InboundEntry {
                resource_group_id,
                job_id,
                task_id: TaskId::Index(task_index),
            };
            entries.push(entry);
            num_emitted += 1;
            if dup_every > 0 && num_emitted.is_multiple_of(dup_every) {
                entries.push(entry);
            }
        }
    }
    entries
}

/// Validates the given config and spawns the scheduler's public run loop as a background task.
///
/// # Returns
///
/// A tuple containing:
///
/// * The join handle yielding the scheduler's exit result.
/// * The cancellation token that stops the scheduler.
///
/// # Panics
///
/// Panics if the given config fails validation.
fn spawn_scheduler(
    config: RoundRobinConfig,
    storage_client: MockStorageClient,
    sink: DispatchQueueWriter,
) -> (
    tokio::task::JoinHandle<Result<(), SchedulerError>>,
    CancellationToken,
) {
    let core = config.make_core().expect("config validation failed");
    let cancellation_token = CancellationToken::new();
    let scheduler_token = cancellation_token.clone();
    let handle = tokio::spawn(async move { core.run(storage_client, sink, scheduler_token).await });
    (handle, cancellation_token)
}

/// Drains exactly `n` task assignments from the dispatch queue, playing the worker pool's role.
///
/// # Returns
///
/// The drained assignments in FIFO order on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Fewer than `n` assignments arrive within [`DRAIN_DEADLINE`].
/// * Forwards [`DispatchQueueSource::dequeue`]'s return values on failure.
async fn drain_n(reader: &DispatchQueueReader, n: usize) -> anyhow::Result<Vec<TaskAssignment>> {
    const DEQUEUE_WAIT: Duration = Duration::from_millis(100);
    let deadline = tokio::time::Instant::now() + DRAIN_DEADLINE;
    let mut assignments = Vec::with_capacity(n);
    while assignments.len() < n {
        if tokio::time::Instant::now() > deadline {
            bail!(
                "timed out draining assignments: got {}, expected {n}",
                assignments.len(),
            );
        }
        if let Some((_session_id, assignment)) = reader.dequeue(DEQUEUE_WAIT).await? {
            assignments.push(assignment);
        }
    }
    Ok(assignments)
}

/// Asserts that no further assignment arrives within a short observation window, proving that
/// duplicated or dropped tasks never leak into the dispatch queue.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`DispatchQueueSource::dequeue`]'s return values on failure.
async fn assert_no_more_assignments(reader: &DispatchQueueReader) -> anyhow::Result<()> {
    const OBSERVATION_WINDOW: Duration = Duration::from_secs(1);
    let unexpected_assignment = reader.dequeue(OBSERVATION_WINDOW).await?;
    assert_eq!(unexpected_assignment, None);
    Ok(())
}

/// Asserts that `assignments` is exactly `rounds` full round-robin rotations over `jobs` in order:
/// rotation `r` consists of task `r` of every job, following the jobs' order, so every job's task
/// indices are dispatched FIFO.
fn assert_strict_rotation(
    assignments: &[TaskAssignment],
    jobs: &[(JobId, ResourceGroupId)],
    rounds: usize,
) {
    let expected: Vec<(JobId, ResourceGroupId, TaskId)> = (0..rounds)
        .flat_map(|round| {
            jobs.iter().map(move |&(job_id, resource_group_id)| {
                (job_id, resource_group_id, TaskId::Index(round))
            })
        })
        .collect();
    let actual: Vec<(JobId, ResourceGroupId, TaskId)> = assignments
        .iter()
        .map(|assignment| {
            (
                assignment.job_id,
                assignment.resource_group_id,
                assignment.task_id,
            )
        })
        .collect();
    assert_eq!(actual, expected);
}

/// Asserts that `assignments` follows the round-robin scheduling policy over `jobs` without pinning
/// down the exact rotation order:
///
/// * Every aligned window of `jobs.len()` assignments (one full rotation pass) contains each job
///   exactly once.
/// * Each job's task indices are dispatched in FIFO order, with the matching resource group.
/// * Each job receives exactly `tasks_per_job` assignments.
fn assert_round_robin_property(
    assignments: &[TaskAssignment],
    jobs: &[(JobId, ResourceGroupId)],
    tasks_per_job: usize,
) {
    assert_eq!(assignments.len(), jobs.len() * tasks_per_job);

    // With equal task counts, no job leaves the rotation mid-phase, so every rotation pass must
    // schedule every job exactly once.
    for rotation_pass in assignments.chunks(jobs.len()) {
        let scheduled_jobs: HashSet<JobId> = rotation_pass
            .iter()
            .map(|assignment| assignment.job_id)
            .collect();
        assert_eq!(
            scheduled_jobs.len(),
            jobs.len(),
            "a rotation pass repeats or misses a job: {rotation_pass:?}",
        );
    }

    let resource_groups: HashMap<JobId, ResourceGroupId> = jobs.iter().copied().collect();
    let mut next_task_indices: HashMap<JobId, usize> = HashMap::new();
    for assignment in assignments {
        let resource_group_id = *resource_groups
            .get(&assignment.job_id)
            .expect("assignment belongs to a job outside the given job set");
        assert_eq!(assignment.resource_group_id, resource_group_id);

        let next_task_index = next_task_indices.entry(assignment.job_id).or_insert(0);
        assert_eq!(assignment.task_id, TaskId::Index(*next_task_index));
        *next_task_index += 1;
    }

    for &(job_id, _) in jobs {
        assert_eq!(next_task_indices.get(&job_id).copied(), Some(tasks_per_job));
    }
}

#[test]
fn zero_capacity_configs_are_rejected() {
    let try_make_core =
        |config: RoundRobinConfig| config.make_core::<MockStorageClient, DispatchQueueWriter>();

    assert!(try_make_core(make_config(2, 2)).is_ok());

    let zeroed_configs = [
        RoundRobinConfig {
            active_job_queue_capacity: 0,
            ..make_config(2, 2)
        },
        RoundRobinConfig {
            dispatch_queue_capacity: 0,
            ..make_config(2, 2)
        },
        RoundRobinConfig {
            ready_task_capacity: 0,
            ..make_config(2, 2)
        },
        RoundRobinConfig {
            commit_ready_task_capacity: 0,
            ..make_config(2, 2)
        },
        RoundRobinConfig {
            cleanup_ready_task_capacity: 0,
            ..make_config(2, 2)
        },
    ];
    for config in zeroed_configs {
        let result = try_make_core(config);
        assert!(
            matches!(result, Err(SchedulerError::InvalidConfig(_))),
            "expected InvalidConfig, got {:?}",
            result.err(),
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn single_capacity_pool_schedules_jobs_serially() -> anyhow::Result<()> {
    const NUM_JOBS: usize = 3;
    const TASKS_PER_JOB: usize = 5;
    const DUP_EVERY: usize = 3;
    const DISPATCH_QUEUE_CAPACITY: usize = 32;

    let jobs = make_jobs(NUM_JOBS);
    let storage_client = MockStorageClient::new(DEFAULT_SESSION_ID);
    storage_client.push_ready_batch(
        DEFAULT_SESSION_ID,
        make_ready_batch(&jobs, TASKS_PER_JOB, DUP_EVERY),
    );

    let (writer, reader) = create_dispatch_queue(DISPATCH_QUEUE_CAPACITY, DEFAULT_SESSION_ID);
    let config = make_config(1, DISPATCH_QUEUE_CAPACITY);
    let (scheduler_handle, cancellation_token) = spawn_scheduler(config, storage_client, writer);

    let assignments = drain_n(&reader, NUM_JOBS * TASKS_PER_JOB).await?;
    assert_no_more_assignments(&reader).await?;

    // With an active job pool of capacity 1, round-robin degenerates to serial job FIFO: the
    // rotation holds a single job at a time, so each job's tasks dispatch as one consecutive
    // single-job rotation, in job-arrival order.
    for (segment, job) in assignments.chunks(TASKS_PER_JOB).zip(&jobs) {
        assert_strict_rotation(segment, std::slice::from_ref(job), TASKS_PER_JOB);
    }

    cancellation_token.cancel();
    scheduler_handle.await.expect("scheduler task panicked")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn active_jobs_dispatch_in_round_robin_order() -> anyhow::Result<()> {
    const NUM_JOBS: usize = 10;
    const TASKS_PER_JOB: usize = 5;
    const DUP_EVERY: usize = 4;
    const DISPATCH_QUEUE_CAPACITY: usize = 32;

    let jobs = make_jobs(NUM_JOBS);
    let storage_client = MockStorageClient::new(DEFAULT_SESSION_ID);
    storage_client.push_ready_batch(
        DEFAULT_SESSION_ID,
        make_ready_batch(&jobs, TASKS_PER_JOB, DUP_EVERY),
    );

    let (writer, reader) = create_dispatch_queue(DISPATCH_QUEUE_CAPACITY, DEFAULT_SESSION_ID);
    let config = make_config(NUM_JOBS, DISPATCH_QUEUE_CAPACITY);
    let (scheduler_handle, cancellation_token) = spawn_scheduler(config, storage_client, writer);

    let assignments = drain_n(&reader, NUM_JOBS * TASKS_PER_JOB).await?;
    assert_no_more_assignments(&reader).await?;

    // All 10 jobs fit into the active job pool, so no job ever pends and dispatch follows the
    // strict rotation: task 0 of every job in batch order, then task 1 of every job, and so on. The
    // exact count of 50 (with no trailing assignments) also proves the in-batch duplicates were
    // deduplicated.
    assert_strict_rotation(&assignments, &jobs, TASKS_PER_JOB);

    cancellation_token.cancel();
    scheduler_handle.await.expect("scheduler task panicked")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn pending_jobs_promote_and_schedule_round_robin() -> anyhow::Result<()> {
    const ACTIVE_JOB_QUEUE_CAPACITY: usize = 10;
    const NUM_JOBS: usize = 20;
    const TASKS_PER_JOB: usize = 5;
    const DUP_EVERY: usize = 5;
    const DISPATCH_QUEUE_CAPACITY: usize = 32;

    let jobs = make_jobs(NUM_JOBS);
    let storage_client = MockStorageClient::new(DEFAULT_SESSION_ID);
    storage_client.push_ready_batch(
        DEFAULT_SESSION_ID,
        make_ready_batch(&jobs, TASKS_PER_JOB, DUP_EVERY),
    );

    let (writer, reader) = create_dispatch_queue(DISPATCH_QUEUE_CAPACITY, DEFAULT_SESSION_ID);
    let config = make_config(ACTIVE_JOB_QUEUE_CAPACITY, DISPATCH_QUEUE_CAPACITY);
    let (scheduler_handle, cancellation_token) = spawn_scheduler(config, storage_client, writer);

    let assignments = drain_n(&reader, NUM_JOBS * TASKS_PER_JOB).await?;
    assert_no_more_assignments(&reader).await?;

    let (active_jobs, pending_jobs) = jobs.split_at(ACTIVE_JOB_QUEUE_CAPACITY);
    let (phase1, phase2) = assignments.split_at(ACTIVE_JOB_QUEUE_CAPACITY * TASKS_PER_JOB);

    // Phase 1: the first 10 jobs in batch order fill the active job pool and dispatch in strict
    // rotation; the pending jobs must not appear while the active jobs still have tasks.
    assert_strict_rotation(phase1, active_jobs, TASKS_PER_JOB);

    // Phase 2: once the active jobs exhaust, the 10 pending jobs are promoted and scheduled
    // round-robin. The exact slot order after the retire-and-promote wave is an implementation
    // detail of the rotation bookkeeping, so assert the round-robin property instead of one
    // hard-coded sequence.
    assert_round_robin_property(phase2, pending_jobs, TASKS_PER_JOB);

    cancellation_token.cancel();
    scheduler_handle.await.expect("scheduler task panicked")?;
    Ok(())
}
