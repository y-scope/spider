//! Unit tests for the round-robin scheduler core.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::bail;
use async_trait::async_trait;
use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SchedulerId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskId;
use tokio_util::sync::CancellationToken;

use super::RoundRobinConfig;
use super::implementation::RoundRobin;
use crate::DispatchQueueSource;
use crate::InboundEntry;
use crate::SchedulerCore;
use crate::SchedulerError;
use crate::SchedulerStorageClient;
use crate::StorageClientError;
use crate::TaskAssignment;
use crate::core::TaskAssignmentIdIssuer;
use crate::dispatch_queue::DispatchQueueReader;
use crate::dispatch_queue::DispatchQueueWriter;
use crate::dispatch_queue::create_dispatch_queue;

/// The session used by tests that never bump the session.
const DEFAULT_SESSION_ID: SessionId = 0;

/// The white-box scheduler under test, driven by manual ticks.
type TestScheduler = RoundRobin<MockStorageClient, DispatchQueueWriter>;

/// The maximum time to wait for expected assignments before failing a test.
const DRAIN_DEADLINE: Duration = Duration::from_secs(5);

struct MockStorageInner {
    session_id: AtomicU64,
    ready_batches: Mutex<VecDeque<(SessionId, Vec<InboundEntry>)>>,
    commit_ready_batches: Mutex<VecDeque<(SessionId, Vec<InboundEntry>)>>,
    cleanup_ready_batches: Mutex<VecDeque<(SessionId, Vec<InboundEntry>)>>,
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
                commit_ready_batches: Mutex::new(VecDeque::new()),
                cleanup_ready_batches: Mutex::new(VecDeque::new()),
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

    /// Scripts a batch to be served by the next unserved
    /// [`SchedulerStorageClient::poll_commit_ready`] call.
    fn push_commit_ready_batch(&self, session_id: SessionId, entries: Vec<InboundEntry>) {
        self.inner
            .commit_ready_batches
            .lock()
            .expect("commit-ready-batch lock poisoned")
            .push_back((session_id, entries));
    }

    /// Scripts a batch to be served by the next unserved
    /// [`SchedulerStorageClient::poll_cleanup_ready`] call.
    fn push_cleanup_ready_batch(&self, session_id: SessionId, entries: Vec<InboundEntry>) {
        self.inner
            .cleanup_ready_batches
            .lock()
            .expect("cleanup-ready-batch lock poisoned")
            .push_back((session_id, entries));
    }

    /// # Returns
    ///
    /// The session reported on polls that have no scripted batch.
    fn current_session(&self) -> SessionId {
        self.inner.session_id.load(Ordering::Relaxed)
    }

    /// Sets the session reported on polls that have no scripted batch.
    fn set_session(&self, session_id: SessionId) {
        self.inner.session_id.store(session_id, Ordering::Relaxed);
    }

    /// Serves one poll from the given lane's script.
    ///
    /// # Returns
    ///
    /// The lane's next scripted batch, or an empty batch under the current session if the lane's
    /// script is exhausted.
    fn serve_batch(
        &self,
        batches: &Mutex<VecDeque<(SessionId, Vec<InboundEntry>)>>,
        max_items: usize,
    ) -> (SessionId, Vec<InboundEntry>) {
        let scripted_batch = batches.lock().expect("batch lock poisoned").pop_front();
        let Some((session_id, entries)) = scripted_batch else {
            return (self.current_session(), Vec::new());
        };
        assert!(
            entries.len() <= max_items,
            "scripted batch of {} entries exceeds the scheduler's poll limit of {max_items}",
            entries.len(),
        );
        (session_id, entries)
    }
}

#[async_trait]
impl SchedulerStorageClient for MockStorageClient {
    async fn register(
        &self,
        _ip_address: std::net::IpAddr,
        _port: u16,
    ) -> Result<SchedulerId, StorageClientError> {
        Ok(SchedulerId::from(0))
    }

    async fn poll_ready(
        &self,
        max_items: usize,
        _wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
        Ok(self.serve_batch(&self.inner.ready_batches, max_items))
    }

    async fn poll_commit_ready(
        &self,
        max_items: usize,
        _wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
        Ok(self.serve_batch(&self.inner.commit_ready_batches, max_items))
    }

    async fn poll_cleanup_ready(
        &self,
        max_items: usize,
        _wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
        Ok(self.serve_batch(&self.inner.cleanup_ready_batches, max_items))
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
        active_job_queue_capacity: NonZeroUsize::new(active_job_queue_capacity)
            .expect("the active job queue capacity should be non-zero"),
        dispatch_queue_capacity: NonZeroUsize::new(dispatch_queue_capacity)
            .expect("the dispatch queue capacity should be non-zero"),
        ready_task_capacity: NonZeroUsize::new(16_384).expect("16384 is non-zero"),
        commit_ready_task_capacity: NonZeroUsize::new(16).expect("16 is non-zero"),
        cleanup_ready_task_capacity: NonZeroUsize::new(16).expect("16 is non-zero"),
        storage_poll_timeout_ms: 10,
        tick_interval_ms: NonZeroU64::new(1).expect("1 is non-zero"),
        finalizing_job_expiration_timeout_sec: 6 * 60 * 60,
    }
}

/// # Returns
///
/// `n` jobs with freshly generated job and resource-group IDs.
fn make_jobs(n: usize) -> Vec<(JobId, ResourceGroupId)> {
    (0..n)
        .map(|_| (JobId::random(), ResourceGroupId::random()))
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

/// Builds one inbound batch that marks each given job as finalizing, with `task_id` (either
/// [`TaskId::Commit`] or [`TaskId::Cleanup`]) set on every entry.
///
/// # Returns
///
/// The inbound entries of the batch.
fn make_finalizing_batch(jobs: &[(JobId, ResourceGroupId)], task_id: TaskId) -> Vec<InboundEntry> {
    jobs.iter()
        .map(|&(job_id, resource_group_id)| InboundEntry {
            resource_group_id,
            job_id,
            task_id,
        })
        .collect()
}

/// Spawns the scheduler's public run loop as a background task.
///
/// # Returns
///
/// A tuple containing:
///
/// * The join handle yielding the scheduler's exit result.
/// * The cancellation token that stops the scheduler.
fn spawn_scheduler(
    config: RoundRobinConfig,
    storage_client: MockStorageClient,
    sink: DispatchQueueWriter,
) -> (
    tokio::task::JoinHandle<Result<(), SchedulerError>>,
    CancellationToken,
) {
    let core = Box::new(config.make_core());
    let cancellation_token = CancellationToken::new();
    let scheduler_token = cancellation_token.clone();
    let handle = tokio::spawn(async move {
        core.run(
            storage_client,
            sink,
            TaskAssignmentIdIssuer::new(),
            scheduler_token,
        )
        .await
    });
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
///
/// # Panics
///
/// Panics if an assignment arrives within the observation window.
async fn assert_no_more_assignments(reader: &DispatchQueueReader) -> anyhow::Result<()> {
    const OBSERVATION_WINDOW: Duration = Duration::from_secs(1);
    let unexpected_assignment = reader.dequeue(OBSERVATION_WINDOW).await?;
    assert_eq!(unexpected_assignment, None);
    Ok(())
}

/// # Returns
///
/// A vector of tuples following the order of the input assignments, each tuple containing:
///
/// * The job ID.
/// * The resource group ID.
/// * The task ID.
fn make_assignment_tuple(assignments: &[TaskAssignment]) -> Vec<(JobId, ResourceGroupId, TaskId)> {
    assignments
        .iter()
        .map(|assignment| {
            (
                assignment.job_id,
                assignment.resource_group_id,
                assignment.task_id,
            )
        })
        .collect()
}

/// Asserts that `assignments` is exactly `rounds` full round-robin rotations over `jobs` in order:
/// rotation `r` consists of task `r` of every job, following the jobs' order, so every job's task
/// indices are dispatched FIFO.
///
/// # Panics
///
/// Panics if `assignments` deviates from the expected strict rotation.
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
    assert_eq!(make_assignment_tuple(assignments), expected);
}

/// Asserts that `assignments` follows the round-robin scheduling policy over `jobs` without pinning
/// down the exact rotation order:
///
/// * Every aligned window of `jobs.len()` assignments (one full rotation pass) contains each job
///   exactly once.
/// * Each job's task indices are dispatched in FIFO order, with the matching resource group.
/// * Each job receives exactly `tasks_per_job` assignments.
///
/// # Panics
///
/// Panics if `assignments` violates any of the properties above.
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

/// # Returns
///
/// A white-box scheduler wired to the given storage client and sink, to be driven by manual
/// [`RoundRobin::tick`] calls.
fn make_scheduler(
    config: RoundRobinConfig,
    storage_client: MockStorageClient,
    sink: DispatchQueueWriter,
) -> TestScheduler {
    RoundRobin::new(
        DEFAULT_SESSION_ID,
        storage_client,
        sink,
        TaskAssignmentIdIssuer::new(),
        CancellationToken::new(),
        config,
    )
}

/// Ticks the scheduler until `predicate` holds on its state.
///
/// # Errors
///
/// Returns an error if:
///
/// * The predicate does not hold within [`DRAIN_DEADLINE`].
/// * Forwards [`RoundRobin::tick`]'s return values on failure.
async fn tick_until(
    scheduler: &mut TestScheduler,
    predicate: impl Fn(&TestScheduler) -> bool,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + DRAIN_DEADLINE;
    while !predicate(scheduler) {
        if tokio::time::Instant::now() > deadline {
            bail!("timed out waiting for the tick predicate to hold");
        }
        scheduler.tick().await?;
        tokio::task::yield_now().await;
    }
    Ok(())
}

/// Drains exactly `n` task assignments while manually ticking the scheduler to refill the dispatch
/// queue (the white-box counterpart of [`drain_n`]).
///
/// # Returns
///
/// The drained assignments in FIFO order on success, each paired with the session under which it
/// was dequeued.
///
/// # Errors
///
/// Returns an error if:
///
/// * Fewer than `n` assignments arrive within [`DRAIN_DEADLINE`].
/// * Forwards [`RoundRobin::tick`]'s return values on failure.
/// * Forwards [`DispatchQueueSource::dequeue`]'s return values on failure.
async fn tick_and_drain_n(
    scheduler: &mut TestScheduler,
    reader: &DispatchQueueReader,
    n: usize,
) -> anyhow::Result<Vec<(SessionId, TaskAssignment)>> {
    let deadline = tokio::time::Instant::now() + DRAIN_DEADLINE;
    let mut assignments = Vec::with_capacity(n);
    while assignments.len() < n {
        if tokio::time::Instant::now() > deadline {
            bail!(
                "timed out draining assignments: got {}, expected {n}",
                assignments.len(),
            );
        }
        scheduler.tick().await?;
        while let Some((session_id, assignment)) = reader.dequeue(Duration::ZERO).await? {
            assignments.push((session_id, assignment));
        }
        tokio::task::yield_now().await;
    }
    Ok(assignments)
}

/// Ticks the scheduler a few extra rounds and asserts that no further assignment is dispatched.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`RoundRobin::tick`]'s return values on failure.
/// * Forwards [`DispatchQueueSource::dequeue`]'s return values on failure.
///
/// # Panics
///
/// Panics if a further assignment is dispatched.
async fn assert_no_further_assignments(
    scheduler: &mut TestScheduler,
    reader: &DispatchQueueReader,
) -> anyhow::Result<()> {
    const EXTRA_TICKS: usize = 8;
    for _ in 0..EXTRA_TICKS {
        scheduler.tick().await?;
        tokio::task::yield_now().await;
    }
    let unexpected_assignment = reader.dequeue(Duration::from_millis(50)).await?;
    assert_eq!(unexpected_assignment, None);
    Ok(())
}

/// Drives the shared scenario where a finalizing batch drops one active and one pending job.
///
/// The finalizing lane is selected by `finalizing_task_id`: commit-ready for [`TaskId::Commit`],
/// or cleanup-ready for [`TaskId::Cleanup`]. The scenario:
///
/// 1. Buffers four jobs (two active, two pending) and freezes dispatch via a full dispatch queue.
/// 2. Delivers a finalizing batch for one active job and one pending job mid-stream.
/// 3. Asserts both jobs leave the placement state with their buffered regular tasks discarded.
/// 4. Unfreezes and asserts the drained sequence: each finalized job dispatches its finalizing task
///    exactly once and no further regular task, while the surviving jobs complete in FIFO order.
/// 5. Re-delivers regular ready tasks for the finalized jobs alongside a fresh canary job. Asserts
///    the re-delivered tasks are ignored (the finalizing gate persists after the finalizing tasks
///    are dispatched) while the canary job schedules normally.
///
/// # Errors
///
/// Returns an error if:
///
/// * `finalizing_task_id` is a regular [`TaskId::Index`] task.
/// * Forwards [`tick_until`]'s return values on failure.
/// * Forwards [`tick_and_drain_n`]'s return values on failure.
/// * Forwards [`assert_no_further_assignments`]'s return values on failure.
///
/// # Panics
///
/// Panics if any scheduling-behavior assertion of the scenario fails.
#[allow(clippy::too_many_lines, clippy::similar_names)]
async fn assert_finalizing_ready_drops_jobs(finalizing_task_id: TaskId) -> anyhow::Result<()> {
    // NOTE: We disable two linting rules for the following reasons:
    // * `clippy::too_many_lines`: This test case is long, but we want to avoid breaking it into
    //   smaller functions since that would also make the overall flow hard to navigate.
    // * `clippy::similar_names`: The linter complains about `job_a_regular`, `job_b_regular`, etc.,
    //   but these names are fine for test cases.
    const ACTIVE_JOB_QUEUE_CAPACITY: usize = 2;
    const DISPATCH_QUEUE_CAPACITY: usize = 2;
    const TASKS_PER_JOB: usize = 3;
    const NUM_PRE_FREEZE_ASSIGNMENTS: usize = DISPATCH_QUEUE_CAPACITY;
    const NUM_FINALIZED_JOBS: usize = 2;

    if matches!(finalizing_task_id, TaskId::Index(_)) {
        bail!("`finalizing_task_id` must be `TaskId::Commit` or `TaskId::Cleanup`");
    }
    let is_commit = finalizing_task_id == TaskId::Commit;

    // Batch order makes `job_a` and `job_b` active, `job_p` and `job_q` pending.
    let jobs = make_jobs(4);
    let (job_a, job_b, job_p, job_q) = (jobs[0], jobs[1], jobs[2], jobs[3]);

    let storage_client = MockStorageClient::new(DEFAULT_SESSION_ID);
    storage_client.push_ready_batch(
        DEFAULT_SESSION_ID,
        make_ready_batch(&jobs, TASKS_PER_JOB, 0),
    );

    let (writer, reader) = create_dispatch_queue(DISPATCH_QUEUE_CAPACITY, DEFAULT_SESSION_ID);
    let mut scheduler = make_scheduler(
        make_config(ACTIVE_JOB_QUEUE_CAPACITY, DISPATCH_QUEUE_CAPACITY),
        storage_client.clone(),
        writer,
    );

    // Step 1: ingest the ready batch. The ingesting tick also dispatches exactly two assignments
    // (`job_a.t0`, `job_b.t0`), filling the dispatch queue; dispatch is frozen from here on because
    // the test does not drain yet.
    tick_until(&mut scheduler, |scheduler| {
        !scheduler.buffered_tasks.is_empty()
    })
    .await?;
    assert_eq!(
        scheduler
            .active_jobs
            .keys()
            .copied()
            .collect::<HashSet<_>>(),
        HashSet::from([job_a.0, job_b.0]),
    );
    assert_eq!(
        scheduler
            .pending_jobs
            .keys()
            .copied()
            .collect::<HashSet<_>>(),
        HashSet::from([job_p.0, job_q.0]),
    );

    // Step 2: with dispatch frozen, deliver the finalizing batch for one active job, `job_b`, and
    // one pending job, `job_q`, before any of their remaining tasks can dispatch.
    let finalizing_batch = make_finalizing_batch(&[job_b, job_q], finalizing_task_id);
    if is_commit {
        storage_client.push_commit_ready_batch(DEFAULT_SESSION_ID, finalizing_batch);
    } else {
        storage_client.push_cleanup_ready_batch(DEFAULT_SESSION_ID, finalizing_batch);
    }
    tick_until(&mut scheduler, |scheduler| {
        scheduler.finalizing_jobs.contains(&job_b.0) && scheduler.finalizing_jobs.contains(&job_q.0)
    })
    .await?;

    // Step 3: both jobs left the placement state and their buffered regular tasks are discarded;
    // only their finalizing assignments remain queued, in arrival order.
    assert!(!scheduler.active_jobs.contains_key(&job_b.0));
    assert!(!scheduler.pending_jobs.contains_key(&job_q.0));
    assert!(
        scheduler.buffered_tasks.iter().all(|&(job_id, task_id)| {
            (job_id != job_b.0 && job_id != job_q.0) || !matches!(task_id, TaskId::Index(_))
        }),
        "a finalized job still has buffered regular tasks",
    );
    let finalizing_queue = if is_commit {
        &scheduler.commit_ready_jobs
    } else {
        &scheduler.cleanup_ready_jobs
    };
    assert_eq!(
        finalizing_queue.iter().copied().collect::<Vec<_>>(),
        vec![job_b, job_q],
    );

    // Step 4: unfreeze. Every remaining assignment is accounted for below: the pre-freeze
    // assignments already queued, one finalizing task per finalized job, `job_a`'s remaining
    // tasks (its first task dispatched pre-freeze), and the full task set of `job_p`, which
    // backfills `job_b`'s freed slot.

    // total number of assignments = pre-freeze assignments + finalizing assignments +
    //     remaining `job_a` assignments + full `job_p` assignments
    let num_assignments =
        NUM_PRE_FREEZE_ASSIGNMENTS + NUM_FINALIZED_JOBS + (TASKS_PER_JOB - 1) + TASKS_PER_JOB;
    let assignments: Vec<TaskAssignment> =
        tick_and_drain_n(&mut scheduler, &reader, num_assignments)
            .await?
            .into_iter()
            .map(|(_session_id, assignment)| assignment)
            .collect();
    assert_no_further_assignments(&mut scheduler, &reader).await?;
    assert_eq!(scheduler.buffered_tasks.len(), 0);

    let triples = make_assignment_tuple(&assignments);

    // The pre-freeze head is exactly `job_a.t0`, `job_b.t0`.
    assert_eq!(
        &triples[..NUM_PRE_FREEZE_ASSIGNMENTS],
        &[
            (job_a.0, job_a.1, TaskId::Index(0)),
            (job_b.0, job_b.1, TaskId::Index(0)),
        ],
    );

    // Each finalized job's finalizing task dispatches exactly once, in arrival (FIFO) order.
    let finalizing_assignments: Vec<_> = triples
        .iter()
        .filter(|&&(_, _, task_id)| task_id == finalizing_task_id)
        .copied()
        .collect();
    assert_eq!(
        finalizing_assignments,
        vec![
            (job_b.0, job_b.1, finalizing_task_id),
            (job_q.0, job_q.1, finalizing_task_id),
        ],
    );

    let job_a_tasks: Vec<TaskId> = triples
        .iter()
        .filter(|&&(job_id, ..)| job_id == job_a.0)
        .map(|&(_, _, task_id)| task_id)
        .collect();
    assert_eq!(
        job_a_tasks,
        vec![TaskId::Index(0), TaskId::Index(1), TaskId::Index(2)],
    );

    let job_b_regular: Vec<_> = triples
        .iter()
        .filter(|&&(job_id, _, task_id)| job_id == job_b.0 && matches!(task_id, TaskId::Index(_)))
        .copied()
        .collect();
    assert_eq!(job_b_regular, vec![(job_b.0, job_b.1, TaskId::Index(0))]);

    let job_p_tasks: Vec<TaskId> = triples
        .iter()
        .filter(|&&(job_id, ..)| job_id == job_p.0)
        .map(|&(_, _, task_id)| task_id)
        .collect();
    assert_eq!(
        job_p_tasks,
        vec![TaskId::Index(0), TaskId::Index(1), TaskId::Index(2)],
    );

    let job_q_regular: Vec<_> = triples
        .iter()
        .filter(|&&(job_id, _, task_id)| job_id == job_q.0 && matches!(task_id, TaskId::Index(_)))
        .copied()
        .collect();
    assert_eq!(job_q_regular, []);

    assert!(scheduler.buffered_tasks.is_empty());
    assert!(scheduler.pending_jobs.is_empty());
    assert!(scheduler.pending_job_queue.is_empty());
    assert!(scheduler.commit_ready_jobs.is_empty());
    assert!(scheduler.cleanup_ready_jobs.is_empty());
    assert_eq!(scheduler.finalizing_jobs.len(), NUM_FINALIZED_JOBS);

    assert!(scheduler.finalizing_jobs.contains(&job_b.0));
    assert!(scheduler.finalizing_jobs.contains(&job_q.0));

    // Step 5: The finalizing gate remains active after the finalizing tasks have been dispatched,
    // so re-delivered regular tasks for finalized jobs must be ignored. A fresh canary job is
    // included in the same batch. Since a batch is ingested atomically within a single tick,
    // successful dispatch of the canary's tasks proves that the finalized jobs' entries have
    // already been processed (and ignored), rather than still being in flight.
    let canary_jobs = make_jobs(1);
    let mut late_batch = make_ready_batch(&[job_b, job_q], TASKS_PER_JOB, 0);
    late_batch.extend(make_ready_batch(&canary_jobs, TASKS_PER_JOB, 0));
    storage_client.push_ready_batch(DEFAULT_SESSION_ID, late_batch);

    let late_assignments: Vec<_> = tick_and_drain_n(&mut scheduler, &reader, TASKS_PER_JOB)
        .await?
        .into_iter()
        .map(|(_session_id, assignment)| assignment)
        .collect();
    assert_strict_rotation(&late_assignments, &canary_jobs, TASKS_PER_JOB);
    assert_no_further_assignments(&mut scheduler, &reader).await?;

    Ok(())
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

#[tokio::test(flavor = "multi_thread")]
async fn commit_drains_each_cycle_cleanup_dispatches_once() -> anyhow::Result<()> {
    const NUM_ACTIVE_JOBS: usize = 4;
    const TASKS_PER_JOB: usize = 3;
    const NUM_COMMIT_READY_JOBS: usize = NUM_ACTIVE_JOBS * TASKS_PER_JOB - 1;
    const NUM_CLEANUP_READY_JOBS: usize = TASKS_PER_JOB;
    const DISPATCH_QUEUE_CAPACITY: usize = 1024;

    let active_jobs = make_jobs(NUM_ACTIVE_JOBS);
    let commit_ready_jobs = make_jobs(NUM_COMMIT_READY_JOBS);
    let cleanup_ready_jobs = make_jobs(NUM_CLEANUP_READY_JOBS);

    let storage_client = MockStorageClient::new(DEFAULT_SESSION_ID);
    storage_client.push_ready_batch(
        DEFAULT_SESSION_ID,
        make_ready_batch(&active_jobs, TASKS_PER_JOB, 0),
    );
    let mut commit_ready_batch = make_finalizing_batch(&commit_ready_jobs, TaskId::Commit);
    // Duplicate one commit-ready entry within the batch: it must dispatch exactly once.
    commit_ready_batch.push(commit_ready_batch[0]);
    storage_client.push_commit_ready_batch(DEFAULT_SESSION_ID, commit_ready_batch);
    storage_client.push_cleanup_ready_batch(
        DEFAULT_SESSION_ID,
        make_finalizing_batch(&cleanup_ready_jobs, TaskId::Cleanup),
    );

    let (writer, reader) = create_dispatch_queue(DISPATCH_QUEUE_CAPACITY, DEFAULT_SESSION_ID);
    let config = make_config(NUM_ACTIVE_JOBS, DISPATCH_QUEUE_CAPACITY);
    let (scheduler_handle, cancellation_token) = spawn_scheduler(config, storage_client, writer);

    let num_assignments =
        NUM_ACTIVE_JOBS * TASKS_PER_JOB + NUM_COMMIT_READY_JOBS + NUM_CLEANUP_READY_JOBS;
    let assignments = drain_n(&reader, num_assignments).await?;
    assert_no_more_assignments(&reader).await?;

    // The rotation is [commit lane, cleanup lane, active jobs...]. The commit lane drains up to
    // `active_job_queue_capacity` (== NUM_ACTIVE_JOBS) jobs per visit, so each cycle dispatches a
    // full chunk of NUM_ACTIVE_JOBS commit tasks (the final cycle dispatches the short remainder),
    // one cleanup task, and one task of every active job. All lanes are drained FIFO.
    let expected: Vec<(JobId, ResourceGroupId, TaskId)> = commit_ready_jobs
        .chunks(NUM_ACTIVE_JOBS)
        .enumerate()
        .flat_map(|(round, commit_chunk)| {
            let (cleanup_job_id, cleanup_resource_group_id) = cleanup_ready_jobs[round];
            commit_chunk
                .iter()
                .map(|&(job_id, resource_group_id)| (job_id, resource_group_id, TaskId::Commit))
                .chain(std::iter::once((
                    cleanup_job_id,
                    cleanup_resource_group_id,
                    TaskId::Cleanup,
                )))
                .chain(active_jobs.iter().map(move |&(job_id, resource_group_id)| {
                    (job_id, resource_group_id, TaskId::Index(round))
                }))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(make_assignment_tuple(&assignments), expected);

    cancellation_token.cancel();
    scheduler_handle.await.expect("scheduler task panicked")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_ready_drops_active_and_pending_jobs() -> anyhow::Result<()> {
    assert_finalizing_ready_drops_jobs(TaskId::Cleanup).await
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_ready_drops_active_and_pending_jobs() -> anyhow::Result<()> {
    assert_finalizing_ready_drops_jobs(TaskId::Commit).await
}

#[tokio::test(flavor = "multi_thread")]
async fn session_bump_clears_buffered_tasks() -> anyhow::Result<()> {
    const ACTIVE_JOB_QUEUE_CAPACITY: usize = 4;
    const DISPATCH_QUEUE_CAPACITY: usize = 4;
    const TASKS_PER_JOB: usize = 4;
    const NEW_SESSION_ID: SessionId = DEFAULT_SESSION_ID + 1;
    const NEW_TASKS_PER_JOB: usize = 2;

    let old_jobs = make_jobs(4);
    let new_jobs = make_jobs(2);

    let storage_client = MockStorageClient::new(DEFAULT_SESSION_ID);
    storage_client.push_ready_batch(
        DEFAULT_SESSION_ID,
        make_ready_batch(&old_jobs, TASKS_PER_JOB, 0),
    );

    let (writer, reader) = create_dispatch_queue(DISPATCH_QUEUE_CAPACITY, DEFAULT_SESSION_ID);
    let mut scheduler = make_scheduler(
        make_config(ACTIVE_JOB_QUEUE_CAPACITY, DISPATCH_QUEUE_CAPACITY),
        storage_client.clone(),
        writer,
    );

    // Step 1: ingest the old-session batch. The ingesting tick dispatches enough assignments to
    // fill the dispatch queue (which the test never drains); the rest will stay in the buffer.
    tick_until(&mut scheduler, |scheduler| {
        !scheduler.buffered_tasks.is_empty()
    })
    .await?;
    assert_eq!(scheduler.active_jobs.len(), old_jobs.len());
    assert_eq!(
        scheduler.buffered_tasks.len(),
        old_jobs.len() * TASKS_PER_JOB - DISPATCH_QUEUE_CAPACITY,
    );

    // Step 2: bump the session on the storage side and deliver a batch under the new session.
    storage_client.set_session(NEW_SESSION_ID);
    storage_client.push_ready_batch(
        NEW_SESSION_ID,
        make_ready_batch(&new_jobs, NEW_TASKS_PER_JOB, 0),
    );
    tick_until(&mut scheduler, |scheduler| {
        scheduler.storage_session_id == NEW_SESSION_ID
            && new_jobs
                .iter()
                .all(|(job_id, _)| scheduler.active_jobs.contains_key(job_id))
    })
    .await?;

    assert_eq!(
        scheduler
            .active_jobs
            .keys()
            .copied()
            .collect::<HashSet<_>>(),
        new_jobs
            .iter()
            .map(|&(job_id, _)| job_id)
            .collect::<HashSet<_>>(),
    );
    assert_eq!(scheduler.pending_jobs.len(), 0);
    assert!(
        scheduler.buffered_tasks.iter().all(|(job_id, _)| {
            new_jobs
                .iter()
                .any(|&(new_job_id, _)| *job_id == new_job_id)
        }),
        "an old-session task survived the session bump",
    );

    // The session bump drained the dispatch queue: the frozen old-session assignments are gone, and
    // draining yields exactly the new jobs' tasks in strict rotation, each paired with the new
    // session.
    let num_new_assignments = new_jobs.len() * NEW_TASKS_PER_JOB;
    let session_stamped = tick_and_drain_n(&mut scheduler, &reader, num_new_assignments).await?;
    assert_no_further_assignments(&mut scheduler, &reader).await?;

    for &(session_id, _) in &session_stamped {
        assert_eq!(session_id, NEW_SESSION_ID);
    }

    let assignments: Vec<TaskAssignment> = session_stamped
        .into_iter()
        .map(|(_session_id, assignment)| assignment)
        .collect();
    assert_strict_rotation(&assignments, &new_jobs, NEW_TASKS_PER_JOB);

    Ok(())
}
