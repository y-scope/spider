//! Unit tests for the resource-group-aware round-robin scheduler core.
//!
//! The tests drive [`RgRoundRobin::tick`] and the structures it decides with directly.
//! Whatever an execution manager would do -- draining a group's queue -- a test body does by hand
//! through the same entry points the dispatch service will use.

/// Drives ticks on `core` until `predicate` holds, failing the calling test if it does not hold
/// within [`TICK_DEADLINE`].
///
/// A macro rather than an async function so that the predicate is an expression rather than a
/// closure, and may therefore borrow whatever the tick mutates.
macro_rules! tick_until {
    ($core:expr, $predicate:expr) => {{
        let deadline = tokio::time::Instant::now() + TICK_DEADLINE;
        loop {
            $core.tick().await?;
            if $predicate {
                break;
            }
            if deadline < tokio::time::Instant::now() {
                ::anyhow::bail!("the core did not reach the expected state in time");
            }
            tokio::time::sleep(TICK_RETRY_INTERVAL).await;
        }
    }};
}

use std::collections::HashSet;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::time::Duration;

use anyhow::bail;
use spider_core::session::SessionTracker;
use spider_core::task::TaskIndex;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskAssignmentId;
use spider_core::types::id::TaskId;
use tokio_util::sync::CancellationToken;

use super::dispatch_queue::DispatchQueueRegistry;
use super::dispatch_queue::RgDispatchQueueReader;
use super::implementation::RgRoundRobin;
use super::implementation::RgRoundRobinConfig;
use super::job_registry::JobKey;
use super::job_registry::JobRegistry;
use super::job_registry::UpsertOutcome;
use super::scheduling_state::RgSchedulingState;
use crate::SchedulerError;
use crate::TaskAssignment;
use crate::core::TaskAssignmentIdIssuer;
use crate::core_impl::inbound_queue_reader::test_harness::DEFAULT_SESSION_ID;
use crate::core_impl::inbound_queue_reader::test_harness::MockStorageClient;
use crate::core_impl::inbound_queue_reader::test_harness::make_entry;

/// The number of active jobs a resource group may hold.
const ACTIVE_JOB_LIST_CAPACITY: usize = 4;

/// The first resource group a test seeds.
const RG_A: ResourceGroupId = ResourceGroupId::from(0);

/// The second resource group a test seeds.
const RG_B: ResourceGroupId = ResourceGroupId::from(1);

/// The third resource group a test seeds.
const RG_C: ResourceGroupId = ResourceGroupId::from(2);

/// The session a test bumps the mock storage to.
const NEXT_SESSION_ID: SessionId = DEFAULT_SESSION_ID + 1;

/// The storage poll timeout every test runs with. The mock storage never blocks on it.
const STORAGE_POLL_TIMEOUT_MS: u64 = 10;

/// The regular-task buffer capacity every test runs with.
const READY_TASK_CAPACITY: usize = 16_384;

/// The commit-task buffer capacity every test runs with.
const COMMIT_READY_TASK_CAPACITY: usize = 64;

/// The cleanup-task buffer capacity every test runs with.
const CLEANUP_READY_TASK_CAPACITY: usize = 64;

/// A finalized job table expiry long enough that no test sweeps an entry unless it asks to.
const NEVER_EXPIRING_TIMEOUT_SEC: u64 = 6 * 60 * 60;

/// The finalized job table expiry an expiry test runs with.
const SHORT_EXPIRATION_TIMEOUT_SEC: u64 = 1;

/// How long an expiry test waits before the tick that must sweep an entry stamped
/// [`SHORT_EXPIRATION_TIMEOUT_SEC`] seconds ago. A sleep is a lower bound on the time that passes,
/// so the margin only has to cover the sweep reading a monotonic clock, never scheduling delay.
const EXPIRATION_WAIT: Duration = Duration::from_millis(1_200);

/// The longest a test waits for the ticks it drives to reach the state it expects.
const TICK_DEADLINE: Duration = Duration::from_secs(10);

/// The interval between two ticks driven by [`tick_until`].
const TICK_RETRY_INTERVAL: Duration = Duration::from_millis(2);

/// A core wired to a mock storage and to the dispatch structures a test inspects, driven by manual
/// [`RgRoundRobin::tick`] calls.
struct CoreFixture {
    core: RgRoundRobin<MockStorageClient>,
    storage: MockStorageClient,
    dispatch_queue_registry: DispatchQueueRegistry,
    session_tracker: SessionTracker,
    reschedule_queue_writer: tokio::sync::mpsc::UnboundedSender<TaskAssignment>,
    active_job_list_capacity: usize,
}

impl CoreFixture {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created fixture whose core holds no buffered task and no active resource group.
    fn new(config: RgRoundRobinConfig, storage: MockStorageClient) -> Self {
        let active_job_list_capacity = config.active_job_list_capacity.get();
        let (reschedule_queue_writer, reschedule_queue_reader) =
            tokio::sync::mpsc::unbounded_channel();
        let core = RgRoundRobin::new(
            storage.clone(),
            reschedule_queue_reader,
            TaskAssignmentIdIssuer::new(),
            CancellationToken::new(),
            config,
        );
        let dispatch_queue_registry = core.dispatch_queue_registry();
        let session_tracker = core.session_tracker.clone();
        Self {
            core,
            storage,
            dispatch_queue_registry,
            session_tracker,
            reschedule_queue_writer,
            active_job_list_capacity,
        }
    }

    /// Puts `rg_id` on the core's active resource group list, appending its scheduling state built
    /// against the group's dispatch queue endpoints if the core has none.
    ///
    /// # Returns
    ///
    /// The position of the group's scheduling state in the core's state vector.
    fn activate_group(&mut self, rg_id: ResourceGroupId) -> usize {
        if let Some(state_idx) = self.core.rg_id_to_idx_map.get(&rg_id) {
            return *state_idx;
        }

        let mut rg_state = RgSchedulingState::new(
            rg_id,
            self.dispatch_queue_registry
                .get_dispatch_queue_writer(rg_id),
            self.active_job_list_capacity,
        );
        rg_state.is_active = true;
        let state_idx = self.core.rg_states.len();
        self.core.rg_states.push(rg_state);
        self.core.rg_id_to_idx_map.insert(rg_id, state_idx);
        self.core.active_rg_list.push(state_idx);
        state_idx
    }

    /// Registers a job of `num_tasks` buffered ready tasks against `rg_id`, exactly as a completed
    /// inbound poll carrying that job would, and activates the group.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`make_job_entry`]'s return values on failure.
    fn seed_job(
        &mut self,
        rg_id: ResourceGroupId,
        job_id: JobId,
        num_tasks: usize,
    ) -> anyhow::Result<()> {
        for task_index in 0..num_tasks {
            self.core
                .global_task_set
                .insert(job_id, TaskId::Index(task_index));
        }
        let job_key = make_job_entry(&mut self.core.job_registry, job_id, num_tasks)?;
        let state_idx = self.activate_group(rg_id);
        self.core.rg_states[state_idx].place_new_job(job_key);
        Ok(())
    }

    /// Puts `num_assignments` assignments straight into `rg_id`'s dispatch queue, standing in for
    /// the occupancy a group carries over from earlier ticks.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`super::dispatch_queue::RgDispatchQueueWriter::try_send`]'s return values on
    ///   failure.
    fn preload_queue(&self, rg_id: ResourceGroupId, num_assignments: usize) -> anyhow::Result<()> {
        let writer = self
            .dispatch_queue_registry
            .get_dispatch_queue_writer(rg_id);
        for idx in 0..num_assignments {
            writer.try_send(make_assignment(
                rg_id,
                JobId::from(u64::MAX),
                TaskId::Index(idx),
                self.session_tracker.current(),
            ))?;
        }
        Ok(())
    }

    /// # Returns
    ///
    /// The number of assignments currently queued for `rg_id`.
    fn queue_len(&self, rg_id: ResourceGroupId) -> usize {
        self.dispatch_queue_registry
            .get_dispatch_queue_writer(rg_id)
            .queue_len()
    }

    /// # Returns
    ///
    /// The read side of `rg_id`'s dispatch queue, which a test hands to [`drain_reader`] to play a
    /// pinned execution manager.
    fn reader(&self, rg_id: ResourceGroupId) -> RgDispatchQueueReader {
        self.dispatch_queue_registry
            .get_dispatch_queue_reader(rg_id)
    }
}

#[tokio::test]
async fn the_rotation_arm_persists_across_ticks() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 2;

    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        MockStorageClient::new(),
    );
    for (idx, rg_id) in [RG_A, RG_B, RG_C].into_iter().enumerate() {
        fixture.seed_job(rg_id, JobId::from(u64::try_from(idx)?), 8)?;
    }

    fixture.core.tick().await?;
    assert_eq!(occupancies_of(&fixture, 3), vec![1, 1, 0]);
    assert_eq!(fixture.core.last_served_rg, Some(RG_B));

    for rg_id in [RG_A, RG_B, RG_C] {
        drain_reader(&fixture.reader(rg_id)).await;
    }

    fixture.core.tick().await?;
    assert_eq!(occupancies_of(&fixture, 3), vec![1, 0, 1]);
    assert_eq!(fixture.core.last_served_rg, Some(RG_A));
    Ok(())
}

#[tokio::test]
async fn dropping_an_exhausted_group_does_not_skip_the_group_moved_into_its_slot()
-> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 1;

    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        MockStorageClient::new(),
    );
    fixture.seed_job(RG_A, JobId::from(0), 8)?;
    fixture.activate_group(RG_B);
    fixture.seed_job(RG_C, JobId::from(2), 8)?;

    // The arm starts on the group that has nothing to schedule, so the tick's single assignment is
    // won by whichever group the removal moves into the vacated slot.
    fixture.core.last_served_rg = Some(RG_A);
    fixture.core.tick().await?;

    assert_eq!(occupancies_of(&fixture, 3), vec![0, 0, 1]);
    assert_eq!(fixture.core.last_served_rg, Some(RG_C));
    assert_eq!(fixture.core.active_rg_list.len(), 2);
    Ok(())
}

#[tokio::test]
async fn an_exhausted_group_stays_active_until_its_dispatch_queue_drains() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 4;

    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        MockStorageClient::new(),
    );
    fixture.activate_group(RG_A);
    fixture.preload_queue(RG_A, 1)?;

    fixture.core.tick().await?;
    assert_eq!(fixture.core.active_rg_list.len(), 1);
    assert!(is_active(&fixture, RG_A));

    assert_eq!(drain_reader(&fixture.reader(RG_A)).await.len(), 1);
    fixture.core.tick().await?;
    assert_eq!(fixture.core.active_rg_list, Vec::<usize>::new());
    assert!(!is_active(&fixture, RG_A));
    Ok(())
}

#[tokio::test]
async fn dispatching_and_retirement_run_while_a_storage_poll_is_in_flight() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 4;
    // The lone group's admission threshold caps it at half the dispatch buffer per tick.
    const NUM_TASKS_PER_TICK: usize = DISPATCH_QUEUE_CAPACITY / 2;
    const NUM_TASKS: usize = 2 * NUM_TASKS_PER_TICK;

    let storage = MockStorageClient::new();
    storage.gate_ready_lane();
    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        storage,
    );
    fixture.seed_job(RG_A, JobId::from(0), NUM_TASKS)?;
    fixture.seed_job(RG_B, JobId::from(1), 0)?;

    // The first tick starts the poll the gate holds, so every later tick finds it still in flight.
    fixture.core.tick().await?;
    assert_eq!(fixture.queue_len(RG_A), NUM_TASKS_PER_TICK);
    assert_eq!(fixture.core.job_registry.len(), 2);

    assert_eq!(
        drain_reader(&fixture.reader(RG_A)).await.len(),
        NUM_TASKS_PER_TICK
    );
    fixture.core.tick().await?;
    assert_eq!(fixture.queue_len(RG_A), NUM_TASKS_PER_TICK);
    assert_eq!(fixture.core.global_task_set.tasks, HashSet::new());

    fixture.core.tick().await?;
    assert_eq!(fixture.core.job_registry.len(), 1);
    assert_eq!(fixture.core.active_rg_list.len(), 1);
    assert_eq!(fixture.storage.num_polls().0, 0);
    Ok(())
}

#[tokio::test]
async fn a_session_bump_clears_the_dedup_set_and_the_finalized_job_table() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 8;
    const SENTINEL_JOB_ID: JobId = JobId::from(4096);

    let storage = MockStorageClient::new();
    storage.push_ready_batch(
        DEFAULT_SESSION_ID,
        vec![
            make_entry(RG_A, JobId::from(0), TaskId::Index(0)),
            make_entry(RG_A, JobId::from(0), TaskId::Index(1)),
        ],
    );
    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        storage,
    );
    tick_until!(fixture.core, 2 == fixture.queue_len(RG_A));

    fixture
        .core
        .global_task_set
        .insert(SENTINEL_JOB_ID, TaskId::Index(0));
    fixture.core.finalized_jobs.insert(SENTINEL_JOB_ID);

    fixture
        .storage
        .push_ready_batch(NEXT_SESSION_ID, Vec::new());
    tick_until!(
        fixture.core,
        NEXT_SESSION_ID == fixture.session_tracker.current()
    );

    assert!(
        !fixture
            .core
            .global_task_set
            .tasks
            .contains(&(SENTINEL_JOB_ID, TaskId::Index(0)))
    );
    assert!(!fixture.core.finalized_jobs.contains(&SENTINEL_JOB_ID));
    Ok(())
}

#[tokio::test]
async fn a_session_bump_readmits_the_tasks_storage_replays() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 1;

    let replayed_entries = vec![
        make_entry(RG_A, JobId::from(0), TaskId::Index(0)),
        make_entry(RG_A, JobId::from(0), TaskId::Index(1)),
    ];
    let storage = MockStorageClient::new();
    storage.push_ready_batch(DEFAULT_SESSION_ID, replayed_entries.clone());
    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        storage,
    );
    tick_until!(fixture.core, 1 == fixture.queue_len(RG_A));

    // The buffer holds one assignment, so the job's second task is still buffered in the core and
    // still in the dedup set when the bump lands.
    let published = drain_reader(&fixture.reader(RG_A)).await;
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].session_id, DEFAULT_SESSION_ID);
    assert_eq!(fixture.core.global_task_set.len(), 1);

    fixture
        .storage
        .push_ready_batch(NEXT_SESSION_ID, replayed_entries);
    let mut replayed = Vec::new();
    let deadline = tokio::time::Instant::now() + TICK_DEADLINE;
    while replayed.len() < 2 {
        fixture.core.tick().await?;
        // A poll issued before the bump still lands under the old session, so the assignments it
        // produces are stale by construction and are not part of the replay.
        replayed.extend(
            drain_reader(&fixture.reader(RG_A))
                .await
                .into_iter()
                .filter(|assignment| assignment.session_id == NEXT_SESSION_ID),
        );
        if deadline < tokio::time::Instant::now() {
            bail!("storage's replayed tasks were not re-admitted: {replayed:?}");
        }
        tokio::time::sleep(TICK_RETRY_INTERVAL).await;
    }

    let replayed_task_ids: HashSet<TaskId> = replayed
        .iter()
        .map(|assignment| assignment.task_id)
        .collect();
    assert_eq!(
        replayed_task_ids,
        HashSet::from([TaskId::Index(0), TaskId::Index(1)])
    );
    Ok(())
}

#[tokio::test]
async fn a_rescheduled_assignment_is_readmitted() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 8;
    const LOST_JOB_ID: JobId = JobId::from(0);
    const LOST_TASK_ID: TaskId = TaskId::Index(9);

    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        MockStorageClient::new(),
    );
    let lost = make_assignment(
        RG_A,
        LOST_JOB_ID,
        LOST_TASK_ID,
        fixture.session_tracker.current(),
    );
    fixture.reschedule_queue_writer.send(lost)?;

    tick_until!(fixture.core, 1 == fixture.queue_len(RG_A));

    let redispatched = drain_reader(&fixture.reader(RG_A)).await;
    assert_eq!(redispatched.len(), 1);
    assert_eq!(redispatched[0].task_id, LOST_TASK_ID);
    assert_eq!(redispatched[0].job_id, LOST_JOB_ID);
    Ok(())
}

#[tokio::test]
async fn a_closed_dispatch_queue_fails_the_tick() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 4;

    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        MockStorageClient::new(),
    );
    fixture.seed_job(RG_A, JobId::from(0), 4)?;
    fixture.dispatch_queue_registry.close_dispatch_queue(RG_A);

    let err = fixture
        .core
        .tick()
        .await
        .expect_err("a closed dispatch queue fails the tick");
    let SchedulerError::DispatchQueueClosed = err else {
        bail!("the tick failed with something other than a closed dispatch queue: {err:?}");
    };
    Ok(())
}

#[tokio::test]
async fn a_closed_broadcast_queue_fails_the_tick() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 4;

    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        MockStorageClient::new(),
    );
    fixture.seed_job(RG_A, JobId::from(0), 4)?;
    fixture.dispatch_queue_registry.close_broadcast_queue();

    let err = fixture
        .core
        .tick()
        .await
        .expect_err("a closed broadcast queue fails the tick");
    let SchedulerError::DispatchQueueClosed = err else {
        bail!("the tick failed with something other than a closed dispatch queue: {err:?}");
    };

    // Both closures fail the tick with the same error, so the queue is what tells them apart: this
    // assignment reached the group's queue first and lost only the hint covering it.
    assert_eq!(fixture.queue_len(RG_A), 1);
    Ok(())
}

#[tokio::test]
async fn an_expired_finalized_job_leaves_the_table_while_a_fresh_one_stays() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 8;
    const EXPIRING_JOB_ID: JobId = JobId::from(0);
    const FRESH_JOB_ID: JobId = JobId::from(1);

    let storage = MockStorageClient::new();
    storage.push_commit_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, EXPIRING_JOB_ID, TaskId::Commit)],
    );
    let mut fixture = CoreFixture::new(
        make_expiring_config(
            DISPATCH_QUEUE_CAPACITY,
            ACTIVE_JOB_LIST_CAPACITY,
            SHORT_EXPIRATION_TIMEOUT_SEC,
        ),
        storage,
    );
    tick_until!(
        fixture.core,
        fixture.core.finalized_jobs.contains(&EXPIRING_JOB_ID)
    );
    assert_eq!(finalized_job_ids(&fixture), vec![EXPIRING_JOB_ID]);

    tokio::time::sleep(EXPIRATION_WAIT).await;
    fixture.storage.push_commit_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, FRESH_JOB_ID, TaskId::Commit)],
    );
    tick_until!(
        fixture.core,
        fixture.core.finalized_jobs.contains(&FRESH_JOB_ID)
    );

    assert_eq!(fixture.core.finalized_jobs, HashSet::from([FRESH_JOB_ID]));
    assert_eq!(finalized_job_ids(&fixture), vec![FRESH_JOB_ID]);
    Ok(())
}

#[tokio::test]
async fn a_cleanup_is_scheduled_after_the_same_job_committed() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 8;
    const JOB_ID: JobId = JobId::from(0);

    let storage = MockStorageClient::new();
    storage.push_commit_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, JOB_ID, TaskId::Commit)],
    );
    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        storage,
    );
    tick_until!(fixture.core, 1 == fixture.queue_len(RG_A));

    fixture.storage.push_cleanup_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, JOB_ID, TaskId::Cleanup)],
    );
    tick_until!(fixture.core, 2 == fixture.queue_len(RG_A));

    let task_ids: Vec<TaskId> = drain_reader(&fixture.reader(RG_A))
        .await
        .into_iter()
        .map(|assignment| assignment.task_id)
        .collect();
    assert_eq!(task_ids, vec![TaskId::Commit, TaskId::Cleanup]);
    Ok(())
}

#[tokio::test]
async fn a_repeated_finalization_is_scheduled_once() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 8;
    const JOB_ID: JobId = JobId::from(0);

    let storage = MockStorageClient::new();
    storage.push_commit_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, JOB_ID, TaskId::Commit)],
    );
    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        storage,
    );
    tick_until!(fixture.core, 1 == fixture.queue_len(RG_A));

    fixture.storage.push_commit_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, JOB_ID, TaskId::Commit)],
    );
    fixture.core.tick().await?;
    fixture.core.tick().await?;

    assert_eq!(fixture.queue_len(RG_A), 1);
    Ok(())
}

#[tokio::test]
async fn an_expired_finalization_readmits_the_jobs_later_tasks() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 8;
    const JOB_ID: JobId = JobId::from(0);
    const LATE_TASK_ID: TaskId = TaskId::Index(0);

    let storage = MockStorageClient::new();
    storage.push_commit_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, JOB_ID, TaskId::Commit)],
    );
    let mut fixture = CoreFixture::new(
        make_expiring_config(
            DISPATCH_QUEUE_CAPACITY,
            ACTIVE_JOB_LIST_CAPACITY,
            SHORT_EXPIRATION_TIMEOUT_SEC,
        ),
        storage,
    );
    tick_until!(fixture.core, 1 == fixture.queue_len(RG_A));
    let finalization = drain_reader(&fixture.reader(RG_A)).await;
    assert_eq!(finalization.len(), 1);
    assert_eq!(finalization[0].task_id, TaskId::Commit);

    let num_polls_before = fixture.storage.num_polls().0;
    fixture.storage.push_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, JOB_ID, LATE_TASK_ID)],
    );
    tick_until!(
        fixture.core,
        num_polls_before + 2 <= fixture.storage.num_polls().0
    );
    assert_eq!(fixture.queue_len(RG_A), 0);
    assert_eq!(fixture.core.global_task_set.tasks, HashSet::new());
    assert_eq!(fixture.core.job_registry.len(), 0);

    tokio::time::sleep(EXPIRATION_WAIT).await;
    tick_until!(fixture.core, fixture.core.finalized_jobs.is_empty());

    fixture.storage.push_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, JOB_ID, LATE_TASK_ID)],
    );
    tick_until!(fixture.core, 1 == fixture.queue_len(RG_A));
    let readmitted = drain_reader(&fixture.reader(RG_A)).await;
    assert_eq!(readmitted.len(), 1);
    assert_eq!(readmitted[0].task_id, LATE_TASK_ID);
    Ok(())
}

#[tokio::test]
async fn a_session_bump_empties_the_finalized_job_table_and_its_queue() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 8;
    const JOB_ID: JobId = JobId::from(0);

    let storage = MockStorageClient::new();
    storage.push_commit_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, JOB_ID, TaskId::Commit)],
    );
    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        storage,
    );
    tick_until!(fixture.core, fixture.core.finalized_jobs.contains(&JOB_ID));
    assert_eq!(finalized_job_ids(&fixture), vec![JOB_ID]);

    fixture
        .storage
        .push_ready_batch(NEXT_SESSION_ID, Vec::new());
    tick_until!(
        fixture.core,
        NEXT_SESSION_ID == fixture.session_tracker.current()
    );

    assert_eq!(fixture.core.finalized_jobs, HashSet::new());
    assert_eq!(finalized_job_ids(&fixture), Vec::<JobId>::new());
    Ok(())
}

#[tokio::test]
async fn publishing_an_assignment_discounts_the_lane_that_buffered_it() -> anyhow::Result<()> {
    // One assignment preloaded into the group's queue leaves the tick no free space, so every task
    // the batches deliver stays buffered until the test drains the queue.
    const DISPATCH_QUEUE_CAPACITY: usize = 1;
    const REGULAR_JOB_ID: JobId = JobId::from(0);
    const COMMIT_JOB_ID: JobId = JobId::from(1);
    const NUM_REGULAR_TASKS: usize = 2;

    let storage = MockStorageClient::new();
    storage.push_ready_batch(
        DEFAULT_SESSION_ID,
        vec![
            make_entry(RG_A, REGULAR_JOB_ID, TaskId::Index(0)),
            make_entry(RG_A, REGULAR_JOB_ID, TaskId::Index(1)),
        ],
    );
    storage.push_commit_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, COMMIT_JOB_ID, TaskId::Commit)],
    );
    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        storage,
    );
    fixture.preload_queue(RG_A, DISPATCH_QUEUE_CAPACITY)?;

    tick_until!(
        fixture.core,
        fixture.core.finalized_jobs.contains(&COMMIT_JOB_ID)
    );
    assert_eq!(lane_counts(&fixture), (NUM_REGULAR_TASKS, 1, 0));
    assert_eq!(
        fixture.core.global_task_set.len(),
        NUM_REGULAR_TASKS + 1,
        "every buffered task is counted exactly once"
    );

    // Draining the preloaded assignment frees exactly one slot, so the next tick publishes exactly
    // one assignment: the finalization, which outranks the regular tasks.
    assert_eq!(drain_reader(&fixture.reader(RG_A)).await.len(), 1);
    fixture.core.tick().await?;
    let published = drain_reader(&fixture.reader(RG_A)).await;
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].task_id, TaskId::Commit);
    assert_eq!(lane_counts(&fixture), (NUM_REGULAR_TASKS, 0, 0));

    fixture.core.tick().await?;
    let published = drain_reader(&fixture.reader(RG_A)).await;
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].job_id, REGULAR_JOB_ID);
    assert_eq!(lane_counts(&fixture), (NUM_REGULAR_TASKS - 1, 0, 0));
    Ok(())
}

#[tokio::test]
async fn the_inbound_poll_is_sized_from_the_lane_counters() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 1;
    const NUM_REGULAR_TASKS: usize = 3;
    const NUM_COMMIT_READY_JOBS: usize = 2;
    const NUM_CLEANUP_READY_JOBS: usize = 2;

    let storage = MockStorageClient::new();
    storage.push_ready_batch(
        DEFAULT_SESSION_ID,
        vec![
            make_entry(RG_A, JobId::from(0), TaskId::Index(0)),
            make_entry(RG_A, JobId::from(0), TaskId::Index(1)),
            make_entry(RG_A, JobId::from(0), TaskId::Index(2)),
        ],
    );
    storage.push_commit_ready_batch(
        DEFAULT_SESSION_ID,
        vec![
            make_entry(RG_A, JobId::from(1), TaskId::Commit),
            make_entry(RG_A, JobId::from(2), TaskId::Commit),
        ],
    );
    storage.push_cleanup_ready_batch(
        DEFAULT_SESSION_ID,
        vec![
            make_entry(RG_A, JobId::from(3), TaskId::Cleanup),
            make_entry(RG_A, JobId::from(4), TaskId::Cleanup),
        ],
    );
    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        storage,
    );
    fixture.preload_queue(RG_A, DISPATCH_QUEUE_CAPACITY)?;

    let num_finalized_jobs = NUM_COMMIT_READY_JOBS + NUM_CLEANUP_READY_JOBS;
    tick_until!(
        fixture.core,
        num_finalized_jobs == fixture.core.finalized_jobs.len()
    );
    assert_eq!(
        lane_counts(&fixture),
        (
            NUM_REGULAR_TASKS,
            NUM_COMMIT_READY_JOBS,
            NUM_CLEANUP_READY_JOBS
        )
    );

    // Every later poll is sized from the same counts, because nothing is published while the
    // preloaded assignment holds the buffer full.
    let (_, num_commit_ready_polls_before, _) = fixture.storage.num_polls();
    tick_until!(
        fixture.core,
        num_commit_ready_polls_before < fixture.storage.num_polls().1
    );
    assert_eq!(
        fixture.storage.last_poll_limits(),
        (
            READY_TASK_CAPACITY - NUM_REGULAR_TASKS,
            COMMIT_READY_TASK_CAPACITY - NUM_COMMIT_READY_JOBS,
            CLEANUP_READY_TASK_CAPACITY - NUM_CLEANUP_READY_JOBS
        )
    );
    Ok(())
}

#[tokio::test]
async fn a_session_bump_zeroes_every_lane_counter() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 1;

    let storage = MockStorageClient::new();
    storage.push_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, JobId::from(0), TaskId::Index(0))],
    );
    storage.push_commit_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, JobId::from(1), TaskId::Commit)],
    );
    storage.push_cleanup_ready_batch(
        DEFAULT_SESSION_ID,
        vec![make_entry(RG_A, JobId::from(2), TaskId::Cleanup)],
    );
    let mut fixture = CoreFixture::new(
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        storage,
    );
    fixture.preload_queue(RG_A, DISPATCH_QUEUE_CAPACITY)?;

    tick_until!(fixture.core, 2 == fixture.core.finalized_jobs.len());
    assert_eq!(lane_counts(&fixture), (1, 1, 1));

    fixture
        .storage
        .push_ready_batch(NEXT_SESSION_ID, Vec::new());
    tick_until!(
        fixture.core,
        NEXT_SESSION_ID == fixture.session_tracker.current()
    );

    assert_eq!(lane_counts(&fixture), (0, 0, 0));
    assert_eq!(fixture.core.global_task_set.tasks, HashSet::new());
    Ok(())
}

#[tokio::test]
async fn the_scheduling_loop_stops_when_it_is_cancelled() -> anyhow::Result<()> {
    const DISPATCH_QUEUE_CAPACITY: usize = 4;

    let (_reschedule_queue_writer, reschedule_queue_reader) =
        tokio::sync::mpsc::unbounded_channel();
    let cancellation_token = CancellationToken::new();
    let core = RgRoundRobin::new(
        MockStorageClient::new(),
        reschedule_queue_reader,
        TaskAssignmentIdIssuer::new(),
        cancellation_token.clone(),
        make_config(DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
    );
    let scheduler_handle = tokio::task::spawn(core.run());

    cancellation_token.cancel();
    tokio::time::timeout(TICK_DEADLINE, scheduler_handle).await???;
    Ok(())
}

#[tokio::test]
async fn one_tick_leaves_every_backlogged_group_at_the_dynamic_threshold() -> anyhow::Result<()> {
    let expected_share = ADMISSION_DISPATCH_QUEUE_CAPACITY / (NUM_BACKLOGGED_GROUPS + 1);
    let mut fixture = new_admission_fixture();
    seed_backlogged_groups(&mut fixture, NUM_BACKLOGGED_GROUPS)?;

    fixture.core.tick().await?;

    let occupancies = occupancies_of(&fixture, NUM_BACKLOGGED_GROUPS);
    let occupancy: usize = occupancies.iter().sum();
    let free = ADMISSION_DISPATCH_QUEUE_CAPACITY - occupancy;
    for (idx, group_occupancy) in occupancies.iter().enumerate() {
        assert!(
            group_occupancy.abs_diff(expected_share) <= SHARE_TOLERANCE,
            "group {idx} holds {group_occupancy} assignments, expected about {expected_share}: \
             {occupancies:?}"
        );
    }
    assert!(
        free.abs_diff(expected_share) <= SHARE_TOLERANCE,
        "the tick left {free} free, expected about {expected_share}: {occupancies:?}"
    );

    let published: usize =
        NUM_BACKLOGGED_GROUPS * NUM_TASKS_PER_JOB - fixture.core.global_task_set.len();
    assert_eq!(published, occupancy);
    Ok(())
}

#[tokio::test]
async fn no_group_is_batch_filled_while_another_waits() -> anyhow::Result<()> {
    let mut fixture = new_admission_fixture();
    seed_backlogged_groups(&mut fixture, NUM_BACKLOGGED_GROUPS)?;

    fixture.core.tick().await?;

    let occupancies = occupancies_of(&fixture, NUM_BACKLOGGED_GROUPS);
    let most = *occupancies
        .iter()
        .max()
        .expect("the tick served at least one group");
    let least = *occupancies
        .iter()
        .min()
        .expect("the tick served at least one group");
    assert!(
        most - least <= 2,
        "the rotation is not interleaved at quantum 1: {occupancies:?}"
    );
    assert!(
        most < ADMISSION_DISPATCH_QUEUE_CAPACITY / 2,
        "a single group took half the dispatch buffer: {occupancies:?}"
    );
    assert!(
        least > 0,
        "a backlogged group was left at zero: {occupancies:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_newly_active_group_is_admitted_against_a_backlogged_incumbent() -> anyhow::Result<()> {
    const INCUMBENT_OCCUPANCY: usize = 100;

    let mut fixture = new_admission_fixture();
    seed_backlogged_groups(&mut fixture, 2)?;
    fixture.preload_queue(RG_A, INCUMBENT_OCCUPANCY)?;

    fixture.core.tick().await?;

    let occupancies = occupancies_of(&fixture, 2);
    let free = ADMISSION_DISPATCH_QUEUE_CAPACITY - occupancies.iter().sum::<usize>();
    assert!(
        occupancies[1] >= ADMISSION_DISPATCH_QUEUE_CAPACITY / 8,
        "the newly active group was starved by the incumbent: {occupancies:?}"
    );
    assert!(
        occupancies[0] - occupancies[1] < INCUMBENT_OCCUPANCY,
        "the incumbent's head start grew instead of shrinking: {occupancies:?}"
    );
    assert!(
        free >= ADMISSION_DISPATCH_QUEUE_CAPACITY / 8,
        "the tick left only {free} free for the next group to arrive: {occupancies:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_lone_group_takes_no_more_than_half_the_dispatch_buffer() -> anyhow::Result<()> {
    let mut fixture = new_admission_fixture();
    seed_backlogged_groups(&mut fixture, 1)?;

    fixture.core.tick().await?;

    let occupancy = fixture.queue_len(RG_A);
    assert!(
        occupancy <= ADMISSION_DISPATCH_QUEUE_CAPACITY / 2,
        "the only active group holds {occupancy} of {ADMISSION_DISPATCH_QUEUE_CAPACITY} \
         assignments"
    );
    assert!(
        occupancy >= ADMISSION_DISPATCH_QUEUE_CAPACITY / 4,
        "the only active group holds only {occupancy} of {ADMISSION_DISPATCH_QUEUE_CAPACITY} \
         assignments"
    );
    Ok(())
}

/// The dispatch buffer capacity `B` of the admission tests, matching the design document's worked
/// example.
const ADMISSION_DISPATCH_QUEUE_CAPACITY: usize = 256;

/// The number of backlogged resource groups `N` in the equilibrium tests.
const NUM_BACKLOGGED_GROUPS: usize = 5;

/// The number of ready tasks each backlogged group is seeded with, more than any single tick may
/// publish.
const NUM_TASKS_PER_JOB: usize = 512;

/// How far a group's occupancy may sit from the equilibrium share before the rotation is considered
/// broken. The staircase these tests exist to catch misses by an order of magnitude: batch filling
/// five groups against `B = 256` yields `64, 64, 64, 64, 0` with no free space at all.
const SHARE_TOLERANCE: usize = 6;

/// # Returns
///
/// A core config with the given capacities, whose remaining tunables never throttle a test.
///
/// # Panics
///
/// Panics if either capacity is zero.
fn make_config(
    dispatch_queue_capacity: usize,
    active_job_list_capacity: usize,
) -> RgRoundRobinConfig {
    make_expiring_config(
        dispatch_queue_capacity,
        active_job_list_capacity,
        NEVER_EXPIRING_TIMEOUT_SEC,
    )
}

/// # Returns
///
/// A core config as [`make_config`] builds it, sweeping its finalized job table after
/// `finalized_job_expiration_timeout_sec`.
///
/// # Panics
///
/// Panics if either capacity is zero.
fn make_expiring_config(
    dispatch_queue_capacity: usize,
    active_job_list_capacity: usize,
    finalized_job_expiration_timeout_sec: u64,
) -> RgRoundRobinConfig {
    RgRoundRobinConfig {
        dispatch_queue_capacity: NonZeroUsize::new(dispatch_queue_capacity)
            .expect("the dispatch queue capacity is non-zero"),
        active_job_list_capacity: NonZeroUsize::new(active_job_list_capacity)
            .expect("the active job list capacity is non-zero"),
        ready_task_capacity: NonZeroUsize::new(READY_TASK_CAPACITY)
            .expect("the regular-task buffer capacity is non-zero"),
        commit_ready_task_capacity: NonZeroUsize::new(COMMIT_READY_TASK_CAPACITY)
            .expect("the commit-task buffer capacity is non-zero"),
        cleanup_ready_task_capacity: NonZeroUsize::new(CLEANUP_READY_TASK_CAPACITY)
            .expect("the cleanup-task buffer capacity is non-zero"),
        storage_poll_timeout_ms: STORAGE_POLL_TIMEOUT_MS,
        tick_interval_ms: NonZeroU64::new(1).expect("1 is non-zero"),
        finalized_job_expiration_timeout_sec,
    }
}

/// # Returns
///
/// A newly created fixture over a mock storage with no scripted batch, so a tick's decisions come
/// from the tasks the test seeded and from nothing else.
fn new_admission_fixture() -> CoreFixture {
    CoreFixture::new(
        make_config(ADMISSION_DISPATCH_QUEUE_CAPACITY, ACTIVE_JOB_LIST_CAPACITY),
        MockStorageClient::new(),
    )
}

/// Seeds the first `num_groups` resource groups with one job each, backlogged with more ready tasks
/// than a single tick may publish.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`CoreFixture::seed_job`]'s return values on failure.
/// * Forwards [`u64::try_from`]'s return values on failure.
fn seed_backlogged_groups(fixture: &mut CoreFixture, num_groups: usize) -> anyhow::Result<()> {
    for idx in 0..num_groups {
        let raw_id = u64::try_from(idx)?;
        fixture.seed_job(
            ResourceGroupId::from(raw_id),
            JobId::from(raw_id),
            NUM_TASKS_PER_JOB,
        )?;
    }
    Ok(())
}

/// Registers a job of `num_tasks` buffered ready tasks, whose task indices are `0..num_tasks`.
///
/// # Returns
///
/// The key of the registered job, which still needs a scheduling position.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if the job is already registered.
fn make_job_entry(
    registry: &mut JobRegistry,
    job_id: JobId,
    num_tasks: usize,
) -> anyhow::Result<JobKey> {
    let task_indices: Vec<TaskIndex> = (0..num_tasks).collect();
    let UpsertOutcome::New(job_key) = registry.upsert(job_id, task_indices) else {
        bail!("job {job_id} is already registered");
    };
    Ok(job_key)
}

/// # Returns
///
/// An assignment of `task_id` carrying an ID no assignment the core publishes can collide with.
fn make_assignment(
    rg_id: ResourceGroupId,
    job_id: JobId,
    task_id: TaskId,
    session_id: SessionId,
) -> TaskAssignment {
    TaskAssignment {
        id: TaskAssignmentId::from(u64::MAX),
        resource_group_id: rg_id,
        job_id,
        task_id,
        session_id,
    }
}

/// # Returns
///
/// The number of assignments queued for each of the first `num_groups` resource groups, indexed by
/// resource group ID.
fn occupancies_of(fixture: &CoreFixture, num_groups: usize) -> Vec<usize> {
    (0..num_groups)
        .map(|idx| {
            let raw_id = u64::try_from(idx).expect("a group index fits a raw ID");
            fixture.queue_len(ResourceGroupId::from(raw_id))
        })
        .collect()
}

/// # Returns
///
/// A tuple containing the core's per-lane counts of buffered tasks:
///
/// * The number of buffered regular tasks.
/// * The number of buffered commit tasks.
/// * The number of buffered cleanup tasks.
fn lane_counts(fixture: &CoreFixture) -> (usize, usize, usize) {
    let global_task_set = &fixture.core.global_task_set;
    (
        global_task_set.num_ready(),
        global_task_set.num_commit_ready(),
        global_task_set.num_cleanup_ready(),
    )
}

/// # Returns
///
/// The jobs the core's finalized job table holds, in the order they were finalized.
fn finalized_job_ids(fixture: &CoreFixture) -> Vec<JobId> {
    fixture
        .core
        .finalized_job_queue
        .iter()
        .map(|(job_id, _)| *job_id)
        .collect()
}

/// # Returns
///
/// Whether the core still holds `rg_id` on its active resource group list.
///
/// # Panics
///
/// Panics if the core has no scheduling state for `rg_id`.
fn is_active(fixture: &CoreFixture, rg_id: ResourceGroupId) -> bool {
    let state_idx = *fixture
        .core
        .rg_id_to_idx_map
        .get(&rg_id)
        .expect("the core holds a scheduling state for the group");
    fixture.core.rg_states[state_idx].is_active
}

/// Takes every assignment currently queued behind `reader`, playing a pinned execution manager,
/// which leaves the group's hint counter untouched.
///
/// # Returns
///
/// The assignments taken, in dispatch order.
async fn drain_reader(reader: &RgDispatchQueueReader) -> Vec<TaskAssignment> {
    let mut assignments = Vec::new();
    while let Some(assignment) = reader.recv_pinned(Duration::ZERO).await {
        assignments.push(assignment);
    }
    assignments
}
