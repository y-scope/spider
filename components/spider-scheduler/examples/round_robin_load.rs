//! Load-test and instrumentation harness for the round-robin scheduler core.
//!
//! Topology:
//!
//! ```text
//!   submitter ──▶ MockStorage (ready lane) ──poll──▶ RoundRobinCore ──enqueue──▶ dispatch queue ──▶ 64 workers
//! ```
//!
//! * A mock storage holds 128 jobs of 1000 tasks each, released gradually (one job at a time) to
//!   simulate a job-submission cycle rather than making everything ready at `t=0`.
//! * 1% of the tasks are submitted twice (back-to-back) so the scheduler's deduplication can be
//!   exercised; workers must still observe every task exactly once.
//! * 64 workers drain the dispatch queue, sleeping 5ms per task to model execution latency.
//!
//! Run with (release recommended so the timings are meaningful):
//!
//! ```bash
//! cargo run -p spider-scheduler --example round_robin_load --release
//! ```

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use dashmap::DashSet;
use spider_core::{
    job::JobState,
    types::id::{JobId, ResourceGroupId, SessionId, TaskId},
};
use spider_scheduler::{
    DispatchQueueSource,
    SchedulerCore,
    SchedulerStorageClient,
    StorageClientError,
    core_impl::RoundRobinConfig,
    dispatch_queue::{DispatchQueueReader, DispatchQueueWriter, create_dispatch_queue},
    types::InboundEntry,
};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------------------------
// Workload parameters
// ---------------------------------------------------------------------------------------------

const NUM_JOBS: usize = 128;
const TASKS_PER_JOB: usize = 1000;
const TOTAL_UNIQUE_TASKS: usize = NUM_JOBS * TASKS_PER_JOB;

/// Every `DUP_EVERY`-th task within a job is submitted twice, yielding exactly 1% duplicates.
const DUP_EVERY: usize = 100;
const EXPECTED_DUPLICATES_SUBMITTED: usize = NUM_JOBS * TASKS_PER_JOB / DUP_EVERY;

const NUM_WORKERS: usize = 64;
const WORKER_SLEEP: Duration = Duration::from_millis(5);
const WORKER_POLL_WAIT: Duration = Duration::from_millis(10);

/// Delay between releasing successive jobs into storage (the "submission cycle").
const JOB_SUBMIT_INTERVAL: Duration = Duration::from_millis(10);

/// A fixed session: this harness never bumps the session, so storage and the dispatch queue both
/// start (and stay) here.
const SESSION_ID: SessionId = 0;

// Round-robin scheduler configuration (as requested).
const ACTIVE_JOB_POOL_CAPACITY: usize = 8;
const DISPATCH_QUEUE_CAPACITY: usize = NUM_WORKERS * 4; // 256
const STORAGE_POLLING_WAIT_TIME_MS: u64 = 10; // dispatch/poll interval
const READY_TASK_CAPACITY: usize = TASKS_PER_JOB * NUM_WORKERS; // 64_000
const COMMIT_READY_TASK_CAPACITY: usize = 10;
const CLEANUP_READY_TASK_CAPACITY: usize = 10;

/// Safety net so a scheduling bug that drops a task cannot hang the harness forever.
const OVERALL_TIMEOUT: Duration = Duration::from_mins(2);

// ---------------------------------------------------------------------------------------------
// Mock storage
// ---------------------------------------------------------------------------------------------

/// A mock [`SchedulerStorageClient`] whose regular lane is backed by an unbounded channel that the
/// submitter feeds. Commit and cleanup lanes are always empty.
#[derive(Clone)]
struct MockStorage {
    inner: Arc<MockStorageInner>,
}

struct MockStorageInner {
    ready_tx: async_channel::Sender<InboundEntry>,
    ready_rx: async_channel::Receiver<InboundEntry>,
}

impl MockStorage {
    fn new() -> Self {
        let (ready_tx, ready_rx) = async_channel::unbounded();
        Self {
            inner: Arc::new(MockStorageInner { ready_tx, ready_rx }),
        }
    }

    /// # Returns
    ///
    /// A cloned sender for the regular ready lane, used by the submitter task.
    fn sender(&self) -> async_channel::Sender<InboundEntry> {
        self.inner.ready_tx.clone()
    }
}

#[async_trait]
impl SchedulerStorageClient for MockStorage {
    async fn poll_ready(
        &self,
        max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
        if max_items == 0 {
            // The scheduler has no buffer headroom; emulate a real blocking poll that yields
            // nothing rather than pulling past the requested cap.
            tokio::time::sleep(wait).await;
            return Ok((SESSION_ID, Vec::new()));
        }

        let mut out = Vec::new();
        // Block up to `wait` for the first entry, mirroring a real long-poll.
        match tokio::time::timeout(wait, self.inner.ready_rx.recv()).await {
            Ok(Ok(entry)) => out.push(entry),
            // Channel closed (never happens here, sender is held by storage) or timed out: return
            // whatever we have (possibly nothing).
            Ok(Err(_)) | Err(_) => return Ok((SESSION_ID, out)),
        }
        // Drain the rest without blocking, up to `max_items`.
        while out.len() < max_items {
            match self.inner.ready_rx.try_recv() {
                Ok(entry) => out.push(entry),
                Err(_) => break,
            }
        }
        Ok((SESSION_ID, out))
    }

    async fn poll_commit_ready(
        &self,
        _max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
        tokio::time::sleep(wait).await;
        Ok((SESSION_ID, Vec::new()))
    }

    async fn poll_cleanup_ready(
        &self,
        _max_items: usize,
        wait: Duration,
    ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
        tokio::time::sleep(wait).await;
        Ok((SESSION_ID, Vec::new()))
    }

    async fn job_state(&self, _job_id: JobId) -> Result<JobState, StorageClientError> {
        Ok(JobState::Running)
    }
}

// ---------------------------------------------------------------------------------------------
// Submitter & workers
// ---------------------------------------------------------------------------------------------

/// Releases each job's tasks into storage one job at a time, duplicating every `DUP_EVERY`-th task
/// back-to-back so the duplicate lands in the same poll batch as its original.
async fn submit_jobs(jobs: Vec<(JobId, ResourceGroupId)>, tx: async_channel::Sender<InboundEntry>) {
    for (job_id, resource_group_id) in jobs {
        for i in 0..TASKS_PER_JOB {
            let entry = InboundEntry {
                resource_group_id,
                job_id,
                task_id: TaskId::Index(i),
            };
            tx.send(entry).await.expect("ready lane closed");
            if i % DUP_EVERY == 0 {
                tx.send(entry).await.expect("ready lane closed");
            }
        }
        tokio::time::sleep(JOB_SUBMIT_INTERVAL).await;
    }
}

/// Shared bookkeeping for the "each task is polled exactly once" check.
struct WorkerStats {
    seen: DashSet<(JobId, TaskId)>,
    total_received: AtomicUsize,
    duplicate_received: AtomicUsize,
}

/// A single worker: drain the dispatch queue, record each assignment, then sleep to model work.
async fn worker(reader: DispatchQueueReader, stats: Arc<WorkerStats>, done: Arc<AtomicBool>) {
    loop {
        if done.load(Ordering::Relaxed) {
            break;
        }
        match reader.dequeue(WORKER_POLL_WAIT).await {
            Ok(Some((_session, assignment))) => {
                stats.total_received.fetch_add(1, Ordering::Relaxed);
                if !stats.seen.insert((assignment.job_id, assignment.task_id)) {
                    stats.duplicate_received.fetch_add(1, Ordering::Relaxed);
                }
                tokio::time::sleep(WORKER_SLEEP).await;
            }
            Ok(None) => {}
            // Dispatch queue closed (scheduler dropped its writer): nothing more will arrive.
            Err(_) => break,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let storage = MockStorage::new();
    let (writer, reader) = create_dispatch_queue(DISPATCH_QUEUE_CAPACITY, SESSION_ID);

    let config = RoundRobinConfig::<MockStorage, DispatchQueueWriter>::new(
        ACTIVE_JOB_POOL_CAPACITY,
        DISPATCH_QUEUE_CAPACITY,
        READY_TASK_CAPACITY,
        COMMIT_READY_TASK_CAPACITY,
        CLEANUP_READY_TASK_CAPACITY,
        STORAGE_POLLING_WAIT_TIME_MS,
    );
    let metrics = config.metrics();

    let jobs: Vec<(JobId, ResourceGroupId)> = (0..NUM_JOBS)
        .map(|_| (JobId::new(), ResourceGroupId::new()))
        .collect();

    // Scheduler.
    let scheduler_token = CancellationToken::new();
    let scheduler_handle = {
        let token = scheduler_token.clone();
        let storage = storage.clone();
        tokio::spawn(async move { config.run(storage, writer, token).await })
    };

    // Workers.
    let stats = Arc::new(WorkerStats {
        seen: DashSet::with_capacity(TOTAL_UNIQUE_TASKS),
        total_received: AtomicUsize::new(0),
        duplicate_received: AtomicUsize::new(0),
    });
    let done = Arc::new(AtomicBool::new(false));
    let worker_handles: Vec<_> = (0..NUM_WORKERS)
        .map(|_| tokio::spawn(worker(reader.clone(), stats.clone(), done.clone())))
        .collect();
    drop(reader);

    // Submitter.
    let submit_handle = tokio::spawn(submit_jobs(jobs, storage.sender()));

    // Drive to completion: every unique task delivered, or the safety timeout. Poll tightly so the
    // metrics are frozen as soon as the last task arrives, keeping the idle tail out of the averages.
    let start = Instant::now();
    let mut timed_out = false;
    loop {
        if stats.seen.len() >= TOTAL_UNIQUE_TASKS {
            break;
        }
        if start.elapsed() > OVERALL_TIMEOUT {
            timed_out = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // Stop timing now that every task has arrived; the scheduler only spins on empty polls past
    // this point and must not pollute the stage 1 & 2 averages.
    metrics.stop();
    let wall = start.elapsed();

    // Tear down.
    done.store(true, Ordering::Relaxed);
    scheduler_token.cancel();
    submit_handle.abort();
    for handle in worker_handles {
        let _ = handle.await;
    }
    match scheduler_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("scheduler returned an error: {e:?}"),
        Err(e) => eprintln!("scheduler task panicked: {e:?}"),
    }
    let _ = submit_handle.await;

    report(&metrics, &stats, wall, timed_out);
}

/// Prints the collected timing and correctness results.
fn report(
    metrics: &spider_scheduler::core_impl::RoundRobinMetrics,
    stats: &WorkerStats,
    wall: Duration,
    timed_out: bool,
) {
    let loop_count = metrics.loop_count.load(Ordering::Relaxed);
    let total_loop_ns = metrics.total_loop_ns.load(Ordering::Relaxed);
    let buffer_count = metrics.buffer_enrich_count.load(Ordering::Relaxed);
    let buffer_ns = metrics.buffer_enrich_ns.load(Ordering::Relaxed);
    let dispatch_count = metrics.dispatch_enrich_count.load(Ordering::Relaxed);
    let dispatch_ns = metrics.dispatch_enrich_ns.load(Ordering::Relaxed);

    let total = stats.total_received.load(Ordering::Relaxed);
    let duplicates = stats.duplicate_received.load(Ordering::Relaxed);
    let unique = stats.seen.len();

    println!("\n================ Round-robin scheduler load test ================");
    println!(
        "Wall-clock runtime:                 {:.3} s",
        wall.as_secs_f64()
    );
    if timed_out {
        println!("!! TIMED OUT before all unique tasks were delivered !!");
    }

    println!("\n---- Workload ----");
    println!("Jobs:                               {NUM_JOBS}");
    println!("Tasks per job:                      {TASKS_PER_JOB}");
    println!("Unique tasks (expected):            {TOTAL_UNIQUE_TASKS}");
    println!("Duplicate task entries submitted:   {EXPECTED_DUPLICATES_SUBMITTED}");
    println!("Workers:                            {NUM_WORKERS}");

    println!("\n---- Scheduling-loop timing ----");
    println!("Scheduling-loop iterations:         {loop_count}");
    println!(
        "Avg time per scheduling loop:       {:>9.3} us",
        avg_us(total_loop_ns, loop_count)
    );
    println!(
        "Avg buffer-enrich time (stage 1):   {:>9.3} us   (over {buffer_count} iterations that \
         polled a non-empty result)",
        avg_us(buffer_ns, buffer_count)
    );
    println!(
        "Avg dispatch-enrich time (stage 2): {:>9.3} us   (over {dispatch_count} iterations that \
         dispatched >=1 task)",
        avg_us(dispatch_ns, dispatch_count)
    );
    let idle_loops = loop_count.saturating_sub(dispatch_count);
    println!(
        "No-dispatch loop iterations:        {idle_loops}   ({:.1}% of all iterations)",
        percent(idle_loops, loop_count)
    );

    println!("\n---- Correctness: each task polled exactly once ----");
    println!("Total assignments received:         {total}");
    println!("Unique (job, task) pairs received:  {unique}");
    println!("Duplicate deliveries observed:      {duplicates}");

    let exactly_once = !timed_out
        && duplicates == 0
        && unique == TOTAL_UNIQUE_TASKS
        && total == TOTAL_UNIQUE_TASKS;
    println!(
        "\nRESULT: each task polled exactly once -> {}",
        if exactly_once { "PASS" } else { "FAIL" }
    );
    println!("=================================================================\n");
}

/// # Returns
///
/// `ns / count` converted to microseconds, or `0.0` when `count` is zero.
fn avg_us(ns: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        ns as f64 / count as f64 / 1_000.0
    }
}

/// # Returns
///
/// `part` as a percentage of `whole`, or `0.0` when `whole` is zero.
fn percent(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 / whole as f64 * 100.0
    }
}
