//! The inbound-poll result formatter of the resource-group-aware round-robin scheduler.
//!
//! The core schedules a resource group's jobs one batch at a time, so the entries drained from the
//! inbound queue are grouped by job before they reach it. The grouping is driven through
//! [`crate::core_impl::inbound_queue_reader::AsyncInboundQueueReader`], which applies it inside
//! each lane's background polling task.

use std::collections::HashMap;

use spider_core::task::TaskIndex;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;

use crate::core_impl::inbound_queue_reader::InboundPollResultFormatter;
use crate::types::InboundEntry;

/// The regular tasks a single poll drained for one job.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ReadyJobBatch {
    /// The resource group that owns the job.
    pub(super) resource_group_id: ResourceGroupId,

    /// The job the tasks belong to.
    pub(super) job_id: JobId,

    /// The ready tasks, sorted and deduplicated.
    pub(super) task_indices: Vec<TaskIndex>,
}

/// A job that has reached one of its terminal states.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct FinalizedJob {
    /// The resource group that owns the job.
    pub(super) resource_group_id: ResourceGroupId,

    /// The finalized job.
    pub(super) job_id: JobId,
}

/// The formatter of the resource-group-aware core, which consumes the drained entries grouped by
/// job.
pub(super) struct GroupedJobs;

impl InboundPollResultFormatter for GroupedJobs {
    type FinalizedResult = Vec<FinalizedJob>;
    type ReadyResult = Vec<ReadyJobBatch>;

    fn format_ready(entries: Vec<InboundEntry>) -> Self::ReadyResult {
        format_ready_job_batches(entries)
    }

    fn format_finalized(entries: Vec<InboundEntry>) -> Self::FinalizedResult {
        format_finalized_jobs(entries)
    }
}

/// Groups the entries drained from the regular-task lane by job, discarding every entry that does
/// not carry a task index.
///
/// # Returns
///
/// One batch per job, in an unspecified order.
fn format_ready_job_batches(entries: Vec<InboundEntry>) -> Vec<ReadyJobBatch> {
    let mut batches: HashMap<JobId, (ResourceGroupId, Vec<TaskIndex>)> = HashMap::new();
    for entry in entries {
        let TaskId::Index(task_index) = entry.task_id else {
            continue;
        };
        batches
            .entry(entry.job_id)
            .or_insert_with(|| (entry.resource_group_id, Vec::new()))
            .1
            .push(task_index);
    }

    batches
        .into_iter()
        .map(|(job_id, (resource_group_id, mut task_indices))| {
            task_indices.sort_unstable();
            task_indices.dedup();
            ReadyJobBatch {
                resource_group_id,
                job_id,
                task_indices,
            }
        })
        .collect()
}

/// # Returns
///
/// One finalized job per entry drained from a finalization lane, in the order they were drained.
fn format_finalized_jobs(entries: Vec<InboundEntry>) -> Vec<FinalizedJob> {
    entries
        .into_iter()
        .map(|entry| FinalizedJob {
            resource_group_id: entry.resource_group_id,
            job_id: entry.job_id,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use anyhow::anyhow;
    use anyhow::bail;
    use async_trait::async_trait;
    use spider_core::job::JobState;
    use spider_core::types::id::SchedulerId;
    use spider_core::types::id::SessionId;
    use tokio::sync::Semaphore;

    use super::*;
    use crate::core_impl::inbound_queue_reader::AsyncInboundQueueReader;
    use crate::core_impl::inbound_queue_reader::InboundPollState;
    use crate::error::SchedulerError;
    use crate::error::StorageClientError;
    use crate::storage_client::SchedulerStorageClient;

    /// The reader under test, which formats every lane's entries with [`GroupedJobs`].
    type TestReader = AsyncInboundQueueReader<MockStorageClient, GroupedJobs>;

    /// The session the mock storage client serves polls under unless a test scripts otherwise.
    const DEFAULT_SESSION_ID: SessionId = 0;

    /// The resource group most entries in this module belong to.
    const RG_ID: ResourceGroupId = ResourceGroupId::from(4);

    /// The resource group used where a test needs a second group.
    const OTHER_RG_ID: ResourceGroupId = ResourceGroupId::from(5);

    /// The first job a test polls entries for.
    const JOB_A: JobId = JobId::from(10);

    /// The second job a test polls entries for.
    const JOB_B: JobId = JobId::from(11);

    /// The third job a test polls entries for.
    const JOB_C: JobId = JobId::from(12);

    /// The entry limit given to lanes a test wants polled.
    const MAX_ENTRIES: usize = 32;

    /// The poll timeout passed to the mock storage client, which never blocks on it.
    const POLL_TIMEOUT: Duration = Duration::from_millis(1);

    /// The maximum time to wait for an in-flight poll to complete before failing a test.
    const COLLECT_DEADLINE: Duration = Duration::from_secs(5);

    /// The interval between two collection attempts while waiting for an in-flight poll.
    const COLLECT_RETRY_INTERVAL: Duration = Duration::from_millis(1);

    struct MockStorageInner {
        session_id: AtomicU64,
        ready_batches: Mutex<VecDeque<(SessionId, Vec<InboundEntry>)>>,
        commit_ready_batches: Mutex<VecDeque<(SessionId, Vec<InboundEntry>)>>,
        cleanup_ready_batches: Mutex<VecDeque<(SessionId, Vec<InboundEntry>)>>,
        num_ready_polls: AtomicU64,
        num_commit_ready_polls: AtomicU64,
        num_cleanup_ready_polls: AtomicU64,
        is_ready_lane_gated: AtomicBool,
        ready_lane_gate: Semaphore,
    }

    /// A mock [`SchedulerStorageClient`] backed by scripted poll batches.
    ///
    /// Each lane serves its scripted batches in FIFO order, one batch per poll; when a lane's
    /// script is empty, polls return an empty batch under the mock's current session immediately
    /// (the `wait` parameter is ignored to keep tests fast). The regular-task lane can be gated so
    /// that a test can observe an in-flight poll.
    #[derive(Clone)]
    struct MockStorageClient {
        inner: Arc<MockStorageInner>,
    }

    impl MockStorageClient {
        /// Factory function.
        ///
        /// # Returns
        ///
        /// A new mock storage client with no scripted batches and no gated lane, reporting
        /// [`DEFAULT_SESSION_ID`] on empty polls.
        fn new() -> Self {
            Self {
                inner: Arc::new(MockStorageInner {
                    session_id: AtomicU64::new(DEFAULT_SESSION_ID),
                    ready_batches: Mutex::new(VecDeque::new()),
                    commit_ready_batches: Mutex::new(VecDeque::new()),
                    cleanup_ready_batches: Mutex::new(VecDeque::new()),
                    num_ready_polls: AtomicU64::new(0),
                    num_commit_ready_polls: AtomicU64::new(0),
                    num_cleanup_ready_polls: AtomicU64::new(0),
                    is_ready_lane_gated: AtomicBool::new(false),
                    ready_lane_gate: Semaphore::new(0),
                }),
            }
        }

        /// Scripts a batch to be served by the next unserved
        /// [`SchedulerStorageClient::poll_ready`] call.
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
        /// A tuple containing the number of polls served by the regular-task, commit-task, and
        /// cleanup-task lanes respectively.
        fn num_polls(&self) -> (u64, u64, u64) {
            (
                self.inner.num_ready_polls.load(Ordering::Relaxed),
                self.inner.num_commit_ready_polls.load(Ordering::Relaxed),
                self.inner.num_cleanup_ready_polls.load(Ordering::Relaxed),
            )
        }

        /// Holds every subsequent regular-task poll until [`Self::admit_ready_poll`] releases it.
        fn gate_ready_lane(&self) {
            self.inner
                .is_ready_lane_gated
                .store(true, Ordering::Relaxed);
        }

        /// Releases one held regular-task poll.
        fn admit_ready_poll(&self) {
            self.inner.ready_lane_gate.add_permits(1);
        }

        /// Serves one poll from the given lane's script.
        ///
        /// # Returns
        ///
        /// The lane's next scripted batch, or an empty batch under the current session if the
        /// lane's script is exhausted.
        ///
        /// # Panics
        ///
        /// Panics if the scripted batch holds more entries than `max_items`.
        fn serve_batch(
            &self,
            batches: &Mutex<VecDeque<(SessionId, Vec<InboundEntry>)>>,
            max_items: usize,
        ) -> (SessionId, Vec<InboundEntry>) {
            let scripted_batch = batches.lock().expect("batch lock poisoned").pop_front();
            let Some((session_id, entries)) = scripted_batch else {
                return (self.inner.session_id.load(Ordering::Relaxed), Vec::new());
            };
            assert!(
                entries.len() <= max_items,
                "scripted batch of {} entries exceeds the poll limit of {max_items}",
                entries.len(),
            );
            (session_id, entries)
        }
    }

    #[async_trait]
    impl SchedulerStorageClient for MockStorageClient {
        async fn register(
            &self,
            _host: spider_utils::config::Host,
            _port: u16,
        ) -> Result<SchedulerId, StorageClientError> {
            Ok(SchedulerId::from(0))
        }

        async fn poll_ready(
            &self,
            max_items: usize,
            _wait: Duration,
        ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
            if self.inner.is_ready_lane_gated.load(Ordering::Relaxed) {
                self.inner
                    .ready_lane_gate
                    .acquire()
                    .await
                    .expect("the regular-task lane gate is never closed")
                    .forget();
            }
            self.inner.num_ready_polls.fetch_add(1, Ordering::Relaxed);
            Ok(self.serve_batch(&self.inner.ready_batches, max_items))
        }

        async fn poll_commit_ready(
            &self,
            max_items: usize,
            _wait: Duration,
        ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
            self.inner
                .num_commit_ready_polls
                .fetch_add(1, Ordering::Relaxed);
            Ok(self.serve_batch(&self.inner.commit_ready_batches, max_items))
        }

        async fn poll_cleanup_ready(
            &self,
            max_items: usize,
            _wait: Duration,
        ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
            self.inner
                .num_cleanup_ready_polls
                .fetch_add(1, Ordering::Relaxed);
            Ok(self.serve_batch(&self.inner.cleanup_ready_batches, max_items))
        }

        async fn job_state(&self, _job_id: JobId) -> Result<JobState, StorageClientError> {
            Ok(JobState::Running)
        }
    }

    /// The formatted results of one completed poll.
    struct PollResult {
        session_id: SessionId,
        ready_jobs: Vec<ReadyJobBatch>,
        commit_ready_jobs: Vec<FinalizedJob>,
        cleanup_ready_jobs: Vec<FinalizedJob>,
    }

    impl PollResult {
        /// # Returns
        ///
        /// The batch [`ReadyJobBatch::job_id`] of which is `job_id`.
        ///
        /// # Errors
        ///
        /// Returns an error if:
        ///
        /// * [`anyhow::Error`] if no batch was polled for `job_id`.
        fn ready_batch(&self, job_id: JobId) -> anyhow::Result<&ReadyJobBatch> {
            self.ready_jobs
                .iter()
                .find(|batch| batch.job_id == job_id)
                .ok_or_else(|| anyhow!("no ready batch was polled for job {job_id}"))
        }
    }

    /// # Returns
    ///
    /// An inbound entry for `job_id`'s `task_id`, owned by `resource_group_id`.
    const fn make_entry(
        resource_group_id: ResourceGroupId,
        job_id: JobId,
        task_id: TaskId,
    ) -> InboundEntry {
        InboundEntry {
            resource_group_id,
            job_id,
            task_id,
        }
    }

    /// Collects the result of the in-flight poll, retrying until every lane task has finished.
    ///
    /// # Returns
    ///
    /// The formatted results of the completed poll on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if the poll does not complete within [`COLLECT_DEADLINE`], or if no poll
    ///   is in flight.
    /// * Forwards [`AsyncInboundQueueReader::try_collect_result`]'s return values on failure.
    async fn collect_when_ready(
        reader: &mut TestReader,
        curr_session_id: SessionId,
    ) -> anyhow::Result<PollResult> {
        let deadline = tokio::time::Instant::now() + COLLECT_DEADLINE;
        loop {
            match reader.try_collect_result(curr_session_id).await? {
                InboundPollState::Ready {
                    session_id,
                    ready_result: ready_jobs,
                    commit_ready_result: commit_ready_jobs,
                    cleanup_ready_result: cleanup_ready_jobs,
                } => {
                    return Ok(PollResult {
                        session_id,
                        ready_jobs,
                        commit_ready_jobs,
                        cleanup_ready_jobs,
                    });
                }
                InboundPollState::NotStarted => bail!("no inbound poll is in flight"),
                InboundPollState::Pending => {
                    if deadline <= tokio::time::Instant::now() {
                        bail!("the inbound poll did not complete in time");
                    }
                    tokio::time::sleep(COLLECT_RETRY_INTERVAL).await;
                }
            }
        }
    }

    #[tokio::test]
    async fn a_reader_without_a_poll_reports_not_started() -> anyhow::Result<()> {
        let mut reader = TestReader::new(MockStorageClient::new());
        assert!(matches!(
            reader.try_collect_result(DEFAULT_SESSION_ID).await?,
            InboundPollState::NotStarted
        ));
        Ok(())
    }

    #[tokio::test]
    async fn an_unfinished_lane_keeps_the_poll_pending() -> anyhow::Result<()> {
        let storage_client = MockStorageClient::new();
        storage_client.gate_ready_lane();
        storage_client.push_ready_batch(
            DEFAULT_SESSION_ID,
            vec![make_entry(RG_ID, JOB_A, TaskId::Index(0))],
        );
        let mut reader = TestReader::new(storage_client.clone());
        reader.start(POLL_TIMEOUT, MAX_ENTRIES, MAX_ENTRIES, MAX_ENTRIES)?;

        assert!(matches!(
            reader.try_collect_result(DEFAULT_SESSION_ID).await?,
            InboundPollState::Pending
        ));
        assert!(matches!(
            reader.try_collect_result(DEFAULT_SESSION_ID).await?,
            InboundPollState::Pending
        ));

        storage_client.admit_ready_poll();
        let result = collect_when_ready(&mut reader, DEFAULT_SESSION_ID).await?;
        assert_eq!(result.ready_batch(JOB_A)?.task_indices, vec![0]);

        assert!(matches!(
            reader.try_collect_result(DEFAULT_SESSION_ID).await?,
            InboundPollState::NotStarted
        ));
        Ok(())
    }

    #[tokio::test]
    async fn ready_entries_are_grouped_by_job() -> anyhow::Result<()> {
        let storage_client = MockStorageClient::new();
        storage_client.push_ready_batch(
            DEFAULT_SESSION_ID,
            vec![
                make_entry(RG_ID, JOB_A, TaskId::Index(3)),
                make_entry(OTHER_RG_ID, JOB_B, TaskId::Index(7)),
                make_entry(RG_ID, JOB_A, TaskId::Index(1)),
                make_entry(RG_ID, JOB_A, TaskId::Index(3)),
                make_entry(RG_ID, JOB_A, TaskId::Index(2)),
            ],
        );
        let mut reader = TestReader::new(storage_client);
        reader.start(POLL_TIMEOUT, MAX_ENTRIES, MAX_ENTRIES, MAX_ENTRIES)?;

        let result = collect_when_ready(&mut reader, DEFAULT_SESSION_ID).await?;
        assert_eq!(result.ready_jobs.len(), 2);
        assert_eq!(
            result.ready_batch(JOB_A)?,
            &ReadyJobBatch {
                resource_group_id: RG_ID,
                job_id: JOB_A,
                task_indices: vec![1, 2, 3],
            }
        );
        assert_eq!(
            result.ready_batch(JOB_B)?,
            &ReadyJobBatch {
                resource_group_id: OTHER_RG_ID,
                job_id: JOB_B,
                task_indices: vec![7],
            }
        );
        assert_eq!(result.commit_ready_jobs.as_slice(), &[]);
        assert_eq!(result.cleanup_ready_jobs.as_slice(), &[]);
        Ok(())
    }

    #[tokio::test]
    async fn a_ready_entry_without_a_task_index_is_skipped() -> anyhow::Result<()> {
        let storage_client = MockStorageClient::new();
        storage_client.push_ready_batch(
            DEFAULT_SESSION_ID,
            vec![
                make_entry(RG_ID, JOB_A, TaskId::Commit),
                make_entry(RG_ID, JOB_B, TaskId::Cleanup),
                make_entry(RG_ID, JOB_C, TaskId::Index(0)),
            ],
        );
        let mut reader = TestReader::new(storage_client);
        reader.start(POLL_TIMEOUT, MAX_ENTRIES, MAX_ENTRIES, MAX_ENTRIES)?;

        let result = collect_when_ready(&mut reader, DEFAULT_SESSION_ID).await?;
        assert_eq!(
            result.ready_jobs.as_slice(),
            &[ReadyJobBatch {
                resource_group_id: RG_ID,
                job_id: JOB_C,
                task_indices: vec![0],
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn finalization_entries_are_mapped_to_finalized_jobs() -> anyhow::Result<()> {
        let storage_client = MockStorageClient::new();
        storage_client.push_commit_ready_batch(
            DEFAULT_SESSION_ID,
            vec![
                make_entry(RG_ID, JOB_A, TaskId::Commit),
                make_entry(OTHER_RG_ID, JOB_B, TaskId::Commit),
            ],
        );
        storage_client.push_cleanup_ready_batch(
            DEFAULT_SESSION_ID,
            vec![make_entry(RG_ID, JOB_C, TaskId::Cleanup)],
        );
        let mut reader = TestReader::new(storage_client);
        reader.start(POLL_TIMEOUT, MAX_ENTRIES, MAX_ENTRIES, MAX_ENTRIES)?;

        let result = collect_when_ready(&mut reader, DEFAULT_SESSION_ID).await?;
        assert_eq!(
            result.commit_ready_jobs.as_slice(),
            &[
                FinalizedJob {
                    resource_group_id: RG_ID,
                    job_id: JOB_A,
                },
                FinalizedJob {
                    resource_group_id: OTHER_RG_ID,
                    job_id: JOB_B,
                }
            ]
        );
        assert_eq!(
            result.cleanup_ready_jobs.as_slice(),
            &[FinalizedJob {
                resource_group_id: RG_ID,
                job_id: JOB_C,
            }]
        );
        assert_eq!(result.ready_jobs.as_slice(), &[]);
        Ok(())
    }

    #[tokio::test]
    async fn a_stale_lane_result_is_dropped() -> anyhow::Result<()> {
        const STALE_SESSION_ID: SessionId = 1;
        const LATEST_SESSION_ID: SessionId = 2;

        let storage_client = MockStorageClient::new();
        storage_client.push_ready_batch(
            STALE_SESSION_ID,
            vec![make_entry(RG_ID, JOB_A, TaskId::Index(0))],
        );
        storage_client.push_commit_ready_batch(
            LATEST_SESSION_ID,
            vec![make_entry(RG_ID, JOB_B, TaskId::Commit)],
        );
        storage_client.push_cleanup_ready_batch(
            STALE_SESSION_ID,
            vec![make_entry(RG_ID, JOB_C, TaskId::Cleanup)],
        );
        let mut reader = TestReader::new(storage_client);
        reader.start(POLL_TIMEOUT, MAX_ENTRIES, MAX_ENTRIES, MAX_ENTRIES)?;

        let result = collect_when_ready(&mut reader, STALE_SESSION_ID).await?;
        assert_eq!(result.session_id, LATEST_SESSION_ID);
        assert_eq!(result.ready_jobs.as_slice(), &[]);
        assert_eq!(result.cleanup_ready_jobs.as_slice(), &[]);
        assert_eq!(
            result.commit_ready_jobs.as_slice(),
            &[FinalizedJob {
                resource_group_id: RG_ID,
                job_id: JOB_B,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn starting_a_poll_while_one_is_in_flight_fails() -> anyhow::Result<()> {
        let storage_client = MockStorageClient::new();
        storage_client.gate_ready_lane();
        let mut reader = TestReader::new(storage_client.clone());
        reader.start(POLL_TIMEOUT, MAX_ENTRIES, MAX_ENTRIES, MAX_ENTRIES)?;

        let Err(SchedulerError::Internal(message)) =
            reader.start(POLL_TIMEOUT, MAX_ENTRIES, MAX_ENTRIES, MAX_ENTRIES)
        else {
            bail!("starting a second poll should be rejected");
        };
        assert_eq!(message, "inbound poll handle already exists");

        storage_client.admit_ready_poll();
        collect_when_ready(&mut reader, DEFAULT_SESSION_ID).await?;
        Ok(())
    }

    #[tokio::test]
    async fn zero_limits_start_no_poll() -> anyhow::Result<()> {
        let storage_client = MockStorageClient::new();
        let mut reader = TestReader::new(storage_client.clone());
        reader.start(POLL_TIMEOUT, 0, 0, 0)?;

        assert!(matches!(
            reader.try_collect_result(DEFAULT_SESSION_ID).await?,
            InboundPollState::NotStarted
        ));
        assert_eq!(storage_client.num_polls(), (0, 0, 0));
        Ok(())
    }

    #[tokio::test]
    async fn a_lane_with_a_zero_limit_is_not_polled() -> anyhow::Result<()> {
        let storage_client = MockStorageClient::new();
        storage_client.push_ready_batch(
            DEFAULT_SESSION_ID,
            vec![make_entry(RG_ID, JOB_A, TaskId::Index(0))],
        );
        storage_client.push_commit_ready_batch(
            DEFAULT_SESSION_ID,
            vec![make_entry(RG_ID, JOB_B, TaskId::Commit)],
        );
        let mut reader = TestReader::new(storage_client.clone());
        reader.start(POLL_TIMEOUT, 0, MAX_ENTRIES, MAX_ENTRIES)?;

        let result = collect_when_ready(&mut reader, DEFAULT_SESSION_ID).await?;
        assert_eq!(storage_client.num_polls(), (0, 1, 1));
        assert_eq!(result.ready_jobs.as_slice(), &[]);
        assert_eq!(
            result.commit_ready_jobs.as_slice(),
            &[FinalizedJob {
                resource_group_id: RG_ID,
                job_id: JOB_B,
            }]
        );
        Ok(())
    }
}
