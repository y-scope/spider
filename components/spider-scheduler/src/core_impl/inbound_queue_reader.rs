//! The asynchronous inbound-queue reader shared by the scheduler cores.

use std::time::Duration;

use spider_core::types::id::SessionId;

use crate::error::SchedulerError;
use crate::error::StorageClientError;
use crate::storage_client::SchedulerStorageClient;
use crate::types::InboundEntry;

/// The reshaping a core applies to the entries drained from each inbound-queue lane.
///
/// The formatting runs inside the lane's background polling task, so it is kept off the core's
/// critical path.
pub(super) trait InboundPollResultFormatter: Send + 'static {
    /// The formatted result of the regular-task lane.
    type ReadyResult: Default + Send + 'static;

    /// The formatted result of a finalization lane.
    type FinalizedResult: Default + Send + 'static;

    /// Formats the entries drained from the regular-task lane.
    ///
    /// # Parameters
    ///
    /// * `entries` - The entries drained from the lane.
    ///
    /// # Returns
    ///
    /// The formatted lane result.
    fn format_ready(entries: Vec<InboundEntry>) -> Self::ReadyResult;

    /// Formats the entries drained from a finalization lane.
    ///
    /// # Parameters
    ///
    /// * `entries` - The entries drained from the lane.
    ///
    /// # Returns
    ///
    /// The formatted lane result.
    fn format_finalized(entries: Vec<InboundEntry>) -> Self::FinalizedResult;
}

/// The formatter of a core that consumes the drained entries as they are.
pub(super) struct RawInboundEntries;

impl InboundPollResultFormatter for RawInboundEntries {
    type FinalizedResult = Vec<InboundEntry>;
    type ReadyResult = Vec<InboundEntry>;

    fn format_ready(entries: Vec<InboundEntry>) -> Self::ReadyResult {
        entries
    }

    fn format_finalized(entries: Vec<InboundEntry>) -> Self::FinalizedResult {
        entries
    }
}

/// The state of an asynchronous inbound-queue poll.
///
/// # Type Parameters
///
/// * `FormatterType` - The formatter applied to the entries drained from each inbound-queue lane.
pub(super) enum InboundPollState<FormatterType: InboundPollResultFormatter = RawInboundEntries> {
    /// The poll has completed, carrying the polled session and the formatted result of each
    /// inbound-queue lane.
    Ready {
        session_id: SessionId,
        ready_result: FormatterType::ReadyResult,
        commit_ready_result: FormatterType::FinalizedResult,
        cleanup_ready_result: FormatterType::FinalizedResult,
    },

    /// The poll is still in flight.
    Pending,

    /// No poll has been started.
    NotStarted,
}

/// A reader that runs inbound-queue polls as background tasks, with at most one polling request
/// (from all three lanes) in flight at a time.
///
/// # Type Parameters
///
/// * `StorageClientType` - The storage client used to poll the inbound queue.
/// * `FormatterType` - The formatter applied to the entries drained from each inbound-queue lane.
pub(super) struct AsyncInboundQueueReader<
    StorageClientType: SchedulerStorageClient + 'static,
    FormatterType: InboundPollResultFormatter = RawInboundEntries,
> {
    storage_client: StorageClientType,
    handle: Option<InboundPollHandles<FormatterType>>,
}

impl<StorageClientType: SchedulerStorageClient + 'static, FormatterType: InboundPollResultFormatter>
    AsyncInboundQueueReader<StorageClientType, FormatterType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A new reader with no poll in flight.
    pub(super) const fn new(storage_client: StorageClientType) -> Self {
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
    pub(super) async fn try_collect_result(
        &mut self,
        curr_session_id: SessionId,
    ) -> Result<InboundPollState<FormatterType>, SchedulerError> {
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

    /// Starts a new inbound poll, polling and formatting each inbound-queue lane as a background
    /// task.
    ///
    /// Lanes whose entry limit is 0 are not polled; if all limits are 0, no poll is started.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::Internal`] if a poll is already in flight.
    pub(super) fn start(
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
                return Ok((0, Default::default()));
            }
            let (session_id, entries) = ready_storage_client
                .poll_ready(max_ready_entries, storage_poll_timeout)
                .await?;
            Ok((session_id, FormatterType::format_ready(entries)))
        });

        let commit_ready_storage_client = self.storage_client.clone();
        let commit_ready_handle = tokio::task::spawn(async move {
            if max_commit_ready_entries == 0 {
                return Ok((0, Default::default()));
            }
            let (session_id, entries) = commit_ready_storage_client
                .poll_commit_ready(max_commit_ready_entries, storage_poll_timeout)
                .await?;
            Ok((session_id, FormatterType::format_finalized(entries)))
        });

        let cleanup_ready_storage_client = self.storage_client.clone();
        let cleanup_ready_handle = tokio::task::spawn(async move {
            if max_cleanup_ready_entries == 0 {
                return Ok((0, Default::default()));
            }
            let (session_id, entries) = cleanup_ready_storage_client
                .poll_cleanup_ready(max_cleanup_ready_entries, storage_poll_timeout)
                .await?;
            Ok((session_id, FormatterType::format_finalized(entries)))
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

/// The join handles of one in-flight inbound poll, one per inbound-queue lane.
///
/// # Type Parameters
///
/// * `FormatterType` - The formatter applied to the entries drained from each inbound-queue lane.
#[allow(clippy::struct_field_names)]
struct InboundPollHandles<FormatterType: InboundPollResultFormatter> {
    ready_handle: tokio::task::JoinHandle<
        Result<(SessionId, FormatterType::ReadyResult), StorageClientError>,
    >,
    commit_ready_handle: tokio::task::JoinHandle<
        Result<(SessionId, FormatterType::FinalizedResult), StorageClientError>,
    >,
    cleanup_ready_handle: tokio::task::JoinHandle<
        Result<(SessionId, FormatterType::FinalizedResult), StorageClientError>,
    >,
}

impl<FormatterType: InboundPollResultFormatter> InboundPollHandles<FormatterType> {
    /// Tries to collect the results of all lane polls without blocking.
    ///
    /// Results from lanes that report an older session than the latest observed session are
    /// dropped.
    ///
    /// # Returns
    ///
    /// On success:
    ///
    /// * [`InboundPollState::Pending`] if any lane poll is still in flight.
    /// * [`InboundPollState::Ready`] with the latest observed session and its results otherwise.
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
    ) -> Result<InboundPollState<FormatterType>, SchedulerError> {
        if !self.ready_handle.is_finished()
            || !self.commit_ready_handle.is_finished()
            || !self.cleanup_ready_handle.is_finished()
        {
            return Ok(InboundPollState::Pending);
        }

        let (ready_session_id, ready_result) = (&mut self.ready_handle)
            .await
            .map_err(|e| SchedulerError::Internal(e.to_string()))??;
        let (commit_session_id, commit_ready_result) = (&mut self.commit_ready_handle)
            .await
            .map_err(|e| SchedulerError::Internal(e.to_string()))??;
        let (cleanup_session_id, cleanup_ready_result) =
            (&mut self.cleanup_ready_handle)
                .await
                .map_err(|e| SchedulerError::Internal(e.to_string()))??;

        let latest_session_id = curr_session_id
            .max(ready_session_id)
            .max(commit_session_id)
            .max(cleanup_session_id);

        Ok(InboundPollState::Ready {
            session_id: latest_session_id,
            ready_result: Self::drop_if_stale(ready_session_id, latest_session_id, ready_result),
            commit_ready_result: Self::drop_if_stale(
                commit_session_id,
                latest_session_id,
                commit_ready_result,
            ),
            cleanup_ready_result: Self::drop_if_stale(
                cleanup_session_id,
                latest_session_id,
                cleanup_ready_result,
            ),
        })
    }

    /// # Type Parameters
    ///
    /// * `LaneResultType` - The formatted result of a single inbound-queue lane.
    ///
    /// # Returns
    ///
    /// `lane_result` if `session_id` matches `latest_session_id`, or a default-constructed result
    /// otherwise.
    fn drop_if_stale<LaneResultType: Default>(
        session_id: SessionId,
        latest_session_id: SessionId,
        lane_result: LaneResultType,
    ) -> LaneResultType {
        if session_id == latest_session_id {
            lane_result
        } else {
            LaneResultType::default()
        }
    }
}

#[cfg(test)]
pub(super) mod test_harness {
    //! The shared test harness of the inbound-queue reader.
    //!
    //! Drives a reader against scripted poll batches instead of a storage service, so that every
    //! core's reader tests can script the same lanes and collect the same formatted results.

    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
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
    use tokio::sync::Semaphore;

    use super::AsyncInboundQueueReader;
    use super::InboundPollResultFormatter;
    use super::InboundPollState;
    use crate::error::StorageClientError;
    use crate::storage_client::SchedulerStorageClient;
    use crate::types::InboundEntry;

    /// The session the mock storage client serves polls under unless a test scripts otherwise.
    pub const DEFAULT_SESSION_ID: SessionId = 0;

    /// The entry limit given to lanes a test wants polled.
    pub const MAX_ENTRIES: usize = 32;

    /// The poll timeout passed to the mock storage client, which never blocks on it.
    pub const POLL_TIMEOUT: Duration = Duration::from_millis(1);

    /// The formatted results of one completed inbound poll.
    ///
    /// # Type Parameters
    ///
    /// * `FormatterType` - The formatter applied to the entries drained from each inbound-queue
    ///   lane.
    pub struct CollectedPoll<FormatterType: InboundPollResultFormatter> {
        /// The latest session the poll observed.
        pub session_id: SessionId,

        /// The formatted result of the regular-task lane.
        pub ready_result: FormatterType::ReadyResult,

        /// The formatted result of the commit-task lane.
        pub commit_ready_result: FormatterType::FinalizedResult,

        /// The formatted result of the cleanup-task lane.
        pub cleanup_ready_result: FormatterType::FinalizedResult,
    }

    /// A mock [`SchedulerStorageClient`] backed by scripted poll batches.
    ///
    /// Each lane serves its scripted batches in FIFO order, one batch per poll; when a lane's
    /// script is empty, polls return an empty batch under the mock's current session immediately
    /// (the `wait` parameter is ignored to keep tests fast). The regular-task lane can be gated so
    /// that a test can observe an in-flight poll.
    #[derive(Clone)]
    pub struct MockStorageClient {
        inner: Arc<MockStorageInner>,
    }

    impl MockStorageClient {
        /// Factory function.
        ///
        /// # Returns
        ///
        /// A new mock storage client with no scripted batches and no gated lane, reporting
        /// [`DEFAULT_SESSION_ID`] on empty polls.
        pub fn new() -> Self {
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

        /// Scripts a batch to be served by the next unserved [`SchedulerStorageClient::poll_ready`]
        /// call.
        pub fn push_ready_batch(&self, session_id: SessionId, entries: Vec<InboundEntry>) {
            self.inner
                .ready_batches
                .lock()
                .expect("ready-batch lock poisoned")
                .push_back((session_id, entries));
        }

        /// Scripts a batch to be served by the next unserved
        /// [`SchedulerStorageClient::poll_commit_ready`] call.
        pub fn push_commit_ready_batch(&self, session_id: SessionId, entries: Vec<InboundEntry>) {
            self.inner
                .commit_ready_batches
                .lock()
                .expect("commit-ready-batch lock poisoned")
                .push_back((session_id, entries));
        }

        /// Scripts a batch to be served by the next unserved
        /// [`SchedulerStorageClient::poll_cleanup_ready`] call.
        pub fn push_cleanup_ready_batch(&self, session_id: SessionId, entries: Vec<InboundEntry>) {
            self.inner
                .cleanup_ready_batches
                .lock()
                .expect("cleanup-ready-batch lock poisoned")
                .push_back((session_id, entries));
        }

        /// # Returns
        ///
        /// A tuple containing:
        ///
        /// * The number of polls served by the regular-task lane.
        /// * The number of polls served by the commit-task lane.
        /// * The number of polls served by the cleanup-task lane.
        pub fn num_polls(&self) -> (u64, u64, u64) {
            (
                self.inner.num_ready_polls.load(Ordering::Relaxed),
                self.inner.num_commit_ready_polls.load(Ordering::Relaxed),
                self.inner.num_cleanup_ready_polls.load(Ordering::Relaxed),
            )
        }

        /// Holds every subsequent regular-task poll until [`Self::admit_ready_poll`] releases it.
        pub fn gate_ready_lane(&self) {
            self.inner
                .is_ready_lane_gated
                .store(true, Ordering::Relaxed);
        }

        /// Releases one held regular-task poll.
        pub fn admit_ready_poll(&self) {
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

    /// # Returns
    ///
    /// An inbound entry for `job_id`'s `task_id`, owned by `resource_group_id`.
    pub const fn make_entry(
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
    /// # Type Parameters
    ///
    /// * `StorageClientType` - The storage client used to poll the inbound queue.
    /// * `FormatterType` - The formatter applied to the entries drained from each inbound-queue
    ///   lane.
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
    pub async fn collect_when_ready<
        StorageClientType: SchedulerStorageClient + 'static,
        FormatterType: InboundPollResultFormatter,
    >(
        reader: &mut AsyncInboundQueueReader<StorageClientType, FormatterType>,
        curr_session_id: SessionId,
    ) -> anyhow::Result<CollectedPoll<FormatterType>> {
        let deadline = tokio::time::Instant::now() + COLLECT_DEADLINE;
        loop {
            match reader.try_collect_result(curr_session_id).await? {
                InboundPollState::Ready {
                    session_id,
                    ready_result,
                    commit_ready_result,
                    cleanup_ready_result,
                } => {
                    return Ok(CollectedPoll {
                        session_id,
                        ready_result,
                        commit_ready_result,
                        cleanup_ready_result,
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

    /// The maximum time to wait for an in-flight poll to complete before failing a test.
    const COLLECT_DEADLINE: Duration = Duration::from_secs(5);

    /// The interval between two collection attempts while waiting for an in-flight poll.
    const COLLECT_RETRY_INTERVAL: Duration = Duration::from_millis(1);

    /// The scripted state shared by every clone of a [`MockStorageClient`].
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
}

#[cfg(test)]
mod tests {
    use anyhow::bail;
    use spider_core::types::id::JobId;
    use spider_core::types::id::ResourceGroupId;
    use spider_core::types::id::SessionId;
    use spider_core::types::id::TaskId;

    use super::test_harness::DEFAULT_SESSION_ID;
    use super::test_harness::MAX_ENTRIES;
    use super::test_harness::MockStorageClient;
    use super::test_harness::POLL_TIMEOUT;
    use super::test_harness::collect_when_ready;
    use super::test_harness::make_entry;
    use super::*;

    /// The resource group the entries in this module belong to.
    const RG_ID: ResourceGroupId = ResourceGroupId::from(4);

    /// The first job a test polls entries for.
    const JOB_A: JobId = JobId::from(10);

    /// The second job a test polls entries for.
    const JOB_B: JobId = JobId::from(11);

    /// The third job a test polls entries for.
    const JOB_C: JobId = JobId::from(12);

    /// The reader under test, which passes the entries drained from every lane through unchanged.
    type TestReader = AsyncInboundQueueReader<MockStorageClient>;

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
    async fn drained_entries_are_returned_unchanged() -> anyhow::Result<()> {
        let ready_entries = vec![
            make_entry(RG_ID, JOB_A, TaskId::Index(3)),
            make_entry(RG_ID, JOB_A, TaskId::Index(1)),
            make_entry(RG_ID, JOB_A, TaskId::Index(3)),
            make_entry(RG_ID, JOB_B, TaskId::Commit),
        ];
        let commit_ready_entries = vec![make_entry(RG_ID, JOB_B, TaskId::Commit)];
        let cleanup_ready_entries = vec![make_entry(RG_ID, JOB_C, TaskId::Cleanup)];

        let storage_client = MockStorageClient::new();
        storage_client.push_ready_batch(DEFAULT_SESSION_ID, ready_entries.clone());
        storage_client.push_commit_ready_batch(DEFAULT_SESSION_ID, commit_ready_entries.clone());
        storage_client.push_cleanup_ready_batch(DEFAULT_SESSION_ID, cleanup_ready_entries.clone());
        let mut reader = TestReader::new(storage_client);
        reader.start(POLL_TIMEOUT, MAX_ENTRIES, MAX_ENTRIES, MAX_ENTRIES)?;

        let result = collect_when_ready(&mut reader, DEFAULT_SESSION_ID).await?;
        assert_eq!(result.ready_result, ready_entries);
        assert_eq!(result.commit_ready_result, commit_ready_entries);
        assert_eq!(result.cleanup_ready_result, cleanup_ready_entries);
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
        assert_eq!(
            result.ready_result.as_slice(),
            &[make_entry(RG_ID, JOB_A, TaskId::Index(0))]
        );

        assert!(matches!(
            reader.try_collect_result(DEFAULT_SESSION_ID).await?,
            InboundPollState::NotStarted
        ));
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
        assert_eq!(result.ready_result.as_slice(), &[]);
        assert_eq!(result.cleanup_ready_result.as_slice(), &[]);
        assert_eq!(
            result.commit_ready_result.as_slice(),
            &[make_entry(RG_ID, JOB_B, TaskId::Commit)]
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
        assert_eq!(result.ready_result.as_slice(), &[]);
        assert_eq!(
            result.commit_ready_result.as_slice(),
            &[make_entry(RG_ID, JOB_B, TaskId::Commit)]
        );
        Ok(())
    }
}
