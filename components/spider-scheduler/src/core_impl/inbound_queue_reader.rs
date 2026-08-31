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
    type ReadyResult = Vec<InboundEntry>;
    type FinalizedResult = Vec<InboundEntry>;

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

impl<
    StorageClientType: SchedulerStorageClient + 'static,
    FormatterType: InboundPollResultFormatter,
> AsyncInboundQueueReader<StorageClientType, FormatterType>
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
        let (cleanup_session_id, cleanup_ready_result) = (&mut self.cleanup_ready_handle)
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
