//! The asynchronous inbound-queue reader shared by the scheduler cores.
//!
//! The reader turns the storage client's blocking inbound-queue polls into background tasks, so a
//! core's serial loop never stalls on storage. At most one poll, covering the regular,
//! commit-ready, and cleanup-ready lanes, is in flight at a time; the core drains its result
//! without blocking and starts the next poll when it is ready for more entries.

use std::time::Duration;

use spider_core::types::id::SessionId;

use crate::error::SchedulerError;
use crate::error::StorageClientError;
use crate::storage_client::SchedulerStorageClient;
use crate::types::InboundEntry;

/// The state of an asynchronous inbound-queue poll.
pub(super) enum InboundPollState {
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
struct InboundPollHandles {
    regular: tokio::task::JoinHandle<Result<(SessionId, Vec<InboundEntry>), StorageClientError>>,
    commit: tokio::task::JoinHandle<Result<(SessionId, Vec<InboundEntry>), StorageClientError>>,
    cleanup: tokio::task::JoinHandle<Result<(SessionId, Vec<InboundEntry>), StorageClientError>>,
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
        if !self.regular.is_finished() || !self.commit.is_finished() || !self.cleanup.is_finished()
        {
            return Ok(InboundPollState::Pending);
        }

        let (ready_session_id, ready_entries) = (&mut self.regular)
            .await
            .map_err(|e| SchedulerError::Internal(e.to_string()))??;
        let (commit_session_id, commit_ready_entries) = (&mut self.commit)
            .await
            .map_err(|e| SchedulerError::Internal(e.to_string()))??;
        let (cleanup_session_id, cleanup_ready_entries) = (&mut self.cleanup)
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
pub(super) struct AsyncInboundQueueReader<StorageClientType: SchedulerStorageClient + 'static> {
    storage_client: StorageClientType,
    handles: Option<InboundPollHandles>,
}

impl<StorageClientType: SchedulerStorageClient + 'static>
    AsyncInboundQueueReader<StorageClientType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A new reader with no poll in flight.
    pub(super) const fn new(storage_client: StorageClientType) -> Self {
        Self {
            storage_client,
            handles: None,
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
    ) -> Result<InboundPollState, SchedulerError> {
        match &mut self.handles {
            None => Ok(InboundPollState::NotStarted),
            Some(handles) => {
                let inbound_poll_state = handles.try_collect_result(curr_session_id).await?;
                if !matches!(inbound_poll_state, InboundPollState::Pending) {
                    self.handles = None;
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
    pub(super) fn start(
        &mut self,
        storage_poll_timeout: Duration,
        max_ready_entries: usize,
        max_commit_ready_entries: usize,
        max_cleanup_ready_entries: usize,
    ) -> Result<(), SchedulerError> {
        if self.handles.is_some() {
            return Err(SchedulerError::Internal(
                "inbound poll handle already exists".to_owned(),
            ));
        }

        if 0 == max_ready_entries && 0 == max_commit_ready_entries && 0 == max_cleanup_ready_entries
        {
            tracing::info!("Inbound poll skipped: all entry limits are 0.");
            return Ok(());
        }

        let ready_storage_client = self.storage_client.clone();
        let regular = tokio::task::spawn(async move {
            if 0 == max_ready_entries {
                return Ok((0, Vec::new()));
            }
            ready_storage_client
                .poll_ready(max_ready_entries, storage_poll_timeout)
                .await
        });

        let commit_ready_storage_client = self.storage_client.clone();
        let commit = tokio::task::spawn(async move {
            if 0 == max_commit_ready_entries {
                return Ok((0, Vec::new()));
            }
            commit_ready_storage_client
                .poll_commit_ready(max_commit_ready_entries, storage_poll_timeout)
                .await
        });

        let cleanup_ready_storage_client = self.storage_client.clone();
        let cleanup = tokio::task::spawn(async move {
            if 0 == max_cleanup_ready_entries {
                return Ok((0, Vec::new()));
            }
            cleanup_ready_storage_client
                .poll_cleanup_ready(max_cleanup_ready_entries, storage_poll_timeout)
                .await
        });

        self.handles = Some(InboundPollHandles {
            regular,
            commit,
            cleanup,
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
