use std::sync::atomic::AtomicUsize;

use spider_core::job::JobState;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::cache::{
    error::{CacheError, InternalError, InternalError::UnexpectedJobState, StaleStateError},
    task::TaskGraph,
};

/// A concurrency-safe handle to a job’s execution state.
///
/// This type wraps [`JobExecutionState`] in a read-write lock and provides controlled access to it.
/// All accessors enforce state invariants by validating the underlying job state before returning a
/// read or write guard.
///
/// This ensures that callers can only observe or mutate the execution state when the job is in a
/// valid state for the requested operation.
struct JobExecutionStateHandle {
    inner: tokio::sync::RwLock<JobExecutionState>,
}

impl JobExecutionStateHandle {
    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_running`]'s return values on failure.
    async fn read_running(&self) -> Result<RwLockReadGuard<'_, JobExecutionState>, CacheError> {
        self.validate_and_read(JobExecutionState::ensure_running)
            .await
    }

    /// # Returns
    ///
    /// A writer guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_running`]'s return values on failure.
    async fn write_running(&self) -> Result<RwLockWriteGuard<'_, JobExecutionState>, CacheError> {
        self.validate_and_write(JobExecutionState::ensure_running)
            .await
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_commit_ready`]'s return values on failure.
    async fn read_commit_ready(
        &self,
    ) -> Result<RwLockReadGuard<'_, JobExecutionState>, CacheError> {
        self.validate_and_read(JobExecutionState::ensure_commit_ready)
            .await
    }

    /// # Returns
    ///
    /// A writer guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_commit_ready`]'s return values on failure.
    async fn write_commit_ready(
        &self,
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState>, CacheError> {
        self.validate_and_write(JobExecutionState::ensure_commit_ready)
            .await
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_cleanup_ready`]'s return values on failure.
    async fn read_cleanup_ready(
        &self,
    ) -> Result<RwLockReadGuard<'_, JobExecutionState>, CacheError> {
        self.validate_and_read(JobExecutionState::ensure_cleanup_ready)
            .await
    }

    /// # Returns
    ///
    /// A writer guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_cleanup_ready`]'s return values on failure.
    async fn write_cleanup_ready(
        &self,
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState>, CacheError> {
        self.validate_and_write(JobExecutionState::ensure_cleanup_ready)
            .await
    }

    /// # Returns
    ///
    /// A writer guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_non_terminated`]'s return values on failure.
    async fn write_non_terminated(
        &self,
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState>, CacheError> {
        self.validate_and_write(JobExecutionState::ensure_non_terminated)
            .await
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards `validator`'s return values on failure.
    async fn validate_and_read(
        &self,
        validator: fn(&JobExecutionState) -> Result<(), CacheError>,
    ) -> Result<RwLockReadGuard<'_, JobExecutionState>, CacheError> {
        let guard = self.inner.read().await;
        validator(&guard)?;
        Ok(guard)
    }

    /// # Returns
    ///
    /// A writer guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards `validator`'s return values on failure.
    async fn validate_and_write(
        &self,
        validator: fn(&JobExecutionState) -> Result<(), CacheError>,
    ) -> Result<RwLockWriteGuard<'_, JobExecutionState>, CacheError> {
        let guard = self.inner.write().await;
        validator(&guard)?;
        Ok(guard)
    }
}

/// Represents the execution state of a job.
///
/// # Note
///
/// This struct doesn't provide synchronization for concurrent access to the underlying task graph.
struct JobExecutionState {
    state: JobState,
    task_graph: TaskGraph,
    num_incomplete_tasks: AtomicUsize,
}

impl JobExecutionState {
    /// Ensures that the job is currently in the [`JobState::Running`] state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::JobNotStarted`] if the job hasn't been started yet.
    /// * [`StaleStateError::JobNoLongerRunning`] if the job is no longer running.
    fn ensure_running(&self) -> Result<(), CacheError> {
        if !self.state.is_running() {
            if matches!(self.state, JobState::Ready) {
                return Err(InternalError::JobNotStarted.into());
            }
            return Err(StaleStateError::JobNoLongerRunning.into());
        }
        Ok(())
    }

    /// Ensures that the job is currently in the [`JobState::CommitReady`] state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UnexpectedJobState`] if the job is in an unexpected state.
    /// * [`StaleStateError::JobNoLongerCommitReady`] if the job is no longer commit-ready.
    fn ensure_commit_ready(&self) -> Result<(), CacheError> {
        if !matches!(self.state, JobState::CommitReady) {
            if self.state.is_terminal() || matches!(self.state, JobState::CleanupReady) {
                return Err(StaleStateError::JobNoLongerCommitReady.into());
            }
            return Err(UnexpectedJobState {
                current: self.state,
                expected: JobState::CommitReady,
            }
            .into());
        }
        Ok(())
    }

    /// Ensures that the job is currently in the [`JobState::CleanupReady`] state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UnexpectedJobState`] if the job is in an unexpected state.
    /// * [`StaleStateError::JobNoLongerCommitReady`] if the job is no longer cleanup-ready.
    fn ensure_cleanup_ready(&self) -> Result<(), CacheError> {
        if !matches!(self.state, JobState::CleanupReady) {
            if self.state.is_terminal() {
                return Err(StaleStateError::JobNoLongerCleanupReady.into());
            }
            return Err(UnexpectedJobState {
                current: self.state,
                expected: JobState::CommitReady,
            }
            .into());
        }
        Ok(())
    }

    /// Ensures that the job is currently in a non-terminated state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::JobAlreadyTerminated`] if the job is already terminated.
    fn ensure_non_terminated(&self) -> Result<(), CacheError> {
        if self.state.is_terminal() {
            return Err(StaleStateError::JobAlreadyTerminated(self.state).into());
        }
        Ok(())
    }
}
