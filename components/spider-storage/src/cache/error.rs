use spider_core::job::JobState;
use spider_core::task::TaskState;
use spider_core::types::id::JobId;
use spider_utils::wire::WireError;

use crate::db::DbError;

/// Enums for all possible errors that can occur in a cache operation.
#[derive(thiserror::Error, Debug)]
pub enum CacheError {
    #[error(transparent)]
    Internal(#[from] InternalError),

    #[error(transparent)]
    StaleState(#[from] StaleStateError),

    #[error("storage service is shutting down")]
    ServiceStopping,
}

impl From<DbError> for CacheError {
    fn from(err: DbError) -> Self {
        Self::Internal(InternalError::Db(err))
    }
}

/// Enums for all internal errors.
///
/// An internal error indicates that the cache has entered an inconsistent or undefined state,
/// typically due to invariant violations, corrupted state, or unexpected system failures.
///
/// When such an error occurs, the cache is considered unsafe to continue serving requests. Recovery
/// generally requires restarting the service and rebuilding the cache state from the underlying
/// persistent storage.
#[derive(thiserror::Error, Debug)]
pub enum InternalError {
    #[error("task not in running state")]
    TaskNotRunning,

    #[error("task not ready when attempting to register a task instance")]
    TaskNotReady,

    #[error("task input not ready")]
    TaskInputNotReady,

    #[error("task outputs length mismatch: expected {0}, got {1}")]
    TaskOutputsLengthMismatch(usize, usize),

    #[error("a single-source task output has already been written")]
    TaskOutputAlreadyWritten,

    #[error("task graph corrupted: {0}")]
    TaskGraphCorrupted(String),

    #[error("job not started")]
    JobNotStarted,

    #[error("job not found: {0:?}")]
    JobNotFound(JobId),

    #[error("job already exists: {0:?}")]
    JobAlreadyExists(JobId),

    #[error("job in state {current}, expect state {expected}")]
    UnexpectedJobState {
        current: JobState,
        expected: JobState,
    },

    #[error("task index out of bound")]
    TaskIndexOutOfBound,

    #[error("job has no commit task")]
    UndefinedCommitTask,

    #[error("job has no cleanup task")]
    UndefinedCleanupTask,

    #[error("job {job_id:?} is cached as {actual}, but a committed transition expects {expected}")]
    CacheDbDivergence {
        job_id: JobId,
        expected: JobState,
        actual: JobState,
    },

    #[error("job terminated unexpectedly")]
    UnexpectedJobTermination,

    #[error("spawned handler body failed: {0}")]
    HandlerBodyFailed(String),

    #[cfg(feature = "test-hooks")]
    #[error("injected fault: {0}")]
    InjectedFault(String),

    #[error("task instance pool corrupted: {0}")]
    TaskInstancePoolCorrupted(String),

    #[error("invalid task instance pool config: {0}")]
    TaskInstancePoolInvalidConfig(&'static str),

    #[error("invalid job cache GC config: {0}")]
    JobCacheGcInvalidConfig(&'static str),

    #[error("inbound queue channel is closed")]
    InboundQueueChannelClosed,

    #[error("job tree poisoned: an event was dropped without a reply")]
    TreePoisoned,

    #[error("invalid recoverable job context: {0}")]
    InvalidRecoverableJobContext(String),

    #[error(transparent)]
    WireError(#[from] WireError),

    #[error(transparent)]
    TaskOutputs(#[from] spider_core::types::io::TaskOutputsError),

    #[error(transparent)]
    Db(#[from] crate::db::DbError),
}

/// Enums for all errors representing operations that are rejected due to stale cache state.
///
/// A stale-state error indicates that the operation cannot be applied to the current cache state:
/// either the state has changed since the operation was issued, or it has not yet reached the state
/// the operation requires.
///
/// These errors are typically caused by stale requests (e.g., outdated cache session or concurrent
/// updates), or by requests made too early (e.g., reading the outputs of a job that has not
/// succeeded). They are expected during normal operation and should be propagated to the caller for
/// notification.
#[derive(thiserror::Error, Debug)]
pub enum StaleStateError {
    #[error("the number of living task instances has reached the upper limit")]
    TaskInstanceLimitExceeded,

    #[error("task already in terminal state {0:?}")]
    TaskAlreadyTerminated(TaskState),

    #[error("the task instance ID is not valid")]
    InvalidTaskInstanceId,

    #[error("job no longer running")]
    JobNoLongerRunning,

    #[error("job no longer in the commit-ready state")]
    JobNoLongerCommitReady,

    #[error("job no longer in the cleanup-ready state")]
    JobNoLongerCleanupReady,

    #[error("job already terminated in state {0:?}")]
    JobAlreadyTerminated(JobState),

    #[error("job already requested for cancellation")]
    JobCancellationAlreadyRequested,

    #[error("job already cancelled")]
    JobAlreadyCancelled,

    #[error("job already started")]
    JobAlreadyStarted,

    #[error("job not succeeded: in state {0:?}")]
    JobNotSucceeded(JobState),
}
