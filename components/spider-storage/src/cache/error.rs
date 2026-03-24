use spider_core::task::TaskState;

/// Enums for all possible errors that can occur in a cache operation.
#[derive(thiserror::Error, Debug)]
pub enum CacheError {
    #[error(transparent)]
    Internal(#[from] InternalError),

    #[error(transparent)]
    StaleState(#[from] StaleStateError),
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

    #[error("A single-source task output has already been written")]
    TaskOutputAlreadyWritten,

    #[error("task graph corrupted: {0}")]
    TaskGraphCorrupted(String),

    #[error("task graph input size mismatch: expected {0}, got {1}")]
    TaskGraphInputsSizeMismatch(usize, usize),
}

/// Enums for all errors representing operations that are rejected due to stale cache state.
///
/// A stale-state error indicates that the operation was valid at the time it was issued, but can no
/// longer be applied because the cache state has since changed.
///
/// These errors are typically caused by stale requests (e.g., outdated cache session or concurrent
/// updates). They are expected during normal operation and should be propagated to the caller for
/// notification.
#[derive(thiserror::Error, Debug)]
pub enum StaleStateError {
    #[error("the number of living task instances has reached the upper limit")]
    TaskInstanceLimitExceeded,

    #[error("task already in terminal state {0:?}")]
    TaskAlreadyTerminated(TaskState),

    #[error("the task instance ID is not valid")]
    InvalidTaskInstanceId,
}
