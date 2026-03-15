use spider_core::{
    task::{TaskIndex, TaskState},
    types::id::JobId,
};

/// Enums for all possible errors that can happen in the cache.
pub enum CacheError {
    Internal(InternalError),
    Rejection(RejectionError),
}

/// Enums for all internal errors. When these error happens, it is considered that the system is in
/// an inconsistent state and cannot continue to service requests. A restart is needed to recover
/// the cache from the storage.
#[derive(thiserror::Error, Debug)]
pub enum InternalError {
    #[error("task output already written by a previous successful task instance")]
    TaskOutputDuplicateWrite,

    #[error("task input not ready when attempting to register a task instance")]
    TaskInputNotReady,

    #[error("out-of-bound task access detected")]
    TaskIndexOutOfBound,

    #[error("task not ready when attempting to register a task instance")]
    TaskNotReady,

    #[error("task graph corrupted: {0}")]
    TaskGraphCorrupted(String),

    #[error("failed to send scheduling context into the channel")]
    TokioSendError(#[from] tokio::sync::mpsc::error::SendError<(JobId, TaskIndex)>),

    #[error("task outputs length mismatch: expected {0}, got {1}")]
    TaskOutputsLengthMismatch(usize, usize),
}

impl From<InternalError> for CacheError {
    fn from(e: InternalError) -> Self {
        CacheError::Internal(e)
    }
}

/// Enums for all rejection errors. When these error happens, it is considered that the request is
/// valid, but cannot be processed due to the current state of the cache. These errors should be
/// forwarded to the client for notification.
#[derive(thiserror::Error, Debug)]
pub enum RejectionError {
    #[error("task instance ID is not registered")]
    InvalidTaskInstanceId,

    #[error("task is already in a terminal state: {0:?}")]
    TaskAlreadyTerminated(TaskState),

    #[error("the number of living task instances has reached the upper limit")]
    TaskInstanceLimitExceeded,

    #[error("task output not ready")]
    TaskOutputNotReady,
}

impl From<RejectionError> for CacheError {
    fn from(e: RejectionError) -> Self {
        CacheError::Rejection(e)
    }
}
