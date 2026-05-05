use spider_core::types::id::JobId;

use crate::{
    cache::error::{InternalError, StaleStateError},
    db::DbError,
};

/// Errors that can occur during storage server operations.
#[derive(thiserror::Error, Debug)]
pub enum StorageServerError {
    #[error(transparent)]
    CacheInternal(#[from] InternalError),

    #[error(transparent)]
    CacheStaleState(#[from] StaleStateError),

    #[error("stale session")]
    StaleSession,

    #[error(transparent)]
    Db(#[from] DbError),

    #[error("server is shutting down: {0}")]
    Stopping(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("job already exists: {0:?}")]
    JobAlreadyExists(JobId),
}
