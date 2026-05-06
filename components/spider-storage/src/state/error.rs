use spider_core::types::id::JobId;

use crate::{cache::error::CacheError, db::DbError};

/// Errors that can occur during storage server operations.
#[derive(thiserror::Error, Debug)]
pub enum StorageServerError {
    #[error(transparent)]
    Cache(#[from] CacheError),

    #[error(transparent)]
    Db(#[from] DbError),

    #[error("stale session")]
    StaleSession,

    #[error("server is shutting down: {0}")]
    Stopping(String),

    #[error("job not found in cache: {0:?}")]
    JobNotFound(JobId),

    #[error("job already exists: {0:?}")]
    JobAlreadyExists(JobId),

    #[error("bad request: {0}")]
    BadRequest(String),
}
