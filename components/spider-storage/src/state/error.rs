use spider_core::types::id::{JobId, SessionId};
use spider_tdl::error::TdlError;

use crate::{cache::error::CacheError, db::DbError};

/// Errors that can occur during storage server operations.
#[derive(thiserror::Error, Debug)]
pub enum StorageServerError {
    #[error(transparent)]
    Cache(#[from] CacheError),

    #[error(transparent)]
    Db(#[from] DbError),

    #[error(transparent)]
    Tdl(#[from] TdlError),

    #[error("current storage session is {0}")]
    StaleSession(SessionId),

    #[error("server is shutting down: {0}")]
    Stopping(String),

    #[error("job not found in cache: {0:?}")]
    JobNotFound(JobId),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("serde: {0}")]
    Serde(#[source] Box<dyn std::error::Error + Send + Sync>),
}
