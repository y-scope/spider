use spider_core::types::id::JobId;
use spider_core::types::id::SessionId;
use spider_tdl::error::TdlError;

use crate::cache::error::CacheError;
use crate::db::DbError;

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

impl StorageServerError {
    /// # Returns
    ///
    /// Whether the error leaves the storage service unable to continue, which is the case for:
    ///
    /// * [`CacheError::Internal`].
    /// * [`DbError::CorruptedDbState`].
    /// * [`DbError::Fenced`].
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::Cache(CacheError::Internal(_))
                | Self::Db(DbError::CorruptedDbState(_) | DbError::Fenced { .. })
        )
    }
}
