use spider_core::types::id::JobId;
use spider_core::types::id::SessionId;
use spider_tdl::error::TdlError;

use crate::cache::error::CacheError;
use crate::db::DbError;

/// Errors that can occur during storage server operations.
#[derive(thiserror::Error, Debug)]
pub enum StorageServerError {
    #[error(transparent)]
    Cache(CacheError),

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

impl From<CacheError> for StorageServerError {
    fn from(error: CacheError) -> Self {
        match error {
            CacheError::ServiceStopping => Self::Stopping("a job tree refused an event".to_owned()),
            error => Self::Cache(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::error::StaleStateError;

    #[test]
    fn only_a_service_stopping_cache_error_converts_to_stopping() {
        let error = StorageServerError::from(CacheError::ServiceStopping);
        assert!(
            matches!(error, StorageServerError::Stopping(_)),
            "a service-stopping cache error should convert to stopping, got {error:?}"
        );

        let error =
            StorageServerError::from(CacheError::StaleState(StaleStateError::JobAlreadyStarted));
        assert!(
            matches!(
                error,
                StorageServerError::Cache(CacheError::StaleState(
                    StaleStateError::JobAlreadyStarted
                ))
            ),
            "any other cache error should stay a cache error, got {error:?}"
        );
    }
}
