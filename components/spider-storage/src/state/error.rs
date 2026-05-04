use crate::{
    cache::error::{InternalError, StaleStateError},
    db::DbError,
};

/// Errors that can occur during storage server operations.
#[derive(thiserror::Error, Debug)]
pub enum StorageServerError {
    #[error(transparent)]
    Internal(#[from] InternalError),

    #[error(transparent)]
    StaleState(#[from] StaleStateError),

    #[error("stale session")]
    StaleSession,

    #[error(transparent)]
    Db(#[from] DbError),

    #[error("server is stopping: {0}")]
    Stopping(&'static str),

    #[error("bad request: {0}")]
    BadRequest(&'static str),
}
