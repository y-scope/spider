use spider_core::job::JobState;
use spider_core::types::id::{JobId, ResourceGroupId};

#[derive(thiserror::Error, Debug)]
pub enum DbError {
    #[error("resource group `{0:?}` not found")]
    ResourceGroupNotFound(ResourceGroupId),
    #[error("resource group `{0:?}` has no access")]
    InvalidAccess(ResourceGroupId),
    #[error("job `{0:?}` does not exist")]
    JobNotFound(JobId),
    #[error("job in wrong state: {0:?}")]
    WrongJobState(JobState),
    #[error(transparent)]
    Sql(#[from] sqlx::error::Error),
}