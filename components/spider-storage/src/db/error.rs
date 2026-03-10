use spider_core::{
    job::JobState,
    types::id::{JobId, ResourceGroupId},
};

#[derive(thiserror::Error, Debug)]
pub enum DbError {
    #[error("resource group `{0:?}` not found")]
    ResourceGroupNotFound(ResourceGroupId),
    #[error("resource group `{0:?}` already exists")]
    ResourceGroupAlreadyExists(ResourceGroupId),
    #[error("resource group `{0:?}` password is incorrect")]
    InvalidPassword(ResourceGroupId),
    #[error("resource group `{0:?}` has no access")]
    InvalidAccess(ResourceGroupId),
    #[error("job `{0:?}` does not exist")]
    JobNotFound(JobId),
    #[error("job in wrong state: {0:?}")]
    WrongJobState(JobState),
    #[error("data integrity error: {0}")]
    DataIntegrity(String),
    #[error(transparent)]
    Sql(#[from] sqlx::error::Error),
}
