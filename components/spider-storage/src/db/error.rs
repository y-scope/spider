use std::fmt::Display;

use spider_core::{
    job::JobState,
    types::id::{JobId, ResourceGroupId},
};

#[derive(thiserror::Error, Debug)]
pub enum DbError {
    ResourceGroupNotFound(ResourceGroupId),

    #[error("resource group `{0:?}` already exists")]
    ResourceGroupAlreadyExists(String),

    #[error("resource group `{0:?}` password is incorrect")]
    InvalidPassword(ResourceGroupId),

    #[error("resource group `{0:?}` has no access")]
    InvalidAccess(ResourceGroupId),

    #[error("job `{0:?}` does not exist")]
    JobNotFound(JobId),

    #[error("job in state {from} cannot transit into state {to}")]
    InvalidJobStateTransition { from: JobState, to: JobState },

    #[error("job in state {current}, expect state {expected}")]
    UnexpectedJobState {
        current: JobState,
        expected: ExpectedStates,
    },

    #[error("data integrity error: {0}")]
    DataIntegrity(String),

    #[error(transparent)]
    Sql(#[from] sqlx::error::Error),
}

#[derive(Debug)]
pub struct ExpectedStates(pub Vec<JobState>);

impl Display for ExpectedStates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let states = self
            .0
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{states}")
    }
}
