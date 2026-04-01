use std::fmt::Display;

use spider_core::{
    job::JobState,
    types::id::{JobId, ResourceGroupId},
};

#[derive(thiserror::Error, Debug)]
pub enum DbError {
    #[error("resource group `{0:?}` does not exist")]
    ResourceGroupNotFound(ResourceGroupId),

    #[error("resource group `{0:?}` already exists")]
    ResourceGroupAlreadyExists(String),

    #[error("resource group `{0:?}` password is incorrect")]
    InvalidPassword(ResourceGroupId),

    #[error("job `{0:?}` does not exist")]
    JobNotFound(JobId),

    #[error("job in state {from} cannot transit into state {to}")]
    InvalidJobStateTransition { from: JobState, to: JobState },

    #[error("job in state {current}, expect state {expected}")]
    UnexpectedJobState {
        current: JobState,
        expected: ExpectedStates,
    },

    #[error("database corrupted: {0}")]
    CorruptedDbState(String),

    #[error("Task graph serialization failure: {0}")]
    TaskGraphSerializationFailure(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("Value serialization failure: {0}")]
    ValueSerializationFailure(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("Value deserialization failure: {0}")]
    ValueDeserializationFailure(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error(transparent)]
    Sql(#[from] sqlx::error::Error),
}

impl DbError {
    pub fn task_graph_ser<SerializationError: serde::ser::Error + Send + Sync + 'static>(
        e: SerializationError,
    ) -> Self {
        Self::TaskGraphSerializationFailure(Box::new(e))
    }

    pub fn value_ser<SerializationError: serde::ser::Error + Send + Sync + 'static>(
        e: SerializationError,
    ) -> Self {
        Self::ValueSerializationFailure(Box::new(e))
    }

    pub fn value_de<DeserializationError: serde::de::Error + Send + Sync + 'static>(
        e: DeserializationError,
    ) -> Self {
        Self::ValueDeserializationFailure(Box::new(e))
    }
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
