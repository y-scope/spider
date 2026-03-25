use spider_core::types::io::TaskInput;

use crate::cache::{
    error::{CacheError, InternalError},
    sync::{Reader, Writer},
};

/// Spider channel implementation placeholder.
pub struct Channel {}

/// Value storage.
pub type ValuePayload = Option<Vec<u8>>;

/// Represents a shared reader for reading a task input.
pub enum InputReader {
    Channel(Channel),
    Value(Reader<ValuePayload>),
}

impl InputReader {
    /// Reads the underlying data as [`TaskInput`].
    ///
    /// # Returns
    ///
    /// A [`TaskInput`] of the underlying data.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskInputNotReady`] if the value payload is not ready (i.e., it is
    ///   `None`).
    pub async fn read_as_task_input(&self) -> Result<TaskInput, CacheError> {
        match self {
            Self::Value(value_reader) => value_reader.read().await.as_ref().map_or_else(
                || Err(InternalError::TaskInputNotReady.into()),
                |value| Ok(TaskInput::ValuePayload(value.clone())),
            ),
            Self::Channel(_) => unimplemented!("channel input is not supported yet"),
        }
    }
}

/// Represents a writer for a shared value payload.
pub type OutputWriter = Writer<ValuePayload>;

/// Represents a reader for a shared value payload.
pub type OutputReader = Reader<ValuePayload>;
