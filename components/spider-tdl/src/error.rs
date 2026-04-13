//! Error type returned from user-authored TDL tasks.
//!
//! [`TdlError`] crosses the C-FFI boundary as a msgpack-encoded payload inside
//! [`ExecutionResult::Error`], so it derives both [`serde::Serialize`] and [`serde::Deserialize`].

/// All possible errors produced while executing a user-defined task inside the task executor.
/// This type can be serialized across the C-FFI boundary as a msgpack-encoded payload through
/// [`serde`].
#[derive(Debug, Eq, PartialEq, thiserror::Error, serde::Serialize, serde::Deserialize)]
pub enum TdlError {
    #[error("task not found: {0}")]
    TaskNotFound(String),

    #[error("deserialization error: {0}")]
    DeserializationError(String),

    #[error("serialization error: {0}")]
    SerializationError(String),

    #[error("execution error: {0}")]
    ExecutionError(String),

    #[error("{0}")]
    Custom(String),
}

#[cfg(test)]
mod tests {
    use super::TdlError;

    #[test]
    fn round_trip_all_variants() -> anyhow::Result<()> {
        let errors_to_test = [
            TdlError::TaskNotFound("task_not_found".to_owned()),
            TdlError::DeserializationError("deserialization_error".to_owned()),
            TdlError::SerializationError("serialization_error".to_owned()),
            TdlError::ExecutionError("execution_error".to_owned()),
            TdlError::Custom("custom".to_owned()),
        ];
        for error in errors_to_test {
            let encoded = rmp_serde::to_vec(&error)?;
            let decoded: TdlError = rmp_serde::from_slice(&encoded)?;
            assert_eq!(decoded, error);
        }
        Ok(())
    }
}
