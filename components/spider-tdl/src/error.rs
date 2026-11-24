//! Error type returned from user-authored TDL tasks.
//!
//! [`TdlError`] crosses the C-FFI boundary as a msgpack-encoded payload inside
//! [`ExecutionResult::Error`], so it derives both [`serde::Serialize`] and [`serde::Deserialize`].

/// All possible errors produced while executing a user-defined task inside the task executor. This
/// type can be serialized across the C-FFI boundary as a msgpack-encoded payload through [`serde`].
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

    #[error("invalid task context: {0}")]
    InvalidTaskContext(String),

    #[error("{0}")]
    Custom(String),

    #[error(
        "child job submissions exceed the limit of {count_limit} children or {size_limit} \
         compressed bytes"
    )]
    ChildJobSubmissionLimitExceeded { count_limit: usize, size_limit: u64 },
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
            TdlError::InvalidTaskContext("invalid_task_context".to_owned()),
            TdlError::Custom("custom".to_owned()),
            TdlError::ChildJobSubmissionLimitExceeded {
                count_limit: 1024,
                size_limit: 16 * 1024 * 1024,
            },
        ];
        for error in errors_to_test {
            let encoded = rmp_serde::to_vec(&error)?;
            let decoded: TdlError = rmp_serde::from_slice(&encoded)?;
            assert_eq!(decoded, error);
        }
        Ok(())
    }
}
