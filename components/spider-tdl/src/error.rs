//! Error type returned from user-authored TDL tasks.
//!
//! [`TdlError`] crosses the C-FFI boundary as a msgpack-encoded payload inside
//! `ExecutionResult::Error`, so it derives both `serde::Serialize` and `serde::Deserialize`.

/// Errors produced while deserializing inputs, executing a user task, or serializing outputs.
///
/// User task functions return `Result<T, TdlError>`. The `TaskHandlerImpl` wrapper additionally
/// produces `TdlError` values for framing failures on either side of the wire.
#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
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
    fn round_trip_task_not_found() -> anyhow::Result<()> {
        let original = TdlError::TaskNotFound("my_task".to_owned());
        let encoded = rmp_serde::to_vec(&original)?;
        let decoded: TdlError = rmp_serde::from_slice(&encoded)?;
        assert!(matches!(decoded, TdlError::TaskNotFound(ref name) if name == "my_task"));
        Ok(())
    }

    #[test]
    fn round_trip_all_variants() -> anyhow::Result<()> {
        let cases = [
            TdlError::TaskNotFound("t".to_owned()),
            TdlError::DeserializationError("d".to_owned()),
            TdlError::SerializationError("s".to_owned()),
            TdlError::ExecutionError("e".to_owned()),
            TdlError::Custom("c".to_owned()),
        ];
        for original in cases {
            let original_display = original.to_string();
            let encoded = rmp_serde::to_vec(&original)?;
            let decoded: TdlError = rmp_serde::from_slice(&encoded)?;
            assert_eq!(decoded.to_string(), original_display);
        }
        Ok(())
    }
}
