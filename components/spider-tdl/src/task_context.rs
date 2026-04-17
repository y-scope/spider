//! Runtime metadata passed to every task function.
//!
//! [`TaskContext`] is constructed and msgpack-serialized by the execution manager, passed opaquely
//! through the task executor, and deserialized inside the TDL package before being handed to the
//! user's task function as the first parameter.

use spider_core::types::id::{JobId, ResourceGroupId, TaskId, TaskInstanceId};

/// Runtime metadata about the current task execution.
///
/// Every task function receives a [`TaskContext`] as its first parameter. It carries identifiers
/// that link the execution back to the job and task definitions in the storage layer.
///
/// Serialized via plain msgpack ([`rmp_serde::to_vec`] or [`rmp_serde::from_slice`]), separately
/// from the task inputs wire stream.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TaskContext {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub task_instance_id: TaskInstanceId,
    pub resource_group_id: ResourceGroupId,
}

#[cfg(test)]
mod tests {
    use spider_core::types::id::{JobId, ResourceGroupId, TaskId};

    use super::TaskContext;

    #[test]
    fn round_trip_msgpack() -> anyhow::Result<()> {
        let ctx = TaskContext {
            job_id: JobId::new(),
            task_id: TaskId::new(),
            task_instance_id: 13,
            resource_group_id: ResourceGroupId::new(),
        };
        let encoded = rmp_serde::to_vec(&ctx)?;
        let decoded: TaskContext = rmp_serde::from_slice(&encoded)?;
        assert_eq!(decoded, ctx);
        Ok(())
    }
}
