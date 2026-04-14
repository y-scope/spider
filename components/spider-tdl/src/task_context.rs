//! Runtime metadata passed to every task function as the first parameter.
//!
//! [`TaskContext`] is constructed by the execution manager, msgpack-serialized, and forwarded
//! through the task executor into the TDL package. It is separate from the task's user-supplied
//! inputs, which travel as a wire-format byte stream.

use spider_core::types::id::{JobId, TaskId, TaskInstanceId};

/// Runtime metadata for a single task execution.
///
/// Every task function receives a [`TaskContext`] as its first parameter, providing identity
/// information about the job, task, and task instance that triggered the execution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskContext {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub task_instance_id: TaskInstanceId,
}

#[cfg(test)]
mod tests {
    use spider_core::types::id::{JobId, TaskId};

    use super::TaskContext;

    #[test]
    fn round_trip_msgpack() -> anyhow::Result<()> {
        let original = TaskContext {
            job_id: JobId::new(),
            task_id: TaskId::new(),
            task_instance_id: 42,
        };

        let encoded = rmp_serde::to_vec(&original)?;
        let decoded: TaskContext = rmp_serde::from_slice(&encoded)?;

        assert_eq!(original.job_id.as_uuid_ref(), decoded.job_id.as_uuid_ref());
        assert_eq!(
            original.task_id.as_uuid_ref(),
            decoded.task_id.as_uuid_ref()
        );
        assert_eq!(original.task_instance_id, decoded.task_instance_id);
        Ok(())
    }
}
