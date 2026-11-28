//! Compressed job submissions and successful task outcomes.

use serde::Deserialize;
use serde::Serialize;

use crate::types::id::TaskId;

/// The maximum number of child jobs of one job, over all of its tasks' reports.
pub const MAX_CHILDREN_PER_JOB: usize = 1024;

/// The maximum compressed size of the child jobs of one job, over all of its tasks' reports.
pub const MAX_JOB_CHILDREN_SIZE: u64 = 16 * 1024 * 1024;

/// The maximum compressed size of child job submissions from one task.
pub const MAX_TASK_CHILDREN_SIZE: u64 = 16 * 1024 * 1024;

/// A job submission as submitted: its zstd-compressed serialized task graph and its zstd-compressed
/// serialized job inputs, not validated yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompressedJobSubmission {
    pub compressed_serialized_task_graph: Vec<u8>,
    pub compressed_serialized_job_inputs: Vec<u8>,
}

impl CompressedJobSubmission {
    /// # Returns
    ///
    /// The total size of the compressed serialized task graph and job inputs.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.compressed_serialized_task_graph.len() as u64
            + self.compressed_serialized_job_inputs.len() as u64
    }
}

/// A commit or cleanup task attempted to submit child jobs.
#[derive(Debug, thiserror::Error)]
#[error("termination tasks cannot submit child jobs")]
pub struct FinalizingTaskWithChildren;

/// The outcome of a successful task execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskSuccessOutcome {
    /// A regular task's wire-format outputs and child submissions in submission order.
    Regular {
        serialized_outputs: Vec<u8>,
        children: Vec<CompressedJobSubmission>,
    },
    /// A commit or cleanup task's success, without outputs or child submissions.
    Finalizing,
}

impl TaskSuccessOutcome {
    /// Creates a successful task outcome, discarding outputs for commit and cleanup tasks.
    ///
    /// # Returns
    ///
    /// The successful task outcome on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`FinalizingTaskWithChildren`] if a commit or cleanup task submits children.
    pub fn new(
        task_id: TaskId,
        serialized_outputs: Vec<u8>,
        children: Vec<CompressedJobSubmission>,
    ) -> Result<Self, FinalizingTaskWithChildren> {
        match task_id {
            TaskId::Index(_) => Ok(Self::Regular {
                serialized_outputs,
                children,
            }),
            TaskId::Commit | TaskId::Cleanup if children.is_empty() => Ok(Self::Finalizing),
            TaskId::Commit | TaskId::Cleanup => Err(FinalizingTaskWithChildren),
        }
    }
}

const _: () = assert!(
    MAX_TASK_CHILDREN_SIZE <= MAX_JOB_CHILDREN_SIZE,
    "one task's children count toward its job's children, so the limit of one task's children \
     should not exceed the job's"
);

#[cfg(test)]
mod tests {
    use super::CompressedJobSubmission;
    use super::TaskSuccessOutcome;
    use crate::types::id::TaskId;

    #[test]
    fn regular_outcome_preserves_outputs_and_children() -> anyhow::Result<()> {
        let children = vec![
            CompressedJobSubmission {
                compressed_serialized_task_graph: vec![1],
                compressed_serialized_job_inputs: vec![2],
            },
            CompressedJobSubmission {
                compressed_serialized_task_graph: vec![3],
                compressed_serialized_job_inputs: vec![4],
            },
        ];
        assert_eq!(
            TaskSuccessOutcome::new(TaskId::Index(0), vec![5], children.clone())?,
            TaskSuccessOutcome::Regular {
                serialized_outputs: vec![5],
                children
            },
        );
        Ok(())
    }

    #[test]
    fn finalizing_outcome_discards_outputs() -> anyhow::Result<()> {
        for task_id in [TaskId::Commit, TaskId::Cleanup] {
            assert_eq!(
                TaskSuccessOutcome::new(task_id, vec![1, 2], Vec::new())?,
                TaskSuccessOutcome::Finalizing,
            );
        }
        Ok(())
    }

    #[test]
    fn finalizing_outcome_rejects_children() {
        for task_id in [TaskId::Commit, TaskId::Cleanup] {
            assert!(
                TaskSuccessOutcome::new(
                    task_id,
                    Vec::new(),
                    vec![CompressedJobSubmission {
                        compressed_serialized_task_graph: vec![1],
                        compressed_serialized_job_inputs: vec![2],
                    }]
                )
                .is_err()
            );
        }
    }
}
