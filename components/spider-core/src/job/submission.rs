//! Compressed job submissions and task success report limits.

use serde::Deserialize;
use serde::Serialize;

/// The maximum compressed size of one child job submission. Storage may enforce a smaller
/// limit based on its database configuration.
pub const MAX_CHILD_SIZE: u64 = 4 * 1024 * 1024;

/// The maximum number of child jobs of one job, over all of its tasks' reports.
pub const MAX_CHILDREN_PER_JOB: usize = 1024;

/// The maximum number of compressed bytes of the child jobs of one success report.
pub const MAX_REPORT_CHILDREN_SIZE: u64 = 16 * 1024 * 1024;

/// The maximum number of bytes of the outputs of one success report.
pub const MAX_REPORT_OUTPUTS_SIZE: u64 = 4 * 1024 * 1024;

/// The maximum size of a decoded message of the task instance management service.
pub const MAX_TASK_INSTANCE_MESSAGE_SIZE: usize = 32 * 1024 * 1024;

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

/// The room that a task instance message leaves beyond a success report's outputs and children, for
/// its framing and metadata.
const TASK_INSTANCE_MESSAGE_MARGIN: u64 = 1024 * 1024;

const _: () = assert!(
    MAX_REPORT_OUTPUTS_SIZE + MAX_REPORT_CHILDREN_SIZE + TASK_INSTANCE_MESSAGE_MARGIN
        <= MAX_TASK_INSTANCE_MESSAGE_SIZE as u64
);
