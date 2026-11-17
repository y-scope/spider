//! Limits that the storage service enforces.

pub use spider_core::job::submission::MAX_CHILDREN_PER_JOB;
pub use spider_core::job::submission::MAX_REPORT_CHILDREN_SIZE;
pub use spider_core::job::submission::MAX_REPORT_OUTPUTS_SIZE;
pub use spider_core::job::submission::MAX_TASK_INSTANCE_MESSAGE_SIZE;

/// The maximum number of events that one round of a tree's committer plans.
pub const MAX_EVENTS_PER_ROUND: usize = 256;

/// The maximum number of bytes of job outputs and child payloads that one round of a tree's
/// committer writes. The round's first event that writes any is admitted even if it alone exceeds
/// this.
pub const MAX_ROUND_PAYLOAD: u64 = 16 * 1024 * 1024;

/// The maximum number of characters of a terminal failure's message that the failed job stores. A
/// longer message is cut to this many characters.
pub const MAX_ERROR_MESSAGE_CHARS: usize = 64 * 1024;

/// The maximum number of compressed bytes of the child jobs of one job.
pub const MAX_JOB_CHILDREN_SIZE: u64 = 16 * 1024 * 1024;

/// The maximum depth of a job in its tree. A root is at depth 0.
pub const MAX_DEPTH: u32 = 8;
