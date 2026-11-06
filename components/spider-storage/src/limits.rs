//! Limits that the storage service enforces.

/// The maximum number of events that one round of a tree's committer plans.
pub const MAX_EVENTS_PER_ROUND: usize = 256;

/// The maximum number of bytes of job outputs that one round of a tree's committer writes. The
/// round's first event that writes any is admitted even if it alone exceeds this.
pub const MAX_ROUND_PAYLOAD: u64 = 16 * 1024 * 1024;

/// The maximum number of characters of a terminal failure's message that the failed job stores. A
/// longer message is cut to this many characters.
pub const MAX_ERROR_MESSAGE_CHARS: usize = 64 * 1024;

/// The maximum number of child jobs of one job, over all of its tasks' reports, and of one report.
pub const MAX_CHILDREN_PER_JOB: usize = 10_000;

/// The maximum number of compressed bytes of the child jobs of one job.
pub const MAX_JOB_CHILD_BYTES: u64 = 16 * 1024 * 1024;
