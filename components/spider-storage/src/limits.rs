//! Limits that the storage service enforces.
//!
//! Each layer enforces the child job limits as follows:
//!
//! * [`ServiceState::succeed_task_instance`] checks every child job limit before it stores a
//!   report's children. A report that breaks one fails its task instance. It checks:
//!   * The children's depth against [`MAX_DEPTH`].
//!   * Each child's size against the database's statement payload.
//!   * The report's children against [`MAX_TASK_CHILDREN_SIZE`].
//!   * The job's children against [`MAX_CHILDREN_PER_JOB`] and [`MAX_JOB_CHILDREN_SIZE`].
//! * A task's [`TaskContext`](spider_tdl::TaskContext) checks its own children early. The service
//!   doesn't rely on it.
//! * The tree planner treats a job's children over the per-job limits as a broken invariant
//!   ([`InternalError::ChildrenOverJobLimits`]).
//! * The database layer rejects a child too large for one row
//!   ([`DbError::ChildTooLarge`](crate::db::DbError::ChildTooLarge)) as a backstop.
//!
//! [`ServiceState::succeed_task_instance`]: crate::state::ServiceState::succeed_task_instance
//! [`InternalError::ChildrenOverJobLimits`]:
//!     crate::cache::error::InternalError::ChildrenOverJobLimits

pub use spider_core::job::submission::MAX_CHILDREN_PER_JOB;
pub use spider_core::job::submission::MAX_JOB_CHILDREN_SIZE;
pub use spider_core::job::submission::MAX_TASK_CHILDREN_SIZE;
use spider_proto_rust::storage::MAX_TASK_INSTANCE_MESSAGE_SIZE;

/// The maximum number of bytes of the outputs of one success report.
pub const MAX_REPORT_OUTPUTS_SIZE: u64 = 4 * 1024 * 1024;

/// The maximum number of events that one round of a tree's committer plans.
pub const MAX_EVENTS_PER_ROUND: usize = 256;

/// The maximum number of bytes of job outputs and child payloads that one round of a tree's
/// committer writes. The round's first event that writes any is admitted even if it alone exceeds
/// this.
pub const MAX_ROUND_PAYLOAD: u64 = 16 * 1024 * 1024;

/// The maximum number of characters of a terminal failure's message that the failed job stores. A
/// longer message is cut to this many characters.
pub const MAX_ERROR_MESSAGE_CHARS: usize = 64 * 1024;

/// The maximum depth of a job in its tree. A root is at depth 0.
pub const MAX_DEPTH: u32 = 8;

/// The maximum number of characters of a failed job's message that is repeated in the failure
/// message of each ancestor that the failure fails. A longer message is cut to this many
/// characters.
pub(crate) const MAX_PROPAGATED_MESSAGE_CHARS: usize = 1024;

/// Space reserved for a success report's framing and metadata.
const TASK_INSTANCE_MESSAGE_MARGIN: u64 = 1024 * 1024;

const _: () = assert!(
    MAX_REPORT_OUTPUTS_SIZE + MAX_TASK_CHILDREN_SIZE + TASK_INSTANCE_MESSAGE_MARGIN
        <= MAX_TASK_INSTANCE_MESSAGE_SIZE as u64
);

const _: () = assert!(
    MAX_PROPAGATED_MESSAGE_CHARS < MAX_ERROR_MESSAGE_CHARS / 2,
    "a propagated failure message, with the prefix naming the failed job, should fit in a stored \
     failure message without being cut again"
);
