//! Error types for converting protobuf wire values into Spider core types.

/// Errors produced when converting a protobuf message into its Spider core representation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A protobuf task index could not be represented as a [`usize`].
    #[error("task index does not fit in `usize`: {0}")]
    TaskIndexOutOfRange(u64),

    /// A protobuf [`crate::common::TaskId`] carried no `kind`.
    #[error("task id missing kind")]
    TaskIdKindMissing,

    /// A protobuf [`crate::storage::JobState`] was left unspecified.
    #[error("job state is unspecified")]
    JobStateUnspecified,

    /// A protobuf [`crate::storage::TdlContext`] was missing.
    #[error("TDL context is missing")]
    TdlContextMissing,

    /// A protobuf [`crate::storage::TimeoutPolicy`] was missing.
    #[error("timeout policy is missing")]
    TimeoutPolicyMissing,

    /// A protobuf [`crate::scheduler::NextTaskResponse`] carried no `result`.
    #[error("next task response is missing its result")]
    NextTaskResultMissing,

    /// A protobuf [`crate::scheduler::SchedulerAssignment`] carried no `task_id`.
    #[error("scheduler assignment is missing its task id")]
    TaskIdMissing,
}
