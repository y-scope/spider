//! Deterministic fault and pause injection points for tests of the job-tree committer.
//!
//! Hooks are per runtime instance: every [`TestHooks`] value is independent, so tests that run in
//! one process never see each other's hooks. Without the `test-hooks` feature, [`TestHooks`] is a
//! zero-sized type whose hook points never fire.

use spider_core::types::id::JobId;
use spider_core::types::id::TaskInstanceId;

#[cfg(feature = "test-hooks")]
mod armed;
#[cfg(not(feature = "test-hooks"))]
mod disarmed;

#[cfg(feature = "test-hooks")]
pub use armed::ArmedHook;
#[cfg(feature = "test-hooks")]
pub use armed::HookAction;
#[cfg(feature = "test-hooks")]
pub use armed::HookCount;
#[cfg(feature = "test-hooks")]
pub use armed::HookFilter;
#[cfg(feature = "test-hooks")]
pub use armed::InjectedFault;
#[cfg(feature = "test-hooks")]
pub use armed::TestHooks;
#[cfg(not(feature = "test-hooks"))]
pub use disarmed::InjectedFault;
#[cfg(not(feature = "test-hooks"))]
pub use disarmed::TestHooks;

/// A point in the storage service where a hook can fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookPoint {
    /// After the committer has received events, before it takes a round from them.
    CommitterAfterReceive,

    /// After the committer has collected a round's inputs, before it plans the round.
    CommitterBeforePlan,

    /// After the committer has planned a round, before its database transaction.
    CommitterBeforeDbTransaction,

    /// After a round's database transaction has committed, before the cache apply.
    CommitterAfterCommit,

    /// After the committer has applied one job's transition to the cache.
    CommitterAfterApplyJob,

    /// In a task success, after the task control block has succeeded, before the job's count of
    /// incomplete tasks is decremented.
    PhaseAAfterTcbFlip,

    /// In a task success, after the job's guard is dropped, before the event is submitted.
    PhaseABeforeSubmit,

    /// In a task success, after the outputs are written, before the first successor's lock.
    PhaseABeforeSuccessorLock,

    /// In an instance creation, after the task registered the instance, before the instance is
    /// sent to the task instance pool.
    CreateInstanceBeforePoolSend,

    /// Right after an event is sent to its tree's committer. Only observing hooks fire here.
    EventSubmitted,
}

/// The kind of a tree event, without its payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKindTag {
    Start,
    RegularTasksDone,
    CommitDone,
    CleanupDone,
    TerminalFailure,
    CancelRoot,
}

/// What a hit of a hook point is about. A filter matches the fields that it sets.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HookContext {
    pub root_id: Option<JobId>,
    pub job_ids: Vec<JobId>,
    pub event_kinds: Vec<EventKindTag>,
    pub task_instance_id: Option<TaskInstanceId>,
}

impl HookContext {
    /// # Returns
    ///
    /// The context of a single job of the tree rooted at `root_id`.
    #[must_use]
    pub fn job(root_id: JobId, job_id: JobId) -> Self {
        Self {
            root_id: Some(root_id),
            job_ids: vec![job_id],
            ..Self::default()
        }
    }

    /// # Returns
    ///
    /// The context of one event of kind `kind` of the job `job_id`, in the tree rooted at
    /// `root_id`.
    #[must_use]
    pub fn event(root_id: JobId, job_id: JobId, kind: EventKindTag) -> Self {
        Self {
            root_id: Some(root_id),
            job_ids: vec![job_id],
            event_kinds: vec![kind],
            task_instance_id: None,
        }
    }
}
