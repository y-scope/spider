//! Deterministic fault and pause injection points for tests of the storage service.
//!
//! Hooks are per [`crate::state::Runtime`] instance: every [`TestHooks`] value is independent, so
//! tests that run in one process never see each other's hooks. Without the `test-hooks` feature,
//! [`TestHooks`] is a zero-sized type whose hook points never fire.
//!
//! # Glossary
//!
//! | Term           | Meaning                                                                    |
//! |----------------|----------------------------------------------------------------------------|
//! | Hook point     | A named place in the storage code where a test can act.                    |
//! | Hit            | One pass of the code over a hook point.                                    |
//! | Context        | What a hit is about: its jobs and its task instance.                       |
//! | Filter         | Which hits at a hook point to act on, chosen by their context.             |
//! | Count          | How many of the filtered hits to act on: once, n times, or always.         |
//! | Action         | What to do to a hit that passes the filter and the count.                  |
//! | Hook           | A hook point with a filter, a count and an action.                         |
//! | Arm            | Activates a hook, so that it starts acting on hits.                        |
//! | Fire           | An armed hook acting on a hit at its point that passes filter and count.   |
//! | Pause          | An action: blocks the hit until the test lets it go on or shutdown starts. |
//! | Release        | Lets a paused hit go on.                                                   |
//! | Disarm         | Deactivates an armed hook and releases its paused hits.                    |
//! | Reached        | Waits until an armed hook has fired a given number of times.               |
//! | Injected fault | The error a hit returns, e.g., when shutdown ends its pause.               |
//!
//! When the code hits a hook point, every hook armed at that point checks the hit in this order:
//!
//! 1. Filter: the hook skips the hit unless the hit's context passes its filter.
//! 2. Count: the hook skips the hit if it has already fired its count of times; otherwise, the hit
//!    uses up one.
//! 3. Action: the hook fires and runs its action on the hit.
//!
//! A hit that the filter skips doesn't use up the count. A hook whose count is used up stays armed
//! but never fires again.

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
    /// In a task success, after the task control block has succeeded, before the job's count of
    /// incomplete tasks is decremented.
    TaskSuccessAfterTcbFlip,

    /// In a task success that completes the job's last task, after the job's read guard is dropped,
    /// before the job's outputs are committed.
    TaskSuccessBeforeOutputsCommit,

    /// In a task success, after the outputs are written, before the first successor's lock.
    TaskSuccessBeforeSuccessorLock,

    /// In an instance creation, after the task registered the instance, before the instance is sent
    /// to the task instance pool.
    CreateInstanceBeforePoolSend,
}

/// What a hit of a hook point is about. A filter matches the fields that it sets.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HookContext {
    pub job_ids: Vec<JobId>,
    pub task_instance_id: Option<TaskInstanceId>,
}

impl HookContext {
    /// # Returns
    ///
    /// The context of the single job `job_id`.
    #[must_use]
    pub fn job(job_id: JobId) -> Self {
        Self {
            job_ids: vec![job_id],
            ..Self::default()
        }
    }
}
