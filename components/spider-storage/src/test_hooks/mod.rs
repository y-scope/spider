//! Deterministic fault and pause injection points for tests of the storage service.
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
    /// In a task success, after the task control block has succeeded, before the job's count of
    /// incomplete tasks is decremented.
    PhaseAAfterTcbFlip,

    /// In a task success that completes the job's last task, after the job's read guard is
    /// dropped, before the job's outputs are committed.
    PhaseABeforeSubmit,

    /// In a task success, after the outputs are written, before the first successor's lock.
    PhaseABeforeSuccessorLock,

    /// In an instance creation, after the task registered the instance, before the instance is
    /// sent to the task instance pool.
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
