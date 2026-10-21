//! The test hooks of a build without the `test-hooks` feature: no hook point ever fires.

use spider_utils::shutdown::ShutdownHandle;

use crate::cache::error::CacheError;
use crate::test_hooks::HookContext;
use crate::test_hooks::HookPoint;

/// A fault that a hook injects. Without the `test-hooks` feature, no fault exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InjectedFault {}

impl From<InjectedFault> for CacheError {
    fn from(fault: InjectedFault) -> Self {
        match fault {}
    }
}

/// The hooks of one storage runtime, none of which can be armed.
#[derive(Clone, Default)]
pub struct TestHooks {}

impl TestHooks {
    /// Does nothing: there is no hook to release a runtime to.
    pub const fn register_runtime(&self, _shutdown_handle: &ShutdownHandle) {}

    /// Fires no hook.
    ///
    /// # Returns
    ///
    /// A future that is ready at once.
    pub fn hit(
        &self,
        _point: HookPoint,
        _context: impl FnOnce() -> HookContext,
    ) -> std::future::Ready<Result<(), InjectedFault>> {
        std::future::ready(Ok(()))
    }
}
