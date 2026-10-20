//! The test hooks of a build with the `test-hooks` feature.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use spider_core::types::id::JobId;
use spider_utils::shutdown::ShutdownHandle;
use tokio::sync::Notify;
use tokio::sync::Semaphore;

use crate::test_hooks::EventKindTag;
use crate::test_hooks::HookContext;
use crate::test_hooks::HookPoint;

/// Selects the hits a hook fires on. An unset field matches anything.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HookFilter {
    pub root_id: Option<JobId>,
    pub job_id: Option<JobId>,
    pub event_kind: Option<EventKindTag>,
}

impl HookFilter {
    /// # Returns
    ///
    /// A filter that matches the hits of the tree rooted at `root_id`.
    #[must_use]
    pub fn root(root_id: JobId) -> Self {
        Self {
            root_id: Some(root_id),
            ..Self::default()
        }
    }

    /// # Returns
    ///
    /// A filter that matches the hits that concern `job_id`.
    #[must_use]
    pub fn job(job_id: JobId) -> Self {
        Self {
            job_id: Some(job_id),
            ..Self::default()
        }
    }

    /// # Returns
    ///
    /// Whether every field that the filter sets matches `context`.
    fn matches(&self, context: &HookContext) -> bool {
        self.root_id
            .is_none_or(|root_id| context.root_id == Some(root_id))
            && self
                .job_id
                .is_none_or(|job_id| context.job_ids.contains(&job_id))
            && self
                .event_kind
                .is_none_or(|event_kind| context.event_kinds.contains(&event_kind))
    }
}

/// What a hook does when it fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookAction {
    /// Blocks until released, or until the runtime's shutdown is requested.
    Pause,

    /// Counts the hit and never blocks.
    Observe,

    /// Fails the operation with an injected internal error.
    Crash,

    /// Panics.
    Panic,
}

/// How many times a hook fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookCount {
    Once,
    Times(usize),
    Always,
}

/// A fault that a hook injects into the code at its hook point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InjectedFault {
    /// A pause ended because the runtime's shutdown was requested.
    Stopping,

    /// An injected crash.
    Crash(HookPoint),
}

impl InjectedFault {
    /// # Returns
    ///
    /// Whether the fault is a pause that ended because the runtime's shutdown was requested.
    #[must_use]
    pub const fn is_stopping(self) -> bool {
        matches!(self, Self::Stopping)
    }
}

/// The hooks of one storage runtime.
#[derive(Clone, Default)]
pub struct TestHooks {
    inner: Arc<Mutex<TestHooksInner>>,
}

impl TestHooks {
    /// Arms a hook.
    ///
    /// # Returns
    ///
    /// The armed hook. Dropping it disarms the hook and releases every waiter.
    ///
    /// # Panics
    ///
    /// Panics if `point` is [`HookPoint::EventSubmitted`] and `action` is not
    /// [`HookAction::Observe`].
    #[must_use]
    pub fn arm(
        &self,
        point: HookPoint,
        filter: HookFilter,
        action: HookAction,
        count: HookCount,
    ) -> ArmedHook {
        assert!(
            point != HookPoint::EventSubmitted || action == HookAction::Observe,
            "only `Observe` can be armed at `EventSubmitted`"
        );
        let remaining = match count {
            HookCount::Once => 1,
            HookCount::Times(times) => times,
            HookCount::Always => usize::MAX,
        };
        let state = Arc::new(ArmedHookState {
            point,
            filter,
            action,
            remaining: AtomicUsize::new(remaining),
            hits: AtomicUsize::new(0),
            notify: Notify::new(),
            semaphore: Semaphore::new(0),
            last_context: Mutex::new(None),
        });
        self.lock().hooks.push(Arc::clone(&state));
        ArmedHook {
            state,
            hooks: self.clone(),
        }
    }

    /// Registers the shutdown handle of the runtime that uses these hooks, whose shutdown ends
    /// every pause.
    pub fn register_runtime(&self, shutdown_handle: &ShutdownHandle) {
        self.lock().shutdown_handle = Some(shutdown_handle.clone());
    }

    /// Fires every armed hook at `point` that matches the context that `context` builds.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InjectedFault::Stopping`] if a pause ended because the runtime's shutdown was requested.
    /// * [`InjectedFault::Crash`] if a crash fired.
    ///
    /// # Panics
    ///
    /// Panics if a panic fired.
    pub async fn hit(
        &self,
        point: HookPoint,
        context: impl FnOnce() -> HookContext,
    ) -> Result<(), InjectedFault> {
        let context = context();
        let (fired, shutdown_handle) = {
            let inner = self.lock();
            let fired: Vec<Arc<ArmedHookState>> = inner
                .hooks
                .iter()
                .filter(|hook| hook.point == point && hook.try_fire(&context))
                .cloned()
                .collect();
            let shutdown_handle = inner.shutdown_handle.clone();
            drop(inner);
            (fired, shutdown_handle)
        };
        for hook in fired {
            match hook.action {
                HookAction::Observe => {}
                HookAction::Pause => {
                    let shutdown_handle = shutdown_handle.clone().unwrap_or_default();
                    tokio::select! {
                        permit = hook.semaphore.acquire() => {
                            if let Ok(permit) = permit {
                                permit.forget();
                            }
                        }
                        () = shutdown_handle.cancelled() => return Err(InjectedFault::Stopping),
                    }
                }
                HookAction::Crash => return Err(InjectedFault::Crash(point)),
                HookAction::Panic => panic!("injected panic at {point:?}"),
            }
        }
        Ok(())
    }

    /// Counts a hit of every armed [`HookAction::Observe`] hook at `point` that matches the context
    /// that `context` builds, without awaiting.
    pub fn observe(&self, point: HookPoint, context: impl FnOnce() -> HookContext) {
        let context = context();
        for hook in &self.lock().hooks {
            if hook.point == point && hook.action == HookAction::Observe {
                hook.try_fire(&context);
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TestHooksInner> {
        self.inner
            .lock()
            .expect("the test hooks' lock should not be poisoned")
    }
}

/// An armed hook. Dropping it disarms the hook and releases every waiter.
pub struct ArmedHook {
    state: Arc<ArmedHookState>,
    hooks: TestHooks,
}

impl ArmedHook {
    /// Waits until the hook has fired `num_hits` times.
    pub async fn reached(&self, num_hits: usize) {
        loop {
            let notified = self.state.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.hits() >= num_hits {
                return;
            }
            notified.await;
        }
    }

    /// Lets one paused hit continue. A release before the hit is not lost.
    pub fn release(&self) {
        self.state.semaphore.add_permits(1);
    }

    /// # Returns
    ///
    /// The number of times the hook has fired.
    #[must_use]
    pub fn hits(&self) -> usize {
        self.state.hits.load(Ordering::Acquire)
    }

    /// # Returns
    ///
    /// The context of the hook's latest hit, if any.
    ///
    /// # Panics
    ///
    /// Panics if the hook's context lock is poisoned.
    #[must_use]
    pub fn last_context(&self) -> Option<HookContext> {
        self.state
            .last_context
            .lock()
            .expect("the hook's context lock should not be poisoned")
            .clone()
    }

    /// Disarms the hook and releases every waiter.
    pub fn disarm(self) {
        drop(self);
    }
}

impl Drop for ArmedHook {
    fn drop(&mut self) {
        self.state.semaphore.close();
        self.hooks
            .lock()
            .hooks
            .retain(|hook| !Arc::ptr_eq(hook, &self.state));
    }
}

#[derive(Default)]
struct TestHooksInner {
    hooks: Vec<Arc<ArmedHookState>>,
    shutdown_handle: Option<ShutdownHandle>,
}

struct ArmedHookState {
    point: HookPoint,
    filter: HookFilter,
    action: HookAction,
    remaining: AtomicUsize,
    hits: AtomicUsize,
    notify: Notify,
    semaphore: Semaphore,
    last_context: Mutex<Option<HookContext>>,
}

impl ArmedHookState {
    /// Fires the hook if `context` matches its filter and it has hits left, counting the hit and
    /// waking every [`ArmedHook::reached`] waiter.
    ///
    /// # Returns
    ///
    /// Whether the hook fired.
    fn try_fire(&self, context: &HookContext) -> bool {
        if !self.filter.matches(context) {
            return false;
        }
        let has_hits_left = self
            .remaining
            .try_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |remaining| match remaining {
                    0 => None,
                    usize::MAX => Some(usize::MAX),
                    remaining => Some(remaining - 1),
                },
            )
            .is_ok();
        if !has_hits_left {
            return false;
        }
        *self
            .last_context
            .lock()
            .expect("the hook's context lock should not be poisoned") = Some(context.clone());
        self.hits.fetch_add(1, Ordering::AcqRel);
        self.notify.notify_waiters();
        true
    }
}
