//! Monotonically increasing session tracker shared across services.
//!
//! Wraps an [`AtomicU64`] in [`Arc`] so multiple tasks (and multiple consumers such as the
//! execution manager and the scheduler) can observe and advance a shared view of storage's current
//! session id.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::types::id::SessionId;

/// Monotonically increasing counter holding a service's view of the current storage session id.
///
/// Cloneable; clones share the same underlying counter so writers in different tasks stay coherent.
#[derive(Clone, Debug, Default)]
pub struct SessionTracker {
    inner: Arc<AtomicU64>,
}

impl SessionTracker {
    /// Builds a tracker pre-loaded with `initial`.
    ///
    /// # Returns
    ///
    /// A newly created [`SessionTracker`] on success.
    #[must_use]
    pub fn new(initial: SessionId) -> Self {
        Self {
            inner: Arc::new(AtomicU64::new(initial)),
        }
    }

    /// # Returns
    ///
    /// The currently stored session id.
    #[must_use]
    pub fn current(&self) -> SessionId {
        self.inner.load(Ordering::Acquire)
    }

    /// Attempts to advance the stored session id to `new_sid`.
    ///
    /// CAS-loop: if the stored value is already `>= new_sid`, the call no-ops. Otherwise the
    /// stored value is bumped to `new_sid`. Coherent under concurrent writers.
    ///
    /// # Returns
    ///
    /// Whether `new_sid` strictly advanced the stored value.
    #[must_use]
    pub fn try_advance(&self, new_sid: SessionId) -> bool {
        let mut cur = self.inner.load(Ordering::Acquire);
        loop {
            if new_sid <= cur {
                return false;
            }
            match self.inner.compare_exchange_weak(
                cur,
                new_sid,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::task::TaskTracker;

    use super::SessionTracker;

    #[test]
    fn try_advance_forward() {
        let tracker = SessionTracker::new(1);
        assert!(tracker.try_advance(5));
        assert_eq!(tracker.current(), 5);
    }

    #[test]
    fn try_advance_stale_or_equal() {
        let tracker = SessionTracker::new(10);
        assert!(!tracker.try_advance(10));
        assert!(!tracker.try_advance(7));
        assert_eq!(tracker.current(), 10);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_advance_converges_to_max() {
        const MAX_TARGET: u64 = 1_000;
        let tracker = SessionTracker::new(0);
        let task_tracker = TaskTracker::new();
        for i in 1..=MAX_TARGET {
            let t = tracker.clone();
            task_tracker.spawn(async move {
                let _ = t.try_advance(i);
            });
        }
        task_tracker.close();
        task_tracker.wait().await;
        assert_eq!(tracker.current(), MAX_TARGET);
    }
}
