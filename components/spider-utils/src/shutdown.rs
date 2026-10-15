//! Shutdown signalling for a service, which distinguishes a fatal shutdown from a graceful one.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio_util::sync::CancellationToken;

/// A cloneable handle to a service's cancellation token and its fatal flag.
#[derive(Clone, Debug, Default)]
pub struct ShutdownHandle {
    token: CancellationToken,
    fatal: Arc<AtomicBool>,
}

impl ShutdownHandle {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created [`ShutdownHandle`] with a new cancellation token and the fatal flag unset.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancels the token without marking the shutdown as fatal.
    pub fn shut_down_gracefully(&self) {
        self.token.cancel();
    }

    /// Marks the shutdown as fatal, then cancels the token.
    pub fn shut_down_fatally(&self) {
        self.fatal.store(true, Ordering::Release);
        self.token.cancel();
    }

    /// # Returns
    ///
    /// Whether [`Self::shut_down_fatally`] has been called on this handle or on any of its clones.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        self.fatal.load(Ordering::Acquire)
    }

    /// # Returns
    ///
    /// Whether the cancellation token has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Waits until the cancellation token is cancelled.
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }
}

#[cfg(test)]
mod tests {
    use super::ShutdownHandle;

    #[test]
    fn shutting_down_fatally_sets_the_flag_and_cancels_the_token() {
        let handle = ShutdownHandle::new();
        let clone = handle.clone();

        clone.shut_down_fatally();

        assert!(handle.is_fatal());
        assert!(handle.is_cancelled());
    }

    #[test]
    fn shutting_down_gracefully_is_not_fatal() {
        let handle = ShutdownHandle::new();

        handle.shut_down_gracefully();

        assert!(handle.is_cancelled());
        assert!(!handle.is_fatal());
    }
}
