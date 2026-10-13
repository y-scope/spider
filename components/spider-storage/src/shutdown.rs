//! Shutdown signalling for the storage service, which distinguishes a fatal shutdown from a
//! graceful one.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio_util::sync::CancellationToken;

/// A cloneable handle to the storage service's cancellation token and its fatal flag.
#[derive(Clone, Debug)]
pub struct ShutdownHandle {
    token: CancellationToken,
    fatal: Arc<AtomicBool>,
}

impl ShutdownHandle {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created [`ShutdownHandle`] over `token`, with the fatal flag unset.
    #[must_use]
    pub fn new(token: CancellationToken) -> Self {
        Self {
            token,
            fatal: Arc::new(AtomicBool::new(false)),
        }
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
    /// The cancellation token that this handle cancels.
    #[must_use]
    pub const fn token(&self) -> &CancellationToken {
        &self.token
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::sync::CancellationToken;

    use super::ShutdownHandle;

    #[test]
    fn shutting_down_fatally_sets_the_flag_and_cancels_the_token() {
        let token = CancellationToken::new();
        let handle = ShutdownHandle::new(token.clone());
        let clone = handle.clone();

        clone.shut_down_fatally();

        assert!(handle.is_fatal());
        assert!(handle.token().is_cancelled());
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancelling_the_token_is_not_fatal() {
        let token = CancellationToken::new();
        let handle = ShutdownHandle::new(token.clone());

        token.cancel();

        assert!(handle.token().is_cancelled());
        assert!(!handle.is_fatal());
    }
}
