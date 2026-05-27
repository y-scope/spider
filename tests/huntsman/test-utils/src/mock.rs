//! In-process mock implementations of the execution manager's client traits.
//!
//! Each mock is `Clone` (internally `Arc`-backed) so the test body retains an inspection handle
//! while the runtime owns a clone. Response queues let the test drive deterministic call sequences;
//! inboxes record every call so assertions can be made.

use std::{
    collections::VecDeque,
    net::IpAddr,
    sync::{
        Arc,
        Mutex,
        MutexGuard,
        PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use spider_core::types::id::{ExecutionManagerId, SessionId};
use spider_execution_manager::client::{
    LivenessClient,
    LivenessResponseError,
    RegistrationResponse,
};
use tokio::sync::Notify;

/// Mock [`LivenessClient`].
#[derive(Clone)]
pub struct MockLiveness {
    inner: Arc<LivenessInner>,
}

impl MockLiveness {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A fresh liveness mock with a freshly generated `em_id`, initial session 1, and Ok(1)
    /// heartbeats by default.
    #[must_use]
    pub fn new() -> Self {
        Self::with_initial_session(1)
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// A fresh liveness mock with the given initial session id (used both for the registration
    /// response and as the default heartbeat reply).
    #[must_use]
    pub fn with_initial_session(initial_session: SessionId) -> Self {
        Self {
            inner: Arc::new(LivenessInner {
                em_id: ExecutionManagerId::new(),
                initial_session: AtomicU64::new(initial_session),
                register_response: Mutex::new(None),
                heartbeat_responses: Mutex::new(VecDeque::new()),
                default_session: AtomicU64::new(initial_session),
                register_calls: Mutex::new(Vec::new()),
                heartbeat_count: AtomicU64::new(0),
                heartbeat_notify: Notify::new(),
            }),
        }
    }

    /// Overrides the registration response. By default `register` returns
    /// `Ok(RegistrationResponse { em_id, session_id: initial_session })`.
    pub fn set_register_response(
        &self,
        response: Result<RegistrationResponse, LivenessResponseError>,
    ) {
        *lock(&self.inner.register_response) = Some(response);
    }

    /// Updates the fallback session id returned by `heartbeat` when the response queue is empty.
    pub fn set_default_heartbeat_session(&self, session: SessionId) {
        self.inner.default_session.store(session, Ordering::Relaxed);
    }

    /// Queues `response` for the next `heartbeat` call (takes priority over the default session).
    pub fn push_heartbeat_response(&self, response: Result<SessionId, LivenessResponseError>) {
        lock(&self.inner.heartbeat_responses).push_back(response);
    }

    /// # Returns
    ///
    /// The `em_id` baked into this mock — the same value the runtime sees through
    /// [`LivenessClient::register`].
    #[must_use]
    pub fn em_id(&self) -> ExecutionManagerId {
        self.inner.em_id
    }

    /// # Returns
    ///
    /// The number of `heartbeat` calls observed.
    #[must_use]
    pub fn heartbeat_count(&self) -> u64 {
        self.inner.heartbeat_count.load(Ordering::Relaxed)
    }

    /// # Returns
    ///
    /// The list of IPs passed to `register`.
    #[must_use]
    pub fn register_calls(&self) -> Vec<IpAddr> {
        lock(&self.inner.register_calls).clone()
    }

    /// Waits until at least `target` heartbeats have been observed, bounded by `timeout`.
    ///
    /// # Returns
    ///
    /// `true` if the threshold was reached, `false` if `timeout` elapsed first.
    pub async fn wait_for_heartbeats(&self, target: u64, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.heartbeat_count() >= target {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let notified = self.inner.heartbeat_notify.notified();
            tokio::select! {
                () = notified => {}
                () = tokio::time::sleep(remaining.min(POLL_INTERVAL)) => {}
            }
        }
    }
}

impl Default for MockLiveness {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LivenessClient for MockLiveness {
    async fn register(&self, ip: IpAddr) -> Result<RegistrationResponse, LivenessResponseError> {
        lock(&self.inner.register_calls).push(ip);
        let programmed = lock(&self.inner.register_response).take();
        if let Some(response) = programmed {
            return response;
        }
        Ok(RegistrationResponse {
            em_id: self.inner.em_id,
            session_id: self.inner.initial_session.load(Ordering::Relaxed),
        })
    }

    async fn heartbeat(
        &self,
        _em_id: ExecutionManagerId,
    ) -> Result<SessionId, LivenessResponseError> {
        self.inner.heartbeat_count.fetch_add(1, Ordering::Relaxed);
        self.inner.heartbeat_notify.notify_waiters();
        let queued = lock(&self.inner.heartbeat_responses).pop_front();
        queued.unwrap_or_else(|| Ok(self.inner.default_session.load(Ordering::Relaxed)))
    }
}

/// Default polling interval for `wait_until_*` helpers. Short enough to keep tests snappy.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Shared state behind [`MockLiveness`].
struct LivenessInner {
    em_id: ExecutionManagerId,
    initial_session: AtomicU64,
    register_response: Mutex<Option<Result<RegistrationResponse, LivenessResponseError>>>,
    heartbeat_responses: Mutex<VecDeque<Result<SessionId, LivenessResponseError>>>,
    default_session: AtomicU64,
    register_calls: Mutex<Vec<IpAddr>>,
    heartbeat_count: AtomicU64,
    heartbeat_notify: Notify,
}

/// Acquires `mutex`, silently recovering from poisoning so the helpers never panic from a peer
/// test's failure.
///
/// # Type Parameters
///
/// * `InnerType` - The type wrapped by `mutex`.
///
/// # Returns
///
/// A [`MutexGuard`] over `mutex`'s contents.
fn lock<InnerType>(mutex: &Mutex<InnerType>) -> MutexGuard<'_, InnerType> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
