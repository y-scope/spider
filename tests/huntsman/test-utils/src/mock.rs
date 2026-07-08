//! In-process mock implementations of the execution manager's client traits.
//!
//! Each mock is `Clone` (internally `Arc`-backed) so the test body retains an inspection handle
//! while the runtime owns a clone. Response queues let the test drive deterministic call sequences;
//! inboxes record every call so assertions can be made.

use std::collections::VecDeque;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashSet;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::ExecutionContext;
use spider_core::types::scheduler::TaskAssignmentRecord;
use spider_execution_manager::client::LivenessClient;
use spider_execution_manager::client::LivenessResponseError;
use spider_execution_manager::client::RegistrationResponse;
use spider_execution_manager::client::SchedulerClient;
use spider_execution_manager::client::SchedulerError;
use spider_execution_manager::client::SchedulerResponse;
use spider_execution_manager::client::StorageClient;
use spider_execution_manager::client::StorageResponseError;
use tokio::sync::Notify;

/// Mock [`SchedulerClient`].
#[derive(Clone)]
pub struct MockScheduler {
    inner: Arc<SchedulerInner>,
}

impl MockScheduler {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A fresh scheduler mock with an empty response queue. `next_task` blocks until the test
    /// pushes a response.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SchedulerInner {
                responses: Mutex::new(VecDeque::new()),
                notify: Notify::new(),
                call_count: AtomicU64::new(0),
                outstanding: DashSet::new(),
                heartbeat_count: AtomicU64::new(0),
            }),
        }
    }

    /// Queues `response` for the next pending or future [`SchedulerClient::next_task`] call.
    pub fn push(&self, response: Result<SchedulerResponse, SchedulerError>) {
        lock(&self.inner.responses).push_back(response);
        self.inner.notify.notify_waiters();
    }

    /// # Returns
    ///
    /// The number of `next_task` calls the scheduler has served (including ones that are still
    /// blocked waiting on the response queue).
    #[must_use]
    pub fn call_count(&self) -> u64 {
        self.inner.call_count.load(Ordering::Relaxed)
    }

    /// # Returns
    ///
    /// A snapshot of the assignment records that have been handed out but not yet checked off by a
    /// `prev_assignment` acknowledgement. An empty snapshot means the runtime has reported every
    /// assignment it was given as consumed.
    #[must_use]
    pub fn outstanding(&self) -> Vec<TaskAssignmentRecord> {
        self.inner.outstanding.iter().map(|r| *r).collect()
    }

    /// # Returns
    ///
    /// The number of `heartbeat` calls observed.
    #[must_use]
    pub fn heartbeat_count(&self) -> u64 {
        self.inner.heartbeat_count.load(Ordering::Relaxed)
    }
}

impl Default for MockScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchedulerClient for MockScheduler {
    async fn next_task(
        &self,
        _em_id: ExecutionManagerId,
        prev_assignment: Option<TaskAssignmentRecord>,
        _wait_time_ms: u64,
    ) -> Result<SchedulerResponse, SchedulerError> {
        if let Some(record) = prev_assignment {
            self.inner.outstanding.remove(&record);
        }
        self.inner.call_count.fetch_add(1, Ordering::Relaxed);
        loop {
            let notified = self.inner.notify.notified();
            let popped = lock(&self.inner.responses).pop_front();
            if let Some(response) = popped {
                // Track every assignment handed out so the test can confirm the runtime later
                // reports it as consumed.
                if let Ok(assignment) = &response {
                    self.inner.outstanding.insert(TaskAssignmentRecord::new(
                        assignment.task_assignment.id,
                        assignment.scheduler_id,
                    ));
                }
                return response;
            }
            notified.await;
        }
    }

    async fn heartbeat(&self, _em_id: ExecutionManagerId) -> Result<(), SchedulerError> {
        self.inner.heartbeat_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn shutdown(
        &self,
        _em_id: ExecutionManagerId,
        prev_assignments: Vec<TaskAssignmentRecord>,
    ) {
        for record in prev_assignments {
            self.inner.outstanding.remove(&record);
        }
    }
}

/// Captured arguments of one `register_task_instance` call.
#[derive(Debug, Clone)]
pub struct RegisterCall {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub em_id: ExecutionManagerId,
    pub session_id: SessionId,
}

/// Captured arguments of one `report_task_success` call.
#[derive(Debug, Clone)]
pub struct SuccessReport {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub task_instance_id: TaskInstanceId,
    pub em_id: ExecutionManagerId,
    pub session_id: SessionId,
    pub serialized_outputs: Option<Vec<u8>>,
}

/// Captured arguments of one `report_task_failure` call.
#[derive(Debug, Clone)]
pub struct FailureReport {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub task_instance_id: TaskInstanceId,
    pub em_id: ExecutionManagerId,
    pub session_id: SessionId,
    pub error_message: String,
}

/// Mock [`StorageClient`].
#[derive(Clone)]
pub struct MockStorage {
    inner: Arc<StorageInner>,
}

impl MockStorage {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A storage mock with no programmed responses. Tests must push register responses before
    /// they fire; success / failure reports default to `Ok(())`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(StorageInner {
                register_responses: Mutex::new(VecDeque::new()),
                success_responses: Mutex::new(VecDeque::new()),
                failure_responses: Mutex::new(VecDeque::new()),
                register_calls: Mutex::new(Vec::new()),
                success_reports: Mutex::new(Vec::new()),
                failure_reports: Mutex::new(Vec::new()),
                notify: Notify::new(),
                block_register: AtomicBool::new(false),
            }),
        }
    }

    /// Queues `response` for the next `register_task_instance` call.
    pub fn push_register_response(&self, response: Result<ExecutionContext, StorageResponseError>) {
        lock(&self.inner.register_responses).push_back(response);
    }

    /// Makes every subsequent `register_task_instance` call record its arguments and then park
    /// indefinitely instead of returning. A test can use this to cancel the runtime while a
    /// registration is in flight.
    pub fn block_register(&self) {
        self.inner.block_register.store(true, Ordering::Relaxed);
    }

    /// Queues `response` for the next `report_task_success` call.
    pub fn push_success_response(&self, response: Result<(), StorageResponseError>) {
        lock(&self.inner.success_responses).push_back(response);
    }

    /// Queues `response` for the next `report_task_failure` call.
    pub fn push_failure_response(&self, response: Result<(), StorageResponseError>) {
        lock(&self.inner.failure_responses).push_back(response);
    }

    /// # Returns
    ///
    /// A snapshot of every `register_task_instance` call recorded so far.
    #[must_use]
    pub fn register_calls(&self) -> Vec<RegisterCall> {
        lock(&self.inner.register_calls).clone()
    }

    /// # Returns
    ///
    /// A snapshot of every `report_task_success` call recorded so far.
    #[must_use]
    pub fn success_reports(&self) -> Vec<SuccessReport> {
        lock(&self.inner.success_reports).clone()
    }

    /// # Returns
    ///
    /// A snapshot of every `report_task_failure` call recorded so far.
    #[must_use]
    pub fn failure_reports(&self) -> Vec<FailureReport> {
        lock(&self.inner.failure_reports).clone()
    }

    /// Waits for at least one `report_*` call to be recorded, with a bounded total wait time.
    ///
    /// # Returns
    ///
    /// Whether a report was observed before `timeout` elapsed.
    pub async fn wait_for_any_report(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if !self.success_reports().is_empty() || !self.failure_reports().is_empty() {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let notified = self.inner.notify.notified();
            tokio::select! {
                () = notified => {}
                () = tokio::time::sleep(remaining.min(POLL_INTERVAL)) => {}
            }
        }
    }
}

impl Default for MockStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageClient for MockStorage {
    async fn register_task_instance(
        &self,
        job_id: JobId,
        task_id: TaskId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
    ) -> Result<ExecutionContext, StorageResponseError> {
        lock(&self.inner.register_calls).push(RegisterCall {
            job_id,
            task_id,
            em_id,
            session_id,
        });
        if self.inner.block_register.load(Ordering::Relaxed) {
            // Park until this future is dropped (e.g. the runtime observes cancellation).
            std::future::pending::<()>().await;
        }
        let response = lock(&self.inner.register_responses).pop_front();
        response.expect("mock storage exhausted register responses")
    }

    async fn report_task_success(
        &self,
        job_id: JobId,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
        serialized_outputs: Option<Vec<u8>>,
    ) -> Result<(), StorageResponseError> {
        lock(&self.inner.success_reports).push(SuccessReport {
            job_id,
            task_id,
            task_instance_id,
            em_id,
            session_id,
            serialized_outputs,
        });
        self.inner.notify.notify_waiters();
        lock(&self.inner.success_responses)
            .pop_front()
            .unwrap_or(Ok(()))
    }

    async fn report_task_failure(
        &self,
        job_id: JobId,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        em_id: ExecutionManagerId,
        session_id: SessionId,
        error_message: String,
    ) -> Result<(), StorageResponseError> {
        lock(&self.inner.failure_reports).push(FailureReport {
            job_id,
            task_id,
            task_instance_id,
            em_id,
            session_id,
            error_message,
        });
        self.inner.notify.notify_waiters();
        lock(&self.inner.failure_responses)
            .pop_front()
            .unwrap_or(Ok(()))
    }
}

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
                em_id: ExecutionManagerId::random(),
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

/// Shared state behind [`MockScheduler`].
struct SchedulerInner {
    responses: Mutex<VecDeque<Result<SchedulerResponse, SchedulerError>>>,
    notify: Notify,
    call_count: AtomicU64,
    /// Records of the assignments handed out by `next_task` that have not yet been checked off by
    /// a `prev_assignment` acknowledgement. An empty set on exit means the runtime reported every
    /// assignment it was given as consumed.
    outstanding: DashSet<TaskAssignmentRecord>,
    heartbeat_count: AtomicU64,
}

/// Shared state behind [`MockStorage`].
struct StorageInner {
    register_responses: Mutex<VecDeque<Result<ExecutionContext, StorageResponseError>>>,
    success_responses: Mutex<VecDeque<Result<(), StorageResponseError>>>,
    failure_responses: Mutex<VecDeque<Result<(), StorageResponseError>>>,
    register_calls: Mutex<Vec<RegisterCall>>,
    success_reports: Mutex<Vec<SuccessReport>>,
    failure_reports: Mutex<Vec<FailureReport>>,
    notify: Notify,
    /// When set, `register_task_instance` parks forever (after recording the call) instead of
    /// returning. Lets a test cancel the runtime while a registration is in flight.
    block_register: AtomicBool,
}

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
