//! Runtime — the execution manager's main loop.
//!
//! The runtime owns the long-lived state that drives task execution on one node: the
//! [`ProcessPool`] of `spider-task-executor` subprocesses, the shared [`SessionTracker`], and the
//! handle to the liveness actor. Its [`Runtime::run`] loop pulls one task assignment at a time from
//! the scheduler, registers the task instance with storage, dispatches it to the pool, and reports
//! the outcome back to storage.
//!
//! Shutdown is driven by a shared [`CancellationToken`]: the liveness actor flips it when storage
//! reaps the execution manager (or rejects its id), and the main loop selects on it so a reap
//! promptly tears the runtime down.

use std::{net::IpAddr, path::PathBuf, sync::Arc, time::Duration};

use spider_core::{session::SessionTracker, types::id::ExecutionManagerId};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    client::{LivenessClient, LivenessResponseError, SchedulerClient, StorageClient},
    liveness::{self, LivenessHandle},
    process_pool::{self, ProcessPool, ProcessPoolConfig},
};

/// Static configuration for a [`Runtime`]. Supplied once at bootstrap and never mutated.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// IP address advertised to storage at registration.
    pub em_ip: IpAddr,

    /// Interval between liveness heartbeats. Handed verbatim to the liveness actor.
    pub heartbeat_interval: Duration,

    /// Absolute path to the `spider-task-executor` binary the process pool spawns.
    pub executor_binary_path: PathBuf,

    /// Directory of TDL packages exposed to executors via `SPIDER_TDL_PACKAGE_DIR`.
    pub package_dir: PathBuf,

    /// Directory the process pool writes per-executor stderr logs into.
    pub log_dir: PathBuf,

    /// Maximum number of times a storage call that fails with a transport error is retried before
    /// the runtime gives up on the current task.
    pub storage_max_retries: u32,

    /// Base delay used when backing off after a transport error from the scheduler or storage.
    pub transport_backoff: Duration,
}

/// Errors returned while bootstrapping a [`Runtime`].
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// Boot-time registration with storage failed.
    #[error("failed to register with storage: {0}")]
    Registration(#[from] LivenessResponseError),

    /// The initial process pool could not be created.
    #[error("failed to create the process pool: {0}")]
    ProcessPool(#[from] process_pool::InternalError),
}

/// The execution manager runtime: the main loop plus all the state it owns.
///
/// # Type Parameters
///
/// * `SchedulerClientType` - Concrete [`SchedulerClient`] the main loop pulls task assignments
///   from.
/// * `StorageClientType` - Concrete [`StorageClient`] used to register task instances and report
///   their outcome.
/// * `LivenessClientType` - Concrete [`LivenessClient`] used to register at boot and, through the
///   spawned liveness actor, heartbeat thereafter.
pub struct Runtime<
    SchedulerClientType: SchedulerClient,
    StorageClientType: StorageClient,
    LivenessClientType: LivenessClient + 'static,
> {
    em_id: ExecutionManagerId,
    scheduler_client: Arc<SchedulerClientType>,
    storage_client: Arc<StorageClientType>,
    liveness_client: Arc<LivenessClientType>,
    process_pool: ProcessPool,
    session_tracker: SessionTracker,
    liveness_handle: LivenessHandle,
    liveness_join: JoinHandle<()>,
    cancellation_token: CancellationToken,
    config: RuntimeConfig,
}

impl<
    SchedulerClientType: SchedulerClient,
    StorageClientType: StorageClient,
    LivenessClientType: LivenessClient + 'static,
> Runtime<SchedulerClientType, StorageClientType, LivenessClientType>
{
    /// Factory function.
    ///
    /// Registers the execution manager with storage, seeds the [`SessionTracker`] with the session
    /// id returned by registration, spawns the initial executor [`ProcessPool`] and the liveness
    /// actor, then assembles a ready-to-run runtime. The liveness actor is heartbeating by the time
    /// this returns.
    ///
    /// # Returns
    ///
    /// A fully wired [`Runtime`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`RuntimeError::Registration`] if boot-time registration with storage failed.
    /// * [`RuntimeError::ProcessPool`] if the initial executor process pool could not be spawned.
    /// * Forwards [`LivenessClient::register`]'s return values on failure.
    /// * Forwards [`ProcessPool::new`]'s return values on failure.
    pub async fn create(
        scheduler_client: Arc<SchedulerClientType>,
        storage_client: Arc<StorageClientType>,
        liveness_client: Arc<LivenessClientType>,
        config: RuntimeConfig,
    ) -> Result<Self, RuntimeError> {
        let registration = liveness_client.register(config.em_ip).await?;
        let em_id = registration.em_id;
        let session_tracker = SessionTracker::new(registration.session_id);
        tracing::info!(
            em_id = %em_id.as_uuid_ref(),
            session_id = registration.session_id,
            "Execution manager registered with storage."
        );

        let process_pool = ProcessPool::new(ProcessPoolConfig {
            em_id,
            executor_binary_path: config.executor_binary_path.clone(),
            package_dir: config.package_dir.clone(),
            log_dir: config.log_dir.clone(),
        })?;

        let cancellation_token = CancellationToken::new();
        let (liveness_handle, liveness_join) = liveness::spawn(
            em_id,
            Arc::clone(&liveness_client),
            session_tracker.clone(),
            cancellation_token.clone(),
            config.heartbeat_interval,
        );

        Ok(Self {
            em_id,
            scheduler_client,
            storage_client,
            liveness_client,
            process_pool,
            session_tracker,
            liveness_handle,
            liveness_join,
            cancellation_token,
            config,
        })
    }

    /// Runs the main loop until the runtime is cancelled, then tears it down.
    ///
    /// Each iteration pulls one task assignment, registers it, executes it, and reports the
    /// outcome. The loop selects every iteration against the shared [`CancellationToken`] so a reap
    /// signalled by the liveness actor exits promptly.
    pub async fn run(self) {
        tracing::info!(em_id = %self.em_id.as_uuid_ref(), "Runtime main loop starting.");

        // TODO(next step): the main loop body. Each iteration, raced against cancellation:
        //   1. `self.scheduler_client.next_task(self.em_id)` to pull an assignment.
        //   2. Local stale-session triage against `self.session_tracker`, nudging the liveness
        //      actor via `self.liveness_handle.refresh()` when the bundle's session is ahead.
        //   3. `self.storage_client.register_task_instance(..)` -> `ExecutionContext`.
        //   4. `self.process_pool.execute(ExecuteRequest { .. }, hard_timeout)`.
        //   5. Report success/failure back to storage with the bundle's pinned session id.
        // Until that lands, just wait for the shutdown signal so the actor and pool stay alive.
        self.cancellation_token.cancelled().await;

        tracing::info!(em_id = %self.em_id.as_uuid_ref(), "Runtime cancelled; shutting down.");
        // TODO(next step): drain the process pool before exit. For now, dropping `self` kills the
        // pooled executor via `kill_on_drop`.
        if let Err(err) = self.liveness_join.await {
            tracing::warn!(err = ?err, "Liveness actor task did not exit cleanly.");
        }
    }
}
