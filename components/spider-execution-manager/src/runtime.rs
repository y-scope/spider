//! Runtime — the execution manager's main loop.

use std::{net::IpAddr, path::PathBuf, sync::Arc, time::Duration};

use spider_core::{
    session::SessionTracker,
    types::{
        id::{ExecutionManagerId, JobId, SessionId, TaskId, TaskInstanceId},
        io::ExecutionContext,
    },
};
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::{
    client::{
        LivenessClient,
        LivenessResponseError,
        SchedulerClient,
        SchedulerResponse,
        StorageClient,
        StorageResponseError,
    },
    liveness::{self, LivenessHandle},
    process_pool::{self, ExecuteRequest, Outcome, ProcessPool, ProcessPoolConfig},
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
}

/// Errors returned by [`Runtime`] during bootstrap or the main loop.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// Boot-time registration with storage failed.
    #[error("failed to register with storage: {0}")]
    Registration(#[from] LivenessResponseError),

    /// The initial process pool could not be created.
    #[error("failed to create the process pool: {0}")]
    ProcessPool(#[from] process_pool::InternalError),

    /// Storage rejected a request as malformed. Indicates a contract bug in the runtime, not a
    /// transient condition, so the runtime treats it as fatal.
    #[error("storage rejected request as invalid: {0}")]
    StorageInvalidInput(String),
}

/// The execution manager runtime: the main loop plus all the state it owns.
///
/// # Type Parameters
///
/// * `SchedulerClientType` - Concrete [`SchedulerClient`] the main loop pulls task assignments
///   from.
/// * `StorageClientType` - Concrete [`StorageClient`] used to register task instances and report
///   their outcome.
pub struct Runtime<
    SchedulerClientType: SchedulerClient + Clone,
    StorageClientType: StorageClient + Clone + 'static,
> {
    em_id: ExecutionManagerId,
    scheduler_client: SchedulerClientType,
    storage_client: StorageClientType,
    process_pool: ProcessPool,
    session_tracker: SessionTracker,
    liveness_handle: LivenessHandle,
    liveness_join: JoinHandle<()>,
    cancellation_token: CancellationToken,
    _cancel_guard: DropGuard,
}

impl<
    SchedulerClientType: SchedulerClient + Clone,
    StorageClientType: StorageClient + Clone + 'static,
> Runtime<SchedulerClientType, StorageClientType>
{
    /// Factory function.
    ///
    /// Registers the execution manager with storage, seeds the [`SessionTracker`] with the session
    /// ID returned by registration, spawns the initial executor [`ProcessPool`] and the liveness
    /// actor, then assembles a ready-to-run runtime. The liveness actor sends the first heartbeat
    /// by the time this returns.
    ///
    /// # Type Parameters
    ///
    /// * `LivenessClientType` - Concrete [`LivenessClient`] used to register at boot and, through
    ///   the spawned liveness actor, heartbeat thereafter.
    ///
    /// # Returns
    ///
    /// A tuple on success, containing:
    ///
    /// * The created [`Runtime`] instance, ready to run.
    /// * The [`CancellationToken`] that the caller can use to request shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`LivenessClient::register`]'s return values on failure.
    /// * Forwards [`ProcessPool::new`]'s return values on failure.
    pub async fn create<LivenessClientType: LivenessClient + 'static>(
        scheduler_client: SchedulerClientType,
        storage_client: StorageClientType,
        liveness_client: Arc<LivenessClientType>,
        config: RuntimeConfig,
    ) -> Result<(Self, CancellationToken), RuntimeError> {
        let registration = liveness_client.register(config.em_ip).await?;
        let em_id = registration.em_id;
        let session_tracker = SessionTracker::new(registration.session_id);
        tracing::info!(
            em_id = ? em_id,
            session_id = registration.session_id,
            "Execution manager registered with storage."
        );

        let process_pool = ProcessPool::new(ProcessPoolConfig {
            em_id,
            executor_binary_path: config.executor_binary_path,
            package_dir: config.package_dir,
            log_dir: config.log_dir,
        })?;

        let cancellation_token = CancellationToken::new();
        let (liveness_handle, liveness_join) = liveness::spawn(
            em_id,
            liveness_client,
            session_tracker.clone(),
            cancellation_token.clone(),
            config.heartbeat_interval,
        );

        let cancel_guard = cancellation_token.clone().drop_guard();
        let runtime = Self {
            em_id,
            scheduler_client,
            storage_client,
            process_pool,
            session_tracker,
            liveness_handle,
            liveness_join,
            cancellation_token: cancellation_token.clone(),
            _cancel_guard: cancel_guard,
        };
        Ok((runtime, cancellation_token))
    }

    /// Runs the main loop until the runtime is cancelled, then tears it down.
    ///
    /// # Returns
    ///
    /// `Ok(())` after a clean shutdown triggered by cancellation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::main_loop`]'s return values on failure.
    pub async fn run(self) -> Result<(), RuntimeError> {
        tracing::info!(em_id = ? self.em_id, "Runtime main loop starting.");
        let result = self.main_loop().await;
        tracing::info!(em_id = ? self.em_id, "Runtime main loop exited. Shutting down.");
        self.cancellation_token.cancel();
        if let Err(err) = self.liveness_join.await {
            tracing::warn!(err = ? err, "Liveness actor task did not exit cleanly.");
        }
        result
    }

    /// Iterates the main loop. Each iteration pulls a task assignment from the scheduler and runs
    /// it through the local pipeline. Returns when the runtime is cancelled or a fatal error
    /// occurs.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the loop exits cleanly because the runtime was cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::register_task_instance`]'s return values on failure.
    /// * Forwards [`ProcessPool::execute`]'s return values on failure.
    async fn main_loop(&self) -> Result<(), RuntimeError> {
        loop {
            let assignment = tokio::select! {
                biased;
                () = self.cancellation_token.cancelled() => return Ok(()),
                result = self.scheduler_client.next_task(self.em_id) => {
                    match result {
                        Ok(assignment) => assignment,
                        Err(e) => {
                            tracing::warn!(err = ? e, "Scheduler returned an error. Retrying.");
                            continue;
                        }
                    }
                }
            };

            tracing::info!(
                bundle_session = assignment.session_id,
                job_id = ? assignment.job_id,
                task_id = ? assignment.task_id,
                "Received a new task assignment from the scheduler."
            );

            let current_session = self.session_tracker.current();
            if assignment.session_id < current_session {
                tracing::warn!(
                    bundle_session = assignment.session_id,
                    current_session,
                    job_id = ? assignment.job_id,
                    task_id = ? assignment.task_id,
                    "Dropping stale task assignment from the scheduler."
                );
                continue;
            }
            if assignment.session_id > current_session {
                tracing::info!(
                    new_session = assignment.session_id,
                    "Observed a newer session via the scheduler. Refreshing liveness."
                );
                self.liveness_handle.refresh().await;
            }

            let Some(execution_context) = self.register_task_instance(assignment).await? else {
                continue;
            };
            let task_instance_id = execution_context.task_instance_id;

            let hard_timeout =
                Duration::from_millis(execution_context.timeout_policy.hard_timeout_ms);
            let request = ExecuteRequest {
                job_id: assignment.job_id,
                task_id: assignment.task_id,
                resource_group_id: assignment.resource_group_id,
                ctx: execution_context,
            };
            let outcome = self
                .process_pool
                .execute(request, hard_timeout)
                .await
                .inspect_err(|err| {
                    tracing::error!(
                        err = ? err,
                        job_id = ? assignment.job_id,
                        task_id = ? assignment.task_id,
                        "Process pool failed to dispatch task. Bailing out."
                    );
                })?;

            let current_session = self.session_tracker.current();
            if assignment.session_id < current_session {
                tracing::warn!(
                    bundle_session = assignment.session_id,
                    current_session,
                    job_id = ? assignment.job_id,
                    task_id = ? assignment.task_id,
                    "Dropping stale task assignment's outcome."
                );
                continue;
            }

            // Fire-and-forget the outcome report so the main loop can dispatch the next task
            // without waiting on storage. Errors are logged inside `report_outcome`.
            tokio::spawn(report_outcome(
                self.storage_client.clone(),
                ReportTarget {
                    em: self.em_id,
                    job: assignment.job_id,
                    task: assignment.task_id,
                    task_instance_id,
                    session: assignment.session_id,
                },
                outcome,
            ));
        }
    }

    /// Registers a task instance with storage.
    ///
    /// Races the storage call against [`Self::cancellation_token`]: when it fires, the method
    /// returns `Ok(None)` and the next [`Self::main_loop`] iteration observes the token via its
    /// top-level [`tokio::select!`] and exits.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(execution_context))` if storage accepted the registration.
    /// * `Ok(None)` if the assignment should be skipped (stale session, transport failure, any
    ///   other recoverable storage error, or cancellation mid-call).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`RuntimeError::StorageInvalidInput`] if storage rejects the request as malformed, which
    ///   the runtime treats as fatal.
    async fn register_task_instance(
        &self,
        assignment: SchedulerResponse,
    ) -> Result<Option<ExecutionContext>, RuntimeError> {
        let register_result = tokio::select! {
            biased;
            () = self.cancellation_token.cancelled() => return Ok(None),
            result = self.storage_client.register_task_instance(
                assignment.job_id,
                assignment.task_id,
                self.em_id,
                assignment.session_id,
            ) => result,
        };

        match register_result {
            Ok(execution_context) => Ok(Some(execution_context)),
            Err(StorageResponseError::StaleSession { storage_session }) => {
                tracing::warn!(
                    bundle_session = assignment.session_id,
                    storage_session = storage_session,
                    job_id = ? assignment.job_id,
                    task_id = ? assignment.task_id,
                    "Storage rejected task registration as stale. Dropping the assignment."
                );
                self.liveness_handle.refresh().await;
                Ok(None)
            }
            Err(StorageResponseError::InvalidInput(err)) => {
                tracing::error!(
                    err = % err,
                    job_id = ? assignment.job_id,
                    task_id = ? assignment.task_id,
                    "Storage rejected task registration as malformed. Bailing out."
                );
                Err(RuntimeError::StorageInvalidInput(err))
            }
            Err(err) => {
                tracing::warn!(
                    err = ? err,
                    job_id = ? assignment.job_id,
                    task_id = ? assignment.task_id,
                    "Storage rejected task registration. Dropping the assignment."
                );
                Ok(None)
            }
        }
    }
}

/// Identifies a single task-instance attempt that an outcome report belongs to.
#[derive(Debug, Clone, Copy)]
struct ReportTarget {
    em: ExecutionManagerId,
    job: JobId,
    task: TaskId,
    task_instance_id: TaskInstanceId,
    session: SessionId,
}

/// A task outcome prepared for transmission to storage. Splits the storage API's two reporting
/// endpoints (success / failure) and carries their payloads.
enum Report {
    Success(Option<Vec<u8>>),
    Failure(String),
}

impl Report {
    /// # Returns
    ///
    /// The constructed report from the task executor's outcome.
    fn from_outcome(outcome: Outcome, target: ReportTarget) -> Self {
        match outcome {
            Outcome::Success {
                outputs,
                elapsed_us,
            } => {
                tracing::info!(
                    job_id = ? target.job,
                    task_id = ? target.task,
                    elapsed_us,
                    "Task completed successfully."
                );
                Self::Success(Some(outputs))
            }
            Outcome::InTaskFailure { error, elapsed_us } => {
                tracing::info!(
                    job_id = ? target.job,
                    task_id = ? target.task,
                    elapsed_us,
                    "Task reported an in-task failure."
                );
                Self::Failure(format!(
                    "in-task failure: {}",
                    String::from_utf8_lossy(&error)
                ))
            }
            Outcome::Timeout { hard_timeout } => {
                tracing::warn!(
                    job_id = ? target.job,
                    task_id = ? target.task,
                    hard_timeout_ms = ?hard_timeout.as_millis(),
                    "Task hit the hard timeout."
                );
                Self::Failure(format!(
                    "hard timeout ({} ms) exceeded",
                    hard_timeout.as_millis()
                ))
            }
            Outcome::ExecutorCrash { exit_status } => {
                tracing::warn!(
                    job_id = ? target.job,
                    task_id = ? target.task,
                    exit_status = ?exit_status,
                    "Task executor crashed."
                );
                Self::Failure(format!("executor crashed (exit_status = {exit_status:?})"))
            }
        }
    }

    /// Consumes `self` and sends it to storage via the matching reporting endpoint.
    ///
    /// # Type Parameters
    ///
    /// * `StorageClientType` - Concrete [`StorageClient`] the report is sent through.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`StorageClient::report_task_success`]'s return values on failure.
    /// * Forwards [`StorageClient::report_task_failure`]'s return values on failure.
    async fn send<StorageClientType: StorageClient>(
        self,
        storage_client: &StorageClientType,
        target: ReportTarget,
    ) -> Result<(), StorageResponseError> {
        let ReportTarget {
            em,
            job,
            task,
            task_instance_id,
            session,
        } = target;
        match self {
            Self::Success(outputs) => {
                storage_client
                    .report_task_success(job, task, task_instance_id, em, session, outputs)
                    .await
            }
            Self::Failure(message) => {
                storage_client
                    .report_task_failure(job, task, task_instance_id, em, session, message)
                    .await
            }
        }
    }
}

/// Reports a single task outcome to storage. Designed to run as a detached background task spawned
/// by [`Runtime::main_loop`] so reporting overlaps with the next round of task dispatching; errors
/// are logged rather than propagated.
///
/// # Type Parameters
///
/// * `StorageClientType` - Concrete [`StorageClient`] the report is sent through.
async fn report_outcome<StorageClientType: StorageClient + 'static>(
    storage_client: StorageClientType,
    target: ReportTarget,
    outcome: Outcome,
) {
    let report = Report::from_outcome(outcome, target);
    let _ = report
        .send(&storage_client, target)
        .await
        .inspect_err(|err| {
            tracing::error!(
                err = ? err,
                job_id = ? target.job,
                task_id = ? target.task,
                "Failed to report task outcome to storage. Dropping the report."
            );
        });
}
