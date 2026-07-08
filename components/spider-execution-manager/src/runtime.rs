//! Runtime — the execution manager's main loop.

use std::collections::VecDeque;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use spider_core::session::SessionTracker;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::ExecutionContext;
use spider_core::types::scheduler::TaskAssignmentRecord;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::DropGuard;

use crate::client::LivenessClient;
use crate::client::LivenessResponseError;
use crate::client::SchedulerClient;
use crate::client::SchedulerResponse;
use crate::client::StorageClient;
use crate::client::StorageResponseError;
use crate::liveness::LivenessHandle;
use crate::liveness::{self};
use crate::process_pool::ExecuteRequest;
use crate::process_pool::Outcome;
use crate::process_pool::ProcessPool;
use crate::process_pool::ProcessPoolConfig;
use crate::process_pool::{self};

/// Static configuration for a [`Runtime`]. Supplied once at bootstrap and never mutated.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// IP address advertised to storage at registration.
    pub em_ip: IpAddr,

    /// Interval between liveness heartbeats. Handed verbatim to the liveness actor.
    pub heartbeat_interval: Duration,

    /// Interval between scheduler heartbeats. Handed verbatim to the scheduler heartbeat task.
    pub scheduler_heartbeat_interval: Duration,

    /// How long, in milliseconds, the scheduler is asked to block each
    /// [`SchedulerClient::next_task`] long poll before returning `NoTask`. Passed verbatim to
    /// the scheduler gRPC as `wait_time_ms`.
    pub scheduler_poll_wait_ms: u64,

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

    /// Storage response error.
    #[error(transparent)]
    StorageResponse(#[from] StorageResponseError),
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
    SchedulerClientType: SchedulerClient + Clone + 'static,
    StorageClientType: StorageClient + Clone + 'static,
> {
    em_id: ExecutionManagerId,
    scheduler_client: SchedulerClientType,
    storage_client: StorageClientType,
    process_pool: ProcessPool,
    session_tracker: SessionTracker,
    liveness_handle: LivenessHandle,
    liveness_join: JoinHandle<()>,
    scheduler_heartbeat_join: JoinHandle<()>,
    scheduler_poll_wait_ms: u64,
    prev_assignments: VecDeque<TaskAssignmentRecord>,
    cancellation_token: CancellationToken,
    _cancel_guard: DropGuard,
}

impl<
    SchedulerClientType: SchedulerClient + Clone + 'static,
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
    pub async fn create<LivenessClientType: LivenessClient + Clone + 'static>(
        scheduler_client: SchedulerClientType,
        storage_client: StorageClientType,
        liveness_client: LivenessClientType,
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

        let scheduler_heartbeat_client = scheduler_client.clone();
        let scheduler_heartbeat_interval = config.scheduler_heartbeat_interval;
        let scheduler_heartbeat_cancellation_token = cancellation_token.child_token();
        let scheduler_heartbeat_join = tokio::spawn(async move {
            let mut interval = tokio::time::interval(scheduler_heartbeat_interval);
            loop {
                tokio::select! {
                    () = scheduler_heartbeat_cancellation_token.cancelled() => {
                        tracing::info!(em_id = ? em_id, "Scheduler heartbeat task cancelled.");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = scheduler_heartbeat_client.heartbeat(em_id).await {
                            tracing::warn!(
                                em_id = ? em_id,
                                error = ? e,
                                "Failed to heartbeat to scheduler."
                            );
                            // Will continue to try on the next tick (if the runtime is not
                            // cancelled).
                        }
                    }
                }
            }
        });

        let cancel_guard = cancellation_token.clone().drop_guard();
        let runtime = Self {
            em_id,
            scheduler_client,
            storage_client,
            process_pool,
            session_tracker,
            liveness_handle,
            liveness_join,
            scheduler_heartbeat_join,
            scheduler_poll_wait_ms: config.scheduler_poll_wait_ms,
            prev_assignments: VecDeque::new(),
            cancellation_token: cancellation_token.clone(),
            _cancel_guard: cancel_guard,
        };
        Ok((runtime, cancellation_token))
    }

    /// # Returns
    ///
    /// The ID of the registered execution manager.
    pub const fn get_em_id(&self) -> ExecutionManagerId {
        self.em_id
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
    pub async fn run(mut self) -> Result<(), RuntimeError> {
        tracing::info!(em_id = ? self.em_id, "Runtime main loop starting.");
        let result = self.main_loop().await;

        tracing::info!(em_id = ? self.em_id, "Runtime main loop exited. Shutting down.");
        self.cancellation_token.cancel();

        let join_liveness_actor = async {
            match self.liveness_join.await {
                Ok(()) => {
                    tracing::info!("Liveness actor stopped.");
                }
                Err(e) => {
                    tracing::error!(error = ? e, "Liveness actor exited on panic.");
                }
            }
        };

        let join_scheduler_heartbeat = async {
            match self.scheduler_heartbeat_join.await {
                Ok(()) => {
                    tracing::info!("Scheduler heartbeat task stopped.");
                }
                Err(e) => {
                    tracing::error!(error = ? e, "Scheduler heartbeat task exited on panic.");
                }
            }
        };

        tokio::join!(
            join_liveness_actor,
            join_scheduler_heartbeat,
            self.scheduler_client
                .shutdown(self.em_id, self.prev_assignments.into())
        );

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
    async fn main_loop(&mut self) -> Result<(), RuntimeError> {
        loop {
            let response = tokio::select! {
                biased;
                () = self.cancellation_token.cancelled() => return Ok(()),
                result = self.scheduler_client.next_task(
                    self.em_id,
                    self.prev_assignments.pop_front(),
                    self.scheduler_poll_wait_ms,
                ) => {
                    match result {
                        Ok(response) => response,
                        Err(e) => {
                            tracing::warn!(err = ? e, "Scheduler returned an error. Retrying.");
                            continue;
                        }
                    }
                }
            };

            tracing::info!(
                bundle_session = response.session_id,
                job_id = ? response.task_assignment.job_id,
                task_id = ? response.task_assignment.task_id,
                "Received a new task assignment from the scheduler."
            );

            let current_session = self.session_tracker.current();
            if response.session_id < current_session {
                tracing::warn!(
                    bundle_session = response.session_id,
                    current_session,
                    job_id = ? response.task_assignment.job_id,
                    task_id = ? response.task_assignment.task_id,
                    "Dropping stale task assignment from the scheduler."
                );
                self.mark_consume(&response);
                continue;
            }
            if response.session_id > current_session {
                tracing::info!(
                    new_session = response.session_id,
                    "Observed a newer session via the scheduler. Refreshing liveness."
                );
                self.liveness_handle.refresh().await;
            }

            let Some(execution_context) = self.register_task_instance(response).await? else {
                continue;
            };
            let task_instance_id = execution_context.task_instance_id;

            let hard_timeout =
                Duration::from_millis(execution_context.timeout_policy.hard_timeout_ms);
            let request = ExecuteRequest {
                job_id: response.task_assignment.job_id,
                task_id: response.task_assignment.task_id,
                resource_group_id: response.task_assignment.resource_group_id,
                ctx: execution_context,
            };
            let outcome = self
                .process_pool
                .execute(request, hard_timeout)
                .await
                .inspect_err(|err| {
                    tracing::error!(
                        err = ? err,
                        job_id = ? response.task_assignment.job_id,
                        task_id = ? response.task_assignment.task_id,
                        "Process pool failed to dispatch task. Bailing out."
                    );
                })?;

            let current_session = self.session_tracker.current();
            if response.session_id < current_session {
                tracing::warn!(
                    bundle_session = response.session_id,
                    current_session,
                    job_id = ? response.task_assignment.job_id,
                    task_id = ? response.task_assignment.task_id,
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
                    job: response.task_assignment.job_id,
                    task: response.task_assignment.task_id,
                    task_instance_id,
                    session: response.session_id,
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
    /// * `Ok(None)` if:
    ///   * The assignment is stale: either from a stale cache session or the task has already in a
    ///     terminal state.
    ///   * The runtime is cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`RuntimeError::StorageResponse`] if storage client returns an error that cannot be
    ///   handled by the runtime.
    async fn register_task_instance(
        &mut self,
        response: SchedulerResponse,
    ) -> Result<Option<ExecutionContext>, RuntimeError> {
        let register_result = tokio::select! {
            biased;
            () = self.cancellation_token.cancelled() => return Ok(None),
            result = self.storage_client.register_task_instance(
                response.task_assignment.job_id,
                response.task_assignment.task_id,
                self.em_id,
                response.session_id,
            ) => result,
        };

        match register_result {
            Ok(execution_context) => {
                self.mark_consume(&response);
                Ok(Some(execution_context))
            }
            Err(err) => match &err {
                StorageResponseError::StaleSession(message) => {
                    tracing::warn!(
                        bundle_session = response.session_id,
                        error = % message,
                        job_id = ? response.task_assignment.job_id,
                        task_id = ? response.task_assignment.task_id,
                        "Storage rejected task registration as stale. Dropping the assignment."
                    );
                    self.liveness_handle.refresh().await;
                    self.mark_consume(&response);
                    Ok(None)
                }
                StorageResponseError::CacheStale(_) => {
                    tracing::warn!(
                        err = % err,
                        job_id = ? response.task_assignment.job_id,
                        task_id = ? response.task_assignment.task_id,
                        "Storage rejected task registration. Dropping the assignment."
                    );
                    self.mark_consume(&response);
                    Ok(None)
                }
                _ => {
                    tracing::error!(
                        err = % err,
                        job_id = ? response.task_assignment.job_id,
                        task_id = ? response.task_assignment.task_id,
                        "Storage client returns an error. Bailing out."
                    );
                    Err(RuntimeError::StorageResponse(err))
                }
            },
        }
    }

    /// Records `response`'s assignment as consumed, queueing it to be acknowledged to the scheduler
    /// on the next [`SchedulerClient::next_task`] poll, or at shutdown if the loop exits first.
    fn mark_consume(&mut self, response: &SchedulerResponse) {
        self.prev_assignments.push_back(TaskAssignmentRecord::new(
            response.task_assignment.id,
            response.scheduler_id,
        ));
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
                    hard_timeout_ms = ? hard_timeout.as_millis(),
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
                    exit_status = ? exit_status,
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
