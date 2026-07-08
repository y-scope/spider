//! The scheduler runtime.
//!
//! This module registers the scheduler with the storage service, wires the scheduler core to a
//! freshly created dispatch queue, and spawns the core's scheduling loop as a background coroutine
//! alongside the execution manager registry. The resulting [`Runtime`] owns the spawned coroutine
//! and is responsible for cancelling and joining it on shutdown.

use std::time::Duration;

use serde::Deserialize;
use spider_core::types::id::SessionId;
use tokio_util::sync::CancellationToken;

use crate::config::SchedulerConfig;
use crate::core::TaskAssignmentIdIssuer;
use crate::dispatch_queue::DispatchQueueReader;
use crate::dispatch_queue::DispatchQueueWriter;
use crate::dispatch_queue::create_dispatch_queue;
use crate::error::SchedulerError;
use crate::error::SchedulerRuntimeError;
use crate::execution_manager_registry::ExecutionManagerRegistry;
use crate::execution_manager_registry::ExecutionManagerRegistryConfig;
use crate::service::SchedulerServiceState;
use crate::storage_client::SchedulerStorageClient;
use crate::types::TaskAssignment;

/// Runtime configuration for the scheduler service.
#[derive(Clone, Debug, Deserialize)]
pub struct RuntimeConfig {
    /// The scheduler core configuration that selects and configures the scheduling algorithm.
    pub scheduler: SchedulerConfig,

    /// The execution manager registry configuration.
    #[serde(default)]
    pub em_registry: ExecutionManagerRegistryConfig,

    /// The IP address this scheduler advertises to the storage service during registration.
    pub host: std::net::IpAddr,

    /// The port this scheduler advertises to the storage service during registration.
    pub port: u16,

    /// The maximum time, in seconds, to wait for background tasks to stop during shutdown.
    #[serde(default = "default_stop_timeout_sec")]
    pub stop_timeout_sec: u64,
}

/// Runtime state for the scheduler service.
pub struct Runtime {
    core_join_handle: tokio::task::JoinHandle<Result<(), SchedulerError>>,
    _reschedule_queue_receiver: tokio::sync::mpsc::UnboundedReceiver<TaskAssignment>,
    cancellation_token: CancellationToken,
    stop_timeout: Duration,
}

impl Runtime {
    /// Stops the runtime.
    ///
    /// The scheduler core coroutine is cancelled and joined. The core's error, if any, is logged
    /// and will not be returned through this method.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerRuntimeError::Stopping`] if the scheduler core does not stop before the
    ///   configured timeout.
    pub async fn stop(self) -> Result<(), SchedulerRuntimeError> {
        self.cancellation_token.cancel();

        let join_result = tokio::time::timeout(self.stop_timeout, self.core_join_handle)
            .await
            .map_err(|_| {
                SchedulerRuntimeError::Stopping("scheduler core stop timed out".to_owned())
            })?;
        match join_result {
            Ok(Ok(())) => {
                tracing::info!("Scheduler core stopped.");
            }
            Ok(Err(e)) => {
                tracing::error!(error = % e, "Scheduler core exited on error.");
            }
            Err(e) => {
                tracing::error!(error = % e, "Scheduler core exited on panic.");
            }
        }

        Ok(())
    }
}

/// Creates a scheduler runtime from the given configuration and storage client.
///
/// Registers this scheduler with the storage service, wires the scheduler core to a freshly created
/// dispatch queue, and starts the core's scheduling loop as a background coroutine.
///
/// # Type Parameters
///
/// * `SchedulerStorageClientType` - The storage client the core polls and registers through.
///
/// # Returns
///
/// A tuple on success, containing:
///
/// * The newly created runtime instance.
/// * The execution-manager-facing scheduler service, built over the dispatch queue reader.
/// * The runtime's cancellation token for cancelling the runtime on error.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`SchedulerStorageClient::register`]'s return values on failure.
pub async fn create_runtime<SchedulerStorageClientType: SchedulerStorageClient + 'static>(
    config: RuntimeConfig,
    storage_client: SchedulerStorageClientType,
) -> Result<
    (
        Runtime,
        SchedulerServiceState<DispatchQueueReader>,
        CancellationToken,
    ),
    SchedulerRuntimeError,
> {
    let cancellation_token = CancellationToken::new();

    let scheduler_id = storage_client.register(config.host, config.port).await?;
    tracing::info!(scheduler_id = % scheduler_id, "Scheduler registered with storage.");

    let RuntimeConfig {
        scheduler: scheduler_config,
        em_registry: execution_manager_registry_config,
        stop_timeout_sec,
        ..
    } = config;
    let dispatch_queue_capacity = scheduler_config.dispatch_queue_capacity();

    let (reschedule_queue_sender, reschedule_queue_receiver) =
        tokio::sync::mpsc::unbounded_channel();

    let registry = ExecutionManagerRegistry::new(
        &execution_manager_registry_config,
        cancellation_token.clone(),
        reschedule_queue_sender,
    );
    let (dispatch_queue_writer, dispatch_queue_reader) =
        create_dispatch_queue(dispatch_queue_capacity.get(), SessionId::default());
    let service = SchedulerServiceState::new(dispatch_queue_reader, registry, scheduler_id);
    let core = scheduler_config.make_core::<SchedulerStorageClientType, DispatchQueueWriter>();

    let core_join_handle = tokio::spawn(core.run(
        storage_client,
        dispatch_queue_writer,
        TaskAssignmentIdIssuer::new(),
        cancellation_token.clone(),
    ));

    let runtime = Runtime {
        core_join_handle,
        _reschedule_queue_receiver: reschedule_queue_receiver,
        cancellation_token: cancellation_token.clone(),
        stop_timeout: Duration::from_secs(stop_timeout_sec),
    };

    Ok((runtime, service, cancellation_token))
}

/// The maximum time, in seconds, to wait for background tasks to stop during shutdown.
const STOP_BACKGROUND_TASKS_TIMEOUT_SEC: u64 = 30;

/// # Returns
///
/// The default maximum time, in seconds, to wait for background tasks to stop during shutdown.
const fn default_stop_timeout_sec() -> u64 {
    STOP_BACKGROUND_TASKS_TIMEOUT_SEC
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::num::NonZeroU64;
    use std::num::NonZeroUsize;

    use async_trait::async_trait;
    use spider_core::job::JobState;
    use spider_core::types::id::JobId;
    use spider_core::types::id::SchedulerId;

    use super::*;
    use crate::core_impl::RoundRobinConfig;
    use crate::error::StorageClientError;
    use crate::types::InboundEntry;

    /// The scheduler identifier the mock storage client hands back from registration.
    const SCHEDULER_ID: u64 = 7;

    /// A minimal [`SchedulerStorageClient`] mock: registration returns a fixed identifier and every
    /// poll returns an empty batch under session zero, so the scheduler core can spin without any
    /// external services.
    #[derive(Clone)]
    struct MockStorageClient;

    #[async_trait]
    impl SchedulerStorageClient for MockStorageClient {
        async fn register(
            &self,
            _ip_address: IpAddr,
            _port: u16,
        ) -> Result<SchedulerId, StorageClientError> {
            Ok(SchedulerId::from(SCHEDULER_ID))
        }

        async fn poll_ready(
            &self,
            _max_items: usize,
            _wait: Duration,
        ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
            Ok((SessionId::default(), Vec::new()))
        }

        async fn poll_commit_ready(
            &self,
            _max_items: usize,
            _wait: Duration,
        ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
            Ok((SessionId::default(), Vec::new()))
        }

        async fn poll_cleanup_ready(
            &self,
            _max_items: usize,
            _wait: Duration,
        ) -> Result<(SessionId, Vec<InboundEntry>), StorageClientError> {
            Ok((SessionId::default(), Vec::new()))
        }

        async fn job_state(&self, _job_id: JobId) -> Result<JobState, StorageClientError> {
            Ok(JobState::Running)
        }
    }

    /// # Returns
    ///
    /// A [`RuntimeConfig`] with a small round-robin core and the given stop timeout.
    fn make_runtime_config(stop_timeout_sec: u64) -> RuntimeConfig {
        RuntimeConfig {
            scheduler: SchedulerConfig::RoundRobin(RoundRobinConfig {
                active_job_queue_capacity: NonZeroUsize::new(4).expect("4 is non-zero"),
                dispatch_queue_capacity: NonZeroUsize::new(8).expect("8 is non-zero"),
                ready_task_capacity: NonZeroUsize::new(64).expect("64 is non-zero"),
                commit_ready_task_capacity: NonZeroUsize::new(8).expect("8 is non-zero"),
                cleanup_ready_task_capacity: NonZeroUsize::new(8).expect("8 is non-zero"),
                storage_poll_timeout_ms: 1,
                tick_interval_ms: NonZeroU64::new(1).expect("1 is non-zero"),
                finalizing_job_expiration_timeout_sec: 60,
            }),
            em_registry: ExecutionManagerRegistryConfig::default(),
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 0,
            stop_timeout_sec,
        }
    }

    /// # Returns
    ///
    /// A [`Runtime`] whose core coroutine is `core_task`, wired to a fresh cancellation token and
    /// reschedule queue.
    fn make_runtime(
        cancellation_token: CancellationToken,
        core_task: tokio::task::JoinHandle<Result<(), SchedulerError>>,
        stop_timeout_sec: u64,
    ) -> Runtime {
        let (_reschedule_queue_sender, reschedule_queue_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        Runtime {
            core_join_handle: core_task,
            _reschedule_queue_receiver: reschedule_queue_receiver,
            cancellation_token,
            stop_timeout: Duration::from_secs(stop_timeout_sec),
        }
    }

    #[tokio::test]
    async fn create_runtime_registers_and_stops() -> anyhow::Result<()> {
        let (runtime, service, cancellation_token) =
            create_runtime(make_runtime_config(30), MockStorageClient).await?;

        // The service is stamped with the identifier handed back by registration.
        assert_eq!(service.scheduler_id(), SchedulerId::from(SCHEDULER_ID));
        assert!(!cancellation_token.is_cancelled());

        runtime.stop().await?;

        // Stopping the runtime cancels the token that drives the core coroutine.
        assert!(cancellation_token.is_cancelled());
        Ok(())
    }

    #[tokio::test]
    async fn stop_runtime_on_success() -> anyhow::Result<()> {
        let cancellation_token = CancellationToken::new();
        let core_cancellation_token = cancellation_token.clone();
        let core_task = tokio::spawn(async move {
            core_cancellation_token.cancelled().await;
            Ok(())
        });

        let runtime = make_runtime(cancellation_token, core_task, 30);
        runtime
            .stop()
            .await
            .expect("the runtime should stop cleanly");
        Ok(())
    }

    #[tokio::test]
    async fn stop_runtime_on_timeout() -> anyhow::Result<()> {
        let cancellation_token = CancellationToken::new();
        let core_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        });

        let runtime = make_runtime(cancellation_token, core_task, 0);
        let result = runtime.stop().await;

        assert!(
            matches!(result, Err(SchedulerRuntimeError::Stopping(_))),
            "a core that does not stop in time should yield a Stopping error"
        );
        Ok(())
    }

    #[tokio::test]
    async fn stop_runtime_on_core_error() -> anyhow::Result<()> {
        let cancellation_token = CancellationToken::new();
        let core_task =
            tokio::spawn(async move { Err(SchedulerError::Internal("test failure".to_owned())) });

        let runtime = make_runtime(cancellation_token, core_task, 30);

        // A core error is logged during teardown, not forwarded through `stop`.
        runtime
            .stop()
            .await
            .expect("a core error should not be forwarded as a stop error");
        Ok(())
    }
}
