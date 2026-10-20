use std::time::Duration;

use serde::Deserialize;
use spider_utils::shutdown::ShutdownHandle;
use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;

use crate::cache::error::CacheError;
use crate::cache::error::InternalError;
use crate::config::DatabaseConfig;
use crate::db::DbStorage;
use crate::db::MariaDbStorageConnector;
use crate::db::SessionManagement;
use crate::inbound_queue::InboundQueueSender;
use crate::inbound_queue::InboundQueueSenderHandle;
use crate::inbound_queue::create_inbound_queue;
use crate::state::JobCache;
use crate::state::JobCacheGcConfig;
use crate::state::ServiceState;
use crate::state::ServiceStateParams;
use crate::state::StorageServerError;
use crate::state::create_job_cache_gc;
use crate::task_instance_pool::TaskInstancePoolConfig;
use crate::task_instance_pool::TaskInstancePoolConnector;
use crate::task_instance_pool::TaskInstancePoolHandle;
use crate::task_instance_pool::create_task_instance_pool;
use crate::test_hooks::TestHooks;

/// Runtime configuration for the storage service.
#[derive(Clone, Debug, Deserialize)]
pub struct RuntimeConfig {
    pub db: DatabaseConfig,
    #[serde(default)]
    pub task_instance_pool: TaskInstancePoolConfig,
    #[serde(default)]
    pub job_cache_gc: JobCacheGcConfig,
}

/// Runtime state for the storage service.
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The inbound queue sender type.
/// * `DbConnectorType` - The database connector type.
/// * `TaskInstancePoolConnectorType` - The task instance pool connector type.
pub struct Runtime<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> {
    service_state:
        ServiceState<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    shutdown_handle: ShutdownHandle,
    task_tracker: TaskTracker,
    task_instance_pool_join_handle: JoinHandle<Result<(), InternalError>>,
    job_cache_gc_join_handle: JoinHandle<Result<(), InternalError>>,
    stop_timeout: Duration,
}

impl<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> Runtime<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    /// Stops the runtime.
    ///
    /// The background tasks, including every task in the service's task tracker, will be cancelled
    /// and joined. The errors of the background tasks are logged.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::Stopping`] if any of the background tasks does not stop before
    ///   timeout, or exits on an error or a panic.
    pub async fn stop(self) -> Result<(), StorageServerError> {
        self.shutdown_handle.shut_down_gracefully();
        self.task_tracker.close();

        let join_task_instance_pool = async {
            match self.task_instance_pool_join_handle.await {
                Ok(Ok(())) => {
                    tracing::info!("Task instance pool stopped.");
                    true
                }
                Ok(Err(e)) => {
                    tracing::error!(error = % e, "Task instance pool exited on error.");
                    false
                }
                Err(e) => {
                    tracing::error!(error = % e, "Task instance pool exited on panic.");
                    false
                }
            }
        };

        let join_job_cache_gc = async {
            match self.job_cache_gc_join_handle.await {
                Ok(Ok(())) => {
                    tracing::info!("Job cache GC stopped.");
                    true
                }
                Ok(Err(e)) => {
                    tracing::error!(error = % e, "Job cache GC exited on error.");
                    false
                }
                Err(e) => {
                    tracing::error!(error = % e, "Job cache GC exited on panic.");
                    false
                }
            }
        };

        let (is_task_instance_pool_stopped, is_job_cache_gc_stopped, ()) =
            tokio::time::timeout(self.stop_timeout, async {
                tokio::join!(
                    join_task_instance_pool,
                    join_job_cache_gc,
                    self.task_tracker.wait()
                )
            })
            .await
            .map_err(|_| {
                StorageServerError::Stopping("background task stop timed out".to_owned())
            })?;
        if !is_task_instance_pool_stopped || !is_job_cache_gc_stopped {
            return Err(StorageServerError::Stopping(
                "background task exited on an error or a panic".to_owned(),
            ));
        }

        Ok(())
    }

    /// # Returns
    ///
    /// A clone of the runtime's [`ServiceState`].
    #[must_use]
    pub fn get_service_state(
        &self,
    ) -> ServiceState<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType> {
        self.service_state.clone()
    }

    /// # Returns
    ///
    /// A clone of the runtime's [`ShutdownHandle`].
    #[must_use]
    pub fn get_shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown_handle.clone()
    }
}

/// Creates a storage server runtime from the given configurations, with no test hook armed.
///
/// # Returns
///
/// Forwards [`create_runtime_with_hooks`]'s return values on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`create_runtime_with_hooks`]'s return values on failure.
pub async fn create_runtime(
    config: &RuntimeConfig,
) -> Result<
    Runtime<InboundQueueSenderHandle, MariaDbStorageConnector, TaskInstancePoolHandle>,
    StorageServerError,
> {
    create_runtime_with_hooks(config, TestHooks::default()).await
}

/// Creates a storage server runtime from the given configurations, with `hooks` as its test hooks:
/// connects to the database, starts the background tasks, and recovers the persisted jobs.
///
/// # Returns
///
/// The newly created runtime instance on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`MariaDbStorageConnector::connect`]'s return values on failure.
/// * Forwards [`create_task_instance_pool`]'s return values on failure.
/// * Forwards [`create_job_cache_gc`]'s return values on failure.
/// * Forwards [`ServiceState::recover_jobs`]'s return values on failure.
pub async fn create_runtime_with_hooks(
    config: &RuntimeConfig,
    hooks: TestHooks,
) -> Result<
    Runtime<InboundQueueSenderHandle, MariaDbStorageConnector, TaskInstancePoolHandle>,
    StorageServerError,
> {
    let shutdown_handle = ShutdownHandle::new();
    let db = MariaDbStorageConnector::connect(&config.db).await?;
    let session_id = db.session_id();
    let (inbound_queue_sender, inbound_queue_receiver) = create_inbound_queue();
    let (task_instance_pool_connector, task_instance_pool_join_handle) = create_task_instance_pool(
        inbound_queue_sender.clone(),
        db.clone(),
        shutdown_handle.clone(),
        &config.task_instance_pool,
    )
    .map_err(CacheError::from)?;

    let job_cache = JobCache::new();
    let (job_cache_gc_handle, job_cache_gc_join_handle) = create_job_cache_gc(
        job_cache.clone(),
        shutdown_handle.clone(),
        &config.job_cache_gc,
    )
    .map_err(CacheError::from)?;
    let service_state = ServiceState::new(ServiceStateParams {
        db,
        session_id,
        job_cache,
        inbound_queue_sender,
        inbound_queue_receiver,
        task_instance_pool_connector,
        job_cache_gc_handle,
        shutdown_handle: shutdown_handle.clone(),
        hooks,
    });
    service_state.recover_jobs().await?;

    Ok(Runtime {
        task_tracker: service_state.task_tracker().clone(),
        service_state,
        shutdown_handle,
        task_instance_pool_join_handle,
        job_cache_gc_join_handle,
        stop_timeout: Duration::from_secs(STOP_BACKGROUND_TASKS_TIMEOUT_SEC),
    })
}

const STOP_BACKGROUND_TASKS_TIMEOUT_SEC: u64 = 30;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::task::JoinHandle;

    use super::*;
    use crate::cache::error::InternalError;
    use crate::db::SessionManagement;
    use crate::inbound_queue::InboundQueueSenderHandle;
    use crate::inbound_queue::create_inbound_queue;
    use crate::state::JobCache;
    use crate::state::ServiceState;
    use crate::state::ServiceStateParams;
    use crate::state::StorageServerError;
    use crate::state::test_utils::MockDbConnector;
    use crate::state::test_utils::MockTaskInstancePoolConnector;

    type TestServerRuntime =
        Runtime<InboundQueueSenderHandle, MockDbConnector, MockTaskInstancePoolConnector>;

    fn create_test_runtime(
        shutdown_handle: ShutdownHandle,
        mock_task_instance_pool_handle: JoinHandle<Result<(), InternalError>>,
        stop_timeout_sec: u64,
    ) -> TestServerRuntime {
        let db = MockDbConnector::default();
        let session_id = db.session_id();
        let (sender, receiver) = create_inbound_queue();
        let job_cache = JobCache::new();
        let (job_cache_gc_handle, job_cache_gc_join_handle) = create_job_cache_gc(
            job_cache.clone(),
            shutdown_handle.clone(),
            &JobCacheGcConfig::default(),
        )
        .expect("job cache GC creation");
        let service_state = ServiceState::new(ServiceStateParams {
            db,
            session_id,
            job_cache,
            inbound_queue_sender: sender,
            inbound_queue_receiver: receiver,
            task_instance_pool_connector: MockTaskInstancePoolConnector,
            job_cache_gc_handle,
            shutdown_handle: shutdown_handle.clone(),
            hooks: TestHooks::default(),
        });

        // Wired with a real job cache GC task, which should always be terminated without errors.
        Runtime {
            task_tracker: service_state.task_tracker().clone(),
            service_state,
            shutdown_handle,
            task_instance_pool_join_handle: mock_task_instance_pool_handle,
            job_cache_gc_join_handle,
            stop_timeout: Duration::from_secs(stop_timeout_sec),
        }
    }

    #[tokio::test]
    async fn stop_runtime_on_success() -> anyhow::Result<()> {
        let shutdown_handle = ShutdownHandle::new();
        let task_shutdown_handle = shutdown_handle.clone();
        let mock_task_instance_pool_handle: JoinHandle<Result<(), InternalError>> =
            tokio::spawn(async move {
                task_shutdown_handle.cancelled().await;
                Ok(())
            });

        let runtime = create_test_runtime(
            shutdown_handle,
            mock_task_instance_pool_handle,
            STOP_BACKGROUND_TASKS_TIMEOUT_SEC,
        );
        runtime
            .stop()
            .await
            .expect("stop_background_tasks should succeed");
        Ok(())
    }

    #[tokio::test]
    async fn stop_runtime_on_timeout() -> anyhow::Result<()> {
        let shutdown_handle = ShutdownHandle::new();
        let mock_task_instance_pool_handle: JoinHandle<Result<(), InternalError>> =
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(())
            });

        let runtime = create_test_runtime(shutdown_handle, mock_task_instance_pool_handle, 0);
        let result = runtime.stop().await;

        assert!(
            matches!(result, Err(StorageServerError::Stopping(_))),
            "timeout should return Stopping"
        );
        Ok(())
    }

    #[tokio::test]
    async fn stop_runtime_on_task_error() -> anyhow::Result<()> {
        let shutdown_handle = ShutdownHandle::new();
        let mock_task_instance_pool_handle: JoinHandle<Result<(), InternalError>> =
            tokio::spawn(async move {
                Err(InternalError::TaskInstancePoolCorrupted(
                    "test failure".to_owned(),
                ))
            });

        let runtime = create_test_runtime(
            shutdown_handle,
            mock_task_instance_pool_handle,
            STOP_BACKGROUND_TASKS_TIMEOUT_SEC,
        );
        let result = runtime.stop().await;

        assert!(
            matches!(result, Err(StorageServerError::Stopping(_))),
            "pool task failure should return Stopping"
        );
        Ok(())
    }

    #[tokio::test]
    async fn stop_runtime_on_task_panic() -> anyhow::Result<()> {
        let shutdown_handle = ShutdownHandle::new();
        let mock_task_instance_pool_handle: JoinHandle<Result<(), InternalError>> =
            tokio::spawn(async move { panic!("test panic") });

        let runtime = create_test_runtime(
            shutdown_handle,
            mock_task_instance_pool_handle,
            STOP_BACKGROUND_TASKS_TIMEOUT_SEC,
        );
        let result = runtime.stop().await;

        assert!(
            matches!(result, Err(StorageServerError::Stopping(_))),
            "pool task panic should return Stopping"
        );
        Ok(())
    }
}
