use std::time::Duration;

use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cache::error::CacheError;
use crate::cache::error::InternalError;
use crate::cache::job::SharedJobControlBlock;
use crate::config::DatabaseConfig;
use crate::db::DbStorage;
use crate::db::MariaDbStorageConnector;
use crate::db::SessionManagement;
use crate::ready_queue::ReadyQueueConfig;
use crate::ready_queue::ReadyQueueSender;
use crate::ready_queue::ReadyQueueSenderHandle;
use crate::ready_queue::create_ready_queue;
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

/// Runtime configuration for the storage service.
#[derive(Clone, Debug, Deserialize)]
pub struct RuntimeConfig {
    pub db_config: DatabaseConfig,
    #[serde(default)]
    pub ready_queue_config: ReadyQueueConfig,
    #[serde(default)]
    pub task_instance_pool_config: TaskInstancePoolConfig,
    #[serde(default)]
    pub job_cache_gc_config: JobCacheGcConfig,
}

/// Runtime state for the storage service.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready queue sender type.
/// * `DbConnectorType` - The database connector type.
/// * `TaskInstancePoolConnectorType` - The task instance pool connector type.
pub struct Runtime<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> {
    service_state:
        ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    cancellation_token: CancellationToken,
    task_instance_pool_join_handle: JoinHandle<Result<(), InternalError>>,
    job_cache_gc_join_handle: JoinHandle<Result<(), InternalError>>,
    stop_timeout: Duration,
}

impl<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: DbStorage + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
> Runtime<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    /// Stops the runtime.
    ///
    /// The background tasks will be cancelled and joined. The errors of the background tasks are
    /// logged and will not be returned through this method.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::Stopping`] if any of the background tasks does not stop before
    ///   timeout.
    pub async fn stop(self) -> Result<(), StorageServerError> {
        self.cancellation_token.cancel();

        let join_task_instance_pool = async {
            match self.task_instance_pool_join_handle.await {
                Ok(Ok(())) => {
                    tracing::info!("Task instance pool stopped.");
                }
                Ok(Err(e)) => {
                    tracing::error!(error = % e, "Task instance pool exited on error.");
                }
                Err(e) => {
                    tracing::error!(error = % e, "Task instance pool exited on panic.");
                }
            }
        };

        let join_job_cache_gc = async {
            match self.job_cache_gc_join_handle.await {
                Ok(Ok(())) => {
                    tracing::info!("Job cache GC stopped.");
                }
                Ok(Err(e)) => {
                    tracing::error!(error = % e, "Job cache GC exited on error.");
                }
                Err(e) => {
                    tracing::error!(error = % e, "Job cache GC exited on panic.");
                }
            }
        };

        let _ = tokio::time::timeout(self.stop_timeout, async {
            tokio::join!(join_task_instance_pool, join_job_cache_gc,)
        })
        .await
        .map_err(|_| StorageServerError::Stopping("background task stop timed out".to_owned()))?;

        Ok(())
    }

    /// # Returns
    ///
    /// A clone of the runtime's [`ServiceState`].
    #[must_use]
    pub fn get_service_state(
        &self,
    ) -> ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType> {
        self.service_state.clone()
    }
}

/// Creates a storage server runtime from the given configurations.
///
/// # Returns
///
/// A tuple on success, containing:
///
/// * The newly created runtime instance.
/// * The runtime's cancellation token.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`MariaDbStorageConnector::connect`]'s return values on failure.
/// * Forwards [`create_task_instance_pool`]'s return values on failure.
/// * Forwards [`create_ready_queue`]'s return values on failure.
/// * Forwards [`create_job_cache_gc`]'s return values on failure.
pub async fn create_runtime(
    config: &RuntimeConfig,
) -> Result<
    (
        Runtime<ReadyQueueSenderHandle, MariaDbStorageConnector, TaskInstancePoolHandle>,
        CancellationToken,
    ),
    StorageServerError,
> {
    let cancellation_token = CancellationToken::new();
    let db = MariaDbStorageConnector::connect(&config.db_config).await?;
    let session_id = db.session_id();
    let (ready_queue_sender, ready_queue_receiver) =
        create_ready_queue(&config.ready_queue_config).map_err(CacheError::from)?;
    let (task_instance_pool_connector, task_instance_pool_join_handle) = create_task_instance_pool(
        ready_queue_sender.clone(),
        db.clone(),
        cancellation_token.clone(),
        &config.task_instance_pool_config,
    )
    .map_err(CacheError::from)?;

    let job_cache = recover_job_cache(
        &db,
        ready_queue_sender.clone(),
        task_instance_pool_connector.clone(),
    )
    .await?;
    let (job_cache_gc_handle, job_cache_gc_join_handle) = create_job_cache_gc(
        job_cache.clone(),
        cancellation_token.clone(),
        &config.job_cache_gc_config,
    )
    .map_err(CacheError::from)?;
    let service_state = ServiceState::new(ServiceStateParams {
        db,
        session_id,
        job_cache,
        ready_queue_sender,
        ready_queue_receiver,
        task_instance_pool_connector,
        job_cache_gc_handle,
        cancellation_token: cancellation_token.clone(),
    });

    Ok((
        Runtime {
            service_state,
            cancellation_token: cancellation_token.clone(),
            task_instance_pool_join_handle,
            job_cache_gc_join_handle,
            stop_timeout: Duration::from_secs(STOP_BACKGROUND_TASKS_TIMEOUT_SEC),
        },
        cancellation_token,
    ))
}

const STOP_BACKGROUND_TASKS_TIMEOUT_SEC: u64 = 30;

/// Recovers jobs from persistent storage into the cache.
///
/// # Returns
///
/// A [`JobCache`] containing all recoverable jobs on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`DbStorage::get_recoverable_jobs`]'s return values on failure.
/// * Forwards [`SharedJobControlBlock::recover`]'s return values on failure.
/// * Forwards [`JobCache::insert`]'s return values on failure.
async fn recover_job_cache<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    db: &DbConnectorType,
    ready_queue_sender: ReadyQueueSenderType,
    task_instance_pool_connector: TaskInstancePoolConnectorType,
) -> Result<
    JobCache<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    StorageServerError,
> {
    let job_cache = JobCache::new();
    for recoverable_job in db.get_recoverable_jobs().await? {
        let id = recoverable_job.id;
        let state = recoverable_job.state;
        let jcb = SharedJobControlBlock::recover(
            recoverable_job,
            ready_queue_sender.clone(),
            db.clone(),
            task_instance_pool_connector.clone(),
        )
        .await?;
        job_cache.insert(jcb).await?;
        tracing::info!(
            job_id = ? id,
            job_state = ? state,
            "Job recovered into cache.",
        );
    }
    Ok(job_cache)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::cache::error::InternalError;
    use crate::db::SessionManagement;
    use crate::ready_queue::ReadyQueueConfig;
    use crate::ready_queue::ReadyQueueSenderHandle;
    use crate::ready_queue::create_ready_queue;
    use crate::state::JobCache;
    use crate::state::ServiceState;
    use crate::state::ServiceStateParams;
    use crate::state::StorageServerError;
    use crate::state::test_utils::MockDbConnector;
    use crate::state::test_utils::MockTaskInstancePoolConnector;

    type TestServerRuntime =
        Runtime<ReadyQueueSenderHandle, MockDbConnector, MockTaskInstancePoolConnector>;

    fn create_test_runtime(
        cancellation_token: CancellationToken,
        mock_task_instance_pool_handle: JoinHandle<Result<(), InternalError>>,
        stop_timeout_sec: u64,
    ) -> TestServerRuntime {
        let db = MockDbConnector::default();
        let session_id = db.session_id();
        let (sender, receiver) =
            create_ready_queue(&ReadyQueueConfig::default()).expect("ready queue creation");
        let job_cache = JobCache::new();
        let (job_cache_gc_handle, job_cache_gc_join_handle) = create_job_cache_gc(
            job_cache.clone(),
            cancellation_token.clone(),
            &JobCacheGcConfig::default(),
        )
        .expect("job cache GC creation");
        let service_state = ServiceState::new(ServiceStateParams {
            db,
            session_id,
            job_cache,
            ready_queue_sender: sender,
            ready_queue_receiver: receiver,
            task_instance_pool_connector: MockTaskInstancePoolConnector,
            job_cache_gc_handle,
            cancellation_token: cancellation_token.clone(),
        });

        // Wired with a real job cache GC task, which should always be terminated without errors.
        Runtime {
            service_state,
            cancellation_token,
            task_instance_pool_join_handle: mock_task_instance_pool_handle,
            job_cache_gc_join_handle,
            stop_timeout: Duration::from_secs(stop_timeout_sec),
        }
    }

    #[tokio::test]
    async fn stop_runtime_on_success() -> anyhow::Result<()> {
        let cancellation_token = CancellationToken::new();
        let task_cancellation_token = cancellation_token.clone();
        let mock_task_instance_pool_handle: JoinHandle<Result<(), InternalError>> =
            tokio::spawn(async move {
                task_cancellation_token.cancelled().await;
                Ok(())
            });

        let runtime = create_test_runtime(
            cancellation_token,
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
        let cancellation_token = CancellationToken::new();
        let mock_task_instance_pool_handle: JoinHandle<Result<(), InternalError>> =
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(())
            });

        let runtime = create_test_runtime(cancellation_token, mock_task_instance_pool_handle, 0);
        let result = runtime.stop().await;

        assert!(
            matches!(result, Err(StorageServerError::Stopping(_))),
            "timeout should return Stopping"
        );
        Ok(())
    }

    #[tokio::test]
    async fn stop_runtime_on_task_error() -> anyhow::Result<()> {
        let cancellation_token = CancellationToken::new();
        let mock_task_instance_pool_handle: JoinHandle<Result<(), InternalError>> =
            tokio::spawn(async move {
                Err(InternalError::TaskInstancePoolCorrupted(
                    "test failure".to_owned(),
                ))
            });

        let runtime = create_test_runtime(
            cancellation_token,
            mock_task_instance_pool_handle,
            STOP_BACKGROUND_TASKS_TIMEOUT_SEC,
        );
        let result = runtime.stop().await;

        assert!(
            matches!(result, Ok(())),
            "pool task failure should not be forwarded as an error"
        );
        Ok(())
    }
}
