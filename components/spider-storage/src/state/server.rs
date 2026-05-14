use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    cache::error::{CacheError, InternalError},
    config::DatabaseConfig,
    db::{DbStorage, MariaDbStorageConnector, SessionManagement},
    ready_queue::{ReadyQueueConfig, ReadyQueueSender, ReadyQueueSenderHandle, create_ready_queue},
    state::{JobCache, ServiceState, StorageServerError},
    task_instance_pool::{
        TaskInstancePoolConfig,
        TaskInstancePoolConnector,
        TaskInstancePoolHandle,
        create_task_instance_pool,
    },
};

/// Per-process storage server runtime.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready queue sender type.
/// * `DbConnectorType` - The database connector type.
/// * `TaskInstancePoolConnectorType` - The task instance pool connector type.
pub struct ServerRuntime<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    service_state:
        ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    cancellation_token: CancellationToken,
    task_instance_pool_join_handle: JoinHandle<Result<(), InternalError>>,
    stop_timeout_sec: u64,
}

/// Creates a storage server runtime from the database configuration.
///
/// # Returns
///
/// A newly created [`ServerRuntime`] on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`MariaDbStorageConnector::connect`]'s return values on failure.
/// * Forwards [`create_ready_queue`]'s return values on failure.
pub async fn create_server_runtime(
    db_config: &DatabaseConfig,
) -> Result<
    ServerRuntime<ReadyQueueSenderHandle, MariaDbStorageConnector, TaskInstancePoolHandle>,
    StorageServerError,
> {
    let cancellation_token = CancellationToken::new();
    let db = MariaDbStorageConnector::connect(db_config).await?;
    let session_id = db.session_id();
    let (ready_queue_sender, ready_queue_receiver) =
        create_ready_queue(ReadyQueueConfig::default()).map_err(CacheError::from)?;
    let (task_instance_pool_connector, task_instance_pool_join_handle) = create_task_instance_pool(
        ready_queue_sender.clone(),
        db.clone(),
        cancellation_token.clone(),
        TaskInstancePoolConfig::default(),
    );
    let service_state = ServiceState::new(
        db,
        session_id,
        JobCache::new(),
        ready_queue_sender,
        ready_queue_receiver,
        task_instance_pool_connector,
    );

    Ok(ServerRuntime {
        service_state,
        cancellation_token,
        task_instance_pool_join_handle,
        stop_timeout_sec: STOP_BACKGROUND_TASKS_TIMEOUT_SEC,
    })
}

impl<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
    ServerRuntime<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
where
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
{
    /// Stops background tasks owned by the runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::Stopping`] if the task instance pool does not stop before timeout.
    /// * [`StorageServerError::Cache`] if the task instance pool task fails or cannot be joined.
    pub async fn stop_background_tasks(self) -> Result<(), StorageServerError> {
        self.cancellation_token.cancel();
        let join_result = tokio::time::timeout(
            Duration::from_secs(self.stop_timeout_sec),
            self.task_instance_pool_join_handle,
        )
        .await
        .map_err(|_| {
            StorageServerError::Stopping("task instance pool stop timed out".to_owned())
        })?;
        let pool_result = join_result.map_err(|e| {
            StorageServerError::Cache(CacheError::Internal(
                InternalError::TaskInstancePoolCorrupted(format!("task join error: {e}")),
            ))
        })?;
        pool_result.map_err(|e| StorageServerError::Cache(CacheError::Internal(e)))
    }

    /// # Returns
    ///
    /// A clone of the runtime's [`ServiceState`].
    #[must_use]
    pub fn service_state(
        &self,
    ) -> ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType> {
        self.service_state.clone()
    }
}

const STOP_BACKGROUND_TASKS_TIMEOUT_SEC: u64 = 30;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    use super::ServerRuntime;
    use crate::{
        cache::error::InternalError,
        db::SessionManagement,
        ready_queue::{ReadyQueueConfig, ReadyQueueSenderHandle, create_ready_queue},
        state::{
            JobCache,
            ServiceState,
            StorageServerError,
            test_utils::{MockDbConnector, MockTaskInstancePoolConnector},
        },
    };

    type TestServerRuntime =
        ServerRuntime<ReadyQueueSenderHandle, MockDbConnector, MockTaskInstancePoolConnector>;

    fn create_test_server_runtime(
        cancellation_token: CancellationToken,
        task: JoinHandle<Result<(), InternalError>>,
        stop_timeout_sec: u64,
    ) -> TestServerRuntime {
        let db = MockDbConnector::default();
        let session_id = db.session_id();
        let (sender, receiver) =
            create_ready_queue(ReadyQueueConfig::default()).expect("ready queue creation");
        let service_state = ServiceState::new(
            db,
            session_id,
            JobCache::new(),
            sender,
            receiver,
            MockTaskInstancePoolConnector,
        );

        ServerRuntime {
            service_state,
            cancellation_token,
            task_instance_pool_join_handle: task,
            stop_timeout_sec,
        }
    }

    #[tokio::test]
    async fn stop_background_tasks_cancels_and_joins_task() -> anyhow::Result<()> {
        let cancellation_token = CancellationToken::new();
        let task_cancellation_token = cancellation_token.clone();
        let task: JoinHandle<Result<(), InternalError>> = tokio::spawn(async move {
            task_cancellation_token.cancelled().await;
            Ok(())
        });

        let runtime = create_test_server_runtime(
            cancellation_token,
            task,
            super::STOP_BACKGROUND_TASKS_TIMEOUT_SEC,
        );
        runtime
            .stop_background_tasks()
            .await
            .expect("stop_background_tasks should succeed");
        Ok(())
    }

    #[tokio::test]
    async fn stop_background_tasks_returns_stopping_on_timeout() -> anyhow::Result<()> {
        let cancellation_token = CancellationToken::new();
        let task: JoinHandle<Result<(), InternalError>> = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        });

        let runtime = create_test_server_runtime(cancellation_token, task, 0);
        let result = runtime.stop_background_tasks().await;

        assert!(
            matches!(result, Err(StorageServerError::Stopping(_))),
            "timeout should return Stopping"
        );
        Ok(())
    }

    #[tokio::test]
    async fn stop_background_tasks_returns_cache_error_on_pool_error() -> anyhow::Result<()> {
        let cancellation_token = CancellationToken::new();
        let task: JoinHandle<Result<(), InternalError>> = tokio::spawn(async move {
            Err(InternalError::TaskInstancePoolCorrupted(
                "test failure".to_owned(),
            ))
        });

        let runtime = create_test_server_runtime(
            cancellation_token,
            task,
            super::STOP_BACKGROUND_TASKS_TIMEOUT_SEC,
        );
        let result = runtime.stop_background_tasks().await;

        assert!(
            matches!(result, Err(StorageServerError::Cache(_))),
            "pool task failure should return Cache error"
        );
        Ok(())
    }
}
