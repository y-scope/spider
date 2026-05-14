use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    cache::error::{CacheError, InternalError},
    config::DatabaseConfig,
    db::{MariaDbStorageConnector, SessionManagement},
    ready_queue::{ReadyQueueConfig, ReadyQueueSenderHandle, create_ready_queue},
    state::{JobCache, ServiceState, StorageServerError},
    task_instance_pool::{
        TaskInstancePoolConfig,
        TaskInstancePoolHandle,
        create_task_instance_pool,
    },
};

/// Production per-process storage server runtime.
pub struct ServerRuntime {
    service_state:
        ServiceState<ReadyQueueSenderHandle, MariaDbStorageConnector, TaskInstancePoolHandle>,
    cancellation_token: CancellationToken,
    task_instance_pool_task: JoinHandle<Result<(), InternalError>>,
}

impl ServerRuntime {
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
    pub async fn create(db_config: &DatabaseConfig) -> Result<Self, StorageServerError> {
        let cancellation_token = CancellationToken::new();
        let db = MariaDbStorageConnector::connect(db_config).await?;
        let session_id = db.session_id();
        let (ready_queue_sender, ready_queue_receiver) =
            create_ready_queue(ReadyQueueConfig::default()).map_err(CacheError::from)?;
        let (task_instance_pool_connector, task_instance_pool_task) = create_task_instance_pool(
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

        Ok(Self {
            service_state,
            cancellation_token,
            task_instance_pool_task,
        })
    }

    /// # Returns
    ///
    /// A clone of the runtime's [`ServiceState`].
    #[must_use]
    pub fn service_state(
        &self,
    ) -> ServiceState<ReadyQueueSenderHandle, MariaDbStorageConnector, TaskInstancePoolHandle> {
        self.service_state.clone()
    }

    /// Stops background tasks owned by the runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::Stopping`] if the task instance pool does not stop before timeout.
    /// * [`StorageServerError::Cache`] if the task instance pool task fails or cannot be joined.
    pub async fn stop_background_tasks(self) -> Result<(), StorageServerError> {
        stop_background_task(
            self.cancellation_token,
            self.task_instance_pool_task,
            STOP_BACKGROUND_TASKS_TIMEOUT,
        )
        .await
    }
}

const STOP_BACKGROUND_TASKS_TIMEOUT: Duration = Duration::from_secs(30);

/// Stops a single cancellation-token-controlled background task.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`StorageServerError::Stopping`] if the task does not stop before `timeout`.
/// * [`StorageServerError::Cache`] if the task fails or cannot be joined.
async fn stop_background_task(
    cancellation_token: CancellationToken,
    task: JoinHandle<Result<(), InternalError>>,
    timeout: Duration,
) -> Result<(), StorageServerError> {
    cancellation_token.cancel();
    let join_result = tokio::time::timeout(timeout, task).await.map_err(|_| {
        StorageServerError::Stopping("task instance pool stop timed out".to_owned())
    })?;
    let pool_result = join_result.map_err(|e| {
        StorageServerError::Cache(CacheError::Internal(
            InternalError::TaskInstancePoolCorrupted(format!("task join error: {e}")),
        ))
    })?;
    pool_result.map_err(|e| StorageServerError::Cache(CacheError::Internal(e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stop_background_task_cancels_and_joins_task() -> anyhow::Result<()> {
        let cancellation_token = CancellationToken::new();
        let task_cancellation_token = cancellation_token.clone();
        let task = tokio::spawn(async move {
            task_cancellation_token.cancelled().await;
            Ok(())
        });

        stop_background_task(cancellation_token, task, Duration::from_secs(1)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn stop_background_task_returns_stopping_on_timeout() -> anyhow::Result<()> {
        let cancellation_token = CancellationToken::new();
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_mins(1)).await;
            Ok(())
        });

        let result = stop_background_task(cancellation_token, task, Duration::from_millis(1)).await;

        assert!(
            matches!(result, Err(StorageServerError::Stopping(_))),
            "timeout should return Stopping"
        );
        Ok(())
    }

    #[tokio::test]
    async fn stop_background_task_returns_cache_error_on_pool_error() -> anyhow::Result<()> {
        let cancellation_token = CancellationToken::new();
        let task = tokio::spawn(async move {
            Err(InternalError::TaskInstancePoolCorrupted(
                "test failure".to_owned(),
            ))
        });

        let result = stop_background_task(cancellation_token, task, Duration::from_secs(1)).await;

        assert!(
            matches!(result, Err(StorageServerError::Cache(_))),
            "pool task failure should return Cache error"
        );
        Ok(())
    }
}
