use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    cache::{
        error::{CacheError, InternalError},
        job::SharedJobControlBlock,
    },
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

/// Runtime state for the storage service.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The ready queue sender type.
/// * `DbConnectorType` - The database connector type.
/// * `TaskInstancePoolConnectorType` - The task instance pool connector type.
pub struct Runtime<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    service_state:
        ServiceState<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    cancellation_token: CancellationToken,
    task_instance_pool_join_handle: JoinHandle<Result<(), InternalError>>,
    stop_timeout: Duration,
}

impl<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: DbStorage,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> Runtime<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    /// Stops the runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StorageServerError::Stopping`] if the task instance pool does not stop before timeout.
    /// * [`StorageServerError::Cache`] if the task instance pool task terminated on error or panic.
    pub async fn stop(mut self) -> Result<(), StorageServerError> {
        self.cancellation_token.cancel();
        tokio::select! {
            result = &mut self.task_instance_pool_join_handle => {
                result
                    .map_err(|e| {
                        let msg = format!("task instance pool panic: {e}");
                        CacheError::Internal(InternalError::TaskInstancePoolCorrupted(msg))
                    })?
                    .map_err(|e| StorageServerError::Cache(CacheError::Internal(e)))
            }
            () = tokio::time::sleep(self.stop_timeout) => {
                self.task_instance_pool_join_handle.abort();
                Err(StorageServerError::Stopping(
                    "task instance pool stop timed out".to_owned(),
                ))
            }
        }
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
pub async fn create_runtime(
    db_config: &DatabaseConfig,
    ready_queue_config: &ReadyQueueConfig,
    task_instance_pool_config: &TaskInstancePoolConfig,
) -> Result<
    (
        Runtime<ReadyQueueSenderHandle, MariaDbStorageConnector, TaskInstancePoolHandle>,
        CancellationToken,
    ),
    StorageServerError,
> {
    let cancellation_token = CancellationToken::new();
    let db = MariaDbStorageConnector::connect(db_config).await?;
    let session_id = db.session_id();
    let (ready_queue_sender, ready_queue_receiver) =
        create_ready_queue(ready_queue_config).map_err(CacheError::from)?;
    let (task_instance_pool_connector, task_instance_pool_join_handle) = create_task_instance_pool(
        ready_queue_sender.clone(),
        db.clone(),
        cancellation_token.clone(),
        task_instance_pool_config,
    )
    .map_err(CacheError::from)?;

    let job_cache = recover_job_cache(
        &db,
        ready_queue_sender.clone(),
        task_instance_pool_connector.clone(),
    )
    .await?;
    let service_state = ServiceState::new(
        db,
        session_id,
        job_cache,
        ready_queue_sender,
        ready_queue_receiver,
        task_instance_pool_connector,
    );

    Ok((
        Runtime {
            service_state,
            cancellation_token: cancellation_token.clone(),
            task_instance_pool_join_handle,
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

    use spider_core::{
        job::JobState,
        task::{
            DataTypeDescriptor,
            ExecutionPolicy,
            TaskDescriptor,
            TaskGraph as SubmittedTaskGraph,
            TdlContext,
            ValueTypeDescriptor,
        },
        types::{
            id::{JobId, ResourceGroupId},
            io::TaskInput,
        },
    };
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        cache::{error::InternalError, job_submission::ValidatedJobSubmission},
        db::{ExternalJobOrchestration, RecoverableJobContext, SessionManagement},
        ready_queue::{ReadyQueueConfig, ReadyQueueSenderHandle, create_ready_queue},
        state::{
            JobCache,
            ServiceState,
            StorageServerError,
            test_utils::{MockDbConnector, MockReadyQueueSender, MockTaskInstancePoolConnector},
        },
    };

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
        let service_state = ServiceState::new(
            db,
            session_id,
            JobCache::new(),
            sender,
            receiver,
            MockTaskInstancePoolConnector,
        );

        Runtime {
            service_state,
            cancellation_token,
            task_instance_pool_join_handle: mock_task_instance_pool_handle,
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
            matches!(result, Err(StorageServerError::Cache(_))),
            "pool task failure should return Cache error"
        );
        Ok(())
    }

    #[tokio::test]
    async fn recover_ready_job_cache_entry_allows_starting_later() -> anyhow::Result<()> {
        let job_id = JobId::random();
        let db = MockDbConnector::default();
        db.states.insert(job_id, JobState::Ready);
        let context = RecoverableJobContext {
            id: job_id,
            resource_group_id: ResourceGroupId::from(1),
            state: JobState::Ready,
            submission: create_test_job_submission()?,
            outputs: None,
        };

        let jcb = SharedJobControlBlock::recover(
            context,
            MockReadyQueueSender,
            db.clone(),
            MockTaskInstancePoolConnector,
        )
        .await?;

        assert_eq!(jcb.state().await, JobState::Ready);
        jcb.start().await?;
        assert_eq!(jcb.state().await, JobState::Running);
        assert_eq!(db.get_state(job_id).await?, JobState::Running);
        Ok(())
    }

    fn create_test_job_submission() -> anyhow::Result<ValidatedJobSubmission> {
        let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
        let mut task_graph = SubmittedTaskGraph::new(None, None)?;
        task_graph.insert_task(TaskDescriptor {
            tdl_context: TdlContext {
                package: "test_pkg".to_owned(),
                task_func: "test_fn".to_owned(),
            },
            execution_policy: Some(ExecutionPolicy::default()),
            inputs: vec![bytes_type.clone()],
            outputs: vec![bytes_type],
            input_sources: None,
        })?;
        Ok(ValidatedJobSubmission::create(
            task_graph,
            vec![TaskInput::ValuePayload(vec![0u8; 4])],
        )?)
    }
}
