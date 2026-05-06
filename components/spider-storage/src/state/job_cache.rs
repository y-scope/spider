use dashmap::{DashMap, mapref::entry::Entry};
use spider_core::types::id::JobId;

use crate::{
    cache::{
        error::{CacheError, InternalError},
        job::SharedJobControlBlock,
    },
    db::InternalJobOrchestration,
    ready_queue::ReadyQueueSender,
    task_instance_pool::TaskInstancePoolConnector,
};

/// An in-memory cache for job control blocks.
///
/// This type provides concurrent access to job control blocks via a `DashMap`. It is generic over
/// the same type parameters as [`SharedJobControlBlock`].
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The type of the ready queue sender.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
pub struct JobCache<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    jobs: DashMap<
        JobId,
        SharedJobControlBlock<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    >,
}

impl<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> JobCache<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    /// Creates a new empty job cache.
    #[must_use]
    pub fn new() -> Self {
        Self {
            jobs: DashMap::new(),
        }
    }

    /// Inserts a job control block into the cache.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`CacheError::Internal`] if a job control block with the same ID already exists.
    pub fn insert(
        &self,
        jcb: SharedJobControlBlock<
            ReadyQueueSenderType,
            DbConnectorType,
            TaskInstancePoolConnectorType,
        >,
    ) -> Result<(), CacheError> {
        let job_id = jcb.id();
        match self.jobs.entry(job_id) {
            Entry::Vacant(e) => {
                e.insert(jcb);
                Ok(())
            }
            Entry::Occupied(_) => Err(InternalError::JobAlreadyExists(job_id).into()),
        }
    }

    /// Gets a job control block from the cache.
    ///
    /// # Returns
    ///
    /// The job control block of the given ID if it exists, [`None`] otherwise.
    #[must_use]
    pub fn get(
        &self,
        job_id: JobId,
    ) -> Option<
        SharedJobControlBlock<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    > {
        self.jobs.get(&job_id).map(|entry| entry.clone())
    }

    /// Removes a job control block from the cache.
    ///
    /// # Returns
    ///
    /// The removed job control block if it existed, [`None`] otherwise.
    #[must_use]
    pub fn remove(
        &self,
        job_id: JobId,
    ) -> Option<
        SharedJobControlBlock<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    > {
        self.jobs.remove(&job_id).map(|(_, v)| v)
    }

    /// Resends all ready tasks for every job in the cache to the ready queue.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`SharedJobControlBlock::resend_ready_tasks`]'s return values on failure.
    pub async fn resend_ready_tasks(&self) -> Result<(), CacheError> {
        for entry in &self.jobs {
            entry.value().resend_ready_tasks().await?;
        }
        Ok(())
    }
}

impl<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> Default for JobCache<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
            id::JobId,
            io::{TaskInput, TaskOutput},
        },
    };

    use super::*;
    use crate::{
        cache::{
            error::{CacheError, InternalError},
            job::SharedJobControlBlock,
            task::{SharedTaskControlBlock, SharedTerminationTaskControlBlock},
        },
        db::DbError,
        ready_queue::ReadyQueueSender,
        task_instance_pool::{TaskInstanceMetadata, TaskInstancePoolConnector},
    };

    /// A mock ready queue sender for testing.
    #[derive(Clone, Default)]
    struct MockReadyQueueSender;

    #[async_trait::async_trait]
    impl ReadyQueueSender for MockReadyQueueSender {
        async fn send_task_ready(
            &self,
            _rg_id: spider_core::types::id::ResourceGroupId,
            _job_id: JobId,
            _task_indices: Vec<usize>,
        ) -> Result<(), InternalError> {
            Ok(())
        }

        async fn send_commit_ready(
            &self,
            _rg_id: spider_core::types::id::ResourceGroupId,
            _job_id: JobId,
        ) -> Result<(), InternalError> {
            Ok(())
        }

        async fn send_cleanup_ready(
            &self,
            _rg_id: spider_core::types::id::ResourceGroupId,
            _job_id: JobId,
        ) -> Result<(), InternalError> {
            Ok(())
        }
    }

    /// A mock DB connector for testing.
    #[derive(Clone, Default)]
    struct MockDbConnector;

    #[async_trait::async_trait]
    impl InternalJobOrchestration for MockDbConnector {
        async fn start(&self, _job_id: JobId) -> Result<(), DbError> {
            Ok(())
        }

        async fn set_state(&self, _job_id: JobId, _state: JobState) -> Result<(), DbError> {
            Ok(())
        }

        async fn commit_outputs(
            &self,
            _job_id: JobId,
            _outputs: Vec<TaskOutput>,
            _has_commit_task: bool,
        ) -> Result<(), DbError> {
            Ok(())
        }

        async fn cancel(&self, _job_id: JobId, _has_cleanup_task: bool) -> Result<(), DbError> {
            Ok(())
        }

        async fn fail(&self, _job_id: JobId, _error_message: String) -> Result<(), DbError> {
            Ok(())
        }

        async fn delete_expired_terminated_jobs(
            &self,
            _expire_after_sec: u64,
        ) -> Result<Vec<JobId>, DbError> {
            Ok(Vec::new())
        }
    }

    /// A mock task instance pool connector for testing.
    #[derive(Clone, Default)]
    struct MockTaskInstancePoolConnector;

    #[async_trait::async_trait]
    impl TaskInstancePoolConnector for MockTaskInstancePoolConnector {
        fn get_next_available_task_instance_id(&self) -> spider_core::types::id::TaskInstanceId {
            1
        }

        async fn register_task_instance(
            &self,
            _tcb: SharedTaskControlBlock,
            _registration: TaskInstanceMetadata,
        ) -> Result<(), InternalError> {
            Ok(())
        }

        async fn register_termination_task_instance(
            &self,
            _termination_tcb: SharedTerminationTaskControlBlock,
            _registration: TaskInstanceMetadata,
        ) -> Result<(), InternalError> {
            Ok(())
        }
    }

    async fn create_test_jcb(
        job_id: JobId,
    ) -> SharedJobControlBlock<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector>
    {
        let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
        let mut submitted =
            SubmittedTaskGraph::new(None, None).expect("task graph creation should succeed");
        submitted
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: "test_pkg".to_owned(),
                    task_func: "test_fn".to_owned(),
                },
                execution_policy: Some(ExecutionPolicy::default()),
                inputs: vec![bytes_type.clone()],
                outputs: vec![bytes_type],
                input_sources: None,
            })
            .expect("task insertion should succeed");

        SharedJobControlBlock::create(
            job_id,
            spider_core::types::id::ResourceGroupId::new(),
            &submitted,
            vec![TaskInput::ValuePayload(vec![0u8; 4])],
            MockReadyQueueSender,
            MockDbConnector,
            MockTaskInstancePoolConnector,
        )
        .await
        .expect("JCB creation should succeed")
    }

    #[tokio::test]
    async fn job_cache_insert_and_get() -> anyhow::Result<()> {
        let cache: JobCache<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector> =
            JobCache::new();
        let job_id = JobId::new();

        let jcb = create_test_jcb(job_id).await;
        cache.insert(jcb)?;

        let result = cache.get(job_id);
        assert!(result.is_some(), "inserted JCB should be retrievable");
        Ok(())
    }

    #[tokio::test]
    async fn job_cache_remove_returns_inserted_jcb() -> anyhow::Result<()> {
        let cache: JobCache<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector> =
            JobCache::new();
        let job_id = JobId::new();

        let jcb = create_test_jcb(job_id).await;
        cache.insert(jcb)?;

        let removed = cache.remove(job_id);
        assert!(removed.is_some(), "remove should return the JCB");

        let result = cache.get(job_id);
        assert!(result.is_none(), "JCB should no longer exist after removal");
        Ok(())
    }

    #[tokio::test]
    async fn job_cache_get_returns_none_for_nonexistent_job() -> anyhow::Result<()> {
        let cache: JobCache<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector> =
            JobCache::new();
        let job_id = JobId::new();

        let result = cache.get(job_id);
        assert!(
            result.is_none(),
            "get should return None for nonexistent job"
        );
        Ok(())
    }

    #[tokio::test]
    async fn job_cache_insert_duplicate_returns_error() -> anyhow::Result<()> {
        let cache: JobCache<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector> =
            JobCache::new();
        let job_id = JobId::new();

        let jcb1 = create_test_jcb(job_id).await;
        cache.insert(jcb1)?;

        let jcb2 = create_test_jcb(job_id).await;
        let result = cache.insert(jcb2);
        assert!(
            matches!(
                result,
                Err(CacheError::Internal(InternalError::JobAlreadyExists(_)))
            ),
            "insert should return JobAlreadyExists error for duplicate key"
        );
        if let Err(CacheError::Internal(InternalError::JobAlreadyExists(id))) = result {
            assert_eq!(id, job_id, "error should contain the duplicate job ID");
        }
        Ok(())
    }

    #[tokio::test]
    async fn job_cache_concurrent_insert_get() {
        use tokio_util::task::TaskTracker;

        let cache: Arc<
            JobCache<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector>,
        > = Arc::new(JobCache::new());

        let tracker = TaskTracker::new();
        let num_tasks = 10;

        for i in 0..num_tasks {
            let cache = Arc::clone(&cache);
            tracker.spawn(async move {
                let job_id = JobId::new();
                let jcb = create_test_jcb(job_id).await;
                cache
                    .insert(jcb)
                    .expect("insert should succeed for new job");

                let result = cache.get(job_id);
                assert!(result.is_some(), "task {i} should find inserted JCB");

                let removed = cache.remove(job_id);
                assert!(removed.is_some(), "task {i} should remove inserted JCB");

                let result = cache.get(job_id);
                assert!(result.is_none(), "task {i} should not find removed JCB");
            });
        }

        tracker.close();
        tracker.wait().await;
    }

    /// A tracking ready queue sender that records calls.
    #[derive(Clone, Default)]
    struct TrackingReadyQueueSender {
        calls: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl ReadyQueueSender for TrackingReadyQueueSender {
        async fn send_task_ready(
            &self,
            _rg_id: spider_core::types::id::ResourceGroupId,
            _job_id: JobId,
            _task_indices: Vec<usize>,
        ) -> Result<(), InternalError> {
            self.calls
                .lock()
                .expect("lock")
                .push("task_ready".to_owned());
            Ok(())
        }

        async fn send_commit_ready(
            &self,
            _rg_id: spider_core::types::id::ResourceGroupId,
            _job_id: JobId,
        ) -> Result<(), InternalError> {
            self.calls
                .lock()
                .expect("lock")
                .push("commit_ready".to_owned());
            Ok(())
        }

        async fn send_cleanup_ready(
            &self,
            _rg_id: spider_core::types::id::ResourceGroupId,
            _job_id: JobId,
        ) -> Result<(), InternalError> {
            self.calls
                .lock()
                .expect("lock")
                .push("cleanup_ready".to_owned());
            Ok(())
        }
    }

    #[tokio::test]
    async fn job_cache_resend_ready_tasks_sends_for_running_job() -> anyhow::Result<()> {
        let calls: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
        let sender = TrackingReadyQueueSender {
            calls: Arc::clone(&calls),
        };

        let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
        let mut submitted =
            SubmittedTaskGraph::new(None, None).expect("task graph creation should succeed");
        submitted
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: "test_pkg".to_owned(),
                    task_func: "test_fn".to_owned(),
                },
                execution_policy: Some(ExecutionPolicy::default()),
                inputs: vec![bytes_type.clone()],
                outputs: vec![bytes_type],
                input_sources: None,
            })
            .expect("task insertion should succeed");

        let job_id = JobId::new();
        let jcb = SharedJobControlBlock::create(
            job_id,
            spider_core::types::id::ResourceGroupId::new(),
            &submitted,
            vec![TaskInput::ValuePayload(vec![0u8; 4])],
            sender,
            MockDbConnector,
            MockTaskInstancePoolConnector,
        )
        .await
        .expect("JCB creation should succeed");
        jcb.start().await.expect("start should succeed");
        calls.lock().expect("lock").clear();

        let cache: JobCache<
            TrackingReadyQueueSender,
            MockDbConnector,
            MockTaskInstancePoolConnector,
        > = JobCache::new();
        cache.insert(jcb)?;

        cache.resend_ready_tasks().await?;

        let task_ready_count = {
            let recorded = calls.lock().expect("lock");
            recorded.iter().filter(|c| **c == "task_ready").count()
        };
        assert_eq!(
            task_ready_count, 1,
            "resend_ready_tasks should send one task_ready after the reset"
        );
        Ok(())
    }
}
