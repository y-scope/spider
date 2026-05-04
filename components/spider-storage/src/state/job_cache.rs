use dashmap::DashMap;
use spider_core::types::id::JobId;

use crate::{
    cache::job::SharedJobControlBlock,
    db::InternalJobOrchestration,
    ready_queue::ReadyQueueSender,
    task_instance_pool::TaskInstancePoolConnector,
};

/// An in-memory cache for job control blocks.
///
/// This type provides concurrent access to job control blocks via a `DashMap`.
/// It is generic over the same type parameters as [`SharedJobControlBlock`].
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
    /// If a job control block with the same ID already exists, it will be replaced.
    pub fn insert(
        &self,
        job_id: JobId,
        jcb: SharedJobControlBlock<
            ReadyQueueSenderType,
            DbConnectorType,
            TaskInstancePoolConnectorType,
        >,
    ) {
        self.jobs.insert(job_id, jcb);
    }

    /// Gets a job control block from the cache.
    ///
    /// # Returns
    ///
    /// A clone of the job control block if it exists, or [`None`] if not found.
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
    /// The removed job control block if it existed, or [`None`] if not found.
    #[must_use]
    pub fn remove(
        &self,
        job_id: JobId,
    ) -> Option<
        SharedJobControlBlock<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    > {
        self.jobs.remove(&job_id).map(|(_, v)| v)
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
    use spider_core::{
        task::{
            DataTypeDescriptor,
            ExecutionPolicy,
            TaskDescriptor,
            TaskGraph as SubmittedTaskGraph,
            TdlContext,
            ValueTypeDescriptor,
        },
        types::{id::JobId, io::TaskInput},
    };

    use super::*;
    use crate::{
        cache::error::InternalError,
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
    impl crate::db::InternalJobOrchestration for MockDbConnector {
        async fn start(&self, _job_id: JobId) -> Result<(), crate::db::DbError> {
            Ok(())
        }

        async fn set_state(
            &self,
            _job_id: JobId,
            _state: spider_core::job::JobState,
        ) -> Result<(), crate::db::DbError> {
            Ok(())
        }

        async fn commit_outputs(
            &self,
            _job_id: JobId,
            _outputs: Vec<spider_core::types::io::TaskOutput>,
            _has_commit_task: bool,
        ) -> Result<(), crate::db::DbError> {
            Ok(())
        }

        async fn cancel(
            &self,
            _job_id: JobId,
            _has_cleanup_task: bool,
        ) -> Result<(), crate::db::DbError> {
            Ok(())
        }

        async fn fail(
            &self,
            _job_id: JobId,
            _error_message: String,
        ) -> Result<(), crate::db::DbError> {
            Ok(())
        }

        async fn delete_expired_terminated_jobs(
            &self,
            _expire_after_sec: u64,
        ) -> Result<Vec<JobId>, crate::db::DbError> {
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
            _tcb: crate::cache::task::SharedTaskControlBlock,
            _registration: TaskInstanceMetadata,
        ) -> Result<(), InternalError> {
            Ok(())
        }

        async fn register_termination_task_instance(
            &self,
            _termination_tcb: crate::cache::task::SharedTerminationTaskControlBlock,
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
    async fn job_cache_insert_and_get_roundtrip() {
        let cache: JobCache<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector> =
            JobCache::new();
        let job_id = JobId::new();

        let jcb = create_test_jcb(job_id).await;
        cache.insert(job_id, jcb);

        let result = cache.get(job_id);
        assert!(result.is_some(), "inserted JCB should be retrievable");
    }

    #[tokio::test]
    async fn job_cache_remove_returns_inserted_jcb() {
        let cache: JobCache<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector> =
            JobCache::new();
        let job_id = JobId::new();

        let jcb = create_test_jcb(job_id).await;
        cache.insert(job_id, jcb);

        let removed = cache.remove(job_id);
        assert!(removed.is_some(), "remove should return the JCB");

        let result = cache.get(job_id);
        assert!(result.is_none(), "JCB should no longer exist after removal");
    }

    #[tokio::test]
    async fn job_cache_get_returns_none_for_nonexistent_job() {
        let cache: JobCache<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector> =
            JobCache::new();
        let job_id = JobId::new();

        let result = cache.get(job_id);
        assert!(
            result.is_none(),
            "get should return None for nonexistent job"
        );
    }

    #[tokio::test]
    async fn job_cache_concurrent_insert_get() {
        use std::sync::Arc;

        let cache: Arc<
            JobCache<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector>,
        > = Arc::new(JobCache::new());

        let num_tasks = 10;
        let mut handles = Vec::new();

        for i in 0..num_tasks {
            let cache = Arc::clone(&cache);
            let handle = tokio::spawn(async move {
                let job_id = JobId::new();
                let jcb = create_test_jcb(job_id).await;
                cache.insert(job_id, jcb);

                // Immediately try to get it
                let result = cache.get(job_id);
                assert!(result.is_some(), "task {i} should find inserted JCB");

                // Try to remove it
                let removed = cache.remove(job_id);
                assert!(removed.is_some(), "task {i} should remove inserted JCB");

                // Verify it's gone
                let result = cache.get(job_id);
                assert!(result.is_none(), "task {i} should not find removed JCB");
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("task should complete successfully");
        }
    }
}
