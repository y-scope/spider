use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use spider_core::types::id::JobId;
use tokio::sync::RwLock;

use crate::cache::error::CacheError;
use crate::cache::error::InternalError;
use crate::cache::job::SharedJobControlBlock;
use crate::inbound_queue::InboundQueueSender;
use crate::state::StorageServerError;
use crate::task_instance_pool::TaskInstancePoolConnector;

/// An in-memory cache for job control blocks.
///
/// This type provides concurrent access to job control blocks via a [`tokio::sync::RwLock`] over a
/// [`HashMap`]. It is generic over the same type parameters as [`SharedJobControlBlock`].
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
#[derive(Clone)]
pub struct JobCache<
    InboundQueueSenderType: InboundQueueSender,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    jobs: SharedJobMap<InboundQueueSenderType, TaskInstancePoolConnectorType>,
}

impl<
    InboundQueueSenderType: InboundQueueSender,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> JobCache<InboundQueueSenderType, TaskInstancePoolConnectorType>
{
    /// Creates a new empty job cache.
    #[must_use]
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Inserts a job control block into the cache.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::JobAlreadyExists`] (wrapped in [`StorageServerError::Cache`]) if a job
    ///   control block with the same ID already exists.
    pub async fn insert(
        &self,
        jcb: SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    ) -> Result<(), StorageServerError> {
        let job_id = jcb.id();
        match self.jobs.write().await.entry(job_id) {
            Entry::Vacant(e) => {
                e.insert(jcb);
                Ok(())
            }
            Entry::Occupied(_) => Err(StorageServerError::Cache(CacheError::Internal(
                InternalError::JobAlreadyExists(job_id),
            ))),
        }
    }

    /// Inserts job control blocks into the cache under one write lock.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::JobAlreadyExists`] if a job control block with the ID of one of `jcbs`
    ///   already exists, or two of `jcbs` share an ID. The job control blocks before it stay
    ///   inserted.
    pub async fn insert_many(
        &self,
        jcbs: Vec<SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>>,
    ) -> Result<(), InternalError> {
        let mut job_map = self.jobs.write().await;
        for jcb in jcbs {
            match job_map.entry(jcb.id()) {
                Entry::Vacant(entry) => {
                    entry.insert(jcb);
                }
                Entry::Occupied(entry) => {
                    return Err(InternalError::JobAlreadyExists(*entry.key()));
                }
            }
        }
        drop(job_map);
        Ok(())
    }

    /// Gets a job control block from the cache.
    ///
    /// # Returns
    ///
    /// The job control block of the given ID if it exists, [`None`] otherwise.
    pub async fn get(
        &self,
        job_id: JobId,
    ) -> Option<SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>> {
        self.jobs.read().await.get(&job_id).cloned()
    }

    /// Removes a job control block from the cache.
    ///
    /// # Returns
    ///
    /// The removed job control block if it existed, [`None`] otherwise.
    pub async fn remove(
        &self,
        job_id: JobId,
    ) -> Option<SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>> {
        self.jobs.write().await.remove(&job_id)
    }

    /// Removes multiple job control blocks from the cache.
    ///
    /// # Returns
    ///
    /// The number of job control blocks that existed and were removed.
    pub async fn remove_batch(&self, job_ids: &[JobId]) -> usize {
        let mut jobs = self.jobs.write().await;
        job_ids
            .iter()
            .filter(|job_id| jobs.remove(job_id).is_some())
            .count()
    }

    /// Resends all ready tasks for every job in the cache to the inbound queue.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`SharedJobControlBlock::resend_ready_tasks`]'s return values on failure.
    pub async fn resend_ready_tasks(&self) -> Result<(), StorageServerError> {
        for jcb in self.jobs.read().await.values() {
            jcb.resend_ready_tasks().await?;
        }
        Ok(())
    }
}

impl<
    InboundQueueSenderType: InboundQueueSender,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> Default for JobCache<InboundQueueSenderType, TaskInstancePoolConnectorType>
{
    fn default() -> Self {
        Self::new()
    }
}

type JobMap<InboundQueueSenderType, TaskInstancePoolConnectorType> =
    HashMap<JobId, SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>>;

type SharedJobMap<InboundQueueSenderType, TaskInstancePoolConnectorType> =
    Arc<RwLock<JobMap<InboundQueueSenderType, TaskInstancePoolConnectorType>>>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use spider_core::job::JobState;
    use spider_core::task::DataTypeDescriptor;
    use spider_core::task::ExecutionPolicy;
    use spider_core::task::TaskDescriptor;
    use spider_core::task::TaskGraph as SubmittedTaskGraph;
    use spider_core::task::TdlContext;
    use spider_core::task::ValueTypeDescriptor;
    use spider_core::types::id::JobId;
    use spider_core::types::io::TaskGraphInputBuilder;
    use spider_utils::shutdown::ShutdownHandle;

    use super::*;
    use crate::cache::error::InternalError;
    use crate::cache::job::SharedJobControlBlock;
    use crate::cache::tree::TreeControlBlock;
    use crate::inbound_queue::InboundQueueSender;
    use crate::job_submission::create_validated_submission;
    use crate::state::test_utils::MockInboundQueueSender;
    use crate::state::test_utils::MockTaskInstancePoolConnector;
    use crate::test_hooks::TestHooks;

    type TestJcb = SharedJobControlBlock<MockInboundQueueSender, MockTaskInstancePoolConnector>;

    async fn create_test_jcb(job_id: JobId) -> TestJcb {
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

        let mut task_graph_input_builder = TaskGraphInputBuilder::new();
        task_graph_input_builder
            .append_task_input(&[0u8; 4])
            .expect("task input appending should succeed");
        let job_submission =
            create_validated_submission(submitted, task_graph_input_builder.build());
        SharedJobControlBlock::create(
            job_id,
            spider_core::types::id::ResourceGroupId::random(),
            job_submission,
            MockInboundQueueSender,
            MockTaskInstancePoolConnector,
            TestHooks::default(),
            TreeControlBlock::new(ShutdownHandle::new()).0,
        )
        .await
        .expect("JCB creation should succeed")
    }

    #[tokio::test]
    async fn job_cache_insert_and_get() -> anyhow::Result<()> {
        let cache: JobCache<MockInboundQueueSender, MockTaskInstancePoolConnector> =
            JobCache::new();
        let job_id = JobId::random();

        let jcb = create_test_jcb(job_id).await;
        cache.insert(jcb).await?;

        let result = cache.get(job_id).await;
        assert!(result.is_some(), "inserted JCB should be retrievable");
        Ok(())
    }

    #[tokio::test]
    async fn job_cache_remove_returns_inserted_jcb() -> anyhow::Result<()> {
        let cache: JobCache<MockInboundQueueSender, MockTaskInstancePoolConnector> =
            JobCache::new();
        let job_id = JobId::random();

        let jcb = create_test_jcb(job_id).await;
        cache.insert(jcb).await?;

        let removed = cache.remove(job_id).await;
        assert!(removed.is_some(), "remove should return the JCB");

        let result = cache.get(job_id).await;
        assert!(result.is_none(), "JCB should no longer exist after removal");
        Ok(())
    }

    #[tokio::test]
    async fn job_cache_remove_batch_removes_existing_jobs_once() -> anyhow::Result<()> {
        let cache: JobCache<MockInboundQueueSender, MockTaskInstancePoolConnector> =
            JobCache::new();
        let first_job_id = JobId::random();
        let second_job_id = JobId::random();
        let missing_job_id = JobId::random();

        cache.insert(create_test_jcb(first_job_id).await).await?;
        cache.insert(create_test_jcb(second_job_id).await).await?;

        let num_removed_jobs = cache
            .remove_batch(&[first_job_id, missing_job_id, second_job_id, second_job_id])
            .await;

        assert_eq!(
            num_removed_jobs, 2,
            "remove_batch should count only existing jobs"
        );
        assert!(
            cache.get(first_job_id).await.is_none(),
            "first job should be removed"
        );
        assert!(
            cache.get(second_job_id).await.is_none(),
            "second job should be removed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn job_cache_get_returns_none_for_nonexistent_job() -> anyhow::Result<()> {
        let cache: JobCache<MockInboundQueueSender, MockTaskInstancePoolConnector> =
            JobCache::new();
        let job_id = JobId::random();

        let result = cache.get(job_id).await;
        assert!(
            result.is_none(),
            "get should return None for nonexistent job"
        );
        Ok(())
    }

    #[tokio::test]
    async fn job_cache_insert_duplicate_returns_error() -> anyhow::Result<()> {
        let cache: JobCache<MockInboundQueueSender, MockTaskInstancePoolConnector> =
            JobCache::new();
        let job_id = JobId::random();

        let jcb1 = create_test_jcb(job_id).await;
        cache.insert(jcb1).await?;

        let jcb2 = create_test_jcb(job_id).await;
        let result = cache.insert(jcb2).await;
        assert!(
            matches!(
                result,
                Err(StorageServerError::Cache(CacheError::Internal(
                    InternalError::JobAlreadyExists(_)
                )))
            ),
            "insert should return JobAlreadyExists error for duplicate key"
        );
        if let Err(StorageServerError::Cache(CacheError::Internal(
            InternalError::JobAlreadyExists(id),
        ))) = result
        {
            assert_eq!(id, job_id, "error should contain the duplicate job ID");
        }
        Ok(())
    }

    #[tokio::test]
    async fn insert_many_inserts_jcbs_until_an_id_that_is_cached_or_repeated() -> anyhow::Result<()>
    {
        let cache: JobCache<MockInboundQueueSender, MockTaskInstancePoolConnector> =
            JobCache::new();
        let existing_job_id = JobId::random();
        cache.insert(create_test_jcb(existing_job_id).await).await?;

        let duplicated_job_id = JobId::random();
        for job_ids in [
            [JobId::random(), existing_job_id],
            [duplicated_job_id, duplicated_job_id],
        ] {
            let mut jcbs = Vec::new();
            for job_id in job_ids {
                jcbs.push(create_test_jcb(job_id).await);
            }
            let result = cache.insert_many(jcbs).await;
            assert!(
                matches!(
                    result,
                    Err(InternalError::JobAlreadyExists(job_id)) if job_id == job_ids[1]
                ),
                "an insert of {job_ids:?} gave {result:?}"
            );
            assert!(cache.get(job_ids[0]).await.is_some());
        }

        let new_job_ids = [JobId::random(), JobId::random()];
        let mut jcbs = Vec::new();
        for job_id in new_job_ids {
            jcbs.push(create_test_jcb(job_id).await);
        }
        cache.insert_many(jcbs).await?;
        for job_id in new_job_ids {
            assert!(cache.get(job_id).await.is_some());
        }
        Ok(())
    }

    #[tokio::test]
    async fn job_cache_concurrent_insert_get() {
        use tokio_util::task::TaskTracker;

        let cache: Arc<JobCache<MockInboundQueueSender, MockTaskInstancePoolConnector>> =
            Arc::new(JobCache::new());

        let tracker = TaskTracker::new();
        let num_tasks = 10;

        for i in 0..num_tasks {
            let cache = Arc::clone(&cache);
            tracker.spawn(async move {
                let job_id = JobId::random();
                let jcb = create_test_jcb(job_id).await;
                cache
                    .insert(jcb)
                    .await
                    .expect("insert should succeed for new job");

                let result = cache.get(job_id).await;
                assert!(result.is_some(), "task {i} should find inserted JCB");

                let removed = cache.remove(job_id).await;
                assert!(removed.is_some(), "task {i} should remove inserted JCB");

                let result = cache.get(job_id).await;
                assert!(result.is_none(), "task {i} should not find removed JCB");
            });
        }

        tracker.close();
        tracker.wait().await;
    }

    /// A tracking inbound queue sender that records the number of calls.
    #[derive(Clone, Default)]
    struct TrackingInboundQueueSender {
        call_count: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl InboundQueueSender for TrackingInboundQueueSender {
        async fn send_task_ready(
            &self,
            _rg_id: spider_core::types::id::ResourceGroupId,
            _job_id: JobId,
            _task_indices: Vec<usize>,
        ) -> Result<(), InternalError> {
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        async fn send_commit_ready(
            &self,
            _rg_id: spider_core::types::id::ResourceGroupId,
            _job_id: JobId,
        ) -> Result<(), InternalError> {
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        async fn send_cleanup_ready(
            &self,
            _rg_id: spider_core::types::id::ResourceGroupId,
            _job_id: JobId,
        ) -> Result<(), InternalError> {
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn job_cache_resend_ready_tasks_sends_for_running_job() -> anyhow::Result<()> {
        let call_count: Arc<std::sync::atomic::AtomicUsize> = Arc::default();
        let sender = TrackingInboundQueueSender {
            call_count: Arc::clone(&call_count),
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

        let job_id = JobId::random();
        let mut task_graph_input_builder = TaskGraphInputBuilder::new();
        task_graph_input_builder
            .append_task_input(&[0u8; 4])
            .expect("task input appending should succeed");
        let job_submission =
            create_validated_submission(submitted, task_graph_input_builder.build());
        let jcb = SharedJobControlBlock::create(
            job_id,
            spider_core::types::id::ResourceGroupId::random(),
            job_submission,
            sender,
            MockTaskInstancePoolConnector,
            TestHooks::default(),
            TreeControlBlock::new(ShutdownHandle::new()).0,
        )
        .await
        .expect("JCB creation should succeed");
        jcb.apply_tree_state(JobState::Ready, JobState::Running)
            .await?;

        let cache: JobCache<TrackingInboundQueueSender, MockTaskInstancePoolConnector> =
            JobCache::new();
        cache.insert(jcb).await?;

        cache.resend_ready_tasks().await?;

        assert_eq!(
            call_count.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "resend_ready_tasks should send one call"
        );
        Ok(())
    }
}
