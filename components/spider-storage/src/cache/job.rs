use std::sync::Arc;
use std::sync::PoisonError;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use spider_core::job::JobState;
use spider_core::task::TaskIndex;
use spider_core::task::TaskState;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::ExecutionContext;
use spider_core::types::io::SerializedTaskOutputs;
use spider_core::types::io::TaskOutput;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;

use crate::cache::error::CacheError;
use crate::cache::error::InternalError;
use crate::cache::error::StaleStateError;
use crate::cache::task::PreparedChild;
use crate::cache::task::TaskGraph;
use crate::cache::tree::ChildJobUsage;
use crate::cache::tree::Lane;
use crate::cache::tree::TreeControlBlock;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::TreeEventKind;
use crate::cache::tree::TreeEventOutcome;
use crate::cache::tree::TreeNode;
use crate::db::RecoverableJobContext;
use crate::inbound_queue::InboundQueueSender;
use crate::job_submission::ValidatedJobSubmission;
use crate::partial_mutation::PartialMutationGuard;
use crate::task_instance_pool::TaskInstanceMetadata;
use crate::task_instance_pool::TaskInstancePoolConnector;
use crate::test_hooks::HookContext;
use crate::test_hooks::HookPoint;
use crate::test_hooks::TestHooks;

/// A shareable control block for a job.
///
/// All mutable state but the job's child reservation, including the task graph, connectors, and
/// inbound-queue sender, is held inside the underlying [`JobExecutionState`] and protected by
/// [`JobExecutionStateHandle`]'s read-write lock.
/// Its job-level transitions are events of the job's tree, which the tree's committer applies.
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
#[derive(Clone)]
pub struct SharedJobControlBlock<
    InboundQueueSenderType: InboundQueueSender,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    inner: Arc<JobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>>,
}

impl<
    InboundQueueSenderType: InboundQueueSender,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The created [`SharedJobControlBlock`], whose job-level events go to `tree`, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TaskGraph::create`]'s return values on failure.
    pub async fn create(
        id: JobId,
        owner_id: ResourceGroupId,
        job_submission: ValidatedJobSubmission,
        inbound_queue_sender: InboundQueueSenderType,
        task_instance_pool_connector: TaskInstancePoolConnectorType,
        hooks: TestHooks,
        tree: TreeControlBlock,
    ) -> Result<Self, CacheError> {
        let num_tasks = job_submission.task_graph().get_num_tasks();
        let task_graph = TaskGraph::create(job_submission).await?;
        let job_execution_state = JobExecutionState {
            state: JobState::Ready,
            task_graph,
            num_incomplete_tasks: AtomicUsize::new(num_tasks),
            inbound_queue_sender,
            task_instance_pool_connector,
        };
        Ok(Self {
            inner: Arc::new(JobControlBlock {
                id,
                owner_id,
                parent_id: None,
                depth: 0,
                reserved_children_usage: std::sync::Mutex::default(),
                hooks,
                tree,
                job_execution_state: JobExecutionStateHandle {
                    inner: tokio::sync::RwLock::new(job_execution_state),
                },
            }),
        })
    }

    /// Factory function.
    ///
    /// Creates the control block of a child of `parent` from its prepared child: a
    /// [`JobState::Running`] job one level below `parent`, owned by `parent`'s owner, in `parent`'s
    /// tree.
    ///
    /// # Returns
    ///
    /// The created [`SharedJobControlBlock`].
    pub async fn from_prepared_child(
        id: JobId,
        parent: &Self,
        prepared_child: PreparedChild,
    ) -> Self {
        let parent_jcb = &parent.inner;
        let parent_execution_state = parent_jcb.job_execution_state.read().await;
        let inbound_queue_sender = parent_execution_state.inbound_queue_sender.clone();
        let task_instance_pool_connector =
            parent_execution_state.task_instance_pool_connector.clone();
        drop(parent_execution_state);
        let (task_graph, num_tasks) = prepared_child.into_parts();
        let job_execution_state = JobExecutionState {
            state: JobState::Running,
            task_graph,
            num_incomplete_tasks: AtomicUsize::new(num_tasks),
            inbound_queue_sender,
            task_instance_pool_connector,
        };
        Self {
            inner: Arc::new(JobControlBlock {
                id,
                owner_id: parent_jcb.owner_id,
                parent_id: Some(parent_jcb.id),
                depth: parent_jcb.depth + 1,
                reserved_children_usage: std::sync::Mutex::default(),
                hooks: parent_jcb.hooks.clone(),
                tree: parent_jcb.tree.clone(),
                job_execution_state: JobExecutionStateHandle {
                    inner: tokio::sync::RwLock::new(job_execution_state),
                },
            }),
        }
    }

    /// Recovers a job control block from the given recoverable job context (assumed to be returned
    /// from persistent storage).
    ///
    /// # NOTE
    ///
    /// * This constructor does not mutate the storage states.
    /// * This constructor does not send recovered tasks to the inbound-queue.
    ///
    /// # Returns
    ///
    /// The recovered [`SharedJobControlBlock`], whose job-level events go to `tree`, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UnexpectedJobState`] if `state` is not recoverable.
    /// * [`InternalError::InvalidRecoverableJobContext`] if:
    ///   * A commit-ready job doesn't have a commit task specified.
    ///   * A commit-ready job doesn't have outputs ready.
    ///   * A cleanup-ready job doesn't have a cleanup task specified.
    /// * Forwards [`TaskGraph::create`]'s return values on failure.
    pub async fn recover(
        recoverable_job_context: RecoverableJobContext,
        inbound_queue_sender: InboundQueueSenderType,
        task_instance_pool_connector: TaskInstancePoolConnectorType,
        hooks: TestHooks,
        tree: TreeControlBlock,
    ) -> Result<Self, CacheError> {
        let RecoverableJobContext {
            id,
            resource_group_id,
            state,
            submission,
            outputs,
        } = recoverable_job_context;

        let (num_incomplete_tasks, task_graph) = match state {
            JobState::Ready | JobState::Running => {
                let num_tasks = submission.task_graph().get_num_tasks();
                let task_graph = TaskGraph::create(submission).await?;
                (num_tasks, task_graph)
            }
            JobState::CommitReady => {
                let Some(commit_task_descriptor) =
                    submission.task_graph().get_commit_task_descriptor()
                else {
                    return Err(InternalError::InvalidRecoverableJobContext(String::from(
                        "commit-ready job doesn't have a commit task specified",
                    ))
                    .into());
                };
                let Some(outputs) = outputs else {
                    return Err(InternalError::InvalidRecoverableJobContext(String::from(
                        "commit-ready job doesn't have outputs ready",
                    ))
                    .into());
                };
                let task_graph = TaskGraph::recover_from_commit_ready(
                    commit_task_descriptor,
                    submission.task_graph().get_cleanup_task_descriptor(),
                    outputs,
                );
                (0, task_graph)
            }
            JobState::CleanupReady => {
                let Some(cleanup_task_descriptor) =
                    submission.task_graph().get_cleanup_task_descriptor()
                else {
                    return Err(InternalError::InvalidRecoverableJobContext(String::from(
                        "cleanup-ready job doesn't have a cleanup task specified",
                    ))
                    .into());
                };
                let task_graph = TaskGraph::recover_from_cleanup_ready(cleanup_task_descriptor);
                (0, task_graph)
            }
            other => {
                return Err(InternalError::UnexpectedJobState {
                    current: other,
                    expected: JobState::Running,
                }
                .into());
            }
        };

        let job_execution_state = JobExecutionState {
            state,
            task_graph,
            num_incomplete_tasks: AtomicUsize::new(num_incomplete_tasks),
            inbound_queue_sender,
            task_instance_pool_connector,
        };
        Ok(Self {
            inner: Arc::new(JobControlBlock {
                id,
                owner_id: resource_group_id,
                parent_id: None,
                depth: 0,
                reserved_children_usage: std::sync::Mutex::default(),
                hooks,
                tree,
                job_execution_state: JobExecutionStateHandle {
                    inner: tokio::sync::RwLock::new(job_execution_state),
                },
            }),
        })
    }

    /// Returns the job ID.
    #[must_use]
    pub fn id(&self) -> JobId {
        self.inner.id
    }

    /// # Returns
    ///
    /// The current job state.
    pub async fn state(&self) -> JobState {
        self.inner.job_execution_state.read_state().await
    }

    /// # Returns
    ///
    /// The ID of the resource group that owns the job.
    #[must_use]
    pub fn owner_id(&self) -> ResourceGroupId {
        self.inner.owner_id
    }

    /// # Returns
    ///
    /// The ID of the job's parent, or [`None`] for a root.
    #[must_use]
    pub fn parent_id(&self) -> Option<JobId> {
        self.inner.parent_id
    }

    /// # Returns
    ///
    /// The job's depth in its tree, 0 for a root.
    #[must_use]
    pub fn depth(&self) -> u32 {
        self.inner.depth
    }

    /// Reserves `children_usage` on top of the children usage that the job's earlier reports
    /// reserved, if the job's children stay within the limits of one job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ChildJobUsage`] with the total children usage that the job's children would reach, if
    ///   they exceed the limits of one job (see [`ChildJobUsage::is_within_job_limits`]). Nothing
    ///   is reserved then.
    pub fn reserve_children_usage(
        &self,
        children_usage: ChildJobUsage,
    ) -> Result<(), ChildJobUsage> {
        let mut reserved_children_usage = self
            .inner
            .reserved_children_usage
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let total_children_usage = *reserved_children_usage + children_usage;
        if !total_children_usage.is_within_job_limits() {
            return Err(total_children_usage);
        }
        *reserved_children_usage = total_children_usage;
        drop(reserved_children_usage);
        Ok(())
    }

    /// Reverts a reservation of [`Self::reserve_children_usage`], saturating at zero.
    pub fn revert_children_usage_reservation(&self, children_usage: ChildJobUsage) {
        let mut reserved_children_usage = self
            .inner
            .reserved_children_usage
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *reserved_children_usage = *reserved_children_usage - children_usage;
    }

    /// # Returns
    ///
    /// The job as a node of its tree, without an active child: its state, its parent and whether it
    /// has a commit task and a cleanup task.
    pub(crate) async fn tree_node(&self) -> TreeNode {
        let job = self.inner.job_execution_state.read().await;
        let node = TreeNode {
            state: job.state,
            parent: self.inner.parent_id,
            has_commit: job.task_graph.has_commit_task(),
            has_cleanup: job.task_graph.has_cleanup_task(),
            active_children: 0,
        };
        drop(job);
        node
    }

    /// # Returns
    ///
    /// The indices of the job's ready tasks on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionStateHandle::read_running`]'s return values on failure.
    pub(crate) async fn get_ready_task_indices(&self) -> Result<Vec<TaskIndex>, CacheError> {
        let job = self.inner.job_execution_state.read_running().await?;
        let ready_task_indices = job.task_graph.get_all_ready_task_indices().await;
        drop(job);
        Ok(ready_task_indices)
    }

    /// Gets the outputs of the job from the in-memory task graph.
    ///
    /// # Returns
    ///
    /// The outputs of the job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionStateHandle::read_succeeded`]'s return values on failure.
    /// * Forwards [`TaskGraph::read_output_payloads`]'s return values on failure.
    pub async fn get_outputs(&self) -> Result<Vec<TaskOutput>, CacheError> {
        let jcb = &self.inner;
        let job = jcb.job_execution_state.read_succeeded().await?;
        let outputs = job.task_graph.read_output_payloads().await?;
        drop(job);
        Ok(outputs)
    }

    /// Starts the job: submits its start, with the job's ready tasks, to the job's tree and waits
    /// until the committer has applied it. The tree's committer publishes the ready tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskGraphCorrupted`] if the job has no ready task.
    /// * Forwards [`JobExecutionStateHandle::read_ready`]'s return values on failure.
    /// * Forwards [`Self::submit_event_and_wait_for_reply`]'s return values on failure.
    pub async fn start(&self) -> Result<(), CacheError> {
        let jcb = &self.inner;
        let job = jcb.job_execution_state.read_ready().await?;
        let ready_task_indices = job.task_graph.get_all_ready_task_indices().await;
        drop(job);
        if ready_task_indices.is_empty() {
            return Err(InternalError::TaskGraphCorrupted(
                "initial task graph has no ready tasks".to_owned(),
            )
            .into());
        }
        self.submit_event_and_wait_for_reply(TreeEventKind::Start { ready_task_indices }, None)
            .await?;
        Ok(())
    }

    /// Resends all ready tasks to the inbound queue.
    ///
    /// The method handles the following job states:
    ///
    /// * [`JobState::Running`] — all tasks in [`TaskState::Ready`] via
    ///   [`InboundQueueSender::send_task_ready`].
    /// * [`JobState::CommitReady`] — the commit task via [`InboundQueueSender::send_commit_ready`].
    /// * [`JobState::CleanupReady`] — the cleanup task via
    ///   [`InboundQueueSender::send_cleanup_ready`].
    ///
    /// For other job states, this method is a no-op and returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`InboundQueueSender::send_task_ready`]'s return values on failure.
    /// * Forwards [`InboundQueueSender::send_commit_ready`]'s return values on failure.
    /// * Forwards [`InboundQueueSender::send_cleanup_ready`]'s return values on failure.
    pub async fn resend_ready_tasks(&self) -> Result<(), CacheError> {
        let jcb = &self.inner;

        if let Ok(job) = jcb.job_execution_state.read_running().await {
            let ready_task_indices = job.task_graph.get_all_ready_task_indices().await;
            if !ready_task_indices.is_empty() {
                job.inbound_queue_sender
                    .send_task_ready(jcb.owner_id, jcb.id, ready_task_indices)
                    .await?;
            }
            return Ok(());
        }

        if let Ok(job) = jcb.job_execution_state.read_commit_ready().await {
            job.inbound_queue_sender
                .send_commit_ready(jcb.owner_id, jcb.id)
                .await?;
            return Ok(());
        }

        if let Ok(job) = jcb.job_execution_state.read_cleanup_ready().await {
            job.inbound_queue_sender
                .send_cleanup_ready(jcb.owner_id, jcb.id)
                .await?;
            return Ok(());
        }

        Ok(())
    }

    /// Creates a task instance for the given task and registers it in the task instance pool.
    ///
    /// # Returns
    ///
    /// The execution context for the created task instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::create_regular_task_instance`]'s return values on failure.
    /// * Forwards [`Self::create_commit_task_instance`]'s return values on failure.
    /// * Forwards [`Self::create_cleanup_task_instance`]'s return values on failure.
    pub async fn create_task_instance(
        &self,
        task_id: TaskId,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<ExecutionContext, CacheError> {
        let jcb = &self.inner;
        match task_id {
            TaskId::Index(task_index) => {
                Self::create_regular_task_instance(jcb, task_index, execution_manager_id).await
            }
            TaskId::Commit => Self::create_commit_task_instance(jcb, execution_manager_id).await,
            TaskId::Cleanup => Self::create_cleanup_task_instance(jcb, execution_manager_id).await,
        }
    }

    /// Marks the task instance as succeeded and stores the children that its report prepared in
    /// the task.
    ///
    /// If all tasks have succeeded, submits the job's outputs and the children that its tasks
    /// prepared to the job's tree and waits until the committer has stored them and moved the job
    /// on. Otherwise, if the completed task unblocks any child tasks, those child tasks are
    /// enqueued to the inbound queue.
    ///
    /// # Returns
    ///
    /// The current [`JobState`] after the operation on success. Must be one of:
    ///
    /// * [`JobState::Running`]
    /// * [`JobState::BlockingOnChildren`] if the job's tasks prepared children.
    /// * [`JobState::CommitReady`]
    /// * [`JobState::Succeeded`]
    /// * [`JobState::Failed`] if the database rejected the job's outputs or children.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskIndexOutOfBound`] if the task index is out of range.
    /// * [`InternalError::TaskGraphCorrupted`] if no incomplete tasks remain while new ready tasks
    ///   are generated.
    /// * Forwards [`JobExecutionStateHandle::read_running`]'s return values on failure.
    /// * Forwards [`SharedTaskControlBlock::succeed_task_instance`]'s return values on failure.
    /// * Forwards [`JobControlBlock::hit_hook`]'s return values on failure.
    /// * Forwards [`InboundQueueSender::send_task_ready`]'s return values on failure.
    /// * Forwards [`TaskGraph::read_output_payloads`]'s return values on failure.
    /// * Forwards [`Self::submit_event_and_wait_for_reply`]'s return values on failure.
    pub async fn succeed_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_index: TaskIndex,
        task_outputs: Vec<TaskOutput>,
        prepared_children: Vec<PreparedChild>,
    ) -> Result<JobState, CacheError> {
        let jcb = &self.inner;
        let job = jcb.job_execution_state.read_running().await?;
        let tcb = job
            .task_graph
            .get_task_control_block(task_index)
            .ok_or(InternalError::TaskIndexOutOfBound)?;
        let (ready_task_indices, partial_mutation_guard) = tcb
            .succeed_task_instance(
                task_instance_id,
                task_outputs,
                prepared_children,
                || jcb.partial_mutation_guard(TaskId::Index(task_index), task_instance_id),
                &jcb.hooks,
                || jcb.hook_context(Some(task_instance_id)),
            )
            .await?;
        jcb.hit_hook(HookPoint::TaskSuccessAfterTcbFlip, Some(task_instance_id))
            .await?;
        // NOTE: `fetch_sub` returns the previous value, so the new count is the returned value
        // minus 1.
        // `AcqRel` orders every sibling's output and prepared-children writes before the last
        // reporter reads them below.
        let num_incomplete_tasks = job.num_incomplete_tasks.fetch_sub(1, Ordering::AcqRel) - 1;

        if !ready_task_indices.is_empty() {
            if num_incomplete_tasks == 0 {
                return Err(InternalError::TaskGraphCorrupted(
                    "no incomplete tasks while new ready task indices are generated".to_owned(),
                )
                .into());
            }
            job.inbound_queue_sender
                .send_task_ready(jcb.owner_id, jcb.id, ready_task_indices)
                .await?;
            partial_mutation_guard.disarm();
            return Ok(job.state);
        }

        if num_incomplete_tasks != 0 {
            partial_mutation_guard.disarm();
            return Ok(job.state);
        }

        let outputs = job.task_graph.read_output_payloads().await?;
        let children = job.task_graph.take_prepared_children().await;
        drop(job);
        jcb.hit_hook(
            HookPoint::TaskSuccessBeforeOutputsCommit,
            Some(task_instance_id),
        )
        .await?;
        self.submit_event_and_wait_for_reply(
            TreeEventKind::RegularTasksDone { outputs, children },
            Some(partial_mutation_guard),
        )
        .await
    }

    /// Marks the commit task instance as succeeded and transitions the job to
    /// [`JobState::Succeeded`].
    ///
    /// # Returns
    ///
    /// [`JobState::Succeeded`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UndefinedCommitTask`] if the job has no commit task.
    /// * Forwards [`JobExecutionStateHandle::read_commit_ready`]'s return values on failure.
    /// * Forwards [`SharedTerminationTaskControlBlock::succeed_task_instance`]'s return values on
    ///   failure.
    /// * Forwards [`Self::submit_event_and_wait_for_reply`]'s return values on failure.
    pub async fn succeed_commit_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, CacheError> {
        let jcb = &self.inner;
        let job = jcb.job_execution_state.read_commit_ready().await?;
        job.task_graph
            .get_commit_task_control_block()
            .ok_or(InternalError::UndefinedCommitTask)?
            .succeed_task_instance(task_instance_id)
            .await?;
        drop(job);
        self.submit_event_and_wait_for_reply(
            TreeEventKind::CommitDone,
            Some(jcb.partial_mutation_guard(TaskId::Commit, task_instance_id)),
        )
        .await
    }

    /// Marks the cleanup task instance as succeeded and transitions the job to
    /// [`JobState::Cancelled`].
    ///
    /// # Returns
    ///
    /// [`JobState::Cancelled`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UndefinedCleanupTask`] if the job has no cleanup task.
    /// * Forwards [`JobExecutionStateHandle::read_cleanup_ready`]'s return values on failure.
    /// * Forwards [`SharedTerminationTaskControlBlock::succeed_task_instance`]'s return values on
    ///   failure.
    /// * Forwards [`Self::submit_event_and_wait_for_reply`]'s return values on failure.
    pub async fn succeed_cleanup_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<JobState, CacheError> {
        let jcb = &self.inner;
        let job = jcb.job_execution_state.read_cleanup_ready().await?;
        job.task_graph
            .get_cleanup_task_control_block()
            .ok_or(InternalError::UndefinedCleanupTask)?
            .succeed_task_instance(task_instance_id)
            .await?;
        drop(job);
        self.submit_event_and_wait_for_reply(
            TreeEventKind::CleanupDone,
            Some(jcb.partial_mutation_guard(TaskId::Cleanup, task_instance_id)),
        )
        .await
    }

    /// Marks a task instance as failed.
    ///
    /// If the task has remaining retries, it is re-enqueued to the inbound queue. Otherwise,
    /// submits the job's failure to the job's tree and waits for the committer; the job moves to
    /// [`JobState::Failed`] unless it has left the task's lane.
    ///
    /// # Returns
    ///
    /// The current [`JobState`] after the operation on success. Must be one of:
    ///
    /// * [`JobState::Running`]
    /// * [`JobState::Failed`]
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskIndexOutOfBound`] if the task index is out of range.
    /// * [`InternalError::UndefinedCommitTask`] if the job has no commit task.
    /// * [`InternalError::UndefinedCleanupTask`] if the job has no cleanup task.
    /// * Forwards [`JobExecutionStateHandle::read_running`]'s return values on failure.
    /// * Forwards [`JobExecutionStateHandle::read_commit_ready`]'s return values on failure.
    /// * Forwards [`JobExecutionStateHandle::read_cleanup_ready`]'s return values on failure.
    /// * Forwards [`SharedTaskControlBlock::fail_task_instance`]'s return values on failure.
    /// * Forwards [`SharedTerminationTaskControlBlock::fail_task_instance`]'s return values on
    ///   failure.
    /// * Forwards [`InboundQueueSender::send_task_ready`]'s return values on failure.
    /// * Forwards [`InboundQueueSender::send_commit_ready`]'s return values on failure.
    /// * Forwards [`InboundQueueSender::send_cleanup_ready`]'s return values on failure.
    /// * Forwards [`JobControlBlock::hit_hook`]'s return values on failure.
    /// * Forwards [`Self::submit_event_and_wait_for_reply`]'s return values on failure.
    pub async fn fail_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_id: TaskId,
        error_message: String,
    ) -> Result<JobState, CacheError> {
        let jcb = &self.inner;
        let (lane, partial_mutation_guard) = match task_id {
            TaskId::Index(task_index) => {
                let job = jcb.job_execution_state.read_running().await?;
                let task_state = job
                    .task_graph
                    .get_task_control_block(task_index)
                    .ok_or(InternalError::TaskIndexOutOfBound)?
                    .fail_task_instance(task_instance_id, error_message.clone())
                    .await?;

                let partial_mutation_guard = jcb.partial_mutation_guard(task_id, task_instance_id);
                if matches!(task_state, TaskState::Ready | TaskState::Running) {
                    job.inbound_queue_sender
                        .send_task_ready(jcb.owner_id, jcb.id, vec![task_index])
                        .await?;
                    partial_mutation_guard.disarm();
                    return Ok(job.state);
                }
                drop(job);
                (Lane::Regular, partial_mutation_guard)
            }
            TaskId::Commit => {
                let job = jcb.job_execution_state.read_commit_ready().await?;
                let task_state = job
                    .task_graph
                    .get_commit_task_control_block()
                    .ok_or(InternalError::UndefinedCommitTask)?
                    .fail_task_instance(task_instance_id, error_message.clone())
                    .await?;

                let partial_mutation_guard = jcb.partial_mutation_guard(task_id, task_instance_id);
                if matches!(task_state, TaskState::Ready | TaskState::Running) {
                    job.inbound_queue_sender
                        .send_commit_ready(jcb.owner_id, jcb.id)
                        .await?;
                    partial_mutation_guard.disarm();
                    return Ok(job.state);
                }
                drop(job);
                (Lane::Commit, partial_mutation_guard)
            }
            TaskId::Cleanup => {
                let job = jcb.job_execution_state.read_cleanup_ready().await?;
                let task_state = job
                    .task_graph
                    .get_cleanup_task_control_block()
                    .ok_or(InternalError::UndefinedCleanupTask)?
                    .fail_task_instance(task_instance_id, error_message.clone())
                    .await?;

                let partial_mutation_guard = jcb.partial_mutation_guard(task_id, task_instance_id);
                if matches!(task_state, TaskState::Ready | TaskState::Running) {
                    job.inbound_queue_sender
                        .send_cleanup_ready(jcb.owner_id, jcb.id)
                        .await?;
                    partial_mutation_guard.disarm();
                    return Ok(job.state);
                }
                drop(job);
                (Lane::Cleanup, partial_mutation_guard)
            }
        };
        jcb.hit_hook(HookPoint::TaskFailureBeforeSubmit, Some(task_instance_id))
            .await?;
        self.submit_event_and_wait_for_reply(
            TreeEventKind::TerminalFailure {
                lane,
                message: error_message,
            },
            Some(partial_mutation_guard),
        )
        .await
    }

    /// Cancels the job: submits its cancel to the job's tree and waits until the committer has
    /// applied it, which publishes the cleanup task, if any.
    ///
    /// # Returns
    ///
    /// The current [`JobState`] after the cancellation operation on success. Must be one of:
    ///
    /// * [`JobState::CleanupReady`]
    /// * [`JobState::Cancelled`]
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::submit_event_and_wait_for_reply`]'s return values on failure.
    pub async fn cancel(&self) -> Result<JobState, CacheError> {
        self.submit_event_and_wait_for_reply(TreeEventKind::CancelRoot, None)
            .await
    }

    /// Applies a transition that the tree's committer has written to the database to the cached
    /// job.
    ///
    /// A job that the transition deactivates gets its non-terminal tasks cancelled; a job that
    /// leaves [`JobState::Running`] or is deactivated drops its tasks' prepared children and
    /// releases its regular tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::CacheDbDivergence`] if the cached state is not `from`.
    pub(crate) async fn apply_tree_state(
        &self,
        from: JobState,
        to: JobState,
    ) -> Result<(), CacheError> {
        let jcb = &self.inner;
        let mut job = jcb.job_execution_state.write().await;
        if job.state != from {
            return Err(InternalError::CacheDbDivergence {
                job_id: jcb.id,
                expected: from,
                actual: job.state,
            }
            .into());
        }
        job.state = to;
        let is_deactivated = matches!(
            to,
            JobState::Failed | JobState::Cancelled | JobState::CleanupReady
        ) && matches!(
            from,
            JobState::Ready | JobState::Running | JobState::CommitReady
        );
        if is_deactivated {
            job.task_graph.cancel_non_terminal().await;
        }
        if from == JobState::Running || is_deactivated {
            job.task_graph.clear_prepared_children().await;
            job.task_graph.finalize();
        }
        drop(job);
        Ok(())
    }

    /// Submits an event of `kind` of this job to the job's tree, disarms `partial_mutation_guard`,
    /// if any, once the event is submitted, and waits for the committer's reply.
    ///
    /// # Returns
    ///
    /// The job's state after the committer applied the event on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`CacheError::StaleState`] if:
    ///   * The job's state does not accept the event.
    ///   * The tree no longer tracks the job, which is then terminal and gets the error that its
    ///     cached state calls for (see
    ///     [`StateRequirement::ensure_met`](crate::cache::tree::StateRequirement::ensure_met)).
    /// * [`InternalError::UntrackedLiveJob`] if the tree no longer tracks the job while its cached
    ///   state accepts the event.
    /// * Forwards [`TreeControlBlock::submit`]'s return values on failure.
    async fn submit_event_and_wait_for_reply(
        &self,
        kind: TreeEventKind,
        partial_mutation_guard: Option<PartialMutationGuard<'_>>,
    ) -> Result<JobState, CacheError> {
        let jcb = &self.inner;
        let state_requirement = kind.state_requirement();
        let reply = jcb.tree.submit(TreeEvent {
            job_id: jcb.id,
            kind,
        });
        if let Some(partial_mutation_guard) = partial_mutation_guard {
            partial_mutation_guard.disarm();
        }
        // The submitted event belongs to the committer, so a pause that the service's shutdown
        // ends lets the wait for its reply go on, as it would without the hook.
        let _ = jcb
            .hooks
            .hit(HookPoint::JobEventAfterSubmit, || jcb.hook_context(None))
            .await;
        match reply.await? {
            TreeEventOutcome::Applied(state) => Ok(state),
            TreeEventOutcome::Stale(error) => Err(error.into()),
            TreeEventOutcome::Untracked => {
                let state = self.state().await;
                match state_requirement.ensure_met(state) {
                    Err(error) => Err(error.into()),
                    Ok(()) => Err(InternalError::UntrackedLiveJob {
                        job_id: jcb.id,
                        state,
                    }
                    .into()),
                }
            }
        }
    }

    /// Creates a task instance for a regular task in the job and registers it in the task instance
    /// pool.
    ///
    /// # Returns
    ///
    /// The execution context for the created task instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskIndexOutOfBound`] if the task index is out of range.
    /// * Forwards [`JobExecutionStateHandle::read_running`]'s return values on failure.
    /// * Forwards [`SharedTaskControlBlock::register_task_instance`]'s return values on failure.
    /// * Forwards [`JobControlBlock::hit_hook`]'s return values on failure.
    /// * Forwards [`TaskInstancePoolConnector::register_task_instance`]'s return values on failure.
    async fn create_regular_task_instance(
        jcb: &JobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
        task_index: TaskIndex,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<ExecutionContext, CacheError> {
        let job = jcb.job_execution_state.read_running().await?;
        let tcb = job
            .task_graph
            .get_task_control_block(task_index)
            .ok_or(InternalError::TaskIndexOutOfBound)?;
        let task_instance_id = job
            .task_instance_pool_connector
            .get_next_available_task_instance_id();
        let execution_context = tcb.register_task_instance(task_instance_id).await?;

        let partial_mutation_guard =
            jcb.partial_mutation_guard(TaskId::Index(task_index), task_instance_id);
        jcb.hit_hook(
            HookPoint::CreateInstanceBeforePoolSend,
            Some(task_instance_id),
        )
        .await?;
        let registration = TaskInstanceMetadata {
            resource_group_id: jcb.owner_id,
            job_id: jcb.id,
            task_id: TaskId::Index(task_index),
            task_instance_id,
            execution_manager_id,
            soft_timeout_ddl: SystemTime::now().checked_add(Duration::from_millis(
                execution_context.timeout_policy.soft_timeout_ms,
            )),
        };
        job.task_instance_pool_connector
            .register_task_instance(tcb.clone(), registration)
            .await?;
        partial_mutation_guard.disarm();

        // The lock is intentionally held until just before return so all TCB accesses
        // observe a consistent state within the lock's scope.
        drop(job);
        Ok(execution_context)
    }

    /// Creates a task instance for the commit task and registers it in the task instance pool.
    ///
    /// # Returns
    ///
    /// The execution context for the created task instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UndefinedCommitTask`] if the job has no commit task.
    /// * Forwards [`JobExecutionStateHandle::read_commit_ready`]'s return values on failure.
    /// * Forwards [`TaskGraph::read_output_payloads`]'s return values on failure.
    /// * Forwards [`SerializedTaskOutputs::serialize_with_size_hint`]'s return values on failure.
    /// * Forwards [`SharedTerminationTaskControlBlock::register_task_instance`]'s return values on
    ///   failure.
    /// * Forwards [`TaskInstancePoolConnector::register_termination_task_instance`]'s return values
    ///   on failure.
    async fn create_commit_task_instance(
        jcb: &JobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<ExecutionContext, CacheError> {
        let job = jcb.job_execution_state.read_commit_ready().await?;
        let commit_tcb = job
            .task_graph
            .get_commit_task_control_block()
            .ok_or(InternalError::UndefinedCommitTask)?;
        let task_graph_outputs = job.task_graph.read_output_payloads().await?;
        let serialized_task_io =
            SerializedTaskOutputs::serialize_with_size_hint(&task_graph_outputs)
                .map_err(InternalError::TaskOutputs)?
                .to_raw();
        let task_instance_id = job
            .task_instance_pool_connector
            .get_next_available_task_instance_id();
        let (tdl_context, timeout_policy) =
            commit_tcb.register_task_instance(task_instance_id).await?;

        let partial_mutation_guard = jcb.partial_mutation_guard(TaskId::Commit, task_instance_id);
        let registration = TaskInstanceMetadata {
            resource_group_id: jcb.owner_id,
            job_id: jcb.id,
            task_id: TaskId::Commit,
            task_instance_id,
            execution_manager_id,
            soft_timeout_ddl: SystemTime::now()
                .checked_add(Duration::from_millis(timeout_policy.soft_timeout_ms)),
        };
        job.task_instance_pool_connector
            .register_termination_task_instance(commit_tcb.clone(), registration)
            .await?;
        partial_mutation_guard.disarm();

        drop(job);
        Ok(ExecutionContext {
            task_instance_id,
            tdl_context,
            timeout_policy,
            serialized_task_io,
        })
    }

    /// Creates a task instance for the cleanup task and registers it in the task instance pool.
    ///
    /// # Returns
    ///
    /// The execution context for the created task instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UndefinedCleanupTask`] if the job has no cleanup task.
    /// * Forwards [`JobExecutionStateHandle::read_cleanup_ready`]'s return values on failure.
    /// * Forwards [`SharedTerminationTaskControlBlock::register_task_instance`]'s return values on
    ///   failure.
    /// * Forwards [`TaskInstancePoolConnector::register_termination_task_instance`]'s return values
    ///   on failure.
    async fn create_cleanup_task_instance(
        jcb: &JobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<ExecutionContext, CacheError> {
        let job = jcb.job_execution_state.read_cleanup_ready().await?;
        let cleanup_tcb = job
            .task_graph
            .get_cleanup_task_control_block()
            .ok_or(InternalError::UndefinedCleanupTask)?;
        let task_instance_id = job
            .task_instance_pool_connector
            .get_next_available_task_instance_id();
        let (tdl_context, timeout_policy) =
            cleanup_tcb.register_task_instance(task_instance_id).await?;

        let partial_mutation_guard = jcb.partial_mutation_guard(TaskId::Cleanup, task_instance_id);
        let registration = TaskInstanceMetadata {
            resource_group_id: jcb.owner_id,
            job_id: jcb.id,
            task_id: TaskId::Cleanup,
            task_instance_id,
            execution_manager_id,
            soft_timeout_ddl: SystemTime::now()
                .checked_add(Duration::from_millis(timeout_policy.soft_timeout_ms)),
        };
        job.task_instance_pool_connector
            .register_termination_task_instance(cleanup_tcb.clone(), registration)
            .await?;
        partial_mutation_guard.disarm();

        drop(job);
        Ok(ExecutionContext {
            task_instance_id,
            tdl_context,
            timeout_policy,
            serialized_task_io: Vec::new(),
        })
    }
}

/// The control block for a job.
///
/// This struct holds the immutable identity of a job and its place in its tree, the children that
/// its reports reserved, its test hooks, the control block of its tree, and a handle to its
/// execution state. All other mutable state and the connectors live inside [`JobExecutionState`]
/// and are protected by the read-write lock in [`JobExecutionStateHandle`].
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
struct JobControlBlock<
    InboundQueueSenderType: InboundQueueSender,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    id: JobId,
    owner_id: ResourceGroupId,
    parent_id: Option<JobId>,
    depth: u32,
    reserved_children_usage: std::sync::Mutex<ChildJobUsage>,
    hooks: TestHooks,
    tree: TreeControlBlock,
    job_execution_state:
        JobExecutionStateHandle<InboundQueueSenderType, TaskInstancePoolConnectorType>,
}

impl<
    InboundQueueSenderType: InboundQueueSender,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> JobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>
{
    /// Fires the job's hooks at `point` for this job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TestHooks::hit`]'s return values on failure.
    async fn hit_hook(
        &self,
        point: HookPoint,
        task_instance_id: Option<TaskInstanceId>,
    ) -> Result<(), CacheError> {
        Ok(self
            .hooks
            .hit(point, || self.hook_context(task_instance_id))
            .await?)
    }

    /// # Returns
    ///
    /// The hook context of this job, for a hit about `task_instance_id`, if any.
    fn hook_context(&self, task_instance_id: Option<TaskInstanceId>) -> HookContext {
        HookContext {
            task_instance_id,
            ..HookContext::job(self.id)
        }
    }

    /// # Returns
    ///
    /// A guard over a mutation of this job for the task instance `task_instance_id` of `task_id`.
    const fn partial_mutation_guard(
        &self,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
    ) -> PartialMutationGuard<'_> {
        PartialMutationGuard::for_task_instance(
            self.tree.shutdown_handle(),
            self.id,
            task_id,
            task_instance_id,
        )
    }
}

/// A concurrency-safe handle to a job's execution state.
///
/// This type wraps [`JobExecutionState`] in a read-write lock and provides controlled access to it.
/// Most accessors validate the job state before returning a guard; [`Self::read_state`],
/// [`Self::read`] and [`Self::write`] do not, for callers that check the state themselves.
///
/// # Type Parameters
///
/// The type parameters are forwarded directly to [`JobExecutionState`] in the same declaration
/// order. Single-character names are used to:
///
/// * Reduce verbosity while preserving consistency with the underlying type.
/// * Avoid formatting issues, as `rustfmt` does not handle line wrapping well when using more
///   descriptive type parameter names in this particular struct.
struct JobExecutionStateHandle<R: InboundQueueSender, T: TaskInstancePoolConnector> {
    inner: tokio::sync::RwLock<JobExecutionState<R, T>>,
}

impl<R: InboundQueueSender, T: TaskInstancePoolConnector> JobExecutionStateHandle<R, T> {
    /// # Returns
    ///
    /// The current job state.
    async fn read_state(&self) -> JobState {
        self.inner.read().await.state
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state, whatever the job state.
    async fn read(&self) -> RwLockReadGuard<'_, JobExecutionState<R, T>> {
        self.inner.read().await
    }

    /// # Returns
    ///
    /// A writer guard of the underlying job execution state, whatever the job state.
    async fn write(&self) -> RwLockWriteGuard<'_, JobExecutionState<R, T>> {
        self.inner.write().await
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_running`]'s return values on failure.
    async fn read_running(
        &self,
    ) -> Result<RwLockReadGuard<'_, JobExecutionState<R, T>>, CacheError> {
        self.validate_and_read(JobExecutionState::ensure_running)
            .await
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_commit_ready`]'s return values on failure.
    async fn read_commit_ready(
        &self,
    ) -> Result<RwLockReadGuard<'_, JobExecutionState<R, T>>, CacheError> {
        self.validate_and_read(JobExecutionState::ensure_commit_ready)
            .await
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_cleanup_ready`]'s return values on failure.
    async fn read_cleanup_ready(
        &self,
    ) -> Result<RwLockReadGuard<'_, JobExecutionState<R, T>>, CacheError> {
        self.validate_and_read(JobExecutionState::ensure_cleanup_ready)
            .await
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_succeeded`]'s return values on failure.
    async fn read_succeeded(
        &self,
    ) -> Result<RwLockReadGuard<'_, JobExecutionState<R, T>>, CacheError> {
        self.validate_and_read(JobExecutionState::ensure_succeeded)
            .await
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`JobExecutionState::ensure_ready`]'s return values on failure.
    async fn read_ready(&self) -> Result<RwLockReadGuard<'_, JobExecutionState<R, T>>, CacheError> {
        self.validate_and_read(JobExecutionState::ensure_ready)
            .await
    }

    /// # Returns
    ///
    /// A reader guard of the underlying job execution state on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards `validator`'s return values on failure.
    async fn validate_and_read(
        &self,
        validator: fn(&JobExecutionState<R, T>) -> Result<(), CacheError>,
    ) -> Result<RwLockReadGuard<'_, JobExecutionState<R, T>>, CacheError> {
        let guard = self.inner.read().await;
        validator(&guard)?;
        Ok(guard)
    }
}

/// Represents the execution state of a job.
///
/// This struct holds all mutable job state, including the task graph, connectors, and
/// inbound-queue sender,
/// so that concurrent access is synchronized through [`JobExecutionStateHandle`]'s read-write lock.
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
struct JobExecutionState<
    InboundQueueSenderType: InboundQueueSender,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    state: JobState,
    task_graph: TaskGraph,
    num_incomplete_tasks: AtomicUsize,
    inbound_queue_sender: InboundQueueSenderType,
    task_instance_pool_connector: TaskInstancePoolConnectorType,
}

impl<
    InboundQueueSenderType: InboundQueueSender,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> JobExecutionState<InboundQueueSenderType, TaskInstancePoolConnectorType>
{
    /// Ensures that the job is currently in the [`JobState::Running`] state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::JobNotStarted`] if the job hasn't been started yet.
    /// * [`StaleStateError::JobNoLongerRunning`] if the job is no longer running.
    fn ensure_running(&self) -> Result<(), CacheError> {
        if !self.state.is_running() {
            if matches!(self.state, JobState::Ready) {
                return Err(InternalError::JobNotStarted.into());
            }
            return Err(StaleStateError::JobNoLongerRunning.into());
        }
        Ok(())
    }

    /// Ensures that the job is currently in the [`JobState::CommitReady`] state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UnexpectedJobState`] if the job is in an unexpected state.
    /// * [`StaleStateError::JobNoLongerCommitReady`] if the job is not commit-ready and the request
    ///   is stale.
    fn ensure_commit_ready(&self) -> Result<(), CacheError> {
        if !matches!(self.state, JobState::CommitReady) {
            if self.state.is_terminal()
                || matches!(
                    self.state,
                    JobState::CleanupReady | JobState::BlockingOnChildren
                )
            {
                return Err(StaleStateError::JobNoLongerCommitReady.into());
            }
            return Err(InternalError::UnexpectedJobState {
                current: self.state,
                expected: JobState::CommitReady,
            }
            .into());
        }
        Ok(())
    }

    /// Ensures that the job is currently in the [`JobState::CleanupReady`] state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::UnexpectedJobState`] if the job is in an unexpected state.
    /// * [`StaleStateError::JobNoLongerCleanupReady`] if the job is not cleanup-ready and the
    ///   request is stale.
    fn ensure_cleanup_ready(&self) -> Result<(), CacheError> {
        if !matches!(self.state, JobState::CleanupReady) {
            if self.state.is_terminal() || matches!(self.state, JobState::BlockingOnChildren) {
                return Err(StaleStateError::JobNoLongerCleanupReady.into());
            }
            return Err(InternalError::UnexpectedJobState {
                current: self.state,
                expected: JobState::CleanupReady,
            }
            .into());
        }
        Ok(())
    }

    /// Ensures that the job is currently in the [`JobState::Succeeded`] state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::JobNotSucceeded`] if the job is not in [`JobState::Succeeded`].
    fn ensure_succeeded(&self) -> Result<(), CacheError> {
        if !matches!(self.state, JobState::Succeeded) {
            return Err(StaleStateError::JobNotSucceeded(self.state).into());
        }
        Ok(())
    }

    /// Ensures that the job is currently in [`JobState::Ready`] state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::JobAlreadyStarted`] if the job has already started.
    fn ensure_ready(&self) -> Result<(), CacheError> {
        if !matches!(self.state, JobState::Ready) {
            return Err(StaleStateError::JobAlreadyStarted.into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use spider_core::job::JobState;
    use spider_core::types::id::JobId;

    use super::*;
    use crate::cache::error::CacheError;
    use crate::cache::error::InternalError;
    use crate::limits::MAX_CHILDREN_PER_JOB;
    use crate::limits::MAX_JOB_CHILDREN_SIZE;
    use crate::state::test_utils::create_test_child_submission_with_independent_tasks;
    use crate::state::test_utils::create_test_jcb;
    use crate::state::test_utils::create_test_jcb_with_independent_tasks;
    use crate::state::test_utils::create_test_prepared_child;

    const JOB_ID: JobId = JobId::from(1);

    #[tokio::test]
    async fn apply_tree_state_rejects_a_from_state_that_is_not_cached() {
        let (jcb, _receivers) = create_test_jcb(JOB_ID).await;

        let result = jcb
            .apply_tree_state(JobState::Running, JobState::Succeeded)
            .await;
        assert!(
            matches!(
                result,
                Err(CacheError::Internal(InternalError::CacheDbDivergence {
                    expected: JobState::Running,
                    actual: JobState::Ready,
                    ..
                }))
            ),
            "an apply from a state that is not cached gave {result:?}"
        );
        assert_eq!(jcb.state().await, JobState::Ready);
    }

    #[tokio::test]
    async fn apply_tree_state_cancels_tasks_on_deactivation_and_releases_tasks_on_leaving_running()
    -> anyhow::Result<()> {
        for (from, to, is_cancelled, is_released) in [
            (JobState::Ready, JobState::Running, false, false),
            (JobState::Running, JobState::CommitReady, false, true),
            (JobState::Running, JobState::Failed, true, true),
            (JobState::Ready, JobState::Cancelled, true, true),
        ] {
            let (jcb, _receivers) = create_test_jcb(JOB_ID).await;
            if from == JobState::Running {
                jcb.apply_tree_state(JobState::Ready, JobState::Running)
                    .await?;
            }
            let tcb = jcb
                .inner
                .job_execution_state
                .read()
                .await
                .task_graph
                .get_task_control_block(0)
                .expect("the job should have a task 0");

            jcb.apply_tree_state(from, to).await?;
            assert_eq!(tcb.is_terminal().await, is_cancelled, "{from} to {to}");
            let is_task_released = jcb
                .inner
                .job_execution_state
                .read()
                .await
                .task_graph
                .get_task_control_block(0)
                .is_none();
            assert_eq!(is_task_released, is_released, "{from} to {to}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn children_usage_is_reserved_up_to_the_job_limits_and_freed_by_a_revert()
    -> anyhow::Result<()> {
        const AT_LIMITS: ChildJobUsage = ChildJobUsage {
            count: MAX_CHILDREN_PER_JOB,
            size: MAX_JOB_CHILDREN_SIZE,
        };
        const ONE: ChildJobUsage = ChildJobUsage { count: 1, size: 1 };

        let (jcb, _receivers) = create_test_jcb(JOB_ID).await;
        let reserved_children_usage = || {
            *jcb.inner
                .reserved_children_usage
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
        };

        assert_eq!(
            jcb.reserve_children_usage(ChildJobUsage {
                count: MAX_CHILDREN_PER_JOB - 1,
                size: MAX_JOB_CHILDREN_SIZE - 1,
            }),
            Ok(())
        );
        assert_eq!(jcb.reserve_children_usage(ONE), Ok(()));
        assert_eq!(reserved_children_usage(), AT_LIMITS);

        for over_limit in [
            ChildJobUsage { count: 1, size: 0 },
            ChildJobUsage { count: 0, size: 1 },
        ] {
            assert_eq!(
                jcb.reserve_children_usage(over_limit),
                Err(AT_LIMITS + over_limit)
            );
            assert_eq!(reserved_children_usage(), AT_LIMITS);
        }

        jcb.revert_children_usage_reservation(ONE);
        assert_eq!(jcb.reserve_children_usage(ONE), Ok(()));
        assert_eq!(reserved_children_usage(), AT_LIMITS);

        jcb.revert_children_usage_reservation(AT_LIMITS + ONE);
        assert_eq!(reserved_children_usage(), ChildJobUsage::default());
        Ok(())
    }

    #[tokio::test]
    async fn apply_tree_state_drops_the_prepared_children_of_a_job_that_leaves_running()
    -> anyhow::Result<()> {
        for to in [JobState::CommitReady, JobState::Failed, JobState::Cancelled] {
            let (jcb, _receivers) = create_test_jcb_with_independent_tasks(JOB_ID, 2).await;
            jcb.apply_tree_state(JobState::Ready, JobState::Running)
                .await?;
            let tcb = jcb
                .inner
                .job_execution_state
                .read()
                .await
                .task_graph
                .get_task_control_block(0)
                .expect("the job should have a task 0");
            let execution_context = jcb
                .create_task_instance(TaskId::Index(0), ExecutionManagerId::random())
                .await?;
            let state = jcb
                .succeed_task_instance(
                    execution_context.task_instance_id,
                    0,
                    vec![vec![0u8; 4]],
                    vec![create_test_prepared_child(0).await],
                )
                .await?;
            assert_eq!(state, JobState::Running, "Running to {to}");
            assert_eq!(tcb.num_prepared_children().await, 1, "Running to {to}");

            jcb.apply_tree_state(JobState::Running, to).await?;
            assert_eq!(tcb.num_prepared_children().await, 0, "Running to {to}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn from_prepared_child_builds_a_running_child_below_its_parent_in_its_tree()
    -> anyhow::Result<()> {
        const CHILD_ID: JobId = JobId::from(2);
        const GRANDCHILD_ID: JobId = JobId::from(3);

        let (parent, mut receivers) = create_test_jcb(JOB_ID).await;
        let child = SharedJobControlBlock::from_prepared_child(
            CHILD_ID,
            &parent,
            create_test_prepared_child(0).await,
        )
        .await;
        let grandchild = SharedJobControlBlock::from_prepared_child(
            GRANDCHILD_ID,
            &child,
            PreparedChild::create(create_test_child_submission_with_independent_tasks(2)).await?,
        )
        .await;

        assert_eq!(
            (child.id(), child.parent_id(), child.depth()),
            (CHILD_ID, Some(JOB_ID), 1)
        );
        assert_eq!(
            (grandchild.id(), grandchild.parent_id(), grandchild.depth()),
            (GRANDCHILD_ID, Some(CHILD_ID), 2)
        );
        for (job, num_tasks, ready_task_indices) in
            [(&child, 1, vec![0]), (&grandchild, 2, vec![0, 1])]
        {
            assert_eq!(job.owner_id(), parent.owner_id());
            assert_eq!(job.state().await, JobState::Running);
            let _reply = job.inner.tree.submit(TreeEvent {
                job_id: job.id(),
                kind: TreeEventKind::CommitDone,
            });
            let pending = receivers
                .regular
                .try_recv()
                .expect("a child's event should go to its parent's tree");
            assert_eq!(pending.event.job_id, job.id());
            let execution_state = job.inner.job_execution_state.read().await;
            assert_eq!(
                execution_state.num_incomplete_tasks.load(Ordering::Acquire),
                num_tasks
            );
            assert_eq!(
                execution_state
                    .task_graph
                    .get_all_ready_task_indices()
                    .await,
                ready_task_indices
            );
            drop(execution_state);
        }
        Ok(())
    }

    #[tokio::test]
    async fn get_ready_task_indices_returns_a_running_childs_ready_tasks() -> anyhow::Result<()> {
        const CHILD_ID: JobId = JobId::from(2);

        let (parent, _receivers) = create_test_jcb(JOB_ID).await;
        let child = SharedJobControlBlock::from_prepared_child(
            CHILD_ID,
            &parent,
            PreparedChild::create(create_test_child_submission_with_independent_tasks(2)).await?,
        )
        .await;

        assert_eq!(child.get_ready_task_indices().await?, vec![0, 1]);
        Ok(())
    }
}
