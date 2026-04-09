use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use spider_core::{
    task::{
        Task,
        TaskGraph as SubmittedTaskGraph,
        TaskIndex,
        TaskState,
        TdlContext,
        TerminationTaskDescriptor,
        TimeoutPolicy,
    },
    types::{
        id::TaskInstanceId,
        io::{ExecutionContext, TaskInput, TaskOutput},
    },
};
use tokio::sync::RwLock;

use crate::cache::{
    error::{CacheError, InternalError, StaleStateError},
    io::{InputReader, OutputReader, OutputWriter, ValuePayload},
    sync::{Reader, SharedRw, Writer},
};

/// Represents the task graph in the cache as a collection of TCBs.
pub struct TaskGraph {
    tasks: Vec<SharedTaskControlBlock>,
    outputs: Vec<OutputReader>,
    commit_task: Option<SharedTerminationTaskControlBlock>,
    cleanup_task: Option<SharedTerminationTaskControlBlock>,
}

impl TaskGraph {
    /// Factory function.
    ///
    /// Creates a new task graph from a submitted task graph and the input task inputs.
    ///
    /// # Returns
    ///
    /// The created task graph instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskGraphCorrupted`] if:
    ///   * Any dataflow deps' index is out-of-range.
    ///   * Any task index is out-of-range.
    /// * [`InternalError::TaskGraphInputsSizeMismatch`] if the number of provided inputs does not
    ///   match the task graph’s expected number of inputs.
    /// * Forwards [`SharedTaskControlBlock::create`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if the internal TCB buffer is corrupted.
    pub async fn create(
        submitted_task_graph: &SubmittedTaskGraph,
        inputs: Vec<TaskInput>,
    ) -> Result<Self, InternalError> {
        let dataflow_dep_buffer: Vec<SharedRw<ValuePayload>> = (0..submitted_task_graph
            .get_num_dataflow_deps())
            .map(|_| SharedRw::new(RwLock::new(ValuePayload::default())))
            .collect();
        let task_graph_input_indices = submitted_task_graph.get_task_graph_input_indices();
        if inputs.len() != task_graph_input_indices.len() {
            return Err(InternalError::TaskGraphInputsSizeMismatch(
                task_graph_input_indices.len(),
                inputs.len(),
            ));
        }
        for (deps_index, input) in task_graph_input_indices.into_iter().zip(inputs) {
            let dataflow_dep = dataflow_dep_buffer.get(deps_index).ok_or_else(|| {
                InternalError::TaskGraphCorrupted(
                    "dataflow dependency index out-of-range".to_owned(),
                )
            })?;
            *dataflow_dep.write().await = match input {
                TaskInput::ValuePayload(value) => Some(value),
            }
        }

        let outputs: Vec<_> = submitted_task_graph
            .get_task_graph_output_indices()
            .into_iter()
            .map(|output_index| {
                let dataflow_dep = dataflow_dep_buffer.get(output_index).ok_or_else(|| {
                    InternalError::TaskGraphCorrupted(
                        "dataflow dependency index out-of-range".to_owned(),
                    )
                })?;

                Ok(OutputReader::new(dataflow_dep.clone()))
            })
            .collect::<Result<_, InternalError>>()?;

        let num_tasks = submitted_task_graph.get_num_tasks();
        let mut tcb_buffer = HashMap::new();
        for task_index in (0..num_tasks).rev() {
            let task = submitted_task_graph.get_task(task_index).ok_or_else(|| {
                InternalError::TaskGraphCorrupted("task index out-of-range".to_owned())
            })?;
            let tcb =
                SharedTaskControlBlock::create(task, &tcb_buffer, &dataflow_dep_buffer).await?;
            tcb_buffer.insert(task.get_index(), tcb);
        }

        let mut tasks = Vec::new();
        for task_index in 0..num_tasks {
            tasks.push(
                tcb_buffer
                    .get(&task_index)
                    .expect("task index should always be valid")
                    .clone(),
            );
        }

        let commit_task = submitted_task_graph
            .get_commit_task_descriptor()
            .map(SharedTerminationTaskControlBlock::create);

        let cleanup_task = submitted_task_graph
            .get_cleanup_task_descriptor()
            .map(SharedTerminationTaskControlBlock::create);

        Ok(Self {
            tasks,
            outputs,
            commit_task,
            cleanup_task,
        })
    }

    /// Marks all TCBs and commit TCB as cancelled, if not terminated.
    pub async fn cancel_non_terminal(&mut self) {
        for tcb in &self.tasks {
            tcb.cancel_non_terminal().await;
        }
        if let Some(commit_tcb) = &self.commit_task {
            commit_tcb.cancel_non_terminal().await;
        }
    }

    /// # Returns
    ///
    /// A vector of task indices of all TCBs in [`TaskState::Ready`] state.
    pub async fn get_all_ready_task_indices(&self) -> Vec<TaskIndex> {
        let mut ready_task_indices = Vec::new();
        for shared_tcb in &self.tasks {
            let tcb = shared_tcb.inner.lock().await;
            if matches!(tcb.base.state, TaskState::Ready) {
                ready_task_indices.push(tcb.index);
            }
        }
        ready_task_indices
    }

    /// # Returns
    ///
    /// The TCB of the given task index if it exists, `None` otherwise.
    #[must_use]
    pub fn get_task_control_block(&self, task_index: TaskIndex) -> Option<SharedTaskControlBlock> {
        self.tasks.get(task_index).cloned()
    }

    /// # Returns
    ///
    /// The TCB of the commit task if it exists, `None` otherwise.
    #[must_use]
    pub fn get_commit_task_control_block(&self) -> Option<SharedTerminationTaskControlBlock> {
        self.commit_task.clone()
    }

    /// # Returns
    ///
    /// The TCB of the cleanup task if it exists, `None` otherwise.
    #[must_use]
    pub fn get_cleanup_task_control_block(&self) -> Option<SharedTerminationTaskControlBlock> {
        self.cleanup_task.clone()
    }

    #[must_use]
    pub const fn get_outputs(&self) -> &Vec<OutputReader> {
        &self.outputs
    }

    #[must_use]
    pub const fn has_commit_task(&self) -> bool {
        self.commit_task.is_some()
    }

    #[must_use]
    pub const fn has_cleanup_task(&self) -> bool {
        self.cleanup_task.is_some()
    }
}

/// A shareable control block for a task in the task graph, defining thread-safe operations to
/// manipulate task execution state.
#[derive(Clone)]
pub struct SharedTaskControlBlock {
    inner: Arc<tokio::sync::Mutex<TaskControlBlock>>,
}

impl SharedTaskControlBlock {
    /// Registers a new task instance to the control block.
    ///
    /// # Returns
    ///
    /// The execution context for the newly registered task instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TaskControlBlockBase::register_task_instance`]'s return values on failure.
    /// * Forwards [`TaskControlBlock::fetch_inputs`]'s return values on failure.
    pub async fn register_task_instance(
        &self,
        instance_id: TaskInstanceId,
    ) -> Result<ExecutionContext, CacheError> {
        let mut tcb = self.inner.lock().await;
        tcb.base.register_task_instance(instance_id)?;

        // NOTE: The following execution can only fail due to internal errors because the cache
        // state has already been updated.
        let result: Result<_, InternalError> = {
            Ok(ExecutionContext {
                task_instance_id: instance_id,
                tdl_context: tcb.base.tdl_context.clone(),
                timeout_policy: tcb.base.timeout_policy.clone(),
                inputs: tcb.fetch_inputs().await?,
            })
        };
        result.map_err(CacheError::from)
    }

    /// Marks a task instance as succeeded.
    ///
    /// # Returns
    ///
    /// A vector of indices of the child tasks that become ready after the task instance succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskGraphCorrupted`] if:
    ///   * Attempting to mark a parent of a child task as completed, but the child task's
    ///     unfinished parent counter is already 0.
    ///   * Attempting to update a child task after parent completion, but the child task is already
    ///     in a terminal state.
    /// * Forwards [`TaskControlBlockBase::succeed_task_instance`]'s return values on failure.
    /// * Forwards [`TaskControlBlock::write_outputs`]'s return values on failure.
    pub async fn succeed_task_instance(
        &self,
        instance_id: TaskInstanceId,
        task_outputs: Vec<TaskOutput>,
    ) -> Result<Vec<TaskIndex>, CacheError> {
        let mut tcb = self.inner.lock().await;
        tcb.base.succeed_task_instance(instance_id)?;

        // NOTE: The following execution can only fail due to internal errors because the cache
        // state has already been updated.
        let result: Result<_, InternalError> = {
            tcb.write_outputs(task_outputs).await?;
            let mut ready_child_indices = Vec::new();
            for child in &tcb.children {
                let mut child_tcb = child.inner.lock().await;
                if child_tcb.num_unfinished_parents == 0 {
                    return Err(InternalError::TaskGraphCorrupted(
                        "child task has no unfinished parents remaining when processing parent \
                         completion"
                            .to_owned(),
                    )
                    .into());
                }
                child_tcb.num_unfinished_parents -= 1;
                if child_tcb.num_unfinished_parents != 0 {
                    continue;
                }

                // In practice, this update is guarded by a read lock on the task graph, which
                // guarantees that the child tasks shouldn't be terminated, as the parent is
                // not.
                if child_tcb.base.state.is_terminal() {
                    return Err(InternalError::TaskGraphCorrupted(
                        "child task is already in a terminal state when processing parent \
                         completion"
                            .to_owned(),
                    )
                    .into());
                }
                child_tcb.base.state = TaskState::Ready;
                ready_child_indices.push(child_tcb.index);
            }
            drop(tcb);

            Ok(ready_child_indices)
        };
        result.map_err(CacheError::from)
    }

    /// Marks a task instance as failed.
    ///
    /// # Returns
    ///
    /// The new state of the task after the failure is processed, forwarded from
    /// [`TaskControlBlockBase::fail_task_instance`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TaskControlBlockBase::fail_task_instance`]'s return values on failure.
    pub async fn fail_task_instance(
        &self,
        instance_id: TaskInstanceId,
        error_message: String,
    ) -> Result<TaskState, CacheError> {
        let mut tcb = self.inner.lock().await;
        tcb.base.fail_task_instance(instance_id, error_message)
    }

    /// Forcefully removes a task instance from the instance pool.
    ///
    /// # Returns
    ///
    /// Forwards [`TaskControlBlockBase::force_remove_task_instance`]'s return values.
    pub async fn force_remove_task_instance(&self, instance_id: TaskInstanceId) -> bool {
        let mut tcb = self.inner.lock().await;
        tcb.base.force_remove_task_instance(instance_id)
    }

    /// Resets the task control block to the initial state.
    pub async fn reset(&self) {
        let mut tcb = self.inner.lock().await;
        tcb.base.instance_pool.reset();
        tcb.base.retry_counter.reset();
        tcb.num_unfinished_parents = tcb.num_parents;
        tcb.base.state = if tcb.num_parents == 0 {
            TaskState::Ready
        } else {
            TaskState::Pending
        };
        for output_writer in &tcb.outputs {
            *output_writer.write().await = None;
        }
    }

    /// Marks the TCB state cancelled.
    pub async fn cancel_non_terminal(&self) {
        let mut tcb = self.inner.lock().await;
        tcb.base.cancel_non_terminal();
    }

    /// Private factory function for creating a new task control block from a task definition.
    ///
    /// The upper-level caller needs to maintain the TCB buffer and the dataflow deps buffer to
    /// provide task-graph-level information for the TCB construction. This method requires:
    ///
    /// * All child TCBs of the task must be already created and stored in the TCB buffer, indexed
    ///   by their task indices.
    /// * All input and output dependency indices of the task must be valid indices in the dataflow
    ///   deps buffer.
    ///
    /// # Returns
    ///
    /// A newly created instance of [`SharedTaskControlBlock`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskGraphCorrupted`] if any of the following happens:
    ///   * The child TCB is not found in the TCB buffer.
    ///   * An input or output dependency index is out of range in the dataflow deps buffer.
    ///   * Any input task has an unset input, or any non-input task has an input set.
    async fn create(
        task: &Task,
        tcb_buffer: &HashMap<TaskIndex, Self>,
        dataflow_dep_buffer: &[SharedRw<ValuePayload>],
    ) -> Result<Self, InternalError> {
        let index = task.get_index();
        let num_parents = task.get_parent_indices().len();
        let state = if num_parents == 0 {
            TaskState::Ready
        } else {
            TaskState::Pending
        };
        let tdl_context = task.get_tdl_context().clone();
        let instance_pool =
            InstancePool::create(task.get_execution_policy().max_num_instances as usize);
        let retry_counter = RetryCounter::new(task.get_execution_policy().max_num_retry as usize);
        let timeout_policy = task.get_execution_policy().timeout_policy.clone();

        let mut children = Vec::new();
        for child_index in task.get_child_indices() {
            let child_tcb = tcb_buffer
                .get(child_index)
                .ok_or_else(|| {
                    InternalError::TaskGraphCorrupted("child index out-of-range".to_owned())
                })?
                .clone();
            children.push(child_tcb);
        }

        let is_input_task = task.is_input_task();
        let mut input_readers = Vec::new();
        for input_dep_index in task.get_input_dep_indices() {
            let reader = Reader::new(
                dataflow_dep_buffer
                    .get(*input_dep_index)
                    .ok_or_else(|| {
                        InternalError::TaskGraphCorrupted(
                            "input dependency index out-of-range".to_owned(),
                        )
                    })?
                    .clone(),
            );

            let has_value_payload = reader.read().await.is_some();
            if (is_input_task && !has_value_payload) || (!is_input_task && has_value_payload) {
                return Err(InternalError::TaskGraphCorrupted(
                    "dataflow deps initialization corrupted".to_owned(),
                ));
            }
            input_readers.push(InputReader::Value(reader));
        }

        let mut output_writers = Vec::new();
        for output_dep_index in task.get_output_dep_indices() {
            let writer = Writer::new(
                dataflow_dep_buffer
                    .get(*output_dep_index)
                    .ok_or_else(|| {
                        InternalError::TaskGraphCorrupted(
                            "output dependency index out-of-range".to_owned(),
                        )
                    })?
                    .clone(),
            );
            output_writers.push(writer);
        }

        let tcb = TaskControlBlock {
            base: TaskControlBlockBase {
                state,
                tdl_context,
                instance_pool,
                retry_counter,
                timeout_policy,
            },
            index,
            num_parents,
            num_unfinished_parents: num_parents,
            children,
            inputs: input_readers,
            outputs: output_writers,
        };

        Ok(Self {
            inner: Arc::new(tokio::sync::Mutex::new(tcb)),
        })
    }
}

/// A shareable control block for a termination task in the task graph, defining thread-safe
/// operations to manipulate task execution state.
#[derive(Clone)]
pub struct SharedTerminationTaskControlBlock {
    inner: Arc<tokio::sync::Mutex<TerminationTaskControlBlock>>,
}

impl SharedTerminationTaskControlBlock {
    /// Registers a new task instance to the control block.
    ///
    /// # Returns
    ///
    /// A tuple on success, containing:
    ///
    /// * The TDL context of the termination task.
    /// * The timeout policy of the termination task.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TaskControlBlockBase::register_task_instance`]'s return values on failure.
    pub async fn register_task_instance(
        &self,
        instance_id: TaskInstanceId,
    ) -> Result<(TdlContext, TimeoutPolicy), CacheError> {
        let mut tcb = self.inner.lock().await;
        tcb.base.register_task_instance(instance_id)?;
        Ok((
            tcb.base.tdl_context.clone(),
            tcb.base.timeout_policy.clone(),
        ))
    }

    /// Marks a task instance as succeeded.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TaskControlBlockBase::succeed_task_instance`]'s return values on failure.
    pub async fn succeed_task_instance(
        &self,
        instance_id: TaskInstanceId,
    ) -> Result<(), CacheError> {
        self.inner
            .lock()
            .await
            .base
            .succeed_task_instance(instance_id)
    }

    /// Marks a task instance as failed.
    ///
    /// # Returns
    ///
    /// The new state of the task after the failure is processed, forwarded from
    /// [`TaskControlBlockBase::fail_task_instance`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TaskControlBlockBase::fail_task_instance`]'s return values on failure.
    pub async fn fail_task_instance(
        &self,
        instance_id: TaskInstanceId,
        error_message: String,
    ) -> Result<TaskState, CacheError> {
        self.inner
            .lock()
            .await
            .base
            .fail_task_instance(instance_id, error_message)
    }

    /// Forcefully removes a task instance from the instance pool.
    ///
    /// # Returns
    ///
    /// Forwards [`TaskControlBlockBase::force_remove_task_instance`]'s return values.
    pub async fn force_remove_task_instance(&self, instance_id: TaskInstanceId) -> bool {
        let mut tcb = self.inner.lock().await;
        tcb.base.force_remove_task_instance(instance_id)
    }

    /// Marks the TCB state cancelled.
    pub async fn cancel_non_terminal(&self) {
        let mut tcb = self.inner.lock().await;
        tcb.base.cancel_non_terminal();
    }

    /// Private factory function for creating a new termination task control block from a task
    /// descriptor.
    ///
    /// # Returns
    ///
    /// A newly created instance of [`SharedTerminationTaskControlBlock`] on success.
    fn create(termination_task_descriptor: &TerminationTaskDescriptor) -> Self {
        let execution_policy = termination_task_descriptor
            .execution_policy
            .clone()
            .unwrap_or_default();
        let tcb = TerminationTaskControlBlock {
            base: TaskControlBlockBase {
                state: TaskState::Ready,
                tdl_context: termination_task_descriptor.tdl_context.clone(),
                instance_pool: InstancePool::create(execution_policy.max_num_instances as usize),
                retry_counter: RetryCounter::new(execution_policy.max_num_retry as usize),
                timeout_policy: execution_policy.timeout_policy,
            },
        };

        Self {
            inner: Arc::new(tokio::sync::Mutex::new(tcb)),
        }
    }
}

/// A pool of task instances that are currently running the same task.
struct InstancePool {
    instance_ids: HashSet<TaskInstanceId>,
    max_num_instances: usize,
}

impl InstancePool {
    /// Factory function.
    ///
    /// # NOTE
    ///
    /// The `max_num_instances` must be at least 1, otherwise it will be set to 1.
    ///
    /// # Returns
    ///
    /// A newly created instance of [`InstancePool`].
    fn create(max_num_instances: usize) -> Self {
        Self {
            instance_ids: HashSet::new(),
            max_num_instances: max(max_num_instances, 1),
        }
    }

    /// Adds a task instance to the pool.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::TaskInstanceLimitExceeded`] if the number of living task instances has
    ///   reached the upper limit.
    fn add(&mut self, instance_id: TaskInstanceId) -> Result<(), StaleStateError> {
        if self.instance_ids.len() >= self.max_num_instances {
            Err(StaleStateError::TaskInstanceLimitExceeded)
        } else {
            self.instance_ids.insert(instance_id);
            Ok(())
        }
    }

    /// Removes a task instance from the pool.
    ///
    /// # Returns
    ///
    /// Whether the instance has been found and removed from the pool.
    fn remove(&mut self, instance_id: TaskInstanceId) -> bool {
        self.instance_ids.remove(&instance_id)
    }

    /// # Returns
    ///
    /// Whether the pool is empty (i.e., has no living task instance).
    fn is_empty(&self) -> bool {
        self.instance_ids.is_empty()
    }

    fn reset(&mut self) {
        self.instance_ids.clear();
    }
}

/// A counter for tracking the number of retries left for a task.
struct RetryCounter {
    max_num_retries_allowed: usize,
    retry_count: usize,
}

impl RetryCounter {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created instance of [`RetryCounter`].
    const fn new(max_num_retries_allowed: usize) -> Self {
        Self {
            max_num_retries_allowed,
            retry_count: max_num_retries_allowed,
        }
    }

    /// # Returns
    ///
    /// The number of retries left before the retry count is exhausted. If 0 is returned, it means
    /// that the retry count is exhausted and no more retries are allowed.
    const fn retry(&mut self) -> usize {
        if self.retry_count == 0 {
            // In practice, this is possible if the total number of task instances creates are
            // greater than the number of retries allowed.
            return 0;
        }
        let num_retries_left = self.retry_count;
        self.retry_count -= 1;
        num_retries_left
    }

    const fn reset(&mut self) {
        self.retry_count = self.max_num_retries_allowed;
    }
}

/// The basic control block for a task in the task graph.
///
/// Operations defined in this struct are not thread-safe. It requires upper-level synchronization
/// to ensure that concurrent operations on the same task are properly serialized.
struct TaskControlBlockBase {
    state: TaskState,
    tdl_context: TdlContext,
    instance_pool: InstancePool,
    retry_counter: RetryCounter,
    timeout_policy: TimeoutPolicy,
}

impl TaskControlBlockBase {
    /// Registers a new task instance to the control block.
    ///
    /// On success, the task instance ID will be added to the instance pool, and the task will
    /// be in [`TaskState::Running`] state on return.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::TaskAlreadyTerminated`] if the task is already in a terminal state.
    /// * [`InternalError::TaskNotReady`] if the task is not yet ready.
    /// * Forwards [`InstancePool::add`]'s return values on failure.
    fn register_task_instance(&mut self, instance_id: TaskInstanceId) -> Result<(), CacheError> {
        if self.state.is_terminal() {
            return Err(StaleStateError::TaskAlreadyTerminated(self.state.clone()).into());
        }
        if !matches!(self.state, TaskState::Ready | TaskState::Running) {
            return Err(InternalError::TaskNotReady.into());
        }

        self.instance_pool.add(instance_id)?;
        self.state = TaskState::Running;
        Ok(())
    }

    /// Marks a task instance as succeeded.
    ///
    /// On success, the task instance ID will be removed from the instance pool, and the task will
    /// be in [`TaskState::Succeeded`] state on return.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::InvalidTaskInstanceId`] if the task instance ID is not found in the
    ///   instance pool. This can happen if the task instance has already been removed due to
    ///   timeout.
    /// * [`StaleStateError::TaskAlreadyTerminated`] if the task is already in a terminal state.
    /// * [`InternalError::TaskNotRunning`] if the task is not in the running state.
    fn succeed_task_instance(&mut self, instance_id: TaskInstanceId) -> Result<(), CacheError> {
        if !self.instance_pool.remove(instance_id) {
            return Err(StaleStateError::InvalidTaskInstanceId.into());
        }
        if self.state.is_terminal() {
            return Err(StaleStateError::TaskAlreadyTerminated(self.state.clone()).into());
        }
        if self.state != TaskState::Running {
            return Err(InternalError::TaskNotRunning.into());
        }
        self.state = TaskState::Succeeded;
        Ok(())
    }

    /// Marks a task instance as failed.
    ///
    /// On success, the task instance ID will be removed from the instance pool.
    ///
    /// # Returns
    ///
    /// The new state of the task after the failure is processed. The state can be either:
    ///
    /// * [`TaskState::Ready`] if the task is still eligible for retry, and there is no other living
    ///   task instance.
    /// * [`TaskState::Running`] if the task is still eligible for retry, and there are still other
    ///   living task instances.
    /// * [`TaskState::Failed`] if the task is no longer eligible for retry.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::InvalidTaskInstanceId`] if the task instance ID is not found in the
    ///   instance pool. This can happen if the task instance has already been removed due to
    ///   timeout.
    /// * [`StaleStateError::TaskAlreadyTerminated`] if the task is already in a terminal state.
    /// * [`InternalError::TaskNotRunning`] if the task is not in the running state.
    fn fail_task_instance(
        &mut self,
        task_instance_id: TaskInstanceId,
        error_message: String,
    ) -> Result<TaskState, CacheError> {
        if !self.instance_pool.remove(task_instance_id) {
            return Err(StaleStateError::InvalidTaskInstanceId.into());
        }
        if self.state.is_terminal() {
            return Err(StaleStateError::TaskAlreadyTerminated(self.state.clone()).into());
        }
        if self.state != TaskState::Running {
            return Err(InternalError::TaskNotRunning.into());
        }

        if self.retry_counter.retry() != 0 {
            self.state = if self.instance_pool.is_empty() {
                TaskState::Ready
            } else {
                TaskState::Running
            };
        } else {
            self.state = TaskState::Failed(error_message);
        }
        Ok(self.state.clone())
    }

    /// Forcefully removes a task instance from the instance pool.
    ///
    /// This method is for the background GC to clean up timed-out task instances (running in a
    /// disconnected execution environment).
    ///
    /// # Returns
    ///
    /// Whether the task instance was found and removed from the instance pool.
    fn force_remove_task_instance(&mut self, instance_id: TaskInstanceId) -> bool {
        let existed = self.instance_pool.remove(instance_id);
        if existed && self.state == TaskState::Running && self.instance_pool.is_empty() {
            // NOTE: We should not reset the task state to [`TaskState::Ready`] if:
            // * The task instance is not found in the instance pool, meaning that it may have
            //   already been removed.
            // * The task is not in the running state, meaning that it may already terminate.
            // * The instance pool is not empty, meaning that there are still other living task
            //   instances.
            self.state = TaskState::Ready;
        }
        existed
    }

    /// Cancels if the task is in a non-terminal state.
    fn cancel_non_terminal(&mut self) {
        if !self.state.is_terminal() {
            self.state = TaskState::Cancelled;
        }
    }
}

/// The control block for a task in the task graph, containing both the basic control block, the
/// dataflow information, and the control flow information.
///
/// Operations defined in this struct are not thread-safe. It requires upper-level synchronization
/// to ensure that concurrent operations on the same task are properly serialized.
struct TaskControlBlock {
    base: TaskControlBlockBase,
    index: TaskIndex,
    num_parents: usize,
    num_unfinished_parents: usize,
    children: Vec<SharedTaskControlBlock>,
    inputs: Vec<InputReader>,
    outputs: Vec<OutputWriter>,
}

impl TaskControlBlock {
    /// Writes the task outputs.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::TaskOutputsLengthMismatch`] if the length of the provided task outputs
    ///   doesn't match the length of the task outputs defined in the task control block.
    /// * [`InternalError::TaskOutputAlreadyWritten`] if any of the task outputs has already been
    ///   written (the task output is assumed to be produced by a single source).
    async fn write_outputs(&self, task_outputs: Vec<TaskOutput>) -> Result<(), InternalError> {
        if task_outputs.len() != self.outputs.len() {
            return Err(InternalError::TaskOutputsLengthMismatch(
                self.outputs.len(),
                task_outputs.len(),
            ));
        }

        // Write task outputs
        // NOTE: Currently, there is only one possible task output type (value payload) and thus we
        // do not need to validate the type. In the future, when more task output types are
        // supported, type validation should be done before any writes happens to avoid partial
        // writes.
        for (output_writer, task_output) in self.outputs.iter().zip(task_outputs) {
            let mut output = output_writer.write().await;
            if output.is_some() {
                return Err(InternalError::TaskOutputAlreadyWritten);
            }
            *output = Some(task_output);
        }

        Ok(())
    }

    /// Reads the task inputs.
    ///
    /// # Returns
    ///
    /// A vector of [`TaskInput`] read from the input readers defined in the task control block.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`InputReader::read_as_task_input`]'s return values on failure.
    async fn fetch_inputs(&self) -> Result<Vec<TaskInput>, CacheError> {
        let mut inputs = Vec::with_capacity(self.inputs.len());
        for input_reader in &self.inputs {
            inputs.push(input_reader.read_as_task_input().await?);
        }
        Ok(inputs)
    }
}

/// The control block for a termination task in the task graph.
///
/// Operations defined in this struct are not thread-safe. It requires upper-level synchronization
/// to ensure that concurrent operations on the same task are properly serialized.
struct TerminationTaskControlBlock {
    base: TaskControlBlockBase,
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        hash::{BuildHasher, Hasher, RandomState},
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use spider_core::task::{
        DataTypeDescriptor,
        ExecutionPolicy,
        TaskDescriptor,
        TaskGraph as SubmittedTaskGraph,
        TerminationTaskDescriptor,
        ValueTypeDescriptor,
    };

    use super::*;

    /// # Returns
    ///
    /// A unique task instance ID issued by a global atomic counter.
    fn next_instance_id() -> TaskInstanceId {
        static INSTANCE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
        INSTANCE_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
    }

    /// # Returns
    ///
    /// A random 4-byte vector using [`RandomState`] as a source of randomness.
    fn random_bytes() -> Vec<u8> {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u8(0);
        hasher.finish().to_ne_bytes()[..4].to_vec()
    }

    /// Spawns `count` concurrent tasks that each wait on the barrier, then call `register` with a
    /// fresh instance ID.
    ///
    /// This is generic over the registration closure, allowing it to work with both
    /// [`SharedTaskControlBlock`] and [`SharedTerminationTaskControlBlock`] despite their different
    /// `register_task_instance` return types.
    ///
    /// # Type Parameters
    ///
    /// * [`RegisterFuncType`] - An async function type that takes a `TaskInstanceId` and returns
    ///   future that resolves to the registration result type.
    /// * [`FutureType`] - The future type returned by the registration function.
    /// * [`ReturnType`] - The type of the registration result returned by the future.
    ///
    /// # Returns
    ///
    /// A vector of join handles, each resolving to a registration result.
    fn spawn_concurrent_registrations<RegisterFuncType, FutureType, ReturnType>(
        barrier: &Arc<tokio::sync::Barrier>,
        count: usize,
        register: RegisterFuncType,
    ) -> Vec<tokio::task::JoinHandle<ReturnType>>
    where
        RegisterFuncType: Fn(TaskInstanceId) -> FutureType + Send + Clone + 'static,
        FutureType: Future<Output = ReturnType> + Send + 'static,
        ReturnType: Send + 'static, {
        (0..count)
            .map(|_| {
                let barrier = barrier.clone();
                let register = register.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    let id = next_instance_id();
                    register(id).await
                })
            })
            .collect()
    }

    /// # Returns
    ///
    /// Bit-level XOR result of the two slices.
    ///
    /// # Panics
    ///
    /// Panics if the slices have different lengths.
    fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
        assert_eq!(a.len(), b.len(), "xor_bytes requires equal-length slices");
        a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect()
    }

    /// Precomputed expected values for the diamond task graph XOR transformations.
    struct DiamondExpectedValues {
        graph_inputs: [Vec<u8>; 2],
        a_outputs: [Vec<u8>; 2],
        b_output: Vec<u8>,
        c_output: Vec<u8>,
        d_output: Vec<u8>,
    }

    /// Computes all expected intermediate and final values for the diamond task graph.
    ///
    /// Transformations:
    ///
    /// * A: `out0 = b ^ a ^ b`, `out1 = a ^ b ^ a`
    /// * B: `out0 = in0 ^ in1 ^ in0`
    /// * C: `out0 = in1 ^ in0 ^ in1`
    /// * D: `out0 = in0 ^ in1`
    ///
    /// # Returns
    ///
    /// The computed expected values for the diamond task graph.
    fn compute_diamond_expected_values(a: &[u8], b: &[u8]) -> DiamondExpectedValues {
        let a_out0 = xor_bytes(&xor_bytes(b, a), b);
        let a_out1 = xor_bytes(&xor_bytes(a, b), a);

        let b_output = xor_bytes(&xor_bytes(&a_out0, &a_out1), &a_out0);
        let c_output = xor_bytes(&xor_bytes(&a_out1, &a_out0), &a_out1);
        let d_output = xor_bytes(&b_output, &c_output);

        DiamondExpectedValues {
            graph_inputs: [a.to_vec(), b.to_vec()],
            a_outputs: [a_out0, a_out1],
            b_output,
            c_output,
            d_output,
        }
    }

    /// Builds a cache [`TaskGraph`] containing a single TCB with configurable execution policy and
    /// I/O counts.
    ///
    /// # Returns
    ///
    /// A cache [`TaskGraph`] with one task at index 0. Each input is initialized to a 4-byte zero
    /// payload.
    async fn build_task_graph_with_single_tcb(
        max_num_instances: u32,
        max_num_retry: u32,
        num_inputs: usize,
        num_outputs: usize,
    ) -> TaskGraph {
        let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
        let mut submitted =
            SubmittedTaskGraph::new(None, None).expect("empty task graph creation should succeed");
        submitted
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: "test_pkg".to_owned(),
                    task_func: "test_fn".to_owned(),
                },
                execution_policy: Some(ExecutionPolicy {
                    max_num_retry,
                    max_num_instances,
                    ..ExecutionPolicy::default()
                }),
                inputs: vec![bytes_type.clone(); num_inputs],
                outputs: vec![bytes_type; num_outputs],
                input_sources: None,
            })
            .expect("task insertion should succeed");

        let inputs: Vec<TaskInput> = (0..num_inputs)
            .map(|_| TaskInput::ValuePayload(vec![0u8; 4]))
            .collect();
        TaskGraph::create(&submitted, inputs)
            .await
            .expect("cache task graph creation should succeed")
    }

    /// Builds a [`SharedTerminationTaskControlBlock`] with configurable execution policy by
    /// creating a [`TaskGraph`] with a commit task and extracting the commit TCB.
    ///
    /// # Returns
    ///
    /// A [`SharedTerminationTaskControlBlock`] configured with the given execution policy.
    async fn build_termination_tcb(
        max_num_instances: u32,
        max_num_retry: u32,
    ) -> SharedTerminationTaskControlBlock {
        let submitted = SubmittedTaskGraph::new(
            Some(TerminationTaskDescriptor {
                tdl_context: TdlContext {
                    package: "test_pkg".to_owned(),
                    task_func: "test_commit_fn".to_owned(),
                },
                execution_policy: Some(ExecutionPolicy {
                    max_num_retry,
                    max_num_instances,
                    ..ExecutionPolicy::default()
                }),
            }),
            None,
        )
        .expect("task graph with commit task should be created");
        let task_graph = TaskGraph::create(&submitted, vec![])
            .await
            .expect("cache task graph creation should succeed");
        task_graph
            .get_commit_task_control_block()
            .expect("commit task should exist")
    }

    /// Builds a cache [`TaskGraph`] with a diamond-shaped structure.
    ///
    /// All tasks use the default execution policy and all data types are `Bytes`.
    ///
    /// Control flow (parent -> child):
    ///
    /// ```text
    ///      A (task 0)
    ///     / \
    ///    B   C  (tasks 1, 2)
    ///     \ /
    ///      D (task 3)
    /// ```
    ///
    /// Dataflow:
    ///
    /// * Task A (input task): 2 graph inputs, 2 outputs.
    /// * Task B: 2 inputs from A (out 0 and out 1), 1 output.
    /// * Task C: 2 inputs from A (out 0 and out 1), 1 output.
    /// * Task D (output task): 2 inputs from B (out 0) and C (out 0), 1 output which is the graph
    ///   output.
    ///
    /// # Returns
    ///
    /// A cache [`TaskGraph`] with 4 tasks at indices 0 (A), 1 (B), 2 (C), 3 (D).
    async fn build_diamond_task_graph(input_a: Vec<u8>, input_b: Vec<u8>) -> TaskGraph {
        let submitted = SubmittedTaskGraph::from_json(
            r#"{
                "schema_version": "0.1.0",
                "commit_task": null,
                "cleanup_task": null,
                "tasks": [
                    {
                        "tdl_context": {
                            "package": "test_pkg",
                            "task_func": "task_a"
                        },
                        "inputs": [
                            {"Value": {"Bytes": {}}},
                            {"Value": {"Bytes": {}}}
                        ],
                        "outputs": [
                            {"Value": {"Bytes": {}}},
                            {"Value": {"Bytes": {}}}
                        ],
                        "input_sources": null
                    },
                    {
                        "tdl_context": {
                            "package": "test_pkg",
                            "task_func": "task_b"
                        },
                        "inputs": [
                            {"Value": {"Bytes": {}}},
                            {"Value": {"Bytes": {}}}
                        ],
                        "outputs": [
                            {"Value": {"Bytes": {}}}
                        ],
                        "input_sources": [
                            {"task_idx": 0, "position": 0},
                            {"task_idx": 0, "position": 1}
                        ]
                    },
                    {
                        "tdl_context": {
                            "package": "test_pkg",
                            "task_func": "task_c"
                        },
                        "inputs": [
                            {"Value": {"Bytes": {}}},
                            {"Value": {"Bytes": {}}}
                        ],
                        "outputs": [
                            {"Value": {"Bytes": {}}}
                        ],
                        "input_sources": [
                            {"task_idx": 0, "position": 0},
                            {"task_idx": 0, "position": 1}
                        ]
                    },
                    {
                        "tdl_context": {
                            "package": "test_pkg",
                            "task_func": "task_d"
                        },
                        "inputs": [
                            {"Value": {"Bytes": {}}},
                            {"Value": {"Bytes": {}}}
                        ],
                        "outputs": [
                            {"Value": {"Bytes": {}}}
                        ],
                        "input_sources": [
                            {"task_idx": 1, "position": 0},
                            {"task_idx": 2, "position": 0}
                        ]
                    }
                ]
            }"#,
        )
        .expect("diamond task graph JSON deserialization should succeed");

        let inputs = vec![
            TaskInput::ValuePayload(input_a),
            TaskInput::ValuePayload(input_b),
        ];
        TaskGraph::create(&submitted, inputs)
            .await
            .expect("cache task graph creation should succeed")
    }

    /// Registers a task instance, verifies its inputs against `expected_inputs`, computes outputs
    /// via `compute_outputs`, succeeds the instance, and returns the ready child indices.
    ///
    /// # Returns
    ///
    /// Forwards [`SharedTaskControlBlock::succeed_task_instance`]'s return values.
    async fn register_verify_and_succeed(
        task_graph: &TaskGraph,
        task_index: TaskIndex,
        expected_inputs: &[Vec<u8>],
        compute_outputs: impl FnOnce(&[TaskInput]) -> Vec<TaskOutput>,
    ) -> Vec<TaskIndex> {
        let tcb = task_graph
            .get_task_control_block(task_index)
            .expect("task should exist");
        let id = next_instance_id();
        let ctx = tcb
            .register_task_instance(id)
            .await
            .expect("register should succeed");
        let expected: Vec<TaskInput> = expected_inputs
            .iter()
            .map(|v| TaskInput::ValuePayload(v.clone()))
            .collect();
        assert_eq!(ctx.inputs, expected, "task {task_index} inputs mismatch");
        let outputs = compute_outputs(&ctx.inputs);
        tcb.succeed_task_instance(id, outputs)
            .await
            .expect("succeed should work")
    }

    /// Spawns a task that registers, verifies inputs against `expected_inputs`, waits on the
    /// barrier, computes outputs via `compute_outputs`, succeeds the instance, and returns the
    /// ready child indices.
    ///
    /// # Returns
    ///
    /// Forwards [`SharedTaskControlBlock::succeed_task_instance`]'s return values.
    async fn spawn_register_verify_and_succeed(
        tcb: SharedTaskControlBlock,
        barrier: Arc<tokio::sync::Barrier>,
        expected_inputs: Vec<Vec<u8>>,
        compute_outputs: impl FnOnce(&[TaskInput]) -> Vec<TaskOutput> + Send + 'static,
    ) -> Vec<TaskIndex> {
        tokio::spawn(async move {
            let id = next_instance_id();
            let ctx = tcb
                .register_task_instance(id)
                .await
                .expect("register should succeed");
            let expected: Vec<TaskInput> = expected_inputs
                .iter()
                .map(|v| TaskInput::ValuePayload(v.clone()))
                .collect();
            assert_eq!(ctx.inputs, expected, "task inputs mismatch");
            barrier.wait().await;
            let outputs = compute_outputs(&ctx.inputs);
            tcb.succeed_task_instance(id, outputs)
                .await
                .expect("succeed should work")
        })
        .await
        .expect("spawned task should not panic")
    }

    /// Extracts the raw byte payload from a [`TaskInput::ValuePayload`].
    fn unwrap_payload(input: &TaskInput) -> &[u8] {
        match input {
            TaskInput::ValuePayload(v) => v,
        }
    }

    /// Asserts that the task graph outputs match the expected byte payloads.
    async fn assert_graph_outputs(task_graph: &TaskGraph, expected: &[Vec<u8>]) {
        let outputs = task_graph.get_outputs();
        assert_eq!(outputs.len(), expected.len(), "graph output count mismatch");
        for (i, (reader, exp)) in outputs.iter().zip(expected).enumerate() {
            let value = reader.read().await;
            assert_eq!(*value, Some(exp.clone()), "graph output {i} mismatch");
            drop(value);
        }
    }

    /// Generates a suite of registration, failure, and termination tests for a TCB type.
    ///
    /// Both [`SharedTaskControlBlock`] and [`SharedTerminationTaskControlBlock`] share the same
    /// underlying [`TaskControlBlockBase`] state machine, so the registration and failure semantics
    /// are identical. This macro captures the shared test logic and parametrizes the three points
    /// where the two types diverge:
    ///
    /// * **`build_tcb`** — how to construct the TCB with a given execution policy.
    /// * **`succeed`** — how to call `succeed_task_instance` (the regular TCB requires output
    ///   payloads, while the termination TCB does not).
    ///
    /// `register_task_instance` and `fail_task_instance` share the same call signature across both
    /// types, so they are called directly inside the macro body. The differing return types of
    /// `register_task_instance` are handled by [`spawn_concurrent_registrations`]'s generic
    /// closure parameter.
    ///
    /// # Generated tests
    ///
    /// * `concurrent_registration_up_to_limit`
    /// * `concurrent_registration_exceeding_limit`
    /// * `registration_after_termination`
    /// * `fail_first_instance_then_succeed_new`
    /// * `fail_instance_then_reject_stale_and_surviving`
    macro_rules! registration_test_suite {
        (
            build_tcb($max_instances:ident, $max_retry:ident) => $build_tcb:expr,
            succeed($s_tcb:ident, $s_id:ident) => $succeed:expr $(,)?
        ) => {
            #[tokio::test(flavor = "multi_thread")]
            async fn concurrent_registration_up_to_limit() {
                const MAX_NUM_INSTANCES: u32 = 10;
                let tcb = {
                    let ($max_instances, $max_retry) = (MAX_NUM_INSTANCES, 0u32);
                    $build_tcb
                };

                let barrier = Arc::new(tokio::sync::Barrier::new(MAX_NUM_INSTANCES as usize));
                let register = {
                    let tcb = tcb.clone();
                    move |id: TaskInstanceId| {
                        let tcb = tcb.clone();
                        async move { tcb.register_task_instance(id).await }
                    }
                };
                let handles =
                    spawn_concurrent_registrations(&barrier, MAX_NUM_INSTANCES as usize, register);

                for handle in handles {
                    let result = handle.await.expect("task should not panic");
                    assert!(
                        result.is_ok(),
                        "all registrations should succeed, got: {result:?}"
                    );
                }
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn concurrent_registration_exceeding_limit() {
                const MAX_NUM_INSTANCES: u32 = 10;
                const NUM_INSTANCES_TO_EXCEED_LIMIT: u32 = 6;
                const NUM_REGISTRATIONS: usize =
                    (MAX_NUM_INSTANCES + NUM_INSTANCES_TO_EXCEED_LIMIT) as usize;
                let tcb = {
                    let ($max_instances, $max_retry) = (MAX_NUM_INSTANCES, 0u32);
                    $build_tcb
                };

                let barrier = Arc::new(tokio::sync::Barrier::new(NUM_REGISTRATIONS));
                let register = {
                    let tcb = tcb.clone();
                    move |id: TaskInstanceId| {
                        let tcb = tcb.clone();
                        async move { tcb.register_task_instance(id).await }
                    }
                };
                let handles = spawn_concurrent_registrations(&barrier, NUM_REGISTRATIONS, register);

                let mut successes = 0u32;
                let mut limit_exceeded = 0u32;
                for handle in handles {
                    let result = handle.await.expect("task should not panic");
                    match result {
                        Ok(_) => successes += 1,
                        Err(CacheError::StaleState(StaleStateError::TaskInstanceLimitExceeded)) => {
                            limit_exceeded += 1;
                        }
                        Err(e) => panic!("unexpected error: {e:?}"),
                    }
                }
                assert_eq!(
                    successes, MAX_NUM_INSTANCES,
                    "exactly {MAX_NUM_INSTANCES} registrations should succeed"
                );
                assert_eq!(
                    limit_exceeded, NUM_INSTANCES_TO_EXCEED_LIMIT,
                    "exactly {NUM_INSTANCES_TO_EXCEED_LIMIT} should be rejected as limit exceeded"
                );
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn registration_after_termination() {
                const NUM_REGISTRATION_ATTEMPTS: usize = 10;
                let tcb = {
                    let ($max_instances, $max_retry) = (1u32, 0u32);
                    $build_tcb
                };

                // Register and succeed one instance to terminate the task.
                let id = next_instance_id();
                tcb.register_task_instance(id)
                    .await
                    .expect("first registration should succeed");
                {
                    let ($s_tcb, $s_id) = (&tcb, id);
                    $succeed
                }
                .expect("succeed should work");

                // All subsequent registrations should be rejected.
                let barrier = Arc::new(tokio::sync::Barrier::new(NUM_REGISTRATION_ATTEMPTS));
                let register = {
                    let tcb = tcb.clone();
                    move |id: TaskInstanceId| {
                        let tcb = tcb.clone();
                        async move { tcb.register_task_instance(id).await }
                    }
                };
                let handles =
                    spawn_concurrent_registrations(&barrier, NUM_REGISTRATION_ATTEMPTS, register);

                for handle in handles {
                    let result = handle.await.expect("task should not panic");
                    assert!(
                        matches!(
                            result,
                            Err(CacheError::StaleState(
                                StaleStateError::TaskAlreadyTerminated(_)
                            ))
                        ),
                        "registration after termination should return `TaskAlreadyTerminated`, \
                         got: {result:?}"
                    );
                }
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn fail_first_instance_then_succeed_new() {
                let tcb = {
                    let ($max_instances, $max_retry) = (1u32, 1u32);
                    $build_tcb
                };

                // Register and fail instance A.
                let id_a = next_instance_id();
                tcb.register_task_instance(id_a)
                    .await
                    .expect("registration A should succeed");
                let state_after_fail = tcb
                    .fail_task_instance(id_a, "test failure".to_owned())
                    .await
                    .expect("fail A should succeed");
                assert!(
                    !state_after_fail.is_terminal(),
                    "state after fail with retries remaining should be non-terminal, got: \
                     {state_after_fail:?}"
                );

                // Register and succeed instance B.
                let id_b = next_instance_id();
                tcb.register_task_instance(id_b)
                    .await
                    .expect("registration B should succeed after A failed");
                {
                    let ($s_tcb, $s_id) = (&tcb, id_b);
                    $succeed
                }
                .expect("succeed B should work");
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn fail_instance_then_reject_stale_and_surviving() {
                let tcb = {
                    let ($max_instances, $max_retry) = (2u32, 0u32);
                    $build_tcb
                };

                // Register two instances.
                let id_a = next_instance_id();
                let id_b = next_instance_id();
                tcb.register_task_instance(id_a)
                    .await
                    .expect("registration A should succeed");
                tcb.register_task_instance(id_b)
                    .await
                    .expect("registration B should succeed");

                // Fail instance A with no retries available.
                let state_after_fail = tcb
                    .fail_task_instance(id_a, "fatal failure".to_owned())
                    .await
                    .expect("fail A should succeed");
                assert!(
                    matches!(state_after_fail, TaskState::Failed(_)),
                    "state should be Failed with no retries remaining, got: {state_after_fail:?}"
                );

                // Attempting to succeed the already-failed instance should be rejected.
                let result = {
                    let ($s_tcb, $s_id) = (&tcb, id_a);
                    $succeed
                };
                assert!(
                    matches!(
                        result,
                        Err(CacheError::StaleState(
                            StaleStateError::InvalidTaskInstanceId
                        ))
                    ),
                    "succeed on failed instance should return `InvalidTaskInstanceId`, got: \
                     {result:?}"
                );

                // Attempting to succeed the surviving instance should also be rejected
                // because the task is already in a terminal Failed state.
                let result = {
                    let ($s_tcb, $s_id) = (&tcb, id_b);
                    $succeed
                };
                assert!(
                    matches!(
                        result,
                        Err(CacheError::StaleState(
                            StaleStateError::TaskAlreadyTerminated(_)
                        ))
                    ),
                    "succeed on surviving instance should return `TaskAlreadyTerminated`, got: \
                     {result:?}"
                );
            }
        };
    }

    mod task_control_block {
        use super::*;

        registration_test_suite! {
            build_tcb(max_instances, max_retry) => {
                let task_graph =
                    build_task_graph_with_single_tcb(max_instances, max_retry, 1, 1).await;
                task_graph
                    .get_task_control_block(0)
                    .expect("task 0 should exist")
            },
            succeed(tcb, id) => tcb.succeed_task_instance(id, vec![vec![0u8; 4]]).await,
        }
    }

    mod termination_task_control_block {
        use super::*;

        registration_test_suite! {
            build_tcb(max_instances, max_retry) =>
                build_termination_tcb(max_instances, max_retry).await,
            succeed(tcb, id) => tcb.succeed_task_instance(id).await,
        }
    }

    #[tokio::test]
    async fn diamond_sequential_execution() {
        let (a_val, b_val) = (random_bytes(), random_bytes());
        let expected = compute_diamond_expected_values(&a_val, &b_val);
        let task_graph = build_diamond_task_graph(
            expected.graph_inputs[0].clone(),
            expected.graph_inputs[1].clone(),
        )
        .await;

        // Task A: out0 = b ^ a ^ b, out1 = a ^ b ^ a.
        let mut ready =
            register_verify_and_succeed(&task_graph, 0, &expected.graph_inputs, |inputs| {
                let a = unwrap_payload(&inputs[0]);
                let b = unwrap_payload(&inputs[1]);
                vec![
                    xor_bytes(&xor_bytes(b, a), b),
                    xor_bytes(&xor_bytes(a, b), a),
                ]
            })
            .await;
        ready.sort_unstable();
        assert_eq!(ready, vec![1, 2], "succeeding A should make B and C ready");

        // Task B: out0 = in0 ^ in1 ^ in0.
        let ready = register_verify_and_succeed(&task_graph, 1, &expected.a_outputs, |inputs| {
            let in0 = unwrap_payload(&inputs[0]);
            let in1 = unwrap_payload(&inputs[1]);
            vec![xor_bytes(&xor_bytes(in0, in1), in0)]
        })
        .await;
        assert!(
            ready.is_empty(),
            "succeeding B alone should not make D ready"
        );

        // Task C: out0 = in1 ^ in0 ^ in1.
        let ready = register_verify_and_succeed(&task_graph, 2, &expected.a_outputs, |inputs| {
            let in0 = unwrap_payload(&inputs[0]);
            let in1 = unwrap_payload(&inputs[1]);
            vec![xor_bytes(&xor_bytes(in1, in0), in1)]
        })
        .await;
        assert_eq!(ready, vec![3], "succeeding C should make D ready");

        // Task D: out0 = in0 ^ in1.
        let ready = register_verify_and_succeed(
            &task_graph,
            3,
            &[expected.b_output.clone(), expected.c_output.clone()],
            |inputs| {
                let in0 = unwrap_payload(&inputs[0]);
                let in1 = unwrap_payload(&inputs[1]);
                vec![xor_bytes(in0, in1)]
            },
        )
        .await;
        assert!(
            ready.is_empty(),
            "D (output task) should have no ready children"
        );

        assert_graph_outputs(&task_graph, &[expected.d_output]).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn diamond_concurrent_bc_execution() {
        let (a_val, b_val) = (random_bytes(), random_bytes());
        let expected = compute_diamond_expected_values(&a_val, &b_val);
        let task_graph = build_diamond_task_graph(
            expected.graph_inputs[0].clone(),
            expected.graph_inputs[1].clone(),
        )
        .await;

        // Complete task A sequentially: out0 = b ^ a ^ b, out1 = a ^ b ^ a.
        register_verify_and_succeed(&task_graph, 0, &expected.graph_inputs, |inputs| {
            let a = unwrap_payload(&inputs[0]);
            let b = unwrap_payload(&inputs[1]);
            vec![
                xor_bytes(&xor_bytes(b, a), b),
                xor_bytes(&xor_bytes(a, b), a),
            ]
        })
        .await;

        // Run B and C concurrently behind a barrier.
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let a_outputs_vec = expected.a_outputs.to_vec();

        // B: out0 = in0 ^ in1 ^ in0.
        let ready_b = spawn_register_verify_and_succeed(
            task_graph
                .get_task_control_block(1)
                .expect("task B should exist"),
            barrier.clone(),
            a_outputs_vec.clone(),
            |inputs| {
                let in0 = unwrap_payload(&inputs[0]);
                let in1 = unwrap_payload(&inputs[1]);
                vec![xor_bytes(&xor_bytes(in0, in1), in0)]
            },
        );
        // C: out0 = in1 ^ in0 ^ in1.
        let ready_c = spawn_register_verify_and_succeed(
            task_graph
                .get_task_control_block(2)
                .expect("task C should exist"),
            barrier,
            a_outputs_vec,
            |inputs| {
                let in0 = unwrap_payload(&inputs[0]);
                let in1 = unwrap_payload(&inputs[1]);
                vec![xor_bytes(&xor_bytes(in1, in0), in1)]
            },
        );
        let (ready_b, ready_c) = tokio::join!(ready_b, ready_c);

        // Exactly one of B or C should report D (index 3) as ready.
        let d_ready_count = ready_b
            .iter()
            .chain(ready_c.iter())
            .filter(|&&i| i == 3)
            .count();
        assert_eq!(
            d_ready_count, 1,
            "exactly one of B/C should report D as ready, B={ready_b:?}, C={ready_c:?}"
        );

        // D: out0 = in0 ^ in1.
        let ready = register_verify_and_succeed(
            &task_graph,
            3,
            &[expected.b_output.clone(), expected.c_output.clone()],
            |inputs| {
                let in0 = unwrap_payload(&inputs[0]);
                let in1 = unwrap_payload(&inputs[1]);
                vec![xor_bytes(in0, in1)]
            },
        )
        .await;
        assert!(ready.is_empty(), "D should have no ready children");

        assert_graph_outputs(&task_graph, &[expected.d_output]).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn force_remove_only_instance_resets_to_ready() {
        let task_graph = build_task_graph_with_single_tcb(1, 0, 1, 1).await;
        let tcb = task_graph
            .get_task_control_block(0)
            .expect("task 0 should exist");

        let id_a = next_instance_id();
        tcb.register_task_instance(id_a)
            .await
            .expect("registration A should succeed");

        // Force-remove A.
        let removed = tcb.force_remove_task_instance(id_a).await;
        assert!(
            removed,
            "force_remove should return true for existing instance"
        );

        // Succeed on the removed instance should fail.
        let result = tcb.succeed_task_instance(id_a, vec![vec![0u8; 4]]).await;
        assert!(
            matches!(
                result,
                Err(CacheError::StaleState(
                    StaleStateError::InvalidTaskInstanceId
                ))
            ),
            "succeed on force-removed instance should return InvalidTaskInstanceId, got: \
             {result:?}"
        );

        // A new instance can still be registered.
        let id_b = next_instance_id();
        tcb.register_task_instance(id_b)
            .await
            .expect("registration after force_remove should succeed");
    }

    /// Force-removing one of two instances keeps the task running. The removed instance can no
    /// longer succeed, but the surviving instance can.
    #[tokio::test(flavor = "multi_thread")]
    async fn force_remove_one_of_two_instances() {
        let task_graph = build_task_graph_with_single_tcb(2, 0, 1, 1).await;
        let tcb = task_graph
            .get_task_control_block(0)
            .expect("task 0 should exist");

        let id_a = next_instance_id();
        let id_b = next_instance_id();
        tcb.register_task_instance(id_a)
            .await
            .expect("registration A should succeed");
        tcb.register_task_instance(id_b)
            .await
            .expect("registration B should succeed");

        // Force-remove A.
        let removed = tcb.force_remove_task_instance(id_a).await;
        assert!(
            removed,
            "force_remove should return true for existing instance"
        );

        // Succeed on removed instance A should fail.
        let result = tcb.succeed_task_instance(id_a, vec![vec![0u8; 4]]).await;
        assert!(
            matches!(
                result,
                Err(CacheError::StaleState(
                    StaleStateError::InvalidTaskInstanceId
                ))
            ),
            "succeed on force-removed instance should return InvalidTaskInstanceId, got: \
             {result:?}"
        );

        // Instance B can still succeed.
        tcb.succeed_task_instance(id_b, vec![vec![0u8; 4]])
            .await
            .expect("succeed B should work after A was force-removed");
    }
}
