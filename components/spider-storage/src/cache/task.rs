use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use spider_core::{
    task::{Task, TaskIndex, TaskState, TdlContext, TimeoutPolicy},
    types::{
        id::TaskInstanceId,
        io::{ExecutionContext, TaskInput, TaskOutput},
    },
};

use crate::cache::{
    error::{CacheError, InternalError, StaleStateError},
    io::{InputReader, OutputWriter, ValuePayload},
    sync::{Reader, Rw, Writer},
};

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
    ///   * Attempt to mark a parent of the child task as completed, but the child task has no
    ///     unfinished parents remaining.
    ///   * Attempt to update a child task after parent completion, but the child task is already in
    ///     a terminal state.
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
                if child_tcb.num_parents == 0 {
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
    pub fn reset(&self) {
        let mut tcb = self.inner.blocking_lock();
        tcb.base.instance_pool.reset();
        tcb.base.retry_counter.reset();
        tcb.num_unfinished_parents = tcb.num_parents;
        tcb.base.state = if tcb.num_parents == 0 {
            TaskState::Ready
        } else {
            TaskState::Pending
        };
        for output_writer in &tcb.outputs {
            *output_writer.blocking_write() = None;
        }
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
    /// # TODO
    ///
    /// Change the visibility of this method to `pub(crate)` after the task graph construction logic
    /// is implemented.
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
    pub fn create(
        task: &Task,
        tcb_buffer: &HashMap<TaskIndex, Self>,
        dataflow_dep_buffer: &[Rw<ValuePayload>],
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
                TaskState::Running
            } else {
                TaskState::Ready
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
