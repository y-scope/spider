use std::{collections::HashSet, sync::Arc};

use spider_core::{
    task::{TaskIndex, TaskState},
    types::{
        id::TaskInstanceId,
        io::{TaskInput, TaskOutput},
    },
};

use crate::cache::{
    error::{CacheError, InternalError, RejectionError},
    types::{ExecutionContext, Reader, TdlContext, Writer},
};

pub struct TaskGraph {
    pub(super) tasks: Vec<SharedTaskControlBlock>,
    pub(super) outputs: Vec<OutputReader>,
    pub(super) commit_task: Option<SharedTerminationTaskControlBlock>,
    pub(super) cleanup_task: Option<SharedTerminationTaskControlBlock>,
}

impl TaskGraph {
    pub fn get_task(&self, task_index: TaskIndex) -> Option<SharedTaskControlBlock> {
        self.tasks.get(task_index).cloned()
    }

    pub async fn get_outputs(&self) -> Result<Vec<TaskOutput>, RejectionError> {
        let mut outputs: Vec<TaskOutput> = Vec::with_capacity(self.outputs.len());
        for output_reader in &self.outputs {
            let output_guard = output_reader.read().await;
            if let Some(output) = &*output_guard {
                outputs.push(output.clone());
            } else {
                return Err(RejectionError::TaskOutputNotReady);
            }
        }
        Ok(outputs)
    }

    pub const fn has_commit_task(&self) -> bool {
        self.commit_task.is_some()
    }

    #[allow(dead_code)]
    pub const fn has_cleanup_task(&self) -> bool {
        self.cleanup_task.is_some()
    }

    pub fn get_commit_task(&self) -> Option<SharedTerminationTaskControlBlock> {
        self.commit_task.clone()
    }

    pub fn get_cleanup_task(&self) -> Option<SharedTerminationTaskControlBlock> {
        self.cleanup_task.clone()
    }
}

#[derive(Clone)]
pub struct SharedTaskControlBlock {
    inner: Arc<tokio::sync::Mutex<TaskControlBlock>>,
}

impl SharedTaskControlBlock {
    pub(super) fn new(inner: TaskControlBlock) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(inner)),
        }
    }

    /// Attempts to lock the inner mutex without blocking. Only intended for use during
    /// construction when no contention is possible.
    pub(super) fn try_lock_for_construction(
        &self,
    ) -> Result<tokio::sync::MutexGuard<'_, TaskControlBlock>, ()> {
        self.inner.try_lock().map_err(|_| ())
    }

    pub async fn register_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<ExecutionContext, CacheError> {
        let mut tcb = self.inner.lock().await;
        tcb.base.register_task_instance(task_instance_id)?;

        // NOTE: The following execution can only fail due to internal errors.
        let result: Result<_, InternalError> = {
            let inputs = tcb.fetch_inputs().await?;
            let execution_context = ExecutionContext {
                task_instance_id,
                tdl_context: tcb.base.tdl_context.clone(),
                inputs: Some(inputs),
            };
            Ok(execution_context)
        };
        result.map_err(CacheError::from)
    }

    pub async fn complete_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_outputs: Vec<TaskOutput>,
    ) -> Result<Vec<TaskIndex>, CacheError> {
        let mut tcb = self.inner.lock().await;
        tcb.base.complete_task_instance(task_instance_id)?;

        // NOTE: The following execution can only fail due to internal errors.
        let result: Result<_, InternalError> = {
            tcb.write_outputs(task_outputs).await?;
            let mut ready_child_indices = Vec::new();
            for child in &tcb.children {
                let mut child_tcb = child.inner.lock().await;
                if child_tcb.num_parents == 0 {
                    return Err(InternalError::TaskGraphCorrupted(
                        "the child has no unfinished parent, but it is still updated as if one of \
                         its parent just completed."
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
                        "a child task is in a terminal state, but it is still updated as if one \
                         of its parent just completed."
                            .to_owned(),
                    )
                    .into());
                }
                child_tcb.base.state = TaskState::Ready;
                ready_child_indices.push(child_tcb.index);
            }

            Ok(ready_child_indices)
        };
        result.map_err(CacheError::from)
    }

    pub async fn fail_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        error_message: String,
    ) -> Result<TaskState, CacheError> {
        let mut tcb = self.inner.lock().await;
        tcb.base
            .fail_task_instance(task_instance_id, error_message)
            .map_err(CacheError::from)
    }

    pub async fn reset(&self) {
        let mut tcb = self.inner.lock().await;
        tcb.base.instance_ids.clear();

        // Reset outputs
        for output_writer in &tcb.outputs {
            let mut output = output_writer.write().await;
            *output = None;
        }

        tcb.base.retry_counter.reset();

        tcb.num_unfinished_parents = tcb.num_parents;
        tcb.base.state = if tcb.num_unfinished_parents == 0 {
            TaskState::Ready
        } else {
            TaskState::Pending
        };
    }

    pub async fn force_remove_task_instance(&self, task_instance_id: TaskInstanceId) -> bool {
        let mut tcb = self.inner.lock().await;
        tcb.base.force_remove_task_instance(task_instance_id)
    }
}

#[derive(Clone)]
pub struct SharedTerminationTaskControlBlock {
    inner: Arc<tokio::sync::Mutex<TerminationTaskControlBlock>>,
}

impl SharedTerminationTaskControlBlock {
    pub(super) fn new(inner: TerminationTaskControlBlock) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(inner)),
        }
    }

    pub async fn register_termination_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<TdlContext, CacheError> {
        let mut tcb = self.inner.lock().await;
        tcb.base.register_task_instance(task_instance_id)?;
        Ok(tcb.base.tdl_context.clone())
    }

    pub async fn complete_termination_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
    ) -> Result<(), CacheError> {
        let mut tcb = self.inner.lock().await;
        tcb.base.complete_task_instance(task_instance_id)
    }

    pub async fn fail_termination_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        error_message: String,
    ) -> Result<TaskState, CacheError> {
        let mut tcb = self.inner.lock().await;
        tcb.base
            .fail_task_instance(task_instance_id, error_message)
            .map_err(CacheError::from)
    }

    pub async fn force_remove_task_instance(&self, task_instance_id: TaskInstanceId) -> bool {
        let mut tcb = self.inner.lock().await;
        tcb.base.force_remove_task_instance(task_instance_id)
    }
}

pub(super) struct BaseTaskControlBlock {
    pub(super) state: TaskState,
    pub(super) tdl_context: TdlContext,
    pub(super) instance_ids: HashSet<TaskInstanceId>,
    pub(super) max_num_instances: usize,
    pub(super) retry_counter: RetryCounter,
}

impl BaseTaskControlBlock {
    fn register_task_instance(
        &mut self,
        task_instance_id: TaskInstanceId,
    ) -> Result<(), CacheError> {
        if self.state.is_terminal() {
            return Err(RejectionError::TaskAlreadyTerminated(self.state.clone()).into());
        }
        if !matches!(self.state, TaskState::Ready | TaskState::Running) {
            return Err(InternalError::TaskNotReady.into());
        }
        if self.instance_ids.len() >= self.max_num_instances {
            return Err(RejectionError::TaskInstanceLimitExceeded.into());
        }
        self.instance_ids.insert(task_instance_id);
        self.state = TaskState::Running;
        Ok(())
    }

    fn complete_task_instance(
        &mut self,
        task_instance_id: TaskInstanceId,
    ) -> Result<(), CacheError> {
        if !self.instance_ids.remove(&task_instance_id) {
            return Err(RejectionError::InvalidTaskInstanceId.into());
        }
        if self.state.is_terminal() {
            return Err(RejectionError::TaskAlreadyTerminated(self.state.clone()).into());
        }
        self.state = TaskState::Succeeded;
        Ok(())
    }

    fn fail_task_instance(
        &mut self,
        task_instance_id: TaskInstanceId,
        error_message: String,
    ) -> Result<TaskState, RejectionError> {
        if !self.instance_ids.remove(&task_instance_id) {
            return Err(RejectionError::InvalidTaskInstanceId);
        }
        if self.state.is_terminal() {
            return Err(RejectionError::TaskAlreadyTerminated(self.state.clone()));
        }

        if self.retry_counter.retry() != 0 {
            self.state = if self.instance_ids.is_empty() {
                TaskState::Running
            } else {
                TaskState::Ready
            };
        } else {
            self.state = TaskState::Failed(error_message);
        }
        Ok(self.state.clone())
    }

    fn force_remove_task_instance(&mut self, task_instance_id: TaskInstanceId) -> bool {
        let existed = self.instance_ids.remove(&task_instance_id);
        if existed && self.state == TaskState::Running {
            self.state = TaskState::Ready;
        }
        existed
    }
}

pub(super) struct TaskControlBlock {
    pub(super) base: BaseTaskControlBlock,
    pub(super) index: TaskIndex,
    pub(super) num_parents: usize,
    pub(super) num_unfinished_parents: usize,
    pub(super) inputs: Vec<InputReader>,
    pub(super) outputs: Vec<OutputWriter>,
    pub(super) children: Vec<SharedTaskControlBlock>,
}

impl TaskControlBlock {
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
        for (output_writer, task_output) in self.outputs.iter().zip(task_outputs.into_iter()) {
            let mut output = output_writer.write().await;
            if output.is_some() {
                return Err(InternalError::TaskOutputDuplicateWrite);
            }
            *output = Some(task_output);
        }

        Ok(())
    }

    async fn fetch_inputs(&self) -> Result<Vec<TaskInput>, CacheError> {
        let mut inputs = Vec::with_capacity(self.inputs.len());
        for input_reader in &self.inputs {
            inputs.push(input_reader.read_as_task_input().await?);
        }
        Ok(inputs)
    }
}

pub(super) struct TerminationTaskControlBlock {
    pub(super) base: BaseTaskControlBlock,
}

pub(super) type ValuePayload = Option<Vec<u8>>;

#[derive(Clone)]
pub(super) struct Channel {}

#[allow(dead_code)]
pub(super) enum InputReader {
    Value(Reader<ValuePayload>),
    Channel(Channel),
}

impl InputReader {
    async fn read_as_task_input(&self) -> Result<TaskInput, CacheError> {
        match self {
            Self::Value(value_payload) => {
                let value_guard = value_payload.read().await;
                if let Some(value) = &*value_guard {
                    Ok(TaskInput::ValuePayload(value.clone()))
                } else {
                    Err(InternalError::TaskInputNotReady.into())
                }
            }
            Self::Channel(_) => unimplemented!("channel input is not supported yet"),
        }
    }
}

pub(super) type OutputReader = Reader<ValuePayload>;

pub(super) type OutputWriter = Writer<ValuePayload>;

pub(super) struct RetryCounter {
    max_num_retries_allowed: usize,
    retry_count: usize,
}

impl RetryCounter {
    pub(super) const fn new(max_num_retries_allowed: usize) -> Self {
        Self {
            max_num_retries_allowed,
            retry_count: max_num_retries_allowed,
        }
    }

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
