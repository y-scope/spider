use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::AtomicUsize},
};

use serde::Serialize;
use spider_core::{
    job::JobState,
    task::{DataflowDependencyIndex, Task, TaskIndex},
    types::{
        id::{JobId, TaskInstanceId},
        io::{TaskInput, TaskOutput},
    },
};

/// Enum for all possible states of a task.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum TaskState {
    Pending,
    Ready,
    Running,
    Succeeded,
    Failed(String),
    Cancelled,
}

impl TaskState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Succeeded | TaskState::Failed(_) | TaskState::Cancelled
        )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid task output")]
    InvalidTaskOutput,

    #[error("task output already written")]
    TaskOutputDuplicateWrite,

    #[error("task input not ready")]
    TaskInputNotReady,

    #[error("task outputs length mismatch: expected {0}, got {1}")]
    TaskOutputsLengthMismatch(usize, usize),

    #[error("task index {0} is out of bounds")]
    TaskIndexOutOfBound(TaskIndex),

    #[error("task is already in a terminal state: {0:?}")]
    TaskAlreadyTerminal(TaskState),

    #[error("job is already in a terminal state: {0:?}")]
    JobAlreadyTerminal(JobState),

    #[error("task is still pending")]
    TaskStillPending,

    #[error("task instance {0} is not registered")]
    TaskInstanceNotRegistered(TaskInstanceId),

    #[error("failed to send ready task to the queue: {0}")]
    TokioSendError(#[from] tokio::sync::mpsc::error::SendError<(JobId, TaskIndex)>),
}

#[derive(Serialize, Clone)]
pub struct TdlContext {
    package: String,
    func: String,
}

#[derive(Serialize)]
pub struct ExecutionContext {
    pub task_instance_id: TaskInstanceId,
    pub tdl_context: TdlContext,
    pub inputs: Vec<TaskInput>,
}

/// Internal representation of a data dependency.
enum Data {
    Value(Option<Vec<u8>>),
    Channel,
}

/// A shareable reference to a data object, allowing multiple tasks to read/write the same data
/// concurrently.
struct DataRef {
    data: Arc<std::sync::RwLock<Data>>,
}

impl DataRef {
    fn new_value(value: Vec<u8>) -> Self {
        Self {
            data: Arc::new(std::sync::RwLock::new(Data::Value(Some(value)))),
        }
    }

    fn new_null_value() -> Self {
        Self {
            data: Arc::new(std::sync::RwLock::new(Data::Value(None))),
        }
    }

    fn write_task_output(&self, task_output: TaskOutput) -> Result<(), Error> {
        match task_output {
            TaskOutput::ValuePayload(payload) => {
                match &mut *self.data.write().expect("rw lock poisoned") {
                    Data::Value(optional_value) => {
                        if optional_value.is_some() {
                            return Err(Error::TaskOutputDuplicateWrite);
                        }
                        *optional_value = Some(payload);
                    }
                    Data::Channel => {
                        return Err(Error::InvalidTaskOutput);
                    }
                }
            }
        }
        Ok(())
    }

    fn as_task_input(&self) -> Result<TaskInput, Error> {
        match &*self.data.read().expect("rw lock poisoned") {
            Data::Value(optional_value) => Ok(TaskInput::ValuePayload(
                optional_value.clone().ok_or(Error::TaskInputNotReady)?,
            )),
            Data::Channel => Err(Error::InvalidTaskOutput),
        }
    }
}

struct TaskMetadata {
    state: TaskState,
    tdl_context: TdlContext,
    registered_instances: HashSet<TaskInstanceId>,
    num_unfinished_parents: usize,
    inputs: Vec<DataRef>,
    outputs: Vec<DataRef>,
    children: Vec<TaskIndex>,
}

impl TaskMetadata {
    fn register(&mut self, task_instance_id: TaskInstanceId) -> Result<ExecutionContext, Error> {
        if self.state.is_terminal() {
            return Err(Error::TaskAlreadyTerminal(self.state.clone()));
        }
        if self.state != TaskState::Ready || self.state != TaskState::Running {
            return Err(Error::TaskStillPending);
        }
        self.state = TaskState::Running;
        self.registered_instances.insert(task_instance_id);
        Ok(ExecutionContext {
            task_instance_id,
            tdl_context: self.tdl_context.clone(),
            inputs: self.fetch_inputs()?,
        })
    }

    fn complete(
        &mut self,
        task_instance_id: TaskInstanceId,
        task_outputs: Vec<TaskOutput>,
    ) -> Result<(), Error> {
        if !self.registered_instances.contains(&task_instance_id) {
            return Err(Error::TaskInstanceNotRegistered(task_instance_id));
        }
        if self.state.is_terminal() {
            return Err(Error::TaskAlreadyTerminal(self.state.clone()));
        }
        self.write_outputs(task_outputs)?;
        self.state = TaskState::Succeeded;
        Ok(())
    }

    fn write_outputs(&self, task_outputs: Vec<TaskOutput>) -> Result<(), Error> {
        if task_outputs.len() != self.outputs.len() {
            return Err(Error::TaskOutputsLengthMismatch(
                self.outputs.len(),
                task_outputs.len(),
            ));
        }
        for (output_ref, output) in self.outputs.iter().zip(task_outputs.into_iter()) {
            output_ref.write_task_output(output)?;
        }
        Ok(())
    }

    fn fetch_inputs(&self) -> Result<Vec<TaskInput>, Error> {
        self.inputs
            .iter()
            .map(|input_ref| input_ref.as_task_input())
            .collect()
    }
}

struct TaskGraph {
    tasks: Vec<std::sync::Mutex<TaskMetadata>>,
}

struct JobMetadata {
    state: JobState,
    task_graph: TaskGraph,
    num_unfinished_tasks: AtomicUsize,
}

pub struct Job {
    id: JobId,
    metadata: std::sync::RwLock<JobMetadata>,
    ready_queue_sender: tokio::sync::mpsc::Sender<(JobId, TaskIndex)>,
}

impl Job {
    pub fn register_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_index: TaskIndex,
    ) -> Result<ExecutionContext, Error> {
        let job_metadata = self.metadata.read().expect("rw lock poisoned");
        let mut task_metadata = job_metadata
            .task_graph
            .tasks
            .get(task_index)
            .ok_or(Error::TaskIndexOutOfBound(task_index))?
            .lock()
            .expect("mutex poisoned");
        task_metadata.register(task_instance_id)
    }

    pub async fn complete_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        task_index: TaskIndex,
        task_outputs: Vec<TaskOutput>,
    ) -> Result<(), Error> {
        let job_metadata = self.metadata.read().expect("rw lock poisoned");

        // Update the task metadata
        let mut task_metadata = job_metadata
            .task_graph
            .tasks
            .get(task_index)
            .ok_or(Error::TaskIndexOutOfBound(task_index))?
            .lock()
            .expect("mutex poisoned");
        task_metadata.complete(task_instance_id, task_outputs)?;
        for child_idx in &task_metadata.children {
            let mut child_metadata = job_metadata
                .task_graph
                .tasks
                .get(*child_idx)
                .ok_or(Error::TaskIndexOutOfBound(*child_idx))?
                .lock()
                .expect("mutex poisoned");
            child_metadata.num_unfinished_parents -= 1;
            if child_metadata.num_unfinished_parents == 0 {
                child_metadata.state = TaskState::Ready;
                self.ready_queue_sender.send((self.id, *child_idx)).await?;
            }
        }
        let num_unfinished_tasks = job_metadata
            .num_unfinished_tasks
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            - 1;
        drop(task_metadata);
        drop(job_metadata);

        if num_unfinished_tasks > 0 {
            return Ok(());
        }

        // Atomic decrement guarantees that only one thread's control flow can reach here.
        let job_metadata = self.metadata.write().expect("rw lock poisoned");

        Ok(())
    }
}
