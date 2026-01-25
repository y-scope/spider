use std::collections::HashSet;
use crate::task::DataTypeDescriptor;
use crate::task::Error;

/// A unique identifier for a task within a task graph, assigned based on insertion order.
///
/// The task graph maintains tasks in topologically sorted order according to their insertion
/// sequence, ensuring that dependencies are always inserted before their dependents.
///
/// # Note
///
/// This type differs from [`crate::types::id::TaskId`]:
///
/// * [`TaskIndex`]: Local to a specific task graph instance, zero-based sequential identifier.
/// * [`crate::types::id::TaskId`]: Globally unique identifier for persisted tasks in storage.
pub type TaskIndex = usize;

/// A unique identifier for a data-flow dependency within a task graph, assigned based on insertion
/// order.
pub type DataflowDependencyIndex = usize;

/// A unique identifier for a task input/output in the task graph, including the task index and the
/// position of the input/output within that task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskInputOutputIndex {
    pub task_idx: TaskIndex,
    pub position: usize,
}

/// An in-memory representation of a task within a task graph.
///
/// This structure maintains:
///
/// * TDL information (including package name and task function name).
/// * Task inputs and outputs, represented as data-flow dependencies (positionally).
/// * Parent and child tasks which imply control-flow dependencies.
pub struct Task {
    idx: TaskIndex,
    tdl_package: String,
    tdl_function: String,
    parent_task_indices: HashSet<TaskIndex>,
    child_task_indices: HashSet<TaskIndex>,
    input_dep_indices: Vec<DataflowDependencyIndex>,
    output_dep_indices: Vec<DataflowDependencyIndex>,
}

impl Task {
    pub fn get_index(&self) -> TaskIndex {
        self.idx
    }

    pub fn get_num_parents(&self) -> usize {
        self.parent_task_indices.len()
    }

    pub fn get_num_children(&self) -> usize {
        self.child_task_indices.len()
    }

    /// # Returns
    ///
    /// Whether this task has no parent tasks. A task with no parent tasks is considered an input
    /// task.
    pub fn is_input_task(&self) -> bool {
        self.parent_task_indices.is_empty()
    }

    /// # Returns
    ///
    /// Whether this task has no child tasks. A task with no parent tasks is considered an output
    /// task.
    pub fn is_output_task(&self) -> bool {
        self.child_task_indices.is_empty()
    }

    pub fn get_parent_task_indices(&self) -> &HashSet<TaskIndex> {
        &self.parent_task_indices
    }

    pub fn get_child_task_indices(&self) -> &HashSet<TaskIndex> {
        &self.child_task_indices
    }

    pub fn get_input_dep_indices(&self) -> &Vec<DataflowDependencyIndex> {
        &self.input_dep_indices
    }

    pub fn get_output_dep_indices(&self) -> &Vec<DataflowDependencyIndex> {
        &self.output_dep_indices
    }

    pub fn get_tdl_package(&self) -> &str {
        self.tdl_package.as_str()
    }

    pub fn get_tdl_function(&self) -> &str {
        self.tdl_function.as_str()
    }

    fn create(
        idx: TaskIndex,
        tdl_package: String,
        tdl_function: String,
        input_dep_indices: Vec<DataflowDependencyIndex>,
        output_dep_indices: Vec<DataflowDependencyIndex>,
    ) -> Self {
        Self {
            idx,
            tdl_package,
            tdl_function,
            parent_task_indices: HashSet::new(),
            child_task_indices: HashSet::new(),
            input_dep_indices,
            output_dep_indices,
        }
    }

    fn add_child_task(&mut self, idx: TaskIndex) {
        self.child_task_indices.insert(idx);
    }
}

/// Represents a data-flow dependency connecting tasks in a task graph.
///
/// # Semantics
///
/// * The data type is marked using [`DataTypeDescriptor`] to ensure type safety across the
///   dependency.
/// * An optional source indicating where the data originates:
///   * `None` if the data is an initial input to the graph.
///   * A specific task output in the graph otherwise.
/// * A list of destinations indicating where the data is consumed in the graph as a task input.
pub struct DataflowDependency {
    type_descriptor: DataTypeDescriptor,
    src: Option<TaskInputOutputIndex>,
    dst: Vec<TaskInputOutputIndex>,
}

impl DataflowDependency {
    pub fn get_type_descriptor(&self) -> &DataTypeDescriptor {
        &self.type_descriptor
    }

    pub fn get_src(&self) -> &Option<TaskInputOutputIndex> {
        &self.src
    }

    pub fn get_dst(&self) -> &Vec<TaskInputOutputIndex> {
        &self.dst
    }

    fn new(type_descriptor: DataTypeDescriptor, src: Option<TaskInputOutputIndex>) -> Self {
        Self {
            type_descriptor,
            src,
            dst: Vec::new(),
        }
    }

    fn add_dst(&mut self, dst: TaskInputOutputIndex) {
        self.dst.push(dst);
    }
}

/// An in-memory representation of a directed acyclic graph (DAG) of tasks and their dependencies.
pub struct TaskGraph {
    dataflow_deps: Vec<DataflowDependency>,
    tasks: Vec<Task>,
}

impl Default for TaskGraph {
    fn default() -> Self {
        Self {
            dataflow_deps: Vec::new(),
            tasks: Vec::new(),
        }
    }
}

impl TaskGraph {
    pub fn insert_task(
        &mut self,
        tdl_package: String,
        tdl_function: String,
        positional_inputs: Vec<DataTypeDescriptor>,
        positional_outputs: Vec<DataTypeDescriptor>,
        input_sources: Option<Vec<TaskInputOutputIndex>>,
    ) -> Result<TaskIndex, Error> {
        self.validate_inputs(&positional_inputs, &input_sources)?;
        let task_idx = self.tasks.len();
        match input_sources {
            Some(input_sources) => {
                let mut position = 0;
                for src_idx in input_sources {
                    let dst_idx = TaskInputOutputIndex {
                        task_idx,
                        position,
                    };
                    self.get_mut_task_output(src_idx.clone()).expect("the input source must be valid").add_dst(dst_idx);
                    position += 1;
                }
            }
            None => {}
        }
        Ok(task_idx)
    }

    /// Retrieves a reference to task from the given task index.
    ///
    /// # Returns
    ///
    /// * [`Some(&Task)`] if the task index is valid.
    /// * [`None`] if the task index is out of bounds.
    pub fn get_task(&self, index: TaskIndex) -> Option<&Task> {
        self.tasks.get(index)
    }

    /// Retrieves a reference to the specified task input as a data-flow dependency.
    ///
    /// # Returns
    ///
    /// * [`Some(&DataflowDependency)`] if the task and input position are valid.
    /// * [`None`] if the task index or input position is out of bounds.
    pub fn get_task_input(&self, index: TaskInputOutputIndex) -> Option<&DataflowDependency> {
        let input_dep_idx = self.get_task(index.task_idx)?.input_dep_indices.get(index.position)?;
        self.dataflow_deps.get(*input_dep_idx)
    }

    /// Retrieves a reference to the specified task output as a data-flow dependency.
    ///
    /// # Returns
    ///
    /// * [`Some(&DataflowDependency)`] if the task and output position are valid.
    /// * [`None`] if the task index or output position is out of bounds.
    pub fn get_task_output(&self, index: TaskInputOutputIndex) -> Option<&DataflowDependency> {
        let output_dep_idx = self.get_task(index.task_idx)?.output_dep_indices.get(index.position)?;
        self.dataflow_deps.get(*output_dep_idx)
    }

    fn get_mut_task_output(&mut self, index: TaskInputOutputIndex) -> Option<&mut DataflowDependency> {
        let output_dep_idx = self.tasks.get_mut(index.task_idx)?.output_dep_indices.get(index.position)?;
        self.dataflow_deps.get_mut(*output_dep_idx)
    }

    fn validate_inputs(
        &self,
        positional_inputs: &Vec<DataTypeDescriptor>,
        input_sources: &Option<Vec<TaskInputOutputIndex>>,
    ) -> Result<(), Error> {
        if let Some(input_sources) = input_sources {
            let num_positional_inputs = positional_inputs.len();
            let num_input_sources = input_sources.len();
            if num_positional_inputs != num_input_sources {
                return Err(Error::InvalidTaskInputs(
                    format!(
                        "Mismatched number of positional inputs ({}) and input sources ({}).",
                        num_positional_inputs,
                        num_input_sources)
                ));
            }
            for input_position in 0..num_positional_inputs {
                let source_idx = &input_sources[input_position];
                let dataflow_dep = self.get_task_output(source_idx.clone()).ok_or(
                    Error::InvalidTaskInputs(
                        format!(
                            "invalid input source at position {} with task output index: {:?}",
                            input_position,
                            source_idx,
                        )
                    )
                )?;
                let expected_type = &positional_inputs[input_position];
                let actual_type = dataflow_dep.get_type_descriptor();
                if expected_type == actual_type {
                    continue;
                }
                return Err(Error::InvalidTaskInputs(
                    format!(
                        "Mismatched input type for input at position {}: expected {:?}, found {:?}",
                        input_position,
                        expected_type,
                        actual_type,
                    )
                ));
            }
        }
        Ok(())
    }
}