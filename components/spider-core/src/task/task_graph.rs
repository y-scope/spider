use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};

use crate::task::{DataTypeDescriptor, Error};

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
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    idx: TaskIndex,
    tdl_package: String,
    tdl_function: String,
    parent_indices: Vec<TaskIndex>,
    child_indices: Vec<TaskIndex>,
    input_dep_indices: Vec<DataflowDependencyIndex>,
    output_dep_indices: Vec<DataflowDependencyIndex>,
}

impl Task {
    #[must_use]
    pub const fn get_index(&self) -> TaskIndex {
        self.idx
    }

    #[must_use]
    pub const fn get_num_parents(&self) -> usize {
        self.parent_indices.len()
    }

    #[must_use]
    pub const fn get_num_children(&self) -> usize {
        self.child_indices.len()
    }

    /// # Returns
    ///
    /// Whether this task has no parent tasks. A task with no parent tasks is considered an input
    /// task.
    #[must_use]
    pub const fn is_input_task(&self) -> bool {
        self.parent_indices.is_empty()
    }

    /// # Returns
    ///
    /// Whether this task has no child tasks. A task with no child tasks is considered an output
    /// task.
    #[must_use]
    pub const fn is_output_task(&self) -> bool {
        self.child_indices.is_empty()
    }

    #[must_use]
    pub const fn get_parent_indices(&self) -> &Vec<TaskIndex> {
        &self.parent_indices
    }

    #[must_use]
    pub const fn get_child_indices(&self) -> &Vec<TaskIndex> {
        &self.child_indices
    }

    #[must_use]
    pub const fn get_input_dep_indices(&self) -> &Vec<DataflowDependencyIndex> {
        &self.input_dep_indices
    }

    #[must_use]
    pub const fn get_output_dep_indices(&self) -> &Vec<DataflowDependencyIndex> {
        &self.output_dep_indices
    }

    #[must_use]
    pub const fn get_tdl_package(&self) -> &str {
        self.tdl_package.as_str()
    }

    #[must_use]
    pub const fn get_tdl_function(&self) -> &str {
        self.tdl_function.as_str()
    }

    const fn new(
        idx: TaskIndex,
        tdl_package: String,
        tdl_function: String,
        input_dep_indices: Vec<DataflowDependencyIndex>,
        output_dep_indices: Vec<DataflowDependencyIndex>,
        parent_indices: Vec<TaskIndex>,
    ) -> Self {
        Self {
            idx,
            tdl_package,
            tdl_function,
            parent_indices,
            child_indices: Vec::new(),
            input_dep_indices,
            output_dep_indices,
        }
    }

    /// Adds a task index as a child.
    ///
    /// # NOTE
    ///
    /// There is no validation on the uniqueness of the child index. The caller must ensure that the
    /// index is unique to avoid duplicate child relationships.
    fn add_child(&mut self, idx: TaskIndex) {
        self.child_indices.push(idx);
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataflowDependency {
    index: DataflowDependencyIndex,
    type_descriptor: DataTypeDescriptor,
    src: Option<TaskInputOutputIndex>,
    dst: Vec<TaskInputOutputIndex>,
}

impl DataflowDependency {
    #[must_use]
    pub const fn get_index(&self) -> DataflowDependencyIndex {
        self.index
    }

    #[must_use]
    pub const fn get_type_descriptor(&self) -> &DataTypeDescriptor {
        &self.type_descriptor
    }

    #[must_use]
    pub const fn get_src(&self) -> Option<TaskInputOutputIndex> {
        self.src
    }

    #[must_use]
    pub const fn get_dst(&self) -> &Vec<TaskInputOutputIndex> {
        &self.dst
    }

    const fn new(
        index: DataflowDependencyIndex,
        type_descriptor: DataTypeDescriptor,
        src: Option<TaskInputOutputIndex>,
    ) -> Self {
        Self {
            index,
            type_descriptor,
            src,
            dst: Vec::new(),
        }
    }

    fn add_dst(&mut self, dst: TaskInputOutputIndex) {
        self.dst.push(dst);
    }
}

/// A self-contained descriptor of a task that captures all information needed for task creation.
///
/// This structure serves two primary purposes:
/// * **Task Creation**: Provides all parameters required by [`TaskGraph::insert_task`] to add a new
///   task to a graph.
/// * **Serialization**: Enables task graph serialization by capturing insertion-time information
///   that can be replayed during deserialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskDescriptor {
    /// The TDL package containing the task function to execute.
    pub tdl_package: String,

    /// The TDL function name to execute within the package.
    pub tdl_function: String,

    /// The data types of the task's positional inputs, in order.
    pub inputs: Vec<DataTypeDescriptor>,

    /// The data types of the task's positional outputs, in order.
    pub outputs: Vec<DataTypeDescriptor>,

    /// The source of each positional input.
    ///
    /// * `Some(sources)`: Each input comes from a specific task output in the graph. The vector
    ///   length must match the length of `inputs`.
    /// * `None`: All inputs are graph inputs (i.e., external inputs with no source tasks). This
    ///   indicates the task is an input task to the graph.
    pub input_sources: Option<Vec<TaskInputOutputIndex>>,
}

/// An in-memory representation of a directed acyclic graph (DAG) of tasks and their dependencies.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct TaskGraph {
    dataflow_deps: Vec<DataflowDependency>,
    tasks: Vec<Task>,
}

impl TaskGraph {
    /// Loads a task graph from a serialized task graph in JSON format.
    ///
    /// # Returns
    ///
    /// The deserialized task graph on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`serde_json::from_str`]'s return values on failure.
    pub fn from_json(json_str: &str) -> Result<Self, Error> {
        serde_json::from_str(json_str).map_err(Into::into)
    }

    /// Serializes the task graph into JSON format.
    ///
    /// # Returns
    ///
    /// The serialized task graph as a JSON string.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`serde_json::to_string`]'s return values on failure.
    pub fn to_json(&self) -> Result<String, Error> {
        serde_json::to_string(&self).map_err(Into::into)
    }

    /// Inserts a new task into the graph with the given details.
    ///
    /// # Returns
    ///
    /// The index of the newly inserted task.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::compute_and_update_dependencies_from_inputs`]'s return values on failure.
    pub fn insert_task(&mut self, task_descriptor: TaskDescriptor) -> Result<TaskIndex, Error> {
        let task_idx = self.get_next_task_index();
        let (input_dep_indices, parent_indices) = self
            .compute_and_update_dependencies_from_inputs(
                task_idx,
                &task_descriptor.inputs,
                task_descriptor.input_sources,
            )?;

        let mut output_dep_indices: Vec<DataflowDependencyIndex> = Vec::new();
        for (position, output_type) in task_descriptor.outputs.into_iter().enumerate() {
            let output_dep_idx = self.get_next_dataflow_dep_index();
            let output_dep = DataflowDependency::new(
                output_dep_idx,
                output_type,
                Some(TaskInputOutputIndex { task_idx, position }),
            );
            self.dataflow_deps.push(output_dep);
            output_dep_indices.push(output_dep_idx);
        }

        self.tasks.push(Task::new(
            task_idx,
            task_descriptor.tdl_package,
            task_descriptor.tdl_function,
            input_dep_indices,
            output_dep_indices,
            parent_indices,
        ));
        Ok(task_idx)
    }

    /// Retrieves a reference to task from the given task index.
    ///
    /// # Returns
    ///
    /// * [`Some(&Task)`] if the task index is valid.
    /// * [`None`] if the task index is out of bounds.
    #[must_use]
    pub fn get_task(&self, index: TaskIndex) -> Option<&Task> {
        self.tasks.get(index)
    }

    /// Retrieves a reference to the specified task input as a data-flow dependency.
    ///
    /// # Returns
    ///
    /// * [`Some(&DataflowDependency)`] if the task and input position are valid.
    /// * [`None`] if the task index or input position is out of bounds.
    #[must_use]
    pub fn get_task_input(&self, index: TaskInputOutputIndex) -> Option<&DataflowDependency> {
        let input_dep_idx = self
            .get_task(index.task_idx)?
            .input_dep_indices
            .get(index.position)?;
        self.dataflow_deps.get(*input_dep_idx)
    }

    /// Retrieves a reference to the specified task output as a data-flow dependency.
    ///
    /// # Returns
    ///
    /// * [`Some(&DataflowDependency)`] if the task and output position are valid.
    /// * [`None`] if the task index or output position is out of bounds.
    #[must_use]
    pub fn get_task_output(&self, index: TaskInputOutputIndex) -> Option<&DataflowDependency> {
        let output_dep_idx = self
            .get_task(index.task_idx)?
            .output_dep_indices
            .get(index.position)?;
        self.dataflow_deps.get(*output_dep_idx)
    }

    /// Computes the input data-flow dependencies and parent task indices for a task based on its
    /// inputs.
    ///
    /// On success, this method also updates the task graph by registering the given task as a child
    /// of the computed parent tasks. This mutation occurs **after** all inputs have been validated.
    /// If input validation fails, the task graph remains unchanged.
    ///
    /// # Returns
    ///
    /// A pair consisting of:
    ///
    /// * A vector of input data-flow dependency indices.
    /// * A vector of parent task indices.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::validate_and_retrieve_input_dep_indices`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// This method panics to signal internal consistency violations (indicative of a bug in the
    /// task graph implementation) if:
    ///
    /// * The results returned by [`Self::validate_and_retrieve_input_dep_indices`] reference
    ///   non-existent data-flow dependencies.
    /// * The referenced data-flow dependencies have no source task.
    /// * Any computed parent task indices reference non-existent tasks.
    fn compute_and_update_dependencies_from_inputs(
        &mut self,
        task_idx: TaskIndex,
        inputs: &[DataTypeDescriptor],
        input_sources: Option<Vec<TaskInputOutputIndex>>,
    ) -> Result<(Vec<DataflowDependencyIndex>, Vec<TaskIndex>), Error> {
        if let Some(input_sources) = input_sources {
            let input_dep_indices =
                self.validate_and_retrieve_input_dep_indices(inputs, &input_sources)?;
            let mut parent_indices = Vec::new();
            for (position, input_dep_idx) in input_dep_indices.iter().enumerate() {
                let input_dep = self
                    .dataflow_deps
                    .get_mut(*input_dep_idx)
                    .expect("indices should be validated in the previous step");
                parent_indices.push(
                    input_dep
                        .get_src()
                        .expect("source should always exist")
                        .task_idx,
                );
                input_dep.add_dst(TaskInputOutputIndex { task_idx, position });
            }
            parent_indices.sort_unstable();
            parent_indices.dedup();
            for parent_idx in &parent_indices {
                self.tasks
                    .get_mut(*parent_idx)
                    .expect("parent indices should be validated in the previous step")
                    .add_child(task_idx);
            }
            Ok((input_dep_indices, parent_indices))
        } else {
            let mut input_dep_indices = Vec::new();
            for (position, input_type) in inputs.iter().enumerate() {
                let input_dep_idx = self.get_next_dataflow_dep_index();
                let mut input_dep =
                    DataflowDependency::new(input_dep_idx, input_type.clone(), None);
                input_dep.add_dst(TaskInputOutputIndex { task_idx, position });
                self.dataflow_deps.push(input_dep);
                input_dep_indices.push(input_dep_idx);
            }
            Ok((input_dep_indices, Vec::new()))
        }
    }

    /// Validates the given inputs and retrieves their corresponding data-flow dependency indices.
    ///
    /// # Returns
    ///
    /// A vector of data-flow dependency indices corresponding to the validated input sources, in
    /// the same order as those input sources.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::InvalidTaskInputs`] if:
    ///   * The number of inputs doesn't match the number of input sources.
    ///   * The given input doesn't correspond to a valid task output in the graph.
    ///   * The type of input doesn't match the type of its corresponding input source.
    fn validate_and_retrieve_input_dep_indices(
        &self,
        input: &[DataTypeDescriptor],
        input_sources: &[TaskInputOutputIndex],
    ) -> Result<Vec<DataflowDependencyIndex>, Error> {
        let mut input_dep_indices = Vec::new();
        let num_inputs = input.len();
        if num_inputs == 0 {
            return Err(Error::InvalidTaskInputs(
                "input task cannot have input sources specified".to_owned(),
            ));
        }
        let num_input_sources = input_sources.len();
        if num_inputs != num_input_sources {
            return Err(Error::InvalidTaskInputs(format!(
                "mismatched number of positional inputs ({num_inputs}) and input sources \
                 ({num_input_sources})."
            )));
        }
        for (input_position, src_idx) in input_sources.iter().enumerate() {
            let dataflow_dep = self.get_task_output(*src_idx).ok_or_else(|| {
                Error::InvalidTaskInputs(format!(
                    "invalid input source at position {input_position} with task output index: \
                     {src_idx:?}",
                ))
            })?;
            let expected_type = &input[input_position];
            let actual_type = dataflow_dep.get_type_descriptor();
            if expected_type == actual_type {
                input_dep_indices.push(dataflow_dep.get_index());
                continue;
            }
            return Err(Error::InvalidTaskInputs(format!(
                "mismatched input type for input at position {input_position}: expected \
                 {expected_type:?}, found {actual_type:?}",
            )));
        }
        Ok(input_dep_indices)
    }

    const fn get_next_dataflow_dep_index(&self) -> DataflowDependencyIndex {
        self.dataflow_deps.len()
    }

    const fn get_next_task_index(&self) -> TaskIndex {
        self.tasks.len()
    }

    /// Converts the task graph to a serialized format.
    ///
    /// # Returns
    ///
    /// A [`SerializedTaskGraph`] representation of the task graph.
    ///
    /// # Panics
    ///
    /// This method panics to signal internal consistency violations (indicative of a bug in the
    /// task graph implementation).
    fn to_serialized_task_graph(&self) -> SerializedTaskGraph {
        let mut serialized_tasks = Vec::new();
        for task in &self.tasks {
            let inputs: Vec<_> = task
                .get_input_dep_indices()
                .iter()
                .map(|input_dep_idx| {
                    self.dataflow_deps
                        .get(*input_dep_idx)
                        .expect("input dep idx should reference to a valid data-flow dep")
                        .get_type_descriptor()
                        .clone()
                })
                .collect();
            let outputs: Vec<_> = task
                .get_output_dep_indices()
                .iter()
                .map(|output_dep_idx| {
                    self.dataflow_deps
                        .get(*output_dep_idx)
                        .expect("output dep idx should reference to a valid data-flow dep")
                        .get_type_descriptor()
                        .clone()
                })
                .collect();
            let input_sources: Option<Vec<_>> = if task.is_input_task() {
                None
            } else {
                Some(
                    task.get_input_dep_indices()
                        .iter()
                        .map(|input_dep_idx| {
                            self.dataflow_deps
                                .get(*input_dep_idx)
                                .expect("input dep idx should reference to a valid data-flow dep")
                                .src
                                .expect("src must exist for non-input tasks")
                        })
                        .collect(),
                )
            };
            serialized_tasks.push(TaskDescriptor {
                tdl_package: task.tdl_package.clone(),
                tdl_function: task.tdl_function.clone(),
                inputs,
                outputs,
                input_sources,
            });
        }
        SerializedTaskGraph {
            schema_version: TASK_GRAPH_SCHEMA_VERSION.to_owned(),
            tasks: serialized_tasks,
        }
    }
}

impl Serialize for TaskGraph {
    fn serialize<SerializerImpl>(
        &self,
        serializer: SerializerImpl,
    ) -> Result<SerializerImpl::Ok, SerializerImpl::Error>
    where
        SerializerImpl: serde::Serializer, {
        self.to_serialized_task_graph().serialize(serializer)
    }
}

impl<'deserializer_lifetime> Deserialize<'deserializer_lifetime> for TaskGraph {
    fn deserialize<DeserializerImpl>(
        deserializer: DeserializerImpl,
    ) -> Result<Self, DeserializerImpl::Error>
    where
        DeserializerImpl: serde::Deserializer<'deserializer_lifetime>, {
        let serializable = SerializedTaskGraph::deserialize(deserializer)?;
        let schema_version = Version::parse(&serializable.schema_version).map_err(|error| {
            serde::de::Error::custom(format!(
                "invalid schema version string '{}': {}",
                serializable.schema_version, error
            ))
        })?;

        if !TASK_GRAPH_SCHEMA_COMPATIBLE_VERSION_REQUIREMENT.matches(&schema_version) {
            return Err(serde::de::Error::custom(format!(
                "incompatible task graph schema version: found {}, compatible requirements: {}",
                serializable.schema_version, TASK_GRAPH_SCHEMA_COMPATIBLE_VERSION
            )));
        }

        let mut graph = Self::default();
        for (idx, task_descriptor) in serializable.tasks.into_iter().enumerate() {
            let inserted_idx = graph.insert_task(task_descriptor).map_err(|error| {
                serde::de::Error::custom(format!(
                    "failed to insert task (index={idx}) during deserialization: {error:?}"
                ))
            })?;

            if inserted_idx != idx {
                return Err(serde::de::Error::custom(format!(
                    "task insertion order corrupted: expected index {idx}, got {inserted_idx}"
                )));
            }
        }

        Ok(graph)
    }
}

/// Task graph schema version of the current build.
const TASK_GRAPH_SCHEMA_VERSION: &str = "0.1.0";

/// Task graph schema version compatibility requirement.
const TASK_GRAPH_SCHEMA_COMPATIBLE_VERSION: &str = ">=0.1.0,<0.2.0";

static TASK_GRAPH_SCHEMA_COMPATIBLE_VERSION_REQUIREMENT: std::sync::LazyLock<VersionReq> =
    std::sync::LazyLock::new(|| {
        VersionReq::parse(TASK_GRAPH_SCHEMA_COMPATIBLE_VERSION)
            .expect("`TASK_GRAPH_SCHEMA_COMPATIBLE_VERSION` must be a valid semver requirement")
    });

/// A serialized representation of a task graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SerializedTaskGraph {
    schema_version: String,
    tasks: Vec<TaskDescriptor>,
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use super::*;
    use crate::task::{
        DataTypeDescriptor,
        IntTypeDescriptor,
        MapKeyTypeDescriptor,
        ValueTypeDescriptor,
    };

    const TEST_PACKAGE: &str = "test_pkg";

    /// Creates a complex task graph with the following structure:
    ///
    /// ```
    /// Graph inputs (dependencies with no source tasks):
    ///  * task_0.input_0: Int32 (graph input)
    ///  * task_0.input_1: Float64 (graph input)
    ///  * task_1.input_0: Bytes (graph input)
    ///
    /// Tasks and dependencies:
    ///
    ///   task_0: "test_pkg::fn_1" (input task)
    ///     Inputs:  [task_0.input_0: Int32, task_0.input_1: Float64]
    ///     Outputs: [task_0.output_0: Int64, task_0.output_1: Boolean]
    ///     Parents: []
    ///
    ///   task_1: "test_pkg::fn_2" (input task)
    ///     Inputs:  [task_1.input_0: Bytes]
    ///     Outputs: [task_1.output_0: List<Int32>, task_1.output_1: Bytes]
    ///     Parents: []
    ///
    ///   task_2: "test_pkg::fn_3" (intermediate task, fan-out source)
    ///     Inputs:  [task_2.input_0: Int64 (from task_0.output_0)]
    ///     Outputs: [task_2.output_0: Map<Int32, Float64>, task_2.output_1: Struct("Result")]
    ///     Parents: [task_0]
    ///
    ///   task_3: "test_pkg::fn_4" (intermediate task, fan-in destination)
    ///     Inputs:  [task_3.input_0: Map<Int32, Float64> (from task_2.output_0),
    ///               task_3.input_1: Boolean (from task_0.output_1)]
    ///     Outputs: [task_3.output_0: Int32]
    ///     Parents: [task_0, task_2]
    ///
    ///   task_4: "test_pkg::fn_5" (intermediate task, fan-in destination)
    ///     Inputs:  [task_4.input_0: Map<Int32, Float64> (from task_2.output_0),
    ///               task_4.input_1: List<Int32> (from task_1.output_0)]
    ///     Outputs: [task_4.output_0: Float32, task_4.output_1: Bytes]
    ///     Parents: [task_1, task_2]
    ///
    ///   task_5: "test_pkg::fn_6" (intermediate task with complex fan-out source)
    ///     Inputs:  [task_5.input_0: Int32 (from task_3.output_0)]
    ///     Outputs: [task_5.output_0: Boolean, task_5.output_1: List<Bytes>]
    ///     Parents: [task_3]
    ///
    ///   task_6: "test_pkg::fn_7" (output task with fan-in, uses the same output more than once)
    ///     Inputs:  [task_6.input_0: List<Bytes> (from task_5.output_1),
    ///               task_6.input_1: List<Bytes> (from task_5.output_1),
    ///               task_6.input_2: Boolean (from task_5.output_0),
    ///               task_6.input_3: Bytes (from task_4.output_1)]
    ///     Outputs: [task_6.output_0: Int64]
    ///     Parents: [task_4, task_5]
    ///
    ///   task_7: "test_pkg::fn_8" (output task, also uses task_5.output_1)
    ///     Inputs:  [task_7.input_0: List<Bytes> (from task_5.output_1)]
    ///     Outputs: [task_7.output_0: Float64]
    ///     Parents: [task_5]
    ///
    ///   task_8: "test_pkg::fn_9" (output task, swapped input order)
    ///     Inputs:  [task_8.input_0: Bytes (from task_1.output_1),
    ///               task_8.input_1: List<Int32> (from task_1.output_0)]
    ///     Outputs: [task_8.output_0: Int32]
    ///     Parents: [task_1]
    ///
    ///   task_9: "test_pkg::fn_10" (both an input task and an output task without inputs/outputs)
    ///     Inputs:  []
    ///     Outputs: []
    ///     Parents: []
    ///
    /// Dependency indices (in insertion order: inputs then outputs for each task):
    ///   task_0: inputs [0 (dst), 1 (dst)],
    ///           outputs [2 (src), 3 (src)]
    ///   task_1: inputs [4 (dst)],
    ///           outputs [5 (src), 6 (src)]
    ///   task_2: inputs [2 (dst)],
    ///           outputs [7 (src), 8 (src)]
    ///   task_3: inputs [7 (dst), 3 (dst)],
    ///           outputs [9 (src)]
    ///   task_4: inputs [7 (dst), 5 (dst)],
    ///           outputs [10 (src), 11 (src)]
    ///   task_5: inputs [9 (dst)],
    ///           outputs [12 (src), 13 (src)]
    ///   task_6: inputs [13 (dst), 13 (dst), 12 (dst), 11 (dst)],
    ///           outputs [14 (src)]
    ///   task_7: inputs [13 (dst)],
    ///           outputs [15 (src)]
    ///   task_8: inputs [6 (dst), 5 (dst)],
    ///           outputs [16 (src)]
    ///   task_9: inputs []
    ///           outputs []
    ///
    /// Dangling outputs (not consumed by any task):
    ///   * task_2.output_1 (index 8): Struct("Result")
    ///   * task_4.output_0 (index 10): Float32
    ///   * task_6.output_0 (index 14): Int64
    ///   * task_7.output_0 (index 15): Float64
    ///   * task_8.output_0 (index 16): Int32
    ///
    /// Control-flow dependencies:
    ///   * task_0 -> children: [task_2, task_3]
    ///   * task_1 -> children: [task_4, task_8]
    ///   * task_2 -> children: [task_3, task_4]
    ///   * task_3 -> children: [task_5]
    ///   * task_4 -> children: [task_6]
    ///   * task_5 -> children: [task_6, task_7]
    /// ```
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_task_graph_construction() {
        let mut graph = TaskGraph::default();

        // Type descriptors
        let int32_type = DataTypeDescriptor::Value(ValueTypeDescriptor::int32());
        let int64_type = DataTypeDescriptor::Value(ValueTypeDescriptor::int64());
        let float32_type = DataTypeDescriptor::Value(ValueTypeDescriptor::float32());
        let float64_type = DataTypeDescriptor::Value(ValueTypeDescriptor::float64());
        let bool_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bool());
        let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
        let list_int32_type =
            DataTypeDescriptor::Value(ValueTypeDescriptor::list(ValueTypeDescriptor::int32()));
        let list_bytes_type =
            DataTypeDescriptor::Value(ValueTypeDescriptor::list(ValueTypeDescriptor::bytes()));
        let map_type = DataTypeDescriptor::Value(ValueTypeDescriptor::map(
            MapKeyTypeDescriptor::Int(IntTypeDescriptor::Int32),
            ValueTypeDescriptor::float64(),
        ));
        let struct_type = DataTypeDescriptor::Value(
            ValueTypeDescriptor::struct_from_name("Result").expect("struct name should be valid"),
        );

        let task_0_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_1".to_string(),
                inputs: vec![int32_type.clone(), float64_type.clone()],
                outputs: vec![int64_type.clone(), bool_type.clone()],
                input_sources: None,
            })
            .expect("task_0 insertion should succeed");

        assert_eq!(task_0_idx, 0, "task_0 should have index 0");

        let task_1_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_2".to_string(),
                inputs: vec![bytes_type.clone()],
                outputs: vec![list_int32_type.clone(), bytes_type.clone()],
                input_sources: None,
            })
            .expect("task_1 insertion should succeed");

        assert_eq!(task_1_idx, 1, "task_1 should have index 1");

        let task_2_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_3".to_string(),
                inputs: vec![int64_type.clone()],
                outputs: vec![map_type.clone(), struct_type.clone()],
                input_sources: Some(vec![TaskInputOutputIndex {
                    task_idx: 0,
                    position: 0,
                }]),
            })
            .expect("task_2 insertion should succeed");

        assert_eq!(task_2_idx, 2, "task_2 should have index 2");

        let task_3_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_4".to_string(),
                inputs: vec![map_type.clone(), bool_type.clone()],
                outputs: vec![int32_type.clone()],
                input_sources: Some(vec![
                    TaskInputOutputIndex {
                        task_idx: 2,
                        position: 0,
                    },
                    TaskInputOutputIndex {
                        task_idx: 0,
                        position: 1,
                    },
                ]),
            })
            .expect("task_3 insertion should succeed");

        assert_eq!(task_3_idx, 3, "task_3 should have index 3");

        let task_4_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_5".to_string(),
                inputs: vec![map_type.clone(), list_int32_type.clone()],
                outputs: vec![float32_type.clone(), bytes_type.clone()],
                input_sources: Some(vec![
                    TaskInputOutputIndex {
                        task_idx: 2,
                        position: 0,
                    },
                    TaskInputOutputIndex {
                        task_idx: 1,
                        position: 0,
                    },
                ]),
            })
            .expect("task_4 insertion should succeed");

        assert_eq!(task_4_idx, 4, "task_4 should have index 4");

        let task_5_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_6".to_string(),
                inputs: vec![int32_type.clone()],
                outputs: vec![bool_type.clone(), list_bytes_type.clone()],
                input_sources: Some(vec![TaskInputOutputIndex {
                    task_idx: 3,
                    position: 0,
                }]),
            })
            .expect("task_5 insertion should succeed");

        assert_eq!(task_5_idx, 5, "task_5 should have index 5");

        let task_6_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_7".to_string(),
                inputs: vec![
                    list_bytes_type.clone(),
                    list_bytes_type.clone(),
                    bool_type.clone(),
                    bytes_type.clone(),
                ],
                outputs: vec![int64_type.clone()],
                input_sources: Some(vec![
                    TaskInputOutputIndex {
                        task_idx: 5,
                        position: 1,
                    },
                    TaskInputOutputIndex {
                        task_idx: 5,
                        position: 1,
                    },
                    TaskInputOutputIndex {
                        task_idx: 5,
                        position: 0,
                    },
                    TaskInputOutputIndex {
                        task_idx: 4,
                        position: 1,
                    },
                ]),
            })
            .expect("task_6 insertion should succeed");

        assert_eq!(task_6_idx, 6, "task_6 should have index 6");

        let task_7_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_8".to_string(),
                inputs: vec![list_bytes_type.clone()],
                outputs: vec![float64_type.clone()],
                input_sources: Some(vec![TaskInputOutputIndex {
                    task_idx: 5,
                    position: 1,
                }]),
            })
            .expect("task_7 insertion should succeed");

        assert_eq!(task_7_idx, 7, "task_7 should have index 7");

        let task_8_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_9".to_string(),
                inputs: vec![bytes_type.clone(), list_int32_type.clone()],
                outputs: vec![int32_type.clone()],
                input_sources: Some(vec![
                    TaskInputOutputIndex {
                        task_idx: 1,
                        position: 1,
                    },
                    TaskInputOutputIndex {
                        task_idx: 1,
                        position: 0,
                    },
                ]),
            })
            .expect("task_8 insertion should succeed");

        assert_eq!(task_8_idx, 8, "task_8 should have index 8");

        let task_9_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_10".to_string(),
                inputs: vec![],
                outputs: vec![],
                input_sources: None,
            })
            .expect("task_9 insertion should succeed");

        assert_eq!(task_9_idx, 9, "task_9 should have index 9");

        let json_serialized = serde_json::to_string_pretty(&graph).expect("shouldn't fail");
        println!("{json_serialized}");

        // Validate task_0
        let task_0 = graph.get_task(0).expect("task_0 should exist");
        assert_eq!(task_0.get_index(), 0);
        assert_eq!(task_0.get_tdl_package(), TEST_PACKAGE);
        assert_eq!(task_0.get_tdl_function(), "fn_1");
        assert_eq!(task_0.get_num_parents(), 0);
        assert_eq!(task_0.get_num_children(), 2);
        assert!(task_0.is_input_task(), "task_0 should be an input task");
        assert!(
            !task_0.is_output_task(),
            "task_0 should not be an output task"
        );
        assert_eq!(task_0.get_child_indices(), &vec![2, 3]);
        assert_eq!(task_0.get_input_dep_indices(), &vec![0, 1]);
        assert_eq!(task_0.get_output_dep_indices(), &vec![2, 3]);

        // Validate task_1
        let task_1 = graph.get_task(1).expect("task_1 should exist");
        assert_eq!(task_1.get_index(), 1);
        assert_eq!(task_1.get_tdl_package(), TEST_PACKAGE);
        assert_eq!(task_1.get_tdl_function(), "fn_2");
        assert_eq!(task_1.get_num_parents(), 0);
        assert_eq!(task_1.get_num_children(), 2);
        assert!(task_1.is_input_task(), "task_1 should be an input task");
        assert!(
            !task_1.is_output_task(),
            "task_1 should not be an output task"
        );
        assert_eq!(task_1.get_child_indices(), &vec![4, 8]);
        assert_eq!(task_1.get_input_dep_indices(), &vec![4]);
        assert_eq!(task_1.get_output_dep_indices(), &vec![5, 6]);

        // Validate task_2
        let task_2 = graph.get_task(2).expect("task_2 should exist");
        assert_eq!(task_2.get_index(), 2);
        assert_eq!(task_2.get_tdl_package(), TEST_PACKAGE);
        assert_eq!(task_2.get_tdl_function(), "fn_3");
        assert_eq!(task_2.get_num_parents(), 1);
        assert_eq!(task_2.get_parent_indices(), &vec![0]);
        assert_eq!(task_2.get_num_children(), 2);
        assert_eq!(task_2.get_child_indices(), &vec![3, 4]);
        assert!(
            !task_2.is_input_task(),
            "task_2 should not be an input task"
        );
        assert!(
            !task_2.is_output_task(),
            "task_2 should not be an output task"
        );
        assert_eq!(task_2.get_input_dep_indices(), &vec![2]);
        assert_eq!(task_2.get_output_dep_indices(), &vec![7, 8]);

        // Validate task_3
        let task_3 = graph.get_task(3).expect("task_3 should exist");
        assert_eq!(task_3.get_index(), 3);
        assert_eq!(task_3.get_tdl_package(), TEST_PACKAGE);
        assert_eq!(task_3.get_tdl_function(), "fn_4");
        assert_eq!(task_3.get_num_parents(), 2);
        assert_eq!(task_3.get_parent_indices(), &vec![0, 2]);
        assert_eq!(task_3.get_num_children(), 1);
        assert_eq!(task_3.get_child_indices(), &vec![5]);
        assert!(
            !task_3.is_input_task(),
            "task_3 should not be an input task"
        );
        assert!(
            !task_3.is_output_task(),
            "task_3 should not be an output task"
        );
        assert_eq!(task_3.get_input_dep_indices(), &vec![7, 3]);
        assert_eq!(task_3.get_output_dep_indices(), &vec![9]);

        // Validate task_4
        let task_4 = graph.get_task(4).expect("task_4 should exist");
        assert_eq!(task_4.get_index(), 4);
        assert_eq!(task_4.get_tdl_package(), TEST_PACKAGE);
        assert_eq!(task_4.get_tdl_function(), "fn_5");
        assert_eq!(task_4.get_num_parents(), 2);
        assert_eq!(task_4.get_parent_indices(), &vec![1, 2]);
        assert_eq!(task_4.get_num_children(), 1);
        assert_eq!(task_4.get_child_indices(), &vec![6]);
        assert!(
            !task_4.is_input_task(),
            "task_4 should not be an input task"
        );
        assert!(
            !task_4.is_output_task(),
            "task_4 should not be an output task"
        );
        assert_eq!(task_4.get_input_dep_indices(), &vec![7, 5]);
        assert_eq!(task_4.get_output_dep_indices(), &vec![10, 11]);

        // Validate task_5
        let task_5 = graph.get_task(5).expect("task_5 should exist");
        assert_eq!(task_5.get_index(), 5);
        assert_eq!(task_5.get_tdl_package(), TEST_PACKAGE);
        assert_eq!(task_5.get_tdl_function(), "fn_6");
        assert_eq!(task_5.get_num_parents(), 1);
        assert_eq!(task_5.get_parent_indices(), &vec![3]);
        assert_eq!(task_5.get_num_children(), 2);
        assert_eq!(task_5.get_child_indices(), &vec![6, 7]);
        assert!(
            !task_5.is_input_task(),
            "task_5 should not be an input task"
        );
        assert!(
            !task_5.is_output_task(),
            "task_5 should not be an output task"
        );
        assert_eq!(task_5.get_input_dep_indices(), &vec![9]);
        assert_eq!(task_5.get_output_dep_indices(), &vec![12, 13]);

        // Validate task_6
        let task_6 = graph.get_task(6).expect("task_6 should exist");
        assert_eq!(task_6.get_index(), 6);
        assert_eq!(task_6.get_tdl_package(), TEST_PACKAGE);
        assert_eq!(task_6.get_tdl_function(), "fn_7");
        assert_eq!(task_6.get_num_parents(), 2);
        assert_eq!(task_6.get_parent_indices(), &vec![4, 5]);
        assert_eq!(task_6.get_num_children(), 0);
        assert!(
            !task_6.is_input_task(),
            "task_6 should not be an input task"
        );
        assert!(task_6.is_output_task(), "task_6 should be an output task");
        assert_eq!(task_6.get_input_dep_indices(), &vec![13, 13, 12, 11]);
        assert_eq!(task_6.get_output_dep_indices(), &vec![14]);

        // Validate task_7
        let task_7 = graph.get_task(7).expect("task_7 should exist");
        assert_eq!(task_7.get_index(), 7);
        assert_eq!(task_7.get_tdl_package(), TEST_PACKAGE);
        assert_eq!(task_7.get_tdl_function(), "fn_8");
        assert_eq!(task_7.get_num_parents(), 1);
        assert_eq!(task_7.get_parent_indices(), &vec![5]);
        assert_eq!(task_7.get_num_children(), 0);
        assert!(
            !task_7.is_input_task(),
            "task_7 should not be an input task"
        );
        assert!(task_7.is_output_task(), "task_7 should be an output task");
        assert_eq!(task_7.get_input_dep_indices(), &vec![13]);
        assert_eq!(task_7.get_output_dep_indices(), &vec![15]);

        // Validate task_8
        let task_8 = graph.get_task(8).expect("task_8 should exist");
        assert_eq!(task_8.get_index(), 8);
        assert_eq!(task_8.get_tdl_package(), TEST_PACKAGE);
        assert_eq!(task_8.get_tdl_function(), "fn_9");
        assert_eq!(task_8.get_num_parents(), 1);
        assert_eq!(task_8.get_parent_indices(), &vec![1]);
        assert_eq!(task_8.get_num_children(), 0);
        assert!(
            !task_8.is_input_task(),
            "task_8 should not be an input task"
        );
        assert!(task_8.is_output_task(), "task_8 should be an output task");
        assert_eq!(task_8.get_input_dep_indices(), &vec![6, 5]);
        assert_eq!(task_8.get_output_dep_indices(), &vec![16]);

        // Validate task_9
        let task_9 = graph.get_task(9).expect("task_9 should exist");
        assert_eq!(task_9.get_index(), 9);
        assert_eq!(task_9.get_tdl_package(), TEST_PACKAGE);
        assert_eq!(task_9.get_tdl_function(), "fn_10");
        assert_eq!(task_9.get_num_parents(), 0);
        assert_eq!(task_9.get_parent_indices(), &Vec::<TaskIndex>::new());
        assert_eq!(task_9.get_num_children(), 0);
        assert!(task_9.is_input_task(), "task_8 should be an input task");
        assert!(task_9.is_output_task(), "task_9 should be an output task");
        assert_eq!(
            task_9.get_input_dep_indices(),
            &Vec::<DataflowDependencyIndex>::new()
        );
        assert_eq!(
            task_9.get_output_dep_indices(),
            &Vec::<DataflowDependencyIndex>::new()
        );

        // Validate graph inputs (dataflow dependencies with no source)
        let task_0_input_0 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 0,
                position: 0,
            })
            .expect("task_0.input_0 should exist");
        assert_eq!(task_0_input_0.get_index(), 0);
        assert_eq!(task_0_input_0.get_type_descriptor(), &int32_type);
        assert!(
            task_0_input_0.get_src().is_none(),
            "task_0.input_0 should be a graph input"
        );
        assert_eq!(task_0_input_0.get_dst().len(), 1);
        assert_eq!(
            task_0_input_0.get_dst()[0],
            TaskInputOutputIndex {
                task_idx: 0,
                position: 0
            }
        );

        let task_0_input_1 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 0,
                position: 1,
            })
            .expect("task_0.input_1 should exist");
        assert_eq!(task_0_input_1.get_index(), 1);
        assert_eq!(task_0_input_1.get_type_descriptor(), &float64_type);
        assert!(
            task_0_input_1.get_src().is_none(),
            "task_0.input_1 should be a graph input"
        );
        assert_eq!(task_0_input_1.get_dst().len(), 1);
        assert_eq!(
            task_0_input_1.get_dst()[0],
            TaskInputOutputIndex {
                task_idx: 0,
                position: 1
            }
        );

        let task_1_input_0 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 1,
                position: 0,
            })
            .expect("task_1.input_0 should exist");
        assert_eq!(task_1_input_0.get_index(), 4);
        assert_eq!(task_1_input_0.get_type_descriptor(), &bytes_type);
        assert!(
            task_1_input_0.get_src().is_none(),
            "task_1.input_0 should be a graph input"
        );
        assert_eq!(task_1_input_0.get_dst().len(), 1);
        assert_eq!(
            task_1_input_0.get_dst()[0],
            TaskInputOutputIndex {
                task_idx: 1,
                position: 0
            }
        );

        // Validate task_0 outputs
        let task_0_output_0 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 0,
                position: 0,
            })
            .expect("task_0.output_0 should exist");
        assert_eq!(task_0_output_0.get_index(), 2);
        assert_eq!(task_0_output_0.get_type_descriptor(), &int64_type);
        assert_eq!(
            task_0_output_0.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 0,
                position: 0,
            })
        );
        assert_eq!(
            task_0_output_0.get_dst().len(),
            1,
            "task_0.output_0 should connect to task_2"
        );
        assert_eq!(
            task_0_output_0.get_dst()[0],
            TaskInputOutputIndex {
                task_idx: 2,
                position: 0
            }
        );

        let task_0_output_1 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 0,
                position: 1,
            })
            .expect("task_0.output_1 should exist");
        assert_eq!(task_0_output_1.get_index(), 3);
        assert_eq!(task_0_output_1.get_type_descriptor(), &bool_type);
        assert_eq!(
            task_0_output_1.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 0,
                position: 1,
            })
        );
        assert_eq!(
            task_0_output_1.get_dst().len(),
            1,
            "task_0.output_1 should connect to task_3"
        );
        assert_eq!(
            task_0_output_1.get_dst()[0],
            TaskInputOutputIndex {
                task_idx: 3,
                position: 1
            }
        );

        // Validate task_1 outputs (both used by task_8 in swapped order)
        let task_1_output_0 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 1,
                position: 0,
            })
            .expect("task_1.output_0 should exist");
        assert_eq!(task_1_output_0.get_index(), 5);
        assert_eq!(task_1_output_0.get_type_descriptor(), &list_int32_type);
        assert_eq!(
            task_1_output_0.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 1,
                position: 0,
            })
        );
        assert_eq!(
            task_1_output_0.get_dst().len(),
            2,
            "task_1.output_0 should fan out to task_4 and task_8"
        );
        assert!(task_1_output_0.get_dst().contains(&TaskInputOutputIndex {
            task_idx: 4,
            position: 1
        }));
        assert!(task_1_output_0.get_dst().contains(&TaskInputOutputIndex {
            task_idx: 8,
            position: 1
        }));

        let task_1_output_1 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 1,
                position: 1,
            })
            .expect("task_1.output_1 should exist");
        assert_eq!(task_1_output_1.get_index(), 6);
        assert_eq!(task_1_output_1.get_type_descriptor(), &bytes_type);
        assert_eq!(
            task_1_output_1.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 1,
                position: 1,
            })
        );
        assert_eq!(
            task_1_output_1.get_dst().len(),
            1,
            "task_1.output_1 should connect to task_8.input_0"
        );
        assert_eq!(
            task_1_output_1.get_dst()[0],
            TaskInputOutputIndex {
                task_idx: 8,
                position: 0
            }
        );

        // Validate fan-out case: task_2.output_0 -> task_3.input_0 and task_4.input_0
        let task_2_output_0 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 2,
                position: 0,
            })
            .expect("task_2.output_0 should exist");
        assert_eq!(task_2_output_0.get_index(), 7);
        assert_eq!(task_2_output_0.get_type_descriptor(), &map_type);
        assert_eq!(
            task_2_output_0.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 2,
                position: 0,
            })
        );
        assert_eq!(
            task_2_output_0.get_dst().len(),
            2,
            "task_2.output_0 should fan out to 2 destinations"
        );
        assert!(
            task_2_output_0.get_dst().contains(&TaskInputOutputIndex {
                task_idx: 3,
                position: 0,
            }),
            "task_2.output_0 should connect to task_3.input_0"
        );
        assert!(
            task_2_output_0.get_dst().contains(&TaskInputOutputIndex {
                task_idx: 4,
                position: 0,
            }),
            "task_2.output_0 should connect to task_4.input_0"
        );

        // Validate dangling output: task_2.output_1
        let task_2_output_1 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 2,
                position: 1,
            })
            .expect("task_2.output_1 should exist");
        assert_eq!(task_2_output_1.get_index(), 8);
        assert_eq!(task_2_output_1.get_type_descriptor(), &struct_type);
        assert_eq!(
            task_2_output_1.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 2,
                position: 1,
            })
        );
        assert_eq!(
            task_2_output_1.get_dst().len(),
            0,
            "task_2.output_1 should be a dangling output"
        );

        // Validate task_3.output_0
        let task_3_output_0 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 3,
                position: 0,
            })
            .expect("task_3.output_0 should exist");
        assert_eq!(task_3_output_0.get_index(), 9);
        assert_eq!(task_3_output_0.get_type_descriptor(), &int32_type);
        assert_eq!(
            task_3_output_0.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 3,
                position: 0,
            })
        );
        assert_eq!(
            task_3_output_0.get_dst().len(),
            1,
            "task_3.output_0 should connect to task_5"
        );
        assert_eq!(
            task_3_output_0.get_dst()[0],
            TaskInputOutputIndex {
                task_idx: 5,
                position: 0
            }
        );

        // Validate dangling output: task_4.output_0
        let task_4_output_0 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 4,
                position: 0,
            })
            .expect("task_4.output_0 should exist");
        assert_eq!(task_4_output_0.get_index(), 10);
        assert_eq!(task_4_output_0.get_type_descriptor(), &float32_type);
        assert_eq!(
            task_4_output_0.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 4,
                position: 0,
            })
        );
        assert_eq!(
            task_4_output_0.get_dst().len(),
            0,
            "task_4.output_0 should be a dangling output"
        );

        // Validate task_4.output_1
        let task_4_output_1 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 4,
                position: 1,
            })
            .expect("task_4.output_1 should exist");
        assert_eq!(task_4_output_1.get_index(), 11);
        assert_eq!(task_4_output_1.get_type_descriptor(), &bytes_type);
        assert_eq!(
            task_4_output_1.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 4,
                position: 1,
            })
        );
        assert_eq!(
            task_4_output_1.get_dst().len(),
            1,
            "task_4.output_1 should connect to task_6"
        );
        assert_eq!(
            task_4_output_1.get_dst()[0],
            TaskInputOutputIndex {
                task_idx: 6,
                position: 3
            }
        );

        // Validate task_5.output_0
        let task_5_output_0 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 5,
                position: 0,
            })
            .expect("task_5.output_0 should exist");
        assert_eq!(task_5_output_0.get_index(), 12);
        assert_eq!(task_5_output_0.get_type_descriptor(), &bool_type);
        assert_eq!(
            task_5_output_0.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 5,
                position: 0,
            })
        );
        assert_eq!(
            task_5_output_0.get_dst().len(),
            1,
            "task_5.output_0 should connect to task_6"
        );
        assert_eq!(
            task_5_output_0.get_dst()[0],
            TaskInputOutputIndex {
                task_idx: 6,
                position: 2
            }
        );

        // Validate fan-out case:
        // task_5.output_1 -> task_6.input_0, task_6.input_1, and task_7.input_0
        let task_5_output_1 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 5,
                position: 1,
            })
            .expect("task_5.output_1 should exist");
        assert_eq!(task_5_output_1.get_index(), 13);
        assert_eq!(task_5_output_1.get_type_descriptor(), &list_bytes_type);
        assert_eq!(
            task_5_output_1.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 5,
                position: 1,
            })
        );
        assert_eq!(
            task_5_output_1.get_dst().len(),
            3,
            "task_5.output_1 should fan out to 3 destinations (2 in same task, 1 in different \
             task)"
        );
        assert!(
            task_5_output_1.get_dst().contains(&TaskInputOutputIndex {
                task_idx: 6,
                position: 0,
            }),
            "task_5.output_1 should connect to task_6.input_0"
        );
        assert!(
            task_5_output_1.get_dst().contains(&TaskInputOutputIndex {
                task_idx: 6,
                position: 1,
            }),
            "task_5.output_1 should connect to task_6.input_1 (same output used twice in same \
             task)"
        );
        assert!(
            task_5_output_1.get_dst().contains(&TaskInputOutputIndex {
                task_idx: 7,
                position: 0,
            }),
            "task_5.output_1 should connect to task_7.input_0"
        );

        // Validate dangling outputs from task_6, task_7, and task_8
        let task_6_output_0 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 6,
                position: 0,
            })
            .expect("task_6.output_0 should exist");
        assert_eq!(task_6_output_0.get_index(), 14);
        assert_eq!(task_6_output_0.get_type_descriptor(), &int64_type);
        assert_eq!(
            task_6_output_0.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 6,
                position: 0,
            })
        );
        assert_eq!(
            task_6_output_0.get_dst().len(),
            0,
            "task_6.output_0 should be a dangling output"
        );

        let task_7_output_0 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 7,
                position: 0,
            })
            .expect("task_7.output_0 should exist");
        assert_eq!(task_7_output_0.get_index(), 15);
        assert_eq!(task_7_output_0.get_type_descriptor(), &float64_type);
        assert_eq!(
            task_7_output_0.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 7,
                position: 0,
            })
        );
        assert_eq!(
            task_7_output_0.get_dst().len(),
            0,
            "task_7.output_0 should be a dangling output"
        );

        let task_8_output_0 = graph
            .get_task_output(TaskInputOutputIndex {
                task_idx: 8,
                position: 0,
            })
            .expect("task_8.output_0 should exist");
        assert_eq!(task_8_output_0.get_index(), 16);
        assert_eq!(task_8_output_0.get_type_descriptor(), &int32_type);
        assert_eq!(
            task_8_output_0.get_src(),
            Some(TaskInputOutputIndex {
                task_idx: 8,
                position: 0,
            })
        );
        assert_eq!(
            task_8_output_0.get_dst().len(),
            0,
            "task_8.output_0 should be a dangling output"
        );

        // Validate intermediate connections using get_task_input
        let task_3_input_0 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 3,
                position: 0,
            })
            .expect("task_3.input_0 should exist");
        assert_eq!(task_3_input_0.get_index(), 7);
        assert_eq!(task_3_input_0.get_type_descriptor(), &map_type);

        let task_3_input_1 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 3,
                position: 1,
            })
            .expect("task_3.input_1 should exist");
        assert_eq!(task_3_input_1.get_index(), 3);
        assert_eq!(task_3_input_1.get_type_descriptor(), &bool_type);

        let task_4_input_0 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 4,
                position: 0,
            })
            .expect("task_4.input_0 should exist");
        assert_eq!(task_4_input_0.get_index(), 7);
        assert_eq!(task_4_input_0.get_type_descriptor(), &map_type);

        let task_4_input_1 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 4,
                position: 1,
            })
            .expect("task_4.input_1 should exist");
        assert_eq!(task_4_input_1.get_index(), 5);
        assert_eq!(task_4_input_1.get_type_descriptor(), &list_int32_type);

        // Validate task_6's complex inputs
        let task_6_input_0 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 6,
                position: 0,
            })
            .expect("task_6.input_0 should exist");
        assert_eq!(
            task_6_input_0.get_index(),
            13,
            "task_6.input_0 should be task_5.output_1"
        );
        assert_eq!(task_6_input_0.get_type_descriptor(), &list_bytes_type);

        let task_6_input_1 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 6,
                position: 1,
            })
            .expect("task_6.input_1 should exist");
        assert_eq!(
            task_6_input_1.get_index(),
            13,
            "task_6.input_1 should also be task_5.output_1"
        );
        assert_eq!(task_6_input_1.get_type_descriptor(), &list_bytes_type);

        let task_6_input_2 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 6,
                position: 2,
            })
            .expect("task_6.input_2 should exist");
        assert_eq!(
            task_6_input_2.get_index(),
            12,
            "task_6.input_2 should be task_5.output_0"
        );
        assert_eq!(task_6_input_2.get_type_descriptor(), &bool_type);

        let task_6_input_3 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 6,
                position: 3,
            })
            .expect("task_6.input_3 should exist");
        assert_eq!(
            task_6_input_3.get_index(),
            11,
            "task_6.input_3 should be task_4.output_1"
        );
        assert_eq!(task_6_input_3.get_type_descriptor(), &bytes_type);

        // Validate task_7's input
        let task_7_input_0 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 7,
                position: 0,
            })
            .expect("task_7.input_0 should exist");
        assert_eq!(
            task_7_input_0.get_index(),
            13,
            "task_7.input_0 should be task_5.output_1"
        );
        assert_eq!(task_7_input_0.get_type_descriptor(), &list_bytes_type);

        // Validate task_8's swapped inputs
        let task_8_input_0 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 8,
                position: 0,
            })
            .expect("task_8.input_0 should exist");
        assert_eq!(
            task_8_input_0.get_index(),
            6,
            "task_8.input_0 should be task_1.output_1 (swapped)"
        );
        assert_eq!(task_8_input_0.get_type_descriptor(), &bytes_type);

        let task_8_input_1 = graph
            .get_task_input(TaskInputOutputIndex {
                task_idx: 8,
                position: 1,
            })
            .expect("task_8.input_1 should exist");
        assert_eq!(
            task_8_input_1.get_index(),
            5,
            "task_8.input_1 should be task_1.output_0 (swapped)"
        );
        assert_eq!(task_8_input_1.get_type_descriptor(), &list_int32_type);
    }

    /// Tests error handling when the number of inputs doesn't match the number of input sources.
    #[test]
    fn test_insert_task_mismatched_input_source_count() {
        let mut graph = TaskGraph::default();

        let int32_type = DataTypeDescriptor::Value(ValueTypeDescriptor::int32());
        let float64_type = DataTypeDescriptor::Value(ValueTypeDescriptor::float64());
        let bool_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bool());

        // Create task_0 with 2 outputs
        let task_0_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_1".to_string(),
                inputs: vec![int32_type.clone()],
                outputs: vec![float64_type.clone(), bool_type.clone()],
                input_sources: None,
            })
            .expect("task_0 insertion should succeed");

        assert_eq!(task_0_idx, 0);

        // Attempt to create task_1 with 3 inputs but only 2 input sources (mismatched count)
        assert_invalid_task_inputs(&graph.insert_task(TaskDescriptor {
            tdl_package: TEST_PACKAGE.to_string(),
            tdl_function: "fn_2".to_string(),
            inputs: vec![float64_type.clone(), bool_type, int32_type.clone()],
            outputs: vec![int32_type.clone()],
            input_sources: Some(vec![
                TaskInputOutputIndex {
                    task_idx: 0,
                    position: 0,
                },
                TaskInputOutputIndex {
                    task_idx: 0,
                    position: 1,
                },
            ]),
        }));

        // Attempt to create task_1 with 1 input but 0 input sources (mismatched count)
        assert_invalid_task_inputs(&graph.insert_task(TaskDescriptor {
            tdl_package: TEST_PACKAGE.to_string(),
            tdl_function: "fn_2".to_string(),
            inputs: vec![float64_type],
            outputs: vec![int32_type.clone()],
            input_sources: Some(vec![]),
        }));

        // Attempt to create task_1 with 0 input but 1 input sources (mismatched count)
        assert_invalid_task_inputs(&graph.insert_task(TaskDescriptor {
            tdl_package: TEST_PACKAGE.to_string(),
            tdl_function: "fn_2".to_string(),
            inputs: vec![],
            outputs: vec![int32_type],
            input_sources: Some(vec![TaskInputOutputIndex {
                task_idx: 0,
                position: 0,
            }]),
        }));

        // Verify graph state is unchanged
        assert_eq!(
            graph
                .get_task(0)
                .expect("task_0 should still exist")
                .get_num_children(),
            0
        );
    }

    /// Tests error handling when input sources are specified, but the task doesn't have any inputs.
    #[test]
    fn test_invalid_task_input_source_for_zero_input() {
        let mut graph = TaskGraph::default();

        let int32_type = DataTypeDescriptor::Value(ValueTypeDescriptor::int32());

        // Create task_0 with a single Int32 output
        let task_0_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_1".to_string(),
                inputs: vec![],
                outputs: vec![int32_type.clone()],
                input_sources: None,
            })
            .expect("task_0 insertion should succeed");

        assert_eq!(task_0_idx, 0);

        // Attempt to create task_1 with no input but the source is not `None`
        assert_invalid_task_inputs(&graph.insert_task(TaskDescriptor {
            tdl_package: TEST_PACKAGE.to_string(),
            tdl_function: "fn_2".to_string(),
            inputs: vec![],
            outputs: vec![int32_type],
            input_sources: Some(vec![]),
        }));
    }

    /// Tests error handling when the input type doesn't match the source output type.
    #[test]
    fn test_insert_task_type_mismatch() {
        let mut graph = TaskGraph::default();

        let int32_type = DataTypeDescriptor::Value(ValueTypeDescriptor::int32());
        let float64_type = DataTypeDescriptor::Value(ValueTypeDescriptor::float64());
        let bool_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bool());
        let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());

        // Create task_0 with Float64 and Boolean outputs
        let task_0_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_1".to_string(),
                inputs: vec![int32_type.clone()],
                outputs: vec![float64_type, bool_type],
                input_sources: None,
            })
            .expect("task_0 insertion should succeed");

        assert_eq!(task_0_idx, 0);

        // Attempt to create task_1 with Bytes input but the source is Float64 (type mismatch)
        assert_invalid_task_inputs(&graph.insert_task(TaskDescriptor {
            tdl_package: TEST_PACKAGE.to_string(),
            tdl_function: "fn_2".to_string(),
            inputs: vec![bytes_type],
            outputs: vec![int32_type],
            input_sources: Some(vec![TaskInputOutputIndex {
                task_idx: 0,
                position: 0,
            }]),
        }));
    }

    /// Tests error handling when the input source references an invalid task index.
    #[test]
    fn test_insert_task_invalid_task_index() {
        let mut graph = TaskGraph::default();

        let int32_type = DataTypeDescriptor::Value(ValueTypeDescriptor::int32());
        let float64_type = DataTypeDescriptor::Value(ValueTypeDescriptor::float64());

        // Create task_0
        let task_0_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_1".to_string(),
                inputs: vec![int32_type.clone()],
                outputs: vec![float64_type.clone()],
                input_sources: None,
            })
            .expect("task_0 insertion should succeed");

        assert_eq!(task_0_idx, 0);

        // Attempt to create task_1 with the source referencing non-existent task_5
        assert_invalid_task_inputs(&graph.insert_task(TaskDescriptor {
            tdl_package: TEST_PACKAGE.to_string(),
            tdl_function: "fn_2".to_string(),
            inputs: vec![float64_type],
            outputs: vec![int32_type],
            input_sources: Some(vec![TaskInputOutputIndex {
                task_idx: 5,
                position: 0,
            }]),
        }));
    }

    /// Tests error handling when the input source references an invalid output position.
    #[test]
    fn test_insert_task_invalid_output_position() {
        let mut graph = TaskGraph::default();

        let int32_type = DataTypeDescriptor::Value(ValueTypeDescriptor::int32());
        let float64_type = DataTypeDescriptor::Value(ValueTypeDescriptor::float64());
        let bool_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bool());

        // Create task_0 with 2 outputs (positions 0 and 1)
        let task_0_idx = graph
            .insert_task(TaskDescriptor {
                tdl_package: TEST_PACKAGE.to_string(),
                tdl_function: "fn_1".to_string(),
                inputs: vec![int32_type.clone()],
                outputs: vec![float64_type.clone(), bool_type],
                input_sources: None,
            })
            .expect("task_0 insertion should succeed");

        assert_eq!(task_0_idx, 0);

        // Attempt to create task_1 with the source referencing non-existent output position 2
        assert_invalid_task_inputs(&graph.insert_task(TaskDescriptor {
            tdl_package: TEST_PACKAGE.to_string(),
            tdl_function: "fn_2".to_string(),
            inputs: vec![float64_type],
            outputs: vec![int32_type],
            input_sources: Some(vec![TaskInputOutputIndex {
                task_idx: 0,
                position: 2,
            }]),
        }));
    }

    /// # Panics
    ///
    /// If the result is not [`Error::InvalidTaskInputs`].
    fn assert_invalid_task_inputs<T: Debug>(result: &Result<T, Error>) {
        match result {
            Err(Error::InvalidTaskInputs(_)) => (),
            _ => panic!("Expected InvalidTaskInputs error, got: {result:?}"),
        }
    }
}
