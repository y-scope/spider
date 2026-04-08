use rand::{Rng, SeedableRng};
use spider_core::{
    task::{
        DataTypeDescriptor,
        ExecutionPolicy,
        TaskDescriptor,
        TaskIndex,
        TaskInputOutputIndex,
        TdlContext,
        TerminationTaskDescriptor,
        ValueTypeDescriptor,
    },
    types::io::TaskInput,
};

/// The submitted task graph type from spider-core.
pub type SubmittedTaskGraph = spider_core::task::TaskGraph;

/// Builds a flat workload of `num_tasks` independent tasks
///
/// # Inputs and Outputs
///
/// Each task in the task graph contains a single byte-typed input and a single byte-typed output.
///
/// # Execution Policy
///
/// * `max_num_retry`: 3
/// * `max_num_instances`: 2
///
/// # TDL Context
///
/// * Package: `test`
/// * Task function: `flat_task`
/// * Commit task function: `noop_commit`
/// * Cleanup task function: `noop_cleanup`
///
/// # Returns
///
/// The submitted task graph and the corresponding job inputs (one `payload_size`-byte payload per
/// task).
///
/// # Panics
///
/// Panics if the task graph or any task descriptor fails to construct.
#[must_use]
pub fn build_flat_task_graph(
    num_tasks: usize,
    payload_size: usize,
    with_commit: bool,
    with_cleanup: bool,
) -> (SubmittedTaskGraph, Vec<TaskInput>) {
    const TDL_TASK: &str = "flat_task";
    const TDL_COMMIT_TASK: &str = "noop_commit";
    const TDL_CLEANUP_TASK: &str = "noop_cleanup";

    let execution_policy = Some(ExecutionPolicy {
        max_num_retry: 3,
        max_num_instances: 2,
        ..ExecutionPolicy::default()
    });
    let commit_task = if with_commit {
        Some(TerminationTaskDescriptor {
            tdl_context: TdlContext {
                package: TDL_PACKAGE.to_owned(),
                task_func: TDL_COMMIT_TASK.to_owned(),
            },
            execution_policy: execution_policy.clone(),
        })
    } else {
        None
    };
    let cleanup_task = if with_cleanup {
        Some(TerminationTaskDescriptor {
            tdl_context: TdlContext {
                package: TDL_PACKAGE.to_owned(),
                task_func: TDL_CLEANUP_TASK.to_owned(),
            },
            execution_policy: execution_policy.clone(),
        })
    } else {
        None
    };
    let mut graph = SubmittedTaskGraph::new(commit_task, cleanup_task)
        .expect("flat task graph creation should succeed");

    let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
    for _ in 0..num_tasks {
        graph
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: TDL_PACKAGE.to_owned(),
                    task_func: TDL_TASK.to_owned(),
                },
                execution_policy: execution_policy.clone(),
                inputs: vec![bytes_type.clone()],
                outputs: vec![bytes_type.clone()],
                input_sources: None,
            })
            .expect("flat task insertion should succeed");
    }

    let inputs: Vec<TaskInput> = (0..num_tasks)
        .map(|_| TaskInput::ValuePayload(vec![0u8; payload_size]))
        .collect();

    (graph, inputs)
}

/// Builds a neural-net workload: 10 layers of 1,000 tasks each (10,000 total), with no commit or
/// cleanup tasks.
///
/// # Inputs and Outputs
///
/// * Layer 0 (input layer): each task has 1 byte-typed input from the graph inputs and 1 byte-typed
///   output.
/// * Layers 1-9: each task has 25 inputs sourced from random tasks in the previous layer
///   (deterministic via seeded RNG) and 1 byte-typed output.
///
/// # Execution Policy
///
/// * `max_num_retry`: 3
/// * `max_num_instances`: 2
///
/// # TDL Context
///
/// * Package: `test`
/// * Task function: `nn_task`
///
/// # Returns
///
/// The submitted task graph and the corresponding job inputs (1,000 payloads of 128 bytes each
/// for layer 0's 1,000 tasks x 1 input).
///
/// # Panics
///
/// Panics if the task graph or any task descriptor fails to construct.
#[must_use]
pub fn build_neural_net_task_graph() -> (SubmittedTaskGraph, Vec<TaskInput>) {
    const TDL_FUNC: &str = "nn_task";
    const NUM_LAYERS: usize = 10;
    const TASKS_PER_LAYER: usize = 1_000;
    const INPUTS_PER_INNER_LAYER: usize = 25;
    const NUM_GRAPH_INPUTS: usize = TASKS_PER_LAYER;

    let execution_policy = Some(ExecutionPolicy {
        max_num_retry: 3,
        max_num_instances: 2,
        ..ExecutionPolicy::default()
    });

    let mut graph =
        SubmittedTaskGraph::new(None, None).expect("neural-net task graph creation should succeed");
    let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());

    // Layer 0: input layer (1 graph input per task, no input_sources).
    for _ in 0..TASKS_PER_LAYER {
        graph
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: TDL_PACKAGE.to_owned(),
                    task_func: TDL_FUNC.to_owned(),
                },
                execution_policy: execution_policy.clone(),
                inputs: vec![bytes_type.clone()],
                outputs: vec![bytes_type.clone()],
                input_sources: None,
            })
            .expect("neural-net layer-0 task insertion should succeed");
    }

    // Layers 1-9: each task draws 25 random inputs from the previous layer.
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);
    for layer in 1..NUM_LAYERS {
        let prev_layer_start: TaskIndex = (layer - 1) * TASKS_PER_LAYER;
        for _ in 0..TASKS_PER_LAYER {
            let input_sources: Vec<TaskInputOutputIndex> = (0..INPUTS_PER_INNER_LAYER)
                .map(|_| {
                    let src_task: TaskIndex =
                        prev_layer_start + rng.random_range(0..TASKS_PER_LAYER);
                    TaskInputOutputIndex {
                        task_idx: src_task,
                        position: 0,
                    }
                })
                .collect();
            graph
                .insert_task(TaskDescriptor {
                    tdl_context: TdlContext {
                        package: TDL_PACKAGE.to_owned(),
                        task_func: TDL_FUNC.to_owned(),
                    },
                    execution_policy: execution_policy.clone(),
                    inputs: vec![bytes_type.clone(); INPUTS_PER_INNER_LAYER],
                    outputs: vec![bytes_type.clone()],
                    input_sources: Some(input_sources),
                })
                .expect("neural-net task insertion should succeed");
        }
    }

    let inputs: Vec<TaskInput> = (0..NUM_GRAPH_INPUTS)
        .map(|_| TaskInput::ValuePayload(vec![0u8; 128]))
        .collect();

    (graph, inputs)
}

const TDL_PACKAGE: &str = "test";
