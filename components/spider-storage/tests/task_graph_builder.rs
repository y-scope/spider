//! Reusable task graph builders for integration tests.
//!
//! These builders construct [`SubmittedTaskGraph`] instances along with their corresponding job
//! inputs for use in test harnesses. Each builder returns a `(SubmittedTaskGraph, Vec<TaskInput>)`
//! pair.
//!
//! # Available workloads
//!
//! * [`build_flat_task_graph`] — configurable-size independent tasks with optional termination
//!   tasks.
//! * [`build_neural_net_task_graph`] — 10 layers × 1,000 tasks in a layered DAG with random
//!   inter-layer connections.
//!
//! # Panics
//!
//! All builders panic if the task graph construction fails (e.g., invalid descriptors). This is
//! intentional — builders are only used in tests where a construction failure indicates a bug in
//! the test setup, not a recoverable condition.

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
type SubmittedTaskGraph = spider_core::task::TaskGraph;

/// Builds a flat workload of `num_tasks` independent tasks, each with 1 byte-typed input and 1
/// byte-typed output.
///
/// # Parameters
///
/// * `num_tasks` -- the number of independent tasks to create.
/// * `payload_size` -- the size (in bytes) of each task's input payload.
/// * `with_commit` -- whether to include a noop commit termination task.
/// * `with_cleanup` -- whether to include a noop cleanup termination task.
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
    let execution_policy = Some(ExecutionPolicy {
        max_num_retry: 3,
        max_num_instances: 2,
        ..ExecutionPolicy::default()
    });
    let commit_task = if with_commit {
        Some(TerminationTaskDescriptor {
            tdl_context: TdlContext {
                package: "test".to_owned(),
                task_func: "noop_commit".to_owned(),
            },
            execution_policy: execution_policy.clone(),
        })
    } else {
        None
    };
    let cleanup_task = if with_cleanup {
        Some(TerminationTaskDescriptor {
            tdl_context: TdlContext {
                package: "test".to_owned(),
                task_func: "noop_cleanup".to_owned(),
            },
            execution_policy: execution_policy.clone(),
        })
    } else {
        None
    };
    let mut graph = SubmittedTaskGraph::new(commit_task, cleanup_task)
        .expect("flat task graph creation should succeed");

    let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
    for i in 0..num_tasks {
        graph
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: "test".to_owned(),
                    task_func: format!("flat_task_{i}"),
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

/// Builds a neural-net workload: 10 layers of 1,000 tasks each (10,000 total).
///
/// * Layer 0: each task has 25 byte-typed inputs from the graph inputs and 1 byte-typed output.
/// * Layers 1-9: each task has 25 inputs sourced from random tasks in the previous layer
///   (deterministic via seeded RNG) and 1 byte-typed output.
///
/// No commit or cleanup tasks.
///
/// # Returns
///
/// The submitted task graph and the corresponding job inputs (25,000 payloads of 128 bytes each
/// for layer 0's 1,000 tasks x 25 inputs).
///
/// # Panics
///
/// Panics if the task graph or any task descriptor fails to construct.
#[must_use]
pub fn build_neural_net_task_graph() -> (SubmittedTaskGraph, Vec<TaskInput>) {
    let num_layers: usize = 10;
    let tasks_per_layer: usize = 1_000;
    let inputs_per_task: usize = 25;

    let execution_policy = Some(ExecutionPolicy {
        max_num_retry: 3,
        max_num_instances: 2,
        ..ExecutionPolicy::default()
    });

    let mut graph =
        SubmittedTaskGraph::new(None, None).expect("neural-net task graph creation should succeed");
    let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());

    // Layer 0: input tasks (no input_sources).
    for i in 0..tasks_per_layer {
        graph
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: "test".to_owned(),
                    task_func: format!("nn_L0_T{i}"),
                },
                execution_policy: execution_policy.clone(),
                inputs: vec![bytes_type.clone(); inputs_per_task],
                outputs: vec![bytes_type.clone()],
                input_sources: None,
            })
            .expect("neural-net layer-0 task insertion should succeed");
    }

    // Layers 1-9: each task draws 25 random inputs from the previous layer.
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    for layer in 1..num_layers {
        let prev_layer_start: TaskIndex = (layer - 1) * tasks_per_layer;
        for i in 0..tasks_per_layer {
            let input_sources: Vec<TaskInputOutputIndex> = (0..inputs_per_task)
                .map(|_| {
                    let src_task: TaskIndex =
                        prev_layer_start + rng.random_range(0..tasks_per_layer);
                    TaskInputOutputIndex {
                        task_idx: src_task,
                        position: 0,
                    }
                })
                .collect();
            graph
                .insert_task(TaskDescriptor {
                    tdl_context: TdlContext {
                        package: "test".to_owned(),
                        task_func: format!("nn_L{layer}_T{i}"),
                    },
                    execution_policy: execution_policy.clone(),
                    inputs: vec![bytes_type.clone(); inputs_per_task],
                    outputs: vec![bytes_type.clone()],
                    input_sources: Some(input_sources),
                })
                .expect("neural-net task insertion should succeed");
        }
    }

    let num_graph_inputs = tasks_per_layer * inputs_per_task;
    let inputs: Vec<TaskInput> = (0..num_graph_inputs)
        .map(|_| TaskInput::ValuePayload(vec![0u8; 128]))
        .collect();

    (graph, inputs)
}
