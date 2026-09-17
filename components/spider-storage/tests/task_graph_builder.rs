use anyhow::Result;
use rand::Rng;
use rand::SeedableRng;
use spider_core::task::DataTypeDescriptor;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TaskDescriptor;
use spider_core::task::TaskIndex;
use spider_core::task::TaskInputOutputIndex;
use spider_core::task::TdlContext;
use spider_core::task::TerminationTaskDescriptor;
use spider_core::task::ValueTypeDescriptor;
use spider_core::types::io::TaskGraphInput;
use spider_core::types::io::TaskGraphInputBuilder;
use spider_storage::job_submission::ValidatedJobSubmission;

/// The submitted task graph type from spider-core.
pub type SubmittedTaskGraph = spider_core::task::TaskGraph;

/// Compresses a task graph into the zstd-compressed JSON format the database persists.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`spider_core::task::TaskGraph::to_zstd_compressed_json`]'s return values on failure.
pub fn compress_task_graph(task_graph: &SubmittedTaskGraph) -> Result<Vec<u8>> {
    Ok(task_graph.to_zstd_compressed_json()?)
}

/// Compresses job inputs into the zstd-compressed serialized [`TaskGraphInput`] format the
/// database persists.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`TaskGraphInput::to_zstd_compressed_bytes`]'s return values on failure.
pub fn compress_job_inputs(task_graph_input: &TaskGraphInput) -> Result<Vec<u8>> {
    Ok(task_graph_input.to_zstd_compressed_bytes()?)
}

/// Compresses a submitted task graph and job inputs into the formats the database persists, then
/// builds a [`ValidatedJobSubmission`].
///
/// The task graph is zstd-compressed JSON and the job inputs are a zstd-compressed serialized
/// [`TaskGraphInput`], matching what the storage service receives from a client.
///
/// # Panics
///
/// Panics if compression or submission validation fails.
// Takes the graph and inputs by value for caller ergonomics: callers build and hand off owned
// values, even though they are only borrowed here to produce the compressed serializations.
#[allow(clippy::needless_pass_by_value)]
#[must_use]
pub fn create_validated_submission(
    task_graph: SubmittedTaskGraph,
    task_graph_input: TaskGraphInput,
) -> ValidatedJobSubmission {
    let compressed_task_graph =
        compress_task_graph(&task_graph).expect("task graph compression should succeed");
    let compressed_inputs =
        compress_job_inputs(&task_graph_input).expect("input compression should succeed");
    ValidatedJobSubmission::create(compressed_task_graph, compressed_inputs)
        .expect("job submission should be valid")
}

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
/// The submitted task graph and the corresponding job inputs (one msgpack-serialized payload of
/// `payload_size` zero bytes per task).
///
/// # Panics
///
/// Panics if:
///
/// * The task graph or any task descriptor fails to construct.
/// * Any job input fails to serialize.
#[must_use]
pub fn build_flat_task_graph(
    num_tasks: usize,
    payload_size: usize,
    with_commit: bool,
    with_cleanup: bool,
) -> (SubmittedTaskGraph, TaskGraphInput) {
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

    let payload = vec![0u8; payload_size];
    let mut task_graph_input_builder = TaskGraphInputBuilder::new();
    for _ in 0..num_tasks {
        task_graph_input_builder
            .append_task_input(&payload)
            .expect("flat task input appending should succeed");
    }

    (graph, task_graph_input_builder.build())
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
/// The submitted task graph and the corresponding job inputs (1,000 msgpack-serialized payloads of
/// 128 zero bytes each for layer 0's 1,000 tasks x 1 input).
///
/// # Panics
///
/// Panics if:
///
/// * The task graph or any task descriptor fails to construct.
/// * Any job input fails to serialize.
#[must_use]
pub fn build_neural_net_task_graph() -> (SubmittedTaskGraph, TaskGraphInput) {
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

    let payload = vec![0u8; 128];
    let mut task_graph_input_builder = TaskGraphInputBuilder::new();
    for _ in 0..NUM_GRAPH_INPUTS {
        task_graph_input_builder
            .append_task_input(&payload)
            .expect("neural-net task input appending should succeed");
    }

    (graph, task_graph_input_builder.build())
}

const TDL_PACKAGE: &str = "test";
