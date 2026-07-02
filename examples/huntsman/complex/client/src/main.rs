//! Simple Spider client that builds a layered `complex::add` task graph and runs it against a live
//! Spider stack.
//!
//! The graph is "neural-network-shaped": `--level` layers of `--width` tasks each. Layer 0 takes
//! its two inputs from the graph inputs; every inner task adds two outputs from the previous layer
//! (a fixed fan-in pattern, `prev[i]` and `prev[(i + 1) % width]`). Tasks within a layer are
//! independent, so `--width` controls how much parallelism the scheduler can exploit -- e.g.
//! `--width 16` keeps a 16-worker stack busy.
//!
//! After the job finishes, the client decodes the final layer's outputs and compares them against
//! an in-process simulation of the same DAG, proving the stack executed the graph correctly.

use std::{
    num::NonZeroUsize,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow};
use clap::Parser;
use huntsman_complex_types::{Complex, ComplexVec};
use spider_client::SpiderClient;
use spider_core::{
    job::JobState,
    task::{
        DataTypeDescriptor,
        TaskDescriptor,
        TaskGraph,
        TaskIndex,
        TaskInputOutputIndex,
        TdlContext,
        ValueTypeDescriptor,
    },
    types::{id::JobId, io::TaskInput},
};
use tonic::transport::Endpoint;

/// TDL package and task function the graph drives.
const PACKAGE: &str = "complex";
const TASK_FUNC: &str = "complex::add";

/// Password used when registering the per-run resource group.
const RESOURCE_GROUP_PASSWORD: &[u8] = b"huntsman-complex-client";

/// Command-line arguments for the example client.
#[derive(Debug, Parser)]
#[command(about = "Build a layered complex::add task graph and run it against the Spider stack.")]
struct Cli {
    /// Spider storage gRPC endpoint to connect to.
    #[arg(long, value_name = "URL", default_value = "http://127.0.0.1:50051")]
    endpoint: String,

    /// Number of layers (graph depth).
    #[arg(long, default_value_t = 10)]
    level: usize,

    /// Tasks per layer. Tasks within a layer are independent, so this controls parallelism
    /// (e.g. `--width 16` saturates a 16-worker stack).
    #[arg(long, default_value_t = 4)]
    width: usize,

    /// `SpiderClient` gRPC connection pool size.
    #[arg(long, default_value_t = 4)]
    pool_size: usize,

    /// Print each final-layer output's complex values alongside the simulation, for inspection.
    #[arg(long)]
    print_outputs: bool,
}

/// Builds the layered `complex::add` task graph.
///
/// Layer 0 has `width` tasks whose two inputs come from the graph inputs. Each subsequent layer
/// has `width` tasks whose two inputs are outputs `prev[i]` and `prev[(i + 1) % width]` from the
/// previous layer.
///
/// # Returns
///
/// The assembled task graph on success.
///
/// # Errors
///
/// Forwards [`TaskGraph::new`]'s return values on failure.
/// Forwards [`TaskGraph::insert_task`]'s return values on failure.
fn build_graph(level: usize, width: usize) -> anyhow::Result<TaskGraph> {
    let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
    let tdl_context = TdlContext {
        package: PACKAGE.to_owned(),
        task_func: TASK_FUNC.to_owned(),
    };

    let mut graph = TaskGraph::new(None, None)?;

    // Layer 0: both inputs come from the graph inputs.
    let mut prev_layer: Vec<TaskIndex> = Vec::with_capacity(width);
    for _ in 0..width {
        let task_idx = graph.insert_task(TaskDescriptor {
            tdl_context: tdl_context.clone(),
            execution_policy: None,
            inputs: vec![bytes_type.clone(); 2],
            outputs: vec![bytes_type.clone()],
            input_sources: None,
        })?;
        prev_layer.push(task_idx);
    }

    // Inner layers: both inputs come from the previous layer's outputs.
    for _ in 1..level {
        let mut cur_layer = Vec::with_capacity(width);
        for i in 0..width {
            let lhs = prev_layer[i];
            let rhs = prev_layer[(i + 1) % width];
            let task_idx = graph.insert_task(TaskDescriptor {
                tdl_context: tdl_context.clone(),
                execution_policy: None,
                inputs: vec![bytes_type.clone(); 2],
                outputs: vec![bytes_type.clone()],
                input_sources: Some(vec![
                    TaskInputOutputIndex {
                        task_idx: lhs,
                        position: 0,
                    },
                    TaskInputOutputIndex {
                        task_idx: rhs,
                        position: 0,
                    },
                ]),
            })?;
            cur_layer.push(task_idx);
        }
        prev_layer = cur_layer;
    }

    Ok(graph)
}

/// Builds the graph inputs: `2 * width` short `ComplexVec` values, one per positional input of the
/// layer-0 tasks (insertion order: task `i` consumes inputs `2 * i` and `2 * i + 1`).
///
/// # Returns
///
/// The graph input vectors.
fn build_graph_inputs(width: usize) -> Vec<ComplexVec> {
    let count = 2usize.checked_mul(width).expect("width too large");
    let mut values = Vec::with_capacity(count);
    // `k` is an f64 counter so no integer->float cast (which would trip clippy::cast_precision_loss
    // under -D pedantic); the values are deterministic test data either way.
    let mut k = 0.0_f64;
    for _ in 0..count {
        values.push(ComplexVec {
            items: vec![
                Complex {
                    re: k + 1.0,
                    im: 0.0,
                },
                Complex {
                    re: 0.0,
                    im: k + 1.0,
                },
            ],
        });
        k += 1.0;
    }
    values
}

/// Element-wise complex addition of two equal-length vectors.
///
/// # Panics
///
/// Panics if `a` and `b` differ in length (the `complex::add` task would reject the same).
fn complex_add(a: &ComplexVec, b: &ComplexVec) -> ComplexVec {
    assert_eq!(
        a.items.len(),
        b.items.len(),
        "complex_add: vector length mismatch"
    );
    ComplexVec {
        items: a
            .items
            .iter()
            .zip(b.items.iter())
            .map(|(x, y)| Complex {
                re: x.re + y.re,
                im: x.im + y.im,
            })
            .collect(),
    }
}

/// Simulates the layered DAG in-process using the same connection pattern as [`build_graph`], so
/// the retrieved outputs can be checked against an independent reference.
///
/// # Returns
///
/// The final layer's output vectors.
fn simulate(level: usize, width: usize, inputs: &[ComplexVec]) -> Vec<ComplexVec> {
    let mut prev: Vec<ComplexVec> = (0..width)
        .map(|i| complex_add(&inputs[2 * i], &inputs[2 * i + 1]))
        .collect();
    for _ in 1..level {
        let cur: Vec<ComplexVec> = (0..width)
            .map(|i| complex_add(&prev[i], &prev[(i + 1) % width]))
            .collect();
        prev = cur;
    }
    prev
}

/// Polls the job state until it reaches a terminal state.
///
/// # Returns
///
/// The terminal [`JobState`] on success.
///
/// # Errors
///
/// Forwards [`SpiderClient::get_job_state`]'s return values on failure.
async fn poll_until_terminal(client: &SpiderClient, job_id: JobId) -> anyhow::Result<JobState> {
    loop {
        let state = client
            .get_job_state(job_id)
            .await
            .context("get_job_state")?;
        if state.is_terminal() {
            return Ok(state);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if cli.level == 0 || cli.width == 0 {
        return Err(anyhow!("--level and --width must be >= 1"));
    }
    let pool_size = NonZeroUsize::new(cli.pool_size).context("--pool-size must be >= 1")?;

    let endpoint: Endpoint = cli
        .endpoint
        .parse()
        .with_context(|| format!("invalid --endpoint {:?}", cli.endpoint))?;
    let client = SpiderClient::connect(endpoint, pool_size)
        .await
        .context("failed to connect to the Spider storage service")?;

    let graph_inputs = build_graph_inputs(cli.width);
    let expected = simulate(cli.level, cli.width, &graph_inputs);
    let graph = build_graph(cli.level, cli.width)?;

    let task_inputs: Vec<TaskInput> = graph_inputs
        .iter()
        .map(|value| {
            Ok::<TaskInput, anyhow::Error>(TaskInput::ValuePayload(
                rmp_serde::to_vec(value).context("failed to serialize a graph input")?,
            ))
        })
        .collect::<anyhow::Result<_>>()?;

    // Use a unique external id per run so repeated runs (against a persistent MariaDB) do not
    // collide with the resource-group-already-exists error.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before UNIX epoch")?
        .as_nanos();
    let resource_group_id = client
        .add_resource_group(
            format!("huntsman-nn-{nanos}"),
            RESOURCE_GROUP_PASSWORD.to_vec(),
        )
        .await
        .context("add_resource_group")?;

    let job_id = client
        .submit_job(resource_group_id, &graph, task_inputs)
        .await
        .context("submit_job")?;
    client.start_job(job_id).await.context("start_job")?;

    println!(
        "Submitted layered complex::add job: level={}, width={}, tasks={}, job_id={}",
        cli.level,
        cli.width,
        cli.level * cli.width,
        job_id.get()
    );

    let state = poll_until_terminal(&client, job_id).await?;
    match state {
        JobState::Succeeded => {
            let outputs = client
                .get_job_outputs(job_id)
                .await
                .context("get_job_outputs")?;
            anyhow::ensure!(
                outputs.len() == cli.width,
                "expected {} graph outputs, got {}",
                cli.width,
                outputs.len()
            );
            let mut mismatches = 0;
            for (i, output) in outputs.iter().enumerate() {
                let got: ComplexVec = rmp_serde::from_slice(output)
                    .with_context(|| format!("failed to decode output {i}"))?;
                if got != expected[i] {
                    mismatches += 1;
                }
                if cli.print_outputs {
                    println!(
                        "output[{i}] got={:?} expected={:?}",
                        got.items, expected[i].items
                    );
                }
            }
            if mismatches == 0 {
                println!(
                    "Job succeeded; all {} final-layer outputs match the local simulation.",
                    outputs.len()
                );
            } else {
                return Err(anyhow!(
                    "job succeeded but {mismatches}/{} outputs mismatched the simulation",
                    outputs.len()
                ));
            }
        }
        JobState::Failed => {
            let message = client
                .get_job_error(job_id)
                .await
                .context("get_job_error")?;
            return Err(anyhow!("job failed: {message}"));
        }
        other => {
            return Err(anyhow!("job ended in unexpected state {other:?}"));
        }
    }

    Ok(())
}
