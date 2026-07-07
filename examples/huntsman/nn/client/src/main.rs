//! Spider client that builds a randomly-wired, neural-network-shaped `nn::dense_*` task graph and
//! runs it on a live Spider instance.

use std::{num::NonZeroUsize, time::Duration};

use anyhow::{Context, anyhow};
use clap::Parser;
use huntsman_nn_core::NUM_INPUTS;
use rand::{
    CryptoRng,
    Rng,
    SeedableRng,
    rngs::StdRng,
    seq::{IndexedRandom, index},
};
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
use spider_utils::logging::set_up_logging;
use tonic::transport::Endpoint;

/// Name of the TDL package supplying the `nn::dense_*` tasks.
const PACKAGE: &str = "nn";

/// An activation function pair.
#[derive(Clone, Copy)]
struct Activation {
    /// The `nn::dense_*` task function name.
    task_func: &'static str,
    /// The core `dense_*` fn.
    evaluate: fn(&[f64; NUM_INPUTS]) -> f64,
}

/// The three `nn::dense_*` activations.
const ACTIVATIONS: &[Activation] = &[
    Activation {
        task_func: "nn::dense_relu",
        evaluate: huntsman_nn_core::dense_relu,
    },
    Activation {
        task_func: "nn::dense_sigmoid",
        evaluate: huntsman_nn_core::dense_sigmoid,
    },
    Activation {
        task_func: "nn::dense_identity",
        evaluate: huntsman_nn_core::dense_identity,
    },
];

/// Command-line arguments for the client.
#[derive(Debug, Parser)]
#[command(
    about = "Build a randomly-wired nn::dense_* task graph and run it on the Spider instance."
)]
struct Cli {
    /// Spider storage gRPC endpoint to connect to.
    #[arg(long, value_name = "URL", default_value = "http://127.0.0.1:50051")]
    endpoint: String,

    /// Number of layers in task graph.
    #[arg(long, default_value_t = 10)]
    level: usize,

    /// Number of neurons per layer. Must be at least the neuron fan-in ([`NUM_INPUTS`]).
    #[arg(long, default_value_t = 1000)]
    width: usize,

    /// Seed for the random task-graph topology.
    #[arg(long, value_name = "UINT")]
    seed: Option<u64>,

    /// gRPC connection pool size.
    #[arg(long, default_value_t = 4)]
    grpc_pool_size: usize,
}

/// Topology of one layer of the graph.
struct Layer {
    /// Activation applied to every neuron in this layer.
    activation: Activation,
    /// Wiring of previous layer's output to current layer's input.
    /// * [`None`] for layer 0, whose inputs come from the graph inputs.
    /// * For other layers, `wiring[i][k]` is the `k`-th previous-layer output index feeding neuron
    ///   `i`.
    wiring: Option<Vec<Vec<usize>>>,
}

/// Topology of the task graph, in layer order.
type Topology = Vec<Layer>;

/// # Returns
///
/// The topology of a random graph. Each layer contains:
///
/// * An activation drawn from [`ACTIVATIONS`],
/// * An optional wiring:
///   * [`None`] for layer 0.
///   * Randomly drawn wiring for other layers, with guarantee that all previous layer's neuron has
///     at least one output wired to this layer, so it won't become job output.
///
/// # Panics
///
/// Panics if [`ACTIVATIONS`] is empty.
fn generate_topology(level: usize, width: usize, rng: &mut StdRng) -> Topology {
    let mut topology = Vec::with_capacity(level);
    for layer in 0..level {
        let activation = *ACTIVATIONS.choose(rng).expect("`ACTIVATIONS` is non-empty");
        let wiring = if layer == 0 {
            None
        } else {
            Some(
                (0..width)
                    .map(|i| {
                        // Force previous-layer output `i` into neuron `i`'s fan-in so every
                        // previous-layer task feeds at least one next-layer neuron; otherwise an
                        // unreferenced intermediate task would surface as a job output.
                        let mut sources: Vec<usize> =
                            index::sample(rng, width, NUM_INPUTS).into_iter().collect();
                        if !sources.contains(&i) {
                            sources[rng.random_range(0..NUM_INPUTS)] = i;
                        }
                        sources
                    })
                    .collect(),
            )
        };
        topology.push(Layer { activation, wiring });
    }
    topology
}

/// # Returns
///
/// A task graph following the `topology`.
///
/// # Errors
///
/// Forwards [`TaskGraph::new`]'s return values on failure.
/// Forwards [`TaskGraph::insert_task`]'s return values on failure.
fn build_graph(width: usize, topology: &Topology) -> anyhow::Result<TaskGraph> {
    let float64 = DataTypeDescriptor::Value(ValueTypeDescriptor::float64());
    let mut graph = TaskGraph::new(None, None)?;
    let mut prev_layer: Vec<TaskIndex> = Vec::with_capacity(width);

    // Layer 0
    for _ in 0..width {
        let task_idx = graph.insert_task(TaskDescriptor {
            tdl_context: TdlContext {
                package: PACKAGE.to_owned(),
                task_func: topology[0].activation.task_func.to_owned(),
            },
            execution_policy: None,
            inputs: vec![float64.clone(); NUM_INPUTS],
            outputs: vec![float64.clone()],
            input_sources: None,
        })?;
        prev_layer.push(task_idx);
    }

    for layer in &topology[1..] {
        let mut curr_layer = Vec::with_capacity(width);
        for task_input_sources in layer.wiring.as_ref().expect("inner layer wiring is set") {
            let input_sources: Vec<TaskInputOutputIndex> = task_input_sources
                .iter()
                .map(|&src| TaskInputOutputIndex {
                    task_idx: prev_layer[src],
                    position: 0,
                })
                .collect();
            let task_idx = graph.insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: PACKAGE.to_owned(),
                    task_func: layer.activation.task_func.to_owned(),
                },
                execution_policy: None,
                inputs: vec![float64.clone(); NUM_INPUTS],
                outputs: vec![float64.clone()],
                input_sources: Some(input_sources),
            })?;
            curr_layer.push(task_idx);
        }
        prev_layer = curr_layer;
    }

    Ok(graph)
}

/// # Returns
///
/// Randomly-generated task graph inputs.
///
/// # Panics
///
/// Panics if `width * NUM_INPUTS` overflows `usize`.
fn generate_graph_inputs(width: usize, rng: &mut StdRng) -> Vec<f64> {
    let count = width
        .checked_mul(NUM_INPUTS)
        .expect("number of graph inputs overflow");
    (0..count).map(|_| rng.random::<f64>()).collect()
}

/// Executes the neural network.
///
/// # Returns
///
/// The neural network outputs.
fn simulate(width: usize, topology: &Topology, inputs: &[f64]) -> Vec<f64> {
    let mut layer_outputs: Vec<f64> = (0..width)
        .map(|i| {
            let start = i * NUM_INPUTS;
            let mut neuron_inputs = [0.0_f64; NUM_INPUTS];
            neuron_inputs.copy_from_slice(&inputs[start..start + NUM_INPUTS]);
            (topology[0].activation.evaluate)(&neuron_inputs)
        })
        .collect();

    for layer in &topology[1..] {
        let wiring = layer.wiring.as_ref().expect("inner layer wiring is set");
        layer_outputs = wiring
            .iter()
            .map(|sources| {
                let neuron_inputs: [f64; NUM_INPUTS] =
                    std::array::from_fn(|k| layer_outputs[sources[k]]);
                (layer.activation.evaluate)(&neuron_inputs)
            })
            .collect();
    }

    layer_outputs
}

/// # Returns
///
/// The msgpack-encoded graph inputs on success.
///
/// # Errors
///
/// Forwards [`rmp_serde::to_vec`]'s return values on failure.
fn encode_graph_inputs(graph_inputs: &[f64]) -> anyhow::Result<Vec<TaskInput>> {
    graph_inputs
        .iter()
        .map(|value| {
            Ok::<TaskInput, anyhow::Error>(TaskInput::ValuePayload(rmp_serde::to_vec(value)?))
        })
        .collect()
}

/// # Returns
///
/// Randomly-generated 32-bytes password.
fn generate_password(rng: &mut (impl Rng + CryptoRng)) -> Vec<u8> {
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes[..]);
    bytes.to_vec()
}

/// Periodically polls the job state until it reaches a terminal state.
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

/// # Returns
///
/// The decoded job outputs.
///
/// # Errors
///
/// Forwards [`SpiderClient::get_job_outputs`]'s return values on failure.
/// Forwards [`rmp_serde::from_slice`]'s return values on failure.
async fn fetch_outputs(client: &SpiderClient, job_id: JobId) -> anyhow::Result<Vec<f64>> {
    let outputs = client
        .get_job_outputs(job_id)
        .await
        .context("get_job_outputs")?;
    outputs
        .iter()
        .enumerate()
        .map(|(i, output)| {
            rmp_serde::from_slice(output).with_context(|| format!("failed to decode output {i}"))
        })
        .collect()
}

/// Checks each output against the expected value, and logs each mismatch.
///
/// # Returns
///
/// `Ok(())` on success and all outputs match.
///
/// # Errors
///
/// Returns an error if:
///
/// * The output count differs from the expected count.
/// * One or more outputs mismatch the expected value within tolerance.
fn verify_outputs(outputs: &[f64], expected: &[f64]) -> anyhow::Result<()> {
    anyhow::ensure!(
        outputs.len() == expected.len(),
        "Expected {} graph outputs, got {}",
        expected.len(),
        outputs.len()
    );
    let mut mismatches = 0;
    for (i, (&got, &exp)) in outputs.iter().zip(expected.iter()).enumerate() {
        let diff = (got - exp).abs();
        let tol = 1.0e-12_f64 * (1.0 + exp.abs());
        if !got.is_finite() || !exp.is_finite() || diff > tol {
            mismatches += 1;
            tracing::warn!(
                output_index = i,
                got,
                expected = exp,
                "Output mismatched the simulation."
            );
        }
    }
    if mismatches == 0 {
        tracing::info!(count = outputs.len(), "All outputs match the simulation.");
        return Ok(());
    }
    Err(anyhow!("{mismatches}/{} wrong output", outputs.len()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _log_guard = set_up_logging();
    let cli = Cli::parse();
    if cli.level == 0 {
        return Err(anyhow!("level must be >= 1"));
    }
    if cli.width < NUM_INPUTS {
        return Err(anyhow!("width must be >= {NUM_INPUTS} (the neuron fan-in)"));
    }
    let pool_size = NonZeroUsize::new(cli.grpc_pool_size).context("grpc-pool-size must be >= 1")?;

    let endpoint: Endpoint = cli
        .endpoint
        .parse()
        .with_context(|| format!("invalid endpoint {:?}", cli.endpoint))?;
    let client = SpiderClient::connect(endpoint, pool_size).await?;

    let seed = cli.seed.unwrap_or_else(rand::random::<u64>);
    tracing::info!(seed, "Seeded the topology RNG.");
    let mut rng = StdRng::seed_from_u64(seed);

    let topology = generate_topology(cli.level, cli.width, &mut rng);

    let graph_inputs = generate_graph_inputs(cli.width, &mut rng);
    let expected = simulate(cli.width, &topology, &graph_inputs);
    let graph = build_graph(cli.width, &topology)?;
    let task_inputs = encode_graph_inputs(&graph_inputs)?;

    let mut entropy_rng = rand::rng();
    let resource_group_id = client
        .add_resource_group(
            format!("huntsman-nn-{:x}", entropy_rng.random::<u128>()),
            generate_password(&mut entropy_rng),
        )
        .await
        .context("add_resource_group")?;

    tracing::info!(
        level = cli.level,
        width = cli.width,
        tasks = cli.level * cli.width,
        "Submitting job.",
    );
    let job_id = client
        .submit_job(resource_group_id, &graph, task_inputs)
        .await
        .context("submit_job")?;
    tracing::info!(job_id = job_id.get(), "Starting job.");
    client.start_job(job_id).await.context("start_job")?;

    let state = poll_until_terminal(&client, job_id).await?;
    match state {
        JobState::Succeeded => {
            let outputs = fetch_outputs(&client, job_id).await?;
            tracing::info!(count = outputs.len(), "Fetched job outputs.");
            verify_outputs(&outputs, &expected)
        }
        JobState::Failed => {
            let message = client
                .get_job_error(job_id)
                .await
                .context("get_job_error")?;
            Err(anyhow!("job failed: {message}"))
        }
        JobState::Cancelled => Err(anyhow!("job cancelled")),
        other => Err(anyhow!("job ended in unexpected state {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inner_layer_wiring_covers_previous_layer() {
        let mut rng = StdRng::seed_from_u64(0);
        let width = 50;
        let topology = generate_topology(5, width, &mut rng);
        for layer in &topology[1..] {
            let wiring = layer.wiring.as_ref().expect("inner layer wiring is set");
            let mut covered = vec![false; width];
            for sources in wiring {
                assert_eq!(sources.len(), NUM_INPUTS);
                for &src in sources {
                    covered[src] = true;
                }
            }
            assert!(
                covered.iter().all(|&c| c),
                "uncovered previous-layer output"
            );
        }
    }
}
