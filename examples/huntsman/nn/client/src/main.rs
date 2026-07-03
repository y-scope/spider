//! Neural-network-shaped benchmark client for the Spider stack.
//!
//! Builds a layered task graph that mimics a neural network -- `--level` layers of `--width` tasks
//! each -- where every task consumes 25 128-byte inputs randomly selected from the previous
//! layer's outputs (layer 0 takes 25 graph inputs per task), sleeps 10 ms inside the
//! `nn_bench::sleep` task, and emits a fixed 128-byte output. The default shape is
//! `--level 10 --width 1000` => 10,000 tasks. The 25 inputs per task is fixed by the task's
//! signature (`nn_bench::sleep` takes 25 positional inputs), not a tunable.
//!
//! After the job finishes the client decodes the final layer's outputs and checks each against the
//! fixed 128-byte payload the task emits, proving every task ran. It reports a per-phase timing
//! breakdown to STDERR (the `[timing]` convention: sub-phases plus `query_processing` /
//! `spider_execution` / `post_processing` / `total` rollups) and, since the task cost is a fixed
//! simulated sleep, an **ideal runtime** lower bound computed from the generated graph shape for
//! 16, 32, 64, and 128 workers -- the floor to compare Spider's actual `spider_execution` against.
//!
//! Per-task START/END timestamps are written by the task itself to stderr, which the execution
//! manager captures into `build/spider-run/em-logs/<em_id>-<executor_id>.log`.

use std::{
    collections::HashSet,
    num::NonZeroUsize,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow};
use clap::Parser;
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

/// TDL package and task function the graph drives. The task sleeps 10 ms and emits a fixed
/// 128-byte payload.
const PACKAGE: &str = "nn_bench";
const TASK_FUNC: &str = "nn_bench::sleep";

/// Fixed 128-byte payload every `nn_bench::sleep` invocation emits; the client checks every
/// final-layer output against this.
const OUTPUT_PAYLOAD: [u8; 128] = [0; 128];

/// Number of positional inputs each `nn_bench::sleep` task consumes. Mirrors the task's `i0..i24`
/// parameter list (in `examples/huntsman/nn/tasks/src/lib.rs`); keep the two in sync. The graph
/// declares this many inputs per task, and the runtime maps them positionally onto the task's
/// parameters, so this is a fixed property of the task -- not a tunable.
const NUM_TASK_INPUT: usize = 25;

/// Per-task compute cost in milliseconds. Mirrors `nn_bench::sleep`'s `SLEEP_DURATION` (in
/// `examples/huntsman/nn/tasks/src/lib.rs`); keep the two in sync. Used to compute the ideal
/// runtime lower bound.
const TASK_DURATION_MS: f64 = 10.0;

/// Worker counts for which the ideal runtime lower bound is reported, alongside the measured
/// `spider_execution`. The client does not know the live stack's worker count, so it reports a
/// fixed reference set.
const IDEAL_WORKER_COUNTS: [usize; 4] = [16, 32, 64, 128];

/// Password used when registering the per-run resource group.
const RESOURCE_GROUP_PASSWORD: &[u8] = b"huntsman-nn-bench";

/// Command-line arguments for the benchmark client.
#[derive(Debug, Parser)]
#[command(
    about = "Build a neural-network-shaped nn_bench::sleep task graph and benchmark the Spider \
             stack."
)]
struct Cli {
    /// Spider storage gRPC endpoint to connect to.
    #[arg(long, value_name = "URL", default_value = "http://127.0.0.1:50051")]
    endpoint: String,

    /// Number of layers (graph depth).
    #[arg(long, default_value_t = 10)]
    level: usize,

    /// Tasks per layer. Tasks within a layer are independent, so this controls the parallelism the
    /// scheduler can exploit (e.g. `--width 1000` against a 32-worker stack). Must be >= the
    /// task's input count (`NUM_TASK_INPUT`) so each task can draw distinct previous-layer
    /// outputs.
    #[arg(long, default_value_t = 1000)]
    width: usize,

    /// Size in bytes of each input and of each task's output (the task emits a fixed payload of
    /// this size).
    #[arg(long, default_value_t = 128)]
    input_bytes: usize,

    /// Seed for the random input-selection topology (deterministic across runs).
    #[arg(long, default_value_t = 0x517_d3ad)]
    seed: u64,

    /// `SpiderClient` gRPC connection pool size.
    #[arg(long, default_value_t = 4)]
    pool_size: usize,

    /// Print each final-layer output alongside the expected payload, for inspection (to STDOUT).
    #[arg(long)]
    print_outputs: bool,
}

/// A tiny xorshift64 RNG seeded by `--seed` so the random input-selection topology is
/// deterministic.
struct Rng(u64);

impl Rng {
    const fn new(seed: u64) -> Self {
        // Avoid a zero seed, which would xorshift-stick at 0.
        Self(if seed == 0 {
            0x9e37_79b9_7f4a_7c15
        } else {
            seed
        })
    }

    const fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Samples `count` distinct task indices in `[0, width)` using rejection sampling.
///
/// # Panics
///
/// Panics if `count > width` (cannot pick that many distinct indices).
fn sample_distinct(rng: &mut Rng, count: usize, width: usize) -> Vec<usize> {
    assert!(
        count <= width,
        "count ({count}) must be <= width ({width}) for distinct sampling"
    );
    let width_u64 = u64::try_from(width).expect("width fits in u64");
    let mut picked: HashSet<usize> = HashSet::with_capacity(count);
    while picked.len() < count {
        let idx = usize::try_from(rng.next_u64() % width_u64).expect("index fits in usize");
        picked.insert(idx);
    }
    picked.into_iter().collect()
}

/// Structural shape of the generated graph, used to compute the ideal runtime lower bound.
struct GraphShape {
    /// Total number of tasks in the graph.
    total_tasks: usize,
    /// Graph depth in tasks -- the longest dependency chain. Every inner-layer task depends on a
    /// previous-layer task, so a chain spans all `level` layers; the chain length equals the layer
    /// count.
    depth: usize,
}

/// Builds the layered `nn_bench::sleep` task graph.
///
/// Layer 0 has `width` tasks whose `NUM_TASK_INPUT` inputs come from the graph inputs. Each
/// subsequent layer has `width` tasks whose `NUM_TASK_INPUT` inputs are distinct random outputs
/// from the previous layer, drawn from a seeded RNG so the topology is reproducible.
///
/// # Returns
///
/// A tuple of the assembled task graph and its [`GraphShape`] (task count + depth, derived from
/// the build so the ideal-runtime calculation tracks the graph that was actually generated).
///
/// # Errors
///
/// Forwards [`TaskGraph::new`]'s return values on failure.
/// Forwards [`TaskGraph::insert_task`]'s return values on failure.
///
/// # Panics
///
/// Panics (via [`sample_distinct`]) if `width < NUM_TASK_INPUT`, since distinct sampling needs at
/// least that many previous-layer outputs to choose from.
fn build_graph(level: usize, width: usize, seed: u64) -> anyhow::Result<(TaskGraph, GraphShape)> {
    let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
    let tdl_context = TdlContext {
        package: PACKAGE.to_owned(),
        task_func: TASK_FUNC.to_owned(),
    };

    let mut graph = TaskGraph::new(None, None)?;
    let mut total_tasks = 0usize;

    // Layer 0: all inputs come from the graph inputs.
    let mut prev_layer: Vec<TaskIndex> = Vec::with_capacity(width);
    for _ in 0..width {
        let task_idx = graph.insert_task(TaskDescriptor {
            tdl_context: tdl_context.clone(),
            execution_policy: None,
            inputs: vec![bytes_type.clone(); NUM_TASK_INPUT],
            outputs: vec![bytes_type.clone()],
            input_sources: None,
        })?;
        prev_layer.push(task_idx);
        total_tasks += 1;
    }

    // Inner layers: `NUM_TASK_INPUT` distinct random outputs from the previous layer per task.
    let mut rng = Rng::new(seed);
    for _ in 1..level {
        let mut cur_layer = Vec::with_capacity(width);
        for _ in 0..width {
            let sources: Vec<TaskInputOutputIndex> =
                sample_distinct(&mut rng, NUM_TASK_INPUT, width)
                    .into_iter()
                    .map(|src| TaskInputOutputIndex {
                        task_idx: prev_layer[src],
                        position: 0,
                    })
                    .collect();
            let task_idx = graph.insert_task(TaskDescriptor {
                tdl_context: tdl_context.clone(),
                execution_policy: None,
                inputs: vec![bytes_type.clone(); NUM_TASK_INPUT],
                outputs: vec![bytes_type.clone()],
                input_sources: Some(sources),
            })?;
            cur_layer.push(task_idx);
            total_tasks += 1;
        }
        prev_layer = cur_layer;
    }

    let shape = GraphShape {
        total_tasks,
        depth: level,
    };
    Ok((graph, shape))
}

/// Builds the graph inputs: `width * NUM_TASK_INPUT` byte vectors of `input_bytes` bytes each, one
/// per positional input of the layer-0 tasks (insertion order: task `i` consumes inputs
/// `NUM_TASK_INPUT * i .. NUM_TASK_INPUT * i + NUM_TASK_INPUT`).
///
/// # Returns
///
/// The graph input byte vectors.
///
/// # Panics
///
/// Panics on arithmetic overflow if `width * NUM_TASK_INPUT` overflows `usize`.
fn build_graph_inputs(width: usize, input_bytes: usize) -> Vec<Vec<u8>> {
    let count = width
        .checked_mul(NUM_TASK_INPUT)
        .expect("width * NUM_TASK_INPUT overflow");
    vec![vec![0u8; input_bytes]; count]
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

/// Builds the graph inputs and msgpack-encodes each into a [`TaskInput`].
///
/// # Returns
///
/// The encoded task inputs on success.
///
/// # Errors
///
/// Forwards `rmp_serde::to_vec`'s return values on failure.
///
/// # Panics
///
/// Panics on arithmetic overflow if `width * NUM_TASK_INPUT` overflows `usize` (via
/// [`build_graph_inputs`]).
fn serialize_graph_inputs(width: usize, input_bytes: usize) -> anyhow::Result<Vec<TaskInput>> {
    let graph_inputs = build_graph_inputs(width, input_bytes);
    graph_inputs
        .iter()
        .map(|value| {
            Ok::<TaskInput, anyhow::Error>(TaskInput::ValuePayload(
                rmp_serde::to_vec(value).context("failed to serialize a graph input")?,
            ))
        })
        .collect()
}

/// Verifies the final-layer outputs against the fixed payload, printing each to STDOUT when
/// `print_outputs` is set.
///
/// # Errors
///
/// Forwards [`SpiderClient::get_job_outputs`]'s return values on failure.
/// Returns an error if any output mismatches the expected payload.
async fn handle_succeeded(
    client: &SpiderClient,
    job_id: JobId,
    width: usize,
    print_outputs: bool,
) -> anyhow::Result<()> {
    let outputs = client
        .get_job_outputs(job_id)
        .await
        .context("get_job_outputs")?;
    anyhow::ensure!(
        outputs.len() == width,
        "expected {width} graph outputs, got {}",
        outputs.len(),
    );

    let mut mismatches = 0;
    for (i, output) in outputs.iter().enumerate() {
        let got: Vec<u8> = rmp_serde::from_slice(output)
            .with_context(|| format!("failed to decode output {i}"))?;
        if got != OUTPUT_PAYLOAD {
            mismatches += 1;
        }
        if print_outputs {
            // Inspection output goes to STDOUT (the results stream); everything else is on STDERR.
            println!(
                "output[{i}] len={} matches_expected={}",
                got.len(),
                got == OUTPUT_PAYLOAD,
            );
        }
    }

    eprintln!("outputs: {}/{} matched", width - mismatches, width);
    if mismatches == 0 {
        Ok(())
    } else {
        Err(anyhow!(
            "job succeeded but {mismatches}/{width} outputs mismatched the expected payload"
        ))
    }
}

/// Prints a single labeled phase-timing line to STDERR, in milliseconds.
///
/// The label is left-padded so the colons of consecutive lines align.
fn print_timing(label: &str, duration: Duration) {
    eprintln!(
        "[timing] {label:<26}: {:.1} ms",
        duration.as_secs_f64() * 1000.0
    );
}

/// Prints a single labeled timing line to STDERR from a millisecond value (used for the analytic
/// ideal-runtime figures, which are not a measured `Duration`).
fn print_timing_ms(label: &str, ms: f64) {
    eprintln!("[timing] {label:<26}: {ms:.1} ms");
}

/// End-to-end per-phase timings for a single nn benchmark run.
struct PhaseTimings {
    graph_and_inputs: Duration,
    connect_and_resource_group: Duration,
    submit_and_start: Duration,
    spider_execution: Duration,
    post_processing: Duration,
    total: Duration,
}

impl PhaseTimings {
    /// Prints the per-phase breakdown followed by the three headline rollups and the total to
    /// STDERR.
    ///
    /// `query_processing` aggregates the graph/input construction, connection, and job-submission
    /// phases (everything before the distributed execution begins).
    fn print(&self) {
        let query_processing =
            self.graph_and_inputs + self.connect_and_resource_group + self.submit_and_start;
        print_timing("graph_and_inputs", self.graph_and_inputs);
        print_timing(
            "connect_and_resource_group",
            self.connect_and_resource_group,
        );
        print_timing("submit_and_start", self.submit_and_start);
        print_timing("spider_execution", self.spider_execution);
        print_timing("post_processing", self.post_processing);
        print_timing("== query_processing", query_processing);
        print_timing("== spider_execution", self.spider_execution);
        print_timing("== post_processing", self.post_processing);
        print_timing("== total", self.total);
    }
}

/// Ideal runtime lower bound (in milliseconds) for the generated graph on `workers` workers.
///
/// For a DAG of equal-duration tasks, no schedule can beat `max(critical_path, total_work / W)`:
/// the `total_work / W` term is the perfect-parallelism bound, and the `critical_path` term is the
/// longest dependency chain that must run serially.
fn ideal_runtime_ms(shape: &GraphShape, workers: usize) -> f64 {
    // Convert through `u32` so the `usize` -> `f64` widening is lossless (avoids
    // `clippy::cast_precision_loss`); the counts are bounded well below `u32::MAX`.
    let total_tasks =
        f64::from(u32::try_from(shape.total_tasks).expect("total task count fits in u32"));
    let depth = f64::from(u32::try_from(shape.depth).expect("graph depth fits in u32"));
    let workers = f64::from(u32::try_from(workers).expect("worker count fits in u32"));
    let total_work_ms = total_tasks * TASK_DURATION_MS;
    let critical_path_ms = depth * TASK_DURATION_MS;
    let work_bound_ms = total_work_ms / workers;
    critical_path_ms.max(work_bound_ms)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if cli.level == 0 || cli.width == 0 {
        return Err(anyhow!("--level and --width must be >= 1"));
    }
    if cli.width < NUM_TASK_INPUT {
        return Err(anyhow!(
            "--width ({}) must be >= {} (the task's positional input count) for distinct sampling",
            cli.width,
            NUM_TASK_INPUT,
        ));
    }
    let pool_size = NonZeroUsize::new(cli.pool_size).context("--pool-size must be >= 1")?;

    eprintln!(
        "NN benchmark: level={} width={} input_bytes={}",
        cli.level, cli.width, cli.input_bytes,
    );

    let total_start = Instant::now();

    // (a) Graph build + input serialization.
    let phase_start = Instant::now();
    let (graph, shape) = build_graph(cli.level, cli.width, cli.seed)?;
    let task_inputs = serialize_graph_inputs(cli.width, cli.input_bytes)?;
    let graph_and_inputs_duration = phase_start.elapsed();

    // (b) Connect + register a per-run resource group. Use a unique external id per run so repeated
    // runs (against a persistent MariaDB) do not collide with the resource-group-already-exists
    // error.
    let phase_start = Instant::now();
    let endpoint: Endpoint = cli
        .endpoint
        .parse()
        .with_context(|| format!("invalid --endpoint {:?}", cli.endpoint))?;
    let client = SpiderClient::connect(endpoint, pool_size)
        .await
        .context("failed to connect to the Spider storage service")?;
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
    let connect_and_resource_group_duration = phase_start.elapsed();

    // (c) submit_job + start_job.
    let phase_start = Instant::now();
    let job_id = client
        .submit_job(resource_group_id, &graph, task_inputs)
        .await
        .context("submit_job")?;
    client.start_job(job_id).await.context("start_job")?;
    let submit_and_start_duration = phase_start.elapsed();

    eprintln!(
        "Submitted nn_bench job: tasks={}, job_id={}",
        shape.total_tasks,
        job_id.get(),
    );

    // (d) start_job -> terminal: the execution wall time the benchmark cares about.
    let phase_start = Instant::now();
    let state = poll_until_terminal(&client, job_id).await?;
    let spider_execution_duration = phase_start.elapsed();

    // (e) Decode + verify the final-layer outputs.
    let phase_start = Instant::now();
    match state {
        JobState::Succeeded => {
            handle_succeeded(&client, job_id, cli.width, cli.print_outputs).await?;
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
    let post_processing_duration = phase_start.elapsed();

    PhaseTimings {
        graph_and_inputs: graph_and_inputs_duration,
        connect_and_resource_group: connect_and_resource_group_duration,
        submit_and_start: submit_and_start_duration,
        spider_execution: spider_execution_duration,
        post_processing: post_processing_duration,
        total: total_start.elapsed(),
    }
    .print();

    // Ideal runtime lower bound for the generated graph at each reference worker count. These are
    // the floors to compare the measured `spider_execution` against -- the gap is Spider's
    // scheduling/coordination overhead.
    eprintln!("[timing] == ideal (lower bound):");
    for workers in IDEAL_WORKER_COUNTS {
        print_timing_ms(
            &format!("ideal {workers} workers"),
            ideal_runtime_ms(&shape, workers),
        );
    }

    let throughput =
        f64::from(u32::try_from(shape.total_tasks).expect("total task count fits in u32"))
            / spider_execution_duration.as_secs_f64();
    eprintln!("throughput: {throughput:.1} tasks/s");
    eprintln!(
        "Job succeeded; all {} final-layer outputs match the expected payload.",
        cli.width,
    );

    Ok(())
}
