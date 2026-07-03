//! Spider client that runs a multi-worker CLP search across a directory of CLP archives.
//!
//! The client discovers every immediate subdirectory of `--input` as one CLP archive, then builds
//! a FLAT Spider task graph with one `clp_search::search` task per archive. All tasks are
//! independent (no data-flow dependencies), so the scheduler is free to run them in parallel across
//! every available execution manager for maximum throughput.
//!
//! Each task writes its per-archive matches to a unique output file under a per-run output
//! directory. After the job succeeds, the client concatenates those files to STDOUT (the search
//! results); all progress and the final end-to-end latency summary are written to STDERR.

use std::{
    fs,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow};
use clap::Parser;
use spider_client::SpiderClient;
use spider_core::{
    job::JobState,
    task::{DataTypeDescriptor, TaskDescriptor, TaskGraph, TdlContext, ValueTypeDescriptor},
    types::{id::JobId, io::TaskInput},
};
use tonic::transport::Endpoint;

/// TDL package and task function each search task drives.
const PACKAGE: &str = "clp_search";
const TASK_FUNC: &str = "clp_search::search";

/// Password used when registering the per-run resource group.
const RESOURCE_GROUP_PASSWORD: &[u8] = b"huntsman-clp-search-client";

/// Command-line arguments for the CLP search client.
#[derive(Debug, Parser)]
#[command(about = "Run a multi-worker CLP search over a directory of CLP archives via Spider.")]
struct Cli {
    /// Directory whose immediate subdirectories are each a CLP archive to search.
    #[arg(long, value_name = "PATH")]
    input: PathBuf,

    /// KQL search query to run against every archive (e.g. `*NonDFS*`).
    #[arg(long, value_name = "STRING")]
    query: String,

    /// Spider storage gRPC endpoint to connect to.
    #[arg(long, value_name = "URL", default_value = "http://127.0.0.1:50051")]
    endpoint: String,

    /// `SpiderClient` gRPC connection pool size.
    #[arg(long, default_value_t = 4)]
    pool_size: usize,

    /// Base directory under which this run creates a unique subdirectory to hold its outputs.
    #[arg(
        long,
        value_name = "PATH",
        default_value = "build/spider-run/clp-search-results"
    )]
    output_dir: PathBuf,
}

/// Discovers the CLP archives directly under `input`.
///
/// Every immediate subdirectory of `input` is treated as one CLP archive. The returned paths are
/// absolute (canonicalized) and sorted by directory name so the task ordering is deterministic.
///
/// # Returns
///
/// The absolute archive directory paths, sorted by name, on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * No archive subdirectory is found under `input`.
/// * Forwards [`fs::read_dir`]'s return values on failure.
/// * Forwards [`fs::canonicalize`]'s return values on failure.
fn discover_archives(input: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let entries = fs::read_dir(input)
        .with_context(|| format!("failed to read --input {}", input.display()))?;
    let mut archives = Vec::new();
    for entry in entries {
        let entry = entry.context("failed to read a directory entry under --input")?;
        let path = entry.path();
        if path.is_dir() {
            let absolute = fs::canonicalize(&path).with_context(|| {
                format!("failed to canonicalize archive path {}", path.display())
            })?;
            archives.push(absolute);
        }
    }
    if archives.is_empty() {
        return Err(anyhow!(
            "no archive subdirectory found under --input {}",
            input.display()
        ));
    }
    archives.sort();
    Ok(archives)
}

/// Builds a flat task graph with one independent `clp_search::search` task per archive.
///
/// Every task has three graph inputs (archive path, query, output path) and no outputs, so all
/// tasks are input tasks with no dependencies between them.
///
/// # Returns
///
/// The assembled task graph on success.
///
/// # Errors
///
/// Forwards [`TaskGraph::new`]'s return values on failure.
/// Forwards [`TaskGraph::insert_task`]'s return values on failure.
fn build_graph(num_tasks: usize) -> anyhow::Result<TaskGraph> {
    let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
    let tdl_context = TdlContext {
        package: PACKAGE.to_owned(),
        task_func: TASK_FUNC.to_owned(),
    };

    let mut graph = TaskGraph::new(None, None)?;
    for _ in 0..num_tasks {
        graph.insert_task(TaskDescriptor {
            tdl_context: tdl_context.clone(),
            execution_policy: None,
            inputs: vec![bytes_type.clone(); 3],
            outputs: vec![],
            input_sources: None,
        })?;
    }
    Ok(graph)
}

/// Serializes a string value into a msgpack task-input payload.
///
/// # Returns
///
/// The [`TaskInput::ValuePayload`] carrying the msgpack-encoded string on success.
///
/// # Errors
///
/// Forwards [`rmp_serde::to_vec`]'s return values on failure.
fn value_input(value: &str) -> anyhow::Result<TaskInput> {
    let payload = rmp_serde::to_vec(value).context("failed to serialize a task input")?;
    Ok(TaskInput::ValuePayload(payload))
}

/// Prepares the per-run outputs, the flat task graph, and the flattened task inputs.
///
/// Creates the unique run output directory `<output_dir>/run-<nanos>`, assigns each archive a
/// unique output file path under it, builds the flat `clp_search::search` task graph, and flattens
/// the graph inputs so that, for archive `i`, positions 0,1,2 are the archive path, the query, and
/// the output path -- matching the order of [`TaskGraph::get_task_graph_input_indices`].
///
/// # Returns
///
/// A tuple of the per-task absolute output paths, the assembled task graph, and the flattened task
/// inputs on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * An archive path has no final path component.
/// * An archive path or a computed output path is not valid UTF-8.
/// * Forwards [`fs::create_dir_all`]'s return values on failure.
/// * Forwards [`fs::canonicalize`]'s return values on failure.
/// * Forwards [`build_graph`]'s return values on failure.
/// * Forwards [`value_input`]'s return values on failure.
fn prepare_job(
    archives: &[PathBuf],
    query: &str,
    output_dir: &Path,
    nanos: u128,
) -> anyhow::Result<(Vec<PathBuf>, TaskGraph, Vec<TaskInput>)> {
    let run_dir = output_dir.join(format!("run-{nanos}"));
    fs::create_dir_all(&run_dir).with_context(|| {
        format!(
            "failed to create run output directory {}",
            run_dir.display()
        )
    })?;
    let run_dir = fs::canonicalize(&run_dir).with_context(|| {
        format!(
            "failed to canonicalize run output directory {}",
            run_dir.display()
        )
    })?;

    let index_width = archives.len().to_string().len();
    let mut output_paths = Vec::with_capacity(archives.len());
    for (i, archive) in archives.iter().enumerate() {
        let archive_name = archive
            .file_name()
            .context("archive path has no final component")?
            .to_string_lossy();
        output_paths.push(run_dir.join(format!("{i:0index_width$}-{archive_name}.jsonl")));
    }

    let graph = build_graph(archives.len())?;

    let mut task_inputs: Vec<TaskInput> = Vec::with_capacity(archives.len() * 3);
    for (archive, output_path) in archives.iter().zip(output_paths.iter()) {
        let archive_str = archive
            .to_str()
            .context("archive path is not valid UTF-8")?;
        let output_str = output_path
            .to_str()
            .context("output path is not valid UTF-8")?;
        task_inputs.push(value_input(archive_str)?);
        task_inputs.push(value_input(query)?);
        task_inputs.push(value_input(output_str)?);
    }

    Ok((output_paths, graph, task_inputs))
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
        tokio::time::sleep(Duration::from_millis(10)).await;
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

/// End-to-end per-phase timings for a single CLP search run.
struct PhaseTimings {
    discovery: Duration,
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
    /// `query_processing` aggregates the discovery, graph/input construction, connection, and
    /// job-submission phases (everything before the distributed execution begins).
    fn print(&self) {
        let query_processing = self.discovery
            + self.graph_and_inputs
            + self.connect_and_resource_group
            + self.submit_and_start;
        print_timing("discovery", self.discovery);
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let start = Instant::now();

    let pool_size = NonZeroUsize::new(cli.pool_size).context("--pool-size must be >= 1")?;

    let phase_start = Instant::now();
    let archives = discover_archives(&cli.input)?;
    let discovery_duration = phase_start.elapsed();

    // Use a unique run id per run so repeated runs do not collide, both for the output directory
    // and for the resource-group external id (checked against a persistent MariaDB).
    let phase_start = Instant::now();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before UNIX epoch")?
        .as_nanos();
    let (output_paths, graph, task_inputs) =
        prepare_job(&archives, &cli.query, &cli.output_dir, nanos)?;
    let graph_and_inputs_duration = phase_start.elapsed();

    let phase_start = Instant::now();
    let endpoint: Endpoint = cli
        .endpoint
        .parse()
        .with_context(|| format!("invalid --endpoint {:?}", cli.endpoint))?;
    let client = SpiderClient::connect(endpoint, pool_size)
        .await
        .context("failed to connect to the Spider storage service")?;

    let resource_group_id = client
        .add_resource_group(
            format!("clp-search-{nanos}"),
            RESOURCE_GROUP_PASSWORD.to_vec(),
        )
        .await
        .context("add_resource_group")?;
    let connect_and_resource_group_duration = phase_start.elapsed();

    let phase_start = Instant::now();
    let job_id = client
        .submit_job(resource_group_id, &graph, task_inputs)
        .await
        .context("submit_job")?;
    client.start_job(job_id).await.context("start_job")?;
    let submit_and_start_duration = phase_start.elapsed();

    eprintln!(
        "Submitted CLP search job: archives={}, tasks={}, query={:?}, job_id={}",
        archives.len(),
        archives.len(),
        cli.query,
        job_id.get()
    );

    let phase_start = Instant::now();
    let state = poll_until_terminal(&client, job_id).await?;
    let spider_execution_duration = phase_start.elapsed();

    let phase_start = Instant::now();
    match state {
        JobState::Succeeded => {
            for output_path in &output_paths {
                let contents = fs::read_to_string(output_path).with_context(|| {
                    format!("failed to read output file {}", output_path.display())
                })?;
                print!("{contents}");
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
    let post_processing_duration = phase_start.elapsed();

    PhaseTimings {
        discovery: discovery_duration,
        graph_and_inputs: graph_and_inputs_duration,
        connect_and_resource_group: connect_and_resource_group_duration,
        submit_and_start: submit_and_start_duration,
        spider_execution: spider_execution_duration,
        post_processing: post_processing_duration,
        total: start.elapsed(),
    }
    .print();

    eprintln!(
        "Job succeeded: archives={}, tasks={}, job_id={}, elapsed={:.3?}",
        archives.len(),
        archives.len(),
        job_id.get(),
        start.elapsed()
    );

    Ok(())
}
