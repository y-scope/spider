//! Local process-pool reference binary that runs the SAME CLP search workload as the Spider
//! client, but WITHOUT Spider.
//!
//! Instead of submitting a Spider job, this binary drives one `clp-s` process per discovered CLP
//! archive through a bounded local pool that keeps at most `--pool-size` processes running
//! concurrently. It exists to establish a baseline end-to-end latency so Spider's
//! scheduling/coordination overhead can be isolated by comparing the two.
//!
//! Archive discovery, the per-run output directory, the per-archive output-file naming, the
//! `clp-s` invocation, and the phase-timing labels are all mirrored from the Spider client so the
//! two runs are directly comparable. All search results are written to STDOUT; all progress and the
//! per-phase timing breakdown are written to STDERR.

use std::{
    env,
    fs,
    fs::File,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow};
use clap::Parser;
use rand::seq::SliceRandom;
use tokio::{process::Command, sync::Semaphore, task::JoinSet};

/// Environment variable that overrides the `clp-s` binary path.
const CLP_S_BIN_ENV: &str = "CLP_S_BIN";

/// Default `clp-s` binary path used when [`CLP_S_BIN_ENV`] is unset.
const DEFAULT_CLP_S_BIN: &str = "/home/lzh/dev/clp/build/core/clp-s";

/// Command-line arguments for the CLP search process-pool reference binary.
#[derive(Debug, Parser)]
#[command(
    about = "Run a CLP search over a directory of CLP archives via a local clp-s process pool (no \
             Spider)."
)]
struct Cli {
    /// Directory whose immediate subdirectories are each a CLP archive to search.
    #[arg(long, value_name = "PATH")]
    input: PathBuf,

    /// KQL search query to run against every archive (e.g. `*NonDFS*`).
    #[arg(long, value_name = "STRING")]
    query: String,

    /// Base directory under which this run creates a unique subdirectory to hold its outputs.
    #[arg(
        long,
        value_name = "PATH",
        default_value = "build/spider-run/clp-search-pool-results"
    )]
    output_dir: PathBuf,

    /// Maximum number of `clp-s` processes running concurrently.
    #[arg(long, default_value_t = 16)]
    pool_size: usize,
}

/// Discovers the CLP archives directly under `input`.
///
/// Every immediate subdirectory of `input` is treated as one CLP archive. The returned paths are
/// absolute (canonicalized) and sorted by directory name so the run ordering is deterministic.
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

/// Prepares the per-run output directory and assigns each archive a unique output-file path.
///
/// Creates the unique run output directory `<output_dir>/run-<nanos>` and, for archive `i`, assigns
/// the absolute output path `<run_dir>/<zero-padded-i>-<archive_name>.jsonl`. This is the local
/// analog of the Spider client's graph/input construction, so it is timed under the same
/// `graph_and_inputs` label.
///
/// # Returns
///
/// The per-archive absolute output paths, in archive order, on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * An archive path has no final path component.
/// * Forwards [`fs::create_dir_all`]'s return values on failure.
/// * Forwards [`fs::canonicalize`]'s return values on failure.
fn prepare_outputs(
    archives: &[PathBuf],
    output_dir: &Path,
    nanos: u128,
) -> anyhow::Result<Vec<PathBuf>> {
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
    Ok(output_paths)
}

/// Runs the KQL `query` over the CLP archive at `archive_path`, writing the matching records as
/// JSONL to `output_path`.
///
/// Invokes `clp-s` (resolved from `clp_s_bin`) as `clp-s s <archive_path> <query>`, redirects its
/// stdout into the freshly truncated file at `output_path`, and captures its stderr so a failing
/// archive can be reported. This mirrors the Spider TDL search task exactly.
///
/// # Errors
///
/// Returns an error if:
///
/// # Returns
///
/// The wall-clock duration of the `clp-s` subprocess on success (for execution-time analysis).
///
/// # Errors
///
/// Returns an error if:
///
/// * The output file cannot be created.
/// * The `clp-s` process cannot be spawned or waited on.
/// * The `clp-s` process exits with a non-success status.
async fn search_archive(
    clp_s_bin: &str,
    archive_path: &Path,
    query: &str,
    output_path: &Path,
) -> anyhow::Result<Duration> {
    let output_file = File::create(output_path).with_context(|| {
        format!(
            "failed to create output file `{}` for archive `{}`",
            output_path.display(),
            archive_path.display()
        )
    })?;

    // Time only the `clp-s` subprocess, matching the Spider task's `clp_s_elapsed_us` metric.
    let clp_s_start = Instant::now();
    let child = Command::new(clp_s_bin)
        .arg("s")
        .arg(archive_path)
        .arg(query)
        .stdout(Stdio::from(output_file))
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| {
            format!(
                "failed to spawn `{clp_s_bin}` for archive `{}`",
                archive_path.display()
            )
        })?;

    let output = child.wait_with_output().await.with_context(|| {
        format!(
            "failed to wait on `{clp_s_bin}` for archive `{}`",
            archive_path.display()
        )
    })?;
    let clp_s_elapsed = clp_s_start.elapsed();

    if output.status.success() {
        return Ok(clp_s_elapsed);
    }

    Err(anyhow!(
        "`clp-s` failed for archive `{}` with status {}: {}",
        archive_path.display(),
        output.status,
        String::from_utf8_lossy(&output.stderr),
    ))
}

/// Runs one `clp-s` search per archive through a bounded local process pool.
///
/// A [`Semaphore`] with `pool_size` permits caps the number of `clp-s` processes running at once:
/// every spawned task must acquire a permit before it spawns its child process and releases it when
/// the child completes. All archives are spawned up front and awaited to completion; the failures
/// of all archives are collected so a single failed archive does not abort the others (mirroring
/// how a failed Spider job is only reported once all tasks settle).
///
/// # Returns
///
/// The per-archive `clp-s` subprocess durations (in completion order) on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Any archive's `clp-s` process fails to spawn or exits with a non-success status.
/// * A search task panics.
async fn run_pool(
    clp_s_bin: String,
    query: String,
    archives: &[PathBuf],
    output_paths: &[PathBuf],
    pool_size: usize,
) -> anyhow::Result<Vec<Duration>> {
    let semaphore = Arc::new(Semaphore::new(pool_size));
    let clp_s_bin = Arc::new(clp_s_bin);
    let query = Arc::new(query);

    let mut join_set = JoinSet::new();
    for (archive, output_path) in archives.iter().zip(output_paths.iter()) {
        let permit_source = Arc::clone(&semaphore);
        let clp_s_bin = Arc::clone(&clp_s_bin);
        let query = Arc::clone(&query);
        let archive = archive.clone();
        let output_path = output_path.clone();
        join_set.spawn(async move {
            let _permit = permit_source
                .acquire_owned()
                .await
                .expect("pool semaphore closed unexpectedly");
            search_archive(&clp_s_bin, &archive, &query, &output_path).await
        });
    }

    let mut failures = Vec::new();
    let mut clp_s_times = Vec::with_capacity(archives.len());
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(Ok(elapsed)) => clp_s_times.push(elapsed),
            Ok(Err(error)) => failures.push(format!("{error:#}")),
            Err(join_error) => failures.push(format!("a search task panicked: {join_error}")),
        }
    }

    if failures.is_empty() {
        return Ok(clp_s_times);
    }

    for failure in &failures {
        eprintln!("clp-s failure: {failure}");
    }
    Err(anyhow!(
        "the pool run failed: {} archive(s) failed",
        failures.len()
    ))
}

/// Prints summary statistics of the per-archive `clp-s` execution times to STDERR.
///
/// Reports count, total, mean, median, min, max, and p95 (all in milliseconds) so the pool's
/// `clp-s` execution distribution can be compared against Spider's `clp_s_elapsed_us` metric.
fn print_clp_s_stats(times: &[Duration]) {
    if times.is_empty() {
        return;
    }
    let mut ms: Vec<f64> = times.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
    ms.sort_by(f64::total_cmp);
    let n = ms.len();
    let sum: f64 = ms.iter().sum();
    let mean = sum / f64::from(u32::try_from(n).unwrap_or(u32::MAX));
    eprintln!(
        "[clp_s] n={n} sum={sum:.1}ms mean={mean:.3}ms median={:.3}ms min={:.3}ms max={:.3}ms \
         p95={:.3}ms",
        ms[n / 2],
        ms[0],
        ms[n - 1],
        ms[(n * 95) / 100],
    );
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

/// End-to-end per-phase timings for a single CLP search pool run.
struct PhaseTimings {
    discovery: Duration,
    graph_and_inputs: Duration,
    spider_execution: Duration,
    post_processing: Duration,
    total: Duration,
}

impl PhaseTimings {
    /// Prints the per-phase breakdown followed by the three headline rollups and the total to
    /// STDERR.
    ///
    /// `query_processing` aggregates the discovery and output-preparation phases (everything before
    /// the pool execution begins), matching the Spider client's rollup layout.
    fn print(&self) {
        let query_processing = self.discovery + self.graph_and_inputs;
        print_timing("discovery", self.discovery);
        print_timing("graph_and_inputs", self.graph_and_inputs);
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

    if cli.pool_size == 0 {
        return Err(anyhow!("--pool-size must be >= 1"));
    }

    let phase_start = Instant::now();
    let mut archives = discover_archives(&cli.input)?;
    // Randomize archive order so heavy archives are spread across the pool instead of clustering
    // (matches the Spider client's shuffle, keeping the two comparable).
    archives.shuffle(&mut rand::rng());
    let discovery_duration = phase_start.elapsed();

    let phase_start = Instant::now();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before UNIX epoch")?
        .as_nanos();
    let output_paths = prepare_outputs(&archives, &cli.output_dir, nanos)?;
    let graph_and_inputs_duration = phase_start.elapsed();

    let clp_s_bin = env::var(CLP_S_BIN_ENV).unwrap_or_else(|_| DEFAULT_CLP_S_BIN.to_owned());

    eprintln!(
        "Starting CLP search pool run: archives={}, pool_size={}, query={:?}, clp_s_bin={:?}",
        archives.len(),
        cli.pool_size,
        cli.query,
        clp_s_bin
    );

    let phase_start = Instant::now();
    let clp_s_times = run_pool(
        clp_s_bin,
        cli.query.clone(),
        &archives,
        &output_paths,
        cli.pool_size,
    )
    .await?;
    let spider_execution_duration = phase_start.elapsed();
    print_clp_s_stats(&clp_s_times);

    let phase_start = Instant::now();
    for output_path in &output_paths {
        let contents = fs::read_to_string(output_path)
            .with_context(|| format!("failed to read output file {}", output_path.display()))?;
        print!("{contents}");
    }
    let post_processing_duration = phase_start.elapsed();

    PhaseTimings {
        discovery: discovery_duration,
        graph_and_inputs: graph_and_inputs_duration,
        spider_execution: spider_execution_duration,
        post_processing: post_processing_duration,
        total: start.elapsed(),
    }
    .print();

    eprintln!(
        "Pool run succeeded: archives={}, pool_size={}, elapsed={:.3?}",
        archives.len(),
        cli.pool_size,
        start.elapsed()
    );

    Ok(())
}
