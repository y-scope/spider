//! Measures the round-trip overhead of one task execution through the `spider-task-executor`
//! binary.
//!
//! Drives the `sleep_and_echo` task — which sleeps for a known constant
//! [`INSTRUMENT_SLEEP_US`](integration_test_tasks::INSTRUMENT_SLEEP_US) and then echoes its
//! `Vec<String>` payload — against a *long-lived* executor subprocess (the FFI library is
//! cached after the first call, so subsequent dispatches measure steady-state overhead, not
//! one-time dlopen cost). With the work portion held constant we can split the cost into:
//!
//! * `e2e`: parent's wall-clock around `send(Execute)` → `recv(Response::Result)`.
//! * `executor`: the in-executor FFI duration, taken straight from
//!   [`Response::Result::elapsed_us`]. This is `INSTRUMENT_SLEEP_US` + the executor's in-FFI
//!   input/output serde.
//! * `executor_internal`: `executor - INSTRUMENT_SLEEP_US`. Approximates the in-executor
//!   input/output serde cost alone.
//! * `ipc_overhead`: `e2e - executor`. The parent-side framing + bincode + pipe traversal.
//!
//! Aggregates (avg, p50, p95, p99) for each metric land in a markdown table at
//! `${SPIDER_TEST_INSTRUMENT_OUTPUT_DIR}/task_executor_overhead.md`.

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use integration_test_tasks::INSTRUMENT_SLEEP_US;
use spider_task_executor::protocol::ExecutorOutcome;
use spider_task_executor::protocol::Response;
use tabled::Table;
use tabled::Tabled;
use test_utils::ExecutorHandle;
use test_utils::decode_single_output;
use test_utils::encode_single_input;
use test_utils::execute_request;

const PAYLOAD_LEN: usize = 100;
const ITERATIONS: usize = 10;
const OUTPUT_FILE: &str = "task_executor_overhead.md";
const INSTRUMENT_OUTPUT_DIR_ENV: &str = "SPIDER_TEST_INSTRUMENT_OUTPUT_DIR";

/// One row in the markdown table: a metric and its aggregate latency statistics.
#[derive(Tabled)]
struct LatencyRow {
    #[tabled(rename = "Metric")]
    metric: &'static str,
    #[tabled(rename = "Count")]
    count: usize,
    #[tabled(rename = "Avg (µs)")]
    avg_us: String,
    #[tabled(rename = "P50 (µs)")]
    p50_us: String,
    #[tabled(rename = "P95 (µs)")]
    p95_us: String,
    #[tabled(rename = "P99 (µs)")]
    p99_us: String,
}

impl LatencyRow {
    /// Sorts `samples` in place and computes `count`, `avg`, `p50`, `p95`, `p99` in microseconds.
    ///
    /// # Returns
    ///
    /// A populated [`LatencyRow`], or a row with `"N/A"` aggregates when `samples` is empty.
    fn from_samples(metric: &'static str, samples: &mut [Duration]) -> Self {
        if samples.is_empty() {
            return Self {
                metric,
                count: 0,
                avg_us: "N/A".to_owned(),
                p50_us: "N/A".to_owned(),
                p95_us: "N/A".to_owned(),
                p99_us: "N/A".to_owned(),
            };
        }
        samples.sort();
        let count = samples.len();
        let sum: Duration = samples.iter().sum();
        #[allow(clippy::cast_precision_loss)]
        let avg = sum.as_secs_f64() * 1_000_000.0 / count as f64;
        let last = count - 1;
        let p50 = samples[(count / 2).min(last)].as_secs_f64() * 1_000_000.0;
        let p95 = samples[(count * 95 / 100).min(last)].as_secs_f64() * 1_000_000.0;
        let p99 = samples[(count * 99 / 100).min(last)].as_secs_f64() * 1_000_000.0;
        Self {
            metric,
            count,
            avg_us: format!("{avg:.2}"),
            p50_us: format!("{p50:.2}"),
            p95_us: format!("{p95:.2}"),
            p99_us: format!("{p99:.2}"),
        }
    }
}

#[tokio::test]
#[ignore = "requires `integration-test-tasks` cdylib, `spider-task-executor` binary, and \
            SPIDER_TEST_INSTRUMENT_OUTPUT_DIR"]
async fn instrument_overhead() {
    let output_dir = std::env::var_os(INSTRUMENT_OUTPUT_DIR_ENV).map_or_else(
        || panic!("{INSTRUMENT_OUTPUT_DIR_ENV} env var not set"),
        PathBuf::from,
    );

    let mut handle = ExecutorHandle::spawn();

    let payload = path_like_payload(PAYLOAD_LEN);
    let raw_inputs = encode_single_input(&payload);
    let sleep_floor = Duration::from_micros(INSTRUMENT_SLEEP_US);

    // Warm-up: first call dlopens the package. Assert correctness; discard timing.
    handle
        .send(&execute_request("sleep_and_echo", raw_inputs.clone()))
        .await;
    expect_echo(&handle.recv().await, &payload);

    let mut e2e_samples = Vec::with_capacity(ITERATIONS);
    let mut executor_samples = Vec::with_capacity(ITERATIONS);
    let mut executor_internal_samples = Vec::with_capacity(ITERATIONS);
    let mut ipc_overhead_samples = Vec::with_capacity(ITERATIONS);

    for _ in 0..ITERATIONS {
        let started = Instant::now();
        handle
            .send(&execute_request("sleep_and_echo", raw_inputs.clone()))
            .await;
        let response = handle.recv().await;
        let e2e = started.elapsed();

        let Response::Result {
            outcome,
            elapsed_us,
        } = response;
        let ExecutorOutcome::Success { outputs } = outcome else {
            panic!("sleep_and_echo task unexpectedly failed in overhead loop");
        };
        let got: Vec<String> = decode_single_output(&outputs);
        assert_eq!(got, payload);

        let executor = Duration::from_micros(elapsed_us);
        // Defensive: a coarse system clock could in principle report e2e < executor, or executor <
        // sleep_floor (the sleep can return slightly early on some platforms). Treat both as zero
        // overhead and keep the sample for visibility.
        let executor_internal = executor.checked_sub(sleep_floor).unwrap_or(Duration::ZERO);
        let ipc_overhead = e2e.checked_sub(executor).unwrap_or(Duration::ZERO);

        e2e_samples.push(e2e);
        executor_samples.push(executor);
        executor_internal_samples.push(executor_internal);
        ipc_overhead_samples.push(ipc_overhead);
    }

    handle.shutdown_clean().await;

    let rows = vec![
        LatencyRow::from_samples("E2E (parent)", &mut e2e_samples.clone()),
        LatencyRow::from_samples("Executor FFI", &mut executor_samples.clone()),
        LatencyRow::from_samples(
            "Executor internal (FFI - sleep)",
            &mut executor_internal_samples.clone(),
        ),
        LatencyRow::from_samples(
            "IPC overhead (E2E - FFI)",
            &mut ipc_overhead_samples.clone(),
        ),
    ];
    let table = Table::new(rows).to_string();

    let preamble = format!(
        "# Task-executor overhead\n\nInputs: `sleep_and_echo` task with {PAYLOAD_LEN} path-like \
         strings echoed after a {INSTRUMENT_SLEEP_US}µs sleep, {ITERATIONS} samples (excluding \
         warm-up).\n\n* `Executor internal` ≈ in-executor input/output serde cost.\n* `IPC \
         overhead` ≈ parent-side framing + bincode + pipe traversal.\n\n"
    );

    let path = output_dir.join(OUTPUT_FILE);
    let mut file =
        File::create(&path).unwrap_or_else(|err| panic!("create {} failed: {err}", path.display()));
    file.write_all(preamble.as_bytes()).expect("write preamble");
    file.write_all(table.as_bytes()).expect("write table");
    file.write_all(b"\n").expect("write trailing newline");
}

/// Builds `len` deterministic path-like strings. Mixing prefixes and suffixes keeps the payload
/// representative of a realistic input without depending on `rand`.
///
/// # Returns
///
/// A `Vec<String>` of length `len`.
fn path_like_payload(len: usize) -> Vec<String> {
    const PREFIXES: &[&str] = &[
        "/var/log",
        "/usr/local/bin",
        "/etc/spider",
        "/home/user/projects",
        "/opt/data/cache",
    ];
    const SUFFIXES: &[&str] = &["log", "txt", "bin", "json", "tmp"];
    (0..len)
        .map(|idx| {
            let prefix = PREFIXES[idx % PREFIXES.len()];
            let suffix = SUFFIXES[(idx / PREFIXES.len()) % SUFFIXES.len()];
            format!("{prefix}/file_{:04}_{idx:05}.{suffix}", (idx * 31) % 10_000)
        })
        .collect()
}

/// Asserts that `response` is a `Success` whose decoded payload equals `expected`.
///
/// # Panics
///
/// Panics if the response is a `Failure` (the decoded
/// [`ExecutorError`](spider_task_executor::ExecutorError) is included in the panic message), or if
/// the decoded payload doesn't match `expected`.
fn expect_echo(response: &Response, expected: &[String]) {
    let Response::Result { outcome, .. } = response;
    let outputs = match outcome {
        ExecutorOutcome::Success { outputs } => outputs,
        ExecutorOutcome::Failure { error } => {
            let err: spider_task_executor::ExecutorError =
                rmp_serde::from_slice(error).expect("decode ExecutorError payload");
            panic!("sleep_and_echo warm-up returned Failure: {err:?}");
        }
    };
    let got: Vec<String> = decode_single_output(outputs);
    assert_eq!(got, expected, "warm-up output mismatch");
}
