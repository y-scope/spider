//! Benchmark TDL package for the neural-network-shaped task graph.
//!
//! Exposes a single task, [`task_decl::sleep`], that mirrors one neuron in a layered NN benchmark:
//! it consumes 25 `bytes` (128-byte) inputs, sleeps for a fixed 10 ms to simulate compute cost, and
//! emits a fixed 128-byte output. The inputs are intentionally ignored -- the task "does no work"
//! beyond the sleep -- so the benchmark measures scheduling/execution overhead rather than real
//! computation.
//!
//! Each invocation logs a START and END line to stderr (captured by the execution manager into
//! `build/spider-run/em-logs/<em_id>-<executor_id>.log`) carrying the job/task ids and a nanosecond
//! timestamp, so per-task start/end times can be recovered from the executor logs.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Fixed sleep duration simulating a neuron's compute cost.
const SLEEP_DURATION: Duration = Duration::from_millis(10);

/// Fixed 128-byte payload every invocation emits.
const OUTPUT_PAYLOAD: [u8; 128] = [0; 128];

/// Nanoseconds since the UNIX epoch, for the per-task log lines.
///
/// # Panics
///
/// Panics only if the system clock is before the UNIX epoch -- never in practice.
fn now_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_nanos()
}

mod task_decl {
    // The `#[task]` macro generates a wrapper (`__sleep`) whose positional arity mirrors the
    // user task's signature, so clippy's `too_many_arguments` (threshold 7) fires on the
    // expansion. The 25-arity is a fixed property of the neuron model, not a code smell. The
    // macro does not forward a function-level `#[allow]` to the generated wrapper, so the allow
    // must be scoped at the module (which contains only this one task). Suppress here rather than
    // restructuring the task or touching the derive macro.
    #![allow(clippy::too_many_arguments)]

    // Alias `std::thread::sleep` to avoid colliding with the `sleep` marker struct the
    // `#[task]` macro generates below (the macro names the marker after the function).
    use std::thread::sleep as thread_sleep;

    use spider_tdl::{TaskContext, TdlError, task};

    use super::{OUTPUT_PAYLOAD, SLEEP_DURATION, now_unix_nanos};

    /// NN benchmark neuron: consumes 25 `bytes` inputs, sleeps 10 ms, emits a fixed 128-byte
    /// output, and logs START/END timestamps to stderr.
    ///
    /// The 25 positional inputs model the neuron's 25 incoming data-flow edges. Their content is
    /// ignored (the task does no real work); only their total byte count is logged to prove the
    /// inputs were delivered.
    #[task(name = "nn_bench::sleep")]
    pub fn sleep(
        ctx: TaskContext,
        i0: Vec<u8>,
        i1: Vec<u8>,
        i2: Vec<u8>,
        i3: Vec<u8>,
        i4: Vec<u8>,
        i5: Vec<u8>,
        i6: Vec<u8>,
        i7: Vec<u8>,
        i8: Vec<u8>,
        i9: Vec<u8>,
        i10: Vec<u8>,
        i11: Vec<u8>,
        i12: Vec<u8>,
        i13: Vec<u8>,
        i14: Vec<u8>,
        i15: Vec<u8>,
        i16: Vec<u8>,
        i17: Vec<u8>,
        i18: Vec<u8>,
        i19: Vec<u8>,
        i20: Vec<u8>,
        i21: Vec<u8>,
        i22: Vec<u8>,
        i23: Vec<u8>,
        i24: Vec<u8>,
    ) -> Result<Vec<u8>, TdlError> {
        // Touch every input so the wrapper's parameters are used (and so the log proves the inputs
        // were delivered); the content itself is irrelevant to the benchmark.
        let input_bytes = i0.len()
            + i1.len()
            + i2.len()
            + i3.len()
            + i4.len()
            + i5.len()
            + i6.len()
            + i7.len()
            + i8.len()
            + i9.len()
            + i10.len()
            + i11.len()
            + i12.len()
            + i13.len()
            + i14.len()
            + i15.len()
            + i16.len()
            + i17.len()
            + i18.len()
            + i19.len()
            + i20.len()
            + i21.len()
            + i22.len()
            + i23.len()
            + i24.len();

        let start_ns = now_unix_nanos();
        eprintln!(
            "[nn_bench::sleep] START job={:?} task={:?} instance={} input_bytes={} t_ns={}",
            ctx.job_id, ctx.task_id, ctx.task_instance_id, input_bytes, start_ns,
        );

        thread_sleep(SLEEP_DURATION);

        let end_ns = now_unix_nanos();
        eprintln!(
            "[nn_bench::sleep] END   job={:?} task={:?} instance={} t_ns={} dur_ns={}",
            ctx.job_id,
            ctx.task_id,
            ctx.task_instance_id,
            end_ns,
            end_ns - start_ns,
        );

        Ok(OUTPUT_PAYLOAD.to_vec())
    }
}

spider_tdl::register_tdl_package! {
    package_name: "nn_bench",
    tasks: [
        task_decl::sleep,
    ],
}
