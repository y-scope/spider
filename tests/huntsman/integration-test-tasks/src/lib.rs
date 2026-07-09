//! Test TDL package used by the `task-executor` integration tests.
//!
//! Exposes four tasks that exercise distinct executor code paths:
//!
//! * [`task_decl::fibonacci`] — basic compute + correctness.
//! * [`task_decl::always_fail`] — in-task error reporting.
//! * [`task_decl::always_panic`] — process-level crash handling.
//! * [`task_decl::sleep_and_echo`] — fixed-cost task: sleeps for a known [`INSTRUMENT_SLEEP_US`]
//!   duration then echoes its `Vec<String>` payload back. Used by the overhead bench so the
//!   non-sleep portion of the executor's reported FFI time isolates the in-executor input/output
//!   serde cost, while the parent-side delta isolates IPC framing cost.

/// The constant sleep duration used by [`task_decl::sleep_and_echo`].
///
/// Exposed at crate scope so the overhead bench (linked dynamically, so it can't read the value
/// through the cdylib) can reference the same number to keep them in sync if changed.
pub const INSTRUMENT_SLEEP_US: u64 = 50;

mod task_decl {
    use std::thread::sleep;
    use std::time::Duration;

    use spider_tdl::TaskContext;
    use spider_tdl::TdlError;
    use spider_tdl::task;

    use crate::INSTRUMENT_SLEEP_US;

    /// Computes the `index`-th Fibonacci number with a deliberately naive recursive
    /// implementation so the call has measurable CPU cost for the overhead benchmark.
    #[task(name = "fibonacci")]
    pub fn fibonacci(_ctx: TaskContext, index: u64) -> Result<u64, TdlError> {
        Ok(fib(index))
    }

    fn fib(index: u64) -> u64 {
        if index < 2 {
            index
        } else {
            fib(index - 1) + fib(index - 2)
        }
    }

    /// Always returns a [`TdlError::ExecutionError`].
    #[task(name = "always_fail")]
    pub fn always_fail(_ctx: TaskContext) -> Result<u64, TdlError> {
        Err(TdlError::ExecutionError(
            "always_fail: intentional failure".to_owned(),
        ))
    }

    /// Always panics. The panic crosses the `extern "C"` FFI boundary, which aborts the executor
    /// process — the test asserts the parent observes that crash.
    #[task(name = "always_panic")]
    pub fn always_panic(_ctx: TaskContext) -> Result<u64, TdlError> {
        panic!("always_panic: intentional panic")
    }

    /// Sleeps for a fixed [`INSTRUMENT_SLEEP_US`] microseconds, then echoes the input back.
    ///
    /// The fixed-cost body lets the overhead bench subtract the known sleep from the executor's
    /// reported FFI duration, isolating the in-executor input/output serde overhead.
    #[task(name = "sleep_and_echo")]
    pub fn sleep_and_echo(_ctx: TaskContext, items: Vec<String>) -> Result<Vec<String>, TdlError> {
        sleep(Duration::from_micros(INSTRUMENT_SLEEP_US));
        Ok(items)
    }
}

spider_tdl::register_tdl_package! {
    package_name: "integration_test_tasks",
    tasks: [
        task_decl::fibonacci,
        task_decl::always_fail,
        task_decl::always_panic,
        task_decl::sleep_and_echo,
    ],
}
