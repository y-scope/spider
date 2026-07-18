//! Test TDL package used by the `task-executor` integration tests.
//!
//! Exposes six tasks that exercise distinct executor code paths:
//!
//! * [`task_decl::fibonacci`] — basic compute + correctness.
//! * [`task_decl::always_fail`] — in-task error reporting.
//! * [`task_decl::always_panic`] — process-level crash handling.
//! * [`task_decl::sleep_and_echo`] — fixed-cost task: sleeps for a known [`INSTRUMENT_SLEEP_US`]
//!   duration then echoes its `Vec<String>` payload back. Used by the overhead bench so the
//!   non-sleep portion of the executor's reported FFI time isolates the in-executor input/output
//!   serde cost, while the parent-side delta isolates IPC framing cost.
//! * [`task_decl::assert_outputs_sum_zero`] — commit task: reads the job's task-graph outputs from
//!   its [`TaskContext`](spider_tdl::TaskContext) and asserts the `i64` outputs sum to zero.
//! * [`task_decl::assert_initialized`] — confirms the package's init hook ran on load.
//!
//! The package also registers an init hook ([`task_decl::package_init`]) that runs once when the
//! package is loaded.

/// The constant sleep duration used by [`task_decl::sleep_and_echo`].
///
/// Exposed at crate scope so the overhead bench (linked dynamically, so it can't read the value
/// through the cdylib) can reference the same number to keep them in sync if changed.
pub const INSTRUMENT_SLEEP_US: u64 = 50;

mod task_decl {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::thread::sleep;
    use std::time::Duration;

    use spider_tdl::TaskContext;
    use spider_tdl::TdlError;
    use spider_tdl::task;

    use crate::INSTRUMENT_SLEEP_US;

    /// Package init hook. Records that init ran, or fails when `SPIDER_TEST_TDL_INIT_SHOULD_FAIL`
    /// is set (used by the executor's init integration test).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TdlError::ExecutionError`] if `SPIDER_TEST_TDL_INIT_SHOULD_FAIL` is set in the
    ///   environment.
    pub fn package_init() -> Result<(), TdlError> {
        if std::env::var_os("SPIDER_TEST_TDL_INIT_SHOULD_FAIL").is_some() {
            return Err(TdlError::ExecutionError(
                "integration_test_tasks: init failure requested".to_owned(),
            ));
        }
        INITIALIZED.store(true, Ordering::SeqCst);
        Ok(())
    }

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

    /// Commit task that reads the job's task-graph outputs and asserts that the `i64` values they
    /// carry sum to zero.
    #[task(name = "assert_outputs_sum_zero")]
    pub fn assert_outputs_sum_zero(ctx: TaskContext) -> Result<(), TdlError> {
        let outputs = ctx.get_task_graph_outputs()?.ok_or_else(|| {
            TdlError::ExecutionError("assert_outputs_sum_zero must run as a commit task".to_owned())
        })?;
        if outputs.is_empty() {
            return Err(TdlError::ExecutionError(
                "assert_outputs_sum_zero: task-graph outputs empty".to_owned(),
            ));
        }
        let mut sum: i64 = 0;
        for output in &outputs {
            let value: i64 = rmp_serde::from_slice(output)
                .map_err(|e| TdlError::DeserializationError(e.to_string()))?;
            sum += value;
        }
        if sum != 0 {
            return Err(TdlError::ExecutionError(format!(
                "task-graph outputs do not sum to zero: got {sum}"
            )));
        }
        Ok(())
    }

    /// Succeeds only if the package's init hook set the [`INITIALIZED`] flag on load.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TdlError::ExecutionError`] if the init hook did not run before this task was dispatched.
    #[task(name = "assert_initialized")]
    pub fn assert_initialized(_ctx: TaskContext) -> Result<(), TdlError> {
        if INITIALIZED.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(TdlError::ExecutionError(
                "assert_initialized: package init hook did not run".to_owned(),
            ))
        }
    }

    /// Set by [`package_init`] so [`assert_initialized`] can confirm the init hook ran on load.
    static INITIALIZED: AtomicBool = AtomicBool::new(false);
}

spider_tdl::register_tdl_package! {
    package_name: "integration_test_tasks",
    init: task_decl::package_init,
    tasks: [
        task_decl::fibonacci,
        task_decl::always_fail,
        task_decl::always_panic,
        task_decl::sleep_and_echo,
        task_decl::assert_outputs_sum_zero,
        task_decl::assert_initialized,
    ],
}
