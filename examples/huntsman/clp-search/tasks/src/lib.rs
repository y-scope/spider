//! TDL package that runs KQL searches over CLP archives via the `clp-s` binary.

mod task_decl {
    use std::{
        env,
        fs::File,
        process::{Command, Stdio},
        sync::Once,
        time::Instant,
    };

    use spider_tdl::{TaskContext, TdlError, task};

    /// Environment variable that overrides the `clp-s` binary path.
    const CLP_S_BIN_ENV: &str = "CLP_S_BIN";

    /// Default `clp-s` binary path used when [`CLP_S_BIN_ENV`] is unset.
    const DEFAULT_CLP_S_BIN: &str = "/home/lzh/dev/clp/build/core/clp-s";

    /// Guards one-time installation of this package's tracing subscriber.
    static LOG_INIT: Once = Once::new();

    /// Installs a package-local tracing subscriber exactly once.
    ///
    /// This TDL package is a `cdylib` with its own copy of `tracing`'s global dispatcher, distinct
    /// from the task executor that `dlopen`s it, so the executor's subscriber never observes events
    /// emitted here. This installs a subscriber owned by the package that writes JSON to stderr --
    /// the same stream the executor redirects to `em-logs/<em_id>-<executor_id>.log` -- and honors
    /// `RUST_LOG` (propagated from the execution manager). `try_init` makes a redundant call on a
    /// later task invocation a no-op.
    fn init_task_logging() {
        LOG_INIT.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .event_format(tracing_subscriber::fmt::format().with_target(false).json())
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .with_ansi(false)
                .with_writer(std::io::stderr)
                .try_init();
        });
    }

    /// Runs the KQL `query` over the CLP archive at `archive_path`, writing the matching records as
    /// JSONL to `output_path`.
    ///
    /// Invokes the `clp-s` binary (resolved from the `CLP_S_BIN` environment variable, or
    /// [`DEFAULT_CLP_S_BIN`] when unset) as `clp-s s <archive_path> <query>` and redirects its
    /// stdout into the freshly truncated file at `output_path`. The parent directory of
    /// `output_path` is assumed to already exist.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TdlError::ExecutionError`] if the output file cannot be created, the `clp-s` process
    ///   cannot be spawned, the process cannot be waited on, or the process exits with a
    ///   non-success status.
    #[task(name = "clp_search::search")]
    pub fn search(
        ctx: TaskContext,
        archive_path: String,
        query: String,
        output_path: String,
    ) -> Result<(), TdlError> {
        init_task_logging();
        let clp_s_bin = env::var(CLP_S_BIN_ENV).unwrap_or_else(|_| DEFAULT_CLP_S_BIN.to_owned());

        let output_file = File::create(&output_path).map_err(|error| {
            TdlError::ExecutionError(format!(
                "failed to create output file `{output_path}` for archive `{archive_path}`: \
                 {error}"
            ))
        })?;

        // Benchmark instrumentation: time only the `clp-s` subprocess.
        let clp_s_start = Instant::now();
        let output = Command::new(&clp_s_bin)
            .arg("s")
            .arg(&archive_path)
            .arg(&query)
            .stdout(Stdio::from(output_file))
            .stderr(Stdio::piped())
            .output()
            .map_err(|error| {
                TdlError::ExecutionError(format!(
                    "failed to run `{clp_s_bin}` for archive `{archive_path}`: {error}"
                ))
            })?;
        let clp_s_elapsed_us = u64::try_from(clp_s_start.elapsed().as_micros()).unwrap_or(u64::MAX);
        tracing::info!(
            clp_s_elapsed_us,
            job_id = ? ctx.job_id,
            task_id = ? ctx.task_id,
            "clp-s subprocess finished."
        );

        if output.status.success() {
            return Ok(());
        }

        Err(TdlError::ExecutionError(format!(
            "`clp-s` failed for archive `{archive_path}` with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
        )))
    }
}

spider_tdl::register_tdl_package! {
    package_name: "clp_search",
    tasks: [
        task_decl::search
    ],
}
