//! TDL package that runs KQL searches over CLP archives via the `clp-s` binary.

mod task_decl {
    use std::{
        env,
        fs::File,
        process::{Command, Stdio},
    };

    use spider_tdl::{TaskContext, TdlError, task};

    /// Environment variable that overrides the `clp-s` binary path.
    const CLP_S_BIN_ENV: &str = "CLP_S_BIN";

    /// Default `clp-s` binary path used when [`CLP_S_BIN_ENV`] is unset.
    const DEFAULT_CLP_S_BIN: &str = "/home/lzh/dev/clp/build/core/clp-s";

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
        _ctx: TaskContext,
        archive_path: String,
        query: String,
        output_path: String,
    ) -> Result<(), TdlError> {
        let clp_s_bin = env::var(CLP_S_BIN_ENV).unwrap_or_else(|_| DEFAULT_CLP_S_BIN.to_owned());

        let output_file = File::create(&output_path).map_err(|error| {
            TdlError::ExecutionError(format!(
                "failed to create output file `{output_path}` for archive `{archive_path}`: \
                 {error}"
            ))
        })?;

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
