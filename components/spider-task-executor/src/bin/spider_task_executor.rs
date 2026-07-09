//! Spider task-executor binary.
//!
//! Reads bincode-framed [`Request`](spider_task_executor::protocol::Request)s from `stdin`,
//! dispatches them through a [`TdlPackageManager`], and writes
//! [`Response`](spider_task_executor::protocol::Response)s to `stdout`. The execution manager
//! spawns this process per slot and supervises it.
//!
//! Package resolution: each `Execute` request names a TDL package; the executor looks for
//! `${SPIDER_TDL_PACKAGE_DIR}/${package}/${package}.so` and caches the loaded library by name.
//!
//! Execution model: requests are processed strictly sequentially on a single-threaded tokio
//! runtime. Tokio is used only to match the async I/O surface on the execution manager side;
//! the executor itself has no concurrency requirements, and exactly one task runs for the
//! lifetime of the process.

use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use anyhow::anyhow;
use bytes::Bytes;
use futures_util::SinkExt;
use futures_util::StreamExt;
use spider_task_executor::ExecutorError;
use spider_task_executor::TdlPackageManager;
use spider_task_executor::protocol::ExecutorOutcome;
use spider_task_executor::protocol::Request;
use spider_task_executor::protocol::Response;
use tokio::io::stdin;
use tokio::io::stdout;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;
use tokio_util::codec::LengthDelimitedCodec;

/// Env var that points to the directory where compiled TDL packages live.
const SPIDER_TDL_PACKAGE_DIR: &str = "SPIDER_TDL_PACKAGE_DIR";

/// Initializes tracing logging.
fn init_tracing() {
    // Send tracing output to stderr so it doesn't pollute the framed-stdout protocol channel.
    tracing_subscriber::fmt()
        .event_format(
            tracing_subscriber::fmt::format()
                .with_level(true)
                .with_target(false)
                .with_file(true)
                .with_line_number(true)
                .json(),
        )
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .init();
}

/// Runs a task from the given TDL context and inputs.
///
/// # Returns
///
/// Forwards [`spider_task_executor::TdlPackage::execute_task`]'s return values on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`TdlPackageManager::load`]'s return values on failure.
/// * Forwards [`spider_task_executor::TdlPackage::execute_task`]'s return values on failure.
fn run_task(
    manager: &mut TdlPackageManager,
    pkg_dir: &Path,
    package: &str,
    task_func: &str,
    raw_ctx: &[u8],
    raw_inputs: &[u8],
) -> Result<Vec<u8>, ExecutorError> {
    let pkg = if let Some(pkg) = manager.get(package) {
        pkg
    } else {
        let path = pkg_dir.join(package).join(format!("lib{package}.so"));
        manager.load(&path)?
    };
    pkg.execute_task(task_func, raw_ctx, raw_inputs)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_tracing();

    let pkg_dir: PathBuf = std::env::var_os(SPIDER_TDL_PACKAGE_DIR)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("{SPIDER_TDL_PACKAGE_DIR} env var not set"))?;

    let mut requests = FramedRead::new(stdin(), LengthDelimitedCodec::new());
    let mut responses = FramedWrite::new(stdout(), LengthDelimitedCodec::new());

    let mut manager = TdlPackageManager::new();

    tracing::info!("Executor starts.");

    while let Some(frame) = requests.next().await {
        let frame = frame
            .inspect_err(|e| tracing::error!(err = ? e, "Failed to receive request frame."))?;
        let req: Request = bincode::deserialize(&frame)
            .inspect_err(|e| tracing::error!(err = ? e, "Failed to deserialize request."))?;
        match req {
            Request::Execute {
                tdl_context,
                raw_ctx,
                raw_inputs,
            } => {
                let started = Instant::now();
                let outcome = match run_task(
                    &mut manager,
                    &pkg_dir,
                    &tdl_context.package,
                    &tdl_context.task_func,
                    &raw_ctx,
                    &raw_inputs,
                ) {
                    Ok(outputs) => ExecutorOutcome::Success { outputs },
                    Err(e) => ExecutorOutcome::Failure {
                        error: rmp_serde::to_vec(&e).inspect_err(
                            |e| tracing::error!(err = ? e, "Failed to serialize execution result."),
                        )?,
                    },
                };
                let elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);

                let resp = Response::Result {
                    outcome,
                    elapsed_us,
                };
                let bytes = bincode::serialize(&resp)
                    .inspect_err(|e| tracing::error!(err = ? e, "Failed to serialize response."))?;
                responses
                    .send(Bytes::from(bytes))
                    .await
                    .inspect_err(|e| tracing::error!(err = ? e, "Failed to send response."))?;
            }
            Request::Shutdown => {
                tracing::info!("Received shutdown request.");
                break;
            }
        }
    }

    tracing::info!("Executor exits.");
    Ok(())
}
