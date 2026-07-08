//! Command-line entrypoint for the execution manager.

use std::error::Error;
use std::path::PathBuf;

use clap::Parser;
use spider_execution_manager::Config;
use spider_execution_manager::client::grpc::GrpcLivenessClient;
use spider_execution_manager::client::grpc::GrpcSchedulerClient;
use spider_execution_manager::client::grpc::GrpcStorageClient;
use spider_execution_manager::runtime::Runtime;
use spider_utils::config::YamlConfig;
use spider_utils::logging::set_up_logging;

/// Command-line arguments for the execution manager.
#[derive(Debug, Parser)]
#[command(about = "Run the Spider execution manager.")]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(short, long, value_name = "PATH")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let _log_guard = set_up_logging();
    let cli = Cli::parse();
    let config = Config::from_yaml_file(&cli.config)
        .inspect_err(|error| tracing::error!(error = % error, "Failed to load configuration."))?;

    let storage_endpoint = config.storage.endpoint().inspect_err(
        |error| tracing::error!(error = % error, "Failed to parse storage endpoint."),
    )?;
    let scheduler_endpoint = config.scheduler.endpoint().inspect_err(
        |error| tracing::error!(error = % error, "Failed to parse scheduler endpoint."),
    )?;
    let pool_size = config.connection_pool_size;

    let storage_client = GrpcStorageClient::connect(storage_endpoint.clone(), pool_size)
        .await
        .inspect_err(
            |error| tracing::error!(error = % error, "Failed to connect to storage gRPC service."),
        )?;
    let liveness_client = GrpcLivenessClient::connect(storage_endpoint, pool_size)
        .await
        .inspect_err(
            |error| tracing::error!(error = % error, "Failed to connect to liveness gRPC service."),
        )?;
    let scheduler_client = GrpcSchedulerClient::connect(scheduler_endpoint, pool_size).await.inspect_err(|error| tracing::error!(error = % error, "Failed to connect to scheduler gRPC service."))?;

    let (runtime, cancellation_token) = Runtime::create(
        scheduler_client,
        storage_client,
        liveness_client,
        config.runtime_config(),
    )
    .await
    .inspect_err(
        |error| tracing::error!(error = % error, "Failed to create execution manager runtime."),
    )?;

    let em_id = runtime.get_em_id().get();

    tracing::info!(em_id, "Execution manager started.");
    let mut run_handle = tokio::spawn(runtime.run());

    let () = tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                tracing::error!(em_id, error = % error, "Failed to listen for Ctrl-C.");
            }
            tracing::info!(em_id, "Received Ctrl-C. Shutting down execution manager.");
            cancellation_token.cancel();
            run_handle.await
        }
        result = &mut run_handle => {
            tracing::info!("Execution manager runtime exited.");
            result
        }
    }
    .inspect_err(
        |error| tracing::error!(em_id, error = % error, "Execution manager runtime panicked."),
    )?
    .inspect_err(
        |error| tracing::error!(em_id, error = % error, "Execution manager exited on error."),
    )?;

    Ok(())
}
