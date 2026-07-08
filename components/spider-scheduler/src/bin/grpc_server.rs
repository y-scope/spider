//! Command-line entrypoint for the scheduler gRPC server.

use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use spider_proto_rust::scheduler::SchedulerServiceServer;
use spider_scheduler::GrpcSchedulerStorageClient;
use spider_scheduler::ServerConfig;
use spider_scheduler::create_runtime;
use spider_scheduler::grpc::GrpcSchedulerService;
use spider_utils::config::YamlConfig;
use spider_utils::logging::set_up_logging;
use tokio::select;
use tonic::transport::Server;

/// Command-line arguments for the scheduler gRPC server.
#[derive(Debug, Parser)]
#[command(about = "Run the Spider scheduler gRPC server.")]
struct Cli {
    /// Path to the YAML server configuration file.
    #[arg(short, long, value_name = "PATH")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let _log_guard = set_up_logging();
    let cli = Cli::parse();
    let server_config = ServerConfig::from_yaml_file(&cli.config)
        .inspect_err(|error| tracing::error!(error = % error, "Failed to load configuration."))?;
    let listen_addr = SocketAddr::new(server_config.runtime.host, server_config.runtime.port);

    let storage_endpoint = server_config.storage_endpoint.endpoint().inspect_err(
        |error| tracing::error!(error = % error, "Failed to parse storage endpoint."),
    )?;

    let storage_client =
        GrpcSchedulerStorageClient::connect(storage_endpoint, server_config.connection_pool_size)
            .await
            .inspect_err(|error| {
                tracing::error!(error = % error, "Failed to connect to storage gRPC service.");
            })?;

    let (runtime, service, cancellation_token) =
        create_runtime(server_config.runtime, storage_client)
            .await
            .inspect_err(|error| {
                tracing::error!(error = % error, "Failed to create scheduler runtime.");
            })?;
    let grpc_service = GrpcSchedulerService::new(service, cancellation_token.clone());
    tracing::info!(listen_addr = % listen_addr, "Starting scheduler gRPC server.");

    Server::builder()
        .add_service(SchedulerServiceServer::new(grpc_service))
        .serve_with_shutdown(listen_addr, async move {
            select! {
                () = cancellation_token.cancelled() => {
                    tracing::info!("Shutting down scheduler gRPC server.");
                }
                result = tokio::signal::ctrl_c() => {
                    if let Err(error) = result {
                        tracing::error!(error = % error, "Failed to listen for Ctrl-C.");
                    }
                    tracing::info!("Received Ctrl-C. Shutting down scheduler gRPC server.");
                    cancellation_token.cancel();
                }
            }
        })
        .await
        .inspect_err(
            |error| tracing::error!(error = % error, "Scheduler gRPC server exited on error."),
        )?;

    let () = runtime.stop().await.inspect_err(
        |error| tracing::error!(error = % error, "Failed to stop scheduler runtime."),
    )?;
    Ok(())
}
