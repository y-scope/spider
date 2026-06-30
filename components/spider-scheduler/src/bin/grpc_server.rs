//! Command-line entrypoint for the scheduler gRPC server.

use std::{error::Error, net::SocketAddr, path::PathBuf};

use clap::Parser;
use spider_proto_rust::scheduler::scheduler_service_server::SchedulerServiceServer;
use spider_scheduler::{
    GrpcSchedulerService,
    GrpcSchedulerStorageClient,
    ServerConfig,
    create_runtime,
};
use spider_utils::{config::YamlConfig, logging::set_up_logging};
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
    let server_config = ServerConfig::from_yaml_file(&cli.config)?;
    let listen_addr = SocketAddr::new(server_config.runtime.host, server_config.runtime.port);

    let storage_client = GrpcSchedulerStorageClient::connect(
        server_config.storage_endpoint.endpoint()?,
        server_config.storage_connection_pool_size,
    )
    .await?;

    let (runtime, service, cancellation_token) =
        create_runtime(server_config.runtime, storage_client).await?;
    let grpc_service = GrpcSchedulerService::new(service, cancellation_token.clone());
    tracing::info!(listen_addr = % listen_addr, "Starting scheduler gRPC server.");

    let serve_result = Server::builder()
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
                    cancellation_token.cancel();
                }
            }
        })
        .await;

    let stop_result = runtime.stop().await;
    serve_result?;
    stop_result?;
    Ok(())
}
