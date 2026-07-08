//! Command-line entrypoint for the storage gRPC server.

use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use spider_proto_rust::storage::ExecutionManagerLivenessServiceServer;
use spider_proto_rust::storage::InboundQueueServiceServer;
use spider_proto_rust::storage::JobOrchestrationServiceServer;
use spider_proto_rust::storage::ResourceGroupManagementServiceServer;
use spider_proto_rust::storage::SchedulerRegistrationServiceServer;
use spider_proto_rust::storage::SessionManagementServiceServer;
use spider_proto_rust::storage::TaskInstanceManagementServiceServer;
use spider_storage::ServerConfig;
use spider_storage::grpc::GrpcServiceState;
use spider_storage::state::runtime::create_runtime;
use spider_utils::config::YamlConfig;
use spider_utils::logging::set_up_logging;
use tokio::select;
use tonic::transport::Server;

/// Command-line arguments for the storage gRPC server.
#[derive(Debug, Parser)]
#[command(about = "Run the Spider storage gRPC server.")]
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
    let listen_addr = SocketAddr::new(server_config.host, server_config.port);
    let (runtime, cancellation_token) = create_runtime(&server_config.runtime).await?;
    let grpc_service =
        GrpcServiceState::new(runtime.get_service_state(), cancellation_token.clone());
    tracing::info!(listen_addr = % listen_addr, "Starting storage gRPC server.");

    let serve_result = Server::builder()
        .add_service(JobOrchestrationServiceServer::new(grpc_service.clone()))
        .add_service(TaskInstanceManagementServiceServer::new(
            grpc_service.clone(),
        ))
        .add_service(InboundQueueServiceServer::new(grpc_service.clone()))
        .add_service(ResourceGroupManagementServiceServer::new(
            grpc_service.clone(),
        ))
        .add_service(ExecutionManagerLivenessServiceServer::new(
            grpc_service.clone(),
        ))
        .add_service(SchedulerRegistrationServiceServer::new(
            grpc_service.clone(),
        ))
        .add_service(SessionManagementServiceServer::new(grpc_service))
        .serve_with_shutdown(listen_addr, async move {
            select! {
                () = cancellation_token.cancelled() => {
                    tracing::info!("Shutting down storage gRPC server.");
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
