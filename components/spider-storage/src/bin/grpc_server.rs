//! Command-line entrypoint for the storage gRPC server.

use std::{error::Error, net::SocketAddr, path::PathBuf};

use clap::Parser;
use spider_proto_rust::storage::{
    execution_manager_liveness_service_server::ExecutionManagerLivenessServiceServer,
    inbound_queue_service_server::InboundQueueServiceServer,
    job_orchestration_service_server::JobOrchestrationServiceServer,
    resource_group_management_service_server::ResourceGroupManagementServiceServer,
    scheduler_registration_service_server::SchedulerRegistrationServiceServer,
    session_management_service_server::SessionManagementServiceServer,
    task_instance_management_service_server::TaskInstanceManagementServiceServer,
};
use spider_storage::{
    ServerConfig,
    grpc::StorageGrpcService,
    logging::set_up_logging,
    state::runtime::create_runtime,
};
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
    let grpc_service = StorageGrpcService::new(runtime.get_service_state());
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
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(error = ? error, "Failed to listen for Ctrl-C.");
            }
            cancellation_token.cancel();
        })
        .await;

    let stop_result = runtime.stop().await;
    serve_result?;
    stop_result?;
    Ok(())
}
