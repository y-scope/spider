//! Command-line entrypoint for the storage gRPC server.

use std::{error::Error, net::SocketAddr};

use clap::Parser;
use secrecy::SecretString;
use spider_proto_rust::storage::{
    execution_manager_liveness_service_server::ExecutionManagerLivenessServiceServer,
    inbound_queue_service_server::InboundQueueServiceServer,
    job_orchestration_service_server::JobOrchestrationServiceServer,
    resource_group_management_service_server::ResourceGroupManagementServiceServer,
    session_management_service_server::SessionManagementServiceServer,
    task_instance_management_service_server::TaskInstanceManagementServiceServer,
};
use spider_storage::{
    DatabaseConfig,
    grpc::StorageGrpcService,
    ready_queue::ReadyQueueConfig,
    state::{
        JobCacheGcConfig,
        runtime::{RuntimeConfig, create_runtime},
    },
    task_instance_pool::TaskInstancePoolConfig,
};
use tonic::transport::Server;

/// Storage gRPC server configuration.
#[derive(Debug, Parser)]
#[command(about = "Run the Spider storage gRPC server.")]
struct Cli {
    /// Address the gRPC server listens on.
    #[arg(
        long,
        env = "SPIDER_STORAGE_LISTEN_ADDR",
        default_value = "127.0.0.1:50051"
    )]
    listen_addr: SocketAddr,

    /// MariaDB host.
    #[arg(long, env = "MARIADB_HOST", default_value = "localhost")]
    db_host: String,

    /// MariaDB port.
    #[arg(long, env = "MARIADB_PORT", default_value_t = 3306)]
    db_port: u16,

    /// MariaDB database name.
    #[arg(long, env = "MARIADB_DATABASE", default_value = "spider")]
    db_name: String,

    /// MariaDB username.
    #[arg(long, env = "MARIADB_USERNAME", default_value = "spider")]
    db_username: String,

    /// MariaDB password.
    #[arg(long, env = "MARIADB_PASSWORD")]
    db_password: String,

    /// Maximum MariaDB connections.
    #[arg(long, env = "MARIADB_MAX_CONNECTIONS", default_value_t = 5)]
    db_max_connections: u32,

    /// Ready task queue capacity.
    #[arg(long, default_value_t = ReadyQueueConfig::default().task_capacity)]
    ready_task_capacity: usize,

    /// Ready commit-task queue capacity.
    #[arg(long, default_value_t = ReadyQueueConfig::default().commit_capacity)]
    ready_commit_capacity: usize,

    /// Ready cleanup-task queue capacity.
    #[arg(long, default_value_t = ReadyQueueConfig::default().cleanup_capacity)]
    ready_cleanup_capacity: usize,

    /// Seconds without heartbeat before an execution manager is stale.
    #[arg(
        long,
        default_value_t = TaskInstancePoolConfig::default().execution_manager_stale_cutoff_sec
    )]
    task_pool_execution_manager_stale_cutoff_sec: u64,

    /// Task instance pool GC interval in seconds.
    #[arg(long, default_value_t = TaskInstancePoolConfig::default().gc_interval_sec)]
    task_pool_gc_interval_sec: u64,

    /// Task instance pool message channel capacity.
    #[arg(
        long,
        default_value_t = TaskInstancePoolConfig::default().message_channel_capacity
    )]
    task_pool_message_channel_capacity: usize,

    /// Seconds to retain terminated jobs in cache.
    #[arg(long, default_value_t = JobCacheGcConfig::default().terminated_job_retention_sec)]
    job_cache_terminated_retention_sec: u64,

    /// Job cache GC interval in seconds.
    #[arg(long, default_value_t = JobCacheGcConfig::default().gc_interval_sec)]
    job_cache_gc_interval_sec: u64,
}

impl Cli {
    /// # Returns
    ///
    /// A [`RuntimeConfig`] built from CLI inputs.
    fn to_runtime_config(self) -> RuntimeConfig {
        RuntimeConfig {
            db_config: DatabaseConfig {
                host: self.db_host,
                port: self.db_port,
                name: self.db_name,
                username: self.db_username,
                password: SecretString::from(self.db_password),
                max_connections: self.db_max_connections,
            },
            ready_queue_config: ReadyQueueConfig {
                task_capacity: self.ready_task_capacity,
                commit_capacity: self.ready_commit_capacity,
                cleanup_capacity: self.ready_cleanup_capacity,
            },
            task_instance_pool_config: TaskInstancePoolConfig {
                execution_manager_stale_cutoff_sec: self
                    .task_pool_execution_manager_stale_cutoff_sec,
                gc_interval_sec: self.task_pool_gc_interval_sec,
                message_channel_capacity: self.task_pool_message_channel_capacity,
            },
            job_cache_gc_config: JobCacheGcConfig {
                terminated_job_retention_sec: self.job_cache_terminated_retention_sec,
                gc_interval_sec: self.job_cache_gc_interval_sec,
            },
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let cli = Cli::parse();
    let listen_addr = cli.listen_addr;
    let (runtime, cancellation_token) = create_runtime(&cli.to_runtime_config()).await?;
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
