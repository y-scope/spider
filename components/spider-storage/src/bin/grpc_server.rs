//! Command-line entrypoint for the storage gRPC server.

use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

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
use spider_storage::shutdown::ShutdownHandle;
use spider_storage::state::StorageServerError;
use spider_storage::state::runtime::create_runtime;
use spider_utils::config::YamlConfig;
use spider_utils::logging::set_up_logging;
use tokio::net::TcpListener;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tokio::signal::unix::signal;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

/// How long the storage service may take to shut down once its cancellation token is cancelled.
/// 30 s matches Kubernetes' default termination grace period (`terminationGracePeriodSeconds`).
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

/// Command-line arguments for the storage gRPC server.
#[derive(Debug, Parser)]
#[command(about = "Run the Spider storage gRPC server.")]
struct Cli {
    /// Path to the YAML server configuration file.
    #[arg(short, long, value_name = "PATH")]
    config: PathBuf,
}

fn main() -> ExitCode {
    let _log_guard = set_up_logging();
    let tokio_runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(tokio_runtime) => tokio_runtime,
        Err(e) => {
            tracing::error!(error = % e, "Failed to build the async runtime.");
            return ExitCode::FAILURE;
        }
    };
    let exit_code = tokio_runtime.block_on(run()).unwrap_or(ExitCode::FAILURE);
    tokio_runtime.shutdown_background();
    exit_code
}

/// Runs the storage gRPC server until it shuts down.
///
/// # Returns
///
/// Forwards [`wait_for_shutdown`]'s return value on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`YamlConfig::from_yaml_file`]'s return values on failure.
/// * Forwards [`create_runtime`]'s return values on failure.
/// * Forwards [`TcpListener::bind`]'s return values on failure.
/// * Forwards [`signal`]'s return values on failure.
async fn run() -> Result<ExitCode, Box<dyn Error>> {
    let cli = Cli::parse();
    let server_config = ServerConfig::from_yaml_file(&cli.config).inspect_err(|e| {
        tracing::error!(error = % e, "Failed to load server configuration.");
    })?;
    let listen_addr = SocketAddr::new(server_config.host, server_config.port);
    let runtime = create_runtime(&server_config.runtime)
        .await
        .inspect_err(|e| {
            tracing::error!(error = % e, "Failed to create runtime.");
        })?;
    let shutdown_handle = runtime.get_shutdown_handle();
    let grpc_service = GrpcServiceState::new(runtime.get_service_state(), shutdown_handle.clone());
    tracing::info!(listen_addr = % listen_addr, "Starting storage gRPC server.");

    let listener = TcpListener::bind(listen_addr).await.inspect_err(
        |error| tracing::error!(error = % error, "Failed to bind storage listen address."),
    )?;
    let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));
    let mut sigterm = signal(SignalKind::terminate()).inspect_err(
        |error| tracing::error!(error = % error, "Failed to register SIGTERM handler."),
    )?;
    let server_shutdown_handle = shutdown_handle.clone();
    let server = tokio::spawn(
        Server::builder()
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
            .serve_with_incoming_shutdown(incoming, async move {
                select! {
                    () = server_shutdown_handle.cancelled() => {
                        tracing::info!("Shutting down storage gRPC server.");
                    }
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            tracing::error!(error = % error, "Failed to listen for Ctrl-C.");
                        }
                        server_shutdown_handle.shut_down_gracefully();
                    }
                    _ = sigterm.recv() => {
                        tracing::info!("Received SIGTERM. Shutting down storage gRPC server.");
                        server_shutdown_handle.shut_down_gracefully();
                    }
                }
            }),
    );

    // Initialize the inbound queue by resending all ready tasks from the current cache.
    //
    // This operation may duplicate ready tasks for jobs submitted after the server starts but
    // before the resend starts. This is safe because the system is designed to tolerate
    // duplicate entries in the inbound queue.
    let service_state = runtime.get_service_state();
    if let Err(e) = service_state.resend_ready_tasks().await {
        tracing::error!(error = % e, "Failed to initialize inbound queue.");
        shutdown_handle.shut_down_fatally();
    }

    Ok(wait_for_shutdown(server, runtime.stop(), &shutdown_handle, SHUTDOWN_DEADLINE).await)
}

/// Waits for the gRPC server to shut down, then stops the runtime through `stop_runtime`.
///
/// Both waits together are bounded by `deadline` once `shutdown_handle`'s token is cancelled.
///
/// # Returns
///
/// [`ExitCode::FAILURE`] if the server or the runtime missed the deadline, the server failed, the
/// runtime failed to stop, or the service shut down fatally. [`ExitCode::SUCCESS`] otherwise.
async fn wait_for_shutdown(
    mut server: JoinHandle<Result<(), tonic::transport::Error>>,
    stop_runtime: impl Future<Output = Result<(), StorageServerError>>,
    shutdown_handle: &ShutdownHandle,
    deadline: Duration,
) -> ExitCode {
    let deadline_elapsed = async {
        shutdown_handle.cancelled().await;
        tokio::time::sleep(deadline).await;
    };
    tokio::pin!(deadline_elapsed);

    let server_result = select! {
        result = &mut server => result,
        () = &mut deadline_elapsed => {
            tracing::error!("The gRPC server did not shut down before the deadline.");
            return ExitCode::FAILURE;
        }
    };

    let mut exit_code = ExitCode::SUCCESS;
    match server_result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::error!(error = % e, "gRPC server failure.");
            exit_code = ExitCode::FAILURE;
        }
        Err(e) => {
            tracing::error!(error = % e, "gRPC server panicked.");
            exit_code = ExitCode::FAILURE;
        }
    }

    let stop_result = select! {
        result = stop_runtime => result,
        () = &mut deadline_elapsed => {
            tracing::error!("The runtime did not stop before the deadline.");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = stop_result {
        tracing::error!(error = % e, "gRPC runtime failure.");
        exit_code = ExitCode::FAILURE;
    }
    if shutdown_handle.is_fatal() {
        tracing::error!("The storage service shut down on a fatal error.");
        exit_code = ExitCode::FAILURE;
    }
    exit_code
}

#[cfg(test)]
mod tests {
    use std::process::ExitCode;
    use std::time::Duration;

    use spider_storage::shutdown::ShutdownHandle;
    use spider_storage::state::StorageServerError;

    use super::wait_for_shutdown;

    const TEST_DEADLINE: Duration = Duration::from_millis(200);

    #[tokio::test]
    async fn clean_shutdown_succeeds() -> anyhow::Result<()> {
        let shutdown_handle = ShutdownHandle::new();
        shutdown_handle.shut_down_gracefully();

        let exit_code = wait_for_shutdown(
            tokio::spawn(async { Ok(()) }),
            async { Ok(()) },
            &shutdown_handle,
            TEST_DEADLINE,
        )
        .await;

        assert_eq!(exit_code, ExitCode::SUCCESS);
        Ok(())
    }

    #[tokio::test]
    async fn fatal_shutdown_fails() -> anyhow::Result<()> {
        let shutdown_handle = ShutdownHandle::new();
        shutdown_handle.shut_down_fatally();

        let exit_code = wait_for_shutdown(
            tokio::spawn(async { Ok(()) }),
            async { Ok(()) },
            &shutdown_handle,
            TEST_DEADLINE,
        )
        .await;

        assert_eq!(exit_code, ExitCode::FAILURE);
        Ok(())
    }

    #[tokio::test]
    async fn runtime_stop_failure_fails() -> anyhow::Result<()> {
        let shutdown_handle = ShutdownHandle::new();
        shutdown_handle.shut_down_gracefully();

        let exit_code = wait_for_shutdown(
            tokio::spawn(async { Ok(()) }),
            async {
                Err(StorageServerError::Stopping(
                    "background task exited on an error or a panic".to_owned(),
                ))
            },
            &shutdown_handle,
            TEST_DEADLINE,
        )
        .await;

        assert_eq!(exit_code, ExitCode::FAILURE);
        Ok(())
    }

    #[tokio::test]
    async fn server_missing_the_deadline_fails() -> anyhow::Result<()> {
        let shutdown_handle = ShutdownHandle::new();
        shutdown_handle.shut_down_gracefully();

        let exit_code = tokio::time::timeout(
            TEST_DEADLINE * 10,
            wait_for_shutdown(
                tokio::spawn(std::future::pending()),
                async { Ok(()) },
                &shutdown_handle,
                TEST_DEADLINE,
            ),
        )
        .await?;

        assert_eq!(exit_code, ExitCode::FAILURE);
        Ok(())
    }

    #[tokio::test]
    async fn server_drain_and_runtime_stop_share_the_deadline() -> anyhow::Result<()> {
        let shutdown_handle = ShutdownHandle::new();
        shutdown_handle.shut_down_gracefully();
        let phase_duration = TEST_DEADLINE * 3 / 4;

        let exit_code = tokio::time::timeout(
            TEST_DEADLINE * 10,
            wait_for_shutdown(
                tokio::spawn(async move {
                    tokio::time::sleep(phase_duration).await;
                    Ok(())
                }),
                async move {
                    tokio::time::sleep(phase_duration).await;
                    Ok(())
                },
                &shutdown_handle,
                TEST_DEADLINE,
            ),
        )
        .await?;

        assert_eq!(exit_code, ExitCode::FAILURE);
        Ok(())
    }
}
