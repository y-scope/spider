use std::net::IpAddr;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use spider_utils::config::EndpointConfig;

use crate::runtime::RuntimeConfig;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// The IP address the execution manager hosts on.
    pub host: IpAddr,

    /// The endpoint of the storage gRPC server.
    pub storage: EndpointConfig,

    /// The endpoint of the scheduler gRPC server.
    pub scheduler: EndpointConfig,

    /// Liveness configuration.
    pub liveness: LivenessConfig,

    /// Task executor configuration.
    pub task_executor: TaskExecutorConfig,

    /// The number of connections each gRPC client pool eagerly establishes.
    ///
    /// Must be greater than zero.
    pub connection_pool_size: NonZeroUsize,

    /// How long, in milliseconds, the scheduler is asked to block each polling request before
    /// returning an empty response on task dispatching.
    pub scheduler_poll_wait_ms: u64,
}

impl Config {
    /// Builds the [`RuntimeConfig`] consumed by the runtime from this configuration.
    ///
    /// # Returns
    ///
    /// The derived [`RuntimeConfig`].
    #[must_use]
    pub fn runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig {
            em_ip: self.host,
            heartbeat_interval: Duration::from_secs(
                self.liveness.storage_heartbeat_interval_sec.get(),
            ),
            scheduler_heartbeat_interval: Duration::from_secs(
                self.liveness.scheduler_heartbeat_interval_sec.get(),
            ),
            scheduler_poll_wait_ms: self.scheduler_poll_wait_ms,
            executor_binary_path: self.task_executor.bin_path.clone(),
            package_dir: self.task_executor.package_dir.clone(),
            log_dir: self.task_executor.log_dir.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct LivenessConfig {
    /// The interval, in seconds, between liveness heartbeats sent to the storage.
    ///
    /// Must be greater than zero.
    pub storage_heartbeat_interval_sec: NonZeroU64,

    /// The interval, in seconds, between scheduler heartbeats sent to the scheduler.
    ///
    /// Must be greater than zero.
    pub scheduler_heartbeat_interval_sec: NonZeroU64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TaskExecutorConfig {
    /// Absolute path the `spider-task-executor` binary the process pool spawns.
    pub bin_path: PathBuf,

    /// Directory of TDL packages exposed to executors via `SPIDER_TDL_PACKAGE_DIR`.
    pub package_dir: PathBuf,

    /// Directory the process pool writes per-executor stderr logs into.
    pub log_dir: PathBuf,
}
