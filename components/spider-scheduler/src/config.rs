//! Scheduler service configuration.

use std::net::IpAddr;
use std::num::NonZeroUsize;

use serde::Deserialize;
use spider_utils::config::EndpointConfig;

use crate::core::SchedulerCore;
use crate::core_impl::ResourceGroupRoundRobinConfig;
use crate::core_impl::RoundRobinConfig;
use crate::runtime::RuntimeConfig;
use crate::storage_client::SchedulerStorageClient;

/// Top-level configuration for the scheduler gRPC server.
#[derive(Clone, Debug, Deserialize)]
pub struct ServerConfig {
    /// The IP address the gRPC server listens on.
    pub host: IpAddr,

    /// The port the gRPC server listens on.
    pub port: u16,

    /// The storage service gRPC endpoint.
    pub storage_endpoint: EndpointConfig,

    /// The number of connections each gRPC client pool eagerly establishes.
    ///
    /// Must be greater than zero.
    pub connection_pool_size: NonZeroUsize,

    /// The scheduler runtime configuration.
    pub runtime: RuntimeConfig,
}

/// The configuration that selects and configures the scheduler core's scheduling policy.
#[derive(Clone, Debug, Deserialize)]
#[serde(
    tag = "policy",
    content = "config",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SchedulerConfig {
    /// The round-robin scheduling algorithm.
    RoundRobin(RoundRobinConfig),

    /// The resource-group-aware round-robin scheduling algorithm.
    ResourceGroupRoundRobin(ResourceGroupRoundRobinConfig),
}

impl SchedulerConfig {
    /// Creates a ready-to-run scheduler core from the selected configuration.
    ///
    /// # Type Parameters
    ///
    /// * `SchedulerStorageClientType` - The storage client the core polls and registers through.
    ///
    /// # Returns
    ///
    /// A boxed [`SchedulerCore`] configured by the selected variant.
    #[must_use]
    pub fn make_core<SchedulerStorageClientType: SchedulerStorageClient + 'static>(
        self,
    ) -> Box<dyn SchedulerCore<StorageClient = SchedulerStorageClientType>> {
        match self {
            Self::RoundRobin(config) => Box::new(config.make_core::<SchedulerStorageClientType>()),
            Self::ResourceGroupRoundRobin(config) => {
                Box::new(config.make_core::<SchedulerStorageClientType>())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::bail;

    use super::*;

    #[test]
    fn deserialize_resource_group_round_robin_policy() -> anyhow::Result<()> {
        const YAML: &str = "
policy: resource_group_round_robin
config:
  dispatch_queue_capacity: 256
  active_job_list_capacity: 16
  ready_task_capacity: 1048576
  commit_ready_task_capacity: 128
  cleanup_ready_task_capacity: 64
  storage_poll_timeout_ms: 10
  tick_interval_ms: 5
  finalized_job_expiration_timeout_sec: 300
";

        let SchedulerConfig::ResourceGroupRoundRobin(config) = yaml_serde::from_str(YAML)? else {
            bail!("the policy should select the resource-group-aware round-robin core");
        };
        assert_eq!(config.dispatch_queue_capacity.get(), 256);
        assert_eq!(config.active_job_list_capacity.get(), 16);
        assert_eq!(config.ready_task_capacity.get(), 1_048_576);
        assert_eq!(config.commit_ready_task_capacity.get(), 128);
        assert_eq!(config.cleanup_ready_task_capacity.get(), 64);
        assert_eq!(config.storage_poll_timeout_ms, 10);
        assert_eq!(config.tick_interval_ms.get(), 5);
        assert_eq!(config.finalized_job_expiration_timeout_sec, 300);
        Ok(())
    }

    #[test]
    fn deserialize_round_robin_policy() -> anyhow::Result<()> {
        const YAML: &str = "
policy: round_robin
config:
  active_job_queue_capacity: 16
  dispatch_queue_capacity: 32
  ready_task_capacity: 1048576
  commit_ready_task_capacity: 128
  cleanup_ready_task_capacity: 64
  storage_poll_timeout_ms: 10
  tick_interval_ms: 5
  finalizing_job_expiration_timeout_sec: 300
";

        let SchedulerConfig::RoundRobin(config) = yaml_serde::from_str(YAML)? else {
            bail!("the policy should select the round-robin core");
        };
        assert_eq!(config.active_job_queue_capacity.get(), 16);
        assert_eq!(config.dispatch_queue_capacity.get(), 32);
        assert_eq!(config.ready_task_capacity.get(), 1_048_576);
        assert_eq!(config.commit_ready_task_capacity.get(), 128);
        assert_eq!(config.cleanup_ready_task_capacity.get(), 64);
        assert_eq!(config.storage_poll_timeout_ms, 10);
        assert_eq!(config.tick_interval_ms.get(), 5);
        assert_eq!(config.finalizing_job_expiration_timeout_sec, 300);
        Ok(())
    }
}
