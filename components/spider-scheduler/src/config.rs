//! Scheduler service configuration.

use std::num::NonZeroUsize;

use serde::Deserialize;
use spider_utils::config::EndpointConfig;

use crate::{
    core::SchedulerCore,
    core_impl::RoundRobinConfig,
    dispatch_queue::DispatchQueueSink,
    runtime::RuntimeConfig,
    storage_client::SchedulerStorageClient,
};

/// Top-level configuration for the scheduler gRPC server.
#[derive(Clone, Debug, Deserialize)]
pub struct ServerConfig {
    /// The storage service gRPC endpoint.
    pub storage_endpoint: EndpointConfig,

    /// The number of connections each gRPC client pool eagerly establishes.
    ///
    /// Must be greater than zero.
    pub storage_connection_pool_size: NonZeroUsize,

    /// The scheduler runtime configuration (also supplies the gRPC listen host/port).
    pub runtime: RuntimeConfig,
}

/// The configuration that selects and configures the scheduler core's scheduling algorithm.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerConfig {
    /// The round-robin scheduling algorithm.
    RoundRobin(RoundRobinConfig),
}

impl SchedulerConfig {
    /// Creates a ready-to-run scheduler core from the selected configuration.
    ///
    /// # Type Parameters
    ///
    /// * `SchedulerStorageClientType` - The storage client the core polls and registers through.
    /// * `DispatchQueueSinkType` - The dispatch sink that task assignments are written to.
    ///
    /// # Returns
    ///
    /// A boxed [`SchedulerCore`] configured by the selected variant.
    #[must_use]
    pub fn make_core<
        SchedulerStorageClientType: SchedulerStorageClient + 'static,
        DispatchQueueSinkType: DispatchQueueSink + 'static,
    >(
        self,
    ) -> Box<
        dyn SchedulerCore<Sink = DispatchQueueSinkType, StorageClient = SchedulerStorageClientType>,
    > {
        match self {
            Self::RoundRobin(config) => {
                Box::new(config.make_core::<SchedulerStorageClientType, DispatchQueueSinkType>())
            }
        }
    }

    /// # Returns
    ///
    /// The dispatch queue capacity of the selected variant.
    #[must_use]
    pub const fn dispatch_queue_capacity(&self) -> std::num::NonZeroUsize {
        match self {
            Self::RoundRobin(config) => config.dispatch_queue_capacity,
        }
    }
}
