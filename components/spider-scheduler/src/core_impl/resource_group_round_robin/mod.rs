//! Resource-group-aware round-robin scheduler.
//!
//! The core is a single-threaded, tick-based loop that makes scheduling decisions using two levels
//! of round-robin: the outer level interleaves resource groups, while the inner level interleaves
//! active jobs within each resource group.

mod dispatch_queue;
mod implementation;
mod inbound_queue_reader;
mod job_registry;
mod scheduling_state;

#[cfg(test)]
mod tests;

use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use dispatch_queue::DispatchQueueRegistry;
use implementation::RgRoundRobin;
use serde::Deserialize;
use spider_core::session::SessionTracker;
use spider_core::types::id::SessionId;
use tokio_util::sync::CancellationToken;

use crate::core::SchedulerCore;
use crate::core::TaskAssignmentIdIssuer;
use crate::dispatch_queue::SharedDispatchQueueHandle;
use crate::error::SchedulerError;
use crate::storage_client::SchedulerStorageClient;
use crate::types::TaskAssignment;

/// The configuration of the resource-group-aware round-robin scheduler core.
#[derive(Clone, Debug, Deserialize)]
pub struct ResourceGroupRoundRobinConfig {
    /// The number of active jobs each resource group may hold, applied per group rather than as a
    /// global budget.
    pub active_job_list_capacity: NonZeroUsize,

    /// The total dispatch buffer size shared by all resource groups.
    pub dispatch_queue_capacity: NonZeroUsize,

    /// The capacity of the total pending ready tasks buffered in the scheduler.
    pub ready_task_capacity: NonZeroUsize,

    /// The capacity of the total pending commit-ready tasks buffered in the scheduler.
    pub commit_ready_task_capacity: NonZeroUsize,

    /// The capacity of the total pending cleanup-ready tasks buffered in the scheduler.
    pub cleanup_ready_task_capacity: NonZeroUsize,

    /// The maximum time (in milliseconds) that the scheduler will wait for the storage server to
    /// fill the inbound-queue reading request.
    pub storage_poll_timeout_ms: u64,

    /// The time (in milliseconds) that the scheduler will spend on each tick. If the tick spends
    /// less than the configured interval, the core will sleep for the remainder.
    pub tick_interval_ms: NonZeroU64,

    /// The time (in seconds) that a job may remain in the finalizing job table before the
    /// scheduler drops it from the table.
    pub finalizing_job_expiration_timeout_sec: u64,
}

impl Default for ResourceGroupRoundRobinConfig {
    fn default() -> Self {
        Self {
            active_job_list_capacity: NonZeroUsize::new(16)
                .expect("default value must be positive"),
            dispatch_queue_capacity: NonZeroUsize::new(32).expect("default value must be positive"),
            ready_task_capacity: NonZeroUsize::new(1048576)
                .expect("default value must be positive"),
            commit_ready_task_capacity: NonZeroUsize::new(256)
                .expect("default value must be positive"),
            cleanup_ready_task_capacity: NonZeroUsize::new(256)
                .expect("default value must be positive"),
            storage_poll_timeout_ms: 10,
            tick_interval_ms: NonZeroU64::new(5).expect("default value must be positive"),
            finalizing_job_expiration_timeout_sec: 300,
        }
    }
}

impl ResourceGroupRoundRobinConfig {
    /// Creates a ready-to-run scheduler core from the configuration.
    ///
    /// # Type Parameters
    ///
    /// * `SchedulerStorageClientType` - The storage client used to poll the inbound queue.
    ///
    /// # Returns
    ///
    /// A newly created resource-group-aware round-robin scheduler core, owning a freshly created
    /// dispatch queue registry.
    #[must_use]
    pub fn make_core<SchedulerStorageClientType: SchedulerStorageClient + 'static>(
        self,
    ) -> ResourceGroupRoundRobinCore<SchedulerStorageClientType> {
        let session_tracker = SessionTracker::new(SessionId::default());
        ResourceGroupRoundRobinCore {
            config: self,
            dispatch_queue_registry: DispatchQueueRegistry::new(session_tracker),
            _marker: std::marker::PhantomData,
        }
    }
}

/// The resource-group-aware round-robin implementation of [`SchedulerCore`], created from
/// [`ResourceGroupRoundRobinConfig::make_core`].
///
/// Holding an instance of this type guarantees the wrapped configuration has passed validation, so
/// the scheduling loop can trust its invariants without re-validating.
///
/// # Type Parameters
///
/// * `SchedulerStorageClientType` - The storage client used to poll the inbound queue.
pub struct ResourceGroupRoundRobinCore<SchedulerStorageClientType: SchedulerStorageClient + 'static>
{
    config: ResourceGroupRoundRobinConfig,
    dispatch_queue_registry: DispatchQueueRegistry,
    _marker: std::marker::PhantomData<SchedulerStorageClientType>,
}

#[async_trait]
impl<SchedulerStorageClientType: SchedulerStorageClient + 'static> SchedulerCore
    for ResourceGroupRoundRobinCore<SchedulerStorageClientType>
{
    type StorageClient = SchedulerStorageClientType;

    fn get_dispatch_queue_handle(&self) -> SharedDispatchQueueHandle {
        Arc::new(self.dispatch_queue_registry.clone())
    }

    async fn run(
        self: Box<Self>,
        storage_client: Self::StorageClient,
        reschedule_queue_reader: tokio::sync::mpsc::UnboundedReceiver<TaskAssignment>,
        id_issuer: TaskAssignmentIdIssuer,
        cancellation_token: CancellationToken,
    ) -> Result<(), SchedulerError> {
        let Self {
            config,
            dispatch_queue_registry,
            ..
        } = *self;
        RgRoundRobin::new(
            storage_client,
            dispatch_queue_registry,
            reschedule_queue_reader,
            id_issuer,
            cancellation_token,
            config,
        )
        .run()
        .await
    }
}
