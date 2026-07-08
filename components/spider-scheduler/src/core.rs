//! The abstract core of a Spider scheduler.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use async_trait::async_trait;
use spider_core::types::id::TaskAssignmentId;

use crate::dispatch_queue::DispatchQueueSink;
use crate::error::SchedulerError;
use crate::storage_client::SchedulerStorageClient;

/// Single-source ID issuer for creating globally unique IDs for task assignments.
pub struct TaskAssignmentIdIssuer {
    id: Arc<AtomicU64>,
}

impl Default for TaskAssignmentIdIssuer {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskAssignmentIdIssuer {
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: Arc::new(AtomicU64::new(0)),
        }
    }

    #[must_use]
    pub fn next(&self) -> TaskAssignmentId {
        TaskAssignmentId::from(self.id.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
}

/// An abstracted core for a scheduling algorithm.
///
/// A core owns its decision loop: it polls the inbound queue through a [`SchedulerStorageClient`],
/// applies its algorithm (reading storage as needed for placement), and writes assignments to a
/// [`DispatchQueueSink`]. Modeling the algorithm as a trait lets different scheduling strategies
/// share the same runtime entry point.
#[async_trait]
pub trait SchedulerCore: Send {
    /// The dispatch sink the core writes assignments to.
    type Sink: DispatchQueueSink;

    /// The storage client used by the core to poll and read for placement decisions.
    type StorageClient: SchedulerStorageClient;

    /// Runs the scheduling loop until `cancellation_token` is triggered.
    ///
    /// The core polls the inbound queue through `storage_client`, applies its scheduling algorithm,
    /// and writes assignments to `sink`, repeating until `cancellation_token` is fired, at which
    /// point it returns.
    ///
    /// # Parameters
    ///
    /// * `storage_client` - The storage client used to poll the inbound queue and read state for
    ///   placement.
    /// * `sink` - The dispatch sink that assignments are written to.
    /// * `id_issuer` - The single-source ID issuer for creating globally unique IDs for task
    ///   assignments.
    /// * `cancellation_token` - The token to signal the scheduling loop to stop.
    ///
    /// # Errors
    ///
    /// Returns a [`SchedulerError`] instance indicating an irrecoverable error.
    async fn run(
        self: Box<Self>,
        storage_client: Self::StorageClient,
        sink: Self::Sink,
        id_issuer: TaskAssignmentIdIssuer,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Result<(), SchedulerError>;
}
