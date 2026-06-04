//! The abstract core of a Spider scheduler.

use async_trait::async_trait;

use crate::{
    dispatch_queue::DispatchQueueSink,
    error::SchedulerError,
    storage_client::SchedulerStorageClient,
};

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
    /// * `cancellation_token` - The token to signal the scheduling loop to stop.
    ///
    /// # Errors
    ///
    /// Returns a [`SchedulerError`] instance indicating an irrecoverable error.
    async fn run(
        self,
        storage_client: Self::StorageClient,
        sink: Self::Sink,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Result<(), SchedulerError>;
}
