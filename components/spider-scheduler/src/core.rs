use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    dispatch::DispatchSink,
    error::SchedulerError,
    storage_client::SchedulerStorageClient,
};

/// A cancellation handle used to signal a running [`SchedulerCore`] to stop.
///
/// Cancelling the token causes [`SchedulerCore::run`] to break out of its scheduling loop and
/// return.
pub type ShutdownToken = tokio_util::sync::CancellationToken;

/// A pluggable scheduling algorithm.
///
/// A core owns its decision loop: it polls the inbound queue through a [`SchedulerStorageClient`],
/// applies its algorithm (reading storage as needed for placement), and writes assignments to a
/// [`DispatchSink`]. Modeling the algorithm as a trait lets different scheduling strategies share
/// the same runtime entry point.
#[async_trait]
pub trait SchedulerCore: Send {
    /// The storage client the core polls and reads for placement decisions.
    type Storage: SchedulerStorageClient;

    /// The dispatch sink the core writes assignments to.
    type Sink: DispatchSink;

    /// Runs the scheduling loop until `shutdown` is triggered.
    ///
    /// The core polls the inbound queue through `storage`, applies its scheduling algorithm, and
    /// writes assignments to `sink`, repeating until `shutdown` is cancelled, at which point it
    /// returns.
    ///
    /// # Parameters
    ///
    /// * `storage` - The storage client used to poll the inbound queue and read state for
    ///   placement.
    /// * `sink` - The dispatch sink that assignments are written to.
    /// * `shutdown` - The token that, once cancelled, signals the loop to stop and return.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError`] if the scheduling loop fails irrecoverably, e.g. the storage client or
    ///   dispatch sink fails.
    async fn run(
        &mut self,
        storage: Arc<Self::Storage>,
        sink: Arc<Self::Sink>,
        shutdown: ShutdownToken,
    ) -> Result<(), SchedulerError>;
}
