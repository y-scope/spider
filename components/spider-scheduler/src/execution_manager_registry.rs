//! Execution manager registry service.

use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
    time::{Duration, Instant},
};

use serde::Deserialize;
use spider_core::types::id::{ExecutionManagerId, TaskAssignmentId};
use tokio::{
    select,
    sync::{Mutex, RwLock, mpsc::UnboundedSender},
};
use tokio_util::sync::CancellationToken;

use crate::TaskAssignment;

#[derive(thiserror::Error, Debug)]
pub enum ExecutionManagerRegistryError {
    #[error("task assignment {1} not found for execution manager {0}")]
    TaskAssignmentNotFound(ExecutionManagerId, TaskAssignmentId),

    #[error("execution manager not found: {0}")]
    EmNotFound(ExecutionManagerId),
}

#[derive(Clone, Deserialize)]
pub struct ExecutionManagerRegistryConfig {
    pub dead_em_cutoff_sec: u64,
    pub liveness_tracking_interval_ms: u64,
}

/// Execution manager registry service.
#[derive(Clone)]
pub struct ExecutionManagerRegistry {
    inner: Arc<ExecutionManagerRegistryInner>,
}

impl ExecutionManagerRegistry {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The newly created execution manager registry.
    #[must_use]
    pub fn new(
        config: &ExecutionManagerRegistryConfig,
        cancellation_token: CancellationToken,
        reschedule_queue_sender: UnboundedSender<TaskAssignment>,
    ) -> Self {
        let dead_em_cutoff = Duration::from_secs(config.dead_em_cutoff_sec);
        let liveness_tracking_interval =
            Duration::from_millis(config.liveness_tracking_interval_ms);
        Self {
            inner: Arc::new(ExecutionManagerRegistryInner {
                em_table: RwLock::new(HashMap::new()),
                cancellation_token,
                dead_em_cutoff,
                liveness_tracking_interval,
                reschedule_queue_sender,
            }),
        }
    }

    /// Assigns a task to an execution manager.
    ///
    /// The execution manager will be added to the registry if it is not already present.
    pub async fn assign(&self, em_id: ExecutionManagerId, task_assignment: TaskAssignment) {
        self.upsert_em_state(
            em_id,
            |state| {
                state
                    .task_assignments
                    .insert(task_assignment.id, task_assignment);
                state.refresh_liveness();
            },
            || ExecutionManagerStateInner {
                last_update: Instant::now(),
                task_assignments: HashMap::from([(task_assignment.id, task_assignment)]),
            },
        )
        .await;
    }

    /// Completes the task assignment.
    ///
    /// The registry is not aware of whether the assignment terminated on success or failure: this
    /// is updated to the storage service only.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ExecutionManagerRegistryError::EmNotFound`] if the execution manager is not registered
    ///   or already dead.
    /// * Forwards [`ExecutionManagerState::remove_assignment`]'s return values on failure.
    pub async fn complete(
        &self,
        em_id: ExecutionManagerId,
        task_assignment_id: TaskAssignmentId,
    ) -> Result<(), ExecutionManagerRegistryError> {
        if let Some(state) = self.inner.em_table.read().await.get(&em_id) {
            state.remove_assignment(task_assignment_id).await
        } else {
            Err(ExecutionManagerRegistryError::EmNotFound(em_id))
        }
    }

    /// Updates the heartbeat of the given execution manager.
    ///
    /// The execution manager will be added to the registry if this is its first heartbeat.
    pub async fn update_heartbeat(&self, em_id: ExecutionManagerId) {
        self.upsert_em_state(em_id, ExecutionManagerStateInner::refresh_liveness, || {
            ExecutionManagerStateInner {
                last_update: Instant::now(),
                task_assignments: HashMap::new(),
            }
        })
        .await;
    }

    /// Marks an execution manager as dead.
    ///
    /// This method does not remove the execution manager from the registry immediately. Instead,
    /// it cancels the liveness tracker running in the background, which removes the registry after
    /// receiving the cancellation signal.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ExecutionManagerRegistryError::EmNotFound`] if the execution manager is not registered
    ///   or already dead.
    pub async fn mark_as_dead(
        &self,
        em_id: ExecutionManagerId,
    ) -> Result<(), ExecutionManagerRegistryError> {
        self.inner.em_table.read().await.get(&em_id).map_or(
            Err(ExecutionManagerRegistryError::EmNotFound(em_id)),
            |state| {
                state.cancel_liveness_tracker();
                Ok(())
            },
        )
    }

    /// Applies an inplace update to an execution manager's state, registering the execution manager
    /// first if it is not yet present.
    ///
    /// The relevant table entry is held under a table-level lock for the entire duration of either
    /// closure. This prevents the state from being concurrently torn down by
    /// [`Self::track_liveness`]: an update can never land on a state that has already been removed
    /// from the registry.
    ///
    /// # Type Parameters
    ///
    /// * `OnExisting` - A closure applied to the mutable state of an already-registered execution
    ///   manager.
    /// * `OnVacant` - A closure producing the initial state for a newly registered execution
    ///   manager.
    async fn upsert_em_state<
        OnExisting: FnOnce(&mut ExecutionManagerStateInner),
        OnVacant: FnOnce() -> ExecutionManagerStateInner,
    >(
        &self,
        em_id: ExecutionManagerId,
        on_existing: OnExisting,
        on_vacant: OnVacant,
    ) {
        if let Some(state) = self.inner.em_table.read().await.get(&em_id) {
            on_existing(&mut *state.inner.lock().await);
            return;
        }

        // Register the execution manager under the write lock, double-checking for a concurrent
        // insertion that may have happened after the read lock was released.
        match self.inner.em_table.write().await.entry(em_id) {
            Entry::Vacant(entry) => {
                let liveness_tracker_cancellation_token =
                    self.inner.cancellation_token.child_token();
                let state = ExecutionManagerState {
                    id: em_id,
                    inner: Mutex::new(on_vacant()),
                    liveness_tracker_cancellation_token: liveness_tracker_cancellation_token
                        .clone(),
                };
                entry.insert(state);
                tokio::spawn(
                    self.clone()
                        .track_liveness(em_id, liveness_tracker_cancellation_token),
                );
            }
            Entry::Occupied(entry) => on_existing(&mut *entry.get().inner.lock().await),
        }
    }

    /// A background task for tracking the liveness of an execution manager.
    ///
    /// On each tick it checks whether the execution manager identified by `em_id` is still alive,
    /// stopping once the manager is found dead or `cancellation_token` is cancelled. On stopping,
    /// it removes the manager from the registry and reschedules its outstanding task assignments.
    ///
    /// This brackground task guarantees the following:
    ///
    /// * The execution manager is inserted into the registry before this coroutine is spawned.
    /// * This coroutine is the single source of the manager's removal from the registry.
    async fn track_liveness(
        self,
        em_id: ExecutionManagerId,
        cancellation_token: CancellationToken,
    ) {
        let mut tracking = tokio::time::interval(self.inner.liveness_tracking_interval);
        loop {
            select! {
            () = cancellation_token.cancelled() => {
                tracing::info!(em_id = % em_id, "Liveness tracker cancelled.");
                break;
            }
            _ = tracking.tick() => {
                    if let Some(state) = self.inner.em_table.read().await.get(&em_id) {
                        if state.is_alive(self.inner.dead_em_cutoff).await {
                            continue;
                        }
                        tracing::info!(
                            em_id = % em_id,
                            "Liveness tracker detects execution manager no longer alive."
                        );
                        break;
                    }
                    tracing::error!(
                        em_id = % em_id,
                        "Execution manager no longer exists. The liveness tracker is corrupted."
                    );
                    self.inner.cancellation_token.cancel();
                    return;
                }
            }
        }

        let Some(state) = self.inner.em_table.write().await.remove(&em_id) else {
            tracing::error!(
                em_id = % em_id,
                "Execution manager no longer exists. The liveness tracker is corrupted."
            );
            self.inner.cancellation_token.cancel();
            return;
        };
        tracing::info!(em_id = % em_id, "Execution manager removed from the registry.");
        for task_assignment in state.inner.lock().await.task_assignments.values() {
            if self
                .inner
                .reschedule_queue_sender
                .send(*task_assignment)
                .is_err()
            {
                // The re-schedule queue would only be closed if the scheduler service is shutting
                // down. Log the warning for observability purposes.
                tracing::warn!(
                    em_id = % em_id,
                    "Reschedule queue has been closed. Task assignments from the dead EM ignored."
                );
                break;
            }
        }
    }
}

/// Internal data structure for the execution manager registry.
struct ExecutionManagerRegistryInner {
    em_table: RwLock<HashMap<ExecutionManagerId, ExecutionManagerState>>,
    cancellation_token: CancellationToken,
    dead_em_cutoff: Duration,
    liveness_tracking_interval: Duration,
    reschedule_queue_sender: UnboundedSender<TaskAssignment>,
}

/// The state of an execution manager tracked by the registry.
struct ExecutionManagerState {
    id: ExecutionManagerId,
    liveness_tracker_cancellation_token: CancellationToken,
    inner: Mutex<ExecutionManagerStateInner>,
}

impl ExecutionManagerState {
    /// Removes a task assignment from the execution manager's state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ExecutionManagerRegistryError::TaskAssignmentNotFound`] if the task assignment is not
    ///   present in the state.
    async fn remove_assignment(
        &self,
        task_assignment_id: TaskAssignmentId,
    ) -> Result<(), ExecutionManagerRegistryError> {
        let mut state = self.inner.lock().await;
        if state.task_assignments.remove(&task_assignment_id).is_none() {
            return Err(ExecutionManagerRegistryError::TaskAssignmentNotFound(
                self.id,
                task_assignment_id,
            ));
        }
        state.refresh_liveness();
        drop(state);
        Ok(())
    }

    /// # Returns
    ///
    /// Whether the execution manager is considered alive with respect to the given cutoff.
    async fn is_alive(&self, cutoff: Duration) -> bool {
        self.inner.lock().await.last_update.elapsed() < cutoff
    }

    /// Cancels the liveness tracker for the execution manager.
    fn cancel_liveness_tracker(&self) {
        self.liveness_tracker_cancellation_token.cancel();
    }
}

/// The mutable, mutex-guarded portion of an execution manager's state.
struct ExecutionManagerStateInner {
    last_update: Instant,
    task_assignments: HashMap<TaskAssignmentId, TaskAssignment>,
}

impl ExecutionManagerStateInner {
    /// Refreshes the last update time.
    fn refresh_liveness(&mut self) {
        self.last_update = Instant::now();
    }
}
