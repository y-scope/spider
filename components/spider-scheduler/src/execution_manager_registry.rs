//! Execution manager registry service.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use serde::Deserialize;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::TaskAssignmentId;
use tokio::select;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

use crate::TaskAssignment;

#[derive(thiserror::Error, Debug)]
pub enum ExecutionManagerRegistryError {
    #[error("task assignment {1} not found for execution manager {0}")]
    TaskAssignmentNotFound(ExecutionManagerId, TaskAssignmentId),

    #[error("execution manager not found: {0}")]
    EmNotFound(ExecutionManagerId),
}

#[derive(Clone, Debug, Deserialize)]
pub struct ExecutionManagerRegistryConfig {
    /// The time, in seconds, that an execution manager is considered dead without receiving any
    /// heartbeat.
    pub dead_em_cutoff_sec: NonZeroU64,

    /// The time interval, in milliseconds, between liveness checks.
    pub liveness_tracking_interval_ms: NonZeroU64,
}

impl Default for ExecutionManagerRegistryConfig {
    fn default() -> Self {
        Self {
            dead_em_cutoff_sec: NonZeroU64::new(30).expect("30 is non-zero"),
            liveness_tracking_interval_ms: NonZeroU64::new(1000).expect("1000 is non-zero"),
        }
    }
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
        let dead_em_cutoff = Duration::from_secs(config.dead_em_cutoff_sec.get());
        let liveness_tracking_interval =
            Duration::from_millis(config.liveness_tracking_interval_ms.get());
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
    /// This background task guarantees the following:
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use spider_core::types::id::JobId;
    use spider_core::types::id::ResourceGroupId;
    use spider_core::types::id::TaskId;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::sync::mpsc::{self};
    use tokio::time::timeout;
    use tokio_util::task::TaskTracker;

    use super::*;

    /// The maximum time to wait for rescheduled assignments before failing a test.
    const RESCHEDULE_TIMEOUT: Duration = Duration::from_secs(2);

    /// The cutoff used by timeout-driven tests. One second is the smallest the seconds-granularity
    /// config supports, so these tests run in real time on the order of a second.
    const LIVENESS_CUTOFF_SEC: u64 = 1;

    /// The tracking interval used by timeout-driven tests; short so that death is detected promptly
    /// after the cutoff elapses.
    const LIVENESS_INTERVAL_MS: u64 = 50;

    /// A generous upper bound for waiting on a timeout-driven reschedule or removal.
    const LIVENESS_TEST_TIMEOUT: Duration = Duration::from_secs(LIVENESS_CUTOFF_SEC * 4);

    /// Builds a registry with the given liveness configuration.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The registry.
    /// * The receiver end of the re-schedule queue.
    /// * The registry-level cancellation token.
    fn build_registry(
        dead_em_cutoff_sec: u64,
        liveness_tracking_interval_ms: u64,
    ) -> (
        ExecutionManagerRegistry,
        UnboundedReceiver<TaskAssignment>,
        CancellationToken,
    ) {
        let config = ExecutionManagerRegistryConfig {
            dead_em_cutoff_sec: NonZeroU64::new(dead_em_cutoff_sec)
                .expect("the cutoff should be non-zero"),
            liveness_tracking_interval_ms: NonZeroU64::new(liveness_tracking_interval_ms)
                .expect("the interval should be non-zero"),
        };
        let cancellation_token = CancellationToken::new();
        let (reschedule_queue_sender, reschedule_queue_receiver) = mpsc::unbounded_channel();
        let registry = ExecutionManagerRegistry::new(
            &config,
            cancellation_token.clone(),
            reschedule_queue_sender,
        );
        (registry, reschedule_queue_receiver, cancellation_token)
    }

    /// Builds a registry whose background liveness tracker never fires during a test (a one-hour
    /// cutoff with a one-minute interval), so that registration, completion, and teardown can be
    /// driven explicitly rather than by timeouts.
    ///
    /// # Returns
    ///
    /// The same tuple as [`build_registry`].
    fn build_test_registry() -> (
        ExecutionManagerRegistry,
        UnboundedReceiver<TaskAssignment>,
        CancellationToken,
    ) {
        build_registry(3600, 60_000)
    }

    /// Builds a task assignment whose ID is derived from `id`; the remaining fields are irrelevant
    /// to the registry and are randomized.
    ///
    /// # Returns
    ///
    /// The task assignment.
    fn build_assignment(id: u64) -> TaskAssignment {
        TaskAssignment {
            id: TaskAssignmentId::from(id),
            resource_group_id: ResourceGroupId::random(),
            job_id: JobId::random(),
            task_id: TaskId::Index(0),
        }
    }

    /// # Returns
    ///
    /// Whether the given execution manager is currently registered.
    async fn is_registered(registry: &ExecutionManagerRegistry, em_id: ExecutionManagerId) -> bool {
        registry.inner.em_table.read().await.contains_key(&em_id)
    }

    /// # Returns
    ///
    /// The set of task assignment IDs recorded for the given execution manager, or `None` if it is
    /// not registered.
    async fn assignment_ids(
        registry: &ExecutionManagerRegistry,
        em_id: ExecutionManagerId,
    ) -> Option<HashSet<TaskAssignmentId>> {
        let ids = registry
            .inner
            .em_table
            .read()
            .await
            .get(&em_id)?
            .inner
            .lock()
            .await
            .task_assignments
            .keys()
            .copied()
            .collect();
        Some(ids)
    }

    /// Polls until the given execution manager is no longer registered, panicking if it is still
    /// registered after `deadline`. Used to observe a timeout-driven removal that produces no
    /// rescheduling signal (an execution manager with no outstanding assignments).
    async fn wait_until_unregistered(
        registry: &ExecutionManagerRegistry,
        em_id: ExecutionManagerId,
        deadline: Duration,
    ) {
        timeout(deadline, async {
            while is_registered(registry, em_id).await {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the execution manager should be unregistered before the deadline");
    }

    #[tokio::test]
    async fn assign_registers_new_execution_manager() -> anyhow::Result<()> {
        let (registry, _reschedule_queue_receiver, _cancellation_token) = build_test_registry();
        let em_id = ExecutionManagerId::from(1);
        let task_assignment = build_assignment(1);

        registry.assign(em_id, task_assignment).await;

        assert!(is_registered(&registry, em_id).await);
        assert_eq!(
            assignment_ids(&registry, em_id).await,
            Some(HashSet::from([task_assignment.id]))
        );
        Ok(())
    }

    #[tokio::test]
    async fn update_heartbeat_registers_new_execution_manager() -> anyhow::Result<()> {
        let (registry, _reschedule_queue_receiver, _cancellation_token) = build_test_registry();
        let em_id = ExecutionManagerId::from(1);

        registry.update_heartbeat(em_id).await;

        assert!(is_registered(&registry, em_id).await);
        assert_eq!(assignment_ids(&registry, em_id).await, Some(HashSet::new()));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn assign_records_multiple_assignments() -> anyhow::Result<()> {
        const NUM_ASSIGNMENTS: u64 = 100;

        let (registry, _reschedule_queue_receiver, _cancellation_token) = build_test_registry();
        let em_id = ExecutionManagerId::from(1);
        let task_assignments: Vec<TaskAssignment> =
            (0..NUM_ASSIGNMENTS).map(build_assignment).collect();

        let task_tracker = TaskTracker::new();
        for &task_assignment in &task_assignments {
            let registry = registry.clone();
            task_tracker.spawn(async move { registry.assign(em_id, task_assignment).await });
        }
        task_tracker.close();
        task_tracker.wait().await;

        let expected_ids: HashSet<TaskAssignmentId> = task_assignments
            .iter()
            .map(|task_assignment| task_assignment.id)
            .collect();
        assert_eq!(assignment_ids(&registry, em_id).await, Some(expected_ids));

        for task_assignment in &task_assignments {
            registry
                .complete(em_id, task_assignment.id)
                .await
                .expect("completing a recorded assignment should succeed");
        }
        assert_eq!(assignment_ids(&registry, em_id).await, Some(HashSet::new()));
        Ok(())
    }

    #[tokio::test]
    async fn mark_as_dead_reschedules_outstanding_assignments() -> anyhow::Result<()> {
        const NUM_ASSIGNMENTS: u64 = 10;

        let (registry, mut reschedule_queue_receiver, _cancellation_token) = build_test_registry();
        let em_id = ExecutionManagerId::from(1);
        let task_assignments: Vec<TaskAssignment> =
            (0..NUM_ASSIGNMENTS).map(build_assignment).collect();

        for &task_assignment in &task_assignments {
            registry.assign(em_id, task_assignment).await;
        }

        registry
            .mark_as_dead(em_id)
            .await
            .expect("marking a registered execution manager as dead should succeed");

        // Teardown runs asynchronously after the cancellation; collect every rescheduled
        // assignment.
        let mut rescheduled = HashSet::new();
        for _ in 0..NUM_ASSIGNMENTS {
            let task_assignment = timeout(RESCHEDULE_TIMEOUT, reschedule_queue_receiver.recv())
                .await
                .expect("a reschedule should arrive before the timeout")
                .expect("the reschedule queue should remain open");
            rescheduled.insert(task_assignment.id);
        }

        let expected_ids: HashSet<TaskAssignmentId> = task_assignments
            .iter()
            .map(|task_assignment| task_assignment.id)
            .collect();
        assert_eq!(rescheduled, expected_ids);
        assert!(!is_registered(&registry, em_id).await);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_assigns_racing_mark_as_dead_reschedule_every_assignment()
    -> anyhow::Result<()> {
        const NUM_ASSIGNMENTS: u64 = 100;

        // The rescheduling timeout (`LIVENESS_TEST_TIMEOUT`) must comfortably exceed the cutoff,
        // since the assignments that re-register the execution manager after teardown are only
        // reclaimed once it times out.
        let (registry, mut reschedule_queue_receiver, _cancellation_token) =
            build_registry(LIVENESS_CUTOFF_SEC, LIVENESS_INTERVAL_MS);
        let em_id = ExecutionManagerId::from(1);
        let task_assignments: Vec<TaskAssignment> =
            (0..NUM_ASSIGNMENTS).map(build_assignment).collect();

        // Assign all tasks to the same execution manager concurrently, marking it dead in between.
        // An assign that races ahead of the teardown re-registers the execution manager under a
        // fresh tracker; that incarnation is later removed by the liveness timeout rather than by
        // the cancellation.
        let task_tracker = TaskTracker::new();
        for &task_assignment in &task_assignments {
            let registry = registry.clone();
            task_tracker.spawn(async move { registry.assign(em_id, task_assignment).await });
        }
        // The mark may no-op if it observes the execution manager before the first assign registers
        // it; either way every incarnation eventually tears down.
        registry.mark_as_dead(em_id).await.ok();
        task_tracker.close();
        task_tracker.wait().await;

        // Every assignment lands in exactly one incarnation, and each incarnation reschedules its
        // assignments exactly once, so the queue ends up holding each assignment exactly once --
        // none lost to the teardown race, none duplicated.
        let mut rescheduled = HashSet::new();
        for _ in 0..NUM_ASSIGNMENTS {
            let task_assignment = timeout(LIVENESS_TEST_TIMEOUT, reschedule_queue_receiver.recv())
                .await
                .expect("a reschedule should arrive before the timeout")
                .expect("the reschedule queue should remain open");
            rescheduled.insert(task_assignment.id);
        }

        let expected_ids: HashSet<TaskAssignmentId> = task_assignments
            .iter()
            .map(|task_assignment| task_assignment.id)
            .collect();
        assert_eq!(rescheduled, expected_ids);

        // Exactly `NUM_ASSIGNMENTS` were rescheduled: nothing remains once all are drained.
        assert!(reschedule_queue_receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn idle_execution_manager_is_removed_after_cutoff() -> anyhow::Result<()> {
        let (registry, mut reschedule_queue_receiver, _cancellation_token) =
            build_registry(LIVENESS_CUTOFF_SEC, LIVENESS_INTERVAL_MS);
        let em_id = ExecutionManagerId::from(1);
        let task_assignment = build_assignment(1);

        registry.assign(em_id, task_assignment).await;

        // With no further heartbeats, the tracker removes the execution manager after the cutoff
        // and reschedules its outstanding assignment.
        let rescheduled = timeout(LIVENESS_TEST_TIMEOUT, reschedule_queue_receiver.recv())
            .await
            .expect("a reschedule should arrive before the timeout")
            .expect("the reschedule queue should remain open");
        assert_eq!(rescheduled.id, task_assignment.id);
        assert!(!is_registered(&registry, em_id).await);
        Ok(())
    }

    #[tokio::test]
    async fn idle_heartbeat_only_execution_manager_is_removed_after_cutoff() -> anyhow::Result<()> {
        let (registry, mut reschedule_queue_receiver, _cancellation_token) =
            build_registry(LIVENESS_CUTOFF_SEC, LIVENESS_INTERVAL_MS);
        let em_id = ExecutionManagerId::from(1);

        registry.update_heartbeat(em_id).await;
        assert!(is_registered(&registry, em_id).await);
        wait_until_unregistered(&registry, em_id, LIVENESS_TEST_TIMEOUT).await;
        assert!(reschedule_queue_receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn heartbeats_keep_execution_manager_alive() -> anyhow::Result<()> {
        const REFRESH_INTERVAL: Duration = Duration::from_millis(250);
        const NUM_REFRESHES: u32 = 6;

        let (registry, mut reschedule_queue_receiver, _cancellation_token) =
            build_registry(LIVENESS_CUTOFF_SEC, LIVENESS_INTERVAL_MS);
        let em_id = ExecutionManagerId::from(1);

        for _ in 0..NUM_REFRESHES {
            registry.update_heartbeat(em_id).await;
            tokio::time::sleep(REFRESH_INTERVAL).await;
            assert!(is_registered(&registry, em_id).await);
        }
        wait_until_unregistered(&registry, em_id, LIVENESS_TEST_TIMEOUT).await;
        assert!(reschedule_queue_receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn assign_and_complete_keep_execution_manager_alive() -> anyhow::Result<()> {
        const REFRESH_INTERVAL: Duration = Duration::from_millis(250);
        const NUM_TASK_ASSIGNMENTS: u64 = 3;

        let (registry, mut reschedule_queue_receiver, _cancellation_token) =
            build_registry(LIVENESS_CUTOFF_SEC, LIVENESS_INTERVAL_MS);
        let em_id = ExecutionManagerId::from(1);

        for i in 0..NUM_TASK_ASSIGNMENTS {
            let task_assignment = build_assignment(i);
            registry.assign(em_id, task_assignment).await;
            tokio::time::sleep(REFRESH_INTERVAL).await;
            assert!(is_registered(&registry, em_id).await);

            registry
                .complete(em_id, task_assignment.id)
                .await
                .expect("complete should succeed");
            tokio::time::sleep(REFRESH_INTERVAL).await;
            assert!(is_registered(&registry, em_id).await);
        }
        assert!(reschedule_queue_receiver.try_recv().is_err());
        wait_until_unregistered(&registry, em_id, LIVENESS_TEST_TIMEOUT).await;
        assert!(reschedule_queue_receiver.try_recv().is_err());
        Ok(())
    }
}
