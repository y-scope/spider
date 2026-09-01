//! The implementation of the resource-group-aware round-robin scheduler core. See the parent
//! module's documentation for the scheduling policy and configuration.

use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::time::Duration;

use serde::Deserialize;
use spider_core::session::SessionTracker;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskId;
use tokio::select;
use tokio_util::sync::CancellationToken;

use super::dispatch_queue::DispatchQueueRegistry;
use super::inbound_queue_reader::FinalizedJob;
use super::inbound_queue_reader::ReadyBatch;
use super::inbound_queue_reader::RgInboundPollState;
use super::inbound_queue_reader::RgInboundQueueReader;
use super::inbound_queue_reader::format_finalized_jobs;
use super::inbound_queue_reader::format_ready_job_batches;
use super::job_registry::JobKey;
use super::job_registry::JobRegistry;
use super::job_registry::UpsertOutcome;
use super::scheduling_state::FinalizeKind;
use super::scheduling_state::MakeAssignmentError;
use super::scheduling_state::RgSchedulingState;
use crate::core::TaskAssignmentIdIssuer;
use crate::error::SchedulerError;
use crate::storage_client::SchedulerStorageClient;
use crate::types::InboundEntry;
use crate::types::TaskAssignment;

/// The configuration of the resource-group-aware round-robin scheduler core.
#[derive(Clone, Debug, Deserialize)]
pub(super) struct RgRoundRobinConfig {
    /// The total dispatch buffer size shared by all resource groups.
    pub(super) dispatch_queue_capacity: NonZeroUsize,

    /// The number of active jobs each resource group may hold, applied per group rather than as a
    /// global budget.
    pub(super) active_job_list_capacity: NonZeroUsize,

    /// The capacity of the total pending ready tasks buffered in the scheduler.
    pub(super) ready_task_capacity: NonZeroUsize,

    /// The capacity of the total pending commit-ready tasks buffered in the scheduler.
    pub(super) commit_ready_task_capacity: NonZeroUsize,

    /// The capacity of the total pending cleanup-ready tasks buffered in the scheduler.
    pub(super) cleanup_ready_task_capacity: NonZeroUsize,

    /// The maximum time (in milliseconds) that the scheduler will wait for the storage server to
    /// fill the inbound-queue reading request.
    pub(super) storage_poll_timeout_ms: u64,

    /// The time (in milliseconds) that the scheduler will spend on each tick.
    pub(super) tick_interval_ms: NonZeroU64,
}

/// The resource-group-aware round-robin scheduler core created from a [`RgRoundRobinConfig`].
///
/// The core owns all of the state it decides with -- job entries in a generational arena, per
/// resource group scheduling states in an append-only vector, and the dispatch queue registry the
/// execution-manager-facing service reads from -- so nothing it holds across an await point is
/// thread-bound.
///
/// # Type Parameters
///
/// * `SchedulerStorageClientType` - The storage client used to poll the inbound queue.
///
/// # Note
///
/// All member variables are marked `pub(super)` to allow the test module to inspect the internal
/// states.
pub(super) struct RgRoundRobin<SchedulerStorageClientType: SchedulerStorageClient + 'static> {
    pub(super) global_task_set: HashSet<(JobId, TaskId)>,
    pub(super) finalized_jobs: HashSet<JobId>,
    pub(super) job_registry: JobRegistry,

    /// The scheduling states of every resource group the core has seen this session.
    ///
    /// Append-only within a session: a group is never removed individually, so a position in this
    /// vector is stable until [`Self::apply_session_bump`] flushes the whole of it.
    pub(super) rg_states: Vec<RgSchedulingState>,

    pub(super) rg_id_to_index_map: HashMap<ResourceGroupId, usize>,
    pub(super) active_rg_list: Vec<usize>,
    pub(super) last_served_rg: Option<ResourceGroupId>,

    pub(super) config: RgRoundRobinConfig,
    pub(super) dispatch_queue_registry: DispatchQueueRegistry,
    pub(super) session_tracker: SessionTracker,
    pub(super) id_issuer: TaskAssignmentIdIssuer,
    pub(super) inbound_queue_reader: RgInboundQueueReader<SchedulerStorageClientType>,
    pub(super) reschedule_queue_reader: tokio::sync::mpsc::UnboundedReceiver<TaskAssignment>,
    pub(super) cancellation_token: CancellationToken,
}

impl<SchedulerStorageClientType: SchedulerStorageClient + 'static>
    RgRoundRobin<SchedulerStorageClientType>
{
    /// Factory function.
    ///
    /// Creates a core owning a freshly created dispatch queue registry and the session tracker that
    /// stamps every group the registry creates.
    ///
    /// # Returns
    ///
    /// A newly created core with no buffered task and no in-flight inbound poll.
    pub(super) fn new(
        storage_client: SchedulerStorageClientType,
        reschedule_queue_reader: tokio::sync::mpsc::UnboundedReceiver<TaskAssignment>,
        id_issuer: TaskAssignmentIdIssuer,
        cancellation_token: CancellationToken,
        config: RgRoundRobinConfig,
    ) -> Self {
        let session_tracker = SessionTracker::new(SessionId::default());
        let dispatch_queue_registry = DispatchQueueRegistry::new(session_tracker.clone());
        Self {
            global_task_set: HashSet::new(),
            finalized_jobs: HashSet::new(),
            job_registry: JobRegistry::new(),
            rg_states: Vec::new(),
            rg_id_to_index_map: HashMap::new(),
            active_rg_list: Vec::new(),
            last_served_rg: None,
            config,
            dispatch_queue_registry,
            session_tracker,
            id_issuer,
            inbound_queue_reader: RgInboundQueueReader::new(storage_client),
            reschedule_queue_reader,
            cancellation_token,
        }
    }

    /// # Returns
    ///
    /// A handle over the core's dispatch queue registry, from which the execution-manager-facing
    /// service reads.
    pub(super) fn dispatch_queue_registry(&self) -> DispatchQueueRegistry {
        self.dispatch_queue_registry.clone()
    }

    /// Runs the scheduling loop until the cancellation token is triggered.
    ///
    /// Each iteration executes one [`Self::tick`] and then sleeps for the remainder of the
    /// configured tick interval.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::tick`]'s return values on failure.
    pub(super) async fn run(mut self) -> Result<(), SchedulerError> {
        tracing::info!(
            config = ? self.config,
            init_session_id = self.session_tracker.current(),
            "Resource-group-aware round-robin scheduler started."
        );
        let tick_interval = Duration::from_millis(self.config.tick_interval_ms.get());
        loop {
            let now = tokio::time::Instant::now();
            let cancellation_token = self.cancellation_token.clone();
            select! {
                () = cancellation_token.cancelled() => {
                    tracing::info!(
                        "Resource-group-aware round-robin scheduler cancelled. Shutting down."
                    );
                    return Ok(());
                }
                result = self.tick() => {
                    result.inspect_err(|err| tracing::error!(
                        err = % err,
                        "Resource-group-aware round-robin scheduler exits on error."
                    ))?;
                }
            }
            let sleep_time = tick_interval.saturating_sub(now.elapsed());
            if sleep_time.is_zero() {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(sleep_time).await;
            }
        }
    }

    /// Executes one tick of the scheduling loop.
    ///
    /// Processing the polling results is skipped while a storage poll is still in flight, but the
    /// dispatch queues are refilled from already-buffered tasks on every tick regardless.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`RgInboundQueueReader::try_collect_result`]'s return values on failure.
    /// * Forwards [`Self::start_inbound_poll`]'s return values on failure.
    /// * Forwards [`Self::fill_dispatch_queues`]'s return values on failure.
    pub(super) async fn tick(&mut self) -> Result<(), SchedulerError> {
        let poll_state = self
            .inbound_queue_reader
            .try_collect_result(self.session_tracker.current())
            .await?;
        match poll_state {
            RgInboundPollState::Ready {
                session_id,
                ready_result,
                commit_ready_result,
                cleanup_ready_result,
            } => {
                if session_id != self.session_tracker.current() {
                    self.apply_session_bump(session_id);
                }
                let rescheduled_entries = self.drain_reschedule_queue(session_id);

                let rg_updates = self.process_polling_results(
                    commit_ready_result,
                    cleanup_ready_result,
                    ready_result,
                    rescheduled_entries,
                );

                self.apply_rg_updates(rg_updates);
                self.start_inbound_poll()?;
            }
            RgInboundPollState::NotStarted => self.start_inbound_poll()?,
            RgInboundPollState::Pending => (),
        }

        let jobs_to_retire = self.fill_dispatch_queues()?;
        self.retire_jobs(jobs_to_retire);
        Ok(())
    }

    /// Discards every piece of state published in a session older than `new_session_id`.
    ///
    /// Clearing the dedup set is load-bearing rather than tidy: storage replays its ready tasks
    /// after a bump, and a stale dedup entry would drop a replayed task while the registry no
    /// longer holds anything to schedule it from.
    ///
    /// [`Self::rg_states`], [`Self::rg_index`], and [`Self::active_rg_list`] must be cleared as one
    /// operation, and this is the only place any of them is cleared. Positions in `rg_states` carry
    /// no generation, so an index that outlives the flush does not fail: it resolves against the
    /// new session's states, either out of bounds or -- once the new session has re-created a few
    /// groups -- silently against the wrong group. Nothing in the type system checks this.
    ///
    /// [`Self::dispatch_queue_registry`] must be cleared together with them, and that too is a
    /// correctness requirement rather than tidiness. A group's queue closes only once every sender
    /// has been dropped, and a scheduling state's write side is one of them; a state that survived
    /// the registry's flush would therefore hold a write side onto a queue whose readers are gone,
    /// and publishing into a closed queue is fatal to the core. Clearing the registry is also what
    /// discards the hints published in the session being left behind.
    fn apply_session_bump(&mut self, new_session_id: SessionId) {
        let previous_session_id = self.session_tracker.current();
        if self.session_tracker.try_advance(new_session_id) {
            tracing::info!(
                from = previous_session_id,
                to = new_session_id,
                num_resource_groups = self.dispatch_queue_registry.len(),
                num_jobs = self.job_registry.len(),
                "Storage session bumped. Flushing the core."
            );
        } else {
            tracing::error!(
                from = previous_session_id,
                to = new_session_id,
                "Storage reported a session no newer than the tracked one. Flushing the core \
                 anyway, but it keeps serving the tracked session."
            );
        }

        self.dispatch_queue_registry.clear();
        self.rg_states.clear();
        self.rg_id_to_index_map.clear();
        self.active_rg_list.clear();
        self.last_served_rg = None;
        self.job_registry.clear();
        self.global_task_set.clear();
        self.finalized_jobs.clear();
    }

    /// Drains the reschedule queue, dropping assignments published in a session other than
    /// `session_id`.
    ///
    /// # Returns
    ///
    /// The rescheduled assignments, in the same form as the entries drained from the inbound queue.
    fn drain_reschedule_queue(&mut self, session_id: SessionId) -> Vec<InboundEntry> {
        let mut entries = Vec::new();
        while let Ok(assignment) = self.reschedule_queue_reader.try_recv() {
            if assignment.session_id != session_id {
                continue;
            }
            entries.push(InboundEntry {
                resource_group_id: assignment.resource_group_id,
                job_id: assignment.job_id,
                task_id: assignment.task_id,
            });
        }
        entries
    }

    /// Folds the tick's ready tasks into the finalized job table, the global task set, and the job
    /// registry.
    ///
    /// Finalizations are processed before regular tasks, so a regular task arriving in the same
    /// batch as its job's finalization is discarded rather than scheduled.
    ///
    /// # Returns
    ///
    /// The per-resource-group updates the tick produced.
    fn process_polling_results(
        &mut self,
        commit_ready_jobs: Vec<FinalizedJob>,
        cleanup_ready_jobs: Vec<FinalizedJob>,
        ready_batches: Vec<ReadyBatch>,
        rescheduled_entries: Vec<InboundEntry>,
    ) -> HashMap<ResourceGroupId, RgUpdate> {
        let mut rescheduled_commit_entries = Vec::new();
        let mut rescheduled_cleanup_entries = Vec::new();
        let mut rescheduled_regular_entries = Vec::new();
        for entry in rescheduled_entries {
            match entry.task_id {
                TaskId::Commit => rescheduled_commit_entries.push(entry),
                TaskId::Cleanup => rescheduled_cleanup_entries.push(entry),
                TaskId::Index(_) => rescheduled_regular_entries.push(entry),
            }
        }

        let mut commit_ready = commit_ready_jobs;
        commit_ready.extend(format_finalized_jobs(rescheduled_commit_entries));
        let mut cleanup_ready = cleanup_ready_jobs;
        cleanup_ready.extend(format_finalized_jobs(rescheduled_cleanup_entries));
        let mut batches = ready_batches;
        batches.extend(format_ready_job_batches(rescheduled_regular_entries));

        let mut rg_updates: HashMap<ResourceGroupId, RgUpdate> = HashMap::new();
        for (finalized_jobs, kind) in [
            (commit_ready, FinalizeKind::Commit),
            (cleanup_ready, FinalizeKind::Cleanup),
        ] {
            for finalized_job in finalized_jobs {
                if !self.finalized_jobs.insert(finalized_job.job_id) {
                    continue;
                }
                // The job's still-buffered regular tasks will never be published, so they must
                // leave the dedup set with it or nothing would ever remove them.
                if let Some(mut job_entry) =
                    self.job_registry.remove_by_job_id(finalized_job.job_id)
                {
                    for task_index in job_entry.take_ready_tasks() {
                        self.global_task_set
                            .remove(&(finalized_job.job_id, TaskId::Index(task_index)));
                    }
                }
                rg_updates
                    .entry(finalized_job.resource_group_id)
                    .or_default()
                    .finalized
                    .push((finalized_job.job_id, kind));
            }
        }

        for batch in batches {
            let ReadyBatch {
                resource_group_id,
                job_id,
                mut task_indices,
            } = batch;
            if self.finalized_jobs.contains(&job_id) {
                continue;
            }
            let global_task_set = &mut self.global_task_set;
            task_indices
                .retain(|task_index| global_task_set.insert((job_id, TaskId::Index(*task_index))));
            if task_indices.is_empty() {
                continue;
            }
            if let UpsertOutcome::New(job_key) = self.job_registry.upsert(job_id, task_indices) {
                rg_updates
                    .entry(resource_group_id)
                    .or_default()
                    .new_jobs
                    .push(job_key);
            }
        }

        rg_updates
    }

    /// Applies the tick's per-resource-group updates to the scheduling states, activating every
    /// group the updates touch.
    fn apply_rg_updates(&mut self, rg_updates: HashMap<ResourceGroupId, RgUpdate>) {
        for (rg_id, update) in rg_updates {
            let state_index = self.get_or_create_state(rg_id);
            let rg_state = &mut self.rg_states[state_index];
            for (job_id, kind) in update.finalized {
                rg_state.push_finalization(job_id, kind);
            }
            for job_key in update.new_jobs {
                rg_state.place_new_job(job_key);
            }
            let activated = !rg_state.is_active;
            rg_state.is_active = true;
            if activated {
                self.active_rg_list.push(state_index);
            }
        }
    }

    /// # Returns
    ///
    /// The position of `rg_id`'s scheduling state in [`Self::rg_states`], appending a state built
    /// against the write side of the group's dispatch queue if the core has none.
    fn get_or_create_state(&mut self, rg_id: ResourceGroupId) -> usize {
        if let Some(state_index) = self.rg_id_to_index_map.get(&rg_id) {
            return *state_index;
        }

        let writer = self
            .dispatch_queue_registry
            .get_dispatch_queue_writer(rg_id);
        let state_index = self.rg_states.len();
        self.rg_states.push(RgSchedulingState::new(
            rg_id,
            writer,
            self.config.active_job_list_capacity.get(),
        ));
        self.rg_id_to_index_map.insert(rg_id, state_index);
        state_index
    }

    /// Publishes assignments into the per-resource-group dispatch queues under the admission
    /// policy, then deactivates the groups that ran out of work.
    ///
    /// # Returns
    ///
    /// The jobs that exhausted their downgrade budget and must be retired, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`SchedulerError::DispatchQueueClosed`] if a group's dispatch queue or the broadcast queue
    ///   is closed, in which case the assignments the core makes can no longer reach an execution
    ///   manager.
    fn fill_dispatch_queues(&mut self) -> Result<Vec<JobKey>, SchedulerError> {
        let mut jobs_to_retire = Vec::new();
        if self.active_rg_list.is_empty() {
            return Ok(jobs_to_retire);
        }

        // The decision loop needs the scheduling states and the job arena borrowed mutably at the
        // same time, which the borrow checker accepts only for bindings that name distinct fields.
        let Self {
            global_task_set,
            job_registry,
            rg_states,
            active_rg_list,
            last_served_rg,
            config,
            session_tracker,
            id_issuer,
            ..
        } = self;

        let mut rg_rr_list = Vec::with_capacity(active_rg_list.len());
        let mut occupancy = 0;
        let mut last_served_index = None;
        for (index, state_index) in active_rg_list.iter().enumerate() {
            let rg_state = &rg_states[*state_index];
            occupancy += rg_state.dispatch_queue_size();
            if Some(rg_state.rg_id) == *last_served_rg {
                last_served_index = Some(index);
            }
            rg_rr_list.push(*state_index);
        }
        // Bounding by the free space measured here is what makes the loop terminate: the queues
        // drain concurrently, so the true free space only ever grows.
        let mut free = config
            .dispatch_queue_capacity
            .get()
            .saturating_sub(occupancy);
        // Rotating the arm rather than the list keeps the same group from always being visited
        // first, which matters because `free` shrinks as the tick proceeds.
        let mut arm = last_served_index.map_or(0, |index| (index + 1) % rg_rr_list.len());

        for state_index in &rg_rr_list {
            rg_states[*state_index].promote_pending_jobs(job_registry, &mut jobs_to_retire);
        }

        let session_id = session_tracker.current();
        let mut exhausted_states = Vec::new();
        while 0 != free && !rg_rr_list.is_empty() {
            let state_index = rg_rr_list[arm];
            let rg_state = &mut rg_states[state_index];
            let result = rg_state.try_make_assignment(
                free,
                session_id,
                id_issuer,
                job_registry,
                &mut jobs_to_retire,
            );
            match result {
                Ok((job_id, task_id)) => {
                    global_task_set.remove(&(job_id, task_id));
                    free -= 1;
                    *last_served_rg = Some(rg_state.rg_id);
                    arm = (arm + 1) % rg_rr_list.len();
                }
                Err(err) => {
                    match err {
                        MakeAssignmentError::NoTask => exhausted_states.push(state_index),
                        MakeAssignmentError::DispatchQueueFull => (),
                        MakeAssignmentError::DispatchQueueClosed => {
                            return Err(SchedulerError::DispatchQueueClosed);
                        }
                    }
                    rg_rr_list.swap_remove(arm);
                    // `swap_remove` moved the tail element into this slot, so advancing the arm
                    // here would skip it.
                    if arm == rg_rr_list.len() {
                        arm = 0;
                    }
                }
            }
        }

        for state_index in &*active_rg_list {
            rg_states[*state_index].apply_downgrades(job_registry);
        }
        self.deactivate_exhausted_states(exhausted_states);

        Ok(jobs_to_retire)
    }

    /// Takes every exhausted group that also holds no assignment off the active resource group
    /// list.
    ///
    /// The empty-queue condition is required for correctness: free space is summed over the active
    /// list alone, so a deactivated group still holding assignments would hide its occupancy and
    /// let the core over-admit.
    fn deactivate_exhausted_states(&mut self, exhausted_states: Vec<usize>) {
        for state_index in exhausted_states {
            let rg_state = &mut self.rg_states[state_index];
            if rg_state.has_schedulable_task() || 0 != rg_state.dispatch_queue_size() {
                continue;
            }
            rg_state.is_active = false;
            if let Some(position) = self
                .active_rg_list
                .iter()
                .position(|active_index| *active_index == state_index)
            {
                self.active_rg_list.swap_remove(position);
            }
        }
    }

    /// Drops the registry's entry for every job that ran out of downgrade lives.
    fn retire_jobs(&mut self, jobs_to_retire: Vec<JobKey>) {
        for job_key in jobs_to_retire {
            self.job_registry.remove(job_key);
        }
    }

    /// Starts the next inbound poll, sizing each lane's fetch count by the buffer capacity that
    /// lane still has left.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`RgInboundQueueReader::start`]'s return values on failure.
    fn start_inbound_poll(&mut self) -> Result<(), SchedulerError> {
        let (num_commit_ready, num_cleanup_ready) = self.count_buffered_finalizations();
        let max_ready_entries = self
            .config
            .ready_task_capacity
            .get()
            .saturating_sub(self.global_task_set.len());
        let max_commit_ready_entries = self
            .config
            .commit_ready_task_capacity
            .get()
            .saturating_sub(num_commit_ready);
        let max_cleanup_ready_entries = self
            .config
            .cleanup_ready_task_capacity
            .get()
            .saturating_sub(num_cleanup_ready);

        self.inbound_queue_reader.start(
            Duration::from_millis(self.config.storage_poll_timeout_ms),
            max_ready_entries,
            max_commit_ready_entries,
            max_cleanup_ready_entries,
        )
    }

    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The number of buffered commit tasks.
    /// * The number of buffered cleanup tasks.
    fn count_buffered_finalizations(&self) -> (usize, usize) {
        let mut num_commit_ready = 0;
        let mut num_cleanup_ready = 0;
        for rg_state in &self.rg_states {
            let (num_commits, num_cleanups) = rg_state.num_buffered_finalize_tasks();
            num_commit_ready += num_commits;
            num_cleanup_ready += num_cleanups;
        }
        (num_commit_ready, num_cleanup_ready)
    }
}

/// The updates one tick produced for a single resource group.
#[derive(Default)]
struct RgUpdate {
    finalized: Vec<(JobId, FinalizeKind)>,
    new_jobs: Vec<JobKey>,
}
