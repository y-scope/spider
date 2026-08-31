//! The core-private scheduling state of a single resource group.

use std::collections::VecDeque;

use spider_core::task::TaskIndex;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskId;
use spider_core::types::scheduler::TaskAssignment;

use super::dispatch_queue::RgDispatchQueueWriter;
use super::job_registry::DOWNGRADE_LIVES;
use super::job_registry::JobKey;
use super::job_registry::JobRegistry;
use crate::core::TaskAssignmentIdIssuer;

/// Errors returned by [`RgSchedulingState::try_make_assignment`].
///
/// Most variants are not fatal; they tell the core why no assignment could be made so it can move
/// on. Variants that require the core to act are documented explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(super) enum MakeAssignmentError {
    /// The resource group has nothing left to schedule this tick.
    #[error("no task to assign")]
    NoTask,

    /// The resource group's queue occupancy has reached the admission threshold.
    #[error("dispatch queue is full")]
    DispatchQueueFull,

    /// A queue the write side publishes into is closed: either the resource group's own dispatch
    /// queue or the broadcast queue.
    ///
    /// This is considered a fatal error, and it may result an unrecoverable scheduling state. The
    /// core should forward it as [`crate::SchedulerError::DispatchQueueClosed`] to terminate the
    /// scheduler service.
    #[error("dispatch queue is closed")]
    DispatchQueueClosed,
}

/// The job's terminal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum FinalizeKind {
    Commit,
    Cleanup,
}

impl From<FinalizeKind> for TaskId {
    fn from(kind: FinalizeKind) -> Self {
        match kind {
            FinalizeKind::Commit => Self::Commit,
            FinalizeKind::Cleanup => Self::Cleanup,
        }
    }
}

/// The scheduling state of one resource group.
pub(super) struct RgSchedulingState {
    /// The resource group this state schedules for.
    pub(super) rg_id: ResourceGroupId,

    /// The jobs assignments are currently drawn from, rotated over by [`Self::rr_arm`].
    pub(super) active_jobs: Vec<JobKey>,

    /// The jobs waiting for a slot in [`Self::active_jobs`].
    pub(super) pending_jobs: VecDeque<JobKey>,

    /// The index into [`Self::active_jobs`] the next regular task is drawn from.
    pub(super) rr_arm: usize,

    /// Whether the group is currently on the core's active resource group list.
    pub(super) is_active: bool,

    finalize_queue: VecDeque<(JobId, FinalizeKind)>,
    num_buffered_commits: usize,
    num_buffered_cleanups: usize,
    writer: RgDispatchQueueWriter,
    active_to_pending_downgrade_buffer: Vec<JobKey>,
    pending_downgrade_buffer: Vec<JobKey>,
    active_job_list_capacity: usize,
}

impl RgSchedulingState {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created, inactive state publishing through `writer`.
    pub(super) fn new(
        rg_id: ResourceGroupId,
        writer: RgDispatchQueueWriter,
        active_job_list_capacity: usize,
    ) -> Self {
        Self {
            rg_id,
            active_jobs: Vec::with_capacity(active_job_list_capacity),
            pending_jobs: VecDeque::new(),
            rr_arm: 0,
            is_active: false,
            finalize_queue: VecDeque::new(),
            num_buffered_commits: 0,
            num_buffered_cleanups: 0,
            writer,
            active_to_pending_downgrade_buffer: Vec::new(),
            pending_downgrade_buffer: Vec::new(),
            active_job_list_capacity,
        }
    }

    /// # Returns
    ///
    /// The number of assignments currently queued for the group.
    pub(super) fn dispatch_queue_size(&self) -> usize {
        self.writer.queue_len()
    }

    /// # Returns
    ///
    /// Whether the group holds any assignments.
    pub(super) fn has_schedulable_task(&self) -> bool {
        !self.finalize_queue.is_empty()
            || !self.active_jobs.is_empty()
            || !self.pending_jobs.is_empty()
    }

    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The number of commit tasks the group has buffered.
    /// * The number of cleanup tasks the group has buffered.
    pub(super) const fn num_buffered_finalize_tasks(&self) -> (usize, usize) {
        (self.num_buffered_commits, self.num_buffered_cleanups)
    }

    /// Records that `job_id` has reached the finalization named by `kind`.
    pub(super) fn push_finalization(&mut self, job_id: JobId, kind: FinalizeKind) {
        self.finalize_queue.push_back((job_id, kind));
        match kind {
            FinalizeKind::Commit => self.num_buffered_commits += 1,
            FinalizeKind::Cleanup => self.num_buffered_cleanups += 1,
        }
    }

    /// Gives a newly registered job its scheduling position in this group.
    pub(super) fn place_new_job(&mut self, job_key: JobKey) {
        if self.active_jobs.len() < self.active_job_list_capacity {
            self.active_jobs.push(job_key);
        } else {
            self.pending_jobs.push_back(job_key);
        }
    }

    /// Tops up the active job list to capacity from the pending job queue.
    ///
    /// Pending jobs that yield nothing spend a downgrade life count, and are collected into
    /// `jobs_to_retire` once they have none left.
    pub(super) fn promote_pending_jobs(
        &mut self,
        job_registry: &mut JobRegistry,
        jobs_to_retire: &mut Vec<JobKey>,
    ) {
        while self.active_jobs.len() < self.active_job_list_capacity {
            let Some(job_key) = self.pop_promotable_job(job_registry, jobs_to_retire) else {
                break;
            };
            self.active_jobs.push(job_key);
        }
    }

    /// Publishes at most one assignment for this group.
    ///
    /// `free` is the tick's remaining free space in the whole dispatch buffer, read but not
    /// modified here -- the caller decrements it once the assignment is published. This is only
    /// used by the admission threshold.
    ///
    /// # Returns
    ///
    /// The job and task the published assignment carries on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`MakeAssignmentError::NoTask`] if the group has nothing left to schedule.
    /// * [`MakeAssignmentError::DispatchQueueFull`] if the group's queue occupancy has reached the
    ///   admission threshold.
    /// * Forwards [`Self::publish`]'s return values on failure.
    pub(super) fn try_make_assignment(
        &mut self,
        free: usize,
        session_id: SessionId,
        id_issuer: &TaskAssignmentIdIssuer,
        job_registry: &mut JobRegistry,
        jobs_to_retire: &mut Vec<JobKey>,
    ) -> Result<(JobId, TaskId), MakeAssignmentError> {
        if !self.has_schedulable_task() {
            return Err(MakeAssignmentError::NoTask);
        }
        if self.dispatch_queue_size() >= free * SHARING_COEFFICIENT {
            // Admission threshold reached.
            return Err(MakeAssignmentError::DispatchQueueFull);
        }

        // NOTE: Prioritize finalization tasks, if any.
        if let Some((job_id, kind)) = self.pop_finalization() {
            let task_id = TaskId::from(kind);
            self.publish(job_id, task_id, session_id, id_issuer)?;
            return Ok((job_id, task_id));
        }

        let (job_id, task_index) = self
            .pop_regular_task(job_registry, jobs_to_retire)
            .ok_or(MakeAssignmentError::NoTask)?;
        let task_id = TaskId::Index(task_index);
        self.publish(job_id, task_id, session_id, id_issuer)?;
        Ok((job_id, task_id))
    }

    /// Re-inserts every job buffered for downgrade to the pending job queue:
    ///
    /// * A job demoted out of the active job list goes to the queue's head with its downgrade
    ///   budget restored.
    /// * A job that was already pending goes to its tail with its budget untouched.
    pub(super) fn apply_downgrades(&mut self, job_registry: &mut JobRegistry) {
        for job_key in std::mem::take(&mut self.active_to_pending_downgrade_buffer)
            .into_iter()
            .rev()
        {
            if let Some(entry) = job_registry.get_mut(job_key) {
                entry.reset_downgrade_counter();
            }
            self.pending_jobs.push_front(job_key);
        }
        self.pending_jobs
            .extend(std::mem::take(&mut self.pending_downgrade_buffer));
    }

    /// Takes the group's next owed finalization off its finalize queue.
    ///
    /// # Returns
    ///
    /// The finalization to schedule, or [`None`] if the group owes no finalization.
    fn pop_finalization(&mut self) -> Option<(JobId, FinalizeKind)> {
        let (job_id, kind) = self.finalize_queue.pop_front()?;
        match kind {
            FinalizeKind::Commit => {
                self.num_buffered_commits = self.num_buffered_commits.saturating_sub(1);
            }
            FinalizeKind::Cleanup => {
                self.num_buffered_cleanups = self.num_buffered_cleanups.saturating_sub(1);
            }
        }
        Some((job_id, kind))
    }

    /// Takes the next regular task to dispatch out of the job that buffers it, rotating the arm
    /// and refilling the active job list from the pending job queue as jobs run dry.
    ///
    /// # Returns
    ///
    /// * A tuple on success, containing:
    ///  * That job's ID.
    ///  * The task index to dispatch.
    /// * [`None`] if no active or pending job yields a task.
    fn pop_regular_task(
        &mut self,
        job_registry: &mut JobRegistry,
        jobs_to_retire: &mut Vec<JobKey>,
    ) -> Option<(JobId, TaskIndex)> {
        const _: () = assert!(
            1 == DOWNGRADE_LIVES,
            "An exhausted active job with no tasks is demoted on its first visit, so this loop \
             never revisits one; a larger downgrade budget needs a per-tick buffer for active \
             jobs already visited."
        );

        loop {
            if self.active_jobs.is_empty() {
                let job_key = self.pop_promotable_job(job_registry, jobs_to_retire)?;
                self.rr_arm = 0;
                self.active_jobs.push(job_key);
            }

            if self.rr_arm >= self.active_jobs.len() {
                self.rr_arm = 0;
            }
            let job_key = self.active_jobs[self.rr_arm];
            let Some(entry) = job_registry.get_mut(job_key) else {
                self.swap_in_pending_job(job_registry, jobs_to_retire);
                continue;
            };
            if let Some(task_index) = entry.pop_next_task() {
                let job_id = entry.job_id();
                self.rr_arm += 1;
                return Some((job_id, task_index));
            }

            entry.decrement_downgrade_counter();
            if 0 == entry.downgrade_counter() {
                self.active_to_pending_downgrade_buffer.push(job_key);
                self.swap_in_pending_job(job_registry, jobs_to_retire);
            } else {
                self.rr_arm += 1;
            }
        }
    }

    /// Replaces the active job the arm points at with the next promotable pending job, or removes
    /// the slot when no pending job is promotable.
    fn swap_in_pending_job(
        &mut self,
        job_registry: &mut JobRegistry,
        jobs_to_retire: &mut Vec<JobKey>,
    ) {
        if let Some(job_key) = self.pop_promotable_job(job_registry, jobs_to_retire) {
            self.active_jobs[self.rr_arm] = job_key;
        } else {
            self.active_jobs.swap_remove(self.rr_arm);
        }
    }

    /// Pops pending jobs until one with a buffered ready task is found, examining each queued job
    /// at most once.
    ///
    /// A key that no longer resolves belongs to a job that has been removed from the registry and
    /// is discarded outright.
    ///
    /// A job that yields nothing spends a downgrade life and is held back until
    /// [`Self::apply_downgrades`] runs, or is collected into `jobs_to_retire` if it has no
    /// downgrade budget left.
    ///
    /// # Returns
    ///
    /// The key of the promotable job, or [`None`] if no pending job yields a task.
    fn pop_promotable_job(
        &mut self,
        job_registry: &mut JobRegistry,
        jobs_to_retire: &mut Vec<JobKey>,
    ) -> Option<JobKey> {
        while let Some(job_key) = self.pending_jobs.pop_front() {
            let Some(entry) = job_registry.get_mut(job_key) else {
                continue;
            };
            if entry.has_ready_task() {
                return Some(job_key);
            }
            if 0 == entry.downgrade_counter() {
                jobs_to_retire.push(job_key);
            } else {
                entry.decrement_downgrade_counter();
                self.pending_downgrade_buffer.push(job_key);
            }
        }
        None
    }

    /// Publishes one assignment for this group.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`MakeAssignmentError::DispatchQueueClosed`] if [`RgDispatchQueueWriter::try_send`]
    ///   rejects the assignment.
    fn publish(
        &self,
        job_id: JobId,
        task_id: TaskId,
        session_id: SessionId,
        id_issuer: &TaskAssignmentIdIssuer,
    ) -> Result<(), MakeAssignmentError> {
        let assignment = TaskAssignment {
            id: id_issuer.next(),
            resource_group_id: self.rg_id,
            job_id,
            task_id,
            session_id,
        };
        self.writer
            .try_send(assignment)
            .map_err(|_| MakeAssignmentError::DispatchQueueClosed)?;
        Ok(())
    }
}

/// The sharing coefficient used by Choudhury & Hahne's dynamic threshold algorithm.
const SHARING_COEFFICIENT: usize = 1;

#[cfg(test)]
mod tests {
    use anyhow::bail;
    use spider_core::session::SessionTracker;
    use spider_core::task::TaskIndex;
    use spider_core::types::id::JobId;
    use spider_core::types::id::ResourceGroupId;
    use spider_core::types::id::SessionId;
    use spider_core::types::id::TaskId;

    use super::super::dispatch_queue::DispatchQueueRegistry;
    use super::super::job_registry::DOWNGRADE_LIVES;
    use super::super::job_registry::UpsertOutcome;
    use super::*;

    /// The session a test that never bumps the session runs under.
    const DEFAULT_SESSION_ID: SessionId = 0;

    /// The resource group every test in this module schedules for.
    const RG_ID: ResourceGroupId = ResourceGroupId::from(4);

    /// The first job a test registers.
    const JOB_A: JobId = JobId::from(10);

    /// The second job a test registers.
    const JOB_B: JobId = JobId::from(11);

    /// The third job a test registers.
    const JOB_C: JobId = JobId::from(12);

    /// The free space passed to an assignment attempt that is not meant to reach the threshold.
    const FREE_SPACE: usize = 32;

    /// One scheduling state and the structures it publishes into.
    struct StateFixture {
        state: RgSchedulingState,
        registry: JobRegistry,
        jobs_to_retire: Vec<JobKey>,
        id_issuer: TaskAssignmentIdIssuer,
        dispatch_queue_registry: DispatchQueueRegistry,
    }

    impl StateFixture {
        /// Factory function.
        ///
        /// # Returns
        ///
        /// A newly created fixture whose state holds no job.
        fn new(active_job_list_capacity: usize) -> Self {
            let dispatch_queue_registry =
                DispatchQueueRegistry::new(SessionTracker::new(DEFAULT_SESSION_ID));
            Self {
                state: make_state(&dispatch_queue_registry, RG_ID, active_job_list_capacity),
                registry: JobRegistry::new(),
                jobs_to_retire: Vec::new(),
                id_issuer: TaskAssignmentIdIssuer::new(),
                dispatch_queue_registry,
            }
        }

        /// Registers a job of `num_tasks` buffered ready tasks and gives it its scheduling
        /// position.
        ///
        /// # Returns
        ///
        /// The key of the registered job.
        ///
        /// # Errors
        ///
        /// Returns an error if:
        ///
        /// * Forwards [`make_job_entry`]'s return values on failure.
        fn add_job(&mut self, job_id: JobId, num_tasks: usize) -> anyhow::Result<JobKey> {
            let job_key = make_job_entry(&mut self.registry, job_id, num_tasks)?;
            self.state.place_new_job(job_key);
            Ok(job_key)
        }

        /// Attempts one assignment against `free` free space, as one turn of the core's decision
        /// loop would.
        ///
        /// # Returns
        ///
        /// The job and task the published assignment carries, on success.
        ///
        /// # Errors
        ///
        /// Returns an error if:
        ///
        /// * Forwards [`RgSchedulingState::try_make_assignment`]'s return values on failure.
        fn try_make(&mut self, free: usize) -> Result<(JobId, TaskId), MakeAssignmentError> {
            let Self {
                state,
                registry,
                jobs_to_retire,
                id_issuer,
                ..
            } = self;
            state.try_make_assignment(
                free,
                DEFAULT_SESSION_ID,
                id_issuer,
                registry,
                jobs_to_retire,
            )
        }

        /// Appends `task_indices` to the ready tasks of the job `job_key` refers to.
        ///
        /// # Panics
        ///
        /// Panics if the job has been removed from the registry.
        fn insert_tasks(&mut self, job_key: JobKey, task_indices: Vec<TaskIndex>) {
            self.registry
                .get_mut(job_key)
                .expect("the job is still registered")
                .insert_tasks(task_indices);
        }

        /// # Returns
        ///
        /// The number of further chances the job `job_key` refers to has to be refilled.
        ///
        /// # Panics
        ///
        /// Panics if the job has been removed from the registry.
        fn downgrade_counter(&mut self, job_key: JobKey) -> u32 {
            self.registry
                .get_mut(job_key)
                .expect("the job is still registered")
                .downgrade_counter()
        }

        /// # Returns
        ///
        /// Whether the job `job_key` refers to still has a buffered ready task.
        ///
        /// # Panics
        ///
        /// Panics if the job has been removed from the registry.
        fn has_ready_task(&mut self, job_key: JobKey) -> bool {
            self.registry
                .get_mut(job_key)
                .expect("the job is still registered")
                .has_ready_task()
        }

        /// # Returns
        ///
        /// The keys of the group's pending jobs, in queue order.
        fn pending_job_keys(&self) -> Vec<JobKey> {
            self.state.pending_jobs.iter().copied().collect()
        }

        /// # Returns
        ///
        /// The number of assignments currently queued for the group.
        fn queue_len(&self) -> usize {
            self.dispatch_queue_registry
                .get_dispatch_queue_writer(RG_ID)
                .queue_len()
        }

        /// Closes the group's dispatch queue, so that the channel rejects the next publication.
        fn close_dispatch_queue(&self) {
            self.dispatch_queue_registry.close_dispatch_queue(RG_ID);
        }

        /// Closes the broadcast queue, so that it rejects the next hint.
        fn close_broadcast_queue(&self) {
            self.dispatch_queue_registry.close_broadcast_queue();
        }
    }

    /// # Returns
    ///
    /// A newly created, inactive scheduling state publishing into `rg_id`'s dispatch queue.
    fn make_state(
        dispatch_queue_registry: &DispatchQueueRegistry,
        rg_id: ResourceGroupId,
        active_job_list_capacity: usize,
    ) -> RgSchedulingState {
        RgSchedulingState::new(
            rg_id,
            dispatch_queue_registry.get_dispatch_queue_writer(rg_id),
            active_job_list_capacity,
        )
    }

    /// Registers a job of `num_tasks` buffered ready tasks, whose task indices are `0..num_tasks`.
    ///
    /// # Returns
    ///
    /// The key of the registered job, which still needs a scheduling position.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if the job is already registered.
    fn make_job_entry(
        registry: &mut JobRegistry,
        job_id: JobId,
        num_tasks: usize,
    ) -> anyhow::Result<JobKey> {
        let task_indices: Vec<TaskIndex> = (0..num_tasks).collect();
        let UpsertOutcome::New(job_key) = registry.upsert(job_id, task_indices) else {
            bail!("job {job_id} is already registered");
        };
        Ok(job_key)
    }

    #[test]
    fn finalization_tasks_are_dispatched_ahead_of_regular_ones() -> anyhow::Result<()> {
        let mut fixture = StateFixture::new(2);
        fixture.add_job(JOB_A, 2)?;
        fixture.state.push_finalization(JOB_B, FinalizeKind::Commit);
        fixture
            .state
            .push_finalization(JOB_C, FinalizeKind::Cleanup);

        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_B, TaskId::Commit)));
        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_C, TaskId::Cleanup)));
        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_A, TaskId::Index(0))));
        Ok(())
    }

    #[test]
    fn an_empty_unit_reports_no_task() -> anyhow::Result<()> {
        let mut fixture = StateFixture::new(2);
        assert!(!fixture.state.has_schedulable_task());
        assert_eq!(
            fixture.try_make(FREE_SPACE),
            Err(MakeAssignmentError::NoTask)
        );

        // A job with nothing buffered still considered schedulable, but yields no task.
        fixture.add_job(JOB_A, 0)?;
        assert!(fixture.state.has_schedulable_task());
        assert_eq!(
            fixture.try_make(FREE_SPACE),
            Err(MakeAssignmentError::NoTask)
        );
        Ok(())
    }

    #[test]
    fn the_admission_threshold_binds_at_the_free_space_boundary() -> anyhow::Result<()> {
        const OCCUPANCY: usize = 3;

        let mut fixture = StateFixture::new(2);
        fixture.add_job(JOB_A, 8)?;
        for task_index in 0..OCCUPANCY {
            assert_eq!(
                fixture.try_make(FREE_SPACE),
                Ok((JOB_A, TaskId::Index(task_index)))
            );
        }
        assert_eq!(fixture.state.dispatch_queue_size(), OCCUPANCY);

        assert_eq!(
            fixture.try_make(OCCUPANCY),
            Err(MakeAssignmentError::DispatchQueueFull)
        );
        assert_eq!(fixture.state.dispatch_queue_size(), OCCUPANCY);

        assert_eq!(
            fixture.try_make(OCCUPANCY + 1),
            Ok((JOB_A, TaskId::Index(OCCUPANCY)))
        );
        Ok(())
    }

    #[test]
    fn an_exhausted_active_job_is_replaced_by_a_pending_one() -> anyhow::Result<()> {
        let mut fixture = StateFixture::new(1);
        let job_a = fixture.add_job(JOB_A, 1)?;
        fixture.add_job(JOB_B, 1)?;
        assert_eq!(fixture.state.active_jobs.len(), 1);
        assert_eq!(fixture.state.pending_jobs.len(), 1);

        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_A, TaskId::Index(0))));
        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_B, TaskId::Index(0))));

        assert_eq!(fixture.state.active_jobs.len(), 1);
        assert_eq!(fixture.state.pending_jobs.len(), 0);
        assert_eq!(fixture.downgrade_counter(job_a), 0);
        assert_eq!(fixture.jobs_to_retire.as_slice(), &[]);
        Ok(())
    }

    #[test]
    fn promotion_discards_a_removed_pending_job() -> anyhow::Result<()> {
        let mut fixture = StateFixture::new(1);
        fixture.add_job(JOB_A, 1)?;
        fixture.add_job(JOB_B, 1)?;
        fixture.add_job(JOB_C, 1)?;
        fixture
            .registry
            .remove_by_job_id(JOB_B)
            .expect("the registered job is removed by its finalization");

        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_A, TaskId::Index(0))));
        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_C, TaskId::Index(0))));
        assert_eq!(fixture.state.pending_jobs.len(), 0);
        assert_eq!(fixture.jobs_to_retire.as_slice(), &[]);
        Ok(())
    }

    #[test]
    fn removed_active_job_swapped_by_pending() -> anyhow::Result<()> {
        let mut fixture = StateFixture::new(1);
        fixture.add_job(JOB_A, 1)?;
        fixture.add_job(JOB_B, 1)?;
        fixture
            .registry
            .remove_by_job_id(JOB_A)
            .expect("the registered job is removed by its finalization");

        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_B, TaskId::Index(0))));
        assert_eq!(fixture.state.active_jobs.len(), 1);
        assert_eq!(fixture.state.pending_jobs.len(), 0);
        Ok(())
    }

    #[test]
    fn job_downgrade_lifecycle() -> anyhow::Result<()> {
        let mut fixture = StateFixture::new(1);
        let job_a = fixture.add_job(JOB_A, 1)?;
        assert_eq!(fixture.downgrade_counter(job_a), DOWNGRADE_LIVES);

        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_A, TaskId::Index(0))));
        assert_eq!(
            fixture.try_make(FREE_SPACE),
            Err(MakeAssignmentError::NoTask)
        );
        assert_eq!(fixture.downgrade_counter(job_a), 0);
        assert_eq!(fixture.state.active_jobs.len(), 0);
        assert_eq!(fixture.jobs_to_retire.as_slice(), &[]);

        fixture.state.apply_downgrades(&mut fixture.registry);
        assert_eq!(fixture.state.pending_jobs.len(), 1);
        assert_eq!(fixture.downgrade_counter(job_a), DOWNGRADE_LIVES);

        let mut jobs_to_retire = Vec::new();
        fixture
            .state
            .promote_pending_jobs(&mut fixture.registry, &mut jobs_to_retire);
        assert_eq!(jobs_to_retire.as_slice(), &[]);
        assert_eq!(fixture.state.active_jobs.len(), 0);
        assert_eq!(fixture.downgrade_counter(job_a), 0);
        assert_eq!(fixture.state.pending_jobs.len(), 0);

        fixture.state.apply_downgrades(&mut fixture.registry);
        assert_eq!(fixture.pending_job_keys(), vec![job_a]);
        assert_eq!(fixture.downgrade_counter(job_a), 0);

        assert_eq!(
            fixture.try_make(FREE_SPACE),
            Err(MakeAssignmentError::NoTask)
        );
        assert_eq!(fixture.jobs_to_retire.as_slice(), &[job_a]);
        assert_eq!(fixture.state.pending_jobs.len(), 0);
        assert!(!fixture.state.has_schedulable_task());
        Ok(())
    }

    #[test]
    fn each_pending_job_is_downgraded_at_most_once_per_tick() -> anyhow::Result<()> {
        let mut fixture = StateFixture::new(1);
        let job_a = fixture.add_job(JOB_A, 1)?;
        let job_b = fixture.add_job(JOB_B, 0)?;

        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_A, TaskId::Index(0))));
        assert_eq!(
            fixture.try_make(FREE_SPACE),
            Err(MakeAssignmentError::NoTask)
        );
        assert_eq!(fixture.downgrade_counter(job_b), 0);

        fixture
            .state
            .promote_pending_jobs(&mut fixture.registry, &mut fixture.jobs_to_retire);
        assert_eq!(fixture.jobs_to_retire.as_slice(), &[]);
        assert_eq!(fixture.downgrade_counter(job_b), 0);
        assert!(!fixture.state.has_schedulable_task());

        fixture.state.apply_downgrades(&mut fixture.registry);
        assert_eq!(fixture.pending_job_keys(), vec![job_a, job_b]);
        assert_eq!(fixture.downgrade_counter(job_a), DOWNGRADE_LIVES);
        assert_eq!(fixture.downgrade_counter(job_b), 0);
        Ok(())
    }

    #[test]
    fn demoted_jobs_keep_their_order_at_the_head_of_the_pending_queue() -> anyhow::Result<()> {
        let mut fixture = StateFixture::new(2);
        let job_a = fixture.add_job(JOB_A, 1)?;
        let job_b = fixture.add_job(JOB_B, 1)?;

        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_A, TaskId::Index(0))));
        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_B, TaskId::Index(0))));
        assert_eq!(
            fixture.try_make(FREE_SPACE),
            Err(MakeAssignmentError::NoTask)
        );
        assert_eq!(fixture.jobs_to_retire.as_slice(), &[]);

        fixture.state.apply_downgrades(&mut fixture.registry);
        assert_eq!(fixture.pending_job_keys(), vec![job_a, job_b]);
        Ok(())
    }

    #[test]
    fn arriving_task_restores_a_downgraded_job() -> anyhow::Result<()> {
        let mut fixture = StateFixture::new(1);
        let job_a = fixture.add_job(JOB_A, 1)?;

        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_A, TaskId::Index(0))));
        assert_eq!(
            fixture.try_make(FREE_SPACE),
            Err(MakeAssignmentError::NoTask)
        );
        fixture.state.apply_downgrades(&mut fixture.registry);

        fixture.insert_tasks(job_a, vec![1]);
        let mut jobs_to_retire = Vec::new();
        fixture
            .state
            .promote_pending_jobs(&mut fixture.registry, &mut jobs_to_retire);
        assert_eq!(jobs_to_retire.as_slice(), &[]);
        assert_eq!(fixture.state.active_jobs.len(), 1);

        assert_eq!(fixture.try_make(FREE_SPACE), Ok((JOB_A, TaskId::Index(1))));
        assert_eq!(fixture.downgrade_counter(job_a), DOWNGRADE_LIVES);
        Ok(())
    }

    #[test]
    fn rejected_publication_discards_the_finalization() {
        let mut fixture = StateFixture::new(1);
        fixture.state.push_finalization(JOB_B, FinalizeKind::Commit);
        fixture.close_dispatch_queue();

        assert_eq!(
            fixture.try_make(FREE_SPACE),
            Err(MakeAssignmentError::DispatchQueueClosed)
        );
        assert_eq!(fixture.state.num_buffered_finalize_tasks(), (0, 0));
    }

    #[test]
    fn rejected_publication_discards_the_regular_task() -> anyhow::Result<()> {
        let mut fixture = StateFixture::new(1);
        let job_a = fixture.add_job(JOB_A, 1)?;
        fixture.close_dispatch_queue();

        assert_eq!(
            fixture.try_make(FREE_SPACE),
            Err(MakeAssignmentError::DispatchQueueClosed)
        );
        assert!(!fixture.has_ready_task(job_a));
        Ok(())
    }

    #[test]
    fn closed_broadcast_queue_fails_the_publication() -> anyhow::Result<()> {
        let mut fixture = StateFixture::new(1);
        fixture.add_job(JOB_A, 1)?;
        fixture.close_broadcast_queue();

        assert_eq!(
            fixture.try_make(FREE_SPACE),
            Err(MakeAssignmentError::DispatchQueueClosed)
        );

        // Both closures report the same error, so the queue is what tells them apart: this
        // publication reached the group's queue first and lost only the hint covering it.
        assert_eq!(fixture.queue_len(), 1);
        assert_eq!(fixture.dispatch_queue_registry.num_outstanding_hints(), 0);
        Ok(())
    }
}
