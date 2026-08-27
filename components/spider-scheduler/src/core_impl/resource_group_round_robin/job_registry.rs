//! Registry of currently running jobs, each holding a queue of ready tasks to schedule from.

use std::collections::HashMap;
use std::collections::VecDeque;

use slotmap::SlotMap;
use spider_core::task::TaskIndex;
use spider_core::types::id::JobId;

slotmap::new_key_type! {
    /// A generational handle to a job entry owned by a [`JobRegistry`].
    pub(super) struct JobKey;
}

/// The result of inserting a batch of ready tasks for a job.
pub(super) enum UpsertOutcome {
    /// The job was already registered.
    Exist,

    /// The job was registered by this call, along with a key to access its entry.
    New(JobKey),
}

/// The scheduling state of a single job, maintaining a FIFO queue for its ready tasks.
pub(super) struct JobEntry {
    job_id: JobId,
    ready_task_queue: VecDeque<TaskIndex>,
    downgrade_counter: u32,
}

impl JobEntry {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created entry for `job_id` holding `task_indices`.
    fn new(job_id: JobId, task_indices: Vec<TaskIndex>) -> Self {
        Self {
            job_id,
            ready_task_queue: task_indices.into(),
            downgrade_counter: DOWNGRADE_LIVES,
        }
    }

    /// # Returns
    ///
    /// The job ID of this entry.
    pub(super) const fn job_id(&self) -> JobId {
        self.job_id
    }

    /// Appends newly arrived ready tasks and restores the job's full downgrade budget.
    pub(super) fn insert_tasks(&mut self, task_indices: Vec<TaskIndex>) {
        self.ready_task_queue.extend(task_indices);
        self.downgrade_counter = DOWNGRADE_LIVES;
    }

    /// Reads the job's next ready task from the queue without taking it.
    ///
    /// # Returns
    ///
    /// The next ready task, or [`None`] if the queue is empty.
    pub(super) fn peek_next_task(&self) -> Option<TaskIndex> {
        self.ready_task_queue.front().copied()
    }

    /// Pops the job's next ready task from the queue.
    ///
    /// # Returns
    ///
    /// The next ready task, or [`None`] if the queue is empty.
    pub(super) fn pop_next_task(&mut self) -> Option<TaskIndex> {
        self.ready_task_queue.pop_front()
    }

    /// Empties the job's ready task queue.
    ///
    /// # Returns
    ///
    /// The job's ready task queue before it was emptied by this call.
    pub(super) fn take_ready_tasks(&mut self) -> VecDeque<TaskIndex> {
        std::mem::take(&mut self.ready_task_queue)
    }

    /// # Returns
    ///
    /// Whether the job has at least one ready task.
    pub(super) fn has_ready_task(&self) -> bool {
        !self.ready_task_queue.is_empty()
    }

    /// # Returns
    ///
    /// The number of further chances the job has to be refilled before it is downgraded.
    pub(super) const fn downgrade_counter(&self) -> u32 {
        self.downgrade_counter
    }

    /// Consumes one of the job's remaining chances to be refilled.
    pub(super) const fn decrement_downgrade_counter(&mut self) {
        self.downgrade_counter = self.downgrade_counter.saturating_sub(1);
    }

    /// Restores the job's full downgrade budget.
    pub(super) const fn reset_downgrade_counter(&mut self) {
        self.downgrade_counter = DOWNGRADE_LIVES;
    }
}

/// The core's registry of job entries, supporting two access methods:
///
/// * Job ID: Lookup through the job ID-to-key mapping.
/// * Job key: Direct lookup using the key returned when the job is created.
#[derive(Default)]
pub(super) struct JobRegistry {
    entries: SlotMap<JobKey, JobEntry>,
    id_to_key: HashMap<JobId, JobKey>,
}

impl JobRegistry {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created, empty registry.
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Registers `task_indices` as ready tasks of `job_id`, creating the job's entry if it has
    /// none.
    ///
    /// # Returns
    ///
    /// * [`UpsertOutcome::New`] with the key of the created entry, if the job was not registered.
    /// * [`UpsertOutcome::Exist`] otherwise.
    pub(super) fn upsert(&mut self, job_id: JobId, task_indices: Vec<TaskIndex>) -> UpsertOutcome {
        if let Some(entry) = self
            .id_to_key
            .get(&job_id)
            .and_then(|key| self.entries.get_mut(*key))
        {
            entry.insert_tasks(task_indices);
            return UpsertOutcome::Exist;
        }

        let key = self.entries.insert(JobEntry::new(job_id, task_indices));
        self.id_to_key.insert(job_id, key);
        UpsertOutcome::New(key)
    }

    /// # Returns
    ///
    /// The entry `key` refers to, or [`None`] if the job has been removed from the registry.
    pub(super) fn get_mut(&mut self, key: JobKey) -> Option<&mut JobEntry> {
        self.entries.get_mut(key)
    }

    /// Removes `job_id`'s entry from the registry, after which every key to it fails to resolve.
    ///
    /// # Returns
    ///
    /// The removed entry, or [`None`] if the job was not registered.
    pub(super) fn remove_by_job_id(&mut self, job_id: JobId) -> Option<JobEntry> {
        let key = self.id_to_key.remove(&job_id)?;
        self.entries.remove(key)
    }

    /// Removes the entry `key` refers to, after which every other key to it fails to resolve.
    ///
    /// # Returns
    ///
    /// The removed entry, or [`None`] if the job has already been removed.
    pub(super) fn remove(&mut self, key: JobKey) -> Option<JobEntry> {
        let entry = self.entries.remove(key)?;
        self.id_to_key.remove(&entry.job_id);
        Some(entry)
    }

    /// Drops every registered job.
    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.id_to_key.clear();
    }

    /// # Returns
    ///
    /// The number of registered jobs.
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }
}

/// The number of chances a job that produced no task gets before it is retired.
pub(super) const DOWNGRADE_LIVES: u32 = 1;

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use anyhow::bail;
    use spider_core::task::TaskIndex;
    use spider_core::types::id::JobId;

    use super::*;

    /// The job every single-job test registers.
    const JOB_ID: JobId = JobId::from(3);

    /// Registers `job_id` in `registry` with `num_tasks` ready tasks indexed from zero.
    ///
    /// # Returns
    ///
    /// The key of the created entry on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if `job_id` is already registered.
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

    /// # Returns
    ///
    /// The ID of the job `job_key` refers to, or [`None`] if the key no longer resolves.
    fn job_id_of(registry: &mut JobRegistry, job_key: JobKey) -> Option<JobId> {
        registry.get_mut(job_key).map(|entry| entry.job_id())
    }

    /// # Returns
    ///
    /// The entry `job_key` refers to.
    ///
    /// # Panics
    ///
    /// Panics if the key no longer resolves.
    fn entry_of(registry: &mut JobRegistry, job_key: JobKey) -> &mut JobEntry {
        registry
            .get_mut(job_key)
            .expect("the job is still registered")
    }

    #[test]
    fn upsert_registers_a_new_job_and_appends_to_an_existing_one() -> anyhow::Result<()> {
        let mut registry = JobRegistry::new();

        let job_key = make_job_entry(&mut registry, JOB_ID, 2)?;
        assert_eq!(job_id_of(&mut registry, job_key), Some(JOB_ID));
        assert_eq!(registry.len(), 1);

        let outcome = registry.upsert(JOB_ID, vec![7, 8]);
        assert!(matches!(outcome, UpsertOutcome::Exist));
        assert_eq!(registry.len(), 1);

        let entry = entry_of(&mut registry, job_key);
        let mut dispatched: Vec<TaskIndex> = Vec::new();
        while let Some(task_index) = entry.pop_next_task() {
            dispatched.push(task_index);
        }
        assert_eq!(dispatched, vec![0, 1, 7, 8]);
        Ok(())
    }

    #[test]
    fn upsert_keeps_the_scheduling_position_of_an_existing_job() -> anyhow::Result<()> {
        let mut registry = JobRegistry::new();
        let job_key = make_job_entry(&mut registry, JOB_ID, 1)?;

        let outcome = registry.upsert(JOB_ID, vec![1]);
        let UpsertOutcome::Exist = outcome else {
            bail!("re-registering a job must not hand out a second key to place");
        };

        // The entry the registry appended to must be the one the scheduling state's key resolves
        // to.
        let entry = entry_of(&mut registry, job_key);
        assert_eq!(entry.pop_next_task(), Some(0));
        assert_eq!(entry.pop_next_task(), Some(1));
        assert_eq!(entry.pop_next_task(), None);
        Ok(())
    }

    #[test]
    fn remove_by_job_id_hands_back_the_entry_and_drops_the_registration() -> anyhow::Result<()> {
        let mut registry = JobRegistry::new();
        let job_key = make_job_entry(&mut registry, JOB_ID, 2)?;

        let mut removed = registry
            .remove_by_job_id(JOB_ID)
            .expect("the registered job is removed by its finalization");
        assert_eq!(removed.job_id(), JOB_ID);
        assert_eq!(removed.take_ready_tasks(), VecDeque::from(vec![0, 1]));
        assert_eq!(registry.len(), 0);

        assert_eq!(job_id_of(&mut registry, job_key), None);
        assert!(registry.remove_by_job_id(JOB_ID).is_none());
        Ok(())
    }

    #[test]
    fn a_removed_jobs_key_never_resolves_to_a_later_job() -> anyhow::Result<()> {
        let mut registry = JobRegistry::new();
        let stale_key = make_job_entry(&mut registry, JOB_ID, 2)?;
        registry
            .remove_by_job_id(JOB_ID)
            .expect("the registered job is removed by its finalization");

        // The freed slot is offered to the next job registered, which is exactly the case a plain
        // index could not distinguish from the removed one.
        let next_job_id = JobId::from(JOB_ID.get() + 1);
        let next_key = make_job_entry(&mut registry, next_job_id, 1)?;
        assert_eq!(job_id_of(&mut registry, next_key), Some(next_job_id));
        assert_eq!(job_id_of(&mut registry, stale_key), None);
        Ok(())
    }

    #[test]
    fn inserting_tasks_restores_the_downgrade_budget() -> anyhow::Result<()> {
        let mut registry = JobRegistry::new();
        let job_key = make_job_entry(&mut registry, JOB_ID, 1)?;
        assert_eq!(
            entry_of(&mut registry, job_key).downgrade_counter(),
            DOWNGRADE_LIVES
        );

        for _ in 0..=DOWNGRADE_LIVES {
            entry_of(&mut registry, job_key).decrement_downgrade_counter();
        }
        assert_eq!(entry_of(&mut registry, job_key).downgrade_counter(), 0);

        assert!(matches!(
            registry.upsert(JOB_ID, vec![1]),
            UpsertOutcome::Exist
        ));
        assert_eq!(
            entry_of(&mut registry, job_key).downgrade_counter(),
            DOWNGRADE_LIVES
        );

        let entry = entry_of(&mut registry, job_key);
        entry.decrement_downgrade_counter();
        entry.reset_downgrade_counter();
        assert_eq!(entry.downgrade_counter(), DOWNGRADE_LIVES);
        Ok(())
    }

    #[test]
    fn take_ready_tasks_empties_a_job_without_removing_it() -> anyhow::Result<()> {
        let mut registry = JobRegistry::new();
        let job_key = make_job_entry(&mut registry, JOB_ID, 3)?;

        let entry = entry_of(&mut registry, job_key);
        assert_eq!(entry.take_ready_tasks(), VecDeque::from(vec![0, 1, 2]));
        assert!(!entry.has_ready_task());
        assert_eq!(entry.pop_next_task(), None);
        assert_eq!(entry.take_ready_tasks(), VecDeque::new());

        assert_eq!(job_id_of(&mut registry, job_key), Some(JOB_ID));
        assert_eq!(registry.len(), 1);
        Ok(())
    }

    #[test]
    fn remove_and_clear_drop_the_registered_jobs() -> anyhow::Result<()> {
        let mut registry = JobRegistry::new();
        let job_key = make_job_entry(&mut registry, JOB_ID, 1)?;
        let other_job_id = JobId::from(JOB_ID.get() + 1);
        let other_job_key = make_job_entry(&mut registry, other_job_id, 1)?;
        assert_eq!(registry.len(), 2);

        let removed = registry
            .remove(job_key)
            .expect("the registered job is removed by its retirement");
        assert_eq!(removed.job_id(), JOB_ID);
        assert_eq!(registry.len(), 1);
        assert!(registry.remove(job_key).is_none());

        // Removing one job leaves every other key resolving as it did.
        assert_eq!(job_id_of(&mut registry, other_job_key), Some(other_job_id));

        registry.clear();
        assert_eq!(registry.len(), 0);
        assert_eq!(job_id_of(&mut registry, other_job_key), None);
        Ok(())
    }
}
