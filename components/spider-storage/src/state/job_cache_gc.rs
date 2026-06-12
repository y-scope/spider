//! Background actor for removing terminated jobs from the in-memory job cache.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use spider_core::types::id::JobId;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    cache::error::InternalError,
    db::InternalJobOrchestration,
    ready_queue::ReadyQueueSender,
    state::JobCache,
    task_instance_pool::TaskInstancePoolConnector,
};

/// Configuration for the job-cache GC actor.
#[derive(Debug, Clone, Copy)]
pub struct JobCacheGcConfig {
    /// Seconds to keep a terminated job in the in-memory cache before GC can remove it.
    pub terminated_job_retention_sec: u64,

    /// Interval in seconds between GC cycles.
    pub gc_interval_sec: u64,
}

impl JobCacheGcConfig {
    /// Validates the configuration parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::JobCacheGcInvalidConfig`] if `terminated_job_retention_sec` or
    ///   `gc_interval_sec` is zero.
    pub const fn validate(&self) -> Result<(), InternalError> {
        if self.terminated_job_retention_sec == 0 {
            return Err(InternalError::JobCacheGcInvalidConfig(
                "terminated_job_retention_sec must be greater than zero",
            ));
        }
        if self.gc_interval_sec == 0 {
            return Err(InternalError::JobCacheGcInvalidConfig(
                "gc_interval_sec must be greater than zero",
            ));
        }
        Ok(())
    }
}

impl Default for JobCacheGcConfig {
    fn default() -> Self {
        Self {
            terminated_job_retention_sec: 300,
            gc_interval_sec: 30,
        }
    }
}

/// Handle for enqueueing terminated jobs into the job-cache GC actor.
#[derive(Clone)]
pub struct JobCacheGcHandle {
    sender: UnboundedSender<JobId>,
}

impl JobCacheGcHandle {
    /// Enqueues a terminated job for delayed cache removal.
    ///
    /// # Errors
    ///
    /// Returns an error if the GC actor has stopped.
    pub fn enqueue_terminated_job(&self, job_id: JobId) -> Result<(), JobId> {
        self.sender.send(job_id).map_err(|e| e.0)
    }

    /// # Returns
    ///
    /// A new [`JobCacheGcHandle`] backed by the given channel sender.
    pub(crate) const fn new(sender: UnboundedSender<JobId>) -> Self {
        Self { sender }
    }
}

struct TerminatedJob {
    job_id: JobId,
    enqueued_at: Instant,
}

struct JobCacheGc<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    job_cache: JobCache<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    pending_jobs: VecDeque<TerminatedJob>,
    terminated_job_retention: Duration,
    receiver: UnboundedReceiver<JobId>,
}

impl<
    ReadyQueueSenderType: ReadyQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> JobCacheGc<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>
{
    /// # Returns
    ///
    /// A new [`JobCacheGc`] actor over the given cache and message receiver.
    const fn new(
        job_cache: JobCache<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
        terminated_job_retention: Duration,
        receiver: UnboundedReceiver<JobId>,
    ) -> Self {
        Self {
            job_cache,
            pending_jobs: VecDeque::new(),
            terminated_job_retention,
            receiver,
        }
    }

    /// Runs the actor loop until cancellation or sender shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * No errors are returned by the current implementation.
    async fn run(
        mut self,
        cancellation_token: CancellationToken,
        gc_interval_sec: u64,
    ) -> Result<(), InternalError> {
        let mut gc_interval = tokio::time::interval(Duration::from_secs(gc_interval_sec));
        gc_interval.tick().await;

        loop {
            tokio::select! {
                () = cancellation_token.cancelled() => {
                    return Ok(());
                }
                job_id = self.receiver.recv() => {
                    let Some(job_id) = job_id else {
                        return Ok(());
                    };
                    self.enqueue_terminated_job(job_id, Instant::now());
                }
                _ = gc_interval.tick() => {
                    let _removed_jobs = self.run_gc_cycle_at(Instant::now()).await;
                }
            }
        }
    }

    /// Adds a terminated job to the actor-owned GC queue.
    fn enqueue_terminated_job(&mut self, job_id: JobId, enqueued_at: Instant) {
        self.pending_jobs.push_back(TerminatedJob {
            job_id,
            enqueued_at,
        });
    }

    /// Removes expired terminated jobs from the cache.
    ///
    /// # Returns
    ///
    /// The number of expired entries processed by this GC cycle.
    async fn run_gc_cycle_at(&mut self, now: Instant) -> usize {
        let mut num_removed_jobs = 0;
        while let Some(terminated_job) = self.pending_jobs.front() {
            if now.duration_since(terminated_job.enqueued_at) < self.terminated_job_retention {
                break;
            }
            let terminated_job = self
                .pending_jobs
                .pop_front()
                .expect("pending terminated job should exist");
            self.job_cache.remove(terminated_job.job_id).await;
            num_removed_jobs += 1;
            tracing::info!(
                job_id = ? terminated_job.job_id,
                "Terminated job removed from cache.",
            );
        }
        num_removed_jobs
    }

    /// # Returns
    ///
    /// The number of jobs currently queued for delayed GC.
    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.pending_jobs.len()
    }
}

/// Creates a job-cache GC actor.
///
/// # Returns
///
/// A tuple containing the enqueue handle and spawned actor join handle on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`JobCacheGcConfig::validate`]'s return values on failure.
pub fn create_job_cache_gc<
    ReadyQueueSenderType: ReadyQueueSender + 'static,
    DbConnectorType: InternalJobOrchestration + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
>(
    job_cache: JobCache<ReadyQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    cancellation_token: CancellationToken,
    config: &JobCacheGcConfig,
) -> Result<(JobCacheGcHandle, JoinHandle<Result<(), InternalError>>), InternalError> {
    config.validate()?;
    let (sender, receiver) = unbounded_channel();
    let gc_interval_sec = config.gc_interval_sec;
    let gc = JobCacheGc::new(
        job_cache,
        Duration::from_secs(config.terminated_job_retention_sec),
        receiver,
    );
    let join_handle =
        tokio::spawn(async move { gc.run(cancellation_token, gc_interval_sec).await });
    Ok((JobCacheGcHandle::new(sender), join_handle))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use spider_core::types::id::JobId;

    use super::{JobCacheGc, JobCacheGcConfig};
    use crate::state::{
        JobCache,
        test_utils::{MockDbConnector, MockReadyQueueSender, MockTaskInstancePoolConnector},
    };

    type TestJobCache =
        JobCache<MockReadyQueueSender, MockDbConnector, MockTaskInstancePoolConnector>;

    #[test]
    fn config_rejects_zero_values() {
        let config = JobCacheGcConfig {
            terminated_job_retention_sec: 0,
            gc_interval_sec: 1,
        };
        assert!(
            config.validate().is_err(),
            "zero retention should be invalid"
        );

        let config = JobCacheGcConfig {
            terminated_job_retention_sec: 1,
            gc_interval_sec: 0,
        };
        assert!(
            config.validate().is_err(),
            "zero GC interval should be invalid"
        );
    }

    #[tokio::test]
    async fn gc_cycle_keeps_jobs_until_retention_expires() -> anyhow::Result<()> {
        let cache = TestJobCache::new();
        let mut gc = JobCacheGc::new(
            cache,
            Duration::from_secs(10),
            tokio::sync::mpsc::unbounded_channel().1,
        );
        let now = Instant::now();
        let job_id = JobId::random();
        gc.enqueue_terminated_job(job_id, now);

        let removed = gc.run_gc_cycle_at(now + Duration::from_secs(9)).await;

        assert_eq!(removed, 0, "job should not be removed before retention");
        assert_eq!(gc.pending_len(), 1, "job should remain queued for GC");
        Ok(())
    }

    #[tokio::test]
    async fn gc_cycle_removes_jobs_after_retention_expires() -> anyhow::Result<()> {
        let cache = TestJobCache::new();
        let mut gc = JobCacheGc::new(
            cache,
            Duration::from_secs(10),
            tokio::sync::mpsc::unbounded_channel().1,
        );
        let now = Instant::now();
        let job_id = JobId::random();
        gc.enqueue_terminated_job(job_id, now);

        let removed = gc.run_gc_cycle_at(now + Duration::from_secs(10)).await;

        assert_eq!(removed, 1, "job should be removed after retention");
        assert_eq!(gc.pending_len(), 0, "job should no longer be queued");
        Ok(())
    }

    #[tokio::test]
    async fn gc_cycle_removes_all_expired_jobs() -> anyhow::Result<()> {
        let cache = TestJobCache::new();
        let mut gc = JobCacheGc::new(
            cache,
            Duration::from_secs(10),
            tokio::sync::mpsc::unbounded_channel().1,
        );
        let now = Instant::now();
        gc.enqueue_terminated_job(JobId::random(), now);
        gc.enqueue_terminated_job(JobId::random(), now);

        let removed = gc.run_gc_cycle_at(now + Duration::from_secs(10)).await;

        assert_eq!(removed, 2, "all expired jobs should be removed");
        assert_eq!(gc.pending_len(), 0, "no expired job should remain queued");
        Ok(())
    }
}
