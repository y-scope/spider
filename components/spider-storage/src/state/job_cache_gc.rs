//! Background actor for removing terminated jobs from the in-memory job cache.

use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;

use serde::Deserialize;
use spider_core::types::id::JobId;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cache::error::InternalError;
use crate::db::InternalJobOrchestration;
use crate::ready_queue::ReadyQueueSender;
use crate::state::JobCache;
use crate::task_instance_pool::TaskInstancePoolConnector;

/// Configuration for the job-cache GC actor.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
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
    pub fn enqueue_terminated_job(&self, job_id: JobId) {
        // Fire-and-forget: if the channel has been closed, just leave it. The GC coroutine is
        // closed by a cancellation token.
        let _ = self.sender.send(job_id);
    }

    /// # Returns
    ///
    /// A new [`JobCacheGcHandle`] backed by the given channel sender.
    pub(crate) const fn new(sender: UnboundedSender<JobId>) -> Self {
        Self { sender }
    }
}

/// Creates a job-cache GC actor.
///
/// # Type Parameters
///
/// * `ReadyQueueSenderType` - The type of the ready queue sender required by the job cache.
/// * `DbConnectorType` - The type of the DB-layer connector required by the job cache.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector required by the
///   job cache.
///
/// # Returns
///
/// A tuple on success, containing:
///
/// * The handle for enqueueing terminated jobs into the GC actor.
/// * The join handle for the GC actor.
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
    terminated_jobs: VecDeque<TerminatedJob>,
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
            terminated_jobs: VecDeque::new(),
            terminated_job_retention,
            receiver,
        }
    }

    /// Runs the actor loop until cancellation or sender shutdown.
    ///
    /// # Errors
    ///
    /// No errors are returned by the current implementation.
    async fn run(
        mut self,
        cancellation_token: CancellationToken,
        gc_interval_sec: u64,
    ) -> Result<(), InternalError> {
        let mut gc_interval = tokio::time::interval(Duration::from_secs(gc_interval_sec));
        gc_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                () = cancellation_token.cancelled() => {
                    return Ok(());
                }
                _ = gc_interval.tick() => {
                    let _removed_jobs = self.run_gc_cycle_at(Instant::now()).await;
                }
                job_id = self.receiver.recv() => {
                    let Some(job_id) = job_id else {
                        return Ok(());
                    };
                    self.enqueue_terminated_job(job_id);
                }
            }
        }
    }

    /// Adds a terminated job to the actor-owned GC queue.
    fn enqueue_terminated_job(&mut self, job_id: JobId) {
        self.terminated_jobs.push_back(TerminatedJob {
            job_id,
            enqueued_at: Instant::now(),
        });
    }

    /// Removes expired terminated jobs from the cache.
    ///
    /// # Returns
    ///
    /// The number of expired entries processed by this GC cycle.
    async fn run_gc_cycle_at(&mut self, now: Instant) -> usize {
        let mut expired_job_ids = Vec::new();
        while let Some(terminated_job) = self.terminated_jobs.front() {
            if now.duration_since(terminated_job.enqueued_at) < self.terminated_job_retention {
                break;
            }
            tracing::info!(
                job_id = % terminated_job.job_id,
                "Terminated job expired, removing from cache."
            );
            expired_job_ids.push(terminated_job.job_id);
            self.terminated_jobs.pop_front();
        }
        if expired_job_ids.is_empty() {
            return 0;
        }
        let num_expired_jobs = expired_job_ids.len();
        self.job_cache.remove_batch(&expired_job_ids).await;
        num_expired_jobs
    }

    /// # Returns
    ///
    /// The number of terminated jobs currently queued for retention.
    #[cfg(test)]
    fn get_num_queued_terminated_jobs(&self) -> usize {
        self.terminated_jobs.len()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use spider_core::types::id::JobId;

    use super::JobCacheGc;
    use super::JobCacheGcConfig;
    use crate::state::JobCache;
    use crate::state::test_utils::MockDbConnector;
    use crate::state::test_utils::MockReadyQueueSender;
    use crate::state::test_utils::MockTaskInstancePoolConnector;

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
        gc.enqueue_terminated_job(job_id);

        let removed = gc.run_gc_cycle_at(now + Duration::from_secs(9)).await;

        assert_eq!(removed, 0, "job should not be removed before retention");
        assert_eq!(
            gc.get_num_queued_terminated_jobs(),
            1,
            "job should remain queued for GC"
        );
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
        let job_id = JobId::random();
        gc.enqueue_terminated_job(job_id);
        let now = Instant::now();

        let removed = gc.run_gc_cycle_at(now + Duration::from_secs(10)).await;

        assert_eq!(removed, 1, "job should be removed after retention");
        assert_eq!(
            gc.get_num_queued_terminated_jobs(),
            0,
            "job should no longer be queued"
        );
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
        gc.enqueue_terminated_job(JobId::random());
        gc.enqueue_terminated_job(JobId::random());
        let now = Instant::now();

        let removed = gc.run_gc_cycle_at(now + Duration::from_secs(10)).await;

        assert_eq!(removed, 2, "all expired jobs should be removed");
        assert_eq!(
            gc.get_num_queued_terminated_jobs(),
            0,
            "no expired job should remain queued"
        );
        Ok(())
    }
}
