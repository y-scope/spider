//! The inbound-poll result formatter of the resource-group-aware round-robin scheduler.

use std::collections::HashMap;

use spider_core::task::TaskIndex;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;

use crate::core_impl::inbound_queue_reader::AsyncInboundQueueReader;
use crate::core_impl::inbound_queue_reader::InboundPollResultFormatter;
use crate::core_impl::inbound_queue_reader::InboundPollState;
use crate::types::InboundEntry;

/// The regular tasks a single poll drained for one job.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ReadyBatch {
    /// The resource group that owns the job.
    pub(super) resource_group_id: ResourceGroupId,

    /// The job the tasks belong to.
    pub(super) job_id: JobId,

    /// The ready tasks, sorted and deduplicated.
    pub(super) task_indices: Vec<TaskIndex>,
}

/// A job that has reached one of its terminal states.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct FinalizedJob {
    /// The resource group that owns the job.
    pub(super) resource_group_id: ResourceGroupId,

    /// The finalized job.
    pub(super) job_id: JobId,
}

/// The formatter of the resource-group-aware core, which consumes the drained entries grouped by
/// job.
pub(super) struct RgInboundPollResultFormatter;

impl InboundPollResultFormatter for RgInboundPollResultFormatter {
    type FinalizedResult = Vec<FinalizedJob>;
    type ReadyResult = Vec<ReadyBatch>;

    fn format_ready(entries: Vec<InboundEntry>) -> Self::ReadyResult {
        format_ready_job_batches(entries)
    }

    fn format_finalized(entries: Vec<InboundEntry>) -> Self::FinalizedResult {
        format_finalized_jobs(entries)
    }
}

/// The inbound-queue reader of the resource-group-aware core, which formats the entries drained
/// from every lane with [`RgInboundPollResultFormatter`].
///
/// # Type Parameters
///
/// * `StorageClientType` - The storage client used to poll the inbound queue.
pub(super) type RgInboundQueueReader<StorageClientType> =
    AsyncInboundQueueReader<StorageClientType, RgInboundPollResultFormatter>;

/// The state of an inbound-queue poll started by the resource-group-aware core.
pub(super) type RgInboundPollState = InboundPollState<RgInboundPollResultFormatter>;

/// Groups the entries drained from the regular-task lane by job, discarding every entry that does
/// not carry a task index.
///
/// # Returns
///
/// One batch per job, where jobs are in an unspecified order.
fn format_ready_job_batches(entries: Vec<InboundEntry>) -> Vec<ReadyBatch> {
    let mut batches: HashMap<JobId, (ResourceGroupId, Vec<TaskIndex>)> = HashMap::new();
    for entry in entries {
        let TaskId::Index(task_index) = entry.task_id else {
            continue;
        };
        batches
            .entry(entry.job_id)
            .or_insert_with(|| (entry.resource_group_id, Vec::new()))
            .1
            .push(task_index);
    }

    batches
        .into_iter()
        .map(|(job_id, (resource_group_id, mut task_indices))| {
            task_indices.sort_unstable();
            task_indices.dedup();
            ReadyBatch {
                resource_group_id,
                job_id,
                task_indices,
            }
        })
        .collect()
}

/// # Returns
///
/// One finalized job per entry drained from a finalization lane, in the order they were drained.
fn format_finalized_jobs(entries: Vec<InboundEntry>) -> Vec<FinalizedJob> {
    entries
        .into_iter()
        .map(|entry| FinalizedJob {
            resource_group_id: entry.resource_group_id,
            job_id: entry.job_id,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core_impl::inbound_queue_reader::test_harness::DEFAULT_SESSION_ID;
    use crate::core_impl::inbound_queue_reader::test_harness::MAX_ENTRIES;
    use crate::core_impl::inbound_queue_reader::test_harness::MockStorageClient;
    use crate::core_impl::inbound_queue_reader::test_harness::POLL_TIMEOUT;
    use crate::core_impl::inbound_queue_reader::test_harness::collect_when_ready;
    use crate::core_impl::inbound_queue_reader::test_harness::make_entry;

    /// The reader under test, which formats every lane's entries with
    /// [`RgInboundPollResultFormatter`].
    type TestReader = RgInboundQueueReader<MockStorageClient>;

    /// The resource group most entries in this module belong to.
    const RG_ID: ResourceGroupId = ResourceGroupId::from(4);

    /// The resource group used where a test needs a second group.
    const OTHER_RG_ID: ResourceGroupId = ResourceGroupId::from(5);

    /// The first job a test polls entries for.
    const JOB_A: JobId = JobId::from(10);

    /// The second job a test polls entries for.
    const JOB_B: JobId = JobId::from(11);

    /// The third job a test polls entries for.
    const JOB_C: JobId = JobId::from(12);

    /// # Returns
    ///
    /// The batch in `batches` whose [`ReadyBatch::job_id`] is `job_id`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if no batch was polled for `job_id`.
    fn find_batch(batches: &[ReadyBatch], job_id: JobId) -> anyhow::Result<&ReadyBatch> {
        batches
            .iter()
            .find(|batch| batch.job_id == job_id)
            .ok_or_else(|| anyhow::anyhow!("no ready batch was polled for job {job_id}"))
    }

    #[tokio::test]
    async fn ready_entries_are_grouped_by_job() -> anyhow::Result<()> {
        let storage_client = MockStorageClient::new();
        storage_client.push_ready_batch(
            DEFAULT_SESSION_ID,
            vec![
                make_entry(RG_ID, JOB_A, TaskId::Index(3)),
                make_entry(OTHER_RG_ID, JOB_B, TaskId::Index(7)),
                make_entry(RG_ID, JOB_A, TaskId::Index(1)),
                make_entry(RG_ID, JOB_A, TaskId::Index(3)),
                make_entry(RG_ID, JOB_A, TaskId::Index(2)),
            ],
        );
        let mut reader = TestReader::new(storage_client);
        reader.start(POLL_TIMEOUT, MAX_ENTRIES, MAX_ENTRIES, MAX_ENTRIES)?;

        let result = collect_when_ready(&mut reader, DEFAULT_SESSION_ID).await?;
        assert_eq!(result.ready_result.len(), 2);
        assert_eq!(
            find_batch(&result.ready_result, JOB_A)?,
            &ReadyBatch {
                resource_group_id: RG_ID,
                job_id: JOB_A,
                task_indices: vec![1, 2, 3],
            }
        );
        assert_eq!(
            find_batch(&result.ready_result, JOB_B)?,
            &ReadyBatch {
                resource_group_id: OTHER_RG_ID,
                job_id: JOB_B,
                task_indices: vec![7],
            }
        );
        assert_eq!(result.commit_ready_result.as_slice(), &[]);
        assert_eq!(result.cleanup_ready_result.as_slice(), &[]);
        Ok(())
    }

    #[tokio::test]
    async fn finalization_entries_are_mapped_to_finalized_jobs() -> anyhow::Result<()> {
        let storage_client = MockStorageClient::new();
        storage_client.push_commit_ready_batch(
            DEFAULT_SESSION_ID,
            vec![
                make_entry(RG_ID, JOB_A, TaskId::Commit),
                make_entry(OTHER_RG_ID, JOB_B, TaskId::Commit),
            ],
        );
        storage_client.push_cleanup_ready_batch(
            DEFAULT_SESSION_ID,
            vec![make_entry(RG_ID, JOB_C, TaskId::Cleanup)],
        );
        let mut reader = TestReader::new(storage_client);
        reader.start(POLL_TIMEOUT, MAX_ENTRIES, MAX_ENTRIES, MAX_ENTRIES)?;

        let result = collect_when_ready(&mut reader, DEFAULT_SESSION_ID).await?;
        assert_eq!(
            result.commit_ready_result.as_slice(),
            &[
                FinalizedJob {
                    resource_group_id: RG_ID,
                    job_id: JOB_A,
                },
                FinalizedJob {
                    resource_group_id: OTHER_RG_ID,
                    job_id: JOB_B,
                }
            ]
        );
        assert_eq!(
            result.cleanup_ready_result.as_slice(),
            &[FinalizedJob {
                resource_group_id: RG_ID,
                job_id: JOB_C,
            }]
        );
        assert_eq!(result.ready_result.as_slice(), &[]);
        Ok(())
    }
}
