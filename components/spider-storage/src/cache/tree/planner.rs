//! The plan of a job-tree event.

use spider_core::job::JobState;
use spider_core::task::TaskIndex;
use spider_core::types::id::JobId;
use spider_core::types::io::TaskOutput;

use crate::cache::tree::TreeEventOutcome;
use crate::cache::tree::TreeNode;
use crate::db::JobStateUpdate;
use crate::limits::MAX_ERROR_MESSAGE_CHARS;

/// The plan of one event.
#[derive(Debug)]
pub struct Plan {
    /// The guarded row updates to write, at most one per job.
    pub updates: Vec<JobStateUpdate>,

    /// The ready work to publish after the updates are applied.
    pub publications: Vec<Publication>,

    /// The outcome of the event.
    pub outcome: TreeEventOutcome,
}

impl Plan {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A plan with no update and no publication whose event has `outcome`.
    pub(super) const fn new(outcome: TreeEventOutcome) -> Self {
        Self {
            updates: Vec::new(),
            publications: Vec::new(),
            outcome,
        }
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of starting the root `root_id`: [`JobState::Ready`] to [`JobState::Running`],
    /// with the tasks of `ready_task_indices` published.
    pub(super) fn start_root(root_id: JobId, ready_task_indices: Vec<TaskIndex>) -> Self {
        Self {
            publications: vec![Publication::Ready {
                job_id: root_id,
                task_indices: ready_task_indices,
            }],
            ..Self::applying(JobStateUpdate::new(
                root_id,
                JobState::Ready,
                JobState::Running,
            ))
        }
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of the regular tasks of `job_id` being done: the job stores `outputs` and moves
    /// from [`JobState::Running`] to [`JobState::CommitReady`], with its commit task published, if
    /// `node` has a commit task, otherwise to [`JobState::Succeeded`].
    pub(super) fn complete_regular_tasks(
        job_id: JobId,
        node: &TreeNode,
        outputs: Vec<TaskOutput>,
    ) -> Self {
        let to = if node.has_commit {
            JobState::CommitReady
        } else {
            JobState::Succeeded
        };
        let mut plan = Self::applying(JobStateUpdate {
            outputs: Some(outputs),
            ..JobStateUpdate::new(job_id, JobState::Running, to)
        });
        if to == JobState::CommitReady {
            plan.publications.push(Publication::Commit(job_id));
        }
        plan
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of the commit task of `job_id` being done: [`JobState::CommitReady`] to
    /// [`JobState::Succeeded`].
    pub(super) fn complete_commit(job_id: JobId) -> Self {
        Self::applying(JobStateUpdate::new(
            job_id,
            JobState::CommitReady,
            JobState::Succeeded,
        ))
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of the cleanup task of `job_id` being done: [`JobState::CleanupReady`] to
    /// [`JobState::Cancelled`].
    pub(super) fn complete_cleanup(job_id: JobId) -> Self {
        Self::applying(JobStateUpdate::new(
            job_id,
            JobState::CleanupReady,
            JobState::Cancelled,
        ))
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of failing `job_id` from `from`: the job moves to [`JobState::Failed`] and stores
    /// `message`, cut to [`MAX_ERROR_MESSAGE_CHARS`] characters.
    pub(super) fn fail_job(job_id: JobId, from: JobState, message: &str) -> Self {
        Self::applying(JobStateUpdate {
            error_message: Some(truncate_chars(message, MAX_ERROR_MESSAGE_CHARS).to_owned()),
            ..JobStateUpdate::new(job_id, from, JobState::Failed)
        })
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of cancelling the root `root_id` from `from`: the root moves to its abort target
    /// (see [`TreeNode::abort_target`]), with its cleanup task published if that is
    /// [`JobState::CleanupReady`].
    pub(super) fn cancel_root(root_id: JobId, from: JobState, node: &TreeNode) -> Self {
        let to = node.abort_target(from);
        let mut plan = Self::applying(JobStateUpdate::new(root_id, from, to));
        if to == JobState::CleanupReady {
            plan.publications.push(Publication::Cleanup(root_id));
        }
        plan
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// A plan that writes `update` alone, publishes nothing, and whose event is applied with the
    /// state that `update` moves its job to.
    fn applying(update: JobStateUpdate) -> Self {
        Self {
            outcome: TreeEventOutcome::Applied(update.to),
            updates: vec![update],
            publications: Vec::new(),
        }
    }
}

/// Ready work that a plan makes available once its updates are applied.
#[derive(Debug, PartialEq, Eq)]
pub enum Publication {
    /// The ready tasks of a job.
    Ready {
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    },

    /// The commit task of a job.
    Commit(JobId),

    /// The cleanup task of a job.
    Cleanup(JobId),
}

/// # Returns
///
/// The first `max_chars` characters of `message`.
fn truncate_chars(message: &str, max_chars: usize) -> &str {
    message
        .char_indices()
        .nth(max_chars)
        .map_or(message, |(end, _)| &message[..end])
}
