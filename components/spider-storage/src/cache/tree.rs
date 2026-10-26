//! Job trees: the tree control block and the planner.
//!
//! A job tree is a root job and its descendants. Every job of a tree submits its job-level events
//! through the tree's control block to the tree's committer, the single owner of the tree's event
//! receivers and structure. The committer plans each event against the tree's structure and its
//! jobs' collected states. Cancels and terminal failures travel on the fast-forward channel, which
//! the committer reads first; every other event travels on the regular channel.

use std::collections::HashMap;
use std::future::Future;

use spider_core::job::JobState;
use spider_core::task::TaskIndex;
use spider_core::types::id::JobId;
use spider_core::types::io::TaskOutput;
use spider_utils::shutdown::ShutdownHandle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::cache::error::CacheError;
use crate::cache::error::InternalError;
use crate::cache::error::StaleStateError;
use crate::db::JobStateUpdate;
use crate::limits::MAX_ERROR_MESSAGE_CHARS;

/// The handle through which every job of a tree submits its events to the tree's committer.
pub struct TreeControlBlock {
    fast_forward: mpsc::UnboundedSender<Pending>,
    regular: mpsc::UnboundedSender<Pending>,
    shutdown_handle: ShutdownHandle,
}

impl TreeControlBlock {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A pair, containing:
    ///
    /// * The control block of a new tree, whose refused events resolve to
    ///   [`CacheError::ServiceStopping`] once `shutdown_handle` is cancelled.
    /// * The receivers of the tree's events, for the tree's committer.
    #[must_use]
    pub fn new(shutdown_handle: ShutdownHandle) -> (Self, EventReceivers) {
        let (fast_forward, fast_forward_receiver) = mpsc::unbounded_channel();
        let (regular, regular_receiver) = mpsc::unbounded_channel();
        (
            Self {
                fast_forward,
                regular,
                shutdown_handle,
            },
            EventReceivers {
                fast_forward: fast_forward_receiver,
                regular: regular_receiver,
            },
        )
    }

    /// Sends `event` to the tree's committer at the call, before the returned future is first
    /// polled: a [`TreeEventKind::CancelRoot`] or a [`TreeEventKind::TerminalFailure`] on the
    /// fast-forward channel, any other event on the regular channel.
    ///
    /// # Returns
    ///
    /// A future that resolves to the committer's reply to `event`, or to
    /// [`TreeOutcome::Untracked`] if the committer no longer takes events and the control block's
    /// shutdown handle is not cancelled.
    ///
    /// # Errors
    ///
    /// The future resolves to an error if:
    ///
    /// * [`CacheError::ServiceStopping`] if the committer no longer takes events once the control
    ///   block's shutdown handle is cancelled, in which case `event` was not and will not be
    ///   applied.
    /// * [`InternalError::TreePoisoned`] if the committer dropped `event` without a reply.
    /// * Any [`CacheError`] the committer replies to `event` with, e.g.,
    ///   [`CacheError::ServiceStopping`] if it shuts down before applying `event`.
    pub fn submit(&self, event: TreeEvent) -> impl Future<Output = TreeReply> + Send + use<> {
        let (reply, receiver) = oneshot::channel();
        let sender = if event.kind.is_fast_forward() {
            &self.fast_forward
        } else {
            &self.regular
        };
        let refusal = sender.send(Pending { event, reply }).is_err().then(|| {
            if self.shutdown_handle.is_cancelled() {
                Err(CacheError::ServiceStopping)
            } else {
                Ok(TreeOutcome::Untracked)
            }
        });
        async move {
            if let Some(refusal) = refusal {
                return refusal;
            }
            receiver
                .await
                .unwrap_or_else(|_| Err(InternalError::TreePoisoned.into()))
        }
    }
}

/// The receivers of a tree's two event channels, owned by the tree's committer.
pub struct EventReceivers {
    /// The [`TreeEventKind::CancelRoot`] and [`TreeEventKind::TerminalFailure`] events.
    pub fast_forward: mpsc::UnboundedReceiver<Pending>,

    /// Every other event.
    pub regular: mpsc::UnboundedReceiver<Pending>,
}

/// An event on its way to the tree's committer, with the sender of its reply.
pub struct Pending {
    pub event: TreeEvent,
    pub reply: oneshot::Sender<TreeReply>,
}

/// A job-level event of one job of a tree.
#[derive(Debug)]
pub struct TreeEvent {
    pub job_id: JobId,
    pub kind: TreeEventKind,
}

/// The kind of a job-level event.
#[derive(Debug, PartialEq, Eq)]
pub enum TreeEventKind {
    /// The root is started.
    Start,

    /// The job's last regular task succeeded.
    RegularTasksDone,

    /// The job's commit task succeeded.
    CommitDone,

    /// The job's cleanup task succeeded.
    CleanupDone,

    /// A task of the job in the [`Lane`] `lane` failed with no retry left.
    TerminalFailure { lane: Lane, message: String },

    /// The root is cancelled.
    CancelRoot,
}

impl TreeEventKind {
    /// # Returns
    ///
    /// Whether an event of this kind travels on its tree's fast-forward channel: a
    /// [`Self::CancelRoot`] or a [`Self::TerminalFailure`].
    #[must_use]
    pub const fn is_fast_forward(&self) -> bool {
        matches!(self, Self::CancelRoot | Self::TerminalFailure { .. })
    }

    /// Checks that a job in `state` accepts an event of this kind.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::JobAlreadyStarted`] if a [`Self::Start`] finds the job not
    ///   [`JobState::Ready`].
    /// * [`StaleStateError::JobAlreadyTerminated`] if a task event ([`Self::RegularTasksDone`],
    ///   [`Self::CommitDone`], [`Self::CleanupDone`] or [`Self::TerminalFailure`]) finds the job
    ///   terminal, or a [`Self::CancelRoot`] finds it [`JobState::Succeeded`] or
    ///   [`JobState::Failed`].
    /// * [`StaleStateError::JobNoLongerRunning`], [`StaleStateError::JobNoLongerCommitReady`] or
    ///   [`StaleStateError::JobNoLongerCleanupReady`] if a task event finds the job non-terminal
    ///   but not in the state that runs the event's [`Lane`].
    /// * [`StaleStateError::JobCancellationAlreadyRequested`] if a [`Self::CancelRoot`] finds the
    ///   job [`JobState::CleanupReady`].
    /// * [`StaleStateError::JobAlreadyCancelled`] if a [`Self::CancelRoot`] finds the job
    ///   [`JobState::Cancelled`].
    pub const fn ensure_accepts(&self, state: JobState) -> Result<(), StaleStateError> {
        match self {
            Self::Start => match state {
                JobState::Ready => Ok(()),
                _ => Err(StaleStateError::JobAlreadyStarted),
            },
            Self::RegularTasksDone => Lane::Regular.ensure_accepts(state),
            Self::CommitDone => Lane::Commit.ensure_accepts(state),
            Self::CleanupDone => Lane::Cleanup.ensure_accepts(state),
            Self::TerminalFailure { lane, .. } => lane.ensure_accepts(state),
            Self::CancelRoot => match state {
                JobState::CleanupReady => Err(StaleStateError::JobCancellationAlreadyRequested),
                JobState::Cancelled => Err(StaleStateError::JobAlreadyCancelled),
                state if state.is_terminal() => Err(StaleStateError::JobAlreadyTerminated(state)),
                _ => Ok(()),
            },
        }
    }
}

/// The lane of a task: whether it is a regular, commit or cleanup task of its job.
///
/// A job runs the tasks of one lane per state: its regular tasks while [`JobState::Running`], its
/// commit task while [`JobState::CommitReady`] and its cleanup task while
/// [`JobState::CleanupReady`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    Regular,
    Commit,
    Cleanup,
}

impl Lane {
    /// Checks that a job in `state` runs the tasks of this lane.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`StaleStateError::JobAlreadyTerminated`] if `state` is terminal.
    /// * [`StaleStateError::JobNoLongerRunning`], [`StaleStateError::JobNoLongerCommitReady`] or
    ///   [`StaleStateError::JobNoLongerCleanupReady`], by lane, if `state` is any other state that
    ///   does not run this lane.
    const fn ensure_accepts(self, state: JobState) -> Result<(), StaleStateError> {
        match (self, state) {
            (Self::Regular, JobState::Running)
            | (Self::Commit, JobState::CommitReady)
            | (Self::Cleanup, JobState::CleanupReady) => Ok(()),
            _ if state.is_terminal() => Err(StaleStateError::JobAlreadyTerminated(state)),
            (Self::Regular, _) => Err(StaleStateError::JobNoLongerRunning),
            (Self::Commit, _) => Err(StaleStateError::JobNoLongerCommitReady),
            (Self::Cleanup, _) => Err(StaleStateError::JobNoLongerCleanupReady),
        }
    }
}

/// The outcome of an event.
#[derive(Debug)]
pub enum TreeOutcome {
    /// The event was applied. Carries the job's new state.
    Applied(JobState),

    /// The job's state does not accept the event, so nothing was written for it.
    Stale(StaleStateError),

    /// The event's job is not in the tree, or the tree's committer no longer takes events.
    Untracked,
}

/// The reply to an event.
pub type TreeReply = Result<TreeOutcome, CacheError>;

/// The structure of a tree: its non-terminal jobs. It holds no job states.
#[derive(Debug)]
pub struct TreeStructure {
    nodes: HashMap<JobId, TreeNode>,
}

impl TreeStructure {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The structure of a tree that holds only its root.
    #[must_use]
    pub fn new_root(root_id: JobId, root: TreeNode) -> Self {
        Self {
            nodes: HashMap::from([(root_id, root)]),
        }
    }

    /// Plans one event against the tree's structure and the collected states of its jobs.
    ///
    /// # Returns
    ///
    /// The plan of `event`:
    ///
    /// * [`TreeOutcome::Untracked`] with no update if the event's job is not in the tree.
    /// * [`TreeOutcome::Stale`] with no update if the job's state does not accept the event (see
    ///   [`TreeEventKind::ensure_accepts`]).
    /// * Otherwise, the event's update and publications:
    ///   * [`TreeEventKind::Start`]: the root moves from [`JobState::Ready`] to
    ///     [`JobState::Running`], and its ready tasks are published.
    ///   * [`TreeEventKind::RegularTasksDone`]: the job stores its outputs and moves from
    ///     [`JobState::Running`] to [`JobState::CommitReady`], with its commit task published, if
    ///     it has a commit task, otherwise to [`JobState::Succeeded`].
    ///   * [`TreeEventKind::CommitDone`]: the job moves from [`JobState::CommitReady`] to
    ///     [`JobState::Succeeded`].
    ///   * [`TreeEventKind::CleanupDone`]: the job moves from [`JobState::CleanupReady`] to
    ///     [`JobState::Cancelled`].
    ///   * [`TreeEventKind::TerminalFailure`]: the job moves from its lane's state to
    ///     [`JobState::Failed`] and stores the failure's message, cut to
    ///     [`MAX_ERROR_MESSAGE_CHARS`] characters.
    ///   * [`TreeEventKind::CancelRoot`]: the root moves to [`JobState::Cancelled`] if it is
    ///     [`JobState::Ready`] or has no cleanup task, otherwise to [`JobState::CleanupReady`],
    ///     with its cleanup task published.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * The state of the event's job, which is in the tree, is not in `collected`.
    /// * An accepted [`TreeEventKind::Start`] does not come with [`PlannerInput::Start`], or an
    ///   accepted [`TreeEventKind::RegularTasksDone`] does not come with
    ///   [`PlannerInput::RegularTasksDone`].
    #[must_use]
    pub fn plan_event(
        &self,
        collected: &HashMap<JobId, JobState>,
        event: &TreeEvent,
        input: PlannerInput,
    ) -> Plan {
        let job_id = event.job_id;
        let Some(node) = self.nodes.get(&job_id) else {
            return Plan::new(TreeOutcome::Untracked);
        };
        let from = *collected
            .get(&job_id)
            .expect("the state of the event's job should be collected");
        if let Err(error) = event.kind.ensure_accepts(from) {
            return Plan::new(TreeOutcome::Stale(error));
        }
        match &event.kind {
            TreeEventKind::Start => Plan::start_root(job_id, input),
            TreeEventKind::RegularTasksDone => Plan::complete_regular_tasks(job_id, node, input),
            TreeEventKind::CommitDone => Plan::complete_commit(job_id),
            TreeEventKind::CleanupDone => Plan::complete_cleanup(job_id),
            TreeEventKind::TerminalFailure { message, .. } => Plan::fail_job(job_id, from, message),
            TreeEventKind::CancelRoot => Plan::cancel_root(job_id, from, node),
        }
    }
}

/// A non-terminal job of a tree, as the planner sees it.
#[derive(Debug)]
pub struct TreeNode {
    pub has_commit: bool,
    pub has_cleanup: bool,
}

impl TreeNode {
    /// # Returns
    ///
    /// The state that a cancel moves the job from `state` to: [`JobState::Cancelled`] if the job
    /// is [`JobState::Ready`] or has no cleanup task, otherwise [`JobState::CleanupReady`]. A
    /// [`JobState::Ready`] job never moves to [`JobState::CleanupReady`]: none of its tasks ran,
    /// and the edge is not a valid job state transition.
    const fn abort_target(&self, state: JobState) -> JobState {
        if matches!(state, JobState::Ready) || !self.has_cleanup {
            JobState::Cancelled
        } else {
            JobState::CleanupReady
        }
    }
}

/// The input, besides the job states, that the committer collects to plan an event.
#[derive(Debug)]
pub enum PlannerInput {
    /// The ready task indices of a [`JobState::Ready`] root.
    Start { ready_task_indices: Vec<TaskIndex> },

    /// The outputs of a [`JobState::Running`] job whose regular tasks are done.
    RegularTasksDone { outputs: Vec<TaskOutput> },

    /// Nothing, because the event needs nothing or its job is no longer in the state that the
    /// event expects.
    Empty,
}

/// The plan of one event.
#[derive(Debug)]
pub struct Plan {
    /// The guarded row updates to write, at most one per job.
    pub updates: Vec<JobStateUpdate>,

    /// The ready work to publish after the updates are applied.
    pub publications: Vec<Publication>,

    /// The outcome of the event.
    pub outcome: TreeOutcome,
}

impl Plan {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A plan with no update and no publication whose event has `outcome`.
    const fn new(outcome: TreeOutcome) -> Self {
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
    /// A plan that writes `update` alone, publishes nothing, and whose event is applied with the
    /// state that `update` moves its job to.
    fn applying(update: JobStateUpdate) -> Self {
        Self {
            outcome: TreeOutcome::Applied(update.to),
            updates: vec![update],
            publications: Vec::new(),
        }
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of starting the root `root_id`: [`JobState::Ready`] to [`JobState::Running`],
    /// with the ready tasks of `input` published.
    ///
    /// # Panics
    ///
    /// Panics if `input` is not [`PlannerInput::Start`].
    fn start_root(root_id: JobId, input: PlannerInput) -> Self {
        let PlannerInput::Start { ready_task_indices } = input else {
            panic!("a ready root's start should carry its ready task indices");
        };
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
    /// The plan of the regular tasks of `job_id` being done: the job stores the outputs of `input`
    /// and moves from [`JobState::Running`] to [`JobState::CommitReady`], with its commit task
    /// published, if `node` has a commit task, otherwise to [`JobState::Succeeded`].
    ///
    /// # Panics
    ///
    /// Panics if `input` is not [`PlannerInput::RegularTasksDone`].
    fn complete_regular_tasks(job_id: JobId, node: &TreeNode, input: PlannerInput) -> Self {
        let PlannerInput::RegularTasksDone { outputs } = input else {
            panic!("a running job's regular-tasks-done event should carry its outputs");
        };
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
    fn complete_commit(job_id: JobId) -> Self {
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
    fn complete_cleanup(job_id: JobId) -> Self {
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
    fn fail_job(job_id: JobId, from: JobState, message: &str) -> Self {
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
    fn cancel_root(root_id: JobId, from: JobState, node: &TreeNode) -> Self {
        let to = node.abort_target(from);
        let mut plan = Self::applying(JobStateUpdate::new(root_id, from, to));
        if to == JobState::CleanupReady {
            plan.publications.push(Publication::Cleanup(root_id));
        }
        plan
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

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT_ID: JobId = JobId::from(1);

    const PLAIN_NODE: TreeNode = TreeNode {
        has_commit: false,
        has_cleanup: false,
    };

    // `BlockingOnChildren` is left out: no job can be in it until a tree has children.
    const STATES: [JobState; 7] = [
        JobState::Ready,
        JobState::Running,
        JobState::CommitReady,
        JobState::CleanupReady,
        JobState::Succeeded,
        JobState::Failed,
        JobState::Cancelled,
    ];

    const fn root_event(kind: TreeEventKind) -> TreeEvent {
        TreeEvent {
            job_id: ROOT_ID,
            kind,
        }
    }

    fn failure(lane: Lane, message: &str) -> TreeEventKind {
        TreeEventKind::TerminalFailure {
            lane,
            message: message.to_owned(),
        }
    }

    /// Plans an event of the root of a tree that holds only `node` as its root, in `state`.
    ///
    /// # Returns
    ///
    /// The plan.
    fn plan_root_event(
        node: TreeNode,
        state: JobState,
        kind: TreeEventKind,
        input: PlannerInput,
    ) -> Plan {
        TreeStructure::new_root(ROOT_ID, node).plan_event(
            &HashMap::from([(ROOT_ID, state)]),
            &root_event(kind),
            input,
        )
    }

    /// Drains the events that `receiver` holds.
    ///
    /// # Returns
    ///
    /// The kinds of the drained events, in the order they were sent.
    fn drain_kinds(receiver: &mut mpsc::UnboundedReceiver<Pending>) -> Vec<TreeEventKind> {
        let mut kinds = Vec::new();
        while let Ok(pending) = receiver.try_recv() {
            kinds.push(pending.event.kind);
        }
        kinds
    }

    #[test]
    fn start_is_accepted_only_by_a_ready_job() {
        for state in STATES {
            let result = TreeEventKind::Start.ensure_accepts(state);
            let is_expected = if state == JobState::Ready {
                matches!(result, Ok(()))
            } else {
                matches!(result, Err(StaleStateError::JobAlreadyStarted))
            };
            assert!(is_expected, "a start in {state} gave {result:?}");
        }
    }

    #[test]
    fn a_task_event_is_accepted_only_in_its_lanes_state() {
        use std::mem::discriminant;

        for (kind, lane_state, no_longer_error) in [
            (
                TreeEventKind::RegularTasksDone,
                JobState::Running,
                StaleStateError::JobNoLongerRunning,
            ),
            (
                TreeEventKind::CommitDone,
                JobState::CommitReady,
                StaleStateError::JobNoLongerCommitReady,
            ),
            (
                TreeEventKind::CleanupDone,
                JobState::CleanupReady,
                StaleStateError::JobNoLongerCleanupReady,
            ),
            (
                failure(Lane::Regular, "boom"),
                JobState::Running,
                StaleStateError::JobNoLongerRunning,
            ),
            (
                failure(Lane::Commit, "boom"),
                JobState::CommitReady,
                StaleStateError::JobNoLongerCommitReady,
            ),
            (
                failure(Lane::Cleanup, "boom"),
                JobState::CleanupReady,
                StaleStateError::JobNoLongerCleanupReady,
            ),
        ] {
            for state in STATES {
                let result = kind.ensure_accepts(state);
                let is_expected = if state == lane_state {
                    matches!(result, Ok(()))
                } else if state.is_terminal() {
                    matches!(
                        result,
                        Err(StaleStateError::JobAlreadyTerminated(terminal_state))
                            if terminal_state == state
                    )
                } else {
                    result
                        .as_ref()
                        .is_err_and(|error| discriminant(error) == discriminant(&no_longer_error))
                };
                assert!(is_expected, "{kind:?} in {state} gave {result:?}");
            }
        }
    }

    #[test]
    fn cancel_root_is_accepted_until_the_root_is_cleanup_ready_or_terminal() {
        for state in [JobState::Ready, JobState::Running, JobState::CommitReady] {
            let result = TreeEventKind::CancelRoot.ensure_accepts(state);
            assert!(
                matches!(result, Ok(())),
                "a cancel in {state} gave {result:?}"
            );
        }

        let result = TreeEventKind::CancelRoot.ensure_accepts(JobState::CleanupReady);
        assert!(
            matches!(
                result,
                Err(StaleStateError::JobCancellationAlreadyRequested)
            ),
            "a cancel in CleanupReady gave {result:?}"
        );

        let result = TreeEventKind::CancelRoot.ensure_accepts(JobState::Cancelled);
        assert!(
            matches!(result, Err(StaleStateError::JobAlreadyCancelled)),
            "a cancel in Cancelled gave {result:?}"
        );

        for state in [JobState::Succeeded, JobState::Failed] {
            let result = TreeEventKind::CancelRoot.ensure_accepts(state);
            assert!(
                matches!(
                    result,
                    Err(StaleStateError::JobAlreadyTerminated(terminal_state))
                        if terminal_state == state
                ),
                "a cancel in {state} gave {result:?}"
            );
        }
    }

    #[test]
    fn an_event_of_a_job_not_in_the_tree_is_untracked_without_reading_its_state() {
        let plan = TreeStructure::new_root(ROOT_ID, PLAIN_NODE).plan_event(
            &HashMap::new(),
            &TreeEvent {
                job_id: JobId::from(2),
                kind: TreeEventKind::CommitDone,
            },
            PlannerInput::Empty,
        );
        assert!(
            matches!(plan.outcome, TreeOutcome::Untracked),
            "an event of a job not in the tree gave {:?}",
            plan.outcome
        );
    }

    #[test]
    fn an_event_that_its_jobs_state_does_not_accept_plans_nothing() {
        let plan = plan_root_event(
            TreeNode {
                has_cleanup: true,
                ..PLAIN_NODE
            },
            JobState::CleanupReady,
            TreeEventKind::CancelRoot,
            PlannerInput::Empty,
        );
        assert!(
            matches!(
                plan.outcome,
                TreeOutcome::Stale(StaleStateError::JobCancellationAlreadyRequested)
            ),
            "a cancel of a cleanup-ready root gave {:?}",
            plan.outcome
        );
        assert_eq!(plan.updates, []);
        assert_eq!(plan.publications, []);
    }

    #[test]
    fn start_moves_a_ready_root_to_running_and_publishes_its_ready_tasks() {
        let plan = plan_root_event(
            PLAIN_NODE,
            JobState::Ready,
            TreeEventKind::Start,
            PlannerInput::Start {
                ready_task_indices: vec![0, 2],
            },
        );
        assert_eq!(
            plan.updates,
            [JobStateUpdate::new(
                ROOT_ID,
                JobState::Ready,
                JobState::Running
            )]
        );
        assert_eq!(
            plan.publications,
            [Publication::Ready {
                job_id: ROOT_ID,
                task_indices: vec![0, 2],
            }]
        );
        assert!(
            matches!(plan.outcome, TreeOutcome::Applied(JobState::Running)),
            "a start of a ready root gave {:?}",
            plan.outcome
        );
    }

    #[test]
    fn regular_tasks_done_stores_the_outputs_and_commits_only_a_job_with_a_commit_task() {
        let outputs = vec![vec![1u8, 2, 3]];
        for (has_commit, to, publications) in [
            (
                true,
                JobState::CommitReady,
                vec![Publication::Commit(ROOT_ID)],
            ),
            (false, JobState::Succeeded, Vec::new()),
        ] {
            let plan = plan_root_event(
                TreeNode {
                    has_commit,
                    ..PLAIN_NODE
                },
                JobState::Running,
                TreeEventKind::RegularTasksDone,
                PlannerInput::RegularTasksDone {
                    outputs: outputs.clone(),
                },
            );
            assert_eq!(
                plan.updates,
                [JobStateUpdate {
                    outputs: Some(outputs.clone()),
                    ..JobStateUpdate::new(ROOT_ID, JobState::Running, to)
                }]
            );
            assert_eq!(plan.publications, publications);
        }
    }

    #[test]
    fn commit_done_succeeds_a_commit_ready_job_without_storing_outputs() {
        let plan = plan_root_event(
            TreeNode {
                has_commit: true,
                ..PLAIN_NODE
            },
            JobState::CommitReady,
            TreeEventKind::CommitDone,
            PlannerInput::Empty,
        );
        assert_eq!(
            plan.updates,
            [JobStateUpdate::new(
                ROOT_ID,
                JobState::CommitReady,
                JobState::Succeeded
            )]
        );
        assert_eq!(plan.publications, []);
    }

    #[test]
    fn cleanup_done_cancels_a_cleanup_ready_job() {
        let plan = plan_root_event(
            TreeNode {
                has_cleanup: true,
                ..PLAIN_NODE
            },
            JobState::CleanupReady,
            TreeEventKind::CleanupDone,
            PlannerInput::Empty,
        );
        assert_eq!(
            plan.updates,
            [JobStateUpdate::new(
                ROOT_ID,
                JobState::CleanupReady,
                JobState::Cancelled
            )]
        );
        assert_eq!(plan.publications, []);
    }

    #[test]
    fn a_terminal_failure_fails_the_job_from_its_lanes_state_with_its_message() {
        for (lane, state) in [
            (Lane::Regular, JobState::Running),
            (Lane::Commit, JobState::CommitReady),
            (Lane::Cleanup, JobState::CleanupReady),
        ] {
            let plan = plan_root_event(
                TreeNode {
                    has_commit: true,
                    has_cleanup: true,
                },
                state,
                failure(lane, "boom"),
                PlannerInput::Empty,
            );
            assert_eq!(
                plan.updates,
                [JobStateUpdate {
                    error_message: Some("boom".to_owned()),
                    ..JobStateUpdate::new(ROOT_ID, state, JobState::Failed)
                }]
            );
            assert_eq!(plan.publications, []);
        }
    }

    #[test]
    fn a_failure_message_is_cut_to_the_maximum_number_of_characters() {
        for (message_chars, stored_chars) in [
            (MAX_ERROR_MESSAGE_CHARS + 1, MAX_ERROR_MESSAGE_CHARS),
            (MAX_ERROR_MESSAGE_CHARS, MAX_ERROR_MESSAGE_CHARS),
        ] {
            let plan = plan_root_event(
                PLAIN_NODE,
                JobState::Running,
                failure(Lane::Regular, &"é".repeat(message_chars)),
                PlannerInput::Empty,
            );
            let stored_message = plan
                .updates
                .first()
                .and_then(|update| update.error_message.as_deref());
            assert_eq!(
                stored_message.map(|message| message.chars().count()),
                Some(stored_chars),
                "a message of {message_chars} characters"
            );
        }
    }

    #[test]
    fn cancel_moves_the_root_to_its_abort_target_and_publishes_a_cleanup() {
        for has_cleanup in [false, true] {
            for state in [JobState::Ready, JobState::Running, JobState::CommitReady] {
                let (to, publications) = if has_cleanup && state != JobState::Ready {
                    (JobState::CleanupReady, vec![Publication::Cleanup(ROOT_ID)])
                } else {
                    (JobState::Cancelled, Vec::new())
                };
                let plan = plan_root_event(
                    TreeNode {
                        has_cleanup,
                        ..PLAIN_NODE
                    },
                    state,
                    TreeEventKind::CancelRoot,
                    PlannerInput::Empty,
                );
                assert_eq!(
                    plan.updates,
                    [JobStateUpdate::new(ROOT_ID, state, to)],
                    "a cancel in {state} with has_cleanup {has_cleanup}"
                );
                assert_eq!(
                    plan.publications, publications,
                    "a cancel in {state} with has_cleanup {has_cleanup}"
                );
            }
        }
    }

    #[test]
    fn submit_routes_cancels_and_failures_to_the_fast_forward_channel() {
        let (control_block, mut receivers) = TreeControlBlock::new(ShutdownHandle::new());
        for kind in [
            TreeEventKind::Start,
            TreeEventKind::CancelRoot,
            TreeEventKind::RegularTasksDone,
            failure(Lane::Commit, "boom"),
            TreeEventKind::CommitDone,
            TreeEventKind::CleanupDone,
        ] {
            drop(control_block.submit(root_event(kind)));
        }

        assert_eq!(
            drain_kinds(&mut receivers.fast_forward),
            [TreeEventKind::CancelRoot, failure(Lane::Commit, "boom")]
        );
        assert_eq!(
            drain_kinds(&mut receivers.regular),
            [
                TreeEventKind::Start,
                TreeEventKind::RegularTasksDone,
                TreeEventKind::CommitDone,
                TreeEventKind::CleanupDone,
            ]
        );
    }

    #[tokio::test]
    async fn a_submitted_event_resolves_to_its_reply_or_to_tree_poisoned_if_dropped_unanswered() {
        let (control_block, mut receivers) = TreeControlBlock::new(ShutdownHandle::new());
        let cancel_reply = control_block.submit(root_event(TreeEventKind::CancelRoot));
        let start_reply = control_block.submit(root_event(TreeEventKind::Start));

        receivers
            .fast_forward
            .try_recv()
            .expect("the cancel should be on the fast-forward channel")
            .reply
            .send(Ok(TreeOutcome::Applied(JobState::Cancelled)))
            .expect("the cancel's reply future should still hold its receiver");
        drop(
            receivers
                .regular
                .try_recv()
                .expect("the start should be on the regular channel"),
        );

        let reply = cancel_reply.await;
        assert!(
            matches!(reply, Ok(TreeOutcome::Applied(JobState::Cancelled))),
            "an answered event should resolve to its reply, got {reply:?}"
        );
        let reply = start_reply.await;
        assert!(
            matches!(
                reply,
                Err(CacheError::Internal(InternalError::TreePoisoned))
            ),
            "an event dropped without a reply should poison its tree, got {reply:?}"
        );
    }

    #[tokio::test]
    async fn a_submit_to_an_exited_committer_is_untracked() {
        let (control_block, receivers) = TreeControlBlock::new(ShutdownHandle::new());
        drop(receivers);

        let reply = control_block.submit(root_event(TreeEventKind::Start)).await;
        assert!(
            matches!(reply, Ok(TreeOutcome::Untracked)),
            "a submit to an exited committer should be untracked, got {reply:?}"
        );
    }

    #[tokio::test]
    async fn a_submit_during_a_shutdown_is_stopping_only_once_the_committer_exits() {
        let shutdown_handle = ShutdownHandle::new();
        let (control_block, mut receivers) = TreeControlBlock::new(shutdown_handle.clone());
        shutdown_handle.shut_down_gracefully();

        let start_reply = control_block.submit(root_event(TreeEventKind::Start));
        receivers
            .regular
            .try_recv()
            .expect("a submit during a shutdown should still reach a live committer")
            .reply
            .send(Ok(TreeOutcome::Applied(JobState::Running)))
            .expect("the start's reply future should still hold its receiver");
        let reply = start_reply.await;
        assert!(
            matches!(reply, Ok(TreeOutcome::Applied(JobState::Running))),
            "a submit queued during a shutdown should resolve to the committer's reply, got \
             {reply:?}"
        );
        drop(receivers);

        let reply = control_block.submit(root_event(TreeEventKind::Start)).await;
        assert!(
            matches!(reply, Err(CacheError::ServiceStopping)),
            "a submit to an exited committer during a shutdown should be stopping, got {reply:?}"
        );
    }
}
