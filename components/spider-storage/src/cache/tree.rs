//! Job trees: event submission, tree structure and the pure planner.
//!
//! Every job of a tree submits its job-level events to the tree's committer through the tree
//! control block. The planner turns an event into one tree transition.

use std::collections::HashMap;

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
use crate::test_hooks::EventKindTag;
use crate::test_hooks::HookContext;
use crate::test_hooks::HookPoint;
use crate::test_hooks::TestHooks;

/// The lane of a task: regular, commit or cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    Regular,
    Commit,
    Cleanup,
}

impl Lane {
    /// # Returns
    ///
    /// Whether a job in `state` runs tasks of this lane.
    #[must_use]
    pub const fn accepts(self, state: JobState) -> bool {
        matches!(
            (self, state),
            (Self::Regular, JobState::Running)
                | (Self::Commit, JobState::CommitReady)
                | (Self::Cleanup, JobState::CleanupReady)
        )
    }

    /// # Returns
    ///
    /// The stale-state error for a report of this lane that finds its job in `state`.
    #[must_use]
    pub const fn stale_error(self, state: JobState) -> StaleStateError {
        if state.is_terminal() {
            return StaleStateError::JobAlreadyTerminated(state);
        }
        match self {
            Self::Regular => StaleStateError::JobNoLongerRunning,
            Self::Commit => StaleStateError::JobNoLongerCommitReady,
            Self::Cleanup => StaleStateError::JobNoLongerCleanupReady,
        }
    }
}

/// The kind of a job-level event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeEventKind {
    /// The root is started.
    Start,

    /// The job's last regular task succeeded.
    RegularTasksDone,

    /// The job's commit task succeeded.
    CommitDone,

    /// The job's cleanup task succeeded.
    CleanupDone,

    /// A task of the job failed with no retry left.
    TerminalFailure { lane: Lane, message: String },

    /// The root is cancelled.
    CancelRoot,
}

impl TreeEventKind {
    /// # Returns
    ///
    /// The stale-state error for an event of this kind whose job is in `state`.
    #[must_use]
    pub const fn stale_error(&self, state: JobState) -> StaleStateError {
        match self {
            Self::Start => StaleStateError::JobAlreadyStarted,
            Self::RegularTasksDone => Lane::Regular.stale_error(state),
            Self::CommitDone => Lane::Commit.stale_error(state),
            Self::CleanupDone => Lane::Cleanup.stale_error(state),
            Self::TerminalFailure { lane, .. } => lane.stale_error(state),
            Self::CancelRoot => match state {
                JobState::CleanupReady => StaleStateError::JobCancellationAlreadyRequested,
                JobState::Cancelled => StaleStateError::JobAlreadyCancelled,
                state => StaleStateError::JobAlreadyTerminated(state),
            },
        }
    }

    /// # Returns
    ///
    /// Whether the event travels on its tree's fast-forward channel: a `CancelRoot` or a
    /// `TerminalFailure`.
    #[must_use]
    pub const fn is_fast_forward(&self) -> bool {
        matches!(self, Self::CancelRoot | Self::TerminalFailure { .. })
    }
}

/// A job-level event of one job of a tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEvent {
    pub job_id: JobId,
    pub kind: TreeEventKind,
}

/// The planned result of an event, and the committer's answer to it.
#[derive(Debug)]
pub enum TreeOutcome {
    /// The event was written and applied. Carries the job's new state.
    Applied(JobState),

    /// The event lost a race, and nothing was written for it.
    Stale(StaleStateError),

    /// The tree no longer tracks the event's job, so the event was not planned: the tree's
    /// committer has exited, or the job became terminal in an earlier tree state transaction.
    Untracked,
}

/// The reply to an event.
pub type TreeReply = Result<TreeOutcome, CacheError>;

/// An event on its way to the tree's committer, with the channel for its reply.
pub struct Pending {
    pub event: TreeEvent,
    pub reply: oneshot::Sender<TreeReply>,
}

/// The receivers of a tree's two event channels, owned by the tree's committer.
pub struct EventReceivers {
    /// The `CancelRoot` and `TerminalFailure` events.
    pub fast_forward: mpsc::UnboundedReceiver<Pending>,

    /// Every other event.
    pub regular: mpsc::UnboundedReceiver<Pending>,
}

/// The handle every job of a tree uses to submit events to the tree's committer.
#[derive(Clone)]
pub struct TreeControlBlock {
    root_id: JobId,
    fast_forward: mpsc::UnboundedSender<Pending>,
    regular: mpsc::UnboundedSender<Pending>,
    shutdown_handle: ShutdownHandle,
    hooks: TestHooks,
}

impl TreeControlBlock {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A pair, containing:
    ///
    /// * The control block of the tree rooted at `root_id`, whose replies are raced against
    ///   `shutdown_handle`'s shutdown.
    /// * The receivers of the tree's events, for the tree's committer.
    #[must_use]
    pub fn new(
        root_id: JobId,
        shutdown_handle: ShutdownHandle,
        hooks: TestHooks,
    ) -> (Self, EventReceivers) {
        let (fast_forward, fast_forward_receiver) = mpsc::unbounded_channel();
        let (regular, regular_receiver) = mpsc::unbounded_channel();
        (
            Self {
                root_id,
                fast_forward,
                regular,
                shutdown_handle,
                hooks,
            },
            EventReceivers {
                fast_forward: fast_forward_receiver,
                regular: regular_receiver,
            },
        )
    }

    #[must_use]
    pub const fn root_id(&self) -> JobId {
        self.root_id
    }

    /// # Returns
    ///
    /// Whether the storage service is shutting down.
    #[must_use]
    pub fn is_stopping(&self) -> bool {
        self.shutdown_handle.is_cancelled()
    }

    #[must_use]
    pub const fn hooks(&self) -> &TestHooks {
        &self.hooks
    }

    /// Sends `event` to the tree's committer without waiting: a `CancelRoot` or a
    /// `TerminalFailure` on the fast-forward channel, any other event on the regular channel.
    ///
    /// If the committer has exited, the event is answered [`TreeOutcome::Untracked`] at once.
    ///
    /// # Returns
    ///
    /// The receiver of the event's reply.
    #[must_use]
    pub fn submit(&self, event: TreeEvent) -> oneshot::Receiver<TreeReply> {
        let (reply, receiver) = oneshot::channel();
        let job_id = event.job_id;
        let kind = EventKindTag::of(&event.kind);
        let sender = if event.kind.is_fast_forward() {
            &self.fast_forward
        } else {
            &self.regular
        };
        match sender.send(Pending { event, reply }) {
            Ok(()) => self.hooks.observe(HookPoint::EventSubmitted, || {
                HookContext::event(self.root_id, job_id, kind)
            }),
            Err(mpsc::error::SendError(pending)) => {
                // The send cannot fail: `receiver` is held until it is returned.
                let _ = pending.reply.send(Ok(TreeOutcome::Untracked));
            }
        }
        receiver
    }

    /// Waits for the reply to an event that [`Self::submit`] sent, raced against the service's
    /// shutdown.
    ///
    /// # Returns
    ///
    /// The committer's reply.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`CacheError::ServiceStopping`] if the service is shutting down.
    /// * [`InternalError::TreePoisoned`] if the committer dropped the event without a reply.
    pub async fn wait_for_reply(&self, receiver: oneshot::Receiver<TreeReply>) -> TreeReply {
        tokio::select! {
            reply = receiver => reply.unwrap_or_else(|_| Err(InternalError::TreePoisoned.into())),
            () = self.shutdown_handle.cancelled() => Err(CacheError::ServiceStopping),
        }
    }
}

/// A non-terminal job of a tree, as the planner sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeNode {
    pub has_commit: bool,
    pub has_cleanup: bool,
}

/// The structure of a tree's non-terminal jobs.
#[derive(Debug, Clone)]
pub struct TreeStructure {
    root_id: JobId,
    nodes: HashMap<JobId, TreeNode>,
}

impl TreeStructure {
    /// # Returns
    ///
    /// The structure of a tree that holds only its root.
    #[must_use]
    pub fn new_root(root_id: JobId, root: TreeNode) -> Self {
        Self {
            root_id,
            nodes: HashMap::from([(root_id, root)]),
        }
    }

    #[must_use]
    pub const fn root_id(&self) -> JobId {
        self.root_id
    }

    #[must_use]
    pub fn node(&self, job_id: JobId) -> Option<&TreeNode> {
        self.nodes.get(&job_id)
    }

    /// # Returns
    ///
    /// The IDs of the tree's jobs whose states the planner reads to plan `event`: the event's job,
    /// if it is in the tree.
    #[must_use]
    pub fn jobs_to_collect(&self, event: &TreeEvent) -> Vec<JobId> {
        if self.nodes.contains_key(&event.job_id) {
            vec![event.job_id]
        } else {
            Vec::new()
        }
    }

    /// Removes a job that has become terminal.
    ///
    /// # Returns
    ///
    /// The removed node, if it was in the tree.
    pub fn remove(&mut self, job_id: JobId) -> Option<TreeNode> {
        self.nodes.remove(&job_id)
    }

    /// # Returns
    ///
    /// Whether every job of the tree is terminal.
    #[must_use]
    pub fn is_quiescent(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// The inputs that the committer collects for an event, besides the job states.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum PlannerInput {
    /// The ready task indices of a `Ready` root.
    Start { ready_task_indices: Vec<TaskIndex> },

    /// The outputs of a `Running` job whose regular tasks are done.
    RegularTasksDone { outputs: Vec<TaskOutput> },

    /// Nothing, because the event needs nothing or its job is no longer in the state that the
    /// event expects.
    #[default]
    Empty,
}

/// Ready work that a tree transition makes available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Publication {
    Ready {
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    },
    Commit(JobId),
    Cleanup(JobId),
}

impl Publication {
    #[must_use]
    pub const fn job_id(&self) -> JobId {
        match self {
            Self::Ready { job_id, .. } | Self::Commit(job_id) | Self::Cleanup(job_id) => *job_id,
        }
    }
}

/// One job's state change, as the cache applies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobTransition {
    pub job_id: JobId,
    pub from: JobState,
    pub to: JobState,
}

/// The plan of one tree state transaction.
#[derive(Debug)]
pub struct Plan {
    /// The guarded row updates to write, at most one per job.
    pub updates: Vec<JobStateUpdate>,

    /// The ready work to publish after the apply.
    pub publications: Vec<Publication>,

    /// The jobs that become terminal.
    pub terminated: Vec<JobId>,

    /// The outcome of the planned event.
    pub outcome: TreeOutcome,

    /// The state changes to apply to the cache, in order.
    pub apply_order: Vec<JobTransition>,
}

impl Plan {
    /// # Returns
    ///
    /// Whether the plan writes nothing.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of a tree state transaction that carries `event_plan` alone.
    fn new(event_plan: EventPlan) -> Self {
        let EventPlan {
            updates,
            publications,
            outcome,
            ..
        } = event_plan;
        let apply_order = updates
            .iter()
            .map(|update| JobTransition {
                job_id: update.job_id,
                from: update.from,
                to: update.to,
            })
            .collect();
        let terminated = updates
            .iter()
            .filter(|update| update.to.is_terminal())
            .map(|update| update.job_id)
            .collect();
        Self {
            updates,
            publications,
            terminated,
            outcome,
            apply_order,
        }
    }
}

/// Plans one event against the tree's structure and the collected job states. An event whose job
/// is no longer in the tree is [`TreeOutcome::Untracked`].
///
/// # Returns
///
/// The plan of the event, which takes the outputs of `input` if it stores them.
///
/// # Panics
///
/// Panics if:
///
/// * The state of the event's job, which is in the tree, was not collected.
/// * A `Ready` root's `Start`, or a `Running` job's `RegularTasksDone`, has no collected input.
/// * A `CancelRoot` does not name the root of the tree.
#[must_use]
pub(crate) fn plan_event(
    structure: &TreeStructure,
    collected: &HashMap<JobId, JobState>,
    event: &TreeEvent,
    input: PlannerInput,
) -> Plan {
    let overlay = Overlay {
        structure,
        collected,
    };
    let job_id = event.job_id;
    if overlay.state(job_id) == NodeState::Absent {
        return Plan::new(EventPlan::new(TreeOutcome::Untracked));
    }
    let event_plan = match &event.kind {
        TreeEventKind::Start => plan_start(&overlay, job_id, &input),
        TreeEventKind::RegularTasksDone => plan_regular_done(&overlay, job_id, &input),
        TreeEventKind::CommitDone => plan_commit_done(&overlay, job_id),
        TreeEventKind::CleanupDone => plan_cleanup_done(&overlay, job_id),
        TreeEventKind::TerminalFailure { lane, message } => {
            plan_failure(&overlay, job_id, *lane, message)
        }
        TreeEventKind::CancelRoot => plan_cancel(&overlay, job_id),
    };
    Plan::new(event_plan.with_input(job_id, input))
}

/// A job's state as the planner reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeState {
    /// The job is a non-terminal job of the tree.
    Present(JobState),

    /// The job is not in the tree: it became terminal in an earlier tree state transaction.
    Absent,
}

/// The plan of one event.
struct EventPlan {
    updates: Vec<JobStateUpdate>,
    publications: Vec<Publication>,

    /// Whether the update of the event's own job stores the outputs of the event's input.
    stores_outputs: bool,

    outcome: TreeOutcome,
}

impl EventPlan {
    /// # Returns
    ///
    /// A plan that writes nothing yet and answers the event with `outcome`.
    const fn new(outcome: TreeOutcome) -> Self {
        Self {
            updates: Vec::new(),
            publications: Vec::new(),
            stores_outputs: false,
            outcome,
        }
    }

    /// # Returns
    ///
    /// A plan that writes nothing and answers the event with `error`.
    const fn stale(error: StaleStateError) -> Self {
        Self::new(TreeOutcome::Stale(error))
    }

    /// Adds the guarded update of `job_id` from `from` to `to`.
    fn update(&mut self, job_id: JobId, from: JobState, to: JobState) {
        self.updates.push(JobStateUpdate::new(job_id, from, to));
    }

    /// Adds the guarded update of `job_id` from `from` to `Failed` with `message`.
    fn fail(&mut self, job_id: JobId, from: JobState, message: String) {
        self.updates.push(JobStateUpdate {
            error_message: Some(message),
            ..JobStateUpdate::new(job_id, from, JobState::Failed)
        });
    }

    /// # Returns
    ///
    /// This plan, with the outputs of `input` moved into the update of the event's job `job_id` if
    /// the plan stores them.
    ///
    /// # Panics
    ///
    /// Panics if the plan stores outputs but has no update of `job_id`.
    fn with_input(mut self, job_id: JobId, input: PlannerInput) -> Self {
        if let PlannerInput::RegularTasksDone { outputs } = input
            && self.stores_outputs
        {
            self.updates
                .iter_mut()
                .find(|update| update.job_id == job_id)
                .expect("a plan that stores outputs should update the event's job")
                .outputs = Some(outputs);
        }
        self
    }
}

/// The tree as the planner reads it: the structure and the collected job states.
struct Overlay<'plan_lifetime> {
    structure: &'plan_lifetime TreeStructure,
    collected: &'plan_lifetime HashMap<JobId, JobState>,
}

impl Overlay<'_> {
    /// # Returns
    ///
    /// The state of `job_id` as collected, or [`NodeState::Absent`] if the job is not in the tree.
    ///
    /// # Panics
    ///
    /// Panics if `job_id` is in the tree but its state was not collected.
    fn state(&self, job_id: JobId) -> NodeState {
        if self.structure.node(job_id).is_none() {
            return NodeState::Absent;
        }
        NodeState::Present(
            *self
                .collected
                .get(&job_id)
                .expect("the state of every job that the planner reads should be collected"),
        )
    }

    /// # Returns
    ///
    /// The state of `job_id`, which is in the tree.
    ///
    /// # Panics
    ///
    /// Panics if `job_id` is not in the tree, or if [`Self::state`] panics.
    fn job_state(&self, job_id: JobId) -> JobState {
        let NodeState::Present(state) = self.state(job_id) else {
            panic!("job {job_id} should be in the tree");
        };
        state
    }

    /// # Returns
    ///
    /// Whether `job_id` is in the tree and in `state`.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::state`] panics.
    fn is(&self, job_id: JobId, state: JobState) -> bool {
        self.state(job_id) == NodeState::Present(state)
    }

    /// # Returns
    ///
    /// The tree node of `job_id`.
    ///
    /// # Panics
    ///
    /// Panics if `job_id` is not in the tree.
    fn node(&self, job_id: JobId) -> &TreeNode {
        self.structure
            .node(job_id)
            .expect("every job that the planner changes should be in the tree")
    }

    /// # Returns
    ///
    /// The state that a cancel moves `job_id` to: `Cancelled` if the job is `Ready` or has no
    /// cleanup task, otherwise `CleanupReady`.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::state`] or [`Self::node`] panics.
    fn abort_target(&self, job_id: JobId) -> JobState {
        if self.is(job_id, JobState::Ready) || !self.node(job_id).has_cleanup {
            JobState::Cancelled
        } else {
            JobState::CleanupReady
        }
    }
}

/// # Returns
///
/// The plan of starting the root: `Ready → Running` with its ready tasks published, or stale if
/// the root is not `Ready`.
///
/// # Panics
///
/// Panics if [`Overlay::job_state`] panics, or if a `Ready` root's `input` carries no ready task
/// indices.
fn plan_start(overlay: &Overlay<'_>, root_id: JobId, input: &PlannerInput) -> EventPlan {
    let state = overlay.job_state(root_id);
    if state != JobState::Ready {
        return EventPlan::stale(TreeEventKind::Start.stale_error(state));
    }
    let PlannerInput::Start { ready_task_indices } = input else {
        panic!("a ready root's start should carry its ready task indices");
    };
    let mut plan = EventPlan::new(TreeOutcome::Applied(JobState::Running));
    plan.update(root_id, JobState::Ready, JobState::Running);
    plan.publications.push(Publication::Ready {
        job_id: root_id,
        task_indices: ready_task_indices.clone(),
    });
    plan
}

/// # Returns
///
/// The plan of the job's regular tasks being done, as [`plan_success`] plans it with the job's
/// outputs, or stale if the job is not `Running`.
///
/// # Panics
///
/// Panics if [`Overlay::job_state`] or [`plan_success`] panics, or if a `Running` job's `input`
/// carries no outputs.
fn plan_regular_done(overlay: &Overlay<'_>, job_id: JobId, input: &PlannerInput) -> EventPlan {
    let state = overlay.job_state(job_id);
    if state != JobState::Running {
        return EventPlan::stale(Lane::Regular.stale_error(state));
    }
    assert!(
        matches!(input, PlannerInput::RegularTasksDone { .. }),
        "a running job's regular-tasks-done event should carry its outputs"
    );
    plan_success(overlay, job_id, true)
}

/// # Returns
///
/// The plan of the job succeeding from its current state: `CommitReady` with the commit task
/// published if the plan stores the job's outputs and the job has a commit task, otherwise
/// `Succeeded`.
///
/// # Panics
///
/// Panics if [`Overlay::job_state`] or [`Overlay::node`] panics.
fn plan_success(overlay: &Overlay<'_>, job_id: JobId, stores_outputs: bool) -> EventPlan {
    let from = overlay.job_state(job_id);
    let to = if stores_outputs && overlay.node(job_id).has_commit {
        JobState::CommitReady
    } else {
        JobState::Succeeded
    };
    let mut plan = EventPlan::new(TreeOutcome::Applied(to));
    plan.update(job_id, from, to);
    plan.stores_outputs = stores_outputs;
    if to == JobState::CommitReady {
        plan.publications.push(Publication::Commit(job_id));
    }
    plan
}

/// # Returns
///
/// The plan of the job's commit task being done, as [`plan_success`] plans it from
/// `CommitReady`, or stale if the job is not `CommitReady`.
///
/// # Panics
///
/// Panics if [`Overlay::job_state`] or [`plan_success`] panics.
fn plan_commit_done(overlay: &Overlay<'_>, job_id: JobId) -> EventPlan {
    let state = overlay.job_state(job_id);
    if state != JobState::CommitReady {
        return EventPlan::stale(Lane::Commit.stale_error(state));
    }
    plan_success(overlay, job_id, false)
}

/// # Returns
///
/// The plan of the job's cleanup task being done: `CleanupReady → Cancelled`, or stale if the job
/// is not `CleanupReady`.
///
/// # Panics
///
/// Panics if [`Overlay::job_state`] panics.
fn plan_cleanup_done(overlay: &Overlay<'_>, job_id: JobId) -> EventPlan {
    let state = overlay.job_state(job_id);
    if state != JobState::CleanupReady {
        return EventPlan::stale(Lane::Cleanup.stale_error(state));
    }
    let mut plan = EventPlan::new(TreeOutcome::Applied(JobState::Cancelled));
    plan.update(job_id, JobState::CleanupReady, JobState::Cancelled);
    plan
}

/// # Returns
///
/// The plan of a terminal failure on `lane`: the job fails with `message`, cut to
/// [`MAX_ERROR_MESSAGE_CHARS`] characters, or the failure is stale if `lane` does not accept the
/// job's state.
///
/// # Panics
///
/// Panics if [`Overlay::job_state`] panics.
fn plan_failure(overlay: &Overlay<'_>, job_id: JobId, lane: Lane, message: &str) -> EventPlan {
    let from = overlay.job_state(job_id);
    if !lane.accepts(from) {
        return EventPlan::stale(lane.stale_error(from));
    }
    let mut plan = EventPlan::new(TreeOutcome::Applied(JobState::Failed));
    plan.fail(
        job_id,
        from,
        truncate_chars(message, MAX_ERROR_MESSAGE_CHARS).to_owned(),
    );
    plan
}

/// # Returns
///
/// The plan of cancelling the root: the root moves to its [`Overlay::abort_target`], with its
/// cleanup task published if that is `CleanupReady`, or the cancel is stale if the root is
/// `CleanupReady` or terminal.
///
/// # Panics
///
/// Panics if:
///
/// * `root_id` is not the root of the tree.
/// * [`Overlay::job_state`] or [`deactivate`] panics.
fn plan_cancel(overlay: &Overlay<'_>, root_id: JobId) -> EventPlan {
    assert_eq!(
        root_id,
        overlay.structure.root_id(),
        "a cancel must name the root of its tree"
    );
    let state = overlay.job_state(root_id);
    if state == JobState::CleanupReady || state.is_terminal() {
        return EventPlan::stale(TreeEventKind::CancelRoot.stale_error(state));
    }
    let mut plan = EventPlan::new(TreeOutcome::Applied(overlay.abort_target(root_id)));
    deactivate(overlay, &mut plan, root_id);
    plan
}

/// Plans the move of `job_id` to its [`Overlay::abort_target`], with its cleanup task published if
/// that is `CleanupReady`.
///
/// # Panics
///
/// Panics if [`Overlay::job_state`] or [`Overlay::abort_target`] panics.
fn deactivate(overlay: &Overlay<'_>, plan: &mut EventPlan, job_id: JobId) {
    let to = overlay.abort_target(job_id);
    plan.update(job_id, overlay.job_state(job_id), to);
    if to == JobState::CleanupReady {
        plan.publications.push(Publication::Cleanup(job_id));
    }
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
    use std::collections::HashMap;

    use spider_core::job::JobState;
    use spider_core::types::id::JobId;
    use spider_utils::shutdown::ShutdownHandle;

    use super::*;

    const ROOT_ID: JobId = JobId::from(1);

    const ROOT_NODE: TreeNode = TreeNode {
        has_commit: false,
        has_cleanup: false,
    };

    /// A tree of one job under test: its structure and the state the committer would collect.
    struct TestTree {
        structure: TreeStructure,
        states: HashMap<JobId, JobState>,
    }

    impl TestTree {
        fn new(root: TreeNode, root_state: JobState) -> Self {
            Self {
                structure: TreeStructure::new_root(ROOT_ID, root),
                states: HashMap::from([(ROOT_ID, root_state)]),
            }
        }

        /// Plans `event` with `input`, collecting the root's state.
        ///
        /// # Returns
        ///
        /// The plan.
        fn plan(&self, (event, input): (TreeEvent, PlannerInput)) -> Plan {
            plan_event(&self.structure, &self.states, &event, input)
        }
    }

    fn event(kind: TreeEventKind) -> TreeEvent {
        TreeEvent {
            job_id: ROOT_ID,
            kind,
        }
    }

    fn start(ready_task_indices: Vec<TaskIndex>) -> (TreeEvent, PlannerInput) {
        (
            event(TreeEventKind::Start),
            PlannerInput::Start { ready_task_indices },
        )
    }

    fn regular_done(outputs: Vec<TaskOutput>) -> (TreeEvent, PlannerInput) {
        (
            event(TreeEventKind::RegularTasksDone),
            PlannerInput::RegularTasksDone { outputs },
        )
    }

    fn failure(lane: Lane, message: &str) -> (TreeEvent, PlannerInput) {
        (
            event(TreeEventKind::TerminalFailure {
                lane,
                message: message.to_owned(),
            }),
            PlannerInput::Empty,
        )
    }

    fn without_input(kind: TreeEventKind) -> (TreeEvent, PlannerInput) {
        (event(kind), PlannerInput::Empty)
    }

    fn cancel() -> (TreeEvent, PlannerInput) {
        without_input(TreeEventKind::CancelRoot)
    }

    /// # Returns
    ///
    /// The planned transitions as `(job, from, to)`, after checking that the apply order lists
    /// exactly them.
    fn transitions(plan: &Plan) -> Vec<(u64, JobState, JobState)> {
        let transitions: Vec<_> = plan
            .updates
            .iter()
            .map(|update| (update.job_id.get(), update.from, update.to))
            .collect();
        assert_eq!(
            plan.apply_order
                .iter()
                .map(|transition| (transition.job_id.get(), transition.from, transition.to))
                .collect::<Vec<_>>(),
            transitions
        );
        transitions
    }

    /// # Returns
    ///
    /// The outcome of the planned event as text: `Applied(state)`, the stale error, or
    /// `Untracked`.
    fn outcome(plan: &Plan) -> String {
        match &plan.outcome {
            TreeOutcome::Applied(state) => format!("Applied({state})"),
            TreeOutcome::Stale(error) => error.to_string(),
            TreeOutcome::Untracked => "Untracked".to_owned(),
        }
    }

    /// # Returns
    ///
    /// The error message that the plan's update of the root stores, if any.
    fn root_error(plan: &Plan) -> Option<&str> {
        plan.updates
            .iter()
            .find(|update| update.job_id == ROOT_ID)
            .and_then(|update| update.error_message.as_deref())
    }

    /// Asserts that `plan` writes, publishes and terminates nothing, and answers its event with
    /// `expected`.
    fn assert_stale(plan: &Plan, expected: &StaleStateError) {
        assert!(plan.is_empty());
        assert_eq!(transitions(plan), Vec::new());
        assert_eq!(plan.publications, Vec::new());
        assert_eq!(plan.terminated, Vec::new());
        assert_eq!(outcome(plan), expected.to_string());
    }

    #[test]
    fn start_moves_a_ready_root_to_running_and_publishes_its_ready_tasks() {
        let tree = TestTree::new(ROOT_NODE, JobState::Ready);
        let plan = tree.plan(start(vec![0, 2]));
        assert_eq!(
            transitions(&plan),
            vec![(1, JobState::Ready, JobState::Running)]
        );
        assert_eq!(outcome(&plan), "Applied(Running)");
        assert_eq!(
            plan.publications,
            vec![Publication::Ready {
                job_id: ROOT_ID,
                task_indices: vec![0, 2],
            }]
        );
        assert_eq!(plan.terminated, Vec::new());
    }

    #[test]
    fn start_of_a_root_that_is_not_ready_is_stale() {
        for state in [
            JobState::Running,
            JobState::CommitReady,
            JobState::CleanupReady,
            JobState::Cancelled,
        ] {
            let tree = TestTree::new(ROOT_NODE, state);
            let plan = tree.plan(without_input(TreeEventKind::Start));
            assert_stale(&plan, &StaleStateError::JobAlreadyStarted);
        }
    }

    #[test]
    fn regular_tasks_done_moves_to_commit_ready_or_succeeded_with_the_outputs() {
        let outputs = vec![vec![1u8, 2, 3]];
        for (has_commit, to, publications, terminated) in [
            (
                true,
                JobState::CommitReady,
                vec![Publication::Commit(ROOT_ID)],
                Vec::new(),
            ),
            (false, JobState::Succeeded, Vec::new(), vec![ROOT_ID]),
        ] {
            let tree = TestTree::new(
                TreeNode {
                    has_commit,
                    ..ROOT_NODE
                },
                JobState::Running,
            );
            let plan = tree.plan(regular_done(outputs.clone()));
            assert_eq!(transitions(&plan), vec![(1, JobState::Running, to)]);
            assert_eq!(outcome(&plan), format!("Applied({to})"));
            assert_eq!(plan.updates[0].outputs, Some(outputs.clone()));
            assert_eq!(plan.publications, publications);
            assert_eq!(plan.terminated, terminated);
        }
    }

    #[test]
    fn regular_tasks_done_of_a_job_that_is_not_running_is_stale() {
        for (state, expected) in [
            (JobState::CommitReady, StaleStateError::JobNoLongerRunning),
            (JobState::CleanupReady, StaleStateError::JobNoLongerRunning),
            (
                JobState::Cancelled,
                StaleStateError::JobAlreadyTerminated(JobState::Cancelled),
            ),
        ] {
            let tree = TestTree::new(ROOT_NODE, state);
            let plan = tree.plan(without_input(TreeEventKind::RegularTasksDone));
            assert_stale(&plan, &expected);
        }
    }

    #[test]
    fn commit_done_succeeds_a_commit_ready_job_without_outputs() {
        let tree = TestTree::new(
            TreeNode {
                has_commit: true,
                ..ROOT_NODE
            },
            JobState::CommitReady,
        );
        let plan = tree.plan(without_input(TreeEventKind::CommitDone));
        assert_eq!(
            transitions(&plan),
            vec![(1, JobState::CommitReady, JobState::Succeeded)]
        );
        assert_eq!(outcome(&plan), "Applied(Succeeded)");
        assert_eq!(plan.updates[0].outputs, None);
        assert_eq!(plan.publications, Vec::new());
        assert_eq!(plan.terminated, vec![ROOT_ID]);

        for (state, expected) in [
            (JobState::Running, StaleStateError::JobNoLongerCommitReady),
            (
                JobState::CleanupReady,
                StaleStateError::JobNoLongerCommitReady,
            ),
            (
                JobState::Cancelled,
                StaleStateError::JobAlreadyTerminated(JobState::Cancelled),
            ),
        ] {
            let tree = TestTree::new(ROOT_NODE, state);
            let plan = tree.plan(without_input(TreeEventKind::CommitDone));
            assert_stale(&plan, &expected);
        }
    }

    #[test]
    fn cleanup_done_cancels_a_cleanup_ready_job() {
        let tree = TestTree::new(
            TreeNode {
                has_cleanup: true,
                ..ROOT_NODE
            },
            JobState::CleanupReady,
        );
        let plan = tree.plan(without_input(TreeEventKind::CleanupDone));
        assert_eq!(
            transitions(&plan),
            vec![(1, JobState::CleanupReady, JobState::Cancelled)]
        );
        assert_eq!(outcome(&plan), "Applied(Cancelled)");
        assert_eq!(plan.publications, Vec::new());
        assert_eq!(plan.terminated, vec![ROOT_ID]);

        for (state, expected) in [
            (JobState::Running, StaleStateError::JobNoLongerCleanupReady),
            (
                JobState::CommitReady,
                StaleStateError::JobNoLongerCleanupReady,
            ),
            (
                JobState::Failed,
                StaleStateError::JobAlreadyTerminated(JobState::Failed),
            ),
        ] {
            let tree = TestTree::new(ROOT_NODE, state);
            let plan = tree.plan(without_input(TreeEventKind::CleanupDone));
            assert_stale(&plan, &expected);
        }
    }

    #[test]
    fn failure_fails_the_job_in_its_lane() {
        for (lane, state) in [
            (Lane::Regular, JobState::Running),
            (Lane::Commit, JobState::CommitReady),
            (Lane::Cleanup, JobState::CleanupReady),
        ] {
            let tree = TestTree::new(
                TreeNode {
                    has_commit: true,
                    has_cleanup: true,
                },
                state,
            );
            let plan = tree.plan(failure(lane, "boom"));
            assert_eq!(transitions(&plan), vec![(1, state, JobState::Failed)]);
            assert_eq!(outcome(&plan), "Applied(Failed)");
            assert_eq!(root_error(&plan), Some("boom"));
            assert_eq!(plan.updates[0].outputs, None);
            assert_eq!(plan.publications, Vec::new());
            assert_eq!(plan.terminated, vec![ROOT_ID]);
        }
    }

    #[test]
    fn failure_outside_its_lane_is_stale() {
        for (lane, state, expected) in [
            (
                Lane::Regular,
                JobState::CleanupReady,
                StaleStateError::JobNoLongerRunning,
            ),
            (
                Lane::Regular,
                JobState::CommitReady,
                StaleStateError::JobNoLongerRunning,
            ),
            (
                Lane::Commit,
                JobState::Running,
                StaleStateError::JobNoLongerCommitReady,
            ),
            (
                Lane::Commit,
                JobState::CleanupReady,
                StaleStateError::JobNoLongerCommitReady,
            ),
            (
                Lane::Cleanup,
                JobState::Running,
                StaleStateError::JobNoLongerCleanupReady,
            ),
            (
                Lane::Regular,
                JobState::Succeeded,
                StaleStateError::JobAlreadyTerminated(JobState::Succeeded),
            ),
            (
                Lane::Cleanup,
                JobState::Cancelled,
                StaleStateError::JobAlreadyTerminated(JobState::Cancelled),
            ),
        ] {
            let tree = TestTree::new(ROOT_NODE, state);
            let plan = tree.plan(failure(lane, "late"));
            assert_stale(&plan, &expected);
        }
    }

    #[test]
    fn a_failure_message_is_truncated() {
        let tree = TestTree::new(ROOT_NODE, JobState::Running);
        let message = "é".repeat(MAX_ERROR_MESSAGE_CHARS + 10);
        let plan = tree.plan(failure(Lane::Regular, &message));
        assert_eq!(
            root_error(&plan),
            Some("é".repeat(MAX_ERROR_MESSAGE_CHARS).as_str())
        );

        let message = "é".repeat(MAX_ERROR_MESSAGE_CHARS);
        let plan = tree.plan(failure(Lane::Regular, &message));
        assert_eq!(root_error(&plan), Some(message.as_str()));
    }

    #[test]
    fn cancel_moves_each_abortable_state_to_its_abort_target() {
        for has_cleanup in [false, true] {
            for state in [JobState::Ready, JobState::Running, JobState::CommitReady] {
                let tree = TestTree::new(
                    TreeNode {
                        has_commit: true,
                        has_cleanup,
                    },
                    state,
                );
                let plan = tree.plan(cancel());
                let to = if has_cleanup && state != JobState::Ready {
                    JobState::CleanupReady
                } else {
                    JobState::Cancelled
                };
                assert_eq!(transitions(&plan), vec![(1, state, to)], "{state}");
                assert_eq!(outcome(&plan), format!("Applied({to})"));
                if to == JobState::CleanupReady {
                    assert_eq!(plan.publications, vec![Publication::Cleanup(ROOT_ID)]);
                    assert_eq!(plan.terminated, Vec::new());
                } else {
                    assert_eq!(plan.publications, Vec::new());
                    assert_eq!(plan.terminated, vec![ROOT_ID]);
                }
            }
        }
    }

    #[test]
    fn cancel_of_a_cleanup_ready_or_terminal_root_is_stale() {
        for (state, expected) in [
            (
                JobState::CleanupReady,
                StaleStateError::JobCancellationAlreadyRequested,
            ),
            (JobState::Cancelled, StaleStateError::JobAlreadyCancelled),
            (
                JobState::Succeeded,
                StaleStateError::JobAlreadyTerminated(JobState::Succeeded),
            ),
            (
                JobState::Failed,
                StaleStateError::JobAlreadyTerminated(JobState::Failed),
            ),
        ] {
            let tree = TestTree::new(
                TreeNode {
                    has_cleanup: true,
                    ..ROOT_NODE
                },
                state,
            );
            let plan = tree.plan(cancel());
            assert_stale(&plan, &expected);
        }
    }

    #[test]
    fn an_event_of_a_job_no_longer_in_the_tree_is_untracked() {
        let mut tree = TestTree::new(ROOT_NODE, JobState::Running);
        assert_eq!(tree.structure.remove(ROOT_ID), Some(ROOT_NODE));
        tree.states.clear();
        assert!(tree.structure.is_quiescent());
        for (event, input) in [
            start(vec![0]),
            regular_done(vec![vec![1]]),
            without_input(TreeEventKind::CommitDone),
            without_input(TreeEventKind::CleanupDone),
            failure(Lane::Regular, "boom"),
            cancel(),
        ] {
            let plan = tree.plan((event, input));
            assert!(plan.is_empty());
            assert_eq!(outcome(&plan), "Untracked");
            assert_eq!(plan.publications, Vec::new());
            assert_eq!(plan.terminated, Vec::new());
            assert_eq!(plan.apply_order, Vec::new());
        }
    }

    #[test]
    fn jobs_to_collect_is_the_event_s_job_while_it_is_in_the_tree() {
        let mut tree = TestTree::new(ROOT_NODE, JobState::Running);
        let commit_done = event(TreeEventKind::CommitDone);
        assert_eq!(tree.structure.jobs_to_collect(&commit_done), vec![ROOT_ID]);
        assert_eq!(tree.structure.jobs_to_collect(&cancel().0), vec![ROOT_ID]);
        assert!(!tree.structure.is_quiescent());

        tree.structure.remove(ROOT_ID);
        assert_eq!(tree.structure.jobs_to_collect(&commit_done), Vec::new());
    }

    #[tokio::test]
    async fn submit_routes_cancels_and_failures_to_the_fast_forward_channel() -> anyhow::Result<()>
    {
        let (tree, mut receivers) =
            TreeControlBlock::new(ROOT_ID, ShutdownHandle::new(), TestHooks::default());
        let kinds = [
            TreeEventKind::Start,
            TreeEventKind::CancelRoot,
            TreeEventKind::RegularTasksDone,
            TreeEventKind::TerminalFailure {
                lane: Lane::Commit,
                message: "boom".to_owned(),
            },
            TreeEventKind::CommitDone,
            TreeEventKind::CleanupDone,
        ];
        let mut replies = Vec::new();
        for kind in &kinds {
            replies.push(tree.submit(event(kind.clone())));
        }

        let mut fast_forward = Vec::new();
        while let Ok(pending) = receivers.fast_forward.try_recv() {
            fast_forward.push(pending);
        }
        let mut regular = Vec::new();
        while let Ok(pending) = receivers.regular.try_recv() {
            regular.push(pending);
        }
        assert_eq!(
            fast_forward
                .iter()
                .map(|pending| pending.event.kind.clone())
                .collect::<Vec<_>>(),
            vec![kinds[1].clone(), kinds[3].clone()]
        );
        assert_eq!(
            regular
                .iter()
                .map(|pending| pending.event.kind.clone())
                .collect::<Vec<_>>(),
            vec![
                kinds[0].clone(),
                kinds[2].clone(),
                kinds[4].clone(),
                kinds[5].clone()
            ]
        );

        let pending = regular.remove(0);
        pending
            .reply
            .send(Ok(TreeOutcome::Applied(JobState::Running)))
            .expect("the waiter should still hold the reply's receiver");
        let reply = tree.wait_for_reply(replies.remove(0)).await;
        assert!(
            matches!(reply, Ok(TreeOutcome::Applied(JobState::Running))),
            "the committer's reply should be returned, got {reply:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn submit_after_the_committer_exits_is_untracked() {
        let (tree, receivers) =
            TreeControlBlock::new(ROOT_ID, ShutdownHandle::new(), TestHooks::default());
        drop(receivers);
        for kind in [TreeEventKind::Start, TreeEventKind::CancelRoot] {
            let reply = tree.wait_for_reply(tree.submit(event(kind))).await;
            assert!(
                matches!(reply, Ok(TreeOutcome::Untracked)),
                "an event after the committer exited should be untracked, got {reply:?}"
            );
        }
    }

    #[tokio::test]
    async fn wait_for_reply_is_stopping_once_the_service_shuts_down() {
        let shutdown_handle = ShutdownHandle::new();
        let (tree, _receivers) =
            TreeControlBlock::new(ROOT_ID, shutdown_handle.clone(), TestHooks::default());
        let receiver = tree.submit(event(TreeEventKind::Start));
        assert!(!tree.is_stopping());

        shutdown_handle.shut_down_gracefully();
        assert!(tree.is_stopping());
        let reply = tree.wait_for_reply(receiver).await;
        assert!(
            matches!(reply, Err(CacheError::ServiceStopping)),
            "a reply awaited during a shutdown should be stopping, got {reply:?}"
        );
    }

    #[tokio::test]
    async fn wait_for_reply_is_poisoned_when_the_committer_drops_the_event() {
        let (tree, mut receivers) =
            TreeControlBlock::new(ROOT_ID, ShutdownHandle::new(), TestHooks::default());
        let receiver = tree.submit(event(TreeEventKind::CancelRoot));
        let pending = receivers
            .fast_forward
            .try_recv()
            .expect("the cancel should be on the fast-forward channel");
        drop(pending);

        let reply = tree.wait_for_reply(receiver).await;
        assert!(
            matches!(
                reply,
                Err(CacheError::Internal(InternalError::TreePoisoned))
            ),
            "a dropped event should poison its tree, got {reply:?}"
        );
    }
}
