//! Property tests of the planner over random trees.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::env::VarError;

use anyhow::Context;
use anyhow::ensure;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::seq::SliceRandom;
use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::io::TaskOutput;

use crate::cache::error::StaleStateError;
use crate::cache::task::PreparedChild;
use crate::cache::tree::ChildJobUsage;
use crate::cache::tree::Lane;
use crate::cache::tree::Plan;
use crate::cache::tree::Publication;
use crate::cache::tree::RoundOutcome;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::TreeEventKind;
use crate::cache::tree::TreeEventOutcome;
use crate::cache::tree::TreeNode;
use crate::cache::tree::TreeStructure;
use crate::cache::tree::Violation;
use crate::cache::tree::planner::Admission;
use crate::cache::tree::test_utils::ROOT_ID;
use crate::limits::MAX_EVENTS_PER_ROUND;
use crate::limits::MAX_JOB_CHILDREN_SIZE;
use crate::limits::MAX_ROUND_PAYLOAD;
use crate::state::test_utils::create_test_prepared_child;
use crate::state::test_utils::create_test_prepared_child_of_size;

const MAX_MODEL_DEPTH: u32 = 5;
const MAX_FAN_OUT: usize = 4;
const MAX_ROUNDS_PER_TREE: usize = 400;
const LARGE_OUTPUT_SIZE: usize = 6 * 1024 * 1024;
const REGISTRATION_PROBABILITIES: [f64; 3] = [0.3, 0.6, 0.95];
const FAILURE_PROBABILITIES: [f64; 2] = [0.15, 0.01];
const CANCEL_PROBABILITIES: [f64; 2] = [0.02, 0.001];
const BURST_PROBABILITY: f64 = 0.005;
const BASE_SEED: u64 = 0x5eed_0009;
const NON_TERMINAL_STATES: [JobState; 5] = [
    JobState::Ready,
    JobState::Running,
    JobState::BlockingOnChildren,
    JobState::CommitReady,
    JobState::CleanupReady,
];

/// How the model raises the events of its jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Raising {
    /// Every round, each job may raise an event for its state again.
    Repeated,

    /// Each job raises the event of each of its states once, as the real producers do.
    Once,
}

/// Which events arrive in a round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arrivals {
    /// The events that the rows raise, and copies of earlier events.
    All,

    /// Only the events that the rows raise.
    Raised,

    /// No event.
    Closed,
}

/// An event that a job of the model raises, before the committer collects the event's outputs and
/// children.
#[derive(Debug, Clone)]
struct RaisedEvent {
    job_id: JobId,
    kind: RaisedKind,
}

/// The kind of a [`RaisedEvent`]: a [`TreeEventKind`] without the outputs and the children.
#[derive(Debug, Clone)]
enum RaisedKind {
    Start,
    RegularTasksDone { registers: bool },
    CommitDone,
    CleanupDone,
    TerminalFailure { lane: Lane, message: String },
    CancelRoot,
}

impl RaisedKind {
    /// # Returns
    ///
    /// Whether an event of this kind travels on the fast-forward channel.
    const fn is_fast_forward(&self) -> bool {
        match self {
            Self::TerminalFailure { .. } | Self::CancelRoot => true,
            Self::Start | Self::RegularTasksDone { .. } | Self::CommitDone | Self::CleanupDone => {
                false
            }
        }
    }

    /// # Returns
    ///
    /// The label of this kind, for the coverage counters.
    const fn label(&self) -> &'static str {
        match self {
            Self::Start => "Start",
            Self::RegularTasksDone { registers: false } => "RegularTasksDone",
            Self::RegularTasksDone { registers: true } => "RegularTasksDone(registers)",
            Self::CommitDone => "CommitDone",
            Self::CleanupDone => "CleanupDone",
            Self::TerminalFailure {
                lane: Lane::Regular,
                ..
            } => "TerminalFailure(Regular)",
            Self::TerminalFailure {
                lane: Lane::Commit, ..
            } => "TerminalFailure(Commit)",
            Self::TerminalFailure {
                lane: Lane::Cleanup,
                ..
            } => "TerminalFailure(Cleanup)",
            Self::CancelRoot => "CancelRoot",
        }
    }

    /// # Returns
    ///
    /// The lane whose tasks raise an event of this kind, or [`None`] for a start or a cancel.
    const fn lane(&self) -> Option<Lane> {
        match self {
            Self::RegularTasksDone { .. } => Some(Lane::Regular),
            Self::CommitDone => Some(Lane::Commit),
            Self::CleanupDone => Some(Lane::Cleanup),
            Self::TerminalFailure { lane, .. } => Some(*lane),
            Self::Start | Self::CancelRoot => None,
        }
    }

    /// # Returns
    ///
    /// Whether an event of this kind acts on a job in `state`, per the design's event table.
    const fn acts_on(&self, state: JobState) -> bool {
        match (self.lane(), self) {
            (Some(Lane::Regular), _) => matches!(state, JobState::Running),
            (Some(Lane::Commit), _) => matches!(state, JobState::CommitReady),
            (Some(Lane::Cleanup), _) => matches!(state, JobState::CleanupReady),
            (None, Self::Start) => matches!(state, JobState::Ready),
            (None, _) => matches!(
                state,
                JobState::Ready
                    | JobState::Running
                    | JobState::BlockingOnChildren
                    | JobState::CommitReady
            ),
        }
    }

    /// # Returns
    ///
    /// The stale-state error that the caller of an event of this kind sees when the event finds its
    /// job in `state`, which it does not act on, per the design's event table.
    const fn stale_error(&self, state: JobState) -> StaleStateError {
        let Some(lane) = self.lane() else {
            return match (self, state) {
                (Self::Start, _) => StaleStateError::JobAlreadyStarted,
                (_, JobState::CleanupReady) => StaleStateError::JobCancellationAlreadyRequested,
                (_, JobState::Cancelled) => StaleStateError::JobAlreadyCancelled,
                (_, state) => StaleStateError::JobAlreadyTerminated(state),
            };
        };
        match (lane, state) {
            (_, JobState::Succeeded | JobState::Failed | JobState::Cancelled) => {
                StaleStateError::JobAlreadyTerminated(state)
            }
            (Lane::Regular, _) => StaleStateError::JobNoLongerRunning,
            (Lane::Commit, _) => StaleStateError::JobNoLongerCommitReady,
            (Lane::Cleanup, _) => StaleStateError::JobNoLongerCleanupReady,
        }
    }
}

impl From<&TreeEventKind> for RaisedKind {
    fn from(kind: &TreeEventKind) -> Self {
        match kind {
            TreeEventKind::Start { .. } => Self::Start,
            TreeEventKind::RegularTasksDone { children, .. } => Self::RegularTasksDone {
                registers: !children.is_empty(),
            },
            TreeEventKind::CommitDone => Self::CommitDone,
            TreeEventKind::CleanupDone => Self::CleanupDone,
            TreeEventKind::TerminalFailure { lane, message } => Self::TerminalFailure {
                lane: *lane,
                message: message.clone(),
            },
            TreeEventKind::CancelRoot => Self::CancelRoot,
        }
    }
}

/// A job row of the in-memory database model.
#[derive(Debug)]
struct Row {
    parent: Option<JobId>,
    depth: u32,
    state: JobState,
    has_commit: bool,
    has_cleanup: bool,
    outputs: Option<Vec<TaskOutput>>,
    error: Option<String>,
}

impl Row {
    /// # Returns
    ///
    /// The tree node of a new job of this row, before it counts any child.
    const fn node(&self) -> TreeNode {
        TreeNode {
            state: self.state,
            parent: self.parent,
            has_commit: self.has_commit,
            has_cleanup: self.has_cleanup,
            num_active_children: 0,
        }
    }

    /// # Returns
    ///
    /// The state that an abort of the tree moves a job of this row to, per the design's event
    /// table: [`JobState::CleanupReady`] if the job has a cleanup task and is not
    /// [`JobState::Ready`], otherwise [`JobState::Cancelled`].
    fn abort_target(&self) -> JobState {
        if self.has_cleanup && self.state != JobState::Ready {
            JobState::CleanupReady
        } else {
            JobState::Cancelled
        }
    }
}

/// An in-memory model of one tree's rows, plus the structure that the committer keeps for it.
struct Model {
    rows: BTreeMap<u64, Row>,
    structure: TreeStructure,
    next_id: u64,
    registration_probability: f64,
    failure_probability: f64,
    cancel_probability: f64,
}

impl Model {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The model of a tree that holds only its ready root, with probabilities drawn from `rng`.
    ///
    /// # Panics
    ///
    /// Panics if a probability list is empty.
    fn new(rng: &mut StdRng) -> Self {
        let root = Row {
            parent: None,
            depth: 0,
            state: JobState::Ready,
            has_commit: rng.random_bool(0.5),
            has_cleanup: rng.random_bool(0.5),
            outputs: None,
            error: None,
        };
        let structure = TreeStructure::new_root(ROOT_ID, root.node());
        Self {
            rows: BTreeMap::from([(ROOT_ID.get(), root)]),
            structure,
            next_id: ROOT_ID.get() + 1,
            registration_probability: *REGISTRATION_PROBABILITIES
                .choose(rng)
                .expect("there should be probabilities"),
            failure_probability: *FAILURE_PROBABILITIES
                .choose(rng)
                .expect("there should be probabilities"),
            cancel_probability: *CANCEL_PROBABILITIES
                .choose(rng)
                .expect("there should be probabilities"),
        }
    }

    /// # Returns
    ///
    /// The row of `job_id`.
    ///
    /// # Panics
    ///
    /// Panics if the model has no row of `job_id`.
    fn row(&self, job_id: JobId) -> &Row {
        self.rows
            .get(&job_id.get())
            .expect("the model should have the row")
    }

    /// # Returns
    ///
    /// The ancestors of `job_id` in the rows, nearest first.
    fn ancestors(&self, job_id: JobId) -> Vec<JobId> {
        std::iter::successors(self.row(job_id).parent, |ancestor_id| {
            self.row(*ancestor_id).parent
        })
        .collect()
    }

    /// # Returns
    ///
    /// The events that the current rows raise this round, each sometimes followed by a failure of
    /// another task instance of its job and lane, and sometimes a cancel of the root. With
    /// [`Raising::Once`], a job raises nothing while it is in the state that `raised` records for
    /// it, and `raised` records the state of each job that raises. Job states never repeat, so each
    /// state raises once.
    fn raise_events(
        &self,
        raising: Raising,
        raised: &mut HashMap<u64, JobState>,
        rng: &mut StdRng,
    ) -> Vec<RaisedEvent> {
        let mut events = Vec::new();
        for (id, row) in &self.rows {
            if row.state.is_terminal()
                || row.state == JobState::BlockingOnChildren
                || !rng.random_bool(0.6)
                || (raising == Raising::Once && raised.insert(*id, row.state) == Some(row.state))
            {
                continue;
            }
            let fails = rng.random_bool(self.failure_probability);
            let (kind, lane) = match row.state {
                JobState::Ready => (RaisedKind::Start, None),
                JobState::Running if !fails => (
                    RaisedKind::RegularTasksDone {
                        registers: row.depth + 1 < MAX_MODEL_DEPTH
                            && rng.random_bool(self.registration_probability),
                    },
                    Some(Lane::Regular),
                ),
                JobState::CommitReady if !fails => (RaisedKind::CommitDone, Some(Lane::Commit)),
                JobState::CleanupReady if !fails => (RaisedKind::CleanupDone, Some(Lane::Cleanup)),
                JobState::Running => (
                    RaisedKind::TerminalFailure {
                        lane: Lane::Regular,
                        message: format!("job {id} failed"),
                    },
                    Some(Lane::Regular),
                ),
                JobState::CommitReady => (
                    RaisedKind::TerminalFailure {
                        lane: Lane::Commit,
                        message: format!("commit of {id} failed"),
                    },
                    Some(Lane::Commit),
                ),
                JobState::CleanupReady => (
                    RaisedKind::TerminalFailure {
                        lane: Lane::Cleanup,
                        message: format!("cleanup of {id} failed"),
                    },
                    Some(Lane::Cleanup),
                ),
                JobState::BlockingOnChildren
                | JobState::Succeeded
                | JobState::Failed
                | JobState::Cancelled => continue,
            };
            let job_id = JobId::from(*id);
            events.push(RaisedEvent { job_id, kind });
            if let Some(lane) = lane
                && rng.random_bool(self.failure_probability)
            {
                events.push(RaisedEvent {
                    job_id,
                    kind: RaisedKind::TerminalFailure {
                        lane,
                        message: format!("another task instance of job {id} failed"),
                    },
                });
            }
        }
        if rng.random_bool(self.cancel_probability) {
            events.push(RaisedEvent {
                job_id: ROOT_ID,
                kind: RaisedKind::CancelRoot,
            });
        }
        events
    }

    /// # Returns
    ///
    /// The tree event that the committer receives for `raised`: a regular-tasks-done event of a
    /// running job carries one output and, if the event registers, children from
    /// [`Self::generate_children`]; any other regular-tasks-done event carries neither.
    ///
    /// # Panics
    ///
    /// Panics if the job's ID modulo 256 does not fit in a byte.
    async fn collect_event(&self, raised: &RaisedEvent, rng: &mut StdRng) -> TreeEvent {
        let kind = match &raised.kind {
            RaisedKind::Start => TreeEventKind::Start {
                ready_task_indices: vec![0],
            },
            RaisedKind::RegularTasksDone { registers }
                if self
                    .structure
                    .node(raised.job_id)
                    .is_some_and(|node| node.state == JobState::Running) =>
            {
                let byte = u8::try_from(raised.job_id.get() % 256).expect("the byte should fit");
                let children = if *registers {
                    Self::generate_children(byte, rng).await
                } else {
                    Vec::new()
                };
                let output_size = if rng.random_bool(0.05) {
                    LARGE_OUTPUT_SIZE
                } else {
                    1
                };
                TreeEventKind::RegularTasksDone {
                    outputs: vec![vec![byte; output_size]],
                    children,
                }
            }
            RaisedKind::RegularTasksDone { .. } => TreeEventKind::RegularTasksDone {
                outputs: Vec::new(),
                children: Vec::new(),
            },
            RaisedKind::CommitDone => TreeEventKind::CommitDone,
            RaisedKind::CleanupDone => TreeEventKind::CleanupDone,
            RaisedKind::TerminalFailure { lane, message } => TreeEventKind::TerminalFailure {
                lane: *lane,
                message: message.clone(),
            },
            RaisedKind::CancelRoot => TreeEventKind::CancelRoot,
        };
        TreeEvent {
            job_id: raised.job_id,
            kind,
        }
    }

    /// # Returns
    ///
    /// A random child set of up to [`MAX_FAN_OUT`] children within the limits of one job, as the
    /// service reserves them: in 5% of the sets exactly [`MAX_JOB_CHILDREN_SIZE`] bytes, in 10%
    /// between half of it and all of it, and otherwise small children.
    ///
    /// # Panics
    ///
    /// Panics if the number of children doesn't fit in a `u64`.
    async fn generate_children(input_byte: u8, rng: &mut StdRng) -> Vec<PreparedChild> {
        let roll: f64 = rng.random();
        let num_children = rng.random_range(1..=MAX_FAN_OUT);
        let size = if roll < 0.05 {
            MAX_JOB_CHILDREN_SIZE
        } else if roll < 0.15 {
            rng.random_range(MAX_JOB_CHILDREN_SIZE / 2..MAX_JOB_CHILDREN_SIZE)
        } else {
            let mut children = Vec::with_capacity(num_children);
            for _ in 0..num_children {
                children.push(create_test_prepared_child(input_byte).await);
            }
            return children;
        };
        let num_children_u64 = u64::try_from(num_children).expect("the count should fit a u64");
        let mut children = Vec::with_capacity(num_children);
        for child_index in 0..num_children_u64 {
            let child_size =
                size / num_children_u64 + u64::from(child_index == 0) * (size % num_children_u64);
            children.push(create_test_prepared_child_of_size(input_byte, child_size).await);
        }
        children
    }

    /// Writes `plan`, the plan of a round of `events`, into the rows and the structure as the
    /// committer applies a written round:
    ///
    /// 1. Writes each update into its row, checking the update's guard and edge.
    /// 2. Sets the numbers of active children in the structure.
    /// 3. Applies the state updates to the structure.
    /// 4. Inserts the registered child sets in the round's order. Each child gets a new ID and a
    ///    random commit and cleanup task.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if the plan updates a job twice, misses a row's guard or takes an
    ///   invalid edge.
    /// * Forwards [`TreeStructure::apply_num_active_children`]'s return values on failure.
    /// * Forwards [`TreeStructure::insert_children`]'s return values on failure.
    fn apply(&mut self, events: &[TreeEvent], plan: &Plan, rng: &mut StdRng) -> anyhow::Result<()> {
        let mut updated_job_ids = HashSet::new();
        for update in &plan.updates {
            ensure!(
                updated_job_ids.insert(update.job_id),
                "the job {:?} is updated twice in one round",
                update.job_id
            );
            let row = self
                .rows
                .get_mut(&update.job_id.get())
                .with_context(|| format!("the updated job {:?} has no row", update.job_id))?;
            ensure!(
                row.state == update.from,
                "the guard of the job {:?} expects {}, but the row is {}",
                update.job_id,
                update.from,
                row.state
            );
            ensure!(
                JobState::is_valid_transition(update.from, update.to),
                "the job {:?} takes the invalid edge {} -> {}",
                update.job_id,
                update.from,
                update.to
            );
            row.state = update.to;
            if update.outputs.is_some() {
                row.outputs.clone_from(&update.outputs);
            }
            if update.error_message.is_some() {
                row.error.clone_from(&update.error_message);
            }
        }
        for (job_id, num_active_children) in &plan.num_active_children {
            self.structure
                .apply_num_active_children(*job_id, *num_active_children)?;
        }
        for update in &plan.updates {
            self.structure.apply_state(update.job_id, update.to);
        }
        for (parent_id, children) in registered_child_sets(events, &plan.outcomes) {
            let depth = self.row(parent_id).depth + 1;
            let mut nodes = Vec::with_capacity(children.count);
            for _ in 0..children.count {
                let row = Row {
                    parent: Some(parent_id),
                    depth,
                    state: JobState::Running,
                    has_commit: rng.random_bool(0.4),
                    has_cleanup: rng.random_bool(0.4),
                    outputs: None,
                    error: None,
                };
                nodes.push((JobId::from(self.next_id), row.node()));
                self.rows.insert(self.next_id, row);
                self.next_id += 1;
            }
            self.structure.insert_children(parent_id, nodes)?;
        }
        Ok(())
    }

    /// Checks every invariant of the tree on the rows:
    ///
    /// * I1 to I3 (see [`Self::check_links`] and [`Self::check_children`]).
    /// * I4: a closed tree, whose root is cleanup-ready or terminal, has no ready, running,
    ///   blocking or commit-ready job.
    /// * I5: the structure holds exactly the non-terminal rows, in their states.
    /// * Every commit-ready or succeeded job has only succeeded children.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] naming the first broken invariant found.
    /// * Forwards [`Self::check_links`]'s return values on failure.
    /// * Forwards [`Self::check_children`]'s return values on failure.
    fn check_rows(&self) -> anyhow::Result<()> {
        let mut children: HashMap<u64, Vec<JobState>> = HashMap::new();
        for row in self.rows.values() {
            if let Some(parent_id) = row.parent {
                children.entry(parent_id.get()).or_default().push(row.state);
            }
        }
        let root = self.row(ROOT_ID);
        let is_tree_closed = root.state == JobState::CleanupReady || root.state.is_terminal();
        for (id, row) in &self.rows {
            let job_id = JobId::from(*id);
            self.check_links(job_id, row)?;
            self.check_children(job_id, row, children.get(id).map_or(&[][..], Vec::as_slice))?;
            if is_tree_closed {
                ensure!(
                    !matches!(
                        row.state,
                        JobState::Ready
                            | JobState::Running
                            | JobState::BlockingOnChildren
                            | JobState::CommitReady
                    ),
                    "I4: the job {id} is {} in a closed tree",
                    row.state
                );
            }
            let node = self.structure.node(job_id);
            ensure!(
                node.is_some() != row.state.is_terminal(),
                "I5: the {} job {id} is in the structure: {}",
                row.state,
                node.is_some()
            );
            if let Some(node) = node {
                ensure!(
                    (node.state, node.parent, node.has_commit, node.has_cleanup)
                        == (row.state, row.parent, row.has_commit, row.has_cleanup),
                    "I5: the node of job {id} is {node:?}, its row {row:?}"
                );
            }
        }
        Ok(())
    }

    /// Checks I1 and I3 on the `row` of `job_id`:
    ///
    /// * Only the root has no parent.
    /// * Only the root is ready.
    /// * A job's ID is above its parent's.
    /// * Every ancestor of a running, blocking or commit-ready job blocks on its children.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] naming the first broken invariant found.
    fn check_links(&self, job_id: JobId, row: &Row) -> anyhow::Result<()> {
        ensure!(
            (job_id == ROOT_ID) == row.parent.is_none(),
            "I1: the parent link of job {job_id:?}"
        );
        ensure!(
            row.state != JobState::Ready || job_id == ROOT_ID,
            "I1: the non-root job {job_id:?} is ready"
        );
        if let Some(parent_id) = row.parent {
            ensure!(
                parent_id.get() < job_id.get(),
                "I1: the ID order of job {job_id:?}"
            );
        }
        if matches!(
            row.state,
            JobState::Running | JobState::BlockingOnChildren | JobState::CommitReady
        ) {
            for ancestor_id in self.ancestors(job_id) {
                let ancestor_state = self.row(ancestor_id).state;
                ensure!(
                    ancestor_state == JobState::BlockingOnChildren,
                    "I3: the ancestor {ancestor_id:?} of the {} job {job_id:?} is {ancestor_state}",
                    row.state
                );
            }
        }
        Ok(())
    }

    /// Checks I2 and the earned successes on the `row` of `job_id`, whose children are in
    /// `child_states`:
    ///
    /// * A blocking job's children are active or succeeded, and at least one is active.
    /// * The structure counts exactly a blocking job's active children.
    /// * A commit-ready or succeeded job's children all succeeded.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] naming the first broken invariant found.
    fn check_children(
        &self,
        job_id: JobId,
        row: &Row,
        child_states: &[JobState],
    ) -> anyhow::Result<()> {
        let is_active = |state: &JobState| {
            matches!(
                state,
                JobState::Running | JobState::BlockingOnChildren | JobState::CommitReady
            )
        };
        if row.state == JobState::BlockingOnChildren {
            ensure!(
                child_states
                    .iter()
                    .all(|state| *state == JobState::Succeeded || is_active(state)),
                "I2: the children of the blocking job {job_id:?} are {child_states:?}"
            );
            let num_active = child_states.iter().filter(|state| is_active(state)).count();
            ensure!(
                num_active > 0,
                "I2: the blocking job {job_id:?} has no active child"
            );
            let num_counted = self
                .structure
                .node(job_id)
                .map(|node| node.num_active_children);
            ensure!(
                num_counted == Some(num_active),
                "I2: the job {job_id:?} counts {num_counted:?} active children, not {num_active}"
            );
        }
        if matches!(row.state, JobState::CommitReady | JobState::Succeeded) {
            ensure!(
                child_states
                    .iter()
                    .all(|state| *state == JobState::Succeeded),
                "the success of job {job_id:?} is not earned: its children are {child_states:?}"
            );
        }
        Ok(())
    }

    /// Checks the tree:
    ///
    /// 1. Checks every invariant on the rows (see [`Self::check_rows`]).
    /// 2. Checks that [`TreeStructure::check_invariants`] agrees.
    /// 3. Perturbs one job's state or number of active children in the structure.
    /// 4. Checks that [`TreeStructure::check_invariants`] reports exactly the violation that
    ///    [`Self::expected_violation`] expects.
    /// 5. Undoes the perturbation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if the checker reports a violation of a consistent tree, or another
    ///   violation than the expected one of a perturbed tree.
    /// * Forwards [`Self::check_rows`]'s return values on failure.
    /// * Forwards [`TreeStructure::apply_num_active_children`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if [`NON_TERMINAL_STATES`] is empty.
    fn check(&mut self, rng: &mut StdRng, coverage: &mut Coverage) -> anyhow::Result<()> {
        self.check_rows()?;
        self.structure
            .check_invariants()
            .context("check_invariants should accept a consistent tree")?;

        let job_ids: Vec<JobId> = self.structure.nodes().map(|(job_id, _)| job_id).collect();
        let Some(job_id) = job_ids.choose(rng).copied() else {
            return Ok(());
        };
        let node = self
            .structure
            .node(job_id)
            .context("a listed job should be in the structure")?;
        let (state, num_active_children) = (node.state, node.num_active_children);
        if rng.random_bool(0.5) {
            let perturbed_state = *NON_TERMINAL_STATES
                .choose(rng)
                .expect("there should be states");
            self.structure.apply_state(job_id, perturbed_state);
        } else {
            self.structure
                .apply_num_active_children(job_id, rng.random_range(0..=MAX_FAN_OUT + 1))?;
        }
        let expected = self.expected_violation();
        let reported = self.structure.check_invariants().err();
        self.structure.apply_state(job_id, state);
        self.structure
            .apply_num_active_children(job_id, num_active_children)?;
        ensure!(
            reported == expected,
            "check_invariants reports {reported:?} for a perturbed job {job_id:?}, the model \
             expects {expected:?}"
        );
        if expected.is_some() {
            coverage.broken_perturbations += 1;
        } else {
            coverage.harmless_perturbations += 1;
        }
        Ok(())
    }

    /// # Returns
    ///
    /// The first violation, in ascending job ID order, of the invariants that
    /// [`TreeStructure::check_invariants`] checks and that a perturbed state or number of active
    /// children can break, or [`None`] if the structure keeps them.
    fn expected_violation(&self) -> Option<Violation> {
        let is_root_inactive = self
            .structure
            .node(ROOT_ID)
            .is_none_or(|root| root.state == JobState::CleanupReady);
        self.structure.nodes().find_map(|(job_id, node)| {
            if job_id != ROOT_ID && node.state == JobState::Ready {
                return Some(Violation::ReadyNonRoot(job_id));
            }
            if node.state == JobState::BlockingOnChildren && node.num_active_children == 0 {
                return Some(Violation::BlockingJobWithoutActiveChild(job_id));
            }
            (is_root_inactive
                && matches!(
                    node.state,
                    JobState::Running | JobState::BlockingOnChildren | JobState::CommitReady
                ))
            .then_some(Violation::AbortableJobOfInactiveRoot(job_id))
        })
    }

    /// # Returns
    ///
    /// The failure that `event`, a failure applied to its job, plans on success: the job fails with
    /// the event's message, and so does every ancestor if the job's parent was found in
    /// `parent_state`, [`JobState::BlockingOnChildren`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if `event` is not a [`TreeEventKind::TerminalFailure`].
    fn expected_failure(
        &self,
        event: &TreeEvent,
        parent_state: Option<JobState>,
    ) -> anyhow::Result<ExpectedFailure> {
        let TreeEventKind::TerminalFailure { message, .. } = &event.kind else {
            anyhow::bail!("a {:?} event cannot fail its job", event.kind);
        };
        let propagates = parent_state == Some(JobState::BlockingOnChildren);
        Ok(ExpectedFailure {
            job_id: event.job_id,
            message: message.clone(),
            chain: if propagates {
                self.ancestors(event.job_id)
            } else {
                Vec::new()
            },
        })
    }

    /// # Returns
    ///
    /// The state that `event`, which acts on its job's `state`, moves the job to, per the design's
    /// event table.
    fn expected_applied_state(&self, event: &TreeEvent, state: JobState) -> JobState {
        let row = self.row(event.job_id);
        match &event.kind {
            TreeEventKind::Start { .. } => JobState::Running,
            TreeEventKind::RegularTasksDone { children, .. } => {
                if !children.is_empty() {
                    JobState::BlockingOnChildren
                } else if row.has_commit {
                    JobState::CommitReady
                } else {
                    JobState::Succeeded
                }
            }
            TreeEventKind::CommitDone => JobState::Succeeded,
            TreeEventKind::CleanupDone => JobState::Cancelled,
            TreeEventKind::TerminalFailure { .. } => JobState::Failed,
            TreeEventKind::CancelRoot if state == JobState::Ready || !row.has_cleanup => {
                JobState::Cancelled
            }
            TreeEventKind::CancelRoot => JobState::CleanupReady,
        }
    }
}

/// How often the random trees reached the cases the planner must handle.
#[derive(Debug, Default)]
struct Coverage {
    max_rows: usize,
    grouped_rounds: usize,
    deferred_events: usize,
    registrations: usize,
    multi_registration_rounds: usize,
    absent_events: usize,
    applied_cancels: usize,
    propagated_failures: usize,
    commit_ready_jobs_aborted_by_cancels: usize,
    commit_ready_jobs_aborted_by_failures: usize,
    unblocked_parents: usize,
    crowded_cancels: usize,
    budget_deferrals: usize,
    budget_deferring_registrations: usize,
    fast_forwarded_rounds: usize,
    full_rounds: usize,
    broken_perturbations: usize,
    harmless_perturbations: usize,
    unfinished_trees: usize,
    backlogged_trees: usize,
    outcomes: BTreeMap<(&'static str, &'static str), usize>,
}

impl Coverage {
    /// Counts what the round of `events`, raised as `raised`, covered with `plan`, before the plan
    /// is applied to `model`.
    fn count(&mut self, model: &Model, events: &[TreeEvent], raised: &[RaisedEvent], plan: &Plan) {
        self.max_rows = self.max_rows.max(model.rows.len());
        let planned: Vec<(usize, &TreeEventOutcome)> = plan
            .outcomes
            .iter()
            .enumerate()
            .filter_map(|(index, outcome)| match outcome {
                RoundOutcome::Planned { outcome, .. } => Some((index, outcome)),
                RoundOutcome::Deferred => None,
            })
            .collect();
        self.grouped_rounds += usize::from(planned.len() > 1);
        self.deferred_events += events.len() - planned.len();
        let num_registrations = registered_child_sets(events, &plan.outcomes).len();
        self.registrations += num_registrations;
        self.multi_registration_rounds += usize::from(num_registrations > 1);
        self.full_rounds += usize::from(events.len() == MAX_EVENTS_PER_ROUND);
        let mut has_applied_cancel = false;
        for (index, outcome) in &planned {
            let kind = &raised[*index].kind;
            let outcome_label = match outcome {
                TreeEventOutcome::Applied(_) => "Applied",
                TreeEventOutcome::Stale(_) => "Stale",
                TreeEventOutcome::Untracked => "Untracked",
            };
            *self
                .outcomes
                .entry((kind.label(), outcome_label))
                .or_default() += 1;
            match (outcome, kind) {
                (TreeEventOutcome::Untracked, _) => self.absent_events += 1,
                (TreeEventOutcome::Applied(_), RaisedKind::CancelRoot) => {
                    has_applied_cancel = true;
                    self.applied_cancels += 1;
                    self.crowded_cancels += usize::from(planned.len() > 1);
                }
                (
                    TreeEventOutcome::Applied(_),
                    RaisedKind::Start
                    | RaisedKind::RegularTasksDone { .. }
                    | RaisedKind::CommitDone
                    | RaisedKind::CleanupDone
                    | RaisedKind::TerminalFailure { .. },
                )
                | (TreeEventOutcome::Stale(_), _) => {}
            }
        }
        let mut num_aborted_commit_ready_jobs = 0;
        for update in &plan.updates {
            let message = update.error_message.as_deref().unwrap_or_default();
            self.propagated_failures += usize::from(message.starts_with("descendant job"));
            self.unblocked_parents += usize::from(
                update.from == JobState::BlockingOnChildren
                    && matches!(update.to, JobState::CommitReady | JobState::Succeeded),
            );
            num_aborted_commit_ready_jobs += usize::from(
                update.from == JobState::CommitReady
                    && matches!(update.to, JobState::CleanupReady | JobState::Cancelled),
            );
        }
        if has_applied_cancel {
            self.commit_ready_jobs_aborted_by_cancels += num_aborted_commit_ready_jobs;
        } else {
            self.commit_ready_jobs_aborted_by_failures += num_aborted_commit_ready_jobs;
        }
    }

    /// Counts an event deferred for `admission` after the round's earlier planned events,
    /// `earlier_events`, whose plan is `earlier_plan`.
    fn count_deferral(
        &mut self,
        admission: &Admission,
        earlier_events: &[&TreeEvent],
        earlier_plan: &Plan,
    ) {
        match admission {
            Admission::PayloadBudgetExceeded => {
                self.budget_deferrals += 1;
                self.budget_deferring_registrations += usize::from(
                    registered_child_sets(earlier_events.iter().copied(), &earlier_plan.outcomes)
                        .iter()
                        .any(|(_, children)| children.size > MAX_ROUND_PAYLOAD / 2),
                );
            }
            Admission::Admitted | Admission::JobConflict(_) => {}
        }
    }

    /// Checks that each of `cases`, a label and its count, was reached.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] naming the first case that was never reached.
    fn ensure_reached(cases: &[(&str, usize)]) -> anyhow::Result<()> {
        for (case, count) in cases {
            ensure!(*count > 0, "the random trees never reached {case}");
        }
        Ok(())
    }

    /// Checks that every event kind reached each of `outcome_labels`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] naming the first event kind that never reached one of `outcome_labels`.
    fn ensure_outcomes_reached(&self, outcome_labels: &[&'static str]) -> anyhow::Result<()> {
        for kind in [
            "Start",
            "RegularTasksDone",
            "RegularTasksDone(registers)",
            "CommitDone",
            "CleanupDone",
            "TerminalFailure(Regular)",
            "TerminalFailure(Commit)",
            "TerminalFailure(Cleanup)",
            "CancelRoot",
        ] {
            for outcome in outcome_labels {
                ensure!(
                    self.outcomes
                        .get(&(kind, *outcome))
                        .copied()
                        .unwrap_or_default()
                        > 0,
                    "no {kind} event was {outcome}"
                );
            }
        }
        Ok(())
    }
}

/// A failure that an applied event planned, to check once the plan is applied.
struct ExpectedFailure {
    job_id: JobId,
    message: String,

    /// The ancestors that fail with the job: all of them if the event found its parent
    /// `BlockingOnChildren`, else none.
    chain: Vec<JobId>,
}

impl ExpectedFailure {
    /// Checks the failed job's error and its failed chain in the rows of `model`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if a row of the failure is not [`JobState::Failed`] with the expected
    ///   error.
    fn check(&self, model: &Model) -> anyhow::Result<()> {
        let row = model.row(self.job_id);
        ensure!(
            (row.state, row.error.as_deref()) == (JobState::Failed, Some(self.message.as_str())),
            "the failed job {:?} is {} with {:?}",
            self.job_id,
            row.state,
            row.error
        );
        let chain_message = format!("descendant job {} failed: {}", self.job_id, self.message);
        for ancestor_id in &self.chain {
            let ancestor = model.row(*ancestor_id);
            ensure!(
                (ancestor.state, ancestor.error.as_deref())
                    == (JobState::Failed, Some(chain_message.as_str())),
                "the ancestor {ancestor_id:?} of the failed job {:?} is {} with {:?}",
                self.job_id,
                ancestor.state,
                ancestor.error
            );
        }
        Ok(())
    }
}

/// An event in a buffer of the simulated committer, with its tree event once the committer has
/// collected it.
struct Buffered {
    raised: RaisedEvent,
    event: Option<TreeEvent>,
}

/// A simulated committer of one tree: events arrive in its two buffers, cancels and failures in the
/// fast-forward buffer and the others in the regular buffer, and each round plans what the
/// committer takes from them against the model and applies the plan.
struct Simulation {
    model: Model,
    raising: Raising,
    fast_forward: VecDeque<Buffered>,
    regular: VecDeque<Buffered>,
    history: Vec<RaisedEvent>,
    raised: HashMap<u64, JobState>,
}

impl Simulation {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A simulation of `model` whose jobs raise their events as `raising` says, with empty buffers.
    fn new(model: Model, raising: Raising) -> Self {
        Self {
            model,
            raising,
            fast_forward: VecDeque::new(),
            regular: VecDeque::new(),
            history: Vec::new(),
            raised: HashMap::new(),
        }
    }

    /// # Returns
    ///
    /// The number of buffered events.
    fn num_buffered(&self) -> usize {
        self.fast_forward.len() + self.regular.len()
    }

    /// Puts `buffered` into its buffer: at the back if `at_front` is false, else at the front.
    fn buffer(&mut self, buffered: Buffered, at_front: bool) {
        let buffer = if buffered.raised.kind.is_fast_forward() {
            &mut self.fast_forward
        } else {
            &mut self.regular
        };
        if at_front {
            buffer.push_front(buffered);
        } else {
            buffer.push_back(buffered);
        }
    }

    /// Runs rounds until the tree is quiescent or [`MAX_ROUNDS_PER_TREE`] rounds ran.
    ///
    /// # Returns
    ///
    /// A tuple containing, on success:
    ///
    /// * The number of rounds run.
    /// * Whether the tree became quiescent.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::round`]'s return values on failure.
    async fn run(
        &mut self,
        rng: &mut StdRng,
        coverage: &mut Coverage,
    ) -> anyhow::Result<(usize, bool)> {
        for round in 0..MAX_ROUNDS_PER_TREE {
            self.round(rng, coverage, Arrivals::All).await?;
            if self.model.structure.is_quiescent() {
                return Ok((round + 1, true));
            }
        }
        Ok((MAX_ROUNDS_PER_TREE, false))
    }

    /// Runs rounds until the tree is quiescent:
    ///
    /// 1. Runs rounds as [`Self::run`] does.
    /// 2. If the tree is not quiescent yet, stops the copies of earlier events and runs rounds
    ///    until the backlog in the buffers and [`MAX_ROUNDS_PER_TREE`] more rounds are planned.
    ///
    /// Each round plans at least its first event, so with every event raised once, a tree that is
    /// not quiescent by then lost an event.
    ///
    /// # Returns
    ///
    /// The number of rounds run on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if the tree is not quiescent at the end.
    /// * Forwards [`Self::run`]'s return values on failure.
    /// * Forwards [`Self::round`]'s return values on failure.
    async fn run_to_quiescence(
        &mut self,
        rng: &mut StdRng,
        coverage: &mut Coverage,
    ) -> anyhow::Result<usize> {
        let (mut rounds, is_quiescent) = self.run(rng, coverage).await?;
        if is_quiescent {
            return Ok(rounds);
        }
        coverage.backlogged_trees += 1;
        let max_rounds = rounds + self.num_buffered() + MAX_ROUNDS_PER_TREE;
        while !self.model.structure.is_quiescent() {
            ensure!(
                rounds < max_rounds,
                "a tree whose jobs raise each event once should be quiescent once its backlog is \
                 planned"
            );
            self.round(rng, coverage, Arrivals::Raised).await?;
            rounds += 1;
        }
        Ok(rounds)
    }

    /// Plans the events left in the buffers of a quiescent tree, with no new arrivals.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if the tree is not quiescent, or if an event is answered other than
    ///   [`TreeEventOutcome::Untracked`].
    /// * Forwards [`Self::round`]'s return values on failure.
    async fn drain(&mut self, rng: &mut StdRng, coverage: &mut Coverage) -> anyhow::Result<()> {
        ensure!(
            self.model.structure.is_quiescent(),
            "only a quiescent tree should be drained"
        );
        while let Some(plan) = self.round(rng, coverage, Arrivals::Closed).await? {
            ensure!(
                plan.outcomes.iter().all(|outcome| matches!(
                    outcome,
                    RoundOutcome::Planned {
                        outcome: TreeEventOutcome::Untracked,
                        ..
                    }
                )),
                "an event of a quiescent tree is answered {:?}",
                plan.outcomes
            );
        }
        Ok(())
    }

    /// Runs one round:
    ///
    /// 1. `arrivals` arrive.
    /// 2. At most [`MAX_EVENTS_PER_ROUND`] events are taken in the committer's order: every
    ///    buffered cancel, then every buffered failure, then a random prefix of the regular buffer.
    ///    After a burst of more than [`MAX_EVENTS_PER_ROUND`] copies of earlier events, the round
    ///    is full.
    /// 3. The committer collects the events taken for the first time.
    /// 4. The round is planned, checked and applied.
    /// 5. The deferred events go back to the front of their buffers.
    ///
    /// # Returns
    ///
    /// The plan of the round on success, or [`None`] if the buffers were empty.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TreeStructure::plan_events`]'s return values on failure.
    /// * Forwards [`Self::check_plan`]'s return values on failure.
    /// * Forwards [`Self::check_outcomes`]'s return values on failure.
    /// * Forwards [`Self::check_applied`]'s return values on failure.
    /// * Forwards [`Model::apply`]'s return values on failure.
    /// * Forwards [`ExpectedFailure::check`]'s return values on failure.
    /// * Forwards [`Model::check`]'s return values on failure.
    async fn round(
        &mut self,
        rng: &mut StdRng,
        coverage: &mut Coverage,
        arrivals: Arrivals,
    ) -> anyhow::Result<Option<Plan>> {
        let mut bursts = false;
        if arrivals != Arrivals::Closed {
            let mut raised = self.model.raise_events(self.raising, &mut self.raised, rng);
            self.history.extend(raised.iter().cloned());
            bursts = arrivals == Arrivals::All && rng.random_bool(BURST_PROBABILITY);
            let num_copies = match arrivals {
                Arrivals::All if bursts => MAX_EVENTS_PER_ROUND + rng.random_range(1..64),
                Arrivals::All => rng.random_range(0..3),
                Arrivals::Raised | Arrivals::Closed => 0,
            };
            for _ in 0..num_copies {
                if let Some(copy) = self.history.choose(rng) {
                    raised.push(copy.clone());
                }
            }
            raised.shuffle(rng);
            for raised in raised {
                self.buffer(
                    Buffered {
                        raised,
                        event: None,
                    },
                    false,
                );
            }
        }
        if self.num_buffered() == 0 {
            return Ok(None);
        }

        let mut fast_forward = Vec::from(std::mem::take(&mut self.fast_forward));
        fast_forward
            .sort_by_key(|buffered| !matches!(buffered.raised.kind, RaisedKind::CancelRoot));
        let num_fast_forward = fast_forward.len().min(MAX_EVENTS_PER_ROUND);
        let mut taken: Vec<Buffered> = fast_forward.drain(..num_fast_forward).collect();
        self.fast_forward = fast_forward.into();
        let max_regular = (MAX_EVENTS_PER_ROUND - taken.len()).min(self.regular.len());
        let num_regular = if bursts {
            max_regular
        } else {
            rng.random_range(usize::from(taken.is_empty())..=max_regular)
        };
        coverage.fast_forwarded_rounds +=
            usize::from(!taken.is_empty() && num_regular < self.regular.len());
        taken.extend(self.regular.drain(..num_regular));

        let mut raised = Vec::with_capacity(taken.len());
        let mut events = Vec::with_capacity(taken.len());
        for buffered in taken {
            let event = match buffered.event {
                Some(event) => event,
                None => self.model.collect_event(&buffered.raised, rng).await,
            };
            raised.push(buffered.raised);
            events.push(event);
        }
        let plan = self
            .model
            .structure
            .plan_events(&events)
            .context("a round in the committer's order should plan")?;

        self.check_plan(&events, &raised, &plan)?;
        let failures = self.check_outcomes(&events, &plan, coverage)?;
        coverage.count(&self.model, &events, &raised, &plan);
        self.model.apply(&events, &plan, rng)?;
        self.check_applied(&events, &plan)?;
        for failure in &failures {
            failure.check(&self.model)?;
        }
        self.model.check(rng, coverage)?;

        let deferred: Vec<Buffered> = raised
            .into_iter()
            .zip(events)
            .zip(&plan.outcomes)
            .filter(|(_, outcome)| matches!(outcome, RoundOutcome::Deferred))
            .map(|((raised, event), _)| Buffered {
                raised,
                event: Some(event),
            })
            .collect();
        for buffered in deferred.into_iter().rev() {
            self.buffer(buffered, true);
        }
        Ok(Some(plan))
    }

    /// Checks the properties of one planned round of `events`, raised as `raised`, over the model
    /// before the plan is applied:
    ///
    /// * Every event has one outcome.
    /// * The first event, every cancel and every failure are planned.
    /// * The plan writes exactly when an event applies.
    /// * An event is untracked exactly when its job left the tree.
    /// * Every aborted job moves to its abort target.
    /// * The registrations are those of the applied registering events.
    /// * The payload stays within its budget unless one job carries it.
    /// * The publications follow from the updates.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] naming the first property that the round breaks.
    /// * Forwards [`Self::check_aborts`]'s return values on failure.
    /// * Forwards [`check_registrations_and_payload`]'s return values on failure.
    /// * Forwards [`check_publications`]'s return values on failure.
    fn check_plan(
        &self,
        events: &[TreeEvent],
        raised: &[RaisedEvent],
        plan: &Plan,
    ) -> anyhow::Result<()> {
        ensure!(
            plan.outcomes.len() == events.len(),
            "a round of {} events has {} outcomes",
            events.len(),
            plan.outcomes.len()
        );
        ensure!(
            plan.outcomes
                .first()
                .is_none_or(|outcome| matches!(outcome, RoundOutcome::Planned { .. })),
            "the round's first event is deferred"
        );
        let mut num_applied = 0;
        for (index, (outcome, raised)) in plan.outcomes.iter().zip(raised).enumerate() {
            ensure!(
                !raised.kind.is_fast_forward() || matches!(outcome, RoundOutcome::Planned { .. }),
                "the cancel or failure {index} is deferred"
            );
            let RoundOutcome::Planned { outcome, .. } = outcome else {
                continue;
            };
            num_applied += usize::from(matches!(outcome, TreeEventOutcome::Applied(_)));
            let is_in_tree = self.model.structure.node(raised.job_id).is_some();
            ensure!(
                matches!(outcome, TreeEventOutcome::Untracked) != is_in_tree,
                "the event {index} is answered {outcome:?}, and its job is in the tree: \
                 {is_in_tree}"
            );
        }
        ensure!(
            (num_applied == 0) == plan.updates.is_empty(),
            "a round of {num_applied} applied events writes {} updates",
            plan.updates.len()
        );
        if plan.updates.is_empty() {
            ensure!(
                plan.publications.is_empty()
                    && plan
                        .outcomes
                        .iter()
                        .all(|outcome| !outcome.has_child_registration())
                    && plan.num_active_children.is_empty(),
                "a round that writes nothing changes something: {plan:?}"
            );
        }

        self.check_aborts(plan)?;
        check_registrations_and_payload(events, plan)?;
        check_publications(plan)
    }

    /// Checks that every update of `plan` that aborts a job, moving it from [`JobState::Ready`],
    /// [`JobState::Running`], [`JobState::BlockingOnChildren`] or [`JobState::CommitReady`] to
    /// [`JobState::CleanupReady`] or [`JobState::Cancelled`], moves it to its row's abort target
    /// (see [`Row::abort_target`]).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if an aborted job moves to another state than its abort target.
    fn check_aborts(&self, plan: &Plan) -> anyhow::Result<()> {
        for update in &plan.updates {
            let is_abort = matches!(
                update.from,
                JobState::Ready
                    | JobState::Running
                    | JobState::BlockingOnChildren
                    | JobState::CommitReady
            ) && matches!(update.to, JobState::CleanupReady | JobState::Cancelled);
            if !is_abort {
                continue;
            }
            let row = self.model.row(update.job_id);
            ensure!(
                update.to == row.abort_target(),
                "the aborted job {:?} moves to {}, not to its abort target {}",
                update.job_id,
                update.to,
                row.abort_target()
            );
        }
        Ok(())
    }

    /// Checks every outcome of a round of `events` exactly against the design's event table. Each
    /// event is checked against the state its job has after the round's earlier planned events,
    /// which are planned again without the later events and must keep their outcomes:
    ///
    /// * An event is deferred exactly when its payload would push the payload of the round's
    ///   earlier planned events over [`MAX_ROUND_PAYLOAD`].
    /// * A planned event is [`TreeEventOutcome::Untracked`] if its job left the tree, applied with
    ///   the table's state if it acts on the state it found, and stale with the table's error
    ///   otherwise. An applied event changes its job from the job's state at the start of the
    ///   round.
    ///
    /// # Returns
    ///
    /// The failures that the round's applied events plan, to check after the apply, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if an outcome is not the expected one.
    /// * Forwards [`Self::plan_again`]'s return values on failure.
    /// * Forwards [`Model::expected_failure`]'s return values on failure.
    fn check_outcomes(
        &self,
        events: &[TreeEvent],
        plan: &Plan,
        coverage: &mut Coverage,
    ) -> anyhow::Result<Vec<ExpectedFailure>> {
        let mut planned_indices: Vec<usize> = Vec::new();
        let mut failures = Vec::new();
        for (index, event) in events.iter().enumerate() {
            let earlier_events: Vec<&TreeEvent> = planned_indices
                .iter()
                .map(|index| &events[*index])
                .collect();
            let earlier_plan = self.plan_again(&earlier_events, planned_indices.last(), plan)?;
            let found = |job_id: JobId| {
                earlier_plan
                    .updates
                    .iter()
                    .find(|update| update.job_id == job_id)
                    .map_or_else(|| self.model.row(job_id).state, |update| update.to)
            };
            let found_state = found(event.job_id);
            let raised_kind = RaisedKind::from(&event.kind);
            let expected = if self.model.structure.node(event.job_id).is_none() {
                TreeEventOutcome::Untracked
            } else if raised_kind.acts_on(found_state) {
                TreeEventOutcome::Applied(self.model.expected_applied_state(event, found_state))
            } else {
                TreeEventOutcome::Stale(raised_kind.stale_error(found_state))
            };
            let admission = expected_admission(event, &expected, &earlier_events, &earlier_plan);
            let RoundOutcome::Planned { outcome, .. } = &plan.outcomes[index] else {
                ensure!(
                    admission != Admission::Admitted,
                    "the event {index} ({event:?}, expected {expected:?}) is deferred"
                );
                coverage.count_deferral(&admission, &earlier_events, &earlier_plan);
                continue;
            };
            ensure!(
                admission == Admission::Admitted,
                "the event {index} ({event:?}) is planned {outcome:?}, not deferred for \
                 {admission:?}"
            );
            ensure!(
                format!("{outcome:?}") == format!("{expected:?}"),
                "the event {index} ({event:?} of a job found {found_state}) is planned \
                 {outcome:?}, not {expected:?}"
            );
            planned_indices.push(index);
            let TreeEventOutcome::Applied(state) = outcome else {
                continue;
            };
            let row = self.model.row(event.job_id);
            let update = plan
                .updates
                .iter()
                .find(|update| update.job_id == event.job_id)
                .with_context(|| format!("the applied event {index} updates no row of its job"))?;
            ensure!(
                (update.from, update.to) == (row.state, *state),
                "the applied event {index} updates its job from {} to {}, not from {} to {state}",
                update.from,
                update.to,
                row.state
            );
            if *state == JobState::Failed {
                let parent_state = row.parent.map(found);
                failures.push(self.model.expected_failure(event, parent_state)?);
            }
        }
        let planned_events: Vec<&TreeEvent> = planned_indices
            .iter()
            .map(|index| &events[*index])
            .collect();
        self.plan_again(&planned_events, planned_indices.last(), plan)?;
        Ok(failures)
    }

    /// Plans `planned_events`, the round's planned events up to the one at `last_index` in the
    /// round, again, alone, and checks that each is planned and that the last one keeps its outcome
    /// in `plan`.
    ///
    /// # Returns
    ///
    /// The plan of `planned_events` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if an event is deferred or the last one changes its outcome.
    /// * Forwards [`TreeStructure::plan_events`]'s return values on failure.
    fn plan_again(
        &self,
        planned_events: &[&TreeEvent],
        last_index: Option<&usize>,
        plan: &Plan,
    ) -> anyhow::Result<Plan> {
        let earlier_plan = self
            .model
            .structure
            .plan_events(planned_events.iter().copied())
            .context("the round's earlier planned events should plan again")?;
        ensure!(
            earlier_plan
                .outcomes
                .iter()
                .all(|outcome| matches!(outcome, RoundOutcome::Planned { .. })),
            "the round's planned events are deferred when planned again without the others: {:?}",
            earlier_plan.outcomes
        );
        if let Some(last_index) = last_index {
            ensure!(
                format!("{:?}", earlier_plan.outcomes.last())
                    == format!("{:?}", plan.outcomes.get(*last_index)),
                "the event {last_index} is planned {:?} without the later events, but {:?} with \
                 them",
                earlier_plan.outcomes.last(),
                plan.outcomes.get(*last_index)
            );
        }
        Ok(earlier_plan)
    }

    /// Checks, after the plan of a round of `events` is applied, that every applied event left its
    /// job in its planned state, and that every applied regular-tasks-done event stored its
    /// outputs.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if a row breaks either property.
    fn check_applied(&self, events: &[TreeEvent], plan: &Plan) -> anyhow::Result<()> {
        for (index, (event, outcome)) in events.iter().zip(&plan.outcomes).enumerate() {
            let RoundOutcome::Planned {
                outcome: TreeEventOutcome::Applied(state),
                ..
            } = outcome
            else {
                continue;
            };
            let row = self.model.row(event.job_id);
            ensure!(
                row.state == *state,
                "the event {index} is applied as {state}, but its job is {}",
                row.state
            );
            if let TreeEventKind::RegularTasksDone { outputs, .. } = &event.kind {
                ensure!(
                    row.outputs.as_ref() == Some(outputs),
                    "the event {index} does not store its outputs"
                );
            }
        }
        Ok(())
    }
}

/// # Returns
///
/// How the planner admits `event`, whose expected outcome is `expected`, after the round's earlier
/// planned events, `earlier_events`, whose plan is `earlier_plan`:
///
/// * [`Admission::PayloadBudgetExceeded`] if the event's outputs and children, and the earlier
///   events' payload, are both non-empty and exceed [`MAX_ROUND_PAYLOAD`] together.
/// * [`Admission::Admitted`] otherwise.
fn expected_admission(
    event: &TreeEvent,
    expected: &TreeEventOutcome,
    earlier_events: &[&TreeEvent],
    earlier_plan: &Plan,
) -> Admission {
    let TreeEventKind::RegularTasksDone { outputs, children } = &event.kind else {
        return Admission::Admitted;
    };
    let TreeEventOutcome::Applied(state) = expected else {
        return Admission::Admitted;
    };
    let payload_size = outputs_size(outputs)
        + if *state == JobState::BlockingOnChildren {
            child_usage(children).size
        } else {
            0
        };
    let earlier_payload_size = plan_payload_size(earlier_events.iter().copied(), earlier_plan);
    if payload_size > 0
        && earlier_payload_size > 0
        && payload_size + earlier_payload_size > MAX_ROUND_PAYLOAD
    {
        Admission::PayloadBudgetExceeded
    } else {
        Admission::Admitted
    }
}

/// # Returns
///
/// The number of `children` and the sum of the sizes of their submissions.
fn child_usage(children: &[PreparedChild]) -> ChildJobUsage {
    ChildJobUsage {
        count: children.len(),
        size: children.iter().map(|child| child.submission().size()).sum(),
    }
}

/// # Returns
///
/// The total number of bytes of `outputs`.
///
/// # Panics
///
/// Panics if the size of an output doesn't fit in a `u64`.
fn outputs_size(outputs: &[TaskOutput]) -> u64 {
    outputs
        .iter()
        .map(|output| u64::try_from(output.len()).expect("the size should fit a u64"))
        .sum()
}

/// # Type Parameters
///
/// * `'event_lifetime` - The lifetime of the borrowed events of the round.
///
/// # Returns
///
/// The registering job and the usage of the child set of each event of `events` whose outcome in
/// `outcomes`, the outcomes of a round of `events`, registers the children it carries, in the
/// round's order.
fn registered_child_sets<'event_lifetime>(
    events: impl IntoIterator<Item = &'event_lifetime TreeEvent>,
    outcomes: &[RoundOutcome],
) -> Vec<(JobId, ChildJobUsage)> {
    events
        .into_iter()
        .zip(outcomes)
        .filter(|(_, outcome)| outcome.has_child_registration())
        .filter_map(|(event, _)| match &event.kind {
            TreeEventKind::RegularTasksDone { children, .. } => {
                Some((event.job_id, child_usage(children)))
            }
            TreeEventKind::Start { .. }
            | TreeEventKind::CommitDone
            | TreeEventKind::CleanupDone
            | TreeEventKind::TerminalFailure { .. }
            | TreeEventKind::CancelRoot => None,
        })
        .collect()
}

/// # Type Parameters
///
/// * `'event_lifetime` - The lifetime of the borrowed events of the round.
///
/// # Returns
///
/// The number of bytes of job outputs and child payloads that `plan`, the plan of a round of
/// `events`, writes.
fn plan_payload_size<'event_lifetime>(
    events: impl IntoIterator<Item = &'event_lifetime TreeEvent>,
    plan: &Plan,
) -> u64 {
    let outputs: u64 = plan
        .updates
        .iter()
        .filter_map(|update| update.outputs.as_deref())
        .map(outputs_size)
        .sum();
    outputs
        + registered_child_sets(events, &plan.outcomes)
            .iter()
            .map(|(_, children)| children.size)
            .sum::<u64>()
}

/// Checks that `plan`, the plan of a round of `events`, registers the children of exactly its
/// applied registering events, and that the round's payload stays within [`MAX_ROUND_PAYLOAD`]
/// unless one job carries all of it.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if the registrations or the payload break either property.
fn check_registrations_and_payload(events: &[TreeEvent], plan: &Plan) -> anyhow::Result<()> {
    let mut payload_carriers = HashSet::new();
    let mut payload_size = 0;
    for update in &plan.updates {
        let outputs_size = update.outputs.as_deref().map_or(0, outputs_size);
        if outputs_size > 0 {
            payload_carriers.insert(update.job_id);
            payload_size += outputs_size;
        }
    }
    for (index, (event, outcome)) in events.iter().zip(&plan.outcomes).enumerate() {
        let registers = matches!(
            (&event.kind, outcome),
            (
                TreeEventKind::RegularTasksDone { children, .. },
                RoundOutcome::Planned {
                    outcome: TreeEventOutcome::Applied(JobState::BlockingOnChildren),
                    ..
                },
            ) if !children.is_empty()
        );
        ensure!(
            outcome.has_child_registration() == registers,
            "the event {index} ({event:?}) is planned {outcome:?}, and registers children: \
             {registers}"
        );
    }
    for (parent_id, children) in registered_child_sets(events, &plan.outcomes) {
        payload_carriers.insert(parent_id);
        payload_size += children.size;
    }
    ensure!(
        payload_size <= MAX_ROUND_PAYLOAD || payload_carriers.len() == 1,
        "a round of {payload_size} payload bytes carries the payload of {} jobs",
        payload_carriers.len()
    );
    Ok(())
}

/// Checks that the publications of `plan` are exactly those that its updates make: the ready tasks
/// of a started job, the commit task of a commit-ready job and the cleanup task of a cleanup-ready
/// job.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if the publications differ.
fn check_publications(plan: &Plan) -> anyhow::Result<()> {
    let mut expected_publications: Vec<Publication> = plan
        .updates
        .iter()
        .filter_map(|update| match update.to {
            JobState::Running => Some(Publication::Ready {
                job_id: update.job_id,
                task_indices: vec![0],
            }),
            JobState::CommitReady => Some(Publication::Commit(update.job_id)),
            JobState::CleanupReady => Some(Publication::Cleanup(update.job_id)),
            JobState::Ready
            | JobState::BlockingOnChildren
            | JobState::Succeeded
            | JobState::Failed
            | JobState::Cancelled => None,
        })
        .collect();
    let mut publications: Vec<&Publication> = plan.publications.iter().collect();
    publications.sort_by_key(|publication| publication.job_id().get());
    expected_publications.sort_by_key(|publication| publication.job_id().get());
    ensure!(
        publications.into_iter().eq(&expected_publications),
        "the round publishes {:?}, but its updates publish {expected_publications:?}",
        plan.publications
    );
    Ok(())
}

/// Runs the random tree of `seed` with `raising` until it is quiescent or the round limit is
/// reached. With [`Raising::Once`], the tree must become quiescent as
/// [`Simulation::run_to_quiescence`] requires, and the events left in the buffers are then drained.
///
/// # Returns
///
/// The number of rounds planned before the tree became quiescent or the limit was reached, on
/// success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`Simulation::run_to_quiescence`]'s return values on failure.
/// * Forwards [`Simulation::drain`]'s return values on failure.
/// * Forwards [`Simulation::run`]'s return values on failure.
async fn run_tree(seed: u64, raising: Raising, coverage: &mut Coverage) -> anyhow::Result<usize> {
    let mut rng = StdRng::seed_from_u64(seed);
    let model = Model::new(&mut rng);
    let mut simulation = Simulation::new(model, raising);
    if raising == Raising::Once {
        let rounds = simulation.run_to_quiescence(&mut rng, coverage).await?;
        simulation.drain(&mut rng, coverage).await?;
        return Ok(rounds);
    }
    let (rounds, is_quiescent) = simulation.run(&mut rng, coverage).await?;
    coverage.unfinished_trees += usize::from(!is_quiescent);
    Ok(rounds)
}

/// Runs random trees with `raising`, from [`BASE_SEED`] on, until at least
/// `JOB_TREE_PROPERTY_ROUNDS` rounds are planned (by default 3,000 in debug builds and 20,000 in
/// others), or only the tree of the seed `JOB_TREE_PROPERTY_SEED` if it is set.
///
/// # Returns
///
/// The coverage of the trees on success, or [`None`] if only one seed was rerun.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`run_tree`]'s return values on failure, with the tree's seed.
async fn run_trees(raising: Raising) -> anyhow::Result<Option<Coverage>> {
    let env_value = |name: &str| match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map(Some)
            .with_context(|| format!("`{name}` is not a u64")),
        Err(VarError::NotPresent) => Ok(None),
        Err(error @ VarError::NotUnicode(_)) => {
            Err(error).with_context(|| format!("`{name}` is not Unicode"))
        }
    };
    if let Some(seed) = env_value("JOB_TREE_PROPERTY_SEED")? {
        run_tree(seed, raising, &mut Coverage::default())
            .await
            .with_context(|| format!("the tree of seed {seed} breaks a property"))?;
        return Ok(None);
    }
    let num_rounds = env_value("JOB_TREE_PROPERTY_ROUNDS")?.map_or(
        Ok(if cfg!(debug_assertions) {
            3_000
        } else {
            20_000
        }),
        usize::try_from,
    )?;
    let mut coverage = Coverage::default();
    let mut rounds = 0;
    let mut seed = BASE_SEED;
    while rounds < num_rounds {
        rounds += run_tree(seed, raising, &mut coverage)
            .await
            .with_context(|| {
                format!(
                    "the tree of seed {seed} breaks a property; rerun it with \
                     JOB_TREE_PROPERTY_SEED={seed}"
                )
            })?;
        seed += 1;
    }
    println!(
        "Planned {rounds} rounds over {} random trees raising events {raising:?}: {coverage:?}.",
        seed - BASE_SEED
    );
    Ok(Some(coverage))
}

#[tokio::test]
async fn planned_rounds_keep_the_tree_consistent() -> anyhow::Result<()> {
    let Some(coverage) = run_trees(Raising::Repeated).await? else {
        return Ok(());
    };
    Coverage::ensure_reached(&[
        ("grouped rounds", coverage.grouped_rounds),
        ("deferred events", coverage.deferred_events),
        ("registrations", coverage.registrations),
        ("absent events", coverage.absent_events),
        ("applied cancels", coverage.applied_cancels),
        ("propagated failures", coverage.propagated_failures),
        (
            "commit-ready jobs aborted by cancels",
            coverage.commit_ready_jobs_aborted_by_cancels,
        ),
        (
            "commit-ready jobs aborted by failures",
            coverage.commit_ready_jobs_aborted_by_failures,
        ),
        ("unblocked parents", coverage.unblocked_parents),
        (
            "cancels planned with other events",
            coverage.crowded_cancels,
        ),
        ("payload budget deferrals", coverage.budget_deferrals),
        (
            "large registrations that defer others for the budget",
            coverage.budget_deferring_registrations,
        ),
        (
            "rounds that register several child sets",
            coverage.multi_registration_rounds,
        ),
        (
            "cancels or failures taken ahead of regular events",
            coverage.fast_forwarded_rounds,
        ),
        ("full rounds", coverage.full_rounds),
        (
            "perturbations that break an invariant",
            coverage.broken_perturbations,
        ),
        (
            "perturbations that break nothing",
            coverage.harmless_perturbations,
        ),
    ])?;
    coverage.ensure_outcomes_reached(&["Applied", "Stale"])
}

#[tokio::test]
async fn events_raised_once_are_each_answered_and_quiesce_the_tree() -> anyhow::Result<()> {
    let Some(coverage) = run_trees(Raising::Once).await? else {
        return Ok(());
    };
    Coverage::ensure_reached(&[
        ("grouped rounds", coverage.grouped_rounds),
        ("deferred events", coverage.deferred_events),
        ("registrations", coverage.registrations),
        ("applied cancels", coverage.applied_cancels),
        ("propagated failures", coverage.propagated_failures),
        (
            "commit-ready jobs aborted by cancels",
            coverage.commit_ready_jobs_aborted_by_cancels,
        ),
        (
            "commit-ready jobs aborted by failures",
            coverage.commit_ready_jobs_aborted_by_failures,
        ),
        ("unblocked parents", coverage.unblocked_parents),
        (
            "rounds that register several child sets",
            coverage.multi_registration_rounds,
        ),
        ("full rounds", coverage.full_rounds),
    ])?;
    coverage.ensure_outcomes_reached(&["Applied", "Stale", "Untracked"])
}
