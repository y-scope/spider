//! The plan of a round of job-tree events.

use std::collections::HashMap;

use spider_core::job::JobState;
use spider_core::task::TaskIndex;
use spider_core::types::id::JobId;
use spider_core::types::io::TaskOutput;

use crate::cache::error::StaleStateError;
use crate::cache::tree::ChildJobUsage;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::TreeEventKind;
use crate::cache::tree::TreeEventOutcome;
use crate::cache::tree::TreeNode;
use crate::cache::tree::TreeStructure;
use crate::db::JobStateUpdate;
use crate::limits::MAX_CHILDREN_PER_JOB;
use crate::limits::MAX_ERROR_MESSAGE_CHARS;
use crate::limits::MAX_JOB_CHILDREN_SIZE;
use crate::limits::MAX_ROUND_PAYLOAD;

/// The plan of a round of events.
#[derive(Debug, Default)]
pub struct Plan {
    /// The guarded row updates to write, at most one per job.
    pub updates: Vec<JobStateUpdate>,

    /// The ready work to publish after the updates are applied.
    pub publications: Vec<Publication>,

    /// The outcome of each event of the round, in the round's order.
    pub outcomes: Vec<RoundOutcome>,

    /// The children to register, at most one parent's.
    pub registration: Option<PlannedRegistration>,

    /// The number of bytes of job outputs and child payloads that the plan writes.
    payload_size: u64,
}

impl Plan {
    /// # Returns
    ///
    /// Whether `event_plan` can join this plan:
    ///
    /// * [`Admission::JobConflict`] if it changes a job that this plan changes.
    /// * [`Admission::ChildSetAlreadyPlanned`] if it registers children and this plan does too.
    /// * [`Admission::PayloadBudgetExceeded`] if it writes payload, this plan writes payload too,
    ///   and the payload of both exceeds [`MAX_ROUND_PAYLOAD`].
    /// * [`Admission::Admitted`] otherwise.
    fn admission(&self, event_plan: &EventPlan) -> Admission {
        if let Some(update) = event_plan.updates.iter().find(|update| {
            self.updates
                .iter()
                .any(|planned_update| planned_update.job_id == update.job_id)
        }) {
            return Admission::JobConflict(update.job_id);
        }
        if event_plan.registration.is_some() && self.registration.is_some() {
            return Admission::ChildSetAlreadyPlanned;
        }
        let payload_size = event_plan.payload_size();
        if payload_size == 0
            || self.payload_size == 0
            || self.payload_size + payload_size <= MAX_ROUND_PAYLOAD
        {
            Admission::Admitted
        } else {
            Admission::PayloadBudgetExceeded
        }
    }

    /// Adds `event_plan`, the plan of the round's next event, to this plan.
    fn merge(&mut self, event_plan: EventPlan) {
        self.payload_size += event_plan.payload_size();
        if let Some(EventRegistration {
            parent_id,
            children,
        }) = event_plan.registration
        {
            self.registration = Some(PlannedRegistration {
                event_index: self.outcomes.len(),
                parent_id,
                children,
            });
        }
        self.outcomes
            .push(RoundOutcome::Planned(event_plan.outcome));
        self.updates.extend(event_plan.updates);
        self.publications.extend(event_plan.publications);
    }

    /// Defers the round's next event to a later round.
    fn defer(&mut self) {
        self.outcomes.push(RoundOutcome::Deferred);
    }
}

/// The registration of a child set that a round plans.
#[derive(Debug, PartialEq, Eq)]
pub struct PlannedRegistration {
    /// The index, in the round, of the event that registers the children.
    pub event_index: usize,

    /// The job whose children the event registers.
    pub parent_id: JobId,

    /// The usage of the children that the event registers.
    pub children: ChildJobUsage,
}

/// The outcome of an event in a round.
#[derive(Debug)]
pub enum RoundOutcome {
    /// The round plans and answers the event with the carried outcome.
    Planned(TreeEventOutcome),

    /// The round defers the event to a later round.
    Deferred,
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

impl Publication {
    /// # Returns
    ///
    /// The job whose work the publication makes available.
    #[must_use]
    pub const fn job_id(&self) -> JobId {
        match self {
            Self::Ready { job_id, .. } | Self::Commit(job_id) | Self::Cleanup(job_id) => *job_id,
        }
    }
}

/// How an event's plan is applied to the plan of a round's earlier events.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Admission {
    /// The event's plan is merged into the round's plan.
    Admitted,

    /// Nothing is applied: the event's plan changes the carried job, which the round's plan
    /// already changes.
    JobConflict(JobId),

    /// The event is deferred to a later round: it registers children while the round's plan already
    /// registers a child set.
    ChildSetAlreadyPlanned,

    /// The event is deferred to a later round: its outputs and children would push the round's
    /// payload over [`MAX_ROUND_PAYLOAD`].
    PayloadBudgetExceeded,
}

/// The planner of a round of events: it plans each event against the result of the round's
/// earlier events and builds the round's plan.
pub(super) struct RoundPlanner<'structure_lifetime> {
    overlay: Overlay<'structure_lifetime>,
    plan: Plan,
}

impl<'structure_lifetime> RoundPlanner<'structure_lifetime> {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The planner of a round against `structure` with no event planned yet.
    pub(super) fn new(structure: &'structure_lifetime TreeStructure) -> Self {
        Self {
            overlay: Overlay::new(structure),
            plan: Plan::default(),
        }
    }

    /// Plans `event` against the overlay, then applies its admission: an admitted event's plan is
    /// merged into the round's plan and the overlay; an event that registers a second child set or
    /// is over the payload budget is deferred; a job conflict applies nothing.
    ///
    /// # Returns
    ///
    /// The admission of `event`'s plan.
    pub(super) fn plan_event(&mut self, event: &TreeEvent) -> Admission {
        let event_plan = self.overlay.plan_event(event);
        let admission = self.plan.admission(&event_plan);
        match admission {
            Admission::Admitted => {
                self.overlay.merge(&event_plan);
                self.plan.merge(event_plan);
            }
            Admission::JobConflict(_) => {}
            Admission::ChildSetAlreadyPlanned | Admission::PayloadBudgetExceeded => {
                self.plan.defer();
            }
        }
        admission
    }

    /// # Returns
    ///
    /// The plan of the round's planned events.
    pub(super) fn finish(self) -> Plan {
        self.plan
    }
}

/// The tree as the planner reads it: the tree's structure with the plan of the round's earlier
/// events applied.
struct Overlay<'structure_lifetime> {
    structure: &'structure_lifetime TreeStructure,
    planned_states: HashMap<JobId, JobState>,
}

impl<'structure_lifetime> Overlay<'structure_lifetime> {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The overlay of `structure` with nothing planned yet.
    fn new(structure: &'structure_lifetime TreeStructure) -> Self {
        Self {
            structure,
            planned_states: HashMap::new(),
        }
    }

    /// Plans one event against the overlay.
    ///
    /// # Returns
    ///
    /// The plan of `event`:
    ///
    /// * [`TreeEventOutcome::Untracked`] if the event's job is not in the tree.
    /// * [`TreeEventOutcome::Stale`] if the job's state does not accept the event (see
    ///   [`TreeEventKind::ensure_accepts`]).
    /// * Otherwise, the event is applied:
    ///   * [`TreeEventKind::Start`]: the root moves from [`JobState::Ready`] to
    ///     [`JobState::Running`], and its ready tasks are published.
    ///   * [`TreeEventKind::RegularTasksDone`] without children: the job stores its outputs and
    ///     moves from [`JobState::Running`] to [`JobState::CommitReady`], with its commit task
    ///     published, if it has a commit task, otherwise to [`JobState::Succeeded`].
    ///   * [`TreeEventKind::RegularTasksDone`] with children over the limits of one job (see
    ///     [`ChildJobUsage::is_within_job_limits`]): the job moves from [`JobState::Running`] to
    ///     [`JobState::Failed`].
    ///   * [`TreeEventKind::RegularTasksDone`] with other children: the job stores its outputs,
    ///     moves from [`JobState::Running`] to [`JobState::BlockingOnChildren`] and registers its
    ///     children.
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
    fn plan_event(&self, event: &TreeEvent) -> EventPlan {
        let job_id = event.job_id;
        let Some(node) = self.node(job_id) else {
            return EventPlan::untracked();
        };
        if let Err(error) = event.kind.ensure_accepts(node.state) {
            return EventPlan::stale(error);
        }
        match &event.kind {
            TreeEventKind::Start { ready_task_indices } => {
                EventPlan::start_root(job_id, ready_task_indices.clone())
            }
            TreeEventKind::RegularTasksDone { outputs, children } if children.is_empty() => {
                EventPlan::complete_regular_tasks(job_id, &node, outputs.clone())
            }
            TreeEventKind::RegularTasksDone { outputs, children } => {
                let children = ChildJobUsage::of(children);
                if children.is_within_job_limits() {
                    EventPlan::register_children(job_id, outputs.clone(), children)
                } else {
                    EventPlan::fail_job(
                        job_id,
                        node.state,
                        &format!(
                            "child jobs ({} of {} bytes) exceed the limits of \
                             {MAX_CHILDREN_PER_JOB} child jobs and {MAX_JOB_CHILDREN_SIZE} bytes \
                             per job",
                            children.count, children.size
                        ),
                    )
                }
            }
            TreeEventKind::CommitDone => EventPlan::complete_commit(job_id),
            TreeEventKind::CleanupDone => EventPlan::complete_cleanup(job_id),
            TreeEventKind::TerminalFailure { message, .. } => {
                EventPlan::fail_job(job_id, node.state, message)
            }
            TreeEventKind::CancelRoot => EventPlan::cancel_root(job_id, &node),
        }
    }

    /// Records an admitted event's plan in the overlay, so that the round's later events read its
    /// result.
    fn merge(&mut self, event_plan: &EventPlan) {
        for update in &event_plan.updates {
            self.planned_states.insert(update.job_id, update.to);
        }
    }

    /// # Returns
    ///
    /// The node of the tree's job `job_id`, in the state that the round's earlier events plan for
    /// it, or [`None`] if the job is not in the tree.
    fn node(&self, job_id: JobId) -> Option<TreeNode> {
        let node = self.structure.node(job_id)?;
        Some(TreeNode {
            state: self
                .planned_states
                .get(&job_id)
                .copied()
                .unwrap_or(node.state),
            ..*node
        })
    }
}

/// The children that the plan of one event registers.
#[derive(Debug, Clone, Copy)]
struct EventRegistration {
    parent_id: JobId,
    children: ChildJobUsage,
}

/// The plan of one event of a round.
#[derive(Debug)]
struct EventPlan {
    updates: Vec<JobStateUpdate>,
    publications: Vec<Publication>,
    outcome: TreeEventOutcome,
    registration: Option<EventRegistration>,
}

impl EventPlan {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of starting the root `root_id`: [`JobState::Ready`] to [`JobState::Running`],
    /// with the tasks of `ready_task_indices` published.
    fn start_root(root_id: JobId, ready_task_indices: Vec<TaskIndex>) -> Self {
        Self::applying_and_publishing(
            JobStateUpdate::new(root_id, JobState::Ready, JobState::Running),
            vec![Publication::Ready {
                job_id: root_id,
                task_indices: ready_task_indices,
            }],
        )
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of the regular tasks of `job_id` being done: the job stores `outputs` and moves
    /// from [`JobState::Running`] to [`JobState::CommitReady`], with its commit task published, if
    /// `node` has a commit task, otherwise to [`JobState::Succeeded`].
    fn complete_regular_tasks(job_id: JobId, node: &TreeNode, outputs: Vec<TaskOutput>) -> Self {
        let to = if node.has_commit {
            JobState::CommitReady
        } else {
            JobState::Succeeded
        };
        let publications = if to == JobState::CommitReady {
            vec![Publication::Commit(job_id)]
        } else {
            Vec::new()
        };
        Self::applying_and_publishing(
            JobStateUpdate {
                outputs: Some(outputs),
                ..JobStateUpdate::new(job_id, JobState::Running, to)
            },
            publications,
        )
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of the regular tasks of `job_id` being done with `children` to register: the job
    /// stores `outputs` and moves from [`JobState::Running`] to [`JobState::BlockingOnChildren`].
    fn register_children(job_id: JobId, outputs: Vec<TaskOutput>, children: ChildJobUsage) -> Self {
        Self {
            registration: Some(EventRegistration {
                parent_id: job_id,
                children,
            }),
            ..Self::applying(JobStateUpdate {
                outputs: Some(outputs),
                ..JobStateUpdate::new(job_id, JobState::Running, JobState::BlockingOnChildren)
            })
        }
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
        let stored_message = truncate_chars(message, MAX_ERROR_MESSAGE_CHARS);
        if stored_message.len() < message.len() {
            tracing::warn!(
                job_id = job_id.get(),
                message_bytes = message.len(),
                max_chars = MAX_ERROR_MESSAGE_CHARS,
                "A job's failure message exceeds the maximum length and is truncated."
            );
        }
        Self::applying(JobStateUpdate {
            error_message: Some(stored_message.to_owned()),
            ..JobStateUpdate::new(job_id, from, JobState::Failed)
        })
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of cancelling the root `root_id`, whose node is `node`: the root moves from the
    /// node's state to its abort target (see [`TreeNode::abort_target`]), with its cleanup task
    /// published if that is [`JobState::CleanupReady`].
    fn cancel_root(root_id: JobId, node: &TreeNode) -> Self {
        let from = node.state;
        let to = node.abort_target();
        let publications = if to == JobState::CleanupReady {
            vec![Publication::Cleanup(root_id)]
        } else {
            Vec::new()
        };
        Self::applying_and_publishing(JobStateUpdate::new(root_id, from, to), publications)
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of an event that its job's state does not accept: it writes nothing, and its event
    /// is answered with `error`.
    const fn stale(error: StaleStateError) -> Self {
        Self {
            updates: Vec::new(),
            publications: Vec::new(),
            outcome: TreeEventOutcome::Stale(error),
            registration: None,
        }
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// The plan of an event whose job is not in the tree: it writes nothing, and its event is
    /// answered [`TreeEventOutcome::Untracked`].
    const fn untracked() -> Self {
        Self {
            updates: Vec::new(),
            publications: Vec::new(),
            outcome: TreeEventOutcome::Untracked,
            registration: None,
        }
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// A plan that writes `update` alone, publishes nothing, and whose event's job moves to the
    /// state that `update` moves it to.
    fn applying(update: JobStateUpdate) -> Self {
        Self::applying_and_publishing(update, Vec::new())
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// A plan that writes `update` alone, then publishes `publications`, and whose event's job
    /// moves to the state that `update` moves it to.
    fn applying_and_publishing(update: JobStateUpdate, publications: Vec<Publication>) -> Self {
        Self {
            outcome: TreeEventOutcome::Applied(update.to),
            updates: vec![update],
            publications,
            registration: None,
        }
    }

    /// # Returns
    ///
    /// The number of bytes of job outputs that the plan's updates write, plus the number of bytes
    /// of the children that the plan registers.
    fn payload_size(&self) -> u64 {
        let outputs_size: u64 = self
            .updates
            .iter()
            .filter_map(|update| update.outputs.as_ref())
            .flatten()
            .map(|output| output.len() as u64)
            .sum();
        outputs_size
            + self
                .registration
                .map_or(0, |registration| registration.children.size)
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
    use spider_core::job::JobState;
    use spider_core::types::id::JobId;

    use crate::cache::tree::ChildJobUsage;
    use crate::cache::tree::Plan;
    use crate::cache::tree::PlannedRegistration;
    use crate::cache::tree::TreeNode;
    use crate::cache::tree::planner::Admission;
    use crate::cache::tree::planner::EventPlan;
    use crate::limits::MAX_ROUND_PAYLOAD;

    /// # Returns
    ///
    /// The plan of the regular tasks of the job `job_id` being done with two outputs of
    /// `payload_size` bytes in all.
    ///
    /// # Panics
    ///
    /// Panics if `payload_size` does not fit in a `usize`.
    fn completion(job_id: u64, payload_size: u64) -> EventPlan {
        let payload_size = usize::try_from(payload_size).expect("the payload should fit a usize");
        let first_output_size = payload_size / 2;
        EventPlan::complete_regular_tasks(
            JobId::from(job_id),
            &TreeNode {
                state: JobState::Running,
                parent: None,
                has_commit: false,
                has_cleanup: false,
                active_children: 0,
            },
            vec![
                vec![0; first_output_size],
                vec![0; payload_size - first_output_size],
            ],
        )
    }

    /// # Returns
    ///
    /// The plan of the regular tasks of the job `job_id` being done with no output and `children`.
    fn registration(job_id: u64, children: ChildJobUsage) -> EventPlan {
        EventPlan::register_children(JobId::from(job_id), Vec::new(), children)
    }

    #[test]
    fn the_payload_budget_defers_only_an_event_that_would_exceed_it_after_other_payload() {
        let mut plan = Plan::default();
        assert_eq!(
            plan.admission(&completion(2, MAX_ROUND_PAYLOAD + 1)),
            Admission::Admitted,
            "a round without payload should admit any payload"
        );

        plan.merge(completion(2, MAX_ROUND_PAYLOAD + 1));
        assert_eq!(
            plan.admission(&completion(3, 1)),
            Admission::PayloadBudgetExceeded
        );
        assert_eq!(
            plan.admission(&EventPlan::complete_commit(JobId::from(3))),
            Admission::Admitted,
            "a round over its payload budget should admit an event without payload"
        );

        let mut plan = Plan::default();
        plan.merge(completion(2, MAX_ROUND_PAYLOAD - 2));
        assert_eq!(plan.admission(&completion(3, 2)), Admission::Admitted);
        assert_eq!(
            plan.admission(&completion(3, 3)),
            Admission::PayloadBudgetExceeded
        );
    }

    #[test]
    fn a_child_set_counts_toward_the_round_payload() {
        let children = ChildJobUsage {
            count: 1,
            size: MAX_ROUND_PAYLOAD,
        };
        let mut plan = Plan::default();
        let registration = registration(2, children);
        assert_eq!(plan.admission(&registration), Admission::Admitted);

        plan.merge(registration);
        assert_eq!(
            plan.admission(&completion(3, 1)),
            Admission::PayloadBudgetExceeded
        );
    }

    #[test]
    fn a_second_child_set_is_deferred_but_an_event_without_one_is_admitted() {
        let children = ChildJobUsage { count: 1, size: 1 };
        let mut plan = Plan::default();
        plan.merge(completion(2, 1));
        plan.merge(registration(3, children));
        assert_eq!(
            plan.registration,
            Some(PlannedRegistration {
                event_index: 1,
                parent_id: JobId::from(3),
                children,
            })
        );

        assert_eq!(
            plan.admission(&registration(4, children)),
            Admission::ChildSetAlreadyPlanned
        );
        assert_eq!(plan.admission(&completion(4, 1)), Admission::Admitted);
    }
}
