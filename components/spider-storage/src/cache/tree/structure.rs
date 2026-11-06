//! The structure of a job tree and the planning of its rounds of events.

use std::collections::HashMap;
use std::collections::HashSet;

use spider_core::job::JobState;
use spider_core::types::id::JobId;

use crate::cache::error::InternalError;
use crate::cache::tree::Plan;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::planner::Admission;
use crate::cache::tree::planner::RoundPlanner;

/// The structure of a tree: its root's ID, and its non-terminal jobs and their states.
#[derive(Debug)]
pub struct TreeStructure {
    root_id: JobId,
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
            root_id,
            nodes: HashMap::from([(root_id, root)]),
        }
    }

    /// # Returns
    ///
    /// The ID of the tree's root.
    #[must_use]
    pub const fn root_id(&self) -> JobId {
        self.root_id
    }

    /// Plans a round of `events` against the tree's structure, in their order, each against the
    /// result of the round's earlier events. An event is deferred to a later round if:
    ///
    /// * It registers a child set after the round's planned child set.
    /// * Its outputs and children would push the round's payload over
    ///   [`MAX_ROUND_PAYLOAD`](crate::limits::MAX_ROUND_PAYLOAD) after the payload of the round's
    ///   earlier events.
    ///
    /// # Type Parameters
    ///
    /// * `'event_lifetime` - The lifetime of the borrowed events of the round.
    ///
    /// # Returns
    ///
    /// The plan of the round, with one outcome per event of `events`, in order, on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::JobChangedTwiceInRound`] if an event would change a job that an earlier
    ///   event of the round changes.
    pub fn plan_events<'event_lifetime>(
        &self,
        events: impl IntoIterator<Item = &'event_lifetime TreeEvent>,
    ) -> Result<Plan, InternalError> {
        let mut round_planner = RoundPlanner::new(self);
        for event in events {
            if let Admission::JobConflict(job_id) = round_planner.plan_event(event) {
                return Err(InternalError::JobChangedTwiceInRound(job_id));
            }
        }
        Ok(round_planner.finish())
    }

    /// # Returns
    ///
    /// The node of the tree's job `job_id`, or [`None`] if the job is not in the tree.
    #[must_use]
    pub fn node(&self, job_id: JobId) -> Option<&TreeNode> {
        self.nodes.get(&job_id)
    }

    /// # Returns
    ///
    /// The IDs of the tree's jobs, in ascending order.
    #[cfg(any(test, debug_assertions))]
    #[must_use]
    pub fn sorted_job_ids(&self) -> Vec<JobId> {
        let mut job_ids: Vec<JobId> = self.nodes.keys().copied().collect();
        job_ids.sort_unstable_by_key(JobId::get);
        job_ids
    }

    /// Inserts `children`, the registered children of the tree's job `parent_id`, into the tree,
    /// and sets the parent's number of active children to the number of `children`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::JobNotFound`] if `parent_id` is not in the tree.
    /// * [`InternalError::JobAlreadyExists`] if a child is in the tree or is given twice.
    pub fn insert_children(
        &mut self,
        parent_id: JobId,
        children: Vec<(JobId, TreeNode)>,
    ) -> Result<(), InternalError> {
        let mut child_ids = HashSet::with_capacity(children.len());
        for (child_id, _) in &children {
            if self.nodes.contains_key(child_id) || !child_ids.insert(*child_id) {
                return Err(InternalError::JobAlreadyExists(*child_id));
            }
        }
        let parent = self
            .nodes
            .get_mut(&parent_id)
            .ok_or(InternalError::JobNotFound(parent_id))?;
        parent.active_children = children.len();
        self.nodes.extend(children);
        Ok(())
    }

    /// Checks the tree invariants that the structure can break, job by job in ascending ID order.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Violation::TerminalJobInTree`] if the structure holds a terminal job.
    /// * [`Violation::RootWithParent`] if the root has a parent.
    /// * [`Violation::NonRootWithoutParent`] if a job other than the root has no parent.
    /// * [`Violation::ReadyNonRoot`] if a job other than the root is [`JobState::Ready`].
    /// * [`Violation::ChildIdNotAboveParent`] if a job's ID is not above its parent's.
    /// * [`Violation::BlockingJobWithoutActiveChild`] if a job blocks on its children without an
    ///   active child.
    #[cfg(any(test, debug_assertions))]
    pub fn check_invariants(&self) -> Result<(), Violation> {
        let mut nodes: Vec<(JobId, &TreeNode)> = self
            .nodes
            .iter()
            .map(|(job_id, node)| (*job_id, node))
            .collect();
        nodes.sort_unstable_by_key(|(job_id, _)| job_id.get());
        for (job_id, node) in nodes {
            if node.state.is_terminal() {
                return Err(Violation::TerminalJobInTree {
                    job_id,
                    state: node.state,
                });
            }
            match (job_id == self.root_id, node.parent) {
                (true, Some(_)) => return Err(Violation::RootWithParent(job_id)),
                (false, None) => return Err(Violation::NonRootWithoutParent(job_id)),
                (false, Some(_)) if node.state == JobState::Ready => {
                    return Err(Violation::ReadyNonRoot(job_id));
                }
                (false, Some(parent_id)) if parent_id.get() >= job_id.get() => {
                    return Err(Violation::ChildIdNotAboveParent { job_id, parent_id });
                }
                _ => {}
            }
            if node.state == JobState::BlockingOnChildren && node.active_children == 0 {
                return Err(Violation::BlockingJobWithoutActiveChild(job_id));
            }
        }
        Ok(())
    }

    /// Sets the state of the tree's job `job_id` to `state`. A terminal state drops the job from
    /// the structure.
    ///
    /// # Panics
    ///
    /// Panics if `job_id` is not in the tree.
    pub fn apply_state(&mut self, job_id: JobId, state: JobState) {
        if state.is_terminal() {
            self.nodes
                .remove(&job_id)
                .expect("a job whose state is applied should be in the tree");
        } else {
            self.nodes
                .get_mut(&job_id)
                .expect("a job whose state is applied should be in the tree")
                .state = state;
        }
    }

    /// # Returns
    ///
    /// Whether the tree has no job left.
    #[must_use]
    pub fn is_quiescent(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// A non-terminal job of a tree, as the planner sees it.
#[derive(Debug)]
pub struct TreeNode {
    pub state: JobState,
    pub parent: Option<JobId>,
    pub has_commit: bool,
    pub has_cleanup: bool,
    pub active_children: usize,
}

impl TreeNode {
    /// # Returns
    ///
    /// The state that a cancel moves the job to: [`JobState::Cancelled`] if the job is
    /// [`JobState::Ready`] or has no cleanup task, otherwise [`JobState::CleanupReady`]. A
    /// [`JobState::Ready`] job never moves to [`JobState::CleanupReady`]: none of its tasks ran,
    /// and the edge is not a valid job state transition.
    pub(super) const fn abort_target(&self) -> JobState {
        if matches!(self.state, JobState::Ready) || !self.has_cleanup {
            JobState::Cancelled
        } else {
            JobState::CleanupReady
        }
    }
}

/// A tree invariant that a tree's structure breaks.
#[cfg(any(test, debug_assertions))]
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum Violation {
    /// The structure holds a terminal job.
    #[error("I5: the terminal job {job_id:?} is in its tree's structure as {state}")]
    TerminalJobInTree { job_id: JobId, state: JobState },

    /// The root has a parent.
    #[error("I1: the root {0:?} has a parent")]
    RootWithParent(JobId),

    /// A job other than the root has no parent.
    #[error("I1: the non-root job {0:?} has no parent")]
    NonRootWithoutParent(JobId),

    /// A job other than the root is [`JobState::Ready`].
    #[error("I1: the non-root job {0:?} is ready")]
    ReadyNonRoot(JobId),

    /// A job's ID is not above its parent's.
    #[error("I1: the job {job_id:?} has an ID not above its parent {parent_id:?}'s")]
    ChildIdNotAboveParent { job_id: JobId, parent_id: JobId },

    /// A job blocks on its children and counts no active child.
    #[error("I2: the job {0:?} blocks on its children without an active child")]
    BlockingJobWithoutActiveChild(JobId),
}

#[cfg(test)]
mod tests {
    use spider_core::job::JobState;
    use spider_core::types::id::JobId;

    use crate::cache::error::InternalError;
    use crate::cache::error::StaleStateError;
    use crate::cache::tree::ChildSetSummary;
    use crate::cache::tree::Lane;
    use crate::cache::tree::Plan;
    use crate::cache::tree::PlannedRegistration;
    use crate::cache::tree::Publication;
    use crate::cache::tree::RoundOutcome;
    use crate::cache::tree::TreeEvent;
    use crate::cache::tree::TreeEventKind;
    use crate::cache::tree::TreeEventOutcome;
    use crate::cache::tree::TreeNode;
    use crate::cache::tree::TreeStructure;
    use crate::cache::tree::Violation;
    use crate::cache::tree::test_utils::ROOT_ID;
    use crate::cache::tree::test_utils::failure;
    use crate::cache::tree::test_utils::registration;
    use crate::cache::tree::test_utils::root_event;
    use crate::db::JobStateUpdate;
    use crate::limits::MAX_CHILDREN_PER_JOB;
    use crate::limits::MAX_ERROR_MESSAGE_CHARS;
    use crate::limits::MAX_EVENTS_PER_ROUND;
    use crate::limits::MAX_JOB_CHILD_BYTES;
    use crate::state::test_utils::create_test_prepared_children;

    const PLAIN_NODE: TreeNode = TreeNode {
        state: JobState::Ready,
        parent: None,
        has_commit: false,
        has_cleanup: false,
        active_children: 0,
    };

    const CHILD_NODE: TreeNode = TreeNode {
        state: JobState::Running,
        parent: Some(ROOT_ID),
        ..PLAIN_NODE
    };

    /// # Returns
    ///
    /// The structure of a tree whose root, which has a cleanup task, blocks on its children of the
    /// IDs `child_ids`, each a [`CHILD_NODE`].
    ///
    /// # Panics
    ///
    /// Panics if `child_ids` holds the root's ID or an ID twice.
    fn tree_with_children(child_ids: impl IntoIterator<Item = u64>) -> TreeStructure {
        let mut structure = TreeStructure::new_root(
            ROOT_ID,
            TreeNode {
                state: JobState::BlockingOnChildren,
                has_cleanup: true,
                ..PLAIN_NODE
            },
        );
        structure
            .insert_children(
                ROOT_ID,
                child_ids
                    .into_iter()
                    .map(|child_id| (JobId::from(child_id), CHILD_NODE))
                    .collect(),
            )
            .expect("the root's children should be inserted");
        structure
    }

    /// # Returns
    ///
    /// An event of `kind` of the job `job_id`.
    const fn event(job_id: u64, kind: TreeEventKind) -> TreeEvent {
        TreeEvent {
            job_id: JobId::from(job_id),
            kind,
        }
    }

    /// Plans a round of events of `kinds` of the root of a tree that holds only `node` as its root.
    ///
    /// # Returns
    ///
    /// The round's plan.
    ///
    /// # Panics
    ///
    /// Panics if the round would change the root twice.
    fn plan_root_events(node: TreeNode, kinds: impl IntoIterator<Item = TreeEventKind>) -> Plan {
        let events: Vec<TreeEvent> = kinds.into_iter().map(root_event).collect();
        TreeStructure::new_root(ROOT_ID, node)
            .plan_events(&events)
            .expect("the round should change each job at most once")
    }

    /// Plans a round of one event of `kind` of the root of a tree that holds only `node` as its
    /// root.
    ///
    /// # Returns
    ///
    /// A pair, containing:
    ///
    /// * The round's plan, without its outcomes.
    /// * The outcome of the event.
    ///
    /// # Panics
    ///
    /// Panics if the round does not plan its event.
    fn plan_root_event(node: TreeNode, kind: TreeEventKind) -> (Plan, TreeEventOutcome) {
        let mut plan = plan_root_events(node, [kind]);
        let outcomes = std::mem::take(&mut plan.outcomes);
        let [RoundOutcome::Planned(outcome)] =
            <[_; 1]>::try_from(outcomes).expect("a round of one event should have one outcome")
        else {
            panic!("a round of one event should plan the event");
        };
        (plan, outcome)
    }

    #[test]
    fn an_event_of_a_job_not_in_the_tree_is_untracked() {
        let plan = TreeStructure::new_root(ROOT_ID, PLAIN_NODE)
            .plan_events(&[TreeEvent {
                job_id: JobId::from(2),
                kind: TreeEventKind::CommitDone,
            }])
            .expect("the round should change each job at most once");
        assert!(
            matches!(
                plan.outcomes.as_slice(),
                [RoundOutcome::Planned(TreeEventOutcome::Untracked)]
            ),
            "an event of a job not in the tree gave {:?}",
            plan.outcomes
        );
        assert_eq!(plan.updates, []);
    }

    #[test]
    fn an_event_that_its_jobs_state_does_not_accept_plans_nothing() {
        let (plan, outcome) = plan_root_event(
            TreeNode {
                state: JobState::CleanupReady,
                has_cleanup: true,
                ..PLAIN_NODE
            },
            TreeEventKind::CancelRoot,
        );
        assert!(
            matches!(
                outcome,
                TreeEventOutcome::Stale(StaleStateError::JobCancellationAlreadyRequested)
            ),
            "a cancel of a cleanup-ready root gave {outcome:?}"
        );
        assert_eq!(plan.updates, []);
        assert_eq!(plan.publications, []);
    }

    #[test]
    fn start_moves_a_ready_root_to_running_and_publishes_its_ready_tasks() {
        let (plan, outcome) = plan_root_event(
            PLAIN_NODE,
            TreeEventKind::Start {
                ready_task_indices: vec![0, 2],
            },
        );
        assert!(
            matches!(outcome, TreeEventOutcome::Applied(JobState::Running)),
            "a start gave {outcome:?}"
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
    }

    #[test]
    fn regular_tasks_done_stores_the_outputs_and_commits_only_a_job_with_a_commit_task() {
        let outputs = vec![vec![1u8, 2, 3]];
        for (has_commit, to, expected_publications) in [
            (
                true,
                JobState::CommitReady,
                vec![Publication::Commit(ROOT_ID)],
            ),
            (false, JobState::Succeeded, Vec::new()),
        ] {
            let (plan, outcome) = plan_root_event(
                TreeNode {
                    state: JobState::Running,
                    has_commit,
                    ..PLAIN_NODE
                },
                TreeEventKind::RegularTasksDone {
                    outputs: outputs.clone(),
                    children: Vec::new(),
                },
            );
            assert!(
                matches!(outcome, TreeEventOutcome::Applied(state) if state == to),
                "the regular tasks being done with has_commit {has_commit} gave {outcome:?}"
            );
            assert_eq!(
                plan.updates,
                [JobStateUpdate {
                    outputs: Some(outputs.clone()),
                    ..JobStateUpdate::new(ROOT_ID, JobState::Running, to)
                }]
            );
            assert_eq!(plan.publications, expected_publications);
        }
    }

    #[tokio::test]
    async fn regular_tasks_done_with_children_blocks_the_job_and_registers_them() {
        let children = create_test_prepared_children(2).await;
        let summary = ChildSetSummary::of(&children);
        let mut plan = plan_root_events(
            TreeNode {
                state: JobState::Running,
                has_commit: true,
                ..PLAIN_NODE
            },
            [registration(children)],
        );
        assert!(
            matches!(
                plan.outcomes.as_slice(),
                [RoundOutcome::Planned(TreeEventOutcome::Applied(
                    JobState::BlockingOnChildren
                ))]
            ),
            "a registration gave {:?}",
            plan.outcomes
        );
        assert_eq!(
            plan.updates,
            [JobStateUpdate {
                outputs: Some(vec![vec![1]]),
                ..JobStateUpdate::new(ROOT_ID, JobState::Running, JobState::BlockingOnChildren)
            }]
        );
        assert_eq!(plan.publications, []);
        assert_eq!(
            plan.registration.take(),
            Some(PlannedRegistration {
                event_index: 0,
                parent_id: ROOT_ID,
                children: summary,
            })
        );
        assert_eq!(summary.count, 2);
    }

    #[tokio::test]
    async fn a_child_set_over_the_job_limits_fails_its_job() -> anyhow::Result<()> {
        let mut children = create_test_prepared_children(1).await;
        let child = children.first_mut().expect("one child was prepared");
        child.submission.compressed_serialized_job_inputs =
            vec![0; usize::try_from(MAX_JOB_CHILD_BYTES)? + 1];
        let bytes = ChildSetSummary::of(&children).bytes;
        let (plan, outcome) = plan_root_event(
            TreeNode {
                state: JobState::Running,
                ..PLAIN_NODE
            },
            registration(children),
        );
        assert!(
            matches!(outcome, TreeEventOutcome::Applied(JobState::Failed)),
            "a child set over the job limits gave {outcome:?}"
        );
        assert_eq!(
            plan.updates,
            [JobStateUpdate {
                error_message: Some(format!(
                    "child jobs (1 of {bytes} bytes) exceed the limits of {MAX_CHILDREN_PER_JOB} \
                     child jobs and {MAX_JOB_CHILD_BYTES} bytes per job"
                )),
                ..JobStateUpdate::new(ROOT_ID, JobState::Running, JobState::Failed)
            }]
        );
        assert_eq!(plan.registration, None);
        Ok(())
    }

    #[tokio::test]
    async fn a_stale_registration_is_answered_at_once_and_registers_nothing() {
        let events = vec![
            root_event(registration(create_test_prepared_children(1).await)),
            root_event(registration(create_test_prepared_children(1).await)),
            event(2, registration(create_test_prepared_children(1).await)),
        ];
        let plan = TreeStructure::new_root(
            ROOT_ID,
            TreeNode {
                state: JobState::CommitReady,
                has_commit: true,
                ..PLAIN_NODE
            },
        )
        .plan_events(&events)
        .expect("the round should change each job at most once");
        assert!(
            matches!(
                plan.outcomes.as_slice(),
                [
                    RoundOutcome::Planned(TreeEventOutcome::Stale(
                        StaleStateError::JobNoLongerRunning
                    )),
                    RoundOutcome::Planned(TreeEventOutcome::Stale(
                        StaleStateError::JobNoLongerRunning
                    )),
                    RoundOutcome::Planned(TreeEventOutcome::Untracked),
                ]
            ),
            "stale and untracked registrations gave {:?}",
            plan.outcomes
        );
        assert_eq!(plan.updates, []);
        assert_eq!(plan.registration, None);
    }

    #[tokio::test]
    async fn rounds_plan_cancels_then_failures_then_one_child_set_then_the_other_events() {
        let children = create_test_prepared_children(2).await;
        let summary = ChildSetSummary::of(&children);
        let events = vec![
            root_event(TreeEventKind::CancelRoot),
            event(2, failure(Lane::Regular, "boom")),
            event(3, registration(children)),
            event(4, registration(create_test_prepared_children(1).await)),
            event(5, registration(Vec::new())),
        ];
        let plan = tree_with_children(2..=5)
            .plan_events(&events)
            .expect("the round should change each job at most once");
        assert!(
            matches!(
                plan.outcomes.as_slice(),
                [
                    RoundOutcome::Planned(TreeEventOutcome::Applied(JobState::CleanupReady)),
                    RoundOutcome::Planned(TreeEventOutcome::Applied(JobState::Failed)),
                    RoundOutcome::Planned(TreeEventOutcome::Applied(JobState::BlockingOnChildren)),
                    RoundOutcome::Deferred,
                    RoundOutcome::Planned(TreeEventOutcome::Applied(JobState::Succeeded)),
                ]
            ),
            "a round with two child sets gave {:?}",
            plan.outcomes
        );
        assert_eq!(
            plan.updates
                .iter()
                .map(|update| (update.job_id.get(), update.to))
                .collect::<Vec<_>>(),
            [
                (ROOT_ID.get(), JobState::CleanupReady),
                (2, JobState::Failed),
                (3, JobState::BlockingOnChildren),
                (5, JobState::Succeeded),
            ]
        );
        assert_eq!(
            plan.registration,
            Some(PlannedRegistration {
                event_index: 2,
                parent_id: JobId::from(3),
                children: summary,
            })
        );

        let children = create_test_prepared_children(1).await;
        let summary = ChildSetSummary::of(&children);
        let events = vec![
            event(2, failure(Lane::Regular, "boom")),
            event(2, registration(create_test_prepared_children(1).await)),
            event(3, registration(children)),
        ];
        let plan = tree_with_children(2..=3)
            .plan_events(&events)
            .expect("the round should change each job at most once");
        assert!(
            matches!(
                plan.outcomes.as_slice(),
                [
                    RoundOutcome::Planned(TreeEventOutcome::Applied(JobState::Failed)),
                    RoundOutcome::Planned(TreeEventOutcome::Stale(
                        StaleStateError::JobAlreadyTerminated(JobState::Failed)
                    )),
                    RoundOutcome::Planned(TreeEventOutcome::Applied(JobState::BlockingOnChildren)),
                ]
            ),
            "a stale child set ahead of an applicable one gave {:?}",
            plan.outcomes
        );
        assert_eq!(
            plan.registration,
            Some(PlannedRegistration {
                event_index: 2,
                parent_id: JobId::from(3),
                children: summary,
            })
        );
    }

    #[test]
    fn commit_done_succeeds_a_commit_ready_job_without_storing_outputs() {
        let (plan, _) = plan_root_event(
            TreeNode {
                state: JobState::CommitReady,
                has_commit: true,
                ..PLAIN_NODE
            },
            TreeEventKind::CommitDone,
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
        let (plan, _) = plan_root_event(
            TreeNode {
                state: JobState::CleanupReady,
                has_cleanup: true,
                ..PLAIN_NODE
            },
            TreeEventKind::CleanupDone,
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
            let (plan, _) = plan_root_event(
                TreeNode {
                    state,
                    has_commit: true,
                    has_cleanup: true,
                    ..PLAIN_NODE
                },
                failure(lane, "boom"),
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
            let (plan, _) = plan_root_event(
                TreeNode {
                    state: JobState::Running,
                    ..PLAIN_NODE
                },
                failure(Lane::Regular, &"é".repeat(message_chars)),
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
                let (to, expected_publications) = if has_cleanup && state != JobState::Ready {
                    (JobState::CleanupReady, vec![Publication::Cleanup(ROOT_ID)])
                } else {
                    (JobState::Cancelled, Vec::new())
                };
                let (plan, _) = plan_root_event(
                    TreeNode {
                        state,
                        has_cleanup,
                        ..PLAIN_NODE
                    },
                    TreeEventKind::CancelRoot,
                );
                assert_eq!(
                    plan.updates,
                    [JobStateUpdate::new(ROOT_ID, state, to)],
                    "a cancel in {state} with has_cleanup {has_cleanup}"
                );
                assert_eq!(
                    plan.publications, expected_publications,
                    "a cancel in {state} with has_cleanup {has_cleanup}"
                );
            }
        }
    }

    #[test]
    fn two_failures_of_one_job_in_one_round_fail_it_once() {
        let plan = plan_root_events(
            TreeNode {
                state: JobState::Running,
                ..PLAIN_NODE
            },
            [
                failure(Lane::Regular, "first"),
                failure(Lane::Regular, "second"),
            ],
        );
        assert!(
            matches!(
                plan.outcomes.as_slice(),
                [
                    RoundOutcome::Planned(TreeEventOutcome::Applied(JobState::Failed)),
                    RoundOutcome::Planned(TreeEventOutcome::Stale(
                        StaleStateError::JobAlreadyTerminated(JobState::Failed)
                    )),
                ]
            ),
            "two failures in one round gave {:?}",
            plan.outcomes
        );
        assert_eq!(
            plan.updates,
            [JobStateUpdate {
                error_message: Some("first".to_owned()),
                ..JobStateUpdate::new(ROOT_ID, JobState::Running, JobState::Failed)
            }]
        );
    }

    #[test]
    fn a_cancel_and_a_failure_in_one_round_cancel_the_root_and_make_the_failure_stale() {
        let plan = plan_root_events(
            TreeNode {
                state: JobState::Running,
                has_cleanup: true,
                ..PLAIN_NODE
            },
            [TreeEventKind::CancelRoot, failure(Lane::Regular, "boom")],
        );
        assert!(
            matches!(
                plan.outcomes.as_slice(),
                [
                    RoundOutcome::Planned(TreeEventOutcome::Applied(JobState::CleanupReady)),
                    RoundOutcome::Planned(TreeEventOutcome::Stale(
                        StaleStateError::JobNoLongerRunning
                    )),
                ]
            ),
            "a cancel and a failure in one round gave {:?}",
            plan.outcomes
        );
        assert_eq!(
            plan.updates,
            [JobStateUpdate::new(
                ROOT_ID,
                JobState::Running,
                JobState::CleanupReady
            )]
        );
        assert_eq!(plan.publications, [Publication::Cleanup(ROOT_ID)]);
    }

    #[test]
    fn a_round_is_planned_in_the_given_order_however_many_events_it_has() {
        let stale_commits =
            std::iter::repeat_with(|| TreeEventKind::CommitDone).take(MAX_EVENTS_PER_ROUND);
        let plan = plan_root_events(
            TreeNode {
                state: JobState::Running,
                has_cleanup: true,
                ..PLAIN_NODE
            },
            [failure(Lane::Regular, "boom"), TreeEventKind::CancelRoot]
                .into_iter()
                .chain(stale_commits),
        );
        let (planned, stale) = plan.outcomes.split_at(2);
        assert!(
            matches!(
                planned,
                [
                    RoundOutcome::Planned(TreeEventOutcome::Applied(JobState::Failed)),
                    RoundOutcome::Planned(TreeEventOutcome::Stale(
                        StaleStateError::JobAlreadyTerminated(JobState::Failed)
                    )),
                ]
            ),
            "a failure given before a cancel gave {planned:?}"
        );
        assert_eq!(stale.len(), MAX_EVENTS_PER_ROUND);
        assert!(
            stale.iter().all(|outcome| matches!(
                outcome,
                RoundOutcome::Planned(TreeEventOutcome::Stale(
                    StaleStateError::JobAlreadyTerminated(JobState::Failed)
                ))
            )),
            "every event after the round's first two should be planned stale"
        );
    }

    #[test]
    fn a_round_of_stale_events_writes_nothing() {
        let plan = plan_root_events(
            TreeNode {
                state: JobState::CleanupReady,
                has_cleanup: true,
                ..PLAIN_NODE
            },
            [
                TreeEventKind::CancelRoot,
                failure(Lane::Regular, "boom"),
                TreeEventKind::CommitDone,
            ],
        );
        assert!(
            matches!(
                plan.outcomes.as_slice(),
                [
                    RoundOutcome::Planned(TreeEventOutcome::Stale(
                        StaleStateError::JobCancellationAlreadyRequested
                    )),
                    RoundOutcome::Planned(TreeEventOutcome::Stale(
                        StaleStateError::JobNoLongerRunning
                    )),
                    RoundOutcome::Planned(TreeEventOutcome::Stale(
                        StaleStateError::JobNoLongerCommitReady
                    )),
                ]
            ),
            "a round of stale events gave {:?}",
            plan.outcomes
        );
        assert_eq!(plan.updates, []);
        assert_eq!(plan.publications, []);
    }

    #[test]
    fn an_event_that_would_change_a_job_twice_in_one_round_is_an_internal_error() {
        let events: Vec<TreeEvent> = [TreeEventKind::CancelRoot, TreeEventKind::CleanupDone]
            .into_iter()
            .map(root_event)
            .collect();
        let result = TreeStructure::new_root(
            ROOT_ID,
            TreeNode {
                state: JobState::Running,
                has_cleanup: true,
                ..PLAIN_NODE
            },
        )
        .plan_events(&events);
        assert!(
            matches!(
                result,
                Err(InternalError::JobChangedTwiceInRound(job_id)) if job_id == ROOT_ID
            ),
            "a cleanup after a cancel in one round gave {result:?}"
        );
    }

    #[test]
    fn applying_a_state_sets_the_node_state_and_a_terminal_state_drops_the_node() {
        let mut structure = TreeStructure::new_root(ROOT_ID, PLAIN_NODE);

        structure.apply_state(ROOT_ID, JobState::Running);
        assert_eq!(
            structure.node(ROOT_ID).map(|node| node.state),
            Some(JobState::Running)
        );
        assert!(!structure.is_quiescent());

        structure.apply_state(ROOT_ID, JobState::Succeeded);
        assert!(structure.is_quiescent());
    }

    #[test]
    fn inserting_children_sets_the_parents_counter_and_refuses_a_missing_parent_or_a_known_child() {
        const GRANDCHILD: TreeNode = TreeNode {
            parent: Some(JobId::from(2)),
            ..CHILD_NODE
        };
        let mut structure = tree_with_children([2, 3]);
        assert_eq!(
            structure.node(ROOT_ID).map(|node| node.active_children),
            Some(2)
        );
        assert_eq!(
            structure.node(JobId::from(3)).map(|node| node.parent),
            Some(Some(ROOT_ID))
        );

        for (parent_id, children, expected_error) in [
            (9, vec![10], InternalError::JobNotFound(JobId::from(9))),
            (
                2,
                vec![4, 3],
                InternalError::JobAlreadyExists(JobId::from(3)),
            ),
            (
                2,
                vec![5, 5],
                InternalError::JobAlreadyExists(JobId::from(5)),
            ),
        ] {
            let result = structure.insert_children(
                JobId::from(parent_id),
                children
                    .iter()
                    .map(|child_id| (JobId::from(*child_id), GRANDCHILD))
                    .collect(),
            );
            assert_eq!(
                result.map_err(|error| error.to_string()),
                Err(expected_error.to_string()),
                "inserting {children:?} under {parent_id}"
            );
        }
        assert_eq!(
            structure.sorted_job_ids(),
            [ROOT_ID, JobId::from(2), JobId::from(3)]
        );
        assert_eq!(
            structure
                .node(JobId::from(2))
                .map(|node| node.active_children),
            Some(0)
        );
    }

    #[test]
    fn check_invariants_reports_each_broken_invariant() {
        const BLOCKING_ROOT: TreeNode = TreeNode {
            state: JobState::BlockingOnChildren,
            ..PLAIN_NODE
        };
        let tree_with_child = |child_id: u64, child: TreeNode| {
            let mut structure = TreeStructure::new_root(ROOT_ID, BLOCKING_ROOT);
            structure
                .insert_children(ROOT_ID, vec![(JobId::from(child_id), child)])
                .expect("the root's child should be inserted");
            structure
        };
        for (description, structure, expected) in [
            (
                "a running root",
                TreeStructure::new_root(
                    ROOT_ID,
                    TreeNode {
                        state: JobState::Running,
                        ..PLAIN_NODE
                    },
                ),
                Ok(()),
            ),
            ("a tree with children", tree_with_children(2..=3), Ok(())),
            (
                "a failed root",
                TreeStructure::new_root(
                    ROOT_ID,
                    TreeNode {
                        state: JobState::Failed,
                        ..PLAIN_NODE
                    },
                ),
                Err(Violation::TerminalJobInTree {
                    job_id: ROOT_ID,
                    state: JobState::Failed,
                }),
            ),
            (
                "a root with a parent",
                TreeStructure::new_root(ROOT_ID, CHILD_NODE),
                Err(Violation::RootWithParent(ROOT_ID)),
            ),
            (
                "a child without a parent",
                tree_with_child(
                    2,
                    TreeNode {
                        parent: None,
                        ..CHILD_NODE
                    },
                ),
                Err(Violation::NonRootWithoutParent(JobId::from(2))),
            ),
            (
                "a ready child",
                tree_with_child(
                    2,
                    TreeNode {
                        state: JobState::Ready,
                        ..CHILD_NODE
                    },
                ),
                Err(Violation::ReadyNonRoot(JobId::from(2))),
            ),
            (
                "a child with an ID below its parent's",
                tree_with_child(0, CHILD_NODE),
                Err(Violation::ChildIdNotAboveParent {
                    job_id: JobId::from(0),
                    parent_id: ROOT_ID,
                }),
            ),
            (
                "a root blocking without a child",
                TreeStructure::new_root(ROOT_ID, BLOCKING_ROOT),
                Err(Violation::BlockingJobWithoutActiveChild(ROOT_ID)),
            ),
        ] {
            assert_eq!(structure.check_invariants(), expected, "{description}");
        }
    }
}
