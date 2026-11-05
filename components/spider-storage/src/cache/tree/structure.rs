//! The structure of a job tree and the planning of its rounds of events.

use std::collections::HashMap;

use spider_core::job::JobState;
use spider_core::types::id::JobId;

use crate::cache::tree::Plan;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::planner::Overlay;

/// The structure of a tree: its non-terminal jobs and their states.
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

    /// Plans a round of `events` against the tree's structure, in their order, each against the
    /// result of the round's earlier events. An event that the plan of the round's earlier events
    /// does not admit is deferred to a later round.
    ///
    /// # Type Parameters
    ///
    /// * `'event_lifetime` - The lifetime of the borrowed events of the round.
    ///
    /// # Returns
    ///
    /// The plan of the round, with one outcome per event of `events`, in order.
    #[must_use]
    pub fn plan_events<'event_lifetime>(
        &self,
        events: impl IntoIterator<Item = &'event_lifetime TreeEvent>,
    ) -> Plan {
        let mut overlay = Overlay::new(self);
        let mut plan = Plan::default();
        for event in events {
            let event_plan = overlay.plan_event(event);
            if plan.admits(&event_plan) {
                overlay.merge(&event_plan);
                plan.merge(event_plan);
            } else {
                plan.defer();
            }
        }
        plan
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

    /// Checks the tree invariants that the structure can break, job by job in ascending ID order.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Violation::TerminalJobInTree`] if the structure holds a terminal job.
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
            if node.state == JobState::BlockingOnChildren {
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
    pub has_commit: bool,
    pub has_cleanup: bool,
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

    /// A job blocks on its children, none of which is active.
    #[error("I2: the job {0:?} blocks on its children without an active child")]
    BlockingJobWithoutActiveChild(JobId),
}

#[cfg(test)]
mod tests {
    use spider_core::job::JobState;
    use spider_core::types::id::JobId;

    use crate::cache::error::StaleStateError;
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
    use crate::cache::tree::test_utils::ROOT_ID;
    use crate::cache::tree::test_utils::failure;
    use crate::cache::tree::test_utils::root_event;
    use crate::db::JobStateUpdate;
    use crate::limits::MAX_ERROR_MESSAGE_CHARS;
    use crate::limits::MAX_EVENTS_PER_ROUND;

    const PLAIN_NODE: TreeNode = TreeNode {
        state: JobState::Ready,
        has_commit: false,
        has_cleanup: false,
    };

    /// Plans a round of events of `kinds` of the root of a tree that holds only `node` as its root.
    ///
    /// # Returns
    ///
    /// The round's plan.
    fn plan_root_events(node: TreeNode, kinds: impl IntoIterator<Item = TreeEventKind>) -> Plan {
        let events: Vec<TreeEvent> = kinds.into_iter().map(root_event).collect();
        TreeStructure::new_root(ROOT_ID, node).plan_events(&events)
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
        let plan = TreeStructure::new_root(ROOT_ID, PLAIN_NODE).plan_events(&[TreeEvent {
            job_id: JobId::from(2),
            kind: TreeEventKind::CommitDone,
        }]);
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
    fn an_event_that_would_change_a_job_again_is_deferred_but_a_stale_one_is_answered() {
        let plan = plan_root_events(
            TreeNode {
                state: JobState::Running,
                has_cleanup: true,
                ..PLAIN_NODE
            },
            [
                TreeEventKind::CancelRoot,
                TreeEventKind::CleanupDone,
                TreeEventKind::CommitDone,
            ],
        );
        assert!(
            matches!(
                plan.outcomes.as_slice(),
                [
                    RoundOutcome::Planned(TreeEventOutcome::Applied(JobState::CleanupReady)),
                    RoundOutcome::Deferred,
                    RoundOutcome::Planned(TreeEventOutcome::Stale(
                        StaleStateError::JobNoLongerCommitReady
                    )),
                ]
            ),
            "a cleanup after a cancel in one round gave {:?}",
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
    fn check_invariants_reports_a_terminal_job_and_a_job_blocking_without_an_active_child() {
        for (state, expected) in [
            (JobState::Running, Ok(())),
            (
                JobState::Failed,
                Err(Violation::TerminalJobInTree {
                    job_id: ROOT_ID,
                    state: JobState::Failed,
                }),
            ),
            (
                JobState::BlockingOnChildren,
                Err(Violation::BlockingJobWithoutActiveChild(ROOT_ID)),
            ),
        ] {
            let structure = TreeStructure::new_root(
                ROOT_ID,
                TreeNode {
                    state,
                    ..PLAIN_NODE
                },
            );
            assert_eq!(structure.check_invariants(), expected, "a root in {state}");
        }
    }
}
