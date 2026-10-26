//! The structure of a job tree and the planning of its events.

use std::collections::HashMap;

use spider_core::job::JobState;
use spider_core::types::id::JobId;

use crate::cache::tree::Plan;
use crate::cache::tree::PlannerInput;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::TreeEventKind;
use crate::cache::tree::TreeOutcome;

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
    ///     [`MAX_ERROR_MESSAGE_CHARS`](crate::limits::MAX_ERROR_MESSAGE_CHARS) characters.
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
    pub(super) const fn abort_target(&self, state: JobState) -> JobState {
        if matches!(state, JobState::Ready) || !self.has_cleanup {
            JobState::Cancelled
        } else {
            JobState::CleanupReady
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use spider_core::job::JobState;
    use spider_core::types::id::JobId;

    use crate::cache::error::StaleStateError;
    use crate::cache::tree::Lane;
    use crate::cache::tree::Plan;
    use crate::cache::tree::PlannerInput;
    use crate::cache::tree::Publication;
    use crate::cache::tree::TreeEvent;
    use crate::cache::tree::TreeEventKind;
    use crate::cache::tree::TreeNode;
    use crate::cache::tree::TreeOutcome;
    use crate::cache::tree::TreeStructure;
    use crate::cache::tree::test_utils::ROOT_ID;
    use crate::cache::tree::test_utils::failure;
    use crate::cache::tree::test_utils::root_event;
    use crate::db::JobStateUpdate;
    use crate::limits::MAX_ERROR_MESSAGE_CHARS;

    const PLAIN_NODE: TreeNode = TreeNode {
        has_commit: false,
        has_cleanup: false,
    };

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
}
