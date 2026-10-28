//! The structure of a job tree and the planning of its events.

use std::collections::HashMap;

use spider_core::job::JobState;
use spider_core::types::id::JobId;

use crate::cache::tree::Plan;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::TreeEventKind;

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

    /// Plans one event against the tree's structure and the states of its jobs.
    ///
    /// # Returns
    ///
    /// The plan of `event`:
    ///
    /// * [`Plan::Untracked`] if the event's job is not in the tree.
    /// * [`Plan::Stale`] if the job's state does not accept the event (see
    ///   [`TreeEventKind::ensure_accepts`]).
    /// * Otherwise, a [`Plan::Apply`] with the event's update and publications:
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
    #[must_use]
    pub fn plan_event(&self, event: TreeEvent) -> Plan {
        let job_id = event.job_id;
        let Some(node) = self.nodes.get(&job_id) else {
            return Plan::Untracked;
        };
        let from = node.state;
        if let Err(error) = event.kind.ensure_accepts(from) {
            return Plan::Stale(error);
        }
        match event.kind {
            TreeEventKind::Start { ready_task_indices } => {
                Plan::start_root(job_id, ready_task_indices)
            }
            TreeEventKind::RegularTasksDone { outputs } => {
                Plan::complete_regular_tasks(job_id, node, outputs)
            }
            TreeEventKind::CommitDone => Plan::complete_commit(job_id),
            TreeEventKind::CleanupDone => Plan::complete_cleanup(job_id),
            TreeEventKind::TerminalFailure { message, .. } => {
                Plan::fail_job(job_id, from, &message)
            }
            TreeEventKind::CancelRoot => Plan::cancel_root(job_id, node),
        }
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
    /// The state that a cancel moves the job from its state to: [`JobState::Cancelled`] if the job
    /// is [`JobState::Ready`] or has no cleanup task, otherwise [`JobState::CleanupReady`]. A
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

#[cfg(test)]
mod tests {
    use spider_core::job::JobState;
    use spider_core::types::id::JobId;

    use crate::cache::error::StaleStateError;
    use crate::cache::tree::Lane;
    use crate::cache::tree::Plan;
    use crate::cache::tree::Publication;
    use crate::cache::tree::TreeEvent;
    use crate::cache::tree::TreeEventKind;
    use crate::cache::tree::TreeNode;
    use crate::cache::tree::TreeStructure;
    use crate::cache::tree::test_utils::ROOT_ID;
    use crate::cache::tree::test_utils::failure;
    use crate::cache::tree::test_utils::root_event;
    use crate::db::JobStateUpdate;
    use crate::limits::MAX_ERROR_MESSAGE_CHARS;

    const PLAIN_NODE: TreeNode = TreeNode {
        state: JobState::Ready,
        has_commit: false,
        has_cleanup: false,
    };

    /// Plans an event of the root of a tree that holds only `node` as its root.
    ///
    /// # Returns
    ///
    /// The plan.
    fn plan_root_event(node: TreeNode, kind: TreeEventKind) -> Plan {
        TreeStructure::new_root(ROOT_ID, node).plan_event(root_event(kind))
    }

    /// # Returns
    ///
    /// The updates, the publications and the new state of `plan`.
    ///
    /// # Panics
    ///
    /// Panics if `plan` is not [`Plan::Apply`].
    fn unwrap_apply(plan: Plan) -> (Vec<JobStateUpdate>, Vec<Publication>, JobState) {
        match plan {
            Plan::Apply {
                updates,
                publications,
                new_state,
            } => (updates, publications, new_state),
            plan => panic!("the plan should apply its event, got {plan:?}"),
        }
    }

    #[test]
    fn an_event_of_a_job_not_in_the_tree_is_untracked() {
        let plan = TreeStructure::new_root(ROOT_ID, PLAIN_NODE).plan_event(TreeEvent {
            job_id: JobId::from(2),
            kind: TreeEventKind::CommitDone,
        });
        assert!(
            matches!(plan, Plan::Untracked),
            "an event of a job not in the tree gave {plan:?}"
        );
    }

    #[test]
    fn an_event_that_its_jobs_state_does_not_accept_plans_nothing() {
        let plan = plan_root_event(
            TreeNode {
                state: JobState::CleanupReady,
                has_cleanup: true,
                ..PLAIN_NODE
            },
            TreeEventKind::CancelRoot,
        );
        assert!(
            matches!(
                plan,
                Plan::Stale(StaleStateError::JobCancellationAlreadyRequested)
            ),
            "a cancel of a cleanup-ready root gave {plan:?}"
        );
    }

    #[test]
    fn start_moves_a_ready_root_to_running_and_publishes_its_ready_tasks() {
        let (updates, publications, new_state) = unwrap_apply(plan_root_event(
            PLAIN_NODE,
            TreeEventKind::Start {
                ready_task_indices: vec![0, 2],
            },
        ));
        assert_eq!(
            updates,
            [JobStateUpdate::new(
                ROOT_ID,
                JobState::Ready,
                JobState::Running
            )]
        );
        assert_eq!(
            publications,
            [Publication::Ready {
                job_id: ROOT_ID,
                task_indices: vec![0, 2],
            }]
        );
        assert_eq!(new_state, JobState::Running);
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
            let (updates, publications, _) = unwrap_apply(plan_root_event(
                TreeNode {
                    state: JobState::Running,
                    has_commit,
                    ..PLAIN_NODE
                },
                TreeEventKind::RegularTasksDone {
                    outputs: outputs.clone(),
                },
            ));
            assert_eq!(
                updates,
                [JobStateUpdate {
                    outputs: Some(outputs.clone()),
                    ..JobStateUpdate::new(ROOT_ID, JobState::Running, to)
                }]
            );
            assert_eq!(publications, expected_publications);
        }
    }

    #[test]
    fn commit_done_succeeds_a_commit_ready_job_without_storing_outputs() {
        let (updates, publications, _) = unwrap_apply(plan_root_event(
            TreeNode {
                state: JobState::CommitReady,
                has_commit: true,
                ..PLAIN_NODE
            },
            TreeEventKind::CommitDone,
        ));
        assert_eq!(
            updates,
            [JobStateUpdate::new(
                ROOT_ID,
                JobState::CommitReady,
                JobState::Succeeded
            )]
        );
        assert_eq!(publications, []);
    }

    #[test]
    fn cleanup_done_cancels_a_cleanup_ready_job() {
        let (updates, publications, _) = unwrap_apply(plan_root_event(
            TreeNode {
                state: JobState::CleanupReady,
                has_cleanup: true,
                ..PLAIN_NODE
            },
            TreeEventKind::CleanupDone,
        ));
        assert_eq!(
            updates,
            [JobStateUpdate::new(
                ROOT_ID,
                JobState::CleanupReady,
                JobState::Cancelled
            )]
        );
        assert_eq!(publications, []);
    }

    #[test]
    fn a_terminal_failure_fails_the_job_from_its_lanes_state_with_its_message() {
        for (lane, state) in [
            (Lane::Regular, JobState::Running),
            (Lane::Commit, JobState::CommitReady),
            (Lane::Cleanup, JobState::CleanupReady),
        ] {
            let (updates, publications, _) = unwrap_apply(plan_root_event(
                TreeNode {
                    state,
                    has_commit: true,
                    has_cleanup: true,
                },
                failure(lane, "boom"),
            ));
            assert_eq!(
                updates,
                [JobStateUpdate {
                    error_message: Some("boom".to_owned()),
                    ..JobStateUpdate::new(ROOT_ID, state, JobState::Failed)
                }]
            );
            assert_eq!(publications, []);
        }
    }

    #[test]
    fn a_failure_message_is_cut_to_the_maximum_number_of_characters() {
        for (message_chars, stored_chars) in [
            (MAX_ERROR_MESSAGE_CHARS + 1, MAX_ERROR_MESSAGE_CHARS),
            (MAX_ERROR_MESSAGE_CHARS, MAX_ERROR_MESSAGE_CHARS),
        ] {
            let (updates, ..) = unwrap_apply(plan_root_event(
                TreeNode {
                    state: JobState::Running,
                    ..PLAIN_NODE
                },
                failure(Lane::Regular, &"é".repeat(message_chars)),
            ));
            let stored_message = updates
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
                let (updates, publications, _) = unwrap_apply(plan_root_event(
                    TreeNode {
                        state,
                        has_cleanup,
                        ..PLAIN_NODE
                    },
                    TreeEventKind::CancelRoot,
                ));
                assert_eq!(
                    updates,
                    [JobStateUpdate::new(ROOT_ID, state, to)],
                    "a cancel in {state} with has_cleanup {has_cleanup}"
                );
                assert_eq!(
                    publications, expected_publications,
                    "a cancel in {state} with has_cleanup {has_cleanup}"
                );
            }
        }
    }
}
