//! Completions of jobs and job trees racing each other, on `MariaDB`.

use std::collections::HashMap;

use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_storage::test_hooks::ArmedHook;
use spider_storage::test_hooks::HookAction;
use spider_storage::test_hooks::HookCount;
use spider_storage::test_hooks::HookFilter;
use spider_storage::test_hooks::HookPoint;

use crate::runtime_infra::JobResult;
use crate::runtime_infra::JobShape;
use crate::runtime_infra::Race;
use crate::runtime_infra::TestEnv;
use crate::runtime_infra::TreeProbes;
use crate::runtime_infra::join;
use crate::runtime_infra::reached;
use crate::runtime_infra::task_outputs;
use crate::runtime_infra::value;

/// How the last reports of sibling jobs land in rounds.
#[derive(Clone, Copy, Debug)]
enum SiblingRounds {
    /// Every report in the round of the first one.
    One,

    /// Each report in its own round, one after the other.
    Each,

    /// Every report at once, in whatever rounds the committer forms.
    Free,
}

/// Where the job under test is in its tree.
#[derive(Clone, Copy, Debug)]
enum Position {
    /// The job is the root.
    Root,

    /// The job is the only child of a one-task root.
    UnderRoot,
}

/// A tree whose parent, with or without a commit task, blocks on one-task children.
struct SiblingTree {
    root_id: JobId,
    parent_id: JobId,
    parent_value: Vec<u8>,
    child_ids: Vec<JobId>,
    child_values: Vec<Vec<u8>>,
    position: Position,
    parent_has_commit: bool,
}

impl SiblingTree {
    /// Starts a tree whose parent is at `position`, has a commit task if `parent_has_commit`, and
    /// blocks on `num_children` children.
    ///
    /// # Returns
    ///
    /// The tree.
    async fn start(
        env: &TestEnv,
        num_children: usize,
        position: Position,
        parent_has_commit: bool,
    ) -> Self {
        let parent_shape = JobShape {
            with_commit: parent_has_commit,
            ..JobShape::ONE_TASK
        };
        let (root_id, parent_id) = env.start_job_at(position, parent_shape).await;
        let parent_value = value(0xb1, 0);
        let child_ids = env
            .register_children(
                parent_id,
                &parent_value,
                &vec![JobShape::ONE_TASK; num_children],
            )
            .await;
        Self {
            root_id,
            parent_id,
            parent_value,
            child_ids,
            child_values: (0..num_children).map(|index| value(0xc0, index)).collect(),
            position,
            parent_has_commit,
        }
    }

    /// # Returns
    ///
    /// The state that the parent advances to when its last child succeeds.
    const fn advanced_parent_state(&self) -> JobState {
        if self.parent_has_commit {
            JobState::CommitReady
        } else {
            JobState::Succeeded
        }
    }
}

/// A tree `R → {P → children, Q}`, where `P` has a commit task, and every job has an instance of
/// its regular task registered except `R` and `P`, whose tasks have succeeded.
struct CommitParentTree {
    root_id: JobId,
    root_value: Vec<u8>,
    parent_id: JobId,
    parent_value: Vec<u8>,
    sibling_id: JobId,
    sibling_instance_id: TaskInstanceId,
    child_ids: Vec<JobId>,
    child_values: Vec<Vec<u8>>,
    child_instance_ids: Vec<TaskInstanceId>,
    root_has_commit: bool,
}

impl CommitParentTree {
    const NUM_CHILDREN: usize = 3;

    /// Starts the tree, with a commit task on `R` if `root_has_commit`.
    ///
    /// # Returns
    ///
    /// The tree.
    async fn start(env: &TestEnv, root_has_commit: bool) -> Self {
        let root_value = value(0xf0, 0);
        let parent_value = value(0xf1, 0);
        let root_id = env
            .start_root(JobShape {
                with_commit: root_has_commit,
                ..JobShape::ONE_TASK
            })
            .await;
        let [parent_id, sibling_id] = env
            .register_children(
                root_id,
                &root_value,
                &[
                    JobShape {
                        with_commit: true,
                        ..JobShape::ONE_TASK
                    },
                    JobShape::ONE_TASK,
                ],
            )
            .await[..]
        else {
            panic!("the root should have two children");
        };
        let child_ids = env
            .register_children(
                parent_id,
                &parent_value,
                &[JobShape::ONE_TASK; Self::NUM_CHILDREN],
            )
            .await;
        let mut child_instance_ids = Vec::with_capacity(Self::NUM_CHILDREN);
        for child_id in &child_ids {
            child_instance_ids.push(env.create_instance(*child_id, TaskId::Index(0)).await);
        }
        Self {
            root_id,
            root_value,
            parent_id,
            parent_value,
            sibling_id,
            sibling_instance_id: env.create_instance(sibling_id, TaskId::Index(0)).await,
            child_ids,
            child_values: (0..Self::NUM_CHILDREN)
                .map(|index| value(0xf3, index))
                .collect(),
            child_instance_ids,
            root_has_commit,
        }
    }

    /// # Returns
    ///
    /// The output value of `Q`'s task.
    fn sibling_value() -> Vec<u8> {
        value(0xf2, 0)
    }

    /// # Returns
    ///
    /// A future that reports the success of `Q`'s task.
    fn sibling_report(&self, env: &TestEnv) -> impl Future<Output = JobResult> + Send + use<> {
        env.task_success(
            self.sibling_id,
            0,
            self.sibling_instance_id,
            &Self::sibling_value(),
            Vec::new(),
        )
    }

    /// # Returns
    ///
    /// A future that reports the success of the task of `P`'s child `index`.
    fn child_report(
        &self,
        env: &TestEnv,
        index: usize,
    ) -> impl Future<Output = JobResult> + Send + use<> {
        env.task_success(
            self.child_ids[index],
            0,
            self.child_instance_ids[index],
            &self.child_values[index],
            Vec::new(),
        )
    }
}

/// When `Q` succeeds, relative to `P`'s children and `P`'s commit.
#[derive(Clone, Copy, Debug)]
enum SiblingTiming {
    /// Before `P`'s children.
    BeforeChildren,

    /// In the round of `P`'s last child.
    WithLastChild,

    /// After `P` is commit-ready, before its commit task reports.
    BeforeCommit,

    /// In the round of `P`'s commit success.
    WithCommit,

    /// While the round of `P`'s commit success is written but not applied.
    AfterCommit,
}

impl TestEnv {
    /// Starts a job of `shape` at `position`: a root, or the only child of a one-task root.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The ID of the tree's root.
    /// * The ID of the job.
    async fn start_job_at(&self, position: Position, shape: JobShape) -> (JobId, JobId) {
        match position {
            Position::Root => {
                let job_id = self.start_root(shape).await;
                (job_id, job_id)
            }
            Position::UnderRoot => {
                let root_id = self.start_root(JobShape::ONE_TASK).await;
                let [job_id] = self
                    .register_children(root_id, &value(0xb0, 0), &[shape])
                    .await[..]
                else {
                    panic!("the root should have one child");
                };
                (root_id, job_id)
            }
        }
    }

    /// Reports the successes of `tree`'s children as `rounds` says, and asserts that the parent
    /// advances exactly once, with every child's outputs written by then, and that the tree
    /// finishes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ServiceState::succeed_task_instance`]'s return values on failure.
    /// * Forwards [`Self::commit_sibling_parent`]'s return values on failure.
    ///
    /// [`ServiceState::succeed_task_instance`]:
    ///     spider_storage::state::ServiceState::succeed_task_instance
    async fn assert_siblings_advance_their_parent_once(
        &self,
        tree: &SiblingTree,
        rounds: SiblingRounds,
    ) -> anyhow::Result<()> {
        let num_children = tree.child_ids.len();
        let what = format!(
            "{num_children} children in {rounds:?} rounds, parent at {:?}, commit: {}",
            tree.position, tree.parent_has_commit
        );
        let mut reports = Vec::with_capacity(num_children);
        for (child_id, child_value) in tree.child_ids.iter().zip(&tree.child_values) {
            let task_instance_id = self.create_instance(*child_id, TaskId::Index(0)).await;
            reports.push(self.task_success(
                *child_id,
                0,
                task_instance_id,
                child_value,
                Vec::new(),
            ));
        }
        let probes = TreeProbes::arm(self, tree.root_id, &[tree.root_id, tree.parent_id]);
        let advance = self.pause(
            HookPoint::CommitterRoundAfterDbWrite,
            HookFilter::job(tree.parent_id),
        );
        let (results, ()) = tokio::join!(
            self.report_sibling_successes(tree, reports, rounds, &what),
            self.assert_children_written_at_advance(tree, &advance, &what)
        );
        for result in results {
            assert_eq!(result?, JobState::Succeeded, "{what}");
        }
        assert_eq!(
            probes.transitions(tree.parent_id),
            1,
            "{what}: the parent advances once"
        );
        match rounds {
            SiblingRounds::One => probes.assert_rounds(1, 1, &what),
            SiblingRounds::Each => probes.assert_rounds(num_children, num_children, &what),
            SiblingRounds::Free => {}
        }
        self.assert_after_siblings(tree, &probes, &what).await;
        if tree.parent_has_commit {
            self.commit_sibling_parent(tree, &probes, &what).await?;
        }
        self.assert_quiescent(tree.root_id).await;
        Ok(())
    }

    /// Submits `reports`, the successes of `tree`'s children in child order, as `rounds` says.
    /// Between two reports of [`SiblingRounds::Each`], asserts that the parent still blocks on its
    /// children.
    ///
    /// # Type Parameters
    ///
    /// * `ReportType` - The type of the futures that report the successes.
    ///
    /// # Returns
    ///
    /// The results of the reports, in child order.
    async fn report_sibling_successes<ReportType: Future<Output = JobResult> + Send + 'static>(
        &self,
        tree: &SiblingTree,
        mut reports: Vec<ReportType>,
        rounds: SiblingRounds,
        what: &str,
    ) -> Vec<JobResult> {
        let num_reports = reports.len();
        let mut report_join_handles = Vec::with_capacity(num_reports);
        match rounds {
            SiblingRounds::One => {
                let paused = self.pause(
                    HookPoint::CommitterBeforeReceive,
                    HookFilter::job(tree.root_id),
                );
                report_join_handles.push(tokio::spawn(reports.remove(0)));
                reached(&paused, 1).await;
                let submissions: Vec<_> = tree.child_ids[1..]
                    .iter()
                    .map(|child_id| {
                        self.observe(HookPoint::JobEventAfterSubmit, HookFilter::job(*child_id))
                    })
                    .collect();
                report_join_handles.extend(reports.into_iter().map(tokio::spawn));
                for submission in &submissions {
                    reached(submission, 1).await;
                }
                paused.release();
            }
            SiblingRounds::Each => {
                let mut results = Vec::with_capacity(num_reports);
                for (index, report) in reports.into_iter().enumerate() {
                    results.push(report.await);
                    if index + 1 < num_reports {
                        self.assert_states(
                            &HashMap::from([(tree.parent_id, JobState::BlockingOnChildren)]),
                            &format!("{what}: after {} reports", index + 1),
                        )
                        .await;
                    }
                }
                return results;
            }
            SiblingRounds::Free => {
                report_join_handles.extend(reports.into_iter().map(tokio::spawn));
            }
        }
        let mut results = Vec::with_capacity(num_reports);
        for report_join_handle in report_join_handles {
            results.push(join(report_join_handle).await);
        }
        results
    }

    /// Waits until the round that advances `tree`'s parent, paused by `advance`, has written its
    /// transitions, asserts that every child's row is `Succeeded` with its outputs and that the
    /// parent's row has advanced, then releases the round.
    async fn assert_children_written_at_advance(
        &self,
        tree: &SiblingTree,
        advance: &ArmedHook,
        what: &str,
    ) {
        reached(advance, 1).await;
        for (child_id, child_value) in tree.child_ids.iter().zip(&tree.child_values) {
            assert_eq!(
                self.row_state(*child_id).await,
                JobState::Succeeded,
                "{what}: child {child_id} when the parent advances"
            );
            assert_eq!(
                self.row_outputs(*child_id).await,
                Some(task_outputs([child_value])),
                "{what}: the outputs of child {child_id} when the parent advances"
            );
        }
        assert_eq!(
            self.row_state(tree.parent_id).await,
            tree.advanced_parent_state(),
            "{what}: the parent's row when it advances"
        );
        advance.release();
    }

    /// Asserts the states of `tree` after its children succeeded, the rounds that transition its
    /// root, and that the parent's own outputs are unchanged.
    async fn assert_after_siblings(&self, tree: &SiblingTree, probes: &TreeProbes, what: &str) {
        let mut expected = HashMap::from([(tree.parent_id, tree.advanced_parent_state())]);
        expected.extend(
            tree.child_ids
                .iter()
                .map(|child_id| (*child_id, JobState::Succeeded)),
        );
        if matches!(tree.position, Position::UnderRoot) {
            let (root_state, root_transitions) = if tree.parent_has_commit {
                (JobState::BlockingOnChildren, 0)
            } else {
                (JobState::Succeeded, 1)
            };
            expected.insert(tree.root_id, root_state);
            assert_eq!(
                probes.transitions(tree.root_id),
                root_transitions,
                "{what}: the rounds that transition the root"
            );
        }
        self.assert_states(&expected, what).await;
        assert_eq!(
            self.row_outputs(tree.parent_id).await,
            Some(task_outputs([&tree.parent_value])),
            "{what}: the parent's own outputs"
        );
    }

    /// Runs the commit task of `tree`'s commit-ready parent, which must be handed only the parent's
    /// outputs, and asserts that the parent and its root succeed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ServiceState::succeed_commit_task_instance`]'s return values on failure.
    ///
    /// [`ServiceState::succeed_commit_task_instance`]:
    ///     spider_storage::state::ServiceState::succeed_commit_task_instance
    async fn commit_sibling_parent(
        &self,
        tree: &SiblingTree,
        probes: &TreeProbes,
        what: &str,
    ) -> anyhow::Result<()> {
        let (task_instance_id, commit_inputs) = self.commit_instance(tree.parent_id).await;
        assert_eq!(
            commit_inputs,
            task_outputs([&tree.parent_value]),
            "{what}: the commit task's inputs"
        );
        assert_eq!(
            self.commit_success(tree.parent_id, task_instance_id)
                .await?,
            JobState::Succeeded,
            "{what}"
        );
        let mut expected = HashMap::from([(tree.parent_id, JobState::Succeeded)]);
        if matches!(tree.position, Position::UnderRoot) {
            expected.insert(tree.root_id, JobState::Succeeded);
            assert_eq!(
                probes.transitions(tree.root_id),
                1,
                "{what}: the rounds that transition the root"
            );
        }
        self.assert_states(&expected, what).await;
        assert_eq!(
            probes.transitions(tree.parent_id),
            2,
            "{what}: the rounds that transition the parent"
        );
        Ok(())
    }

    /// Holds every task of a job of `num_tasks` tasks at `position` between its task-level success
    /// and the job's count decrement, then lets them all decrement at once, and asserts that
    /// exactly one of them completes the job, with every task's output.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ServiceState::succeed_task_instance`]'s return values on failure.
    /// * Forwards [`ServiceState::get_job_outputs`]'s return values on failure.
    ///
    /// [`ServiceState::succeed_task_instance`]:
    ///     spider_storage::state::ServiceState::succeed_task_instance
    /// [`ServiceState::get_job_outputs`]: spider_storage::state::ServiceState::get_job_outputs
    async fn assert_sibling_tasks_complete_their_job_once(
        &self,
        num_tasks: usize,
        position: Position,
    ) -> anyhow::Result<()> {
        let what = format!("{num_tasks} tasks, job at {position:?}");
        let (root_id, job_id) = self
            .start_job_at(
                position,
                JobShape {
                    num_tasks,
                    ..JobShape::ONE_TASK
                },
            )
            .await;
        let task_values: Vec<_> = (0..num_tasks).map(|index| value(0xd1, index)).collect();
        let mut reports = Vec::with_capacity(num_tasks);
        for (task_index, task_value) in task_values.iter().enumerate() {
            let task_instance_id = self
                .create_instance(job_id, TaskId::Index(task_index))
                .await;
            reports.push(self.task_success(
                job_id,
                task_index,
                task_instance_id,
                task_value,
                Vec::new(),
            ));
        }
        let flips = self.hooks.arm(
            HookPoint::TaskSuccessAfterTcbFlip,
            HookFilter::job(job_id),
            HookAction::Pause,
            HookCount::Times(num_tasks),
        );
        let submissions = self.observe(HookPoint::JobEventAfterSubmit, HookFilter::job(job_id));
        let probes = TreeProbes::arm(self, root_id, &[root_id, job_id]);

        let report_join_handles: Vec<_> = reports.into_iter().map(tokio::spawn).collect();
        reached(&flips, num_tasks).await;
        assert_eq!(
            submissions.hits(),
            0,
            "{what}: no completion before the decrements"
        );
        assert_eq!(self.row_state(job_id).await, JobState::Running, "{what}");
        for _ in 0..num_tasks {
            flips.release();
        }
        let mut results = Vec::with_capacity(num_tasks);
        for report_join_handle in report_join_handles {
            results.push(join(report_join_handle).await?);
        }
        let mut expected_results = vec![JobState::Running; num_tasks - 1];
        expected_results.push(JobState::Succeeded);
        results.sort_unstable_by_key(|state| *state == JobState::Succeeded);
        assert_eq!(results, expected_results, "{what}: the report results");
        assert_eq!(submissions.hits(), 1, "{what}: the completion events");

        let expected_outputs = task_outputs(&task_values);
        assert_eq!(
            self.row_outputs(job_id).await,
            Some(expected_outputs.clone()),
            "{what}"
        );
        if job_id == root_id {
            assert_eq!(
                self.service.get_job_outputs(job_id).await?,
                expected_outputs,
                "{what}"
            );
        }
        self.assert_states(
            &HashMap::from([
                (job_id, JobState::Succeeded),
                (root_id, JobState::Succeeded),
            ]),
            &what,
        )
        .await;
        probes.assert_transitions(&HashMap::from([(root_id, 1), (job_id, 1)]), &what);
        self.assert_quiescent(root_id).await;
        Ok(())
    }

    /// Completes `P`'s children, with `Q` before them or in the round of the last one as
    /// `sibling_timing` says, and asserts that `P` is commit-ready with its commit task published
    /// once, its children's and its own outputs written, while `R` still blocks.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ServiceState::succeed_task_instance`]'s return values on failure.
    ///
    /// [`ServiceState::succeed_task_instance`]:
    ///     spider_storage::state::ServiceState::succeed_task_instance
    async fn complete_commit_parent_children(
        &self,
        tree: &CommitParentTree,
        probes: &TreeProbes,
        sibling_timing: SiblingTiming,
        what: &str,
    ) -> anyhow::Result<()> {
        if matches!(sibling_timing, SiblingTiming::BeforeChildren) {
            assert_eq!(
                tree.sibling_report(self).await?,
                JobState::Succeeded,
                "{what}"
            );
            self.assert_states(
                &HashMap::from([
                    (tree.sibling_id, JobState::Succeeded),
                    (tree.root_id, JobState::BlockingOnChildren),
                ]),
                what,
            )
            .await;
        }
        let last_index = CommitParentTree::NUM_CHILDREN - 1;
        for index in 0..last_index {
            assert_eq!(
                tree.child_report(self, index).await?,
                JobState::Succeeded,
                "{what}"
            );
            self.assert_states(
                &HashMap::from([(tree.parent_id, JobState::BlockingOnChildren)]),
                what,
            )
            .await;
        }
        if matches!(sibling_timing, SiblingTiming::WithLastChild) {
            let rounds = self.observe(
                HookPoint::CommitterBeforeReceive,
                HookFilter::job(tree.root_id),
            );
            let (last_child, sibling) = Race::start(
                self,
                HookPoint::CommitterBeforeReceive,
                HookFilter::job(tree.root_id),
                tree.child_report(self, last_index),
                tree.sibling_report(self),
                tree.sibling_id,
            )
            .await
            .finish()
            .await;
            assert_eq!(last_child?, JobState::Succeeded, "{what}");
            assert_eq!(sibling?, JobState::Succeeded, "{what}");
            assert_eq!(rounds.hits(), 1, "{what}: the rounds of the last child");
        } else {
            assert_eq!(
                tree.child_report(self, last_index).await?,
                JobState::Succeeded,
                "{what}"
            );
        }

        let mut expected: HashMap<JobId, JobState> = tree
            .child_ids
            .iter()
            .map(|child_id| (*child_id, JobState::Succeeded))
            .collect();
        expected.insert(tree.parent_id, JobState::CommitReady);
        expected.insert(tree.root_id, JobState::BlockingOnChildren);
        self.assert_states(&expected, what).await;
        for (child_id, child_value) in tree.child_ids.iter().zip(&tree.child_values) {
            assert_eq!(
                self.row_outputs(*child_id).await,
                Some(task_outputs([child_value])),
                "{what}: the outputs of child {child_id}"
            );
        }
        assert_eq!(
            self.row_outputs(tree.parent_id).await,
            Some(task_outputs([&tree.parent_value])),
            "{what}: the parent's outputs"
        );
        probes.assert_transitions(
            &HashMap::from([(tree.parent_id, 1), (tree.root_id, 0)]),
            what,
        );
        Ok(())
    }

    /// Runs `P`'s commit task, which must be handed only `P`'s outputs, with `Q` before it, in its
    /// round, or queued behind its round as `sibling_timing` says, unless `Q` already succeeded.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ServiceState::succeed_task_instance`]'s return values on failure.
    /// * Forwards [`ServiceState::succeed_commit_task_instance`]'s return values on failure.
    ///
    /// [`ServiceState::succeed_task_instance`]:
    ///     spider_storage::state::ServiceState::succeed_task_instance
    /// [`ServiceState::succeed_commit_task_instance`]:
    ///     spider_storage::state::ServiceState::succeed_commit_task_instance
    async fn commit_parent(
        &self,
        tree: &CommitParentTree,
        sibling_timing: SiblingTiming,
        what: &str,
    ) -> anyhow::Result<()> {
        let (task_instance_id, commit_inputs) = self.commit_instance(tree.parent_id).await;
        assert_eq!(
            commit_inputs,
            task_outputs([&tree.parent_value]),
            "{what}: the parent's commit task sees only the parent's outputs"
        );
        if matches!(sibling_timing, SiblingTiming::BeforeCommit) {
            assert_eq!(
                tree.sibling_report(self).await?,
                JobState::Succeeded,
                "{what}"
            );
            self.assert_states(
                &HashMap::from([
                    (tree.sibling_id, JobState::Succeeded),
                    (tree.parent_id, JobState::CommitReady),
                    (tree.root_id, JobState::BlockingOnChildren),
                ]),
                what,
            )
            .await;
        }
        let commit = self.commit_success(tree.parent_id, task_instance_id);
        let (point, filter, num_rounds) = match sibling_timing {
            SiblingTiming::WithCommit => (
                HookPoint::CommitterBeforeReceive,
                HookFilter::job(tree.root_id),
                1,
            ),
            SiblingTiming::AfterCommit => (
                HookPoint::CommitterRoundAfterDbWrite,
                HookFilter::job(tree.parent_id),
                2,
            ),
            SiblingTiming::BeforeChildren
            | SiblingTiming::WithLastChild
            | SiblingTiming::BeforeCommit => {
                assert_eq!(commit.await?, JobState::Succeeded, "{what}");
                return Ok(());
            }
        };
        let rounds = self.observe(
            HookPoint::CommitterBeforeReceive,
            HookFilter::job(tree.root_id),
        );
        let (committed, sibling) = Race::start(
            self,
            point,
            filter,
            commit,
            tree.sibling_report(self),
            tree.sibling_id,
        )
        .await
        .finish()
        .await;
        assert_eq!(committed?, JobState::Succeeded, "{what}");
        assert_eq!(sibling?, JobState::Succeeded, "{what}");
        assert_eq!(rounds.hits(), num_rounds, "{what}: the number of rounds");
        Ok(())
    }

    /// Asserts that `P` and `Q` succeeded and that the success climbed to `R` once, then runs `R`'s
    /// commit task if it has one, which must be handed only `R`'s outputs.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ServiceState::succeed_commit_task_instance`]'s return values on failure.
    /// * Forwards [`ServiceState::get_job_outputs`]'s return values on failure.
    ///
    /// [`ServiceState::succeed_commit_task_instance`]:
    ///     spider_storage::state::ServiceState::succeed_commit_task_instance
    /// [`ServiceState::get_job_outputs`]: spider_storage::state::ServiceState::get_job_outputs
    async fn assert_commit_parent_tree_climbed(
        &self,
        tree: &CommitParentTree,
        probes: &TreeProbes,
        what: &str,
    ) -> anyhow::Result<()> {
        let root_state = if tree.root_has_commit {
            JobState::CommitReady
        } else {
            JobState::Succeeded
        };
        self.assert_states(
            &HashMap::from([
                (tree.parent_id, JobState::Succeeded),
                (tree.sibling_id, JobState::Succeeded),
                (tree.root_id, root_state),
            ]),
            what,
        )
        .await;
        probes.assert_transitions(
            &HashMap::from([(tree.parent_id, 2), (tree.sibling_id, 1), (tree.root_id, 1)]),
            what,
        );
        assert_eq!(
            self.row_outputs(tree.parent_id).await,
            Some(task_outputs([&tree.parent_value])),
            "{what}: the parent's outputs"
        );
        assert_eq!(
            self.row_outputs(tree.sibling_id).await,
            Some(task_outputs([&CommitParentTree::sibling_value()])),
            "{what}: the sibling's outputs"
        );
        if tree.root_has_commit {
            let (task_instance_id, commit_inputs) = self.commit_instance(tree.root_id).await;
            assert_eq!(
                commit_inputs,
                task_outputs([&tree.root_value]),
                "{what}: the root's commit task sees only the root's outputs"
            );
            assert_eq!(
                self.commit_success(tree.root_id, task_instance_id).await?,
                JobState::Succeeded,
                "{what}"
            );
            self.assert_states(&HashMap::from([(tree.root_id, JobState::Succeeded)]), what)
                .await;
            assert_eq!(
                probes.transitions(tree.root_id),
                2,
                "{what}: the rounds that transition the root"
            );
        }
        assert_eq!(
            self.service.get_job_outputs(tree.root_id).await?,
            task_outputs([&tree.root_value]),
            "{what}: the root's outputs"
        );
        assert_eq!(
            self.tree_errors(tree.root_id).await,
            HashMap::new(),
            "{what}"
        );
        self.assert_quiescent(tree.root_id).await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn sibling_jobs_completing_together_advance_their_parent_once() -> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    for num_children in [2, 8, 64] {
        for rounds in [SiblingRounds::One, SiblingRounds::Each, SiblingRounds::Free] {
            for position in [Position::Root, Position::UnderRoot] {
                for parent_has_commit in [false, true] {
                    let tree =
                        SiblingTree::start(&env, num_children, position, parent_has_commit).await;
                    env.assert_siblings_advance_their_parent_once(&tree, rounds)
                        .await?;
                }
            }
        }
    }
    env.assert_lanes_empty().await;
    env.finish().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn sibling_tasks_completing_together_complete_their_job_once() -> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    for num_tasks in [2, 8] {
        for position in [Position::Root, Position::UnderRoot] {
            env.assert_sibling_tasks_complete_their_job_once(num_tasks, position)
                .await?;
        }
    }
    env.assert_lanes_empty().await;
    env.finish().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_parent_with_a_commit_task_commits_then_climbs() -> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    for sibling_timing in [
        SiblingTiming::BeforeChildren,
        SiblingTiming::WithLastChild,
        SiblingTiming::BeforeCommit,
        SiblingTiming::WithCommit,
        SiblingTiming::AfterCommit,
    ] {
        for root_has_commit in [false, true] {
            let what = format!("Q {sibling_timing:?}, root commit: {root_has_commit}");
            let tree = CommitParentTree::start(&env, root_has_commit).await;
            let probes = TreeProbes::arm(
                &env,
                tree.root_id,
                &[tree.root_id, tree.parent_id, tree.sibling_id],
            );
            env.complete_commit_parent_children(&tree, &probes, sibling_timing, &what)
                .await?;
            env.commit_parent(&tree, sibling_timing, &what).await?;
            env.assert_commit_parent_tree_climbed(&tree, &probes, &what)
                .await?;
        }
    }
    env.assert_lanes_empty().await;
    env.finish().await;
    Ok(())
}
