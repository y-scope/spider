//! The completion of jobs and job trees through the storage service on `MariaDB`.

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
use strum::EnumIter;
use strum::IntoEnumIterator;

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
#[derive(Clone, Copy, Debug, EnumIter)]
enum SiblingRounds {
    /// Every report in the round of the first one.
    One,

    /// Each report in its own round, one after the other.
    Each,

    /// Every report at once, in whatever rounds the committer forms.
    Free,
}

/// Where the job under test is in its tree.
#[derive(Clone, Copy, Debug, EnumIter)]
enum Position {
    /// The job is the root.
    Root,

    /// The job is the only child of a one-task root.
    UnderRoot,
}

/// When the sibling of a [`CommitParentTree`]'s parent succeeds, relative to the parent's children
/// and the parent's commit.
#[derive(Clone, Copy, Debug, EnumIter)]
enum SiblingTiming {
    /// Before the parent's children.
    BeforeChildren,

    /// In the round of the parent's last child.
    WithLastChild,

    /// After the parent is commit-ready, before its commit task reports.
    BeforeCommit,

    /// In the round of the parent's commit success.
    WithCommit,

    /// While the round of the parent's commit success is written but not applied.
    AfterCommit,
}

/// A job under test, with the root of its tree.
struct JobInTree {
    root_id: JobId,
    job_id: JobId,
}

/// A one-task job with an instance of its task registered, and the output value that the task
/// reports.
struct Child {
    id: JobId,
    value: Vec<u8>,
    instance_id: TaskInstanceId,
}

impl Child {
    /// Factory function.
    ///
    /// Registers an instance of the task of the one-task job `id`, whose task reports `value`.
    ///
    /// # Returns
    ///
    /// The job.
    async fn register(env: &TestEnv, id: JobId, value: Vec<u8>) -> Self {
        Self {
            id,
            value,
            instance_id: env.create_instance(id, TaskId::Index(0)).await,
        }
    }

    /// Registers an instance of the task of each one-task job of `ids`. The task of the job at
    /// `index` in `ids` reports [`value`] of `tag` and `index`.
    ///
    /// # Returns
    ///
    /// The jobs, in the order of `ids`.
    async fn register_each(env: &TestEnv, ids: Vec<JobId>, tag: u8) -> Vec<Self> {
        let mut children = Vec::with_capacity(ids.len());
        for (index, id) in ids.into_iter().enumerate() {
            children.push(Self::register(env, id, value(tag, index)).await);
        }
        children
    }

    /// # Returns
    ///
    /// A future that reports the success of the job's task (see [`TestEnv::task_success`]).
    fn report(&self, env: &TestEnv) -> impl Future<Output = JobResult> + Send + use<> {
        env.task_success(self.id, 0, self.instance_id, &self.value, Vec::new())
    }
}

/// A tree whose parent, with or without a commit task, blocks on one-task children.
struct SiblingTree {
    root_id: JobId,
    parent_id: JobId,
    parent_value: Vec<u8>,
    children: Vec<Child>,
    position: Position,
    parent_has_commit: bool,
}

impl SiblingTree {
    /// Factory function.
    ///
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
        let JobInTree {
            root_id,
            job_id: parent_id,
        } = env.start_job_at(position, &parent_shape).await;
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
            children: Child::register_each(env, child_ids, 0xc0).await,
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

/// A tree whose root, with or without a commit task, has two children:
///
/// * The parent, which has a commit task and blocks on [`Self::NUM_CHILDREN`] one-task children.
/// * The parent's sibling, a one-task job.
///
/// The tasks of the root and of the parent have succeeded.
struct CommitParentTree {
    root_id: JobId,
    root_value: Vec<u8>,
    parent_id: JobId,
    parent_value: Vec<u8>,
    sibling: Child,
    children: Vec<Child>,
    root_has_commit: bool,
}

impl CommitParentTree {
    const NUM_CHILDREN: usize = 3;

    /// Factory function.
    ///
    /// Starts the tree, with a commit task on the root if `root_has_commit`.
    ///
    /// # Returns
    ///
    /// The tree.
    ///
    /// # Panics
    ///
    /// Panics if the root doesn't have two children.
    async fn start(env: &TestEnv, root_has_commit: bool) -> Self {
        let root_value = value(0xf0, 0);
        let parent_value = value(0xf1, 0);
        let root_id = env
            .start_root(&JobShape {
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
        Self {
            root_id,
            root_value,
            parent_id,
            parent_value,
            sibling: Child::register(env, sibling_id, value(0xf2, 0)).await,
            children: Child::register_each(env, child_ids, 0xf3).await,
            root_has_commit,
        }
    }
}

impl TestEnv {
    /// Starts a job of `shape` at `position`: a root, or the only child of a one-task root.
    ///
    /// # Returns
    ///
    /// The started job, with the root of its tree.
    ///
    /// # Panics
    ///
    /// Panics if the root doesn't have one child.
    async fn start_job_at(&self, position: Position, shape: &JobShape) -> JobInTree {
        match position {
            Position::Root => {
                let job_id = self.start_root(shape).await;
                JobInTree {
                    root_id: job_id,
                    job_id,
                }
            }
            Position::UnderRoot => {
                let root_id = self.start_root(&JobShape::ONE_TASK).await;
                let [job_id] = self
                    .register_children(root_id, &value(0xb0, 0), std::slice::from_ref(shape))
                    .await[..]
                else {
                    panic!("the root should have one child");
                };
                JobInTree { root_id, job_id }
            }
        }
    }

    /// Reports the successes of `tree`'s children as `rounds` says, and asserts that:
    ///
    /// * The parent advances exactly once, with every child's outputs written by then.
    /// * The reports land in the rounds that `rounds` says.
    /// * The parent's commit, if it has a commit task, finishes the tree.
    ///
    /// # Panics
    ///
    /// Panics if an assertion fails.
    async fn assert_siblings_advance_their_parent_once(
        &self,
        tree: &SiblingTree,
        rounds: SiblingRounds,
    ) {
        let num_children = tree.children.len();
        let what = format!(
            "{num_children} children in {rounds:?} rounds, parent at {:?}, commit: {}",
            tree.position, tree.parent_has_commit
        );
        let probes = TreeProbes::arm(self, tree.root_id, &[tree.root_id, tree.parent_id]);
        let advance_hook = self.pause(
            HookPoint::CommitterRoundAfterDbWrite,
            HookFilter::job(tree.parent_id),
        );
        tokio::join!(
            self.report_sibling_successes(tree, rounds, &what),
            self.assert_children_written_at_advance(tree, &advance_hook, &what)
        );
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
            self.commit_sibling_parent(tree, &probes, &what).await;
        }
    }

    /// Reports the successes of `tree`'s children as `rounds` says, and asserts that each report
    /// succeeds its child. Between two reports of [`SiblingRounds::Each`], asserts that the parent
    /// still blocks on its children.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * `tree` has no child.
    /// * An assertion fails.
    async fn report_sibling_successes(
        &self,
        tree: &SiblingTree,
        rounds: SiblingRounds,
        what: &str,
    ) {
        let report_join_handles = match rounds {
            SiblingRounds::One => {
                let (first_child, other_children) = tree
                    .children
                    .split_first()
                    .expect("the tree should have a child");
                let pause_hook = self.pause(
                    HookPoint::CommitterBeforeReceive,
                    HookFilter::job(tree.root_id),
                );
                let mut report_join_handles = vec![tokio::spawn(first_child.report(self))];
                reached(&pause_hook, 1).await;
                let submit_hooks: Vec<_> = other_children
                    .iter()
                    .map(|child| {
                        self.observe(HookPoint::JobEventAfterSubmit, HookFilter::job(child.id))
                    })
                    .collect();
                report_join_handles.extend(
                    other_children
                        .iter()
                        .map(|child| tokio::spawn(child.report(self))),
                );
                for submit_hook in &submit_hooks {
                    reached(submit_hook, 1).await;
                }
                pause_hook.release();
                report_join_handles
            }
            SiblingRounds::Each => {
                for (index, child) in tree.children.iter().enumerate() {
                    assert_succeeded(&child.report(self).await, what);
                    if index + 1 < tree.children.len() {
                        self.assert_states(
                            &HashMap::from([(tree.parent_id, JobState::BlockingOnChildren)]),
                            &format!("{what}: after {} reports", index + 1),
                        )
                        .await;
                    }
                }
                return;
            }
            SiblingRounds::Free => tree
                .children
                .iter()
                .map(|child| tokio::spawn(child.report(self)))
                .collect(),
        };
        for report_join_handle in report_join_handles {
            assert_succeeded(&join(report_join_handle).await, what);
        }
    }

    /// Waits until the round that advances `tree`'s parent pauses at `advance_hook` after writing
    /// its transitions, then releases the round after asserting that:
    ///
    /// * Every child's row is [`JobState::Succeeded`] with the child's outputs.
    /// * The parent's row has advanced.
    ///
    /// # Panics
    ///
    /// Panics if an assertion fails.
    async fn assert_children_written_at_advance(
        &self,
        tree: &SiblingTree,
        advance_hook: &ArmedHook,
        what: &str,
    ) {
        reached(advance_hook, 1).await;
        for child in &tree.children {
            assert_eq!(
                self.row_state(child.id).await,
                JobState::Succeeded,
                "{what}: child {} when the parent advances",
                child.id
            );
            assert_eq!(
                self.row_outputs(child.id).await,
                Some(task_outputs([&child.value])),
                "{what}: the outputs of child {} when the parent advances",
                child.id
            );
        }
        assert_eq!(
            self.row_state(tree.parent_id).await,
            tree.advanced_parent_state(),
            "{what}: the parent's row when it advances"
        );
        advance_hook.release();
    }

    /// Asserts that, once `tree`'s children have succeeded:
    ///
    /// * Every child is [`JobState::Succeeded`].
    /// * The parent has advanced.
    /// * A root above the parent has succeeded in one round if the parent has no commit task, and
    ///   still blocks on its children otherwise.
    /// * The parent's outputs are its own.
    ///
    /// # Panics
    ///
    /// Panics if an assertion fails.
    async fn assert_after_siblings(&self, tree: &SiblingTree, probes: &TreeProbes, what: &str) {
        let mut expected = HashMap::from([(tree.parent_id, tree.advanced_parent_state())]);
        expected.extend(
            tree.children
                .iter()
                .map(|child| (child.id, JobState::Succeeded)),
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

    /// Reports the success of the commit task of `tree`'s commit-ready parent, and asserts that:
    ///
    /// * The commit task is handed only the parent's outputs.
    /// * The parent succeeds, in the second round that transitions it.
    /// * A root above the parent succeeds, in one round.
    ///
    /// # Panics
    ///
    /// Panics if an assertion fails.
    async fn commit_sibling_parent(&self, tree: &SiblingTree, probes: &TreeProbes, what: &str) {
        let (task_instance_id, commit_inputs) = self.commit_instance(tree.parent_id).await;
        assert_eq!(
            commit_inputs,
            task_outputs([&tree.parent_value]),
            "{what}: the commit task's inputs"
        );
        assert_succeeded(
            &self.commit_success(tree.parent_id, task_instance_id).await,
            what,
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
    }

    /// Races the successes of every task of a job of `num_tasks` tasks at `position` to complete
    /// the job, and asserts that:
    ///
    /// * No report completes the job before every task has succeeded.
    /// * Exactly one report completes the job, with every task's output.
    /// * The job and its tree's root succeed, each in one round.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * A report fails.
    /// * The job's outputs fail to be read.
    /// * An assertion fails.
    async fn assert_sibling_tasks_complete_their_job_once(
        &self,
        num_tasks: usize,
        position: Position,
    ) {
        let what = format!("{num_tasks} tasks, job at {position:?}");
        let JobInTree { root_id, job_id } = self
            .start_job_at(
                position,
                &JobShape {
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
        let flip_hook = self.hooks.arm(
            HookPoint::TaskSuccessAfterTcbFlip,
            HookFilter::job(job_id),
            HookAction::Pause,
            HookCount::Times(num_tasks),
        );
        let submit_hook = self.observe(HookPoint::JobEventAfterSubmit, HookFilter::job(job_id));
        let probes = TreeProbes::arm(self, root_id, &[root_id, job_id]);

        let report_join_handles: Vec<_> = reports.into_iter().map(tokio::spawn).collect();
        reached(&flip_hook, num_tasks).await;
        assert_eq!(
            submit_hook.hits(),
            0,
            "{what}: no completion before the decrements"
        );
        assert_eq!(self.row_state(job_id).await, JobState::Running, "{what}");
        for _ in 0..num_tasks {
            flip_hook.release();
        }
        let mut results = Vec::with_capacity(num_tasks);
        for report_join_handle in report_join_handles {
            results.push(
                join(report_join_handle)
                    .await
                    .expect("a task's report should succeed"),
            );
        }
        let mut expected_results = vec![JobState::Running; num_tasks - 1];
        expected_results.push(JobState::Succeeded);
        results.sort_unstable_by_key(|state| *state == JobState::Succeeded);
        assert_eq!(results, expected_results, "{what}: the report results");
        assert_eq!(submit_hook.hits(), 1, "{what}: the completion events");

        let expected_outputs = task_outputs(&task_values);
        assert_eq!(
            self.row_outputs(job_id).await,
            Some(expected_outputs.clone()),
            "{what}"
        );
        if job_id == root_id {
            assert_eq!(
                self.service
                    .get_job_outputs(job_id)
                    .await
                    .expect("the root's outputs should be readable"),
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
    }

    /// Reports the successes of the parent's children of `tree` one after the other, with the
    /// sibling's success before them or in the round of the last one as `sibling_timing` says, and
    /// asserts that:
    ///
    /// * Every report succeeds its job.
    /// * The parent blocks on its children until the last one succeeds.
    /// * A sibling's success before the children leaves the root blocking on its children.
    /// * A sibling's success in the round of the last child lands in that one round.
    /// * The parent is then commit-ready, with its children's and its own outputs written, after
    ///   one round that transitions it.
    /// * The root still blocks on its children, without a round that transitions it.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * The parent has no child.
    /// * An assertion fails.
    async fn complete_commit_parent_children(
        &self,
        tree: &CommitParentTree,
        probes: &TreeProbes,
        sibling_timing: SiblingTiming,
        what: &str,
    ) {
        if matches!(sibling_timing, SiblingTiming::BeforeChildren) {
            assert_succeeded(&tree.sibling.report(self).await, what);
            self.assert_states(
                &HashMap::from([
                    (tree.sibling.id, JobState::Succeeded),
                    (tree.root_id, JobState::BlockingOnChildren),
                ]),
                what,
            )
            .await;
        }
        let (last_child, other_children) = tree
            .children
            .split_last()
            .expect("the parent should have a child");
        for child in other_children {
            assert_succeeded(&child.report(self).await, what);
            self.assert_states(
                &HashMap::from([(tree.parent_id, JobState::BlockingOnChildren)]),
                what,
            )
            .await;
        }
        if matches!(sibling_timing, SiblingTiming::WithLastChild) {
            let round_hook = self.observe(
                HookPoint::CommitterBeforeReceive,
                HookFilter::job(tree.root_id),
            );
            let (last_child_result, sibling_result) = Race::start(
                self,
                HookPoint::CommitterBeforeReceive,
                HookFilter::job(tree.root_id),
                last_child.report(self),
                tree.sibling.report(self),
                tree.sibling.id,
            )
            .await
            .finish()
            .await;
            assert_succeeded(&last_child_result, what);
            assert_succeeded(&sibling_result, what);
            assert_eq!(round_hook.hits(), 1, "{what}: the rounds of the last child");
        } else {
            assert_succeeded(&last_child.report(self).await, what);
        }

        let mut expected: HashMap<JobId, JobState> = tree
            .children
            .iter()
            .map(|child| (child.id, JobState::Succeeded))
            .collect();
        expected.insert(tree.parent_id, JobState::CommitReady);
        expected.insert(tree.root_id, JobState::BlockingOnChildren);
        self.assert_states(&expected, what).await;
        for child in &tree.children {
            assert_eq!(
                self.row_outputs(child.id).await,
                Some(task_outputs([&child.value])),
                "{what}: the outputs of child {}",
                child.id
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
    }

    /// Reports the success of the commit task of `tree`'s parent, with the sibling's success as
    /// `sibling_timing` says, unless the sibling has already succeeded:
    ///
    /// * [`SiblingTiming::BeforeCommit`]: before the commit's report.
    /// * [`SiblingTiming::WithCommit`]: in the round of the commit's report.
    /// * [`SiblingTiming::AfterCommit`]: while the round of the commit's report is written but not
    ///   applied.
    ///
    /// Asserts that:
    ///
    /// * The commit task is handed only the parent's outputs.
    /// * Every report succeeds its job.
    /// * A sibling's success before the commit leaves the parent commit-ready and the root blocking
    ///   on its children.
    /// * A sibling's success with or after the commit lands in one or two rounds respectively.
    ///
    /// # Panics
    ///
    /// Panics if an assertion fails.
    async fn commit_parent(
        &self,
        tree: &CommitParentTree,
        sibling_timing: SiblingTiming,
        what: &str,
    ) {
        let (task_instance_id, commit_inputs) = self.commit_instance(tree.parent_id).await;
        assert_eq!(
            commit_inputs,
            task_outputs([&tree.parent_value]),
            "{what}: the parent's commit task sees only the parent's outputs"
        );
        if matches!(sibling_timing, SiblingTiming::BeforeCommit) {
            assert_succeeded(&tree.sibling.report(self).await, what);
            self.assert_states(
                &HashMap::from([
                    (tree.sibling.id, JobState::Succeeded),
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
                assert_succeeded(&commit.await, what);
                return;
            }
        };
        let round_hook = self.observe(
            HookPoint::CommitterBeforeReceive,
            HookFilter::job(tree.root_id),
        );
        let (commit_result, sibling_result) = Race::start(
            self,
            point,
            filter,
            commit,
            tree.sibling.report(self),
            tree.sibling.id,
        )
        .await
        .finish()
        .await;
        assert_succeeded(&commit_result, what);
        assert_succeeded(&sibling_result, what);
        assert_eq!(
            round_hook.hits(),
            num_rounds,
            "{what}: the number of rounds"
        );
    }

    /// Asserts that the successes of `tree`'s parent and sibling climbed to the root once, then
    /// finishes the root:
    ///
    /// 1. Asserts that the parent and the sibling are [`JobState::Succeeded`] with their own
    ///    outputs, and that the root has advanced in one round.
    /// 2. If the root has a commit task, reports the task's success, and asserts that the task is
    ///    handed only the root's outputs and that the root succeeds.
    /// 3. Asserts that the root's outputs are its own, and that no job of the tree has an error.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * The root's outputs fail to be read.
    /// * An assertion fails.
    async fn assert_commit_parent_tree_climbed(
        &self,
        tree: &CommitParentTree,
        probes: &TreeProbes,
        what: &str,
    ) {
        let root_state = if tree.root_has_commit {
            JobState::CommitReady
        } else {
            JobState::Succeeded
        };
        self.assert_states(
            &HashMap::from([
                (tree.parent_id, JobState::Succeeded),
                (tree.sibling.id, JobState::Succeeded),
                (tree.root_id, root_state),
            ]),
            what,
        )
        .await;
        probes.assert_transitions(
            &HashMap::from([(tree.parent_id, 2), (tree.sibling.id, 1), (tree.root_id, 1)]),
            what,
        );
        assert_eq!(
            self.row_outputs(tree.parent_id).await,
            Some(task_outputs([&tree.parent_value])),
            "{what}: the parent's outputs"
        );
        assert_eq!(
            self.row_outputs(tree.sibling.id).await,
            Some(task_outputs([&tree.sibling.value])),
            "{what}: the sibling's outputs"
        );
        if tree.root_has_commit {
            let (task_instance_id, commit_inputs) = self.commit_instance(tree.root_id).await;
            assert_eq!(
                commit_inputs,
                task_outputs([&tree.root_value]),
                "{what}: the root's commit task sees only the root's outputs"
            );
            assert_succeeded(
                &self.commit_success(tree.root_id, task_instance_id).await,
                what,
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
            self.service
                .get_job_outputs(tree.root_id)
                .await
                .expect("the root's outputs should be readable"),
            task_outputs([&tree.root_value]),
            "{what}: the root's outputs"
        );
        assert_eq!(
            self.tree_errors(tree.root_id).await,
            HashMap::new(),
            "{what}"
        );
    }
}

/// Asserts that `result`, the result of a report, succeeds the report's job.
///
/// # Panics
///
/// Panics if the assertion fails.
fn assert_succeeded(result: &JobResult, what: &str) {
    assert!(
        matches!(result, Ok(JobState::Succeeded)),
        "{what}: the report gave {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn sibling_jobs_succeeding_in_any_rounds_advance_their_parent_once() -> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    for num_children in [2, 8, 64] {
        for rounds in SiblingRounds::iter() {
            for position in Position::iter() {
                for parent_has_commit in [false, true] {
                    let tree =
                        SiblingTree::start(&env, num_children, position, parent_has_commit).await;
                    env.assert_siblings_advance_their_parent_once(&tree, rounds)
                        .await;
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
        for position in Position::iter() {
            env.assert_sibling_tasks_complete_their_job_once(num_tasks, position)
                .await;
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
    for sibling_timing in SiblingTiming::iter() {
        for root_has_commit in [false, true] {
            let what = format!("sibling {sibling_timing:?}, root commit: {root_has_commit}");
            let tree = CommitParentTree::start(&env, root_has_commit).await;
            let probes = TreeProbes::arm(
                &env,
                tree.root_id,
                &[tree.root_id, tree.parent_id, tree.sibling.id],
            );
            env.complete_commit_parent_children(&tree, &probes, sibling_timing, &what)
                .await;
            env.commit_parent(&tree, sibling_timing, &what).await;
            env.assert_commit_parent_tree_climbed(&tree, &probes, &what)
                .await;
        }
    }
    env.assert_lanes_empty().await;
    env.finish().await;
    Ok(())
}
