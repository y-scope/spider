//! The completion and failure of jobs and job trees through the storage service on `MariaDB`.

use std::collections::HashMap;

use spider_core::job::JobState;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TaskState;
use spider_core::types::id::JobId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_storage::cache::error::StaleStateError;
use spider_storage::test_hooks::ArmedHook;
use spider_storage::test_hooks::HookAction;
use spider_storage::test_hooks::HookCount;
use spider_storage::test_hooks::HookFilter;
use spider_storage::test_hooks::HookPoint;
use strum::EnumIter;
use strum::IntoEnumIterator;

use crate::runtime_infra::HOOK_TIMEOUT;
use crate::runtime_infra::JobResult;
use crate::runtime_infra::JobShape;
use crate::runtime_infra::Race;
use crate::runtime_infra::TestEnv;
use crate::runtime_infra::TreeProbes;
use crate::runtime_infra::assert_job_result;
use crate::runtime_infra::assert_stale;
use crate::runtime_infra::join;
use crate::runtime_infra::reached;
use crate::runtime_infra::task_outputs;
use crate::runtime_infra::value;

/// The message of the first of two failures of one job's tasks.
const FIRST_FAILURE: &str = "first failure";

/// The message of the second of two failures of one job's tasks.
const SECOND_FAILURE: &str = "second failure";

/// The message of a straggler's failure.
const STRAGGLER_FAILURE: &str = "straggler failure";

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

/// The job whose two tasks fail.
#[derive(Clone, Copy, Debug, EnumIter)]
enum FailingJob {
    /// A root job.
    Root,

    /// A leaf `J` in the tree `R → A → {J, S}`, where the sibling `S` has a cleanup task.
    Leaf,
}

/// When the second of two failures of one job's tasks reaches the job's tree.
#[derive(Clone, Copy, Debug, EnumIter)]
enum SecondFailure {
    /// In the round of the first failure.
    SameRound,

    /// While the first failure's round is between its database write and its apply.
    InFlight,

    /// After the first failure is answered.
    AfterReply,
}

/// The shape of a tree whose two-task job fails.
#[derive(Clone, Copy)]
enum TwoFailureShape {
    /// The failing job is the root.
    Root,

    /// The failing job is a leaf `J` in the tree `R → A → {J, S}`.
    Leaf { middle_id: JobId, sibling_id: JobId },
}

/// A tree whose two-task job fails.
struct TwoFailureTree {
    root_id: JobId,
    job_id: JobId,
    shape: TwoFailureShape,
}

impl TwoFailureTree {
    /// Starts a tree whose two-task job is `failing_job`.
    ///
    /// # Returns
    ///
    /// The tree.
    ///
    /// # Panics
    ///
    /// Panics if the root or `A` doesn't get its children.
    async fn start(env: &TestEnv, failing_job: FailingJob) -> Self {
        let two_tasks = JobShape {
            num_tasks: 2,
            ..JobShape::ONE_TASK
        };
        match failing_job {
            FailingJob::Root => {
                let job_id = env.start_root(&two_tasks).await;
                Self {
                    root_id: job_id,
                    job_id,
                    shape: TwoFailureShape::Root,
                }
            }
            FailingJob::Leaf => {
                let root_id = env.start_root(&JobShape::ONE_TASK).await;
                let [middle_id] = env
                    .register_children(root_id, &value(0xa0, 0), &[JobShape::ONE_TASK])
                    .await[..]
                else {
                    panic!("the root should have one child");
                };
                let [job_id, sibling_id] = env
                    .register_children(
                        middle_id,
                        &value(0xa1, 0),
                        &[
                            two_tasks,
                            JobShape {
                                with_cleanup: true,
                                ..JobShape::ONE_TASK
                            },
                        ],
                    )
                    .await[..]
                else {
                    panic!("the middle job should have two children");
                };
                Self {
                    root_id,
                    job_id,
                    shape: TwoFailureShape::Leaf {
                        middle_id,
                        sibling_id,
                    },
                }
            }
        }
    }

    /// # Returns
    ///
    /// Every job of the tree.
    fn job_ids(&self) -> Vec<JobId> {
        match self.shape {
            TwoFailureShape::Root => vec![self.job_id],
            TwoFailureShape::Leaf {
                middle_id,
                sibling_id,
            } => vec![self.root_id, middle_id, self.job_id, sibling_id],
        }
    }
}

/// The job whose task a straggler fails after the task's success.
#[derive(Clone, Copy, Debug, EnumIter)]
enum StragglerJob {
    /// A one-task root, which the success completes.
    Root,

    /// A one-task leaf with a one-task sibling: the success completes the leaf but not its tree.
    Leaf,

    /// A two-task root, which the success of task 0 doesn't complete.
    TwoTaskRoot,
}

/// The instance that fails a task after the task's success.
#[derive(Clone, Copy, Debug, EnumIter)]
enum Straggler {
    /// The instance that succeeded.
    SameInstance,

    /// A second instance of the task.
    OtherInstance,
}

/// When a straggler fails a task, relative to the task's success.
#[derive(Clone, Copy, Debug, EnumIter)]
enum StragglerWindow {
    /// After the success's task-level flip, before the job's count of incomplete tasks is
    /// decremented.
    AfterTcbFlip,

    /// While the success's round is between its database write and its apply.
    InFlight,

    /// After the success is answered.
    AfterReply,
}

/// The tree of a straggler job, as [`StragglerJob`] shapes it.
#[derive(Clone, Copy)]
enum StragglerTree {
    /// A one-task root.
    Root { job_id: JobId },

    /// A one-task leaf with a one-task sibling.
    Leaf {
        root_id: JobId,
        job_id: JobId,
        sibling_id: JobId,
    },

    /// A two-task root.
    TwoTaskRoot { job_id: JobId },
}

impl StragglerTree {
    const fn root_id(&self) -> JobId {
        match *self {
            Self::Root { job_id } | Self::TwoTaskRoot { job_id } => job_id,
            Self::Leaf { root_id, .. } => root_id,
        }
    }

    const fn job_id(&self) -> JobId {
        match *self {
            Self::Root { job_id } | Self::Leaf { job_id, .. } | Self::TwoTaskRoot { job_id } => {
                job_id
            }
        }
    }
}

/// A straggler job, with the instance that succeeds its task 0 and the instance that fails it.
struct StragglerCase {
    tree: StragglerTree,
    winner_instance_id: TaskInstanceId,
    straggler_instance_id: TaskInstanceId,
}

impl StragglerCase {
    /// Starts a job of `job` whose tasks may run two instances at once and are retried at most
    /// `max_num_retry` times, and registers the instances of `straggler`.
    ///
    /// # Returns
    ///
    /// The case.
    ///
    /// # Panics
    ///
    /// Panics if the root of a leaf doesn't get two children.
    async fn start(
        env: &TestEnv,
        job: StragglerJob,
        straggler: Straggler,
        max_num_retry: u32,
    ) -> Self {
        let shape = JobShape {
            num_tasks: if matches!(job, StragglerJob::TwoTaskRoot) {
                2
            } else {
                1
            },
            execution_policy: Some(ExecutionPolicy {
                max_num_instances: 2,
                max_num_retry,
                ..ExecutionPolicy::default()
            }),
            ..JobShape::ONE_TASK
        };
        let tree = match job {
            StragglerJob::Root => StragglerTree::Root {
                job_id: env.start_root(&shape).await,
            },
            StragglerJob::TwoTaskRoot => StragglerTree::TwoTaskRoot {
                job_id: env.start_root(&shape).await,
            },
            StragglerJob::Leaf => {
                let root_id = env.start_root(&JobShape::ONE_TASK).await;
                let [job_id, sibling_id] = env
                    .register_children(root_id, &value(0xe0, 0), &[shape, JobShape::ONE_TASK])
                    .await[..]
                else {
                    panic!("the root should have two children");
                };
                StragglerTree::Leaf {
                    root_id,
                    job_id,
                    sibling_id,
                }
            }
        };
        let job_id = tree.job_id();
        let winner_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
        let straggler_instance_id = match straggler {
            Straggler::SameInstance => winner_instance_id,
            Straggler::OtherInstance => env.create_instance(job_id, TaskId::Index(0)).await,
        };
        Self {
            tree,
            winner_instance_id,
            straggler_instance_id,
        }
    }

    /// # Returns
    ///
    /// The state of the job after its task 0 succeeded.
    const fn state_after_success(&self) -> JobState {
        if matches!(self.tree, StragglerTree::TwoTaskRoot { .. }) {
            JobState::Running
        } else {
            JobState::Succeeded
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

    /// Fails tasks 0 and 1 of the two-task job of a tree of `failing_job`, the second as
    /// `second_failure` says, and asserts that only the first failure fails the job, its ancestors
    /// and its tree, in one database write.
    ///
    /// # Panics
    ///
    /// Panics if the outcome is not the one asserted.
    async fn assert_two_failures_fail_the_job_once(
        &self,
        failing_job: FailingJob,
        second_failure: SecondFailure,
    ) {
        let what = format!("{failing_job:?}, second failure {second_failure:?}");
        let tree = TwoFailureTree::start(self, failing_job).await;
        let job_id = tree.job_id;
        let first_instance_id = self.create_instance(job_id, TaskId::Index(0)).await;
        let second_instance_id = self.create_instance(job_id, TaskId::Index(1)).await;
        let job_ids = tree.job_ids();
        let probes = TreeProbes::arm(self, tree.root_id, &job_ids);
        let submit_hook = self.observe(HookPoint::JobEventAfterSubmit, HookFilter::job(job_id));

        let (first, second) = self
            .race_two_failures(
                &tree,
                second_failure,
                [first_instance_id, second_instance_id],
            )
            .await;

        assert_job_result(
            &first,
            JobState::Failed,
            &format!("{what}: the first failure"),
        );
        let (expected_stale, num_submissions) =
            if matches!(second_failure, SecondFailure::AfterReply) {
                (StaleStateError::JobNoLongerRunning, 1)
            } else {
                (StaleStateError::JobAlreadyTerminated(JobState::Failed), 2)
            };
        assert_stale(
            &second,
            &expected_stale,
            &format!("{what}: the second failure"),
        );
        assert_eq!(
            submit_hook.hits(),
            num_submissions,
            "{what}: the submitted failures"
        );
        let num_rounds = if matches!(
            (second_failure, tree.shape),
            (SecondFailure::InFlight, TwoFailureShape::Leaf { .. })
        ) {
            2
        } else {
            1
        };
        probes.assert_rounds(num_rounds, 1, &what);
        probes.assert_transitions(&job_ids.iter().map(|job_id| (*job_id, 1)).collect(), &what);

        let mut expected_errors = HashMap::from([(job_id, FIRST_FAILURE.to_owned())]);
        let mut expected_states = HashMap::from([(job_id, JobState::Failed)]);
        if let TwoFailureShape::Leaf {
            middle_id,
            sibling_id,
        } = tree.shape
        {
            for ancestor_id in [tree.root_id, middle_id] {
                expected_errors.insert(
                    ancestor_id,
                    format!("descendant job {job_id} failed: {FIRST_FAILURE}"),
                );
                expected_states.insert(ancestor_id, JobState::Failed);
            }
            expected_states.insert(sibling_id, JobState::CleanupReady);
        }
        assert_eq!(
            self.tree_errors(tree.root_id).await,
            expected_errors,
            "{what}: the error messages of the tree"
        );
        self.assert_states(&expected_states, &what).await;
        self.assert_tree_rows(tree.root_id, &expected_states, &what)
            .await;
        if let TwoFailureShape::Leaf { sibling_id, .. } = tree.shape {
            self.run_cleanups(&[sibling_id]).await;
            self.assert_states(&HashMap::from([(sibling_id, JobState::Cancelled)]), &what)
                .await;
        }
        self.assert_quiescent(tree.root_id).await;
    }

    /// Reports the failures of tasks 0 and 1 of `tree`'s two-task job, through their instances
    /// `task_instance_ids`, the second as `second_failure` says.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The result of the first failure.
    /// * The result of the second failure.
    async fn race_two_failures(
        &self,
        tree: &TwoFailureTree,
        second_failure: SecondFailure,
        task_instance_ids: [TaskInstanceId; 2],
    ) -> (JobResult, JobResult) {
        let job_id = tree.job_id;
        let [first_instance_id, second_instance_id] = task_instance_ids;
        let first = self.task_failure(job_id, TaskId::Index(0), first_instance_id, FIRST_FAILURE);
        let second =
            self.task_failure(job_id, TaskId::Index(1), second_instance_id, SECOND_FAILURE);
        let (point, filter) = match second_failure {
            SecondFailure::SameRound => (
                HookPoint::CommitterBeforeReceive,
                HookFilter::job(tree.root_id),
            ),
            SecondFailure::InFlight => (
                HookPoint::CommitterRoundAfterDbWrite,
                HookFilter::job(job_id),
            ),
            SecondFailure::AfterReply => {
                let first = first.await;
                return (first, second.await);
            }
        };
        Race::start_on_one_job(self, point, filter, first, second, job_id)
            .await
            .finish()
            .await
    }

    /// Succeeds task 0 of `case`'s job, fails it from `straggler` in `window`, and asserts that the
    /// failure is stale and changes nothing, and that the job's tree then finishes.
    ///
    /// # Panics
    ///
    /// Panics if the outcome is not the one asserted.
    async fn assert_straggler_failure_is_stale(
        &self,
        case: &StragglerCase,
        straggler: Straggler,
        window: StragglerWindow,
        what: &str,
    ) {
        let submit_hook = self.observe(
            HookPoint::JobEventAfterSubmit,
            HookFilter::job(case.tree.job_id()),
        );
        let winner_value = value(0xe1, 0);

        let (success, failure) = self
            .succeed_then_fail(case, window, &winner_value, what)
            .await;

        assert_job_result(
            &success,
            case.state_after_success(),
            &format!("{what}: the success"),
        );
        let expected_stale = match (window, case.tree, straggler) {
            (
                StragglerWindow::AfterReply,
                StragglerTree::Root { .. } | StragglerTree::Leaf { .. },
                _,
            ) => StaleStateError::JobNoLongerRunning,
            (_, _, Straggler::SameInstance) => StaleStateError::InvalidTaskInstanceId,
            (_, _, Straggler::OtherInstance) => {
                StaleStateError::TaskAlreadyTerminated(TaskState::Succeeded)
            }
        };
        assert_stale(&failure, &expected_stale, &format!("{what}: the straggler"));
        assert_eq!(
            submit_hook.hits(),
            usize::from(!matches!(case.tree, StragglerTree::TwoTaskRoot { .. })),
            "{what}: the submitted events, of the success only"
        );
        assert_eq!(
            self.tree_errors(case.tree.root_id()).await,
            HashMap::new(),
            "{what}: the error messages of the tree"
        );
        let mut expected = HashMap::from([(case.tree.job_id(), case.state_after_success())]);
        if let StragglerTree::Leaf {
            root_id,
            sibling_id,
            ..
        } = case.tree
        {
            expected.insert(root_id, JobState::BlockingOnChildren);
            expected.insert(sibling_id, JobState::Running);
        }
        self.assert_states(&expected, what).await;
        self.assert_tree_rows(case.tree.root_id(), &expected, what)
            .await;
        self.finish_straggler_job(case, &winner_value, what).await;
    }

    /// Reports the success of `case`'s winner with the output `winner_value`, then the failure of
    /// its straggler in `window`.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The result of the success.
    /// * The result of the failure.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// * The straggler isn't answered within [`HOOK_TIMEOUT`] while the success is paused.
    /// * A row or the cached root isn't in its state while the success is written but not applied.
    async fn succeed_then_fail(
        &self,
        case: &StragglerCase,
        window: StragglerWindow,
        winner_value: &[u8],
        what: &str,
    ) -> (JobResult, JobResult) {
        let success = self.task_success(
            case.tree.job_id(),
            0,
            case.winner_instance_id,
            winner_value,
            Vec::new(),
        );
        let failure = self.task_failure(
            case.tree.job_id(),
            TaskId::Index(0),
            case.straggler_instance_id,
            STRAGGLER_FAILURE,
        );
        let point = match window {
            StragglerWindow::AfterTcbFlip => HookPoint::TaskSuccessAfterTcbFlip,
            StragglerWindow::InFlight => HookPoint::CommitterRoundAfterDbWrite,
            StragglerWindow::AfterReply => {
                let success = success.await;
                return (success, failure.await);
            }
        };
        let pause_hook = self.pause(point, HookFilter::job(case.tree.job_id()));
        let success_join_handle = tokio::spawn(success);
        reached(&pause_hook, 1).await;
        if matches!(window, StragglerWindow::InFlight) {
            assert_eq!(
                self.row_state(case.tree.job_id()).await,
                JobState::Succeeded,
                "{what}: the row while the success is written but not applied"
            );
            if case.tree.job_id() == case.tree.root_id() {
                assert_eq!(
                    self.service
                        .get_job_state(case.tree.job_id())
                        .await
                        .expect("the root's state should be readable"),
                    JobState::Running,
                    "{what}: the cached root while the success is written but not applied"
                );
            }
        }
        let failure = tokio::time::timeout(HOOK_TIMEOUT, failure)
            .await
            .expect("the straggler should be answered while the success is paused");
        pause_hook.release();
        (join(success_join_handle).await, failure)
    }

    /// Runs the rest of `case`'s tree to success and asserts the job's outputs: `winner_value` from
    /// task 0, followed by task 1's output for a two-task job.
    ///
    /// # Panics
    ///
    /// Panics if a report or a root's outputs read fails, or if an assertion fails.
    async fn finish_straggler_job(&self, case: &StragglerCase, winner_value: &[u8], what: &str) {
        let mut expected_outputs = task_outputs([winner_value]);
        match case.tree {
            StragglerTree::Root { .. } => {}
            StragglerTree::Leaf {
                root_id,
                sibling_id,
                ..
            } => {
                assert_eq!(
                    self.run_task(sibling_id, 0, &value(0xe2, 0)).await,
                    JobState::Succeeded,
                    "{what}: the sibling"
                );
                self.assert_states(
                    &HashMap::from([
                        (root_id, JobState::Succeeded),
                        (sibling_id, JobState::Succeeded),
                    ]),
                    what,
                )
                .await;
            }
            StragglerTree::TwoTaskRoot { job_id } => {
                let last_value = value(0xe1, 1);
                assert_eq!(
                    self.run_task(job_id, 1, &last_value).await,
                    JobState::Succeeded,
                    "{what}: task 1"
                );
                expected_outputs.extend(task_outputs([&last_value]));
                self.assert_states(&HashMap::from([(job_id, JobState::Succeeded)]), what)
                    .await;
            }
        }
        assert_eq!(
            self.row_outputs(case.tree.job_id()).await,
            Some(expected_outputs.clone()),
            "{what}: the job's outputs"
        );
        if case.tree.job_id() == case.tree.root_id() {
            assert_eq!(
                self.service
                    .get_job_outputs(case.tree.job_id())
                    .await
                    .expect("the root's outputs should be readable"),
                expected_outputs,
                "{what}: the root's outputs"
            );
        }
        self.assert_quiescent(case.tree.root_id()).await;
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
async fn sibling_jobs_succeeding_in_any_rounds_advance_their_parent_once() {
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn sibling_tasks_completing_together_complete_their_job_once() {
    let env = TestEnv::start_keeping_task_instances().await;
    for num_tasks in [2, 8] {
        for position in Position::iter() {
            env.assert_sibling_tasks_complete_their_job_once(num_tasks, position)
                .await;
        }
    }
    env.assert_lanes_empty().await;
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_parent_with_a_commit_task_commits_then_climbs() {
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn two_tasks_failing_one_job_fail_it_once() {
    let env = TestEnv::start_keeping_task_instances().await;
    for failing_job in FailingJob::iter() {
        for second_failure in SecondFailure::iter() {
            env.assert_two_failures_fail_the_job_once(failing_job, second_failure)
                .await;
        }
    }
    env.assert_lanes_empty().await;
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_straggler_failure_after_a_success_is_stale() {
    let env = TestEnv::start_keeping_task_instances().await;
    for max_num_retry in [0, 1] {
        for job in StragglerJob::iter() {
            for straggler in Straggler::iter() {
                for window in StragglerWindow::iter() {
                    if matches!(job, StragglerJob::TwoTaskRoot)
                        && matches!(window, StragglerWindow::InFlight)
                    {
                        continue;
                    }
                    let what =
                        format!("{job:?}, {straggler:?}, {window:?}, retries: {max_num_retry}");
                    let case = StragglerCase::start(&env, job, straggler, max_num_retry).await;
                    env.assert_straggler_failure_is_stale(&case, straggler, window, &what)
                        .await;
                }
            }
        }
    }
    env.assert_lanes_empty().await;
    env.finish().await;
}
