//! Faults of jobs, alone and in trees, on `MariaDB`.

use std::collections::HashMap;
use std::time::Duration;

use spider_core::job::JobState;
use spider_core::task::ExecutionPolicy;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_storage::cache::error::StaleStateError;
use spider_storage::job_submission::CompressedJobSubmission;
use spider_storage::limits::MAX_CHILDREN_PER_JOB;
use spider_storage::limits::MAX_DEPTH;
use spider_storage::limits::MAX_JOB_CHILDREN_SIZE;
use spider_storage::limits::MAX_REPORT_CHILDREN_SIZE;
use spider_storage::limits::MAX_REPORT_OUTPUTS_SIZE;
use spider_storage::state::error::ReportRejection;
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
use crate::runtime_infra::Lane;
use crate::runtime_infra::TestEnv;
use crate::runtime_infra::assert_job_result;
use crate::runtime_infra::assert_stale;
use crate::runtime_infra::join;
use crate::runtime_infra::large_child;
use crate::runtime_infra::reached;
use crate::runtime_infra::task_outputs;
use crate::runtime_infra::undecodable_child;
use crate::runtime_infra::value;

/// The number of children of a registering report.
const NUM_CHILDREN: usize = 3;

/// The message of a dropped task failure.
const DROPPED_FAILURE: &str = "dropped failure";

/// Where a test job `J` runs.
#[derive(Debug, Clone, Copy, EnumIter)]
enum Placement {
    /// `J` is a root job alone.
    Alone,

    /// `J` is a leaf of `R → {J, S}`, where `S` has a cleanup task.
    Leaf,
}

/// The tree of a test job `J`, placed as a [`Placement`] says.
enum TestTree {
    /// `J` is a root job alone.
    Alone { job: JobId },

    /// `J` is a leaf of `R → {J, S}`, where `S` has a cleanup task.
    Leaf {
        root: JobId,
        job: JobId,
        sibling: JobId,
    },
}

impl TestTree {
    /// Starts the tree of a job `J` of `shape`, placed as `placement` says, and drains the ready
    /// tasks of its jobs.
    ///
    /// # Returns
    ///
    /// The started tree.
    ///
    /// # Panics
    ///
    /// Panics if the root of a [`Placement::Leaf`] doesn't get two children.
    async fn start(env: &TestEnv, shape: JobShape, placement: Placement) -> Self {
        match placement {
            Placement::Alone => {
                let job_id = env.start_root(&shape).await;
                Self::Alone { job: job_id }
            }
            Placement::Leaf => {
                let root_id = env.start_root(&JobShape::ONE_TASK).await;
                let [job_id, sibling_id] = env
                    .register_children(
                        root_id,
                        &value(0x70, 0),
                        &[shape, JobShape::ONE_TASK_WITH_CLEANUP],
                    )
                    .await[..]
                else {
                    panic!("the root should have two children");
                };
                Self::Leaf {
                    root: root_id,
                    job: job_id,
                    sibling: sibling_id,
                }
            }
        }
    }

    /// Registers a root job `J` of `shape` alone, without starting it.
    ///
    /// # Returns
    ///
    /// The tree.
    async fn register_alone(env: &TestEnv, shape: &JobShape) -> Self {
        let (graph, inputs) = shape.build();
        let job_id = env.register_job(&graph, &inputs).await;
        Self::Alone { job: job_id }
    }

    /// # Returns
    ///
    /// The ID of the tree's root.
    const fn root(&self) -> JobId {
        match self {
            Self::Alone { job } => *job,
            Self::Leaf { root, .. } => *root,
        }
    }

    /// # Returns
    ///
    /// The ID of `J`.
    const fn job(&self) -> JobId {
        match self {
            Self::Alone { job } | Self::Leaf { job, .. } => *job,
        }
    }

    /// # Returns
    ///
    /// The ID of the sibling `S` if the tree has one, or [`None`] otherwise.
    const fn sibling(&self) -> Option<JobId> {
        match self {
            Self::Alone { .. } => None,
            Self::Leaf { sibling, .. } => Some(*sibling),
        }
    }

    /// # Returns
    ///
    /// The states of the tree's jobs once `J` is `job_state` and the root and the sibling are
    /// `rest_state`.
    fn states(&self, job_state: JobState, rest_state: JobState) -> HashMap<JobId, JobState> {
        let mut states = HashMap::from([(self.root(), rest_state), (self.job(), job_state)]);
        if let Some(sibling_id) = self.sibling() {
            states.insert(sibling_id, rest_state);
        }
        states
    }

    /// Runs the sibling's task to success, if the tree has a sibling.
    async fn finish_sibling(&self, env: &TestEnv) {
        if let Some(sibling_id) = self.sibling() {
            assert_eq!(
                env.run_task(sibling_id, 0, &value(0x7f, 0)).await,
                JobState::Succeeded,
                "the sibling's report"
            );
        }
    }

    /// # Returns
    ///
    /// The state that a cancel moves the root to: [`JobState::Cancelled`] for the root of a leaf,
    /// which has no cleanup task, else `root_target`.
    const fn cancelled_root_state(&self, root_target: JobState) -> JobState {
        match self {
            Self::Leaf { .. } => JobState::Cancelled,
            Self::Alone { .. } => root_target,
        }
    }
}

/// An RPC that a test drops while its work is paused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter)]
enum DropCase {
    /// The last task's success report, between its task's success and the count of the job's
    /// incomplete tasks.
    AfterTcbFlip,

    /// As [`Self::AfterTcbFlip`], for a report that registers children.
    AfterTcbFlipRegistering,

    /// As [`Self::AfterTcbFlip`], for the report of task 0 of a two-task job while the report of
    /// task 1 completes in full.
    AfterTcbFlipWhileSiblingTaskReports,

    /// As [`Self::AfterTcbFlip`], for the reports of both tasks of a two-task job.
    AfterTcbFlipBothTasks,

    /// As [`Self::AfterTcbFlip`], while a cancel of the root is written and waits to apply.
    AfterTcbFlipRacingCancel,

    /// The last task's success report, after the job's guard is dropped, before the submit.
    BeforeSubmit,

    /// The success report of the first task of a two-task chain, before its successor is locked.
    BeforeSuccessorLock,

    /// The last task's success report, with its event received by the committer, before the round
    /// that plans it.
    InCommitterBeforeReceive,

    /// The last task's success report, with its round between its database write and its apply.
    InCommitterAfterDbWrite,

    /// A terminal task failure, with its round between its database write and its apply.
    FailureInCommitter,

    /// A commit task's success, with its round between its database write and its apply.
    CommitInCommitter,

    /// A cleanup task's success, with its round between its database write and its apply.
    CleanupInCommitter,

    /// A root cancel, with its round between its database write and its apply.
    CancelInCommitter,

    /// A root start, with its round between its database write and its apply.
    StartInCommitter,

    /// An instance creation, before the instance is sent to the task instance pool.
    CreateInstance,

    /// As [`Self::CreateInstance`], for an execution manager that the pool finds dead.
    CreateInstanceForDeadExecutionManager,
}

impl DropCase {
    /// # Returns
    ///
    /// The shape of the job whose RPC is dropped.
    const fn shape(self) -> JobShape {
        match self {
            Self::AfterTcbFlipWhileSiblingTaskReports | Self::AfterTcbFlipBothTasks => JobShape {
                num_tasks: 2,
                ..JobShape::ONE_TASK
            },
            Self::BeforeSuccessorLock => JobShape {
                num_tasks: 2,
                is_chain: true,
                ..JobShape::ONE_TASK
            },
            Self::CommitInCommitter => JobShape {
                with_commit: true,
                ..JobShape::ONE_TASK
            },
            Self::AfterTcbFlipRacingCancel | Self::CleanupInCommitter => {
                JobShape::ONE_TASK_WITH_CLEANUP
            }
            Self::AfterTcbFlip
            | Self::AfterTcbFlipRegistering
            | Self::BeforeSubmit
            | Self::InCommitterBeforeReceive
            | Self::InCommitterAfterDbWrite
            | Self::FailureInCommitter
            | Self::CancelInCommitter
            | Self::StartInCommitter
            | Self::CreateInstance
            | Self::CreateInstanceForDeadExecutionManager => JobShape::ONE_TASK,
        }
    }

    /// # Returns
    ///
    /// A tuple containing, for the job `J` of a tree placed as `placement` says:
    ///
    /// * The number of events that `J` submits in the case, which include the root's cancel if `J`
    ///   is the root.
    /// * The number of rounds that write a transition of `J` in the case.
    const fn num_submissions_and_writes(self, placement: Placement) -> (usize, usize) {
        let num_cancels = match placement {
            Placement::Alone => 1,
            Placement::Leaf => 0,
        };
        match self {
            Self::AfterTcbFlip
            | Self::AfterTcbFlipWhileSiblingTaskReports
            | Self::AfterTcbFlipBothTasks
            | Self::BeforeSubmit
            | Self::BeforeSuccessorLock
            | Self::InCommitterBeforeReceive
            | Self::InCommitterAfterDbWrite
            | Self::FailureInCommitter
            | Self::CreateInstance
            | Self::CreateInstanceForDeadExecutionManager => (1, 1),
            Self::AfterTcbFlipRegistering => (1, 2),
            Self::AfterTcbFlipRacingCancel => (num_cancels + 2, 2),
            Self::CommitInCommitter | Self::StartInCommitter => (2, 2),
            Self::CleanupInCommitter => (num_cancels + 1, 2),
            Self::CancelInCommitter => (num_cancels, 1),
        }
    }
}

/// How the tree of a dropped RPC ends.
struct DropOutcome {
    /// The final state of `J`.
    job_state: JobState,

    /// The final state of the root and the sibling, unless `J` is the root.
    rest_state: JobState,

    /// The values of the outputs that `J` stores, if any.
    output_values: Option<Vec<Vec<u8>>>,
}

impl DropOutcome {
    /// # Returns
    ///
    /// The outcome of a tree that succeeds, with `J` storing the outputs of `output_values`.
    const fn succeeded(output_values: Vec<Vec<u8>>) -> Self {
        Self {
            job_state: JobState::Succeeded,
            rest_state: JobState::Succeeded,
            output_values: Some(output_values),
        }
    }

    /// # Returns
    ///
    /// The outcome of a tree that ends cancelled.
    const fn cancelled() -> Self {
        Self {
            job_state: JobState::Cancelled,
            rest_state: JobState::Cancelled,
            output_values: None,
        }
    }
}

/// One dropped RPC in its tree, with observers of the events that `J` submits and of the rounds
/// that write a transition of `J`.
///
/// # Type Parameters
///
/// * `'env_lifetime` - The lifetime of the test environment that the RPC runs in.
struct DropRun<'env_lifetime> {
    env: &'env_lifetime TestEnv,
    tree: TestTree,
    submissions: ArmedHook,
    writes: ArmedHook,
    what: String,
}

impl DropRun<'_> {
    /// Drops the RPC of `case` while its work is paused, releases the work, and drives the tree to
    /// its end without repeating the dropped RPC.
    ///
    /// # Returns
    ///
    /// How the tree ended.
    async fn drop_rpc(
        &self,
        case: DropCase,
        dead_execution_manager_id: ExecutionManagerId,
    ) -> DropOutcome {
        match case {
            DropCase::AfterTcbFlip => {
                self.drop_last_report(HookPoint::TaskSuccessAfterTcbFlip)
                    .await
            }
            DropCase::BeforeSubmit => {
                self.drop_last_report(HookPoint::TaskSuccessBeforeOutputsCommit)
                    .await
            }
            DropCase::AfterTcbFlipRegistering => self.drop_registering_report().await,
            DropCase::AfterTcbFlipWhileSiblingTaskReports => {
                self.drop_report_while_sibling_task_reports().await
            }
            DropCase::AfterTcbFlipBothTasks => self.drop_both_reports().await,
            DropCase::AfterTcbFlipRacingCancel => self.drop_report_racing_cancel().await,
            DropCase::BeforeSuccessorLock => self.drop_report_before_successor_lock().await,
            DropCase::InCommitterBeforeReceive => {
                self.drop_report_in_committer(self.env.pause(
                    HookPoint::CommitterBeforeReceive,
                    HookFilter::job(self.tree.root()),
                ))
                .await
            }
            DropCase::InCommitterAfterDbWrite => {
                self.drop_report_in_committer(self.pause_write_of_job())
                    .await
            }
            DropCase::FailureInCommitter => self.drop_failure_in_committer().await,
            DropCase::CommitInCommitter => self.drop_commit_in_committer().await,
            DropCase::CleanupInCommitter => self.drop_cleanup_in_committer().await,
            DropCase::CancelInCommitter => self.drop_cancel_in_committer().await,
            DropCase::StartInCommitter => self.drop_start_in_committer().await,
            DropCase::CreateInstance => {
                self.drop_instance_creation(self.env.execution_manager_id, false)
                    .await
            }
            DropCase::CreateInstanceForDeadExecutionManager => {
                self.drop_instance_creation(dead_execution_manager_id, true)
                    .await
            }
        }
    }

    /// Asserts that the tree ended as `outcome` says, with `J`'s registered children succeeded,
    /// with exactly the submissions of `J` and the rounds writing `J` that `case` calls for, no
    /// lane entry left, and no fatal shutdown.
    async fn assert_outcome(&self, case: DropCase, placement: Placement, outcome: &DropOutcome) {
        let (env, what, root_id, job_id) = (
            self.env,
            self.what.as_str(),
            self.tree.root(),
            self.tree.job(),
        );
        env.wait_for_job_state(root_id, outcome.rest_state).await;
        let mut states = self.tree.states(outcome.job_state, outcome.rest_state);
        let mut errors = HashMap::new();
        if case == DropCase::FailureInCommitter {
            errors.insert(job_id, DROPPED_FAILURE.to_owned());
            if let Some(sibling_id) = self.tree.sibling() {
                states.insert(sibling_id, JobState::Cancelled);
                errors.insert(
                    root_id,
                    format!("descendant job {job_id} failed: {DROPPED_FAILURE}"),
                );
            }
        }
        for child_id in env.children_of(job_id).await {
            states.insert(child_id, JobState::Succeeded);
        }
        env.assert_tree_rows(root_id, &states, what).await;
        env.assert_states(&states, what).await;
        assert_eq!(
            env.row_outputs(job_id).await,
            outcome.output_values.as_ref().map(task_outputs),
            "{what}: the job's stored outputs"
        );
        assert_eq!(env.tree_errors(root_id).await, errors, "{what}: the errors");
        assert_eq!(
            (self.submissions.hits(), self.writes.hits()),
            case.num_submissions_and_writes(placement),
            "{what}: the job's submissions and written rounds"
        );
        env.assert_lanes_empty().await;
        env.assert_quiescent(root_id).await;
    }

    /// Drops the last task's success report at `point`.
    async fn drop_last_report(&self, point: HookPoint) -> DropOutcome {
        let (env, job_id) = (self.env, self.tree.job());
        let paused = env.pause(point, HookFilter::job(job_id));
        let task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
        drop_rpc_at(
            &paused,
            env.task_success(job_id, 0, task_instance_id, &value(0x71, 0), Vec::new()),
        )
        .await;
        assert_eq!(
            self.submissions.hits(),
            0,
            "{}: submitted while paused",
            self.what
        );
        paused.release();
        reached(&self.submissions, 1).await;
        self.tree.finish_sibling(env).await;
        DropOutcome::succeeded(vec![value(0x71, 0)])
    }

    /// Drops a registering last report after its task's success.
    async fn drop_registering_report(&self) -> DropOutcome {
        let (env, job_id) = (self.env, self.tree.job());
        let paused = env.pause(HookPoint::TaskSuccessAfterTcbFlip, HookFilter::job(job_id));
        let task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
        drop_rpc_at(
            &paused,
            env.task_success(
                job_id,
                0,
                task_instance_id,
                &value(0x72, 0),
                (0..NUM_CHILDREN)
                    .map(|_| JobShape::ONE_TASK.child())
                    .collect(),
            ),
        )
        .await;
        assert_eq!(env.children_of(job_id).await, Vec::new());
        paused.release();
        reached(&self.writes, 1).await;
        let child_ids = env.children_of(job_id).await;
        assert_eq!(child_ids.len(), NUM_CHILDREN, "{}: the children", self.what);
        let ready_tasks: Vec<_> = child_ids
            .iter()
            .flat_map(|child_id| JobShape::ONE_TASK.ready_tasks(*child_id))
            .collect();
        env.drain_lane(Lane::Task, &ready_tasks).await;
        for (index, child_id) in child_ids.iter().enumerate() {
            assert_eq!(
                env.run_task(*child_id, 0, &value(0x73, index)).await,
                JobState::Succeeded
            );
        }
        self.tree.finish_sibling(env).await;
        DropOutcome::succeeded(vec![value(0x72, 0)])
    }

    /// Drops the report of task 0 of a two-task job after its task's success, and reports task 1 in
    /// full meanwhile.
    async fn drop_report_while_sibling_task_reports(&self) -> DropOutcome {
        let (env, job_id) = (self.env, self.tree.job());
        let paused = env.pause(HookPoint::TaskSuccessAfterTcbFlip, HookFilter::job(job_id));
        let first_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
        let second_instance_id = env.create_instance(job_id, TaskId::Index(1)).await;
        drop_rpc_at(
            &paused,
            env.task_success(job_id, 0, first_instance_id, &value(0x74, 0), Vec::new()),
        )
        .await;
        assert_job_result(
            &env.task_success(job_id, 1, second_instance_id, &value(0x74, 1), Vec::new())
                .await,
            JobState::Running,
            &format!("{}: the second task's report", self.what),
        );
        assert_eq!(
            self.submissions.hits(),
            0,
            "{}: submitted while paused",
            self.what
        );
        paused.release();
        reached(&self.submissions, 1).await;
        self.tree.finish_sibling(env).await;
        DropOutcome::succeeded(vec![value(0x74, 0), value(0x74, 1)])
    }

    /// Drops the reports of both tasks of a two-task job after their tasks' successes.
    async fn drop_both_reports(&self) -> DropOutcome {
        let (env, job_id) = (self.env, self.tree.job());
        let paused = env.hooks.arm(
            HookPoint::TaskSuccessAfterTcbFlip,
            HookFilter::job(job_id),
            HookAction::Pause,
            HookCount::Times(2),
        );
        let first_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
        let second_instance_id = env.create_instance(job_id, TaskId::Index(1)).await;
        let report_join_handles = [
            tokio::spawn(env.task_success(
                job_id,
                0,
                first_instance_id,
                &value(0x75, 0),
                Vec::new(),
            )),
            tokio::spawn(env.task_success(
                job_id,
                1,
                second_instance_id,
                &value(0x75, 1),
                Vec::new(),
            )),
        ];
        reached(&paused, 2).await;
        for report_join_handle in report_join_handles {
            report_join_handle.abort();
            assert!(
                tokio::time::timeout(HOOK_TIMEOUT, report_join_handle)
                    .await
                    .expect("the aborted RPC's task should end within the timeout")
                    .expect_err("the dropped RPC should not complete")
                    .is_cancelled()
            );
        }
        paused.release();
        paused.release();
        reached(&self.submissions, 1).await;
        self.tree.finish_sibling(env).await;
        DropOutcome::succeeded(vec![value(0x75, 0), value(0x75, 1)])
    }

    /// Drops the last report after its task's success, holding the job's guard, while a cancel of
    /// the root is written and waits to apply.
    async fn drop_report_racing_cancel(&self) -> DropOutcome {
        let (env, root_id, job_id) = (self.env, self.tree.root(), self.tree.job());
        let paused = env.pause(HookPoint::TaskSuccessAfterTcbFlip, HookFilter::job(job_id));
        let cancel_written = env.observe(
            HookPoint::CommitterRoundAfterDbWrite,
            HookFilter::job(root_id),
        );
        let apply_contended = env.observe(
            HookPoint::TreeStateApplyLockContended,
            HookFilter::job(job_id),
        );
        let task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
        drop_rpc_at(
            &paused,
            env.task_success(job_id, 0, task_instance_id, &value(0x76, 0), Vec::new()),
        )
        .await;
        let num_cancel_submissions = usize::from(matches!(self.tree, TestTree::Alone { .. }));
        let cancel_join_handle = tokio::spawn(env.cancel(root_id));
        reached(&cancel_written, 1).await;
        reached(&apply_contended, 1).await;
        assert!(
            !cancel_join_handle.is_finished(),
            "{}: the cancel should wait for the job's guard to apply",
            self.what
        );
        paused.release();
        assert_job_result(
            &join(cancel_join_handle).await,
            self.tree.cancelled_root_state(JobState::CleanupReady),
            &format!("{}: the cancel", self.what),
        );
        reached(&self.submissions, num_cancel_submissions + 1).await;
        let mut cleanup_ids = vec![job_id];
        cleanup_ids.extend(self.tree.sibling());
        env.run_cleanups(&cleanup_ids).await;
        DropOutcome::cancelled()
    }

    /// Drops the report of the first task of a two-task chain before its successor is locked.
    async fn drop_report_before_successor_lock(&self) -> DropOutcome {
        let (env, job_id) = (self.env, self.tree.job());
        let paused = env.pause(
            HookPoint::TaskSuccessBeforeSuccessorLock,
            HookFilter::job(job_id),
        );
        let task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
        drop_rpc_at(
            &paused,
            env.task_success(job_id, 0, task_instance_id, &value(0x77, 0), Vec::new()),
        )
        .await;
        paused.release();
        env.drain_lane(Lane::Task, &[(job_id, TaskId::Index(1))])
            .await;
        assert_eq!(
            env.run_task(job_id, 1, &value(0x77, 1)).await,
            JobState::Succeeded
        );
        self.tree.finish_sibling(env).await;
        DropOutcome::succeeded(vec![value(0x77, 1)])
    }

    /// Drops the last report while its event is held in the committer by `paused`.
    async fn drop_report_in_committer(&self, paused: ArmedHook) -> DropOutcome {
        let (env, job_id) = (self.env, self.tree.job());
        let task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
        drop_rpc_at(
            &paused,
            env.task_success(job_id, 0, task_instance_id, &value(0x78, 0), Vec::new()),
        )
        .await;
        paused.release();
        reached(&self.writes, 1).await;
        self.tree.finish_sibling(env).await;
        DropOutcome::succeeded(vec![value(0x78, 0)])
    }

    /// Drops a terminal failure while its round is between its database write and its apply.
    async fn drop_failure_in_committer(&self) -> DropOutcome {
        let (env, job_id) = (self.env, self.tree.job());
        let paused = self.pause_write_of_job();
        let task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
        drop_rpc_at(
            &paused,
            env.task_failure(job_id, TaskId::Index(0), task_instance_id, DROPPED_FAILURE),
        )
        .await;
        paused.release();
        if let Some(sibling_id) = self.tree.sibling() {
            env.run_cleanups(&[sibling_id]).await;
        }
        DropOutcome {
            job_state: JobState::Failed,
            rest_state: JobState::Failed,
            output_values: None,
        }
    }

    /// Drops a commit success while its round is between its database write and its apply.
    async fn drop_commit_in_committer(&self) -> DropOutcome {
        let (env, job_id) = (self.env, self.tree.job());
        assert_eq!(
            env.run_task(job_id, 0, &value(0x79, 0)).await,
            JobState::CommitReady
        );
        let (task_instance_id, _) = env.commit_instance(job_id).await;
        let paused = self.pause_write_of_job();
        drop_rpc_at(&paused, env.commit_success(job_id, task_instance_id)).await;
        paused.release();
        reached(&self.writes, 2).await;
        self.tree.finish_sibling(env).await;
        DropOutcome::succeeded(vec![value(0x79, 0)])
    }

    /// Drops a cleanup success while its round is between its database write and its apply.
    async fn drop_cleanup_in_committer(&self) -> DropOutcome {
        let (env, root_id, job_id) = (self.env, self.tree.root(), self.tree.job());
        assert_job_result(
            &env.cancel(root_id).await,
            self.tree.cancelled_root_state(JobState::CleanupReady),
            &format!("{}: the cancel", self.what),
        );
        let mut cleanups = vec![(job_id, TaskId::Cleanup)];
        cleanups.extend(
            self.tree
                .sibling()
                .map(|sibling_id| (sibling_id, TaskId::Cleanup)),
        );
        env.drain_lane(Lane::Cleanup, &cleanups).await;
        let paused = self.pause_write_of_job();
        let task_instance_id = env.create_instance(job_id, TaskId::Cleanup).await;
        drop_rpc_at(&paused, env.cleanup_success(job_id, task_instance_id)).await;
        paused.release();
        reached(&self.writes, 2).await;
        if let Some(sibling_id) = self.tree.sibling() {
            env.run_cleanup(sibling_id).await;
        }
        DropOutcome::cancelled()
    }

    /// Drops a root cancel while its round is between its database write and its apply.
    async fn drop_cancel_in_committer(&self) -> DropOutcome {
        let env = self.env;
        let paused = self.pause_write_of_job();
        drop_rpc_at(&paused, env.cancel(self.tree.root())).await;
        paused.release();
        if let Some(sibling_id) = self.tree.sibling() {
            env.run_cleanups(&[sibling_id]).await;
        }
        DropOutcome::cancelled()
    }

    /// Drops a root start while its round is between its database write and its apply.
    async fn drop_start_in_committer(&self) -> DropOutcome {
        let (env, job_id) = (self.env, self.tree.job());
        let paused = self.pause_write_of_job();
        let service = env.service.clone();
        drop_rpc_at(&paused, async move { service.start_job(job_id).await }).await;
        paused.release();
        env.drain_lane(Lane::Task, &[(job_id, TaskId::Index(0))])
            .await;
        assert_eq!(
            env.run_task(job_id, 0, &value(0x7a, 0)).await,
            JobState::Succeeded
        );
        DropOutcome::succeeded(vec![value(0x7a, 0)])
    }

    /// Drops an instance creation for `execution_manager_id` before the instance is sent to the
    /// task instance pool. If `is_dead`, the pool finds the execution manager dead, which proves
    /// that the dropped creation still sent the instance: the pool removes it and republishes its
    /// task.
    async fn drop_instance_creation(
        &self,
        execution_manager_id: ExecutionManagerId,
        is_dead: bool,
    ) -> DropOutcome {
        let (env, job_id) = (self.env, self.tree.job());
        let paused = env.pause(
            HookPoint::CreateInstanceBeforePoolSend,
            HookFilter::job(job_id),
        );
        let service = env.service.clone();
        drop_rpc_at(&paused, async move {
            service
                .create_task_instance(
                    service.session_id(),
                    job_id,
                    TaskId::Index(0),
                    execution_manager_id,
                )
                .await
                .map(|execution_context| execution_context.task_instance_id)
        })
        .await;
        let dropped_instance_id = paused
            .last_context()
            .and_then(|context| context.task_instance_id)
            .expect("the hook context should carry the instance ID");
        paused.release();
        let task_instance_id = if is_dead {
            env.drain_lane(Lane::Task, &[(job_id, TaskId::Index(0))])
                .await;
            assert_stale(
                &env.task_success(job_id, 0, dropped_instance_id, &value(0x7b, 0), Vec::new())
                    .await,
                &StaleStateError::InvalidTaskInstanceId,
                &format!("{}: the report of the removed instance", self.what),
            );
            env.create_instance(job_id, TaskId::Index(0)).await
        } else {
            dropped_instance_id
        };
        assert_job_result(
            &env.task_success(job_id, 0, task_instance_id, &value(0x7b, 0), Vec::new())
                .await,
            JobState::Succeeded,
            &format!("{}: the report", self.what),
        );
        self.tree.finish_sibling(env).await;
        DropOutcome::succeeded(vec![value(0x7b, 0)])
    }

    /// # Returns
    ///
    /// A pause of the next round that writes a transition of `J`, between its database write and
    /// its apply.
    fn pause_write_of_job(&self) -> ArmedHook {
        self.env.pause(
            HookPoint::CommitterRoundAfterDbWrite,
            HookFilter::job(self.tree.job()),
        )
    }
}

/// A success report that breaks a limit of the storage service.
#[derive(Debug, Clone, Copy, EnumIter)]
enum LimitCase {
    /// One child more than a job may have.
    TooManyChildren,

    /// A child over the database's statement payload.
    ChildTooLarge,

    /// Children over the per-report size limit.
    ChildrenTooLarge,

    /// A child that does not decode.
    InvalidChild,

    /// Outputs over the per-report size limit.
    OutputsTooLarge,
}

/// The error message that a failure stores.
#[derive(Debug)]
enum ExpectedError {
    /// The message is exactly this.
    Exact(String),

    /// The message starts with this and goes on.
    StartingWith(String),
}

/// The report of a [`LimitCase`]: its output, its children and the error it fails its job with.
struct LimitReport {
    output: Vec<u8>,
    children: fn() -> Vec<CompressedJobSubmission>,
    error: ExpectedError,
}

impl LimitReport {
    /// # Returns
    ///
    /// The report of `case`.
    ///
    /// # Panics
    ///
    /// Panics if a limit doesn't fit in a `usize`.
    fn new(case: LimitCase) -> Self {
        const MIB: usize = 1024 * 1024;
        match case {
            LimitCase::TooManyChildren => Self {
                output: value(0x80, 0),
                children: || {
                    (0..=MAX_CHILDREN_PER_JOB)
                        .map(|_| JobShape::ONE_TASK.child())
                        .collect()
                },
                error: ExpectedError::Exact(
                    ReportRejection::JobChildrenTooLarge {
                        count: MAX_CHILDREN_PER_JOB + 1,
                        size: u64::try_from(MAX_CHILDREN_PER_JOB + 1)
                            .expect("the count should fit in a `u64`")
                            * JobShape::ONE_TASK.child().size(),
                        count_limit: MAX_CHILDREN_PER_JOB,
                        size_limit: MAX_JOB_CHILDREN_SIZE,
                    }
                    .to_string(),
                ),
            },
            LimitCase::ChildTooLarge => Self {
                output: value(0x80, 0),
                children: || vec![JobShape::ONE_TASK.child(), undecodable_child(4 * MIB + 1)],
                error: ExpectedError::StartingWith(format!(
                    "child job 1 of {} bytes exceeds the limit of ",
                    4 * MIB + 1
                )),
            },
            LimitCase::ChildrenTooLarge => Self {
                output: value(0x80, 0),
                children: || {
                    (0..5)
                        .map(|_| undecodable_child(3 * MIB + MIB / 2))
                        .collect()
                },
                error: ExpectedError::Exact(
                    ReportRejection::ChildrenTooLarge {
                        size: 5 * u64::try_from(3 * MIB + MIB / 2)
                            .expect("the size should fit in a `u64`"),
                        limit: MAX_REPORT_CHILDREN_SIZE,
                    }
                    .to_string(),
                ),
            },
            LimitCase::InvalidChild => Self {
                output: value(0x80, 0),
                children: || vec![JobShape::ONE_TASK.child(), undecodable_child(16)],
                error: ExpectedError::StartingWith("child job 1 is invalid: ".to_owned()),
            },
            LimitCase::OutputsTooLarge => {
                let output = vec![
                    0x5a;
                    usize::try_from(MAX_REPORT_OUTPUTS_SIZE)
                        .expect("the limit should fit in a `usize`")
                ];
                let size: usize = task_outputs([&output]).iter().map(Vec::len).sum();
                Self {
                    output,
                    children: Vec::new,
                    error: ExpectedError::Exact(
                        ReportRejection::OutputsTooLarge {
                            size: u64::try_from(size).expect("the size should fit in a `u64`"),
                            limit: MAX_REPORT_OUTPUTS_SIZE,
                        }
                        .to_string(),
                    ),
                }
            }
        }
    }
}

/// How two reports whose children together exceed the job's child limits interleave.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter)]
enum ReservationRace {
    /// Both reports run at once.
    Free,

    /// The first report is held after its task's success, holding its job's guard, while the second
    /// report is rejected and, without a retry left, its failure is written.
    FirstHeldAfterTcbFlip,

    /// The first report completes before the second starts.
    Sequential,
}

impl ReservationRace {
    /// # Returns
    ///
    /// The number of times the race runs: the free race contends, the others are ordered.
    const fn num_iterations(self) -> usize {
        match self {
            Self::Free => 10,
            Self::FirstHeldAfterTcbFlip | Self::Sequential => 1,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_dropped_rpc_never_strands_its_job() -> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    let dead_execution_manager_id = env.register_dead_execution_manager().await;
    for case in DropCase::iter() {
        for placement in Placement::iter() {
            let tree = match (case, placement) {
                (DropCase::StartInCommitter, Placement::Alone) => {
                    TestTree::register_alone(&env, &case.shape()).await
                }
                (DropCase::StartInCommitter, Placement::Leaf) => continue,
                (
                    DropCase::AfterTcbFlip
                    | DropCase::AfterTcbFlipRegistering
                    | DropCase::AfterTcbFlipWhileSiblingTaskReports
                    | DropCase::AfterTcbFlipBothTasks
                    | DropCase::AfterTcbFlipRacingCancel
                    | DropCase::BeforeSubmit
                    | DropCase::BeforeSuccessorLock
                    | DropCase::InCommitterBeforeReceive
                    | DropCase::InCommitterAfterDbWrite
                    | DropCase::FailureInCommitter
                    | DropCase::CommitInCommitter
                    | DropCase::CleanupInCommitter
                    | DropCase::CancelInCommitter
                    | DropCase::CreateInstance
                    | DropCase::CreateInstanceForDeadExecutionManager,
                    Placement::Alone | Placement::Leaf,
                ) => TestTree::start(&env, case.shape(), placement).await,
            };
            let run = DropRun {
                env: &env,
                submissions: env
                    .observe(HookPoint::JobEventAfterSubmit, HookFilter::job(tree.job())),
                writes: env.observe(
                    HookPoint::CommitterRoundAfterDbWrite,
                    HookFilter::job(tree.job()),
                ),
                tree,
                what: format!("{case:?}, {placement:?}"),
            };
            let outcome = run.drop_rpc(case, dead_execution_manager_id).await;
            run.assert_outcome(case, placement, &outcome).await;
        }
    }
    env.finish().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_report_over_a_limit_fails_its_job_and_tree_but_not_the_service() -> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    for case in LimitCase::iter() {
        let report = LimitReport::new(case);
        for max_num_retry in [0, 1] {
            for placement in Placement::iter() {
                let what = format!("{case:?}, {max_num_retry} retries, {placement:?}");
                let tree = TestTree::start(
                    &env,
                    JobShape {
                        execution_policy: Some(ExecutionPolicy {
                            max_num_retry,
                            ..ExecutionPolicy::default()
                        }),
                        ..JobShape::ONE_TASK
                    },
                    placement,
                )
                .await;
                let bystander_id = env.start_root(&JobShape::ONE_TASK).await;
                let job_id = tree.job();
                for attempt in 0..=max_num_retry {
                    if attempt > 0 {
                        env.drain_lane(Lane::Task, &[(job_id, TaskId::Index(0))])
                            .await;
                    }
                    let expected = if attempt == max_num_retry {
                        JobState::Failed
                    } else {
                        JobState::Running
                    };
                    let task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
                    assert_job_result(
                        &env.task_success(
                            job_id,
                            0,
                            task_instance_id,
                            &report.output,
                            (report.children)(),
                        )
                        .await,
                        expected,
                        &format!("{what}: attempt {attempt}"),
                    );
                    assert_eq!(
                        env.children_of(job_id).await,
                        Vec::new(),
                        "{what}: the children after attempt {attempt}"
                    );
                }
                assert_failed_for_limit(&env, &tree, &report.error, bystander_id, &what).await;
            }
        }
    }
    env.finish().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn reports_racing_over_the_job_child_limit_fail_one_task_only() -> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    let large = large_child(usize::try_from(MAX_JOB_CHILDREN_SIZE)? / 5);
    let child_size = large.size();
    assert!(
        3 * child_size <= MAX_REPORT_CHILDREN_SIZE && 6 * child_size > MAX_JOB_CHILDREN_SIZE,
        "a child of {child_size} bytes does not fit the test"
    );
    for race in ReservationRace::iter() {
        for iteration in 0..race.num_iterations() {
            for max_num_retry in [0, 1] {
                if race == ReservationRace::Sequential && max_num_retry == 0 {
                    continue;
                }
                let what = format!("{race:?}, {max_num_retry} retries, iteration {iteration}");
                assert_reservation_race(&env, &large, race, max_num_retry, &what).await;
            }
        }
    }
    env.finish().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_report_beyond_the_maximum_depth_fails_the_whole_chain() -> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    let root_id = env.start_root(&JobShape::ONE_TASK).await;
    let [first_child_id, sibling_id] = env
        .register_children(
            root_id,
            &value(0x90, 0),
            &[JobShape::ONE_TASK, JobShape::ONE_TASK_WITH_CLEANUP],
        )
        .await[..]
    else {
        panic!("the root should have two children");
    };
    let mut chain = vec![root_id, first_child_id];
    for depth in 2..=MAX_DEPTH {
        let parent_id = *chain.last().expect("the chain should not be empty");
        let depth = usize::try_from(depth)?;
        chain.push(
            env.register_children(parent_id, &value(0x91, depth), &[JobShape::ONE_TASK])
                .await[0],
        );
    }
    let deepest_id = *chain.last().expect("the chain should not be empty");
    let task_instance_id = env.create_instance(deepest_id, TaskId::Index(0)).await;
    assert_job_result(
        &env.task_success(
            deepest_id,
            0,
            task_instance_id,
            &value(0x92, 0),
            vec![JobShape::ONE_TASK.child()],
        )
        .await,
        JobState::Failed,
        "the report beyond the maximum depth",
    );
    assert_eq!(env.children_of(deepest_id).await, Vec::new());

    let error = ReportRejection::TooDeep {
        depth: MAX_DEPTH + 1,
        limit: MAX_DEPTH,
    }
    .to_string();
    let propagated_error = format!("descendant job {deepest_id} failed: {error}");
    let mut expected_errors: HashMap<JobId, String> = chain
        .iter()
        .map(|job_id| (*job_id, propagated_error.clone()))
        .collect();
    expected_errors.insert(deepest_id, error);
    assert_eq!(env.tree_errors(root_id).await, expected_errors);
    let mut expected: HashMap<JobId, JobState> = chain
        .iter()
        .map(|job_id| (*job_id, JobState::Failed))
        .collect();
    expected.insert(sibling_id, JobState::CleanupReady);
    env.assert_tree_rows(root_id, &expected, "after the report")
        .await;
    env.assert_states(&expected, "after the report").await;

    env.run_cleanups(&[sibling_id]).await;
    expected.insert(sibling_id, JobState::Cancelled);
    env.assert_states(&expected, "after the sibling's cleanup")
        .await;
    env.assert_quiescent(root_id).await;
    env.assert_lanes_empty().await;
    env.finish().await;
    Ok(())
}

/// Asserts that a report over a limit failed the job `J` of `tree` with `error`, failed the root
/// with the propagated message and aborted the sibling, whose cleanup then runs; that nothing of
/// the report is stored; and that the bystander tree `bystander_id` still runs.
async fn assert_failed_for_limit(
    env: &TestEnv,
    tree: &TestTree,
    error: &ExpectedError,
    bystander_id: JobId,
    what: &str,
) {
    let (root_id, job_id) = (tree.root(), tree.job());
    let errors = env.tree_errors(root_id).await;
    let job_error = errors
        .get(&job_id)
        .unwrap_or_else(|| panic!("{what}: the failed job should have an error message"))
        .clone();
    match error {
        ExpectedError::Exact(message) => assert_eq!(&job_error, message, "{what}: the job's error"),
        ExpectedError::StartingWith(prefix) => assert!(
            job_error.starts_with(prefix) && job_error.len() > prefix.len(),
            "{what}: unexpected error `{job_error}`"
        ),
    }
    let mut expected_errors = HashMap::from([(job_id, job_error.clone())]);
    let mut states = tree.states(JobState::Failed, JobState::Failed);
    if let Some(sibling_id) = tree.sibling() {
        expected_errors.insert(
            root_id,
            format!("descendant job {job_id} failed: {job_error}"),
        );
        states.insert(sibling_id, JobState::CleanupReady);
        env.assert_tree_rows(root_id, &states, what).await;
        env.assert_states(&states, what).await;
        env.run_cleanups(&[sibling_id]).await;
        states.insert(sibling_id, JobState::Cancelled);
    }
    assert_eq!(errors, expected_errors, "{what}: the tree's errors");
    env.assert_tree_rows(root_id, &states, what).await;
    env.assert_states(&states, what).await;
    assert_eq!(
        env.row_outputs(job_id).await,
        None,
        "{what}: the stored outputs"
    );
    assert_eq!(
        env.run_task(bystander_id, 0, &value(0x81, 0)).await,
        JobState::Succeeded,
        "{what}: the bystander"
    );
    env.assert_lanes_empty().await;
    env.assert_quiescent(root_id).await;
}

/// Races two reports of the two tasks of one root job as `race` says, each with [`NUM_CHILDREN`]
/// children of `large`, whose union exceeds [`MAX_JOB_CHILDREN_SIZE`], and asserts that exactly one
/// task fails for it.
async fn assert_reservation_race(
    env: &TestEnv,
    large: &CompressedJobSubmission,
    race: ReservationRace,
    max_num_retry: u32,
    what: &str,
) {
    let job_id = env
        .start_root(&JobShape {
            num_tasks: 2,
            execution_policy: Some(ExecutionPolicy {
                max_num_retry,
                ..ExecutionPolicy::default()
            }),
            ..JobShape::ONE_TASK
        })
        .await;
    let (winner, results) = race_reports(env, job_id, large, race, max_num_retry, what).await;
    let loser = 1 - winner;
    let loser_state = if max_num_retry == 0 {
        JobState::Failed
    } else {
        JobState::Running
    };
    assert_job_result(&results[loser], loser_state, &format!("{what}: the loser"));
    match (&results[winner], race) {
        (Ok(JobState::Running), _) => {}
        (result, ReservationRace::Free) if max_num_retry == 0 => assert_stale(
            result,
            &StaleStateError::JobNoLongerRunning,
            &format!("{what}: the winner"),
        ),
        (result, _) => panic!("{what}: unexpected winner result {result:?}"),
    }
    assert_eq!(env.children_of(job_id).await, Vec::new());

    if max_num_retry == 0 {
        let error = ReportRejection::JobChildrenTooLarge {
            count: 2 * NUM_CHILDREN,
            size: u64::try_from(2 * NUM_CHILDREN).expect("the count should fit in a `u64`")
                * large.size(),
            count_limit: MAX_CHILDREN_PER_JOB,
            size_limit: MAX_JOB_CHILDREN_SIZE,
        }
        .to_string();
        assert_eq!(
            env.tree_errors(job_id).await,
            HashMap::from([(job_id, error)]),
            "{what}: the job's error"
        );
        let expected = HashMap::from([(job_id, JobState::Failed)]);
        env.assert_tree_rows(job_id, &expected, what).await;
        env.assert_states(&expected, what).await;
        assert_eq!(env.row_outputs(job_id).await, None);
        env.assert_lanes_empty().await;
        return;
    }
    if race != ReservationRace::Free {
        env.drain_lane(Lane::Task, &[(job_id, TaskId::Index(loser))])
            .await;
    }
    finish_after_rejected_report(env, job_id, loser, what).await;
}

/// Races the reports of the two tasks of `job_id` as `race` says, each with [`NUM_CHILDREN`]
/// children of `large`.
///
/// # Returns
///
/// A tuple containing:
///
/// * The index of the task whose children were reserved.
/// * The results of the reports of task 0 and task 1.
async fn race_reports(
    env: &TestEnv,
    job_id: JobId,
    large: &CompressedJobSubmission,
    race: ReservationRace,
    max_num_retry: u32,
    what: &str,
) -> (usize, [JobResult; 2]) {
    let task_instance_ids: [TaskInstanceId; 2] = [
        env.create_instance(job_id, TaskId::Index(0)).await,
        env.create_instance(job_id, TaskId::Index(1)).await,
    ];
    let report = |task_index: usize| {
        env.task_success(
            job_id,
            task_index,
            task_instance_ids[task_index],
            &value(0xa0, task_index),
            (0..NUM_CHILDREN)
                .map(|_| CompressedJobSubmission {
                    compressed_serialized_task_graph: large
                        .compressed_serialized_task_graph
                        .clone(),
                    compressed_serialized_job_inputs: large
                        .compressed_serialized_job_inputs
                        .clone(),
                })
                .collect(),
        )
    };
    match race {
        ReservationRace::Free => {
            let first_join_handle = tokio::spawn(report(0));
            let second_join_handle = tokio::spawn(report(1));
            let results = [
                join(first_join_handle).await,
                join(second_join_handle).await,
            ];
            let loser = if max_num_retry == 0 {
                let losers: Vec<usize> = (0..2)
                    .filter(|index| matches!(results[*index], Ok(JobState::Failed)))
                    .collect();
                assert_eq!(losers.len(), 1, "{what}: one loser, got {results:?}");
                losers[0]
            } else {
                let retried = env.poll_lane(Lane::Task, 1, Duration::from_secs(10)).await;
                let [(retried_job_id, TaskId::Index(loser))] = retried[..] else {
                    panic!("{what}: unexpected retried tasks {retried:?}");
                };
                assert_eq!(retried_job_id, job_id);
                loser
            };
            (1 - loser, results)
        }
        ReservationRace::FirstHeldAfterTcbFlip => {
            let paused = env.pause(HookPoint::TaskSuccessAfterTcbFlip, HookFilter::job(job_id));
            let failure_written = env.observe(
                HookPoint::CommitterRoundAfterDbWrite,
                HookFilter::job(job_id),
            );
            let apply_contended = env.observe(
                HookPoint::TreeStateApplyLockContended,
                HookFilter::job(job_id),
            );
            let first_join_handle = tokio::spawn(report(0));
            reached(&paused, 1).await;
            let second_join_handle = tokio::spawn(report(1));
            let second = if max_num_retry == 0 {
                reached(&failure_written, 1).await;
                reached(&apply_contended, 1).await;
                assert!(
                    !second_join_handle.is_finished(),
                    "{what}: the failure should wait for the job's guard to apply"
                );
                assert_eq!(env.row_state(job_id).await, JobState::Failed);
                paused.release();
                join(second_join_handle).await
            } else {
                let second = join(second_join_handle).await;
                paused.release();
                second
            };
            let first = join(first_join_handle).await;
            assert_eq!(failure_written.hits(), usize::from(max_num_retry == 0));
            (0, [first, second])
        }
        ReservationRace::Sequential => {
            let first = report(0).await;
            let second = report(1).await;
            (0, [first, second])
        }
    }
}

/// Reruns the rejected task `loser` of `job_id` without children, and asserts that the job then
/// registers exactly the other task's children and succeeds with both tasks' outputs.
async fn finish_after_rejected_report(env: &TestEnv, job_id: JobId, loser: usize, what: &str) {
    let task_instance_id = env.create_instance(job_id, TaskId::Index(loser)).await;
    assert_job_result(
        &env.task_success(
            job_id,
            loser,
            task_instance_id,
            &value(0xa0, loser),
            Vec::new(),
        )
        .await,
        JobState::BlockingOnChildren,
        &format!("{what}: the retried task"),
    );
    let child_ids = env.children_of(job_id).await;
    assert_eq!(
        child_ids.len(),
        NUM_CHILDREN,
        "{what}: the winner's children"
    );
    let ready_tasks: Vec<_> = child_ids
        .iter()
        .flat_map(|child_id| JobShape::ONE_TASK.ready_tasks(*child_id))
        .collect();
    env.drain_lane(Lane::Task, &ready_tasks).await;
    for (index, child_id) in child_ids.iter().enumerate() {
        assert_eq!(
            env.run_task(*child_id, 0, &value(0xa1, index)).await,
            JobState::Succeeded
        );
    }
    let mut expected: HashMap<JobId, JobState> = child_ids
        .iter()
        .map(|child_id| (*child_id, JobState::Succeeded))
        .collect();
    expected.insert(job_id, JobState::Succeeded);
    env.assert_tree_rows(job_id, &expected, what).await;
    env.assert_states(&expected, what).await;
    assert_eq!(env.tree_errors(job_id).await, HashMap::new());
    assert_eq!(
        env.row_outputs(job_id).await,
        Some(task_outputs([&value(0xa0, 0), &value(0xa0, 1)])),
        "{what}: the job's outputs"
    );
    env.assert_lanes_empty().await;
}

/// Runs `rpc` as the task of an RPC handler until its work reaches the pause `paused`, then drops
/// the RPC by aborting its task, with the work still paused.
///
/// # Type Parameters
///
/// * `OutputType` - The output type of `rpc`.
///
/// # Panics
///
/// Panics if the RPC completes, which it cannot while its work is paused.
async fn drop_rpc_at<OutputType: std::fmt::Debug + Send + 'static>(
    paused: &ArmedHook,
    rpc: impl Future<Output = OutputType> + Send + 'static,
) {
    let rpc_join_handle = tokio::spawn(rpc);
    reached(paused, 1).await;
    rpc_join_handle.abort();
    assert!(
        tokio::time::timeout(HOOK_TIMEOUT, rpc_join_handle)
            .await
            .expect("the aborted RPC's task should end within the timeout")
            .expect_err("the dropped RPC should not complete")
            .is_cancelled()
    );
}
