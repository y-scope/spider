//! Cancels of job trees racing the events of their jobs, on `MariaDB`.

use std::collections::HashMap;

use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::TaskOutputsSerializer;
use spider_storage::cache::error::StaleStateError;
use spider_storage::test_hooks::HookFilter;
use spider_storage::test_hooks::HookPoint;
use strum::EnumIter;
use strum::IntoEnumIterator;

use crate::runtime_infra::CommitSnapshot;
use crate::runtime_infra::JobResult;
use crate::runtime_infra::JobShape;
use crate::runtime_infra::Lane;
use crate::runtime_infra::Race;
use crate::runtime_infra::TestEnv;
use crate::runtime_infra::TreeProbes;
use crate::runtime_infra::assert_job_result;
use crate::runtime_infra::assert_stale;
use crate::runtime_infra::join;
use crate::runtime_infra::reached;
use crate::runtime_infra::value;
use crate::task_graph_builder::build_flat_task_graph;

/// The number of children that a registration race's registering job reports.
const NUM_GRANDCHILDREN: usize = 3;

/// The message of a commit task's failure.
const COMMIT_FAILURE: &str = "commit failure";

/// How the commit of a `CommitReady` root interleaves with the root's cancel.
#[derive(Debug, Clone, Copy, EnumIter)]
enum RootCommitRace {
    /// The cancel is answered, then the commit task's registered instance succeeds.
    CancelThenLateCommit,

    /// The cancel is answered, then the commit task's registered instance fails.
    CancelThenLateCommitFailure,

    /// The cancel is answered, then an instance of the commit task is registered.
    CancelThenNewInstance,

    /// The commit's success and the cancel in one round; the commit arrives first.
    SameRoundCommitFirst,

    /// The commit's success and the cancel in one round; the cancel arrives first.
    SameRoundCancelFirst,

    /// The commit's success arrives while the cancel's round is between its database write and its
    /// apply.
    CancelInFlight,

    /// The cancel arrives while the commit's round is between its database write and its apply.
    CommitInFlight,

    /// The cancel arrives after the commit's success is answered.
    AfterCommitReply,
}

impl RootCommitRace {
    /// # Returns
    ///
    /// Whether the commit is planned before the cancel.
    const fn is_commit_first(self) -> bool {
        matches!(self, Self::CommitInFlight | Self::AfterCommitReply)
    }
}

/// Which jobs of a test tree have a cleanup task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cleanups {
    /// No job: every deactivated job becomes `Cancelled`, so the tree is quiescent once the race
    /// is decided and its committer exits.
    None,

    /// Every job, except where a tree says otherwise: the raced job stays in the tree as
    /// `CleanupReady`, and the committer stays alive.
    All,

    /// Only the jobs that are neither the root nor on the raced job's path: the raced job leaves
    /// the tree while the committer stays alive for the others.
    BystandersOnly,

    /// Only the root: every other job leaves the tree while the committer stays alive for the root.
    RootOnly,
}

/// The job whose task registers children in a registration race.
#[derive(Debug, Clone, Copy)]
enum Registrar {
    /// The root, before it has any child.
    Root,

    /// A leaf `L` in the tree `R → {L, S}`.
    Leaf,
}

/// How a registration and the root's cancel interleave.
#[derive(Debug, Clone, Copy, EnumIter)]
enum RegistrationRace {
    /// Both in one round; the registration arrives first.
    SameRoundRegistrationFirst,

    /// Both in one round; the cancel arrives first.
    SameRoundCancelFirst,

    /// The registration is submitted while the cancel's round is between its database write and
    /// its apply.
    CancelInFlight,

    /// The registration's phase A holds the registering job's read guard while the cancel's round
    /// writes, so that the round's apply waits for it.
    PhaseAAcrossCancelWrite,

    /// The registration's phase A has dropped its guard and is about to submit when the cancel is
    /// answered.
    PhaseABeforeSubmitAcrossCancel,

    /// The registration is reported after the cancel is answered.
    AfterCancelReply,

    /// The cancel is submitted while the registration's round, which inserts the children, is
    /// between its database write and its apply.
    RegistrationInFlight,

    /// The cancel is submitted after the registration is answered.
    AfterRegistrationReply,
}

impl RegistrationRace {
    /// # Returns
    ///
    /// Whether the cancel is planned before the registration.
    const fn is_cancel_first(self) -> bool {
        !matches!(
            self,
            Self::RegistrationInFlight | Self::AfterRegistrationReply
        )
    }
}

/// The jobs of a registration race's tree.
struct RegistrationTree {
    root_id: JobId,
    registrar_id: JobId,
    has_cleanup: HashMap<JobId, bool>,
    grandchild_shapes: Vec<JobShape>,
}

impl RegistrationTree {
    /// Starts a tree whose registering job is `registrar`, with `cleanups`. With
    /// [`Cleanups::All`], every job but the second registered child has a cleanup task; with
    /// [`Cleanups::BystandersOnly`], only the sibling has one.
    ///
    /// # Returns
    ///
    /// The tree.
    ///
    /// # Panics
    ///
    /// Panics if the root doesn't get two children.
    async fn start(env: &TestEnv, registrar: Registrar, cleanups: Cleanups) -> Self {
        let all = cleanups == Cleanups::All;
        let root_id = env.start_root(shape_with_cleanup(all)).await;
        let mut has_cleanup = HashMap::from([(root_id, all)]);
        let registrar_id = match registrar {
            Registrar::Root => root_id,
            Registrar::Leaf => {
                let sibling_has_cleanup = cleanups != Cleanups::None;
                let [leaf_id, sibling_id] = env
                    .register_children(
                        root_id,
                        &value(0x10, 0),
                        &[
                            shape_with_cleanup(all),
                            shape_with_cleanup(sibling_has_cleanup),
                        ],
                    )
                    .await[..]
                else {
                    panic!("the root should have two children");
                };
                has_cleanup.insert(leaf_id, all);
                has_cleanup.insert(sibling_id, sibling_has_cleanup);
                leaf_id
            }
        };
        Self {
            root_id,
            registrar_id,
            has_cleanup,
            grandchild_shapes: (0..NUM_GRANDCHILDREN)
                .map(|index| shape_with_cleanup(all && index != 1))
                .collect(),
        }
    }

    /// # Returns
    ///
    /// The jobs of the tree before the registration.
    fn job_ids(&self) -> Vec<JobId> {
        self.has_cleanup.keys().copied().collect()
    }

    /// # Returns
    ///
    /// The state that the cancel moves `job_id`, a job of the tree before the registration, to.
    fn target(&self, job_id: JobId) -> JobState {
        abort_target(self.has_cleanup[&job_id])
    }

    /// # Returns
    ///
    /// Whether the tree's committer stays alive after the cancel, because a job of the tree is
    /// `CleanupReady`.
    fn outlives_cancel(&self) -> bool {
        self.has_cleanup.values().any(|has_cleanup| *has_cleanup)
    }

    /// # Returns
    ///
    /// The states of the jobs of the tree before the race.
    fn before_race(&self) -> HashMap<JobId, JobState> {
        self.job_ids()
            .into_iter()
            .map(|job_id| {
                let state = if job_id == self.root_id && self.registrar_id != self.root_id {
                    JobState::BlockingOnChildren
                } else {
                    JobState::Running
                };
                (job_id, state)
            })
            .collect()
    }

    /// # Returns
    ///
    /// The states that the cancel moves the jobs of the tree before the registration to.
    fn cancelled(&self) -> HashMap<JobId, JobState> {
        self.job_ids()
            .into_iter()
            .map(|job_id| (job_id, self.target(job_id)))
            .collect()
    }
}

/// How the registrations of two sibling leaves interleave with the root's cancel.
#[derive(Debug, Clone, Copy, EnumIter)]
enum TwoRegistrationRace {
    /// The first registration, the second registration and the cancel arrive in this order before
    /// one round, which plans the cancel first.
    SameRound,

    /// The second registration and the cancel are submitted while the first registration's round
    /// is between its database write and its apply.
    FirstInFlight,

    /// Both registrations are in one round, which registers the first child set and defers the
    /// second; the cancel is submitted while that round is between its database write and its
    /// apply, and is planned ahead of the deferred registration.
    SecondDeferred,
}

/// The jobs of a tree `R → {L, S}` whose two leaves both register children.
struct TwoRegistrationTree {
    root_id: JobId,
    leaf_ids: [JobId; 2],
    root_has_cleanup: bool,
    leaf_has_cleanup: bool,
    grandchild_shapes: Vec<JobShape>,
}

impl TwoRegistrationTree {
    /// Starts a tree with `cleanups`: with [`Cleanups::All`], every job but the second registered
    /// child of each leaf has a cleanup task; with [`Cleanups::RootOnly`], only the root has one.
    ///
    /// # Returns
    ///
    /// The tree.
    ///
    /// # Panics
    ///
    /// Panics if the root doesn't get two children.
    async fn start(env: &TestEnv, cleanups: Cleanups) -> Self {
        let all = cleanups == Cleanups::All;
        let root_has_cleanup = cleanups != Cleanups::None;
        let root_id = env.start_root(shape_with_cleanup(root_has_cleanup)).await;
        let [first_id, second_id] = env
            .register_children(
                root_id,
                &value(0x11, 0),
                &[shape_with_cleanup(all), shape_with_cleanup(all)],
            )
            .await[..]
        else {
            panic!("the root should have two children");
        };
        Self {
            root_id,
            leaf_ids: [first_id, second_id],
            root_has_cleanup,
            leaf_has_cleanup: all,
            grandchild_shapes: (0..NUM_GRANDCHILDREN)
                .map(|index| shape_with_cleanup(all && index != 1))
                .collect(),
        }
    }

    /// # Returns
    ///
    /// The states that the cancel moves the root and the leaves to.
    fn cancelled(&self) -> HashMap<JobId, JobState> {
        let leaf_target = abort_target(self.leaf_has_cleanup);
        HashMap::from([
            (self.root_id, abort_target(self.root_has_cleanup)),
            (self.leaf_ids[0], leaf_target),
            (self.leaf_ids[1], leaf_target),
        ])
    }

    /// # Returns
    ///
    /// A future that reports the success of `leaf_id`'s only task, through `task_instance_id`,
    /// with the tree's registered children.
    fn registration(
        &self,
        env: &TestEnv,
        leaf_id: JobId,
        task_instance_id: TaskInstanceId,
    ) -> impl Future<Output = JobResult> + Send + use<> {
        env.task_success(
            leaf_id,
            0,
            task_instance_id,
            &value(0x21, 0),
            self.grandchild_shapes
                .iter()
                .map(|shape| shape.child())
                .collect(),
        )
    }
}

/// How the root's cancel and the commit of a `CommitReady` non-root job interleave.
#[derive(Debug, Clone, Copy, EnumIter)]
enum CommitRace {
    /// The commit task's instance is registered, then the cancel is answered, then the commit
    /// succeeds.
    CancelThenCommit,

    /// The cancel is answered, then the commit task gets an instance, which succeeds.
    CancelThenNewInstanceCommit,

    /// The commit task's instance is registered, then the cancel is answered, then the commit
    /// fails for good.
    CancelThenCommitFailure,

    /// The commit's success and the cancel in one round; the commit arrives first.
    SameRoundCommitFirst,

    /// The commit's success and the cancel in one round; the cancel arrives first.
    SameRoundCancelFirst,

    /// The commit's failure and the cancel in one round; the failure arrives first.
    SameRoundCommitFailure,

    /// The commit's success is submitted while the cancel's round is between its database write
    /// and its apply.
    CancelInFlight,

    /// The cancel is submitted while the commit's round is between its database write and its
    /// apply.
    CommitInFlight,

    /// The cancel is submitted after the commit's success is answered.
    AfterCommitReply,
}

impl CommitRace {
    /// # Returns
    ///
    /// Whether the commit is planned before the cancel.
    const fn is_commit_first(self) -> bool {
        matches!(self, Self::CommitInFlight | Self::AfterCommitReply)
    }

    /// # Returns
    ///
    /// Whether the commit task fails instead of succeeding.
    const fn is_commit_failure(self) -> bool {
        matches!(
            self,
            Self::CancelThenCommitFailure | Self::SameRoundCommitFailure
        )
    }

    /// # Returns
    ///
    /// Whether the cancel is answered before the commit is reported.
    const fn is_cancel_answered_first(self) -> bool {
        matches!(
            self,
            Self::CancelThenCommit
                | Self::CancelThenNewInstanceCommit
                | Self::CancelThenCommitFailure
        )
    }
}

/// The jobs of a tree `R → {P → C, Q}`, where `P` has a commit task and is `CommitReady`, and the
/// uncle `Q` is optional.
struct CommitTree {
    root_id: JobId,
    parent_id: JobId,
    child_id: JobId,
    uncle_id: Option<JobId>,
    has_cleanup: bool,
}

impl CommitTree {
    /// Starts a tree whose jobs, `C` excepted, all have a cleanup task if `has_cleanup`, with `Q`
    /// if `with_uncle`, and runs `C` so that `P` is `CommitReady`, then drains `P`'s commit task.
    ///
    /// # Returns
    ///
    /// The tree.
    ///
    /// # Panics
    ///
    /// Panics if `P` doesn't get one child, or if `C` doesn't succeed.
    async fn start(env: &TestEnv, has_cleanup: bool, with_uncle: bool) -> Self {
        let shape = shape_with_cleanup(has_cleanup);
        let root_id = env.start_root(shape).await;
        let parent_shape = JobShape {
            with_commit: true,
            ..shape
        };
        let shapes: &[JobShape] = if with_uncle {
            &[parent_shape, shape]
        } else {
            &[parent_shape]
        };
        let children = env
            .register_children(root_id, &value(0x12, 0), shapes)
            .await;
        let parent_id = children[0];
        let [child_id] = env
            .register_children(parent_id, &value(0x13, 0), &[JobShape::ONE_TASK])
            .await[..]
        else {
            panic!("the parent should have one child");
        };
        assert_eq!(
            env.run_task(child_id, 0, &value(0x30, 0)).await,
            JobState::Succeeded
        );
        env.drain_lane(Lane::Commit, &[(parent_id, TaskId::Commit)])
            .await;
        Self {
            root_id,
            parent_id,
            child_id,
            uncle_id: children.get(1).copied(),
            has_cleanup,
        }
    }

    /// # Returns
    ///
    /// The jobs whose transitions a commit race can write.
    fn job_ids(&self) -> Vec<JobId> {
        let mut job_ids = vec![self.root_id, self.parent_id, self.child_id];
        job_ids.extend(self.uncle_id);
        job_ids
    }

    /// # Returns
    ///
    /// The states of the tree's jobs once `race` is decided, with `parent_state` as `P`'s state.
    fn states(&self, race: CommitRace, parent_state: JobState) -> HashMap<JobId, JobState> {
        let target = abort_target(self.has_cleanup);
        let root_state = if race.is_commit_first() && self.uncle_id.is_none() {
            JobState::Succeeded
        } else {
            target
        };
        let mut states = HashMap::from([
            (self.root_id, root_state),
            (self.parent_id, parent_state),
            (self.child_id, JobState::Succeeded),
        ]);
        states.extend(self.uncle_id.map(|uncle_id| (uncle_id, target)));
        states
    }
}

/// How a leaf's failure and the root's cancel interleave.
#[derive(Debug, Clone, Copy, EnumIter)]
enum FailureRace {
    /// Both in one round; the failure arrives first.
    SameRoundFailureFirst,

    /// Both in one round; the cancel arrives first.
    SameRoundCancelFirst,

    /// The failure is submitted while the cancel's round is between its database write and its
    /// apply.
    CancelInFlight,

    /// The failure is reported after the cancel is answered.
    AfterCancelReply,

    /// The cancel is submitted while the failure's round is between its database write and its
    /// apply.
    FailureInFlight,

    /// The cancel is submitted after the failure is answered.
    AfterFailureReply,
}

impl FailureRace {
    /// # Returns
    ///
    /// Whether the cancel is planned before the failure.
    const fn is_cancel_first(self) -> bool {
        !matches!(self, Self::FailureInFlight | Self::AfterFailureReply)
    }
}

/// The jobs of a tree `R → {A → {L, S}, Q}`, where the leaf `L` fails.
struct FailureTree {
    root_id: JobId,
    middle_id: JobId,
    leaf_id: JobId,
    has_cleanup: HashMap<JobId, bool>,
}

impl FailureTree {
    /// The message of the leaf's failure.
    const LEAF_FAILURE: &str = "leaf failure";

    /// Starts a tree with `cleanups`. With [`Cleanups::BystandersOnly`], only `S` and `Q` have a
    /// cleanup task.
    ///
    /// # Returns
    ///
    /// The tree.
    async fn start(env: &TestEnv, cleanups: Cleanups) -> Self {
        let all = cleanups == Cleanups::All;
        let bystander = cleanups != Cleanups::None;
        let root_id = env.start_root(shape_with_cleanup(all)).await;
        let [middle_id, uncle_id] = env
            .register_children(
                root_id,
                &value(0x14, 0),
                &[shape_with_cleanup(all), shape_with_cleanup(bystander)],
            )
            .await[..]
        else {
            panic!("the root should have two children");
        };
        let [leaf_id, sibling_id] = env
            .register_children(
                middle_id,
                &value(0x15, 0),
                &[shape_with_cleanup(all), shape_with_cleanup(bystander)],
            )
            .await[..]
        else {
            panic!("the middle job should have two children");
        };
        Self {
            root_id,
            middle_id,
            leaf_id,
            has_cleanup: HashMap::from([
                (root_id, all),
                (middle_id, all),
                (leaf_id, all),
                (sibling_id, bystander),
                (uncle_id, bystander),
            ]),
        }
    }

    /// # Returns
    ///
    /// Every job of the tree.
    fn job_ids(&self) -> Vec<JobId> {
        self.has_cleanup.keys().copied().collect()
    }

    /// # Returns
    ///
    /// The state that a cancel or an abort moves `job_id` to.
    fn target(&self, job_id: JobId) -> JobState {
        abort_target(self.has_cleanup[&job_id])
    }

    /// # Returns
    ///
    /// The states of the tree's jobs after the cancel.
    fn cancelled(&self) -> HashMap<JobId, JobState> {
        self.job_ids()
            .into_iter()
            .map(|job_id| (job_id, self.target(job_id)))
            .collect()
    }

    /// # Returns
    ///
    /// The states of the tree's jobs after the leaf's failure: `L` and its ancestors fail, and the
    /// other jobs are aborted.
    fn failed(&self) -> HashMap<JobId, JobState> {
        self.job_ids()
            .into_iter()
            .map(|job_id| {
                let state = if [self.root_id, self.middle_id, self.leaf_id].contains(&job_id) {
                    JobState::Failed
                } else {
                    self.target(job_id)
                };
                (job_id, state)
            })
            .collect()
    }

    /// # Returns
    ///
    /// The error messages of the tree's jobs after the leaf's failure.
    fn failure_errors(&self) -> HashMap<JobId, String> {
        let propagated = format!(
            "descendant job {} failed: {}",
            self.leaf_id,
            Self::LEAF_FAILURE
        );
        HashMap::from([
            (self.root_id, propagated.clone()),
            (self.middle_id, propagated),
            (self.leaf_id, Self::LEAF_FAILURE.to_owned()),
        ])
    }
}

impl TestEnv {
    /// Races the cancel of a `CommitReady` root, with a cleanup task if `has_cleanup`, with the
    /// root's commit as `race` says, and asserts the outcome: the cancel deactivates the root, and
    /// the commit is stale, unless the commit is planned first, which succeeds the root and makes
    /// the cancel stale.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`TaskOutputsSerializer::from_tuple`]'s return values on failure.
    /// * Forwards [`TaskOutputsSerializer::deserialize`]'s return values on failure.
    /// * Forwards [`ServiceState::succeed_commit_task_instance`]'s return values on failure.
    /// * Forwards [`ServiceState::cancel_job`]'s return values on failure.
    /// * Forwards [`ServiceState::succeed_task_instance`]'s return values on failure.
    /// * Forwards [`ServiceState::get_job_outputs`]'s return values on failure.
    /// * Forwards [`ServiceState::succeed_cleanup_task_instance`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if the outcome is not the one asserted, or if [`Self::race_root_commit`] or another
    /// [`TestEnv`] helper panics.
    ///
    /// [`ServiceState::succeed_commit_task_instance`]:
    ///     spider_storage::state::ServiceState::succeed_commit_task_instance
    /// [`ServiceState::cancel_job`]: spider_storage::state::ServiceState::cancel_job
    /// [`ServiceState::succeed_task_instance`]:
    ///     spider_storage::state::ServiceState::succeed_task_instance
    /// [`ServiceState::get_job_outputs`]: spider_storage::state::ServiceState::get_job_outputs
    /// [`ServiceState::succeed_cleanup_task_instance`]:
    ///     spider_storage::state::ServiceState::succeed_cleanup_task_instance
    async fn assert_cancel_deactivates_commit_ready_root(
        &self,
        has_cleanup: bool,
        race: RootCommitRace,
    ) -> anyhow::Result<()> {
        let what = format!("has_cleanup {has_cleanup}, {race:?}");
        let (graph, inputs) = build_flat_task_graph(1, 4, true, has_cleanup);
        let job_id = self.start_job(&graph, &inputs).await;
        let task_instance_id = self.create_instance(job_id, TaskId::Index(0)).await;
        let serialized_outputs = TaskOutputsSerializer::from_tuple(&(vec![1u8; 4],))?;
        let state = self
            .service
            .succeed_task_instance(
                self.service.session_id(),
                job_id,
                task_instance_id,
                0,
                serialized_outputs.clone(),
                Vec::new(),
            )
            .await?;
        assert_eq!(state, JobState::CommitReady, "{what}: the regular task");
        self.poll_commit_ready_task(job_id).await;
        let cancelled_state = if has_cleanup {
            JobState::CleanupReady
        } else {
            JobState::Cancelled
        };

        let (commit, cancel) = self.race_root_commit(job_id, race).await;

        if race.is_commit_first() {
            assert_eq!(commit?, JobState::Succeeded, "{what}: the commit");
            assert_stale(
                &cancel,
                &StaleStateError::JobAlreadyTerminated(JobState::Succeeded),
                &format!("{what}: the cancel"),
            );
            assert_eq!(
                self.service.get_job_outputs(job_id).await?,
                TaskOutputsSerializer::deserialize(&serialized_outputs)?,
                "{what}: the root's outputs"
            );
            self.assert_job_state(job_id, JobState::Succeeded).await;
            return Ok(());
        }

        assert_eq!(cancel?, cancelled_state, "{what}: the cancel");
        let expected_stale = match race {
            RootCommitRace::SameRoundCommitFirst
            | RootCommitRace::SameRoundCancelFirst
            | RootCommitRace::CancelInFlight => {
                if has_cleanup {
                    StaleStateError::JobNoLongerCommitReady
                } else {
                    StaleStateError::JobAlreadyTerminated(JobState::Cancelled)
                }
            }
            RootCommitRace::CancelThenLateCommit
            | RootCommitRace::CancelThenLateCommitFailure
            | RootCommitRace::CancelThenNewInstance
            | RootCommitRace::CommitInFlight
            | RootCommitRace::AfterCommitReply => StaleStateError::JobNoLongerCommitReady,
        };
        assert_stale(&commit, &expected_stale, &format!("{what}: the commit"));
        self.assert_job_state(job_id, cancelled_state).await;
        if has_cleanup {
            self.poll_cleanup_ready_task(job_id).await;
            let cleanup_instance_id = self.create_instance(job_id, TaskId::Cleanup).await;
            let state = self
                .service
                .succeed_cleanup_task_instance(
                    self.service.session_id(),
                    job_id,
                    cleanup_instance_id,
                )
                .await?;
            assert_eq!(state, JobState::Cancelled, "{what}: the cleanup");
            self.assert_job_state(job_id, JobState::Cancelled).await;
        }
        Ok(())
    }

    /// Runs the commit of the `CommitReady` root `job_id` and the root's cancel as `race` says.
    ///
    /// # Returns
    ///
    /// A pair, containing:
    ///
    /// * The result of the commit's success or, for the races that say so, of the commit's failure
    ///   or of the commit's instance registration.
    /// * The result of the cancel.
    async fn race_root_commit(
        &self,
        job_id: JobId,
        race: RootCommitRace,
    ) -> (JobResult, JobResult) {
        let service = self.service.clone();
        let cancel = async move { service.cancel_job(job_id).await };
        match race {
            RootCommitRace::CancelThenLateCommit => {
                let commit = self.succeed_commit(job_id).await;
                let cancel = cancel.await;
                (commit.await, cancel)
            }
            RootCommitRace::CancelThenLateCommitFailure => {
                let commit_instance_id = self.create_instance(job_id, TaskId::Commit).await;
                let cancel = cancel.await;
                let failure = self
                    .service
                    .fail_task_instance(
                        self.service.session_id(),
                        job_id,
                        commit_instance_id,
                        TaskId::Commit,
                        "late commit failure".to_owned(),
                    )
                    .await;
                (failure, cancel)
            }
            RootCommitRace::CancelThenNewInstance => {
                let cancel = cancel.await;
                let registration = self
                    .service
                    .create_task_instance(
                        self.service.session_id(),
                        job_id,
                        TaskId::Commit,
                        self.execution_manager_id,
                    )
                    .await
                    .map(|_| JobState::CommitReady);
                (registration, cancel)
            }
            RootCommitRace::SameRoundCommitFirst => {
                let commit = self.succeed_commit(job_id).await;
                self.race(HookPoint::CommitterBeforeReceive, job_id, commit, cancel)
                    .await
            }
            RootCommitRace::SameRoundCancelFirst => {
                let commit = self.succeed_commit(job_id).await;
                let (cancel, commit) = self
                    .race(HookPoint::CommitterBeforeReceive, job_id, cancel, commit)
                    .await;
                (commit, cancel)
            }
            RootCommitRace::CancelInFlight => {
                let commit = self.succeed_commit(job_id).await;
                let (cancel, commit) = self
                    .race(
                        HookPoint::CommitterRoundAfterDbWrite,
                        job_id,
                        cancel,
                        commit,
                    )
                    .await;
                (commit, cancel)
            }
            RootCommitRace::CommitInFlight => {
                let commit = self.succeed_commit(job_id).await;
                self.race(
                    HookPoint::CommitterRoundAfterDbWrite,
                    job_id,
                    commit,
                    cancel,
                )
                .await
            }
            RootCommitRace::AfterCommitReply => {
                let commit = self.succeed_commit(job_id).await.await;
                (commit, cancel.await)
            }
        }
    }

    /// Registers an instance of the commit task of `job_id`.
    ///
    /// # Returns
    ///
    /// A future that reports the instance's success.
    ///
    /// # Panics
    ///
    /// Panics if the instance registration fails.
    async fn succeed_commit(
        &self,
        job_id: JobId,
    ) -> impl Future<Output = JobResult> + Send + 'static {
        let commit_instance_id = self.create_instance(job_id, TaskId::Commit).await;
        let service = self.service.clone();
        async move {
            service
                .succeed_commit_task_instance(service.session_id(), job_id, commit_instance_id)
                .await
        }
    }

    /// Races `first` and `second`, two RPCs on the root `job_id`, at `point` (see [`Race::start`]).
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The result of `first`.
    /// * The result of `second`.
    async fn race(
        &self,
        point: HookPoint,
        job_id: JobId,
        first: impl Future<Output = JobResult> + Send + 'static,
        second: impl Future<Output = JobResult> + Send + 'static,
    ) -> (JobResult, JobResult) {
        Race::start(self, point, HookFilter::job(job_id), first, second, job_id)
            .await
            .finish()
            .await
    }
}

impl TestEnv {
    /// Races the cancel of a tree whose registering job is `registrar`, with `cleanups`, with the
    /// registration of that job's children as `race` says, and asserts the outcome: a cancel
    /// planned first inserts no child, and a registration planned first inserts the children, which
    /// the cancel then deactivates in its own database write.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ServiceState::cancel_job`]'s return values on failure.
    ///
    /// [`ServiceState::cancel_job`]: spider_storage::state::ServiceState::cancel_job
    async fn assert_cancel_races_registration(
        &self,
        registrar: Registrar,
        cleanups: Cleanups,
        race: RegistrationRace,
    ) -> anyhow::Result<()> {
        let what = format!("{registrar:?}, {cleanups:?}, {race:?}");
        let tree = RegistrationTree::start(self, registrar, cleanups).await;
        let task_instance_id = self
            .create_instance(tree.registrar_id, TaskId::Index(0))
            .await;
        let probes = TreeProbes::arm(self, tree.root_id, &tree.job_ids());
        let submissions = self.observe(
            HookPoint::JobEventAfterSubmit,
            HookFilter::job(tree.registrar_id),
        );
        let num_root_writes_before_cancel =
            if !race.is_cancel_first() && tree.registrar_id == tree.root_id {
                2
            } else {
                1
            };
        let snapshot = CommitSnapshot::arm(
            self,
            tree.root_id,
            tree.root_id,
            num_root_writes_before_cancel,
        );

        let (report, cancelled) =
            Box::pin(self.race_registration(&tree, race, task_instance_id, &what)).await;

        assert_job_result(
            &cancelled,
            tree.target(tree.root_id),
            &format!("{what}: the cancel"),
        );
        cancelled?;
        let after_cancel = self.assert_registration(&tree, race, &report, &what).await;

        let num_registrations = usize::from(!matches!(race, RegistrationRace::AfterCancelReply));
        let num_cancel_submissions = usize::from(tree.registrar_id == tree.root_id);
        assert_eq!(
            submissions.hits(),
            num_registrations + num_cancel_submissions,
            "{what}: the events that the registering job submitted"
        );
        let (num_rounds, num_writes) = match race {
            RegistrationRace::SameRoundRegistrationFirst
            | RegistrationRace::SameRoundCancelFirst
            | RegistrationRace::AfterCancelReply => (1, 1),
            RegistrationRace::CancelInFlight
            | RegistrationRace::PhaseAAcrossCancelWrite
            | RegistrationRace::PhaseABeforeSubmitAcrossCancel => {
                if tree.outlives_cancel() {
                    (2, 1)
                } else {
                    (1, 1)
                }
            }
            RegistrationRace::RegistrationInFlight | RegistrationRace::AfterRegistrationReply => {
                (2, 2)
            }
        };
        probes.assert_rounds(num_rounds, num_writes, &what);
        probes.assert_transitions(
            &tree
                .job_ids()
                .into_iter()
                .map(|job_id| {
                    let num_transitions = if !race.is_cancel_first() && job_id == tree.registrar_id
                    {
                        2
                    } else {
                        1
                    };
                    (job_id, num_transitions)
                })
                .collect(),
            &what,
        );
        assert_eq!(
            snapshot.rows().await,
            after_cancel,
            "{what}: the rows at the cancel's write"
        );
        self.finish_tree(tree.root_id, &after_cancel, &HashMap::new(), &what)
            .await;
        Ok(())
    }

    /// Runs the registration of `tree`'s children, through the instance `task_instance_id`, and
    /// the cancel of its root as `race` says.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The result of the registering report.
    /// * The result of the cancel.
    async fn race_registration(
        &self,
        tree: &RegistrationTree,
        race: RegistrationRace,
        task_instance_id: TaskInstanceId,
        what: &str,
    ) -> (JobResult, JobResult) {
        let root_id = tree.root_id;
        let registrar_id = tree.registrar_id;
        let report = self.task_success(
            registrar_id,
            0,
            task_instance_id,
            &value(0x20, 0),
            tree.grandchild_shapes
                .iter()
                .map(|shape| shape.child())
                .collect(),
        );
        let cancelled = self.cancel(root_id);
        match race {
            RegistrationRace::SameRoundRegistrationFirst => {
                Race::start(
                    self,
                    HookPoint::CommitterBeforeReceive,
                    HookFilter::job(root_id),
                    report,
                    cancelled,
                    root_id,
                )
                .await
                .finish()
                .await
            }
            RegistrationRace::SameRoundCancelFirst => {
                let (cancelled, report) = Race::start(
                    self,
                    HookPoint::CommitterBeforeReceive,
                    HookFilter::job(root_id),
                    cancelled,
                    report,
                    registrar_id,
                )
                .await
                .finish()
                .await;
                (report, cancelled)
            }
            RegistrationRace::CancelInFlight => {
                let race = Race::start(
                    self,
                    HookPoint::CommitterRoundAfterDbWrite,
                    HookFilter::job(root_id),
                    cancelled,
                    report,
                    registrar_id,
                )
                .await;
                self.assert_cancel_written_not_applied(tree, what).await;
                let (cancelled, report) = race.finish().await;
                (report, cancelled)
            }
            RegistrationRace::PhaseAAcrossCancelWrite => {
                self.race_phase_a_across_cancel_write(tree, report, cancelled, what)
                    .await
            }
            RegistrationRace::PhaseABeforeSubmitAcrossCancel => {
                let held = self.pause(
                    HookPoint::TaskSuccessBeforeOutputsCommit,
                    HookFilter::job(registrar_id),
                );
                let report_join_handle = tokio::spawn(report);
                reached(&held, 1).await;
                let cancelled = cancelled.await;
                held.release();
                (join(report_join_handle).await, cancelled)
            }
            RegistrationRace::AfterCancelReply => {
                let cancelled = cancelled.await;
                (report.await, cancelled)
            }
            RegistrationRace::RegistrationInFlight => {
                let race = Race::start(
                    self,
                    HookPoint::CommitterRoundAfterDbWrite,
                    HookFilter::job(registrar_id),
                    report,
                    cancelled,
                    root_id,
                )
                .await;
                self.assert_registration_written_not_applied(tree, what)
                    .await;
                race.finish().await
            }
            RegistrationRace::AfterRegistrationReply => {
                let report = report.await;
                (report, cancelled.await)
            }
        }
    }

    /// Runs `report` until its phase A, holding the registering job's read guard, is paused right
    /// after the task's success, then runs `cancelled` until its round of `tree` has written, and
    /// releases the phase A.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The result of `report`.
    /// * The result of `cancelled`.
    ///
    /// # Panics
    ///
    /// Panics if the cancel finishes while the phase A is paused.
    async fn race_phase_a_across_cancel_write(
        &self,
        tree: &RegistrationTree,
        report: impl Future<Output = JobResult> + Send + 'static,
        cancelled: impl Future<Output = JobResult> + Send + 'static,
        what: &str,
    ) -> (JobResult, JobResult) {
        let held = self.pause(
            HookPoint::TaskSuccessAfterTcbFlip,
            HookFilter::job(tree.registrar_id),
        );
        let written = self.observe(
            HookPoint::CommitterRoundAfterDbWrite,
            HookFilter::job(tree.root_id),
        );
        let report_join_handle = tokio::spawn(report);
        reached(&held, 1).await;
        let cancel_join_handle = tokio::spawn(cancelled);
        reached(&written, 1).await;
        // The cancel's apply needs the registering job's write lock, against which the paused phase
        // A holds a read guard. A read of that job's cached state would queue behind the waiting
        // writer, so only the rows are read here.
        self.assert_tree_rows(tree.root_id, &tree.cancelled(), what)
            .await;
        assert!(
            !cancel_join_handle.is_finished(),
            "{what}: the cancel should wait for the paused phase A"
        );
        held.release();
        (
            join(report_join_handle).await,
            join(cancel_join_handle).await,
        )
    }

    /// Asserts the state of `tree` while the cancel's round is paused between its database write
    /// and its apply, after the registration's phase A has passed: the rows are deactivated, while
    /// the cached root is still in its state before the race.
    async fn assert_cancel_written_not_applied(&self, tree: &RegistrationTree, what: &str) {
        self.assert_tree_rows(tree.root_id, &tree.cancelled(), what)
            .await;
        assert_eq!(
            self.service
                .get_job_state(tree.root_id)
                .await
                .expect("the root's state should be readable"),
            tree.before_race()[&tree.root_id],
            "{what}: the cached root while the cancel is written but not applied"
        );
    }

    /// Asserts the state of `tree` while the registration's round is paused between its database
    /// write and its apply: the children's rows exist and are `Running` under the
    /// `BlockingOnChildren` registering job, but the cache doesn't know them yet.
    async fn assert_registration_written_not_applied(&self, tree: &RegistrationTree, what: &str) {
        let grandchild_ids = self.children_of(tree.registrar_id).await;
        assert_eq!(
            grandchild_ids.len(),
            NUM_GRANDCHILDREN,
            "{what}: the children that the registration's write inserts"
        );
        let mut written = tree.before_race();
        written.insert(tree.registrar_id, JobState::BlockingOnChildren);
        for grandchild_id in &grandchild_ids {
            written.insert(*grandchild_id, JobState::Running);
            self.assert_not_cached(*grandchild_id, what).await;
        }
        self.assert_tree_rows(tree.root_id, &written, what).await;
        if tree.registrar_id == tree.root_id {
            assert_eq!(
                self.service
                    .get_job_state(tree.root_id)
                    .await
                    .expect("the root's state should be readable"),
                JobState::Running,
                "{what}: the cached registering root before the apply"
            );
        }
    }

    /// Asserts the registering report's result and the children it left in `tree`, and that the
    /// published tasks of registered children can no longer get an instance.
    ///
    /// # Returns
    ///
    /// The states of every job of the tree after the cancel.
    async fn assert_registration(
        &self,
        tree: &RegistrationTree,
        race: RegistrationRace,
        report: &JobResult,
        what: &str,
    ) -> HashMap<JobId, JobState> {
        let grandchild_ids = self.children_of(tree.registrar_id).await;
        let mut after_cancel = tree.cancelled();
        if race.is_cancel_first() {
            let expected = if matches!(race, RegistrationRace::AfterCancelReply) {
                StaleStateError::JobNoLongerRunning
            } else {
                regular_stale(tree.target(tree.registrar_id))
            };
            assert_stale(report, &expected, &format!("{what}: the registration"));
            assert_eq!(
                grandchild_ids,
                Vec::new(),
                "{what}: a registration planned after the cancel should insert no child"
            );
            return after_cancel;
        }
        assert_job_result(
            report,
            JobState::BlockingOnChildren,
            &format!("{what}: the registration"),
        );
        assert_eq!(
            grandchild_ids.len(),
            NUM_GRANDCHILDREN,
            "{what}: the registered children"
        );
        for (grandchild_id, shape) in grandchild_ids.iter().zip(&tree.grandchild_shapes) {
            after_cancel.insert(*grandchild_id, abort_target(shape.with_cleanup));
        }
        self.assert_cancelled_children_get_no_instance(&grandchild_ids, what)
            .await;
        after_cancel
    }

    /// Drains the published tasks of `child_ids`, cancelled one-task children, and asserts that
    /// none of them can get an instance.
    async fn assert_cancelled_children_get_no_instance(&self, child_ids: &[JobId], what: &str) {
        let ready_tasks: Vec<_> = child_ids
            .iter()
            .map(|child_id| (*child_id, TaskId::Index(0)))
            .collect();
        self.drain_lane(Lane::Task, &ready_tasks).await;
        for child_id in child_ids {
            let instance = self
                .service
                .create_task_instance(
                    self.service.session_id(),
                    *child_id,
                    TaskId::Index(0),
                    self.execution_manager_id,
                )
                .await;
            assert_stale(
                &instance,
                &StaleStateError::JobNoLongerRunning,
                &format!("{what}: an instance of the cancelled child {child_id}'s published task"),
            );
        }
    }

    /// Finishes a tree whose race is decided and whose jobs are in the states of `expected`:
    /// asserts those states, that the rows are exactly `expected`'s jobs, that the only error
    /// messages are `expected_errors`, and what the root shows to the client; then runs the
    /// cleanup task of every `CleanupReady` job and asserts that the tree is quiescent.
    async fn finish_tree(
        &self,
        root_id: JobId,
        expected: &HashMap<JobId, JobState>,
        expected_errors: &HashMap<JobId, String>,
        what: &str,
    ) {
        self.assert_states(expected, what).await;
        self.assert_tree_rows(root_id, expected, what).await;
        assert_eq!(
            self.tree_errors(root_id).await,
            *expected_errors,
            "{what}: the error messages of the tree"
        );
        let root_state = expected[&root_id];
        assert_eq!(
            self.service
                .get_job_state(root_id)
                .await
                .expect("the root's state should be readable"),
            root_state,
            "{what}: the root's state"
        );
        assert_stale(
            &self.cancel(root_id).await,
            &second_cancel_stale(root_state),
            &format!("{what}: one more cancel"),
        );

        let mut cleanup_ready: Vec<JobId> = expected
            .iter()
            .filter(|(_, state)| **state == JobState::CleanupReady)
            .map(|(job_id, _)| *job_id)
            .collect();
        cleanup_ready.sort_by_key(JobId::get);
        self.run_cleanups(&cleanup_ready).await;
        let finished: HashMap<JobId, JobState> = expected
            .iter()
            .map(|(job_id, state)| {
                let state = if *state == JobState::CleanupReady {
                    JobState::Cancelled
                } else {
                    *state
                };
                (*job_id, state)
            })
            .collect();
        self.assert_states(&finished, what).await;
        self.assert_quiescent(root_id).await;
    }

    /// Races the cancel of a tree with `cleanups` with the registrations of both its leaves as
    /// `race` says, and asserts the outcome: a cancel planned before both registrations inserts no
    /// child, and a cancel planned after the first one deactivates its children and makes the
    /// second one stale.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ServiceState::cancel_job`]'s return values on failure.
    ///
    /// [`ServiceState::cancel_job`]: spider_storage::state::ServiceState::cancel_job
    async fn assert_cancel_races_two_registrations(
        &self,
        cleanups: Cleanups,
        race: TwoRegistrationRace,
    ) -> anyhow::Result<()> {
        let what = format!("two registrations, {cleanups:?}, {race:?}");
        let tree = TwoRegistrationTree::start(self, cleanups).await;
        let root_id = tree.root_id;
        let [first_id, second_id] = tree.leaf_ids;
        let first_instance_id = self.create_instance(first_id, TaskId::Index(0)).await;
        let second_instance_id = self.create_instance(second_id, TaskId::Index(0)).await;
        let probes = TreeProbes::arm(self, root_id, &[root_id, first_id, second_id]);
        let snapshot = CommitSnapshot::arm(self, root_id, root_id, 1);

        let (first, second, cancelled) = self
            .race_two_registrations(&tree, race, [first_instance_id, second_instance_id])
            .await;

        let mut after_cancel = tree.cancelled();
        assert_job_result(
            &cancelled,
            after_cancel[&root_id],
            &format!("{what}: the cancel"),
        );
        cancelled?;
        let leaf_stale = regular_stale(after_cancel[&first_id]);
        assert_stale(
            &second,
            &leaf_stale,
            &format!("{what}: the second registration"),
        );
        assert_eq!(
            self.children_of(second_id).await,
            Vec::new(),
            "{what}: the second registration should insert no child"
        );
        let first_children = self.children_of(first_id).await;
        if matches!(race, TwoRegistrationRace::SameRound) {
            assert_stale(
                &first,
                &leaf_stale,
                &format!("{what}: the first registration"),
            );
            assert_eq!(first_children, Vec::new(), "{what}");
            probes.assert_rounds(1, 1, &what);
            probes.assert_transitions(
                &HashMap::from([(root_id, 1), (first_id, 1), (second_id, 1)]),
                &what,
            );
        } else {
            assert_job_result(
                &first,
                JobState::BlockingOnChildren,
                &format!("{what}: the first registration"),
            );
            assert_eq!(first_children.len(), NUM_GRANDCHILDREN, "{what}");
            for (grandchild_id, shape) in first_children.iter().zip(&tree.grandchild_shapes) {
                after_cancel.insert(*grandchild_id, abort_target(shape.with_cleanup));
            }
            self.assert_cancelled_children_get_no_instance(&first_children, &what)
                .await;
            probes.assert_rounds(2, 2, &what);
            probes.assert_transitions(
                &HashMap::from([(root_id, 1), (first_id, 2), (second_id, 1)]),
                &what,
            );
        }
        assert_eq!(
            snapshot.rows().await,
            after_cancel,
            "{what}: the rows at the cancel's write"
        );
        self.finish_tree(root_id, &after_cancel, &HashMap::new(), &what)
            .await;
        Ok(())
    }

    /// Runs the registrations of both leaves of `tree`, through their instances
    /// `task_instance_ids`, and the cancel of its root as `race` says.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The result of the first registration.
    /// * The result of the second registration.
    /// * The result of the cancel.
    async fn race_two_registrations(
        &self,
        tree: &TwoRegistrationTree,
        race: TwoRegistrationRace,
        task_instance_ids: [TaskInstanceId; 2],
    ) -> (JobResult, JobResult, JobResult) {
        let root_id = tree.root_id;
        let [first_id, second_id] = tree.leaf_ids;
        let [first_instance_id, second_instance_id] = task_instance_ids;
        let (received, written) = match race {
            TwoRegistrationRace::SameRound => (
                Some(self.pause(HookPoint::CommitterBeforeReceive, HookFilter::job(root_id))),
                None,
            ),
            TwoRegistrationRace::FirstInFlight => (
                None,
                Some(self.pause(
                    HookPoint::CommitterRoundAfterDbWrite,
                    HookFilter::job(first_id),
                )),
            ),
            TwoRegistrationRace::SecondDeferred => (
                Some(self.pause(HookPoint::CommitterBeforeReceive, HookFilter::job(root_id))),
                Some(self.pause(
                    HookPoint::CommitterRoundAfterDbWrite,
                    HookFilter::job(first_id),
                )),
            ),
        };
        let first_join_handle = tokio::spawn(tree.registration(self, first_id, first_instance_id));
        if let Some(received) = &received {
            reached(received, 1).await;
        } else if let Some(written) = &written {
            reached(written, 1).await;
        }
        let second_submitted =
            self.observe(HookPoint::JobEventAfterSubmit, HookFilter::job(second_id));
        let second_join_handle =
            tokio::spawn(tree.registration(self, second_id, second_instance_id));
        reached(&second_submitted, 1).await;
        if let (Some(received), Some(written)) = (&received, &written) {
            received.release();
            reached(written, 1).await;
        }
        let cancel_submitted =
            self.observe(HookPoint::JobEventAfterSubmit, HookFilter::job(root_id));
        let cancel_join_handle = tokio::spawn(self.cancel(root_id));
        reached(&cancel_submitted, 1).await;
        if let Some(written) = &written {
            written.release();
        } else if let Some(received) = &received {
            received.release();
        }
        (
            join(first_join_handle).await,
            join(second_join_handle).await,
            join(cancel_join_handle).await,
        )
    }

    /// Races the cancel of a commit race's tree, with a cleanup task on its jobs if `has_cleanup`
    /// and with `Q` if `with_uncle`, with the commit of its `CommitReady` non-root job `P` as
    /// `race` says, and asserts the outcome: the cancel skips `P`, which finishes its commit
    /// without moving its cancelled parent, unless the commit is planned first and climbs as
    /// usual.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ServiceState::succeed_commit_task_instance`]'s return values on failure.
    /// * Forwards [`ServiceState::fail_task_instance`]'s return values on failure.
    ///
    /// [`ServiceState::succeed_commit_task_instance`]:
    ///     spider_storage::state::ServiceState::succeed_commit_task_instance
    /// [`ServiceState::fail_task_instance`]:
    ///     spider_storage::state::ServiceState::fail_task_instance
    async fn assert_cancel_skips_commit_ready_job(
        &self,
        has_cleanup: bool,
        with_uncle: bool,
        race: CommitRace,
    ) -> anyhow::Result<()> {
        let what = format!("cleanup: {has_cleanup}, uncle: {with_uncle}, {race:?}");
        let tree = CommitTree::start(self, has_cleanup, with_uncle).await;
        let parent_id = tree.parent_id;
        let parent_final_state = if race.is_commit_failure() {
            JobState::Failed
        } else {
            JobState::Succeeded
        };
        let is_cancel_written = !race.is_commit_first() || with_uncle;
        let probes = TreeProbes::arm(self, tree.root_id, &tree.job_ids());
        let snapshot =
            is_cancel_written.then(|| CommitSnapshot::arm(self, tree.root_id, tree.root_id, 1));

        let (commit, cancelled) = self.race_commit(&tree, race, &what).await;

        assert_job_result(&commit, parent_final_state, &format!("{what}: the commit"));
        commit?;
        if is_cancel_written {
            assert_job_result(
                &cancelled,
                abort_target(has_cleanup),
                &format!("{what}: the cancel"),
            );
        } else {
            assert_stale(
                &cancelled,
                &StaleStateError::JobAlreadyTerminated(JobState::Succeeded),
                &format!("{what}: the cancel"),
            );
        }

        let num_rounds = match race {
            CommitRace::SameRoundCommitFirst
            | CommitRace::SameRoundCancelFirst
            | CommitRace::SameRoundCommitFailure => 1,
            CommitRace::CommitInFlight | CommitRace::AfterCommitReply => {
                if with_uncle {
                    2
                } else {
                    1
                }
            }
            CommitRace::CancelThenCommit
            | CommitRace::CancelThenNewInstanceCommit
            | CommitRace::CancelThenCommitFailure
            | CommitRace::CancelInFlight => 2,
        };
        probes.assert_rounds(num_rounds, num_rounds, &what);
        let mut transitions =
            HashMap::from([(tree.root_id, 1), (parent_id, 1), (tree.child_id, 0)]);
        transitions.extend(tree.uncle_id.map(|uncle_id| (uncle_id, 1)));
        probes.assert_transitions(&transitions, &what);
        if let Some(snapshot) = snapshot {
            let parent_state_at_cancel = match race {
                CommitRace::SameRoundCommitFirst
                | CommitRace::SameRoundCancelFirst
                | CommitRace::SameRoundCommitFailure => parent_final_state,
                CommitRace::CommitInFlight | CommitRace::AfterCommitReply => JobState::Succeeded,
                CommitRace::CancelThenCommit
                | CommitRace::CancelThenNewInstanceCommit
                | CommitRace::CancelThenCommitFailure
                | CommitRace::CancelInFlight => JobState::CommitReady,
            };
            assert_eq!(
                snapshot.rows().await,
                tree.states(race, parent_state_at_cancel),
                "{what}: the rows at the cancel's write"
            );
        }
        let expected_errors = if race.is_commit_failure() {
            HashMap::from([(parent_id, COMMIT_FAILURE.to_owned())])
        } else {
            HashMap::new()
        };
        self.finish_tree(
            tree.root_id,
            &tree.states(race, parent_final_state),
            &expected_errors,
            &what,
        )
        .await;
        Ok(())
    }

    /// Cancels the root of `tree`, then commits its `CommitReady` non-root job `P`, whose commit
    /// task gets its instance before or after the cancel as `race` says, and asserts the states of
    /// the tree in between.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The result of the commit's report.
    /// * The result of the cancel.
    async fn commit_after_cancel_reply(
        &self,
        tree: &CommitTree,
        race: CommitRace,
        what: &str,
    ) -> (JobResult, JobResult) {
        let parent_id = tree.parent_id;
        let early_instance_id = if matches!(race, CommitRace::CancelThenNewInstanceCommit) {
            None
        } else {
            Some(self.create_instance(parent_id, TaskId::Commit).await)
        };
        let cancelled = self.cancel(tree.root_id).await;
        self.assert_states(
            &tree.states(race, JobState::CommitReady),
            &format!("{what}: after the cancel"),
        )
        .await;
        let commit_instance_id = match early_instance_id {
            Some(commit_instance_id) => commit_instance_id,
            None => self.create_instance(parent_id, TaskId::Commit).await,
        };
        let commit = if race.is_commit_failure() {
            self.task_failure(
                parent_id,
                TaskId::Commit,
                commit_instance_id,
                COMMIT_FAILURE,
            )
            .await
        } else {
            self.commit_success(parent_id, commit_instance_id).await
        };
        (commit, cancelled)
    }

    /// Runs the commit of `tree`'s `CommitReady` non-root job and the cancel of its root as `race`
    /// says.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The result of the commit's report.
    /// * The result of the cancel.
    async fn race_commit(
        &self,
        tree: &CommitTree,
        race: CommitRace,
        what: &str,
    ) -> (JobResult, JobResult) {
        let root_id = tree.root_id;
        let parent_id = tree.parent_id;
        if race.is_cancel_answered_first() {
            return self.commit_after_cancel_reply(tree, race, what).await;
        }
        let cancelled = self.cancel(root_id);
        let commit_instance_id = self.create_instance(parent_id, TaskId::Commit).await;
        let commit = self.commit_success(parent_id, commit_instance_id);
        match race {
            CommitRace::SameRoundCommitFirst => {
                Race::start(
                    self,
                    HookPoint::CommitterBeforeReceive,
                    HookFilter::job(root_id),
                    commit,
                    cancelled,
                    root_id,
                )
                .await
                .finish()
                .await
            }
            CommitRace::SameRoundCommitFailure => {
                Race::start(
                    self,
                    HookPoint::CommitterBeforeReceive,
                    HookFilter::job(root_id),
                    self.task_failure(
                        parent_id,
                        TaskId::Commit,
                        commit_instance_id,
                        COMMIT_FAILURE,
                    ),
                    cancelled,
                    root_id,
                )
                .await
                .finish()
                .await
            }
            CommitRace::SameRoundCancelFirst | CommitRace::CancelInFlight => {
                let (point, filter) = if matches!(race, CommitRace::SameRoundCancelFirst) {
                    (HookPoint::CommitterBeforeReceive, HookFilter::job(root_id))
                } else {
                    (
                        HookPoint::CommitterRoundAfterDbWrite,
                        HookFilter::job(root_id),
                    )
                };
                let (cancelled, commit) =
                    Race::start(self, point, filter, cancelled, commit, parent_id)
                        .await
                        .finish()
                        .await;
                (commit, cancelled)
            }
            CommitRace::CommitInFlight => {
                Race::start(
                    self,
                    HookPoint::CommitterRoundAfterDbWrite,
                    HookFilter::job(parent_id),
                    commit,
                    cancelled,
                    root_id,
                )
                .await
                .finish()
                .await
            }
            CommitRace::CancelThenCommit
            | CommitRace::CancelThenNewInstanceCommit
            | CommitRace::CancelThenCommitFailure
            | CommitRace::AfterCommitReply => {
                let commit = commit.await;
                (commit, cancelled.await)
            }
        }
    }

    /// Races the cancel of a failure race's tree, with `cleanups`, with the failure of its leaf as
    /// `race` says, and asserts the outcome: the event planned first wins. A cancel planned first
    /// deactivates the whole tree and makes the failure stale, which stores no error; a failure
    /// planned first fails the leaf and its ancestors, aborts the rest of the tree and makes the
    /// cancel stale.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ServiceState::cancel_job`]'s return values on failure.
    /// * Forwards [`ServiceState::fail_task_instance`]'s return values on failure.
    ///
    /// [`ServiceState::cancel_job`]: spider_storage::state::ServiceState::cancel_job
    /// [`ServiceState::fail_task_instance`]:
    ///     spider_storage::state::ServiceState::fail_task_instance
    async fn assert_cancel_races_failure(
        &self,
        cleanups: Cleanups,
        race: FailureRace,
    ) -> anyhow::Result<()> {
        let what = format!("{cleanups:?}, {race:?}");
        let tree = FailureTree::start(self, cleanups).await;
        let root_id = tree.root_id;
        let leaf_id = tree.leaf_id;
        let task_instance_id = self.create_instance(leaf_id, TaskId::Index(0)).await;
        let job_ids = tree.job_ids();
        let probes = TreeProbes::arm(self, root_id, &job_ids);
        let failure_submissions =
            self.observe(HookPoint::JobEventAfterSubmit, HookFilter::job(leaf_id));
        let cancel_submissions =
            self.observe(HookPoint::JobEventAfterSubmit, HookFilter::job(root_id));
        let snapshot = CommitSnapshot::arm(self, root_id, root_id, 1);

        let (failure, cancelled) = self.race_failure(&tree, race, task_instance_id).await;

        let (expected, expected_errors) = if race.is_cancel_first() {
            assert_job_result(
                &cancelled,
                tree.target(root_id),
                &format!("{what}: the cancel"),
            );
            let expected_stale = if matches!(race, FailureRace::AfterCancelReply) {
                StaleStateError::JobNoLongerRunning
            } else {
                regular_stale(tree.target(leaf_id))
            };
            assert_stale(&failure, &expected_stale, &format!("{what}: the failure"));
            cancelled?;
            (tree.cancelled(), HashMap::new())
        } else {
            assert_job_result(&failure, JobState::Failed, &format!("{what}: the failure"));
            assert_stale(
                &cancelled,
                &StaleStateError::JobAlreadyTerminated(JobState::Failed),
                &format!("{what}: the cancel"),
            );
            failure?;
            (tree.failed(), tree.failure_errors())
        };
        let outlives = expected
            .values()
            .any(|state| *state == JobState::CleanupReady);

        assert_eq!(
            failure_submissions.hits(),
            usize::from(!matches!(race, FailureRace::AfterCancelReply)),
            "{what}: the submitted failures"
        );
        assert_eq!(
            cancel_submissions.hits(),
            1,
            "{what}: the submitted cancels"
        );
        let num_rounds = match race {
            FailureRace::CancelInFlight
            | FailureRace::FailureInFlight
            | FailureRace::AfterFailureReply => {
                if outlives {
                    2
                } else {
                    1
                }
            }
            FailureRace::SameRoundFailureFirst
            | FailureRace::SameRoundCancelFirst
            | FailureRace::AfterCancelReply => 1,
        };
        probes.assert_rounds(num_rounds, 1, &what);
        probes.assert_transitions(&job_ids.iter().map(|job_id| (*job_id, 1)).collect(), &what);
        assert_eq!(
            snapshot.rows().await,
            expected,
            "{what}: the rows at the winner's write"
        );
        self.finish_tree(root_id, &expected, &expected_errors, &what)
            .await;
        Ok(())
    }

    /// Runs the failure of `tree`'s leaf, through its instance `task_instance_id`, and the cancel
    /// of its root as `race` says.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The result of the failure's report.
    /// * The result of the cancel.
    async fn race_failure(
        &self,
        tree: &FailureTree,
        race: FailureRace,
        task_instance_id: TaskInstanceId,
    ) -> (JobResult, JobResult) {
        let root_id = tree.root_id;
        let leaf_id = tree.leaf_id;
        let failure = self.task_failure(
            leaf_id,
            TaskId::Index(0),
            task_instance_id,
            FailureTree::LEAF_FAILURE,
        );
        let cancelled = self.cancel(root_id);
        match race {
            FailureRace::SameRoundFailureFirst => {
                Race::start(
                    self,
                    HookPoint::CommitterBeforeReceive,
                    HookFilter::job(root_id),
                    failure,
                    cancelled,
                    root_id,
                )
                .await
                .finish()
                .await
            }
            FailureRace::SameRoundCancelFirst | FailureRace::CancelInFlight => {
                let point = if matches!(race, FailureRace::SameRoundCancelFirst) {
                    HookPoint::CommitterBeforeReceive
                } else {
                    HookPoint::CommitterRoundAfterDbWrite
                };
                let (cancelled, failure) = Race::start(
                    self,
                    point,
                    HookFilter::job(root_id),
                    cancelled,
                    failure,
                    leaf_id,
                )
                .await
                .finish()
                .await;
                (failure, cancelled)
            }
            FailureRace::AfterCancelReply => {
                let cancelled = cancelled.await;
                (failure.await, cancelled)
            }
            FailureRace::FailureInFlight => {
                Race::start(
                    self,
                    HookPoint::CommitterRoundAfterDbWrite,
                    HookFilter::job(leaf_id),
                    failure,
                    cancelled,
                    root_id,
                )
                .await
                .finish()
                .await
            }
            FailureRace::AfterFailureReply => {
                let failure = failure.await;
                (failure, cancelled.await)
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_cancel_deactivates_a_commit_ready_root() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    for has_cleanup in [false, true] {
        for race in RootCommitRace::iter() {
            env.assert_cancel_deactivates_commit_ready_root(has_cleanup, race)
                .await?;
        }
    }
    env.finish().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_root_cancel_racing_a_registration_never_leaves_a_live_child() -> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    for (registrar, cleanups) in [
        (Registrar::Leaf, Cleanups::None),
        (Registrar::Leaf, Cleanups::All),
        (Registrar::Leaf, Cleanups::BystandersOnly),
        (Registrar::Root, Cleanups::None),
        (Registrar::Root, Cleanups::All),
    ] {
        for race in RegistrationRace::iter() {
            Box::pin(env.assert_cancel_races_registration(registrar, cleanups, race)).await?;
        }
    }
    env.assert_lanes_empty().await;
    env.finish().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_root_cancel_racing_two_registrations_never_leaves_a_live_child() -> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    for cleanups in [Cleanups::None, Cleanups::All, Cleanups::RootOnly] {
        for race in TwoRegistrationRace::iter() {
            env.assert_cancel_races_two_registrations(cleanups, race)
                .await?;
        }
    }
    env.assert_lanes_empty().await;
    env.finish().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_cancel_skips_a_commit_ready_non_root_job_which_then_finishes() -> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    for has_cleanup in [false, true] {
        for race in CommitRace::iter() {
            env.assert_cancel_skips_commit_ready_job(has_cleanup, true, race)
                .await?;
            if race.is_commit_first() {
                env.assert_cancel_skips_commit_ready_job(has_cleanup, false, race)
                    .await?;
            }
        }
    }
    env.assert_lanes_empty().await;
    env.finish().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_root_cancel_racing_a_leaf_failure_lets_the_first_planned_win() -> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    for cleanups in [Cleanups::None, Cleanups::All, Cleanups::BystandersOnly] {
        for race in FailureRace::iter() {
            env.assert_cancel_races_failure(cleanups, race).await?;
        }
    }
    env.assert_lanes_empty().await;
    env.finish().await;
    Ok(())
}

/// # Returns
///
/// A one-task job shape with a cleanup task if `has_cleanup`.
const fn shape_with_cleanup(has_cleanup: bool) -> JobShape {
    JobShape {
        with_cleanup: has_cleanup,
        ..JobShape::ONE_TASK
    }
}

/// # Returns
///
/// The state that a cancel or an abort moves a running or blocking job to.
const fn abort_target(has_cleanup: bool) -> JobState {
    if has_cleanup {
        JobState::CleanupReady
    } else {
        JobState::Cancelled
    }
}

/// # Returns
///
/// The stale error that an event of a job's regular lane gets when it is planned against the job's
/// `state` after a cancel.
const fn regular_stale(state: JobState) -> StaleStateError {
    if state.is_terminal() {
        StaleStateError::JobAlreadyTerminated(state)
    } else {
        StaleStateError::JobNoLongerRunning
    }
}

/// # Returns
///
/// The stale error that one more cancel of a root in `state` gets.
const fn second_cancel_stale(state: JobState) -> StaleStateError {
    match state {
        JobState::CleanupReady => StaleStateError::JobCancellationAlreadyRequested,
        JobState::Cancelled => StaleStateError::JobAlreadyCancelled,
        state => StaleStateError::JobAlreadyTerminated(state),
    }
}
