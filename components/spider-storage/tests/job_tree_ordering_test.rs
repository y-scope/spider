//! Event ordering in a tree's committer, on `MariaDB`.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_storage::cache::error::StaleStateError;
use spider_storage::limits::MAX_CHILDREN_PER_JOB;
use spider_storage::limits::MAX_EVENTS_PER_ROUND;
use spider_storage::state::StorageServerError;
use spider_storage::test_hooks::HookFilter;
use spider_storage::test_hooks::HookPoint;
use tokio::task::JoinHandle;

use crate::runtime_infra::HOOK_TIMEOUT;
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

/// The message of a task's failure.
const FAILURE: &str = "sibling failure";

/// How long a step of a test that nobody polls the lanes for may take before the test counts it as
/// stalled.
const STALL_TIMEOUT: Duration = Duration::from_secs(60);

/// The event that a test fast-forwards past a backlog of completions.
#[derive(Debug, Clone, Copy)]
enum FastForwarded {
    Cancel,
    Failure,
}

/// A root job with a cleanup task whose children have task instances: one child whose completion a
/// test holds in flight, the children whose completions queue behind it, a child that can fail, and
/// a bystander with a cleanup task.
struct BacklogTree {
    root_id: JobId,
    held_id: JobId,
    queued_ids: Vec<JobId>,
    failing_id: JobId,
    bystander_id: JobId,
    task_instance_ids: HashMap<JobId, TaskInstanceId>,
}

impl BacklogTree {
    /// Starts a backlog tree with `num_queued` queued children, and creates an instance of the task
    /// of each child but the bystander.
    ///
    /// # Returns
    ///
    /// The started tree.
    ///
    /// # Panics
    ///
    /// Panics if the bystander, the failing child or the held child isn't registered.
    async fn start(env: &TestEnv, num_queued: usize) -> Self {
        let root_id = env.start_root(&JobShape::ONE_TASK_WITH_CLEANUP).await;
        let mut shapes = vec![JobShape::ONE_TASK; num_queued + 2];
        shapes.push(JobShape::ONE_TASK_WITH_CLEANUP);
        let mut child_ids = env
            .register_children(root_id, &value(0x60, 0), &shapes)
            .await;
        let bystander_id = child_ids.pop().expect("the bystander should be registered");
        let mut task_instance_ids = HashMap::new();
        for child_id in &child_ids {
            task_instance_ids.insert(
                *child_id,
                env.create_instance(*child_id, TaskId::Index(0)).await,
            );
        }
        let failing_id = child_ids
            .pop()
            .expect("the failing child should be registered");
        let held_id = child_ids.remove(0);
        Self {
            root_id,
            held_id,
            queued_ids: child_ids,
            failing_id,
            bystander_id,
            task_instance_ids,
        }
    }

    /// # Returns
    ///
    /// The states of the tree's jobs once `fast_forwarded` and the backlog are planned.
    fn states_after(&self, fast_forwarded: FastForwarded) -> HashMap<JobId, JobState> {
        let (root_state, failing_state) = match fast_forwarded {
            FastForwarded::Cancel => (JobState::CleanupReady, JobState::Cancelled),
            FastForwarded::Failure => (JobState::Failed, JobState::Failed),
        };
        let mut states: HashMap<JobId, JobState> = self
            .queued_ids
            .iter()
            .map(|queued_id| (*queued_id, JobState::Cancelled))
            .collect();
        states.extend([
            (self.root_id, root_state),
            (self.held_id, JobState::Succeeded),
            (self.failing_id, failing_state),
            (self.bystander_id, JobState::CleanupReady),
        ]);
        states
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_cancel_behind_a_full_backlog_is_planned_in_the_next_round() -> anyhow::Result<()> {
    assert_fast_forwarded_past_a_full_backlog(FastForwarded::Cancel).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_failure_behind_a_full_backlog_is_planned_in_the_next_round() -> anyhow::Result<()> {
    assert_fast_forwarded_past_a_full_backlog(FastForwarded::Failure).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_failure_aborting_a_registering_parent_in_its_round_inserts_no_child()
-> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    let root_id = env.start_root(&JobShape::ONE_TASK).await;
    let [registrar_id, failing_id] = env
        .register_children(
            root_id,
            &value(0x50, 0),
            &[JobShape::ONE_TASK, JobShape::ONE_TASK],
        )
        .await[..]
    else {
        panic!("the root should have two children");
    };
    let registrar_instance_id = env.create_instance(registrar_id, TaskId::Index(0)).await;
    let failing_instance_id = env.create_instance(failing_id, TaskId::Index(0)).await;
    let probes = TreeProbes::arm(&env, root_id, &[root_id, registrar_id, failing_id]);

    let registration = env.task_success(
        registrar_id,
        0,
        registrar_instance_id,
        &value(0x51, 0),
        (0..3).map(|_| JobShape::ONE_TASK.child()).collect(),
    );
    let failure = env.task_failure(failing_id, TaskId::Index(0), failing_instance_id, FAILURE);
    let (registration, failure) = Race::start(
        &env,
        HookPoint::CommitterBeforeReceive,
        HookFilter::job(root_id),
        registration,
        failure,
        failing_id,
    )
    .await
    .finish()
    .await;

    assert_job_result(&failure, JobState::Failed, "the failure");
    assert_stale(
        &registration,
        &StaleStateError::JobAlreadyTerminated(JobState::Cancelled),
        "the registration",
    );
    probes.assert_rounds(1, 1, "the failure and the registration");
    probes.assert_transitions(
        &HashMap::from([(root_id, 1), (registrar_id, 1), (failing_id, 1)]),
        "the failure and the registration",
    );
    assert_eq!(env.children_of(registrar_id).await, Vec::new());
    let expected = HashMap::from([
        (root_id, JobState::Failed),
        (registrar_id, JobState::Cancelled),
        (failing_id, JobState::Failed),
    ]);
    env.assert_tree_rows(root_id, &expected, "after the round")
        .await;
    env.assert_states(&expected, "after the round").await;
    assert_eq!(
        env.tree_errors(root_id).await,
        HashMap::from([
            (
                root_id,
                format!("descendant job {failing_id} failed: {FAILURE}")
            ),
            (failing_id, FAILURE.to_owned()),
        ])
    );
    env.assert_quiescent(root_id).await;
    env.assert_lanes_empty().await;
    env.finish().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_scheduler_that_never_polls_the_lanes_stalls_no_tree() -> anyhow::Result<()> {
    const NUM_CHILDREN: usize = 300;

    let env = TestEnv::start_keeping_task_instances().await;
    let num_filler_cleanups = publish_filler_cleanups(&env).await;
    let root_id = env
        .start_root_without_draining(&JobShape::ONE_TASK_WITH_CLEANUP)
        .await;
    let child_ids = without_stalling(
        env.register_children_without_draining(
            root_id,
            &value(0x63, 0),
            &vec![JobShape::ONE_TASK_WITH_CLEANUP; NUM_CHILDREN],
        ),
        "the registration",
    )
    .await;
    let before_pool_send = env.observe(
        HookPoint::CreateInstanceBeforePoolSend,
        HookFilter::default(),
    );
    let creation_join_handles: Vec<JoinHandle<Result<_, StorageServerError>>> = child_ids
        .iter()
        .map(|child_id| {
            let service = env.service.clone();
            let (child_id, execution_manager_id) = (*child_id, env.execution_manager_id);
            tokio::spawn(async move {
                service
                    .create_task_instance(
                        service.session_id(),
                        child_id,
                        TaskId::Index(0),
                        execution_manager_id,
                    )
                    .await
                    .map(|execution_context| execution_context.task_instance_id)
            })
        })
        .collect();
    reached(&before_pool_send, NUM_CHILDREN).await;
    assert_job_result(
        &without_stalling(env.cancel(root_id), "the cancel").await,
        JobState::CleanupReady,
        "the cancel",
    );
    for creation_join_handle in creation_join_handles {
        let created = join(creation_join_handle).await;
        assert!(
            created.is_ok(),
            "an instance creation past its state check should succeed, got {created:?}"
        );
    }

    let mut job_ids = child_ids;
    job_ids.push(root_id);
    for job_id in &job_ids {
        without_stalling(env.run_cleanup(*job_id), "a cleanup").await;
    }
    let expected: HashMap<JobId, JobState> = job_ids
        .iter()
        .map(|job_id| (*job_id, JobState::Cancelled))
        .collect();
    env.assert_tree_rows(root_id, &expected, "after the cleanups")
        .await;
    env.assert_states(&expected, "after the cleanups").await;
    assert_eq!(
        drain_cleanup_lane(&env).await,
        num_filler_cleanups + job_ids.len(),
        "every cleanup task should stay published while nobody polls the lane"
    );
    env.finish().await;
    Ok(())
}

/// Holds a round of a tree's committer between its database write and its apply, queues more than
/// [`MAX_EVENTS_PER_ROUND`] completions of the root's children behind it, then submits
/// `fast_forwarded`, and checks that the round after the held one plans `fast_forwarded` first and
/// enough of the completions to fill it up, and that the round after that plans the rest of them.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`wait_until_answered`]'s return values on failure.
///
/// # Panics
///
/// Panics if an assertion fails.
async fn assert_fast_forwarded_past_a_full_backlog(
    fast_forwarded: FastForwarded,
) -> anyhow::Result<()> {
    let env = TestEnv::start_keeping_task_instances().await;
    let tree = BacklogTree::start(&env, MAX_EVENTS_PER_ROUND + 44).await;
    let root_id = tree.root_id;
    let held = env.pause(
        HookPoint::CommitterRoundAfterDbWrite,
        HookFilter::job(tree.held_id),
    );
    let held_completion_join_handle = tokio::spawn(env.task_success(
        tree.held_id,
        0,
        tree.task_instance_ids[&tree.held_id],
        &value(0x61, 0),
        Vec::new(),
    ));
    reached(&held, 1).await;

    let queued_submissions = env.observe(HookPoint::JobEventAfterSubmit, HookFilter::default());
    let queued_completion_join_handles: Vec<JoinHandle<JobResult>> = tree
        .queued_ids
        .iter()
        .enumerate()
        .map(|(index, queued_id)| {
            tokio::spawn(env.task_success(
                *queued_id,
                0,
                tree.task_instance_ids[queued_id],
                &value(0x62, index),
                Vec::new(),
            ))
        })
        .collect();
    reached(&queued_submissions, tree.queued_ids.len()).await;

    let (fast_forwarded_job_id, fast_forwarded_state) = match fast_forwarded {
        FastForwarded::Cancel => (root_id, JobState::CleanupReady),
        FastForwarded::Failure => (tree.failing_id, JobState::Failed),
    };
    let fast_forwarded_submission = env.observe(
        HookPoint::JobEventAfterSubmit,
        HookFilter::job(fast_forwarded_job_id),
    );
    let fast_forwarded_join_handle = match fast_forwarded {
        FastForwarded::Cancel => tokio::spawn(env.cancel(root_id)),
        FastForwarded::Failure => tokio::spawn(env.task_failure(
            tree.failing_id,
            TaskId::Index(0),
            tree.task_instance_ids[&tree.failing_id],
            FAILURE,
        )),
    };
    reached(&fast_forwarded_submission, 1).await;
    let probes = TreeProbes::arm(&env, root_id, &[]);
    let fast_forwarded_round = env.pause(
        HookPoint::CommitterRoundAfterDbWrite,
        HookFilter::job(root_id),
    );
    held.release();
    reached(&fast_forwarded_round, 1).await;
    probes.assert_rounds(1, 1, "the round after the held one");
    let next_receive = env.pause(HookPoint::CommitterBeforeReceive, HookFilter::job(root_id));
    fast_forwarded_round.release();
    reached(&next_receive, 1).await;
    wait_until_answered(&queued_completion_join_handles, MAX_EVENTS_PER_ROUND - 1)
        .await
        .context(
            "the round after the held one should answer enough queued completions to fill it up",
        )?;
    next_receive.release();

    assert_job_result(
        &join(held_completion_join_handle).await,
        JobState::Succeeded,
        "the held completion",
    );
    assert_job_result(
        &join(fast_forwarded_join_handle).await,
        fast_forwarded_state,
        "the fast-forwarded event",
    );
    for queued_completion_join_handle in queued_completion_join_handles {
        assert_stale(
            &join(queued_completion_join_handle).await,
            &StaleStateError::JobAlreadyTerminated(JobState::Cancelled),
            "a queued completion",
        );
    }
    probes.assert_rounds(2, 1, "the backlog");
    assert_backlog_tree_finishes(&env, &tree, fast_forwarded).await;
    env.finish().await;
    Ok(())
}

/// Asserts the rows and the errors of `tree` once `fast_forwarded` and the backlog are planned,
/// then runs the published cleanups and asserts that the tree is quiescent.
///
/// # Panics
///
/// Panics if an assertion fails.
async fn assert_backlog_tree_finishes(
    env: &TestEnv,
    tree: &BacklogTree,
    fast_forwarded: FastForwarded,
) {
    let (root_id, failing_id) = (tree.root_id, tree.failing_id);
    let mut expected = tree.states_after(fast_forwarded);
    env.assert_tree_rows(root_id, &expected, "after the backlog")
        .await;
    env.assert_states(&expected, "after the backlog").await;
    let (expected_errors, cleanup_ids) = match fast_forwarded {
        FastForwarded::Cancel => (HashMap::new(), vec![root_id, tree.bystander_id]),
        FastForwarded::Failure => (
            HashMap::from([
                (
                    root_id,
                    format!("descendant job {failing_id} failed: {FAILURE}"),
                ),
                (failing_id, FAILURE.to_owned()),
            ]),
            vec![tree.bystander_id],
        ),
    };
    assert_eq!(env.tree_errors(root_id).await, expected_errors);
    env.run_cleanups(&cleanup_ids).await;
    for cleanup_id in cleanup_ids {
        expected.insert(cleanup_id, JobState::Cancelled);
    }
    env.assert_states(&expected, "after the cleanups").await;
    env.assert_quiescent(root_id).await;
    env.assert_lanes_empty().await;
}

/// Waits until at least `num_answered` of the completions of `completion_join_handles` are
/// answered.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`tokio::time::timeout`]'s return values on failure, if fewer than `num_answered`
///   completions are answered within [`HOOK_TIMEOUT`].
async fn wait_until_answered(
    completion_join_handles: &[JoinHandle<JobResult>],
    num_answered: usize,
) -> anyhow::Result<()> {
    let count_answered = || {
        completion_join_handles
            .iter()
            .filter(|completion_join_handle| completion_join_handle.is_finished())
            .count()
    };
    tokio::time::timeout(HOOK_TIMEOUT, async {
        while count_answered() < num_answered {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    Ok(())
}

/// Starts a filler tree of [`MAX_CHILDREN_PER_JOB`] children with cleanup tasks and cancels it, so
/// that the cleanup tasks of all its jobs are published, then creates instances of 8 of those
/// cleanup tasks for a dead execution manager, whose tasks the task instance pool publishes again,
/// all without polling a lane.
///
/// # Returns
///
/// The number of cleanup tasks published.
///
/// # Panics
///
/// Panics if an instance creation for the dead execution manager fails.
async fn publish_filler_cleanups(env: &TestEnv) -> usize {
    const NUM_DEAD_INSTANCES: usize = 8;

    let filler_root_id = env
        .start_root_without_draining(&JobShape::ONE_TASK_WITH_CLEANUP)
        .await;
    let filler_child_ids = without_stalling(
        env.register_children_without_draining(
            filler_root_id,
            &value(0x63, 0),
            &vec![JobShape::ONE_TASK_WITH_CLEANUP; MAX_CHILDREN_PER_JOB],
        ),
        "the filler tree's registration",
    )
    .await;
    assert_job_result(
        &without_stalling(env.cancel(filler_root_id), "the filler tree's cancel").await,
        JobState::CleanupReady,
        "the filler tree's cancel",
    );
    let dead_execution_manager_id = env.register_dead_execution_manager().await;
    for filler_child_id in &filler_child_ids[..NUM_DEAD_INSTANCES] {
        without_stalling(
            env.service.create_task_instance(
                env.service.session_id(),
                *filler_child_id,
                TaskId::Cleanup,
                dead_execution_manager_id,
            ),
            "an instance creation for the dead execution manager",
        )
        .await
        .expect("the instance creation for the dead execution manager should succeed");
    }
    filler_child_ids.len() + 1 + NUM_DEAD_INSTANCES
}

/// Polls the cleanup lane until a poll returns nothing.
///
/// # Returns
///
/// The number of entries polled.
///
/// # Panics
///
/// Panics if the lane doesn't drain within [`STALL_TIMEOUT`].
async fn drain_cleanup_lane(env: &TestEnv) -> usize {
    tokio::time::timeout(STALL_TIMEOUT, async {
        let mut num_entries = 0;
        loop {
            let entries = env
                .poll_lane(Lane::Cleanup, 4096, Duration::from_millis(500))
                .await;
            if entries.is_empty() {
                return num_entries;
            }
            num_entries += entries.len();
        }
    })
    .await
    .expect("the cleanup lane should drain within the timeout")
}

/// Awaits `future`, which must not wait for anybody to poll an inbound-queue lane.
///
/// # Type Parameters
///
/// * `OutputType` - The output type of `future`.
///
/// # Returns
///
/// The output of `future`.
///
/// # Panics
///
/// Panics if `future` doesn't finish within [`STALL_TIMEOUT`].
async fn without_stalling<OutputType>(
    future: impl Future<Output = OutputType>,
    what: &str,
) -> OutputType {
    tokio::time::timeout(STALL_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("{what} should finish while nobody polls the lanes"))
}
