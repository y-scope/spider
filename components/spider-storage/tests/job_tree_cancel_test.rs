//! Cancels of job trees racing the events of their jobs, on `MariaDB`.

use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::id::TaskId;
use spider_core::types::io::TaskOutputsSerializer;
use spider_storage::cache::error::CacheError;
use spider_storage::cache::error::StaleStateError;
use spider_storage::state::StorageServerError;
use spider_storage::test_hooks::HookAction;
use spider_storage::test_hooks::HookCount;
use spider_storage::test_hooks::HookFilter;
use spider_storage::test_hooks::HookPoint;
use strum::EnumIter;
use strum::IntoEnumIterator;

use crate::runtime_infra::HOOK_TIMEOUT;
use crate::runtime_infra::TestEnv;
use crate::task_graph_builder::build_flat_task_graph;

/// The result of a job RPC.
type JobResult = Result<JobState, StorageServerError>;

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

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_cancel_deactivates_a_commit_ready_root() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    for has_cleanup in [false, true] {
        for race in RootCommitRace::iter() {
            assert_cancel_deactivates_commit_ready_root(&env, has_cleanup, race).await?;
        }
    }
    env.finish().await;
    Ok(())
}

/// Races the cancel of a `CommitReady` root, with a cleanup task if `has_cleanup`, with the root's
/// commit as `race` says, and asserts the outcome: the cancel deactivates the root, and the commit
/// is stale, unless the commit is planned first, which succeeds the root and makes the cancel
/// stale.
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
/// Panics if the outcome is not the one asserted, or if [`race_root_commit`] or a [`TestEnv`]
/// helper panics.
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
    env: &TestEnv,
    has_cleanup: bool,
    race: RootCommitRace,
) -> anyhow::Result<()> {
    let what = format!("has_cleanup {has_cleanup}, {race:?}");
    let (graph, inputs) = build_flat_task_graph(1, 4, true, has_cleanup);
    let job_id = env.start_job(&graph, &inputs).await;
    let task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    let serialized_outputs = TaskOutputsSerializer::from_tuple(&(vec![1u8; 4],))?;
    let state = env
        .service
        .succeed_task_instance(
            env.service.session_id(),
            job_id,
            task_instance_id,
            0,
            serialized_outputs.clone(),
        )
        .await?;
    assert_eq!(state, JobState::CommitReady, "{what}: the regular task");
    env.poll_commit_ready_task(job_id).await;
    let cancelled_state = if has_cleanup {
        JobState::CleanupReady
    } else {
        JobState::Cancelled
    };

    let (commit, cancel) = race_root_commit(env, job_id, race).await;

    if race.is_commit_first() {
        assert_eq!(commit?, JobState::Succeeded, "{what}: the commit");
        assert_stale(
            &cancel,
            &StaleStateError::JobAlreadyTerminated(JobState::Succeeded),
            &format!("{what}: the cancel"),
        );
        assert_eq!(
            env.service.get_job_outputs(job_id).await?,
            TaskOutputsSerializer::deserialize(&serialized_outputs)?,
            "{what}: the root's outputs"
        );
        env.assert_job_state(job_id, JobState::Succeeded).await;
        return Ok(());
    }

    assert_eq!(cancel?, cancelled_state, "{what}: the cancel");
    let expected_stale = match race {
        RootCommitRace::SameRoundCommitFirst
        | RootCommitRace::SameRoundCancelFirst
        | RootCommitRace::CancelInFlight
            if !has_cleanup =>
        {
            StaleStateError::JobAlreadyTerminated(JobState::Cancelled)
        }
        _ => StaleStateError::JobNoLongerCommitReady,
    };
    assert_stale(&commit, &expected_stale, &format!("{what}: the commit"));
    env.assert_job_state(job_id, cancelled_state).await;
    if has_cleanup {
        env.poll_cleanup_ready_task(job_id).await;
        let cleanup_instance_id = env.create_instance(job_id, TaskId::Cleanup).await;
        let state = env
            .service
            .succeed_cleanup_task_instance(env.service.session_id(), job_id, cleanup_instance_id)
            .await?;
        assert_eq!(state, JobState::Cancelled, "{what}: the cleanup");
        env.assert_job_state(job_id, JobState::Cancelled).await;
    }
    Ok(())
}

/// Runs the commit of the `CommitReady` root `job_id` and the root's cancel as `race` says.
///
/// # Returns
///
/// A pair, containing:
///
/// * The result of the commit's success or, for the races that say so, of the commit's failure or
///   of the commit's instance registration.
/// * The result of the cancel.
///
/// # Panics
///
/// Panics if an RPC panics, or if a hook is not reached within [`HOOK_TIMEOUT`].
async fn race_root_commit(
    env: &TestEnv,
    job_id: JobId,
    race: RootCommitRace,
) -> (JobResult, JobResult) {
    let service = env.service.clone();
    let cancel = async move { service.cancel_job(job_id).await };
    match race {
        RootCommitRace::CancelThenLateCommit => {
            let commit = succeed_commit(env, job_id).await;
            let cancel = cancel.await;
            (commit.await, cancel)
        }
        RootCommitRace::CancelThenLateCommitFailure => {
            let commit_instance_id = env.create_instance(job_id, TaskId::Commit).await;
            let cancel = cancel.await;
            let failure = env
                .service
                .fail_task_instance(
                    env.service.session_id(),
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
            let registration = env
                .service
                .create_task_instance(
                    env.service.session_id(),
                    job_id,
                    TaskId::Commit,
                    env.execution_manager_id,
                )
                .await
                .map(|_| JobState::CommitReady);
            (registration, cancel)
        }
        RootCommitRace::SameRoundCommitFirst => {
            let commit = succeed_commit(env, job_id).await;
            race_in_committer(
                env,
                job_id,
                HookPoint::CommitterBeforeReceive,
                commit,
                cancel,
            )
            .await
        }
        RootCommitRace::SameRoundCancelFirst => {
            let commit = succeed_commit(env, job_id).await;
            let (cancel, commit) = race_in_committer(
                env,
                job_id,
                HookPoint::CommitterBeforeReceive,
                cancel,
                commit,
            )
            .await;
            (commit, cancel)
        }
        RootCommitRace::CancelInFlight => {
            let commit = succeed_commit(env, job_id).await;
            let (cancel, commit) = race_in_committer(
                env,
                job_id,
                HookPoint::CommitterRoundAfterDbWrite,
                cancel,
                commit,
            )
            .await;
            (commit, cancel)
        }
        RootCommitRace::CommitInFlight => {
            let commit = succeed_commit(env, job_id).await;
            race_in_committer(
                env,
                job_id,
                HookPoint::CommitterRoundAfterDbWrite,
                commit,
                cancel,
            )
            .await
        }
        RootCommitRace::AfterCommitReply => {
            let commit = succeed_commit(env, job_id).await.await;
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
    env: &TestEnv,
    job_id: JobId,
) -> impl Future<Output = JobResult> + Send + 'static {
    let commit_instance_id = env.create_instance(job_id, TaskId::Commit).await;
    let service = env.service.clone();
    async move {
        service
            .succeed_commit_task_instance(service.session_id(), job_id, commit_instance_id)
            .await
    }
}

/// Runs `first` until the committer of the tree rooted at `job_id` pauses at `point`, then runs
/// `second` until it has submitted its event to the committer, then lets the committer go on.
///
/// # Returns
///
/// The results of `first` and `second`.
///
/// # Panics
///
/// Panics if either RPC panics, or if a hook is not reached within [`HOOK_TIMEOUT`].
async fn race_in_committer(
    env: &TestEnv,
    job_id: JobId,
    point: HookPoint,
    first: impl Future<Output = JobResult> + Send + 'static,
    second: impl Future<Output = JobResult> + Send + 'static,
) -> (JobResult, JobResult) {
    let committer_pause = env.hooks.arm(
        point,
        HookFilter::job(job_id),
        HookAction::Pause,
        HookCount::Once,
    );
    let first = tokio::spawn(first);
    committer_pause
        .reached(1, HOOK_TIMEOUT)
        .await
        .expect("the first RPC's event should reach the committer's hook");

    let submit_pause = env.hooks.arm(
        HookPoint::JobEventAfterSubmit,
        HookFilter::job(job_id),
        HookAction::Pause,
        HookCount::Once,
    );
    let second = tokio::spawn(second);
    submit_pause
        .reached(1, HOOK_TIMEOUT)
        .await
        .expect("the second RPC should submit its event");
    submit_pause.release();
    committer_pause.release();

    (
        first.await.expect("the first RPC should not panic"),
        second.await.expect("the second RPC should not panic"),
    )
}

/// Asserts that `result` is the stale state error `expected`.
///
/// # Panics
///
/// Panics if `result` is not the [`StaleStateError`] `expected`.
fn assert_stale(result: &JobResult, expected: &StaleStateError, what: &str) {
    assert!(
        matches!(
            result,
            Err(StorageServerError::Cache(CacheError::StaleState(error)))
                if format!("{error:?}") == format!("{expected:?}")
        ),
        "{what}: expected the stale error {expected:?}, got {result:?}"
    );
}
