//! The order in which a tree's committer plans the events of a round, on `MariaDB`.

use std::collections::HashMap;

use spider_core::job::JobState;
use spider_core::types::id::TaskId;
use spider_storage::cache::error::StaleStateError;
use spider_storage::test_hooks::HookFilter;
use spider_storage::test_hooks::HookPoint;

use crate::runtime_infra::JobShape;
use crate::runtime_infra::Race;
use crate::runtime_infra::TestEnv;
use crate::runtime_infra::TreeProbes;
use crate::runtime_infra::assert_job_result;
use crate::runtime_infra::assert_stale;
use crate::runtime_infra::value;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MariaDB"]
async fn a_failure_aborting_a_registering_parent_in_its_round_inserts_no_child() {
    const FAILURE: &str = "sibling failure";

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
}
