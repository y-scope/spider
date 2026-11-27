//! Child-job registration through the storage service on `MariaDB`.

use std::collections::HashMap;
use std::iter;

use spider_core::job::JobState;
use spider_core::task::ExecutionPolicy;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::io::TaskGraphInputBuilder;
use spider_core::types::io::TaskOutputsSerializer;
use spider_proto_rust::storage::ReportTaskSuccessRequest;
use spider_proto_rust::storage::TaskInstanceManagementService;
use spider_storage::cache::error::CacheError;
use spider_storage::cache::error::StaleStateError;
use spider_storage::db::DbError;
use spider_storage::grpc::GrpcServiceState;
use spider_storage::job_submission::CompressedJobSubmission;
use spider_storage::limits::MAX_CHILDREN_PER_JOB;
use spider_storage::limits::MAX_DEPTH;
use spider_storage::limits::MAX_JOB_CHILDREN_SIZE;
use spider_storage::limits::MAX_REPORT_CHILDREN_SIZE;
use spider_storage::limits::MAX_REPORT_OUTPUTS_SIZE;
use spider_storage::state::StorageServerError;
use spider_storage::state::error::ReportRejection;
use spider_storage::test_hooks::HookAction;
use spider_storage::test_hooks::HookCount;
use spider_storage::test_hooks::HookFilter;
use spider_storage::test_hooks::HookPoint;
use tokio::time::timeout;
use tonic::Code;
use tonic::Request;

use crate::runtime_infra::ChildRow;
use crate::runtime_infra::HOOK_TIMEOUT;
use crate::runtime_infra::JobShape;
use crate::runtime_infra::TestEnv;
use crate::runtime_infra::build_chain_task_graph;
use crate::runtime_infra::task_outputs;
use crate::runtime_infra::value;
use crate::task_graph_builder::compress_job_inputs;
use crate::task_graph_builder::compress_task_graph;

const MIB: usize = 1024 * 1024;

impl TestEnv {
    /// Runs a new instance of every task of `job_id`, a job of `shape`, to success in task order,
    /// and asserts that only the last report succeeds the job.
    ///
    /// # Panics
    ///
    /// Panics if the assertion fails.
    async fn run_all_tasks(&self, job_id: JobId, shape: &JobShape) {
        for task_index in 0..shape.num_tasks {
            let expected_state = if task_index + 1 == shape.num_tasks {
                JobState::Succeeded
            } else {
                JobState::Running
            };
            assert_eq!(
                self.run_task(job_id, task_index, &value(0x21, task_index))
                    .await,
                expected_state,
                "task {task_index} of job {job_id}"
            );
        }
    }

    /// Asserts that each of the five external job RPCs answers that `unknown_id`, a job that the
    /// database doesn't hold, isn't found.
    ///
    /// # Panics
    ///
    /// Panics if an RPC answers otherwise.
    async fn assert_job_rpcs_answer_unknown_job(&self, unknown_id: JobId) {
        let start = self.service.start_job(unknown_id).await;
        assert!(
            matches!(
                start,
                Err(StorageServerError::Db(DbError::JobNotFound(job_id))) if job_id == unknown_id
            ),
            "the start of an unknown job gave {start:?}"
        );
        let cancel = self.service.cancel_job(unknown_id).await;
        assert!(
            matches!(
                cancel,
                Err(StorageServerError::JobNotFound(job_id)) if job_id == unknown_id
            ),
            "the cancel of an unknown job gave {cancel:?}"
        );
        let state = self.service.get_job_state(unknown_id).await;
        assert!(
            matches!(
                state,
                Err(StorageServerError::Db(DbError::JobNotFound(job_id))) if job_id == unknown_id
            ),
            "the state of an unknown job gave {state:?}"
        );
        let outputs = self.service.get_job_outputs(unknown_id).await;
        assert!(
            matches!(
                outputs,
                Err(StorageServerError::Db(DbError::JobNotFound(job_id))) if job_id == unknown_id
            ),
            "the outputs of an unknown job gave {outputs:?}"
        );
        let error = self.service.get_job_error(unknown_id).await;
        assert!(
            matches!(
                error,
                Err(StorageServerError::Db(DbError::JobNotFound(job_id))) if job_id == unknown_id
            ),
            "the error of an unknown job gave {error:?}"
        );
    }

    /// Asserts that each of `StartJob`, `CancelJob` and `GetJobOutputs` rejects `child_id`, a child
    /// job, as a bad request that names the child, and that `GetJobState` and `GetJobError` answer
    /// that the child is in `expected_state`, a state other than [`JobState::Failed`].
    ///
    /// # Panics
    ///
    /// Panics if an RPC answers otherwise.
    async fn assert_job_rpcs_answer_child(
        &self,
        child_id: JobId,
        expected_state: JobState,
        what: &str,
    ) {
        let expected_message = format!("job {child_id} is not a root job");
        assert_bad_request(
            &self.service.start_job(child_id).await,
            &expected_message,
            &format!("{what}: the start"),
        );
        assert_bad_request(
            &self.service.cancel_job(child_id).await,
            &expected_message,
            &format!("{what}: the cancel"),
        );
        assert_bad_request(
            &self.service.get_job_outputs(child_id).await,
            &expected_message,
            &format!("{what}: the outputs"),
        );
        let state = self.service.get_job_state(child_id).await;
        assert!(
            matches!(state, Ok(current) if current == expected_state),
            "{what}: expected the state {expected_state}, got {state:?}"
        );
        let error = self.service.get_job_error(child_id).await;
        assert!(
            matches!(
                error,
                Err(StorageServerError::Db(DbError::UnexpectedJobState { current, .. }))
                    if current == expected_state
            ),
            "{what}: expected the error of a job in {expected_state} to be unavailable, got \
             {error:?}"
        );
    }

    /// Asserts that `job_id` is not in the job cache: an instance creation for its task 0 finds no
    /// job.
    ///
    /// # Panics
    ///
    /// Panics if the assertion fails.
    async fn assert_not_cached(&self, job_id: JobId, what: &str) {
        let instance = self
            .service
            .create_task_instance(
                self.service.session_id(),
                job_id,
                TaskId::Index(0),
                self.execution_manager_id,
            )
            .await;
        assert!(
            matches!(
                instance,
                Err(StorageServerError::JobNotFound(not_found_id)) if not_found_id == job_id
            ),
            "{what}: job {job_id} should not be cached, got {instance:?}"
        );
    }
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_report_with_children_blocks_its_job_and_inserts_them_once() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let parent_id = env.start_chain_job(1, 1).await;
    let children: Vec<_> = (1..=3).map(chain_child).collect();
    let task_graphs: Vec<_> = children
        .iter()
        .map(|child| child.compressed_serialized_task_graph.clone())
        .collect();

    let task_id = TaskId::Index(0);
    let task_instance_id = env.create_instance(parent_id, task_id).await;
    let grpc = GrpcServiceState::new(env.service.clone(), env.runtime.get_shutdown_handle());
    let report = ReportTaskSuccessRequest {
        job_id: parent_id.get(),
        task_id: Some(task_id.into()),
        task_instance_id,
        execution_manager_id: env.execution_manager_id.get(),
        session_id: env.service.session_id(),
        serialized_outputs: TaskOutputsSerializer::from_tuple(&(&[1u8; 4][..],))?,
        children: children.into_iter().map(Into::into).collect(),
    };
    timeout(
        HOOK_TIMEOUT,
        grpc.report_task_success(Request::new(report.clone())),
    )
    .await??;
    env.assert_job_state(parent_id, JobState::BlockingOnChildren)
        .await;
    let parent_outputs = env
        .row_outputs(parent_id)
        .await
        .expect("the parent should have stored outputs");

    let rows = env.child_rows(parent_id).await;
    assert_eq!(rows.len(), task_graphs.len());
    let child_ids: Vec<JobId> = rows.iter().map(|row| row.id).collect();
    assert_eq!(
        rows,
        child_ids
            .iter()
            .zip(task_graphs)
            .map(|(child_id, task_graph)| ChildRow {
                id: *child_id,
                parent_id: Some(parent_id),
                root_id: Some(parent_id),
                state: JobState::Running,
                compressed_serialized_task_graph: task_graph,
            })
            .collect::<Vec<_>>()
    );
    let resource_groups = sqlx::query_scalar::<_, ResourceGroupId>(
        "SELECT `resource_group_id` FROM `jobs` WHERE `parent_id` = ?;",
    )
    .bind(parent_id)
    .fetch_all(&env.pool)
    .await?;
    assert_eq!(resource_groups, vec![env.resource_group_id; rows.len()]);

    let replay = timeout(HOOK_TIMEOUT, grpc.report_task_success(Request::new(report)))
        .await?
        .expect_err("replaying the accepted report should be stale");
    assert_eq!(replay.code(), Code::FailedPrecondition);
    env.assert_job_state(parent_id, JobState::BlockingOnChildren)
        .await;
    assert_eq!(env.row_outputs(parent_id).await, Some(parent_outputs));
    assert_eq!(env.child_rows(parent_id).await, rows);

    for child_id in &child_ids {
        env.assert_job_state(*child_id, JobState::Running).await;
    }
    env.poll_first_task_of_each(&child_ids).await;
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_registered_parent_commits_after_its_children_succeed() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let parent_id = env
        .start_root(&JobShape {
            with_commit: true,
            ..JobShape::ONE_TASK
        })
        .await;
    let parent_value = value(0x20, 0);
    let child_shapes: Vec<_> = (1..=3)
        .map(|num_tasks| JobShape {
            num_tasks,
            ..JobShape::ONE_TASK
        })
        .collect();

    let task_instance_id = env.create_instance(parent_id, TaskId::Index(0)).await;
    assert_eq!(
        env.task_success(
            parent_id,
            0,
            task_instance_id,
            &parent_value,
            child_shapes.iter().map(JobShape::child).collect()
        )
        .await?,
        JobState::BlockingOnChildren
    );
    assert_eq!(
        env.row_outputs(parent_id).await,
        Some(task_outputs([&parent_value]))
    );
    let child_ids: Vec<JobId> = env
        .child_rows(parent_id)
        .await
        .into_iter()
        .map(|row| row.id)
        .collect();
    let children: Vec<_> = child_ids.iter().copied().zip(&child_shapes).collect();
    env.drain_ready_tasks(&children).await;

    for (position, (child_id, shape)) in children.iter().enumerate() {
        env.run_all_tasks(*child_id, shape).await;
        let parent_state = if position + 1 == children.len() {
            JobState::CommitReady
        } else {
            JobState::BlockingOnChildren
        };
        env.assert_states(
            &HashMap::from([(*child_id, JobState::Succeeded), (parent_id, parent_state)]),
            &format!("after child {position}"),
        )
        .await;
    }
    let (commit_instance_id, commit_inputs) = env.commit_instance(parent_id).await;
    assert_eq!(commit_inputs, task_outputs([&parent_value]));
    assert_eq!(
        env.commit_success(parent_id, commit_instance_id).await?,
        JobState::Succeeded
    );
    env.assert_states(
        &HashMap::from([(parent_id, JobState::Succeeded)]),
        "after the commit",
    )
    .await;
    assert_eq!(
        env.service.get_job_outputs(parent_id).await?,
        task_outputs([&parent_value])
    );
    env.assert_lanes_empty().await;
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn sibling_tasks_register_the_union_of_their_children_in_task_order() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let parent_id = env
        .start_root(&JobShape {
            num_tasks: 2,
            ..JobShape::ONE_TASK
        })
        .await;
    let first_task_instance_id = env.create_instance(parent_id, TaskId::Index(0)).await;
    let second_task_instance_id = env.create_instance(parent_id, TaskId::Index(1)).await;
    let task_values = [value(0x30, 0), value(0x30, 1)];
    let child_shapes: Vec<_> = (1..=3)
        .map(|num_tasks| JobShape {
            num_tasks,
            ..JobShape::ONE_TASK
        })
        .collect();

    assert_eq!(
        env.task_success(
            parent_id,
            1,
            second_task_instance_id,
            &task_values[1],
            vec![child_shapes[2].child()]
        )
        .await?,
        JobState::Running
    );
    assert_eq!(env.child_rows(parent_id).await, Vec::new());
    assert_eq!(
        env.task_success(
            parent_id,
            0,
            first_task_instance_id,
            &task_values[0],
            child_shapes[..2].iter().map(JobShape::child).collect()
        )
        .await?,
        JobState::BlockingOnChildren
    );
    let rows = env.child_rows(parent_id).await;
    assert_eq!(
        rows.iter()
            .map(|row| row.compressed_serialized_task_graph.clone())
            .collect::<Vec<_>>(),
        child_shapes
            .iter()
            .map(|shape| shape.child().compressed_serialized_task_graph)
            .collect::<Vec<_>>()
    );
    let child_ids: Vec<JobId> = rows.iter().map(|row| row.id).collect();
    let children: Vec<_> = child_ids.iter().copied().zip(&child_shapes).collect();
    env.drain_ready_tasks(&children).await;
    for (child_id, shape) in &children {
        env.run_all_tasks(*child_id, shape).await;
    }
    let mut expected_states = HashMap::from([(parent_id, JobState::Succeeded)]);
    expected_states.extend(
        child_ids
            .iter()
            .map(|child_id| (*child_id, JobState::Succeeded)),
    );
    env.assert_states(&expected_states, "after the children")
        .await;
    assert_eq!(
        env.service.get_job_outputs(parent_id).await?,
        task_outputs(&task_values)
    );
    env.assert_lanes_empty().await;
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_report_over_a_limit_fails_its_task() -> anyhow::Result<()> {
    struct Case {
        output_size: usize,
        children: fn() -> Vec<CompressedJobSubmission>,
        error_prefix: String,
    }

    let output_limit = usize::try_from(MAX_REPORT_OUTPUTS_SIZE)?;
    let cases = [
        Case {
            output_size: 4,
            children: || {
                iter::repeat_with(|| chain_child(1))
                    .take(MAX_CHILDREN_PER_JOB + 1)
                    .collect()
            },
            error_prefix: ReportRejection::JobChildrenTooLarge {
                count: MAX_CHILDREN_PER_JOB + 1,
                size: (MAX_CHILDREN_PER_JOB + 1) as u64 * chain_child(1).size(),
                count_limit: MAX_CHILDREN_PER_JOB,
                size_limit: MAX_JOB_CHILDREN_SIZE,
            }
            .to_string(),
        },
        Case {
            output_size: 4,
            children: || vec![chain_child(1), undecodable_child(4 * MIB + 1)],
            error_prefix: format!("child job 1 of {} bytes exceeds the limit of ", 4 * MIB + 1),
        },
        Case {
            output_size: 4,
            children: || {
                iter::repeat_with(|| undecodable_child(3 * MIB + MIB / 2))
                    .take(5)
                    .collect()
            },
            error_prefix: ReportRejection::ChildrenTooLarge {
                size: 5 * (3 * MIB + MIB / 2) as u64,
                limit: MAX_REPORT_CHILDREN_SIZE,
            }
            .to_string(),
        },
        Case {
            output_size: 4,
            children: || vec![undecodable_child(16)],
            error_prefix: "child job 0 is invalid: ".to_owned(),
        },
        Case {
            output_size: output_limit + 1,
            children: Vec::new,
            error_prefix: "task outputs of ".to_owned(),
        },
    ];

    let env = TestEnv::start().await;
    for case in cases {
        for max_num_retry in [0, 1] {
            let (graph, inputs) = build_chain_task_graph(
                1,
                ExecutionPolicy {
                    max_num_retry,
                    ..ExecutionPolicy::default()
                },
            );
            let job_id = env.start_job(&graph, &inputs).await;
            for attempt in 0..=max_num_retry {
                if attempt > 0 {
                    assert_eq!(env.poll_ready_tasks(job_id, 1).await, vec![0]);
                }
                let expected_state = if attempt == max_num_retry {
                    JobState::Failed
                } else {
                    JobState::Running
                };
                let task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
                assert_eq!(
                    env.task_success(
                        job_id,
                        0,
                        task_instance_id,
                        &vec![0u8; case.output_size],
                        (case.children)()
                    )
                    .await?,
                    expected_state,
                    "case `{}`, attempt {attempt}",
                    case.error_prefix
                );
            }
            env.assert_job_state(job_id, JobState::Failed).await;
            let error = env.service.get_job_error(job_id).await?;
            assert!(
                error.starts_with(&case.error_prefix),
                "case `{}`: unexpected error `{error}`",
                case.error_prefix
            );
            assert_eq!(env.child_rows(job_id).await, Vec::new());
        }
    }
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn children_over_the_job_limits_across_reports_fail_the_task_before_decoding()
-> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let job_id = env
        .start_independent_job(2, ExecutionPolicy::default(), false)
        .await;
    let first_task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    let second_task_instance_id = env.create_instance(job_id, TaskId::Index(1)).await;

    let stored_children: Vec<_> = iter::repeat_with(|| large_child(2 * MIB)).take(2).collect();
    let stored_bytes: u64 = stored_children
        .iter()
        .map(CompressedJobSubmission::size)
        .sum();
    assert_eq!(
        env.task_success(
            job_id,
            0,
            first_task_instance_id,
            &[0u8; 4],
            stored_children
        )
        .await?,
        JobState::Running
    );

    let undecodable_children: Vec<_> = iter::repeat_with(|| undecodable_child(3 * MIB + MIB / 2))
        .take(4)
        .collect();
    let undecodable_bytes: u64 = undecodable_children
        .iter()
        .map(CompressedJobSubmission::size)
        .sum();
    assert_eq!(
        env.task_success(
            job_id,
            1,
            second_task_instance_id,
            &[0u8; 4],
            undecodable_children
        )
        .await?,
        JobState::Failed
    );
    env.assert_job_state(job_id, JobState::Failed).await;
    assert_eq!(
        env.service.get_job_error(job_id).await?,
        ReportRejection::JobChildrenTooLarge {
            count: 6,
            size: stored_bytes + undecodable_bytes,
            count_limit: MAX_CHILDREN_PER_JOB,
            size_limit: MAX_JOB_CHILDREN_SIZE,
        }
        .to_string()
    );
    assert_eq!(env.child_rows(job_id).await, Vec::new());
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn rejected_reports_revert_their_children_usage_reservation() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let job_id = env
        .start_independent_job(
            2,
            ExecutionPolicy {
                max_num_retry: 1,
                ..ExecutionPolicy::default()
            },
            false,
        )
        .await;
    let child_size = large_child(2 * MIB).size();
    let max_children = usize::try_from(MAX_JOB_CHILDREN_SIZE / child_size)?;
    assert!(
        max_children >= 3,
        "a child of {child_size} bytes is too large for the test"
    );
    let large_children =
        |num_children: usize| iter::repeat_with(|| large_child(2 * MIB)).take(num_children);

    let invalid_task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    assert_eq!(
        env.task_success(
            job_id,
            0,
            invalid_task_instance_id,
            &[0u8; 4],
            large_children(2)
                .chain(iter::once(undecodable_child(16)))
                .collect()
        )
        .await?,
        JobState::Running
    );
    assert_eq!(env.poll_ready_tasks(job_id, 1).await, vec![0]);

    let first_task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    assert_eq!(
        env.task_success(
            job_id,
            0,
            first_task_instance_id,
            &[0u8; 4],
            large_children(2).collect()
        )
        .await?,
        JobState::Running
    );
    let duplicate_result = env
        .task_success(
            job_id,
            0,
            first_task_instance_id,
            &[0u8; 4],
            large_children(2).collect(),
        )
        .await;
    assert!(
        matches!(
            duplicate_result,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::InvalidTaskInstanceId
            )))
        ),
        "a duplicate report should be stale, got {duplicate_result:?}"
    );

    let second_task_instance_id = env.create_instance(job_id, TaskId::Index(1)).await;
    assert_eq!(
        env.task_success(
            job_id,
            1,
            second_task_instance_id,
            &[0u8; 4],
            large_children(max_children - 2).collect()
        )
        .await?,
        JobState::BlockingOnChildren
    );
    env.assert_job_state(job_id, JobState::BlockingOnChildren)
        .await;
    assert_eq!(env.child_rows(job_id).await.len(), max_children);
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_report_at_the_maximum_depth_with_children_fails_its_task() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let mut job_id = env.start_chain_job(1, 1).await;
    for _ in 0..MAX_DEPTH {
        let task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
        assert_eq!(
            env.task_success(job_id, 0, task_instance_id, &[0u8; 4], vec![chain_child(1)])
                .await?,
            JobState::BlockingOnChildren
        );
        let rows = env.child_rows(job_id).await;
        assert_eq!(rows.len(), 1);
        job_id = rows[0].id;
        env.poll_first_task_of_each(&[job_id]).await;
    }

    let task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    assert_eq!(
        env.task_success(job_id, 0, task_instance_id, &[0u8; 4], vec![chain_child(1)])
            .await?,
        JobState::Failed
    );
    env.assert_job_state(job_id, JobState::Failed).await;
    assert_eq!(
        env.service.get_job_error(job_id).await?,
        ReportRejection::TooDeep {
            depth: MAX_DEPTH + 1,
            limit: MAX_DEPTH,
        }
        .to_string()
    );
    assert_eq!(env.child_rows(job_id).await, Vec::new());
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn external_job_rpcs_reject_a_child_except_for_its_state_and_error() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let root_id = env.start_chain_job(1, 1).await;
    let task_instance_id = env.create_instance(root_id, TaskId::Index(0)).await;
    let registration_write = env.hooks.arm(
        HookPoint::CommitterRoundAfterDbWrite,
        HookFilter::job(root_id),
        HookAction::Pause,
        HookCount::Once,
    );
    let (report, child_id) = tokio::join!(
        env.task_success(
            root_id,
            0,
            task_instance_id,
            &[1u8; 4],
            vec![chain_child(1)]
        ),
        async {
            registration_write
                .reached(1, HOOK_TIMEOUT)
                .await
                .expect("the registration should reach the committer's hook");
            let rows = env.child_rows(root_id).await;
            let [ChildRow { id: child_id, .. }] = rows[..] else {
                panic!("the registration should insert one child, got {rows:?}");
            };
            env.assert_not_cached(child_id, "the child before its registration is applied")
                .await;
            env.assert_job_rpcs_answer_child(child_id, JobState::Running, "an uncached child")
                .await;
            assert!(!env.is_fatal());
            registration_write.release();
            child_id
        }
    );
    assert_eq!(report?, JobState::BlockingOnChildren);

    env.assert_job_rpcs_answer_child(child_id, JobState::Running, "a cached child")
        .await;
    env.assert_job_state(root_id, JobState::BlockingOnChildren)
        .await;
    assert_eq!(env.row_state(child_id).await, JobState::Running);
    assert_eq!(
        env.run_task(child_id, 0, &[2u8; 4]).await,
        JobState::Succeeded
    );
    env.assert_job_rpcs_answer_child(child_id, JobState::Succeeded, "a succeeded child")
        .await;

    env.assert_job_rpcs_answer_unknown_job(JobId::from(u64::MAX))
        .await;
    assert!(!env.is_fatal());
    env.finish().await;
    Ok(())
}

/// # Returns
///
/// A child job of [`build_chain_task_graph`] of `num_tasks` tasks under the default execution
/// policy.
///
/// # Panics
///
/// Panics if the job fails to compress.
fn chain_child(num_tasks: usize) -> CompressedJobSubmission {
    let (graph, inputs) = build_chain_task_graph(num_tasks, ExecutionPolicy::default());
    CompressedJobSubmission {
        compressed_serialized_task_graph: compress_task_graph(&graph)
            .expect("task graph compression should succeed"),
        compressed_serialized_job_inputs: compress_job_inputs(&inputs)
            .expect("input compression should succeed"),
    }
}

/// # Returns
///
/// A valid one-task child job whose only input is `input_size` incompressible bytes.
///
/// # Panics
///
/// Panics if the job fails to build or to compress.
fn large_child(input_size: usize) -> CompressedJobSubmission {
    let (graph, _) = build_chain_task_graph(1, ExecutionPolicy::default());
    let mut inputs = TaskGraphInputBuilder::new();
    inputs
        .append_task_input(&incompressible(input_size))
        .expect("task input appending should succeed");
    CompressedJobSubmission {
        compressed_serialized_task_graph: compress_task_graph(&graph)
            .expect("task graph compression should succeed"),
        compressed_serialized_job_inputs: compress_job_inputs(&inputs.build())
            .expect("input compression should succeed"),
    }
}

/// # Returns
///
/// A child job of `size` bytes that fails to decode.
fn undecodable_child(size: usize) -> CompressedJobSubmission {
    CompressedJobSubmission {
        compressed_serialized_task_graph: vec![0u8; size],
        compressed_serialized_job_inputs: Vec::new(),
    }
}

/// # Returns
///
/// `size` pseudo-random bytes.
fn incompressible(size: usize) -> Vec<u8> {
    let mut xorshift_state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..size)
        .map(|_| {
            xorshift_state ^= xorshift_state << 13;
            xorshift_state ^= xorshift_state >> 7;
            xorshift_state ^= xorshift_state << 17;
            xorshift_state.to_le_bytes()[0]
        })
        .collect()
}

/// Asserts that `result` is a [`StorageServerError::BadRequest`] of `expected_message`.
///
/// # Type Parameters
///
/// * `ValueType` - The type of `result`'s success value.
///
/// # Panics
///
/// Panics if the assertion fails.
fn assert_bad_request<ValueType: std::fmt::Debug>(
    result: &Result<ValueType, StorageServerError>,
    expected_message: &str,
    what: &str,
) {
    assert!(
        matches!(
            result,
            Err(StorageServerError::BadRequest(message)) if message == expected_message
        ),
        "{what}: expected a bad request of {expected_message:?}, got {result:?}"
    );
}
