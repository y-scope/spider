//! Child-job registration through the storage service on `MariaDB`.

use std::iter;

use spider_core::job::JobState;
use spider_core::task::ExecutionPolicy;
use spider_core::types::id::JobId;
use spider_core::types::id::TaskId;
use spider_core::types::io::TaskGraphInputBuilder;
use spider_storage::cache::error::CacheError;
use spider_storage::cache::error::StaleStateError;
use spider_storage::job_submission::ChildJobSubmission;
use spider_storage::limits::MAX_CHILDREN_PER_JOB;
use spider_storage::limits::MAX_DEPTH;
use spider_storage::limits::MAX_JOB_CHILD_BYTES;
use spider_storage::limits::MAX_REPORT_CHILD_BYTES;
use spider_storage::limits::MAX_REPORT_OUTPUT_BYTES;
use spider_storage::state::StorageServerError;
use spider_storage::state::error::ReportRejection;

use crate::runtime_infra::ChildRow;
use crate::runtime_infra::TestEnv;
use crate::runtime_infra::build_chain_task_graph;
use crate::task_graph_builder::compress_job_inputs;
use crate::task_graph_builder::compress_task_graph;

const MIB: usize = 1024 * 1024;

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_report_with_children_blocks_its_job_and_inserts_them() -> anyhow::Result<()> {
    let env = TestEnv::start().await;
    let parent_id = env.start_chain_job(1, 1).await;
    let children: Vec<_> = (1..=3).map(chain_child).collect();
    let task_graphs: Vec<_> = children
        .iter()
        .map(|child| child.compressed_serialized_task_graph.clone())
        .collect();

    let task_instance_id = env.create_instance(parent_id, TaskId::Index(0)).await;
    assert_eq!(
        env.report_success(parent_id, 0, task_instance_id, vec![1u8; 4], children)
            .await?,
        JobState::BlockingOnChildren
    );
    env.assert_job_state(parent_id, JobState::BlockingOnChildren)
        .await;
    assert!(env.has_outputs(parent_id).await);

    let rows = env.child_rows(parent_id).await;
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
    for child_id in &child_ids {
        env.assert_job_state(*child_id, JobState::Running).await;
    }
    env.poll_first_task_of_each(&child_ids).await;
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_report_over_a_limit_fails_its_task() -> anyhow::Result<()> {
    struct Case {
        output_size: usize,
        children: fn() -> Vec<ChildJobSubmission>,
        error_prefix: String,
    }

    let output_limit = usize::try_from(MAX_REPORT_OUTPUT_BYTES)?;
    let cases = [
        Case {
            output_size: 4,
            children: || {
                iter::repeat_with(|| chain_child(1))
                    .take(MAX_CHILDREN_PER_JOB + 1)
                    .collect()
            },
            error_prefix: ReportRejection::TooManyChildren {
                count: MAX_CHILDREN_PER_JOB + 1,
                limit: MAX_CHILDREN_PER_JOB,
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
                limit: MAX_REPORT_CHILD_BYTES,
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
                    env.report_success(
                        job_id,
                        0,
                        task_instance_id,
                        vec![0u8; case.output_size],
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
    let stored_bytes: u64 = stored_children.iter().map(ChildJobSubmission::size).sum();
    assert_eq!(
        env.report_success(
            job_id,
            0,
            first_task_instance_id,
            vec![0u8; 4],
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
        .map(ChildJobSubmission::size)
        .sum();
    assert_eq!(
        env.report_success(
            job_id,
            1,
            second_task_instance_id,
            vec![0u8; 4],
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
            bytes: stored_bytes + undecodable_bytes,
            count_limit: MAX_CHILDREN_PER_JOB,
            bytes_limit: MAX_JOB_CHILD_BYTES,
        }
        .to_string()
    );
    assert_eq!(env.child_rows(job_id).await, Vec::new());
    env.finish().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn rejected_reports_release_their_children_from_the_job_limits() -> anyhow::Result<()> {
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
    let max_children = usize::try_from(MAX_JOB_CHILD_BYTES / child_size)?;
    assert!(
        max_children >= 3,
        "a child of {child_size} bytes is too large for the test"
    );
    let large_children =
        |num_children: usize| iter::repeat_with(|| large_child(2 * MIB)).take(num_children);

    let invalid_task_instance_id = env.create_instance(job_id, TaskId::Index(0)).await;
    assert_eq!(
        env.report_success(
            job_id,
            0,
            invalid_task_instance_id,
            vec![0u8; 4],
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
        env.report_success(
            job_id,
            0,
            first_task_instance_id,
            vec![0u8; 4],
            large_children(2).collect()
        )
        .await?,
        JobState::Running
    );
    let duplicate_result = env
        .report_success(
            job_id,
            0,
            first_task_instance_id,
            vec![0u8; 4],
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
        env.report_success(
            job_id,
            1,
            second_task_instance_id,
            vec![0u8; 4],
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
            env.report_success(
                job_id,
                0,
                task_instance_id,
                vec![0u8; 4],
                vec![chain_child(1)]
            )
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
        env.report_success(
            job_id,
            0,
            task_instance_id,
            vec![0u8; 4],
            vec![chain_child(1)]
        )
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

/// # Returns
///
/// A child job of [`build_chain_task_graph`] of `num_tasks` tasks under the default execution
/// policy.
///
/// # Panics
///
/// Panics if the job fails to compress.
fn chain_child(num_tasks: usize) -> ChildJobSubmission {
    let (graph, inputs) = build_chain_task_graph(num_tasks, ExecutionPolicy::default());
    ChildJobSubmission {
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
fn large_child(input_size: usize) -> ChildJobSubmission {
    let (graph, _) = build_chain_task_graph(1, ExecutionPolicy::default());
    let mut inputs = TaskGraphInputBuilder::new();
    inputs
        .append_task_input(&incompressible(input_size))
        .expect("task input appending should succeed");
    ChildJobSubmission {
        compressed_serialized_task_graph: compress_task_graph(&graph)
            .expect("task graph compression should succeed"),
        compressed_serialized_job_inputs: compress_job_inputs(&inputs.build())
            .expect("input compression should succeed"),
    }
}

/// # Returns
///
/// A child job of `size` bytes that fails to decode.
fn undecodable_child(size: usize) -> ChildJobSubmission {
    ChildJobSubmission {
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
