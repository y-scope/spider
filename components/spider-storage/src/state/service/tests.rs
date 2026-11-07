//! Unit tests for the storage service state.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

use spider_core::compression::encode_zstd_bytes;
use spider_core::job::JobState;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TaskGraph as SubmittedTaskGraph;
use spider_core::task::TaskIndex;
use spider_core::task::TdlContext;
use spider_core::task::TerminationTaskDescriptor;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::TaskGraphInputBuilder;
use spider_core::types::io::TaskOutput;
use spider_core::types::io::TaskOutputsSerializer;
use spider_core::types::resource_group::ExternalResourceGroupCredentials;
use spider_utils::config::Host;
use spider_utils::shutdown::ShutdownHandle;
use tokio::sync::mpsc::error::TryRecvError;

use crate::cache::error::CacheError;
use crate::cache::error::InternalError;
use crate::cache::error::StaleStateError;
use crate::cache::job::SharedJobControlBlock;
use crate::cache::tree::TreeControlBlock;
use crate::db::DbError;
use crate::db::RecoverableJobContext;
use crate::inbound_queue::CleanupTaskMarker;
use crate::inbound_queue::CommitTaskMarker;
use crate::inbound_queue::InboundQueueSender;
use crate::inbound_queue::InboundQueueSenderHandle;
use crate::job_submission::compress_job_inputs;
use crate::job_submission::compress_task_graph;
use crate::job_submission::create_validated_submission;
use crate::state::JobCache;
use crate::state::JobCacheGcHandle;
use crate::state::StorageServerError;
use crate::state::service::ServiceState;
use crate::state::service::ServiceStateParams;
use crate::state::service::test_utils::TEST_SESSION_ID;
use crate::state::service::test_utils::TestServiceState;
use crate::state::service::test_utils::create_inbound_queue_receiver;
use crate::state::service::test_utils::create_test_job_submission;
use crate::state::service::test_utils::create_test_serialized_outputs;
use crate::state::service::test_utils::create_test_service;
use crate::state::service::test_utils::create_test_service_with_db;
use crate::state::service::test_utils::create_test_service_with_db_and_session;
use crate::state::test_utils::MOCK_MAX_OUTPUTS_SIZE;
use crate::state::test_utils::MockDbConnector;
use crate::state::test_utils::MockInboundQueueSender;
use crate::state::test_utils::MockTaskInstancePoolConnector;
use crate::state::test_utils::create_test_jcb;
use crate::state::test_utils::create_test_task_graph;
use crate::state::test_utils::create_test_task_graph_input;
use crate::state::test_utils::create_test_task_graph_with_termination_tasks;
use crate::test_hooks::TestHooks;

type TestServiceStateWithInboundQueue =
    ServiceState<InboundQueueSenderHandle, MockDbConnector, MockTaskInstancePoolConnector>;

/// Creates a [`ServiceState`] backed by [`crate::inbound_queue::InboundQueueReceiverHandle`].
///
/// # Returns
///
/// A tuple of the service state and the inbound queue sender handle on success.
fn create_test_service_with_inbound_queue(
    db: MockDbConnector,
) -> (TestServiceStateWithInboundQueue, InboundQueueSenderHandle) {
    use crate::inbound_queue::create_inbound_queue;
    let (sender, receiver) = create_inbound_queue();
    let service = TestServiceStateWithInboundQueue::new(ServiceStateParams {
        db,
        session_id: 0,
        job_cache: JobCache::new(),
        inbound_queue_sender: sender.clone(),
        inbound_queue_receiver: receiver,
        task_instance_pool_connector: MockTaskInstancePoolConnector,
        job_cache_gc_handle: JobCacheGcHandle::new(tokio::sync::mpsc::unbounded_channel().0),
        shutdown_handle: ShutdownHandle::new(),
        hooks: TestHooks::default(),
    });
    (service, sender)
}

fn create_empty_compressed_serialized_task_graph_input() -> Vec<u8> {
    compress_job_inputs(&TaskGraphInputBuilder::new().build())
}

#[tokio::test]
async fn register_job_returns_job_id_and_inserts_into_cache() -> anyhow::Result<()> {
    let service = create_test_service();
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;
    assert!(
        service.inner.job_cache.get(job_id).await.is_some(),
        "JCB should be in cache after register_job"
    );
    Ok(())
}

#[tokio::test]
async fn register_job_returns_error_on_invalid_task_graph() -> anyhow::Result<()> {
    let service = create_test_service();
    let result = service
        .register_job(
            ResourceGroupId::random(),
            encode_zstd_bytes(b"invalid json")
                .expect("invalid task graph compression should succeed"),
            create_empty_compressed_serialized_task_graph_input(),
        )
        .await;
    assert!(
        matches!(result, Err(StorageServerError::BadRequest(_))),
        "register_job should return a bad-request error on invalid task graph JSON"
    );
    Ok(())
}

#[tokio::test]
async fn register_job_returns_error_on_input_size_mismatch() -> anyhow::Result<()> {
    let service = create_test_service();
    let task_graph = create_test_task_graph()
        .to_json()
        .expect("task graph serialization should succeed");
    let result = service
        .register_job(
            ResourceGroupId::random(),
            encode_zstd_bytes(task_graph.as_bytes())
                .expect("task graph compression should succeed"),
            create_empty_compressed_serialized_task_graph_input(),
        )
        .await;
    assert!(
        matches!(result, Err(StorageServerError::BadRequest(_))),
        "register_job should return a bad-request error on input size mismatch"
    );
    Ok(())
}

#[tokio::test]
async fn register_job_returns_error_on_empty_task_graph() -> anyhow::Result<()> {
    let service = create_test_service();
    let task_graph = SubmittedTaskGraph::new(None, None)
        .expect("empty task graph creation should succeed")
        .to_json()
        .expect("task graph serialization should succeed");
    let result = service
        .register_job(
            ResourceGroupId::random(),
            encode_zstd_bytes(task_graph.as_bytes())
                .expect("task graph compression should succeed"),
            create_empty_compressed_serialized_task_graph_input(),
        )
        .await;
    assert!(
        matches!(result, Err(StorageServerError::BadRequest(_))),
        "register_job should return a bad-request error on empty task graph"
    );
    Ok(())
}

#[tokio::test]
async fn start_job_starts_cached_job() -> anyhow::Result<()> {
    let service = create_test_service();
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;

    service.start_job(job_id).await?;

    let state = service.get_job_state(job_id).await?;
    assert_eq!(state, JobState::Running);
    Ok(())
}

#[tokio::test]
async fn start_job_returns_job_not_found_if_not_exist() -> anyhow::Result<()> {
    let service = create_test_service();
    let job_id = JobId::random();
    let result = service.start_job(job_id).await;
    assert!(
        matches!(result, Err(StorageServerError::Db(DbError::JobNotFound(id))) if id == job_id),
        "unexpected result: {result:?}"
    );
    Ok(())
}

#[tokio::test]
async fn start_job_rejects_terminal_jobs_not_in_cache() -> anyhow::Result<()> {
    for state in [JobState::Succeeded, JobState::Failed, JobState::Cancelled] {
        let db = MockDbConnector::default();
        let job_id = JobId::random();
        db.states.insert(job_id, state);
        let service = create_test_service_with_db(db);

        let result = service.start_job(job_id).await;
        assert!(
            matches!(
                result,
                Err(StorageServerError::Cache(CacheError::StaleState(
                    StaleStateError::JobAlreadyTerminated(actual)
                ))) if actual == state
            ),
            "unexpected result for {state:?}: {result:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn start_job_of_a_started_or_cancelled_job_is_stale_and_not_fatal() -> anyhow::Result<()> {
    let service = create_test_service();
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let started_job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;
    service.start_job(started_job_id).await?;
    service
        .create_task_instance(
            TEST_SESSION_ID,
            started_job_id,
            TaskId::Index(0),
            ExecutionManagerId::random(),
        )
        .await?;

    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let cancelled_job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;
    service.cancel_job(cancelled_job_id).await?;

    for (job_id, state) in [
        (started_job_id, JobState::Running),
        (cancelled_job_id, JobState::Cancelled),
    ] {
        let result = service.start_job(job_id).await;
        assert!(
            matches!(
                result,
                Err(StorageServerError::Cache(CacheError::StaleState(
                    StaleStateError::JobAlreadyStarted
                )))
            ),
            "a start of a {state} job gave {result:?}"
        );
        assert_eq!(service.get_job_state(job_id).await?, state);
    }
    assert!(!service.inner.shutdown_handle.is_fatal());
    Ok(())
}

#[tokio::test]
async fn cancel_job_returns_job_not_found_if_not_exist() -> anyhow::Result<()> {
    let service = create_test_service();
    let result = service.cancel_job(JobId::random()).await;
    assert!(
        matches!(result, Err(StorageServerError::JobNotFound(_))),
        "cancel_job should return JobNotFound when job is not in cache or DB"
    );
    Ok(())
}

#[tokio::test]
async fn cancel_job_returns_terminal_state_from_db_when_not_in_cache() -> anyhow::Result<()> {
    let db = MockDbConnector::default();
    let job_id = JobId::random();
    db.states.insert(job_id, JobState::Cancelled);

    let service = create_test_service_with_db(db);
    let state = service.cancel_job(job_id).await?;
    assert_eq!(
        state,
        JobState::Cancelled,
        "cancel_job should return Cancelled when job is already cancelled in DB"
    );
    Ok(())
}

#[tokio::test]
async fn cancel_job_transitions_to_terminal_state() -> anyhow::Result<()> {
    let db = MockDbConnector::default();
    let service = create_test_service_with_db(db.clone());
    let job_id = JobId::random();
    db.states.insert(job_id, JobState::Ready);
    let (jcb, receivers) = create_test_jcb(job_id).await;
    service.insert_tree(jcb, receivers).await?;

    let state = service.cancel_job(job_id).await?;
    assert!(
        matches!(state, JobState::Cancelled),
        "cancel should result in terminal state"
    );
    assert!(
        service.inner.job_cache.get(job_id).await.is_some(),
        "JCB should remain in cache after terminal cancel"
    );
    Ok(())
}

#[tokio::test]
async fn cancel_job_of_a_ready_job_with_a_cleanup_task_cancels_it_without_its_cleanup()
-> anyhow::Result<()> {
    let db = MockDbConnector::default();
    let (service, _sender) = create_test_service_with_inbound_queue(db.clone());
    let task_graph = create_test_task_graph_with_termination_tasks(
        None,
        Some(TerminationTaskDescriptor {
            tdl_context: TdlContext {
                package: "test_pkg".to_owned(),
                task_func: "cleanup_fn".to_owned(),
            },
            execution_policy: Some(ExecutionPolicy::default()),
        }),
    );
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compress_task_graph(&task_graph),
            compress_job_inputs(&create_test_task_graph_input()),
        )
        .await?;

    assert_eq!(service.cancel_job(job_id).await?, JobState::Cancelled);
    assert_eq!(
        db.states.get(&job_id).map(|state| *state),
        Some(JobState::Cancelled)
    );
    assert_eq!(
        service
            .poll_cleanup_ready_tasks(10, Duration::from_millis(100))
            .await?,
        []
    );
    assert!(!service.inner.shutdown_handle.is_fatal());
    Ok(())
}

#[tokio::test]
async fn cancel_job_of_a_cancelled_cached_job_is_already_cancelled() -> anyhow::Result<()> {
    let service = create_test_service();
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;
    assert_eq!(service.cancel_job(job_id).await?, JobState::Cancelled);

    let result = service.cancel_job(job_id).await;
    assert!(
        matches!(
            result,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::JobAlreadyCancelled
            )))
        ),
        "a second cancel gave {result:?}"
    );
    assert!(!service.inner.shutdown_handle.is_fatal());
    Ok(())
}

#[tokio::test]
async fn cancel_job_enqueues_cleanup_task_of_recovered_commit_ready_job() -> anyhow::Result<()> {
    let db = MockDbConnector::default();
    let (service, sender) = create_test_service_with_inbound_queue(db.clone());

    let create_termination_task_descriptor = |task_func: &str| TerminationTaskDescriptor {
        tdl_context: TdlContext {
            package: "test_pkg".to_owned(),
            task_func: task_func.to_owned(),
        },
        execution_policy: Some(ExecutionPolicy::default()),
    };
    let task_graph = create_test_task_graph_with_termination_tasks(
        Some(create_termination_task_descriptor("commit_fn")),
        Some(create_termination_task_descriptor("cleanup_fn")),
    );

    let job_id = JobId::random();
    db.states.insert(job_id, JobState::CommitReady);
    let (tree, receivers) = TreeControlBlock::new(ShutdownHandle::new());
    let jcb = SharedJobControlBlock::recover(
        RecoverableJobContext {
            id: job_id,
            resource_group_id: ResourceGroupId::random(),
            state: JobState::CommitReady,
            submission: create_validated_submission(task_graph, create_test_task_graph_input()),
            outputs: Some(TaskOutputsSerializer::deserialize(
                &create_test_serialized_outputs(),
            )?),
        },
        sender,
        MockTaskInstancePoolConnector,
        TestHooks::default(),
        tree,
    )
    .await?;
    service.insert_tree(jcb, receivers).await?;

    let state = service.cancel_job(job_id).await?;
    assert_eq!(state, JobState::CleanupReady);

    let entries = service
        .poll_cleanup_ready_tasks(10, Duration::from_millis(100))
        .await?;
    assert_eq!(entries.len(), 1, "should receive one cleanup-ready entry");
    assert_eq!(entries[0].job_id, job_id);
    assert_eq!(entries[0].task_kind, CleanupTaskMarker);

    let context = service
        .create_task_instance(
            TEST_SESSION_ID,
            job_id,
            TaskId::Cleanup,
            ExecutionManagerId::random(),
        )
        .await?;
    let state = service
        .succeed_cleanup_task_instance(TEST_SESSION_ID, job_id, context.task_instance_id)
        .await?;
    assert_eq!(state, JobState::Cancelled);
    Ok(())
}

#[tokio::test]
async fn get_job_state_serves_from_cache_when_jcb_present() -> anyhow::Result<()> {
    let service = create_test_service();
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;

    let state = service.get_job_state(job_id).await?;
    assert_eq!(state, JobState::Ready);
    Ok(())
}

#[tokio::test]
async fn get_job_state_falls_back_to_db_when_not_in_cache() -> anyhow::Result<()> {
    let db = MockDbConnector::default();
    let job_id = JobId::random();
    db.states.insert(job_id, JobState::Failed);

    let service = create_test_service_with_db(db);
    let state = service.get_job_state(job_id).await?;
    assert_eq!(state, JobState::Failed);
    Ok(())
}

#[tokio::test]
async fn get_job_state_returns_error_for_unknown_job() -> anyhow::Result<()> {
    let service = create_test_service();
    let result = service.get_job_state(JobId::random()).await;
    assert!(result.is_err(), "get_job_state should fail for unknown job");
    Ok(())
}

#[tokio::test]
async fn get_job_outputs_returns_outputs_from_db() -> anyhow::Result<()> {
    let db = MockDbConnector::default();
    let job_id = JobId::random();
    let outputs: Vec<TaskOutput> = vec![vec![1, 2, 3]];
    db.outputs.insert(job_id, outputs.clone());

    let service = create_test_service_with_db(db);
    let result = service.get_job_outputs(job_id).await?;
    assert_eq!(result, outputs);
    Ok(())
}

#[tokio::test]
async fn get_job_outputs_returns_outputs_from_cache_when_jcb_present() -> anyhow::Result<()> {
    let service = create_test_service();
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;
    service.start_job(job_id).await?;

    let context = service
        .create_task_instance(
            TEST_SESSION_ID,
            job_id,
            TaskId::Index(0),
            ExecutionManagerId::random(),
        )
        .await?;
    let serialized_outputs = create_test_serialized_outputs();
    let expected = TaskOutputsSerializer::deserialize(&serialized_outputs)
        .expect("test serialized outputs should deserialize successfully");
    service
        .succeed_task_instance(
            TEST_SESSION_ID,
            job_id,
            context.task_instance_id,
            0,
            serialized_outputs,
        )
        .await?;

    let actual = service.get_job_outputs(job_id).await?;
    assert_eq!(actual, expected);
    Ok(())
}

#[tokio::test]
async fn get_job_outputs_returns_error_for_unknown_job() -> anyhow::Result<()> {
    let service = create_test_service();
    let result = service.get_job_outputs(JobId::random()).await;
    assert!(
        result.is_err(),
        "get_job_outputs should fail for unknown job"
    );
    Ok(())
}

#[tokio::test]
async fn get_job_outputs_returns_error_when_job_not_succeeded() -> anyhow::Result<()> {
    let service = create_test_service();
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;
    // JCB is in cache but job is still Ready (not Succeeded).
    let result = service.get_job_outputs(job_id).await;
    assert!(
        matches!(
            result,
            Err(StorageServerError::Cache(CacheError::StaleState(
                StaleStateError::JobNotSucceeded(JobState::Ready)
            )))
        ),
        "get_job_outputs should fail with a stale-state error when job is not Succeeded"
    );
    Ok(())
}

#[tokio::test]
async fn get_job_error_returns_error_message_from_db() -> anyhow::Result<()> {
    let db = MockDbConnector::default();
    let job_id = JobId::random();
    let error_msg = "something went wrong".to_owned();
    db.errors.insert(job_id, error_msg.clone());

    let service = create_test_service_with_db(db);
    let result = service.get_job_error(job_id).await?;
    assert_eq!(result, error_msg);
    Ok(())
}

#[tokio::test]
async fn get_job_error_returns_error_for_unknown_job() -> anyhow::Result<()> {
    let service = create_test_service();
    let result = service.get_job_error(JobId::random()).await;
    assert!(
        matches!(result, Err(StorageServerError::Db(DbError::JobNotFound(_)))),
        "get_job_error should fail for unknown job"
    );
    Ok(())
}

#[tokio::test]
async fn create_task_instance_returns_execution_context() -> anyhow::Result<()> {
    let service = create_test_service();
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;
    service.start_job(job_id).await?;

    let context = service
        .create_task_instance(
            TEST_SESSION_ID,
            job_id,
            TaskId::Index(0),
            ExecutionManagerId::random(),
        )
        .await?;
    assert_eq!(
        context.task_instance_id, 1,
        "task instance ID should match mock pool counter"
    );
    Ok(())
}

#[tokio::test]
async fn create_task_instance_returns_job_not_found_when_not_in_cache() -> anyhow::Result<()> {
    let service = create_test_service();
    let result = service
        .create_task_instance(
            TEST_SESSION_ID,
            JobId::random(),
            TaskId::Index(0),
            ExecutionManagerId::random(),
        )
        .await;
    assert!(
        matches!(result, Err(StorageServerError::JobNotFound(_))),
        "create_task_instance should return JobNotFound when job is not in cache"
    );
    Ok(())
}

#[tokio::test]
async fn succeed_task_instance_transitions_job_to_succeeded() -> anyhow::Result<()> {
    let db = MockDbConnector::default();
    let service = create_test_service_with_db(db.clone());
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;
    service.start_job(job_id).await?;

    let context = service
        .create_task_instance(
            TEST_SESSION_ID,
            job_id,
            TaskId::Index(0),
            ExecutionManagerId::random(),
        )
        .await?;
    let serialized_outputs = create_test_serialized_outputs();
    let outputs = TaskOutputsSerializer::deserialize(&serialized_outputs)?;
    let state = service
        .succeed_task_instance(
            TEST_SESSION_ID,
            job_id,
            context.task_instance_id,
            0,
            serialized_outputs,
        )
        .await?;
    assert_eq!(state, JobState::Succeeded);
    assert!(
        service.inner.job_cache.get(job_id).await.is_some(),
        "JCB should remain in cache after terminal succeed"
    );
    assert_eq!(
        db.outputs.get(&job_id).map(|stored| stored.clone()),
        Some(outputs)
    );
    Ok(())
}

#[tokio::test]
async fn succeed_task_instance_with_outputs_too_large_for_the_database_fails_the_job()
-> anyhow::Result<()> {
    let db = MockDbConnector::default();
    let service = create_test_service_with_db(db.clone());
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;
    service.start_job(job_id).await?;
    let context = service
        .create_task_instance(
            TEST_SESSION_ID,
            job_id,
            TaskId::Index(0),
            ExecutionManagerId::random(),
        )
        .await?;

    let serialized_outputs =
        TaskOutputsSerializer::from_tuple(&(vec![
            0u8;
            usize::try_from(MOCK_MAX_OUTPUTS_SIZE)? + 1
        ],))?;
    let size = TaskOutputsSerializer::deserialize(&serialized_outputs)?
        .iter()
        .map(|output| output.len() as u64)
        .sum();
    let state = service
        .succeed_task_instance(
            TEST_SESSION_ID,
            job_id,
            context.task_instance_id,
            0,
            serialized_outputs,
        )
        .await?;
    assert_eq!(state, JobState::Failed);
    assert_eq!(service.get_job_state(job_id).await?, JobState::Failed);
    let expected_message = DbError::OutputsTooLarge {
        job_id,
        size,
        limit: MOCK_MAX_OUTPUTS_SIZE,
    }
    .to_string();
    assert_eq!(
        db.errors.get(&job_id).map(|stored| stored.clone()),
        Some(expected_message)
    );
    assert!(!service.inner.shutdown_handle.is_fatal());
    Ok(())
}

#[tokio::test]
async fn succeed_task_instance_returns_job_not_found_when_not_in_cache() -> anyhow::Result<()> {
    let service = create_test_service();
    let result = service
        .succeed_task_instance(
            TEST_SESSION_ID,
            JobId::random(),
            1,
            0,
            create_test_serialized_outputs(),
        )
        .await;
    assert!(
        matches!(result, Err(StorageServerError::JobNotFound(_))),
        "succeed_task_instance should return JobNotFound when job is not in cache"
    );
    Ok(())
}

#[tokio::test]
async fn a_report_with_the_wrong_output_count_fails_the_task_instance() -> anyhow::Result<()> {
    let service = create_test_service();
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;
    service.start_job(job_id).await?;
    let context = service
        .create_task_instance(
            TEST_SESSION_ID,
            job_id,
            TaskId::Index(0),
            ExecutionManagerId::random(),
        )
        .await?;

    let state = service
        .succeed_task_instance(
            TEST_SESSION_ID,
            job_id,
            context.task_instance_id,
            0,
            TaskOutputsSerializer::from_tuple(&(1, 2))
                .expect("output serialization should succeed"),
        )
        .await?;

    assert_eq!(state, JobState::Failed);
    assert_eq!(service.get_job_state(job_id).await?, JobState::Failed);
    assert!(!service.inner.shutdown_handle.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn fail_task_instance_transitions_job_to_failed() -> anyhow::Result<()> {
    let service = create_test_service();
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;
    service.start_job(job_id).await?;

    let context = service
        .create_task_instance(
            TEST_SESSION_ID,
            job_id,
            TaskId::Index(0),
            ExecutionManagerId::random(),
        )
        .await?;
    let state = service
        .fail_task_instance(
            TEST_SESSION_ID,
            job_id,
            context.task_instance_id,
            TaskId::Index(0),
            "test failure".to_owned(),
        )
        .await?;
    assert_eq!(state, JobState::Failed);
    assert!(
        service.inner.job_cache.get(job_id).await.is_some(),
        "JCB should remain in cache after terminal fail"
    );
    Ok(())
}

#[tokio::test]
async fn fail_task_instance_returns_job_not_found_when_not_in_cache() -> anyhow::Result<()> {
    let service = create_test_service();
    let result = service
        .fail_task_instance(
            TEST_SESSION_ID,
            JobId::random(),
            1,
            TaskId::Index(0),
            "error".to_owned(),
        )
        .await;
    assert!(
        matches!(result, Err(StorageServerError::JobNotFound(_))),
        "fail_task_instance should return JobNotFound when job is not in cache"
    );
    Ok(())
}

#[tokio::test]
async fn cancel_job_enqueues_terminal_job_for_cache_gc() -> anyhow::Result<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let db = MockDbConnector::default();
    let service = TestServiceState::new(ServiceStateParams {
        db: db.clone(),
        session_id: TEST_SESSION_ID,
        job_cache: JobCache::new(),
        inbound_queue_sender: MockInboundQueueSender,
        inbound_queue_receiver: create_inbound_queue_receiver(),
        task_instance_pool_connector: MockTaskInstancePoolConnector,
        job_cache_gc_handle: JobCacheGcHandle::new(sender),
        shutdown_handle: ShutdownHandle::new(),
        hooks: TestHooks::default(),
    });
    let job_id = JobId::random();
    db.states.insert(job_id, JobState::Ready);
    let (jcb, receivers) = create_test_jcb(job_id).await;
    service.insert_tree(jcb, receivers).await?;

    service.cancel_job(job_id).await?;

    assert_eq!(
        receiver.try_recv(),
        Ok(job_id),
        "cancelled job should be enqueued for cache GC"
    );
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    Ok(())
}

#[tokio::test]
async fn succeed_task_instance_enqueues_terminal_job_for_cache_gc() -> anyhow::Result<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let service = TestServiceState::new(ServiceStateParams {
        db: MockDbConnector::default(),
        session_id: TEST_SESSION_ID,
        job_cache: JobCache::new(),
        inbound_queue_sender: MockInboundQueueSender,
        inbound_queue_receiver: create_inbound_queue_receiver(),
        task_instance_pool_connector: MockTaskInstancePoolConnector,
        job_cache_gc_handle: JobCacheGcHandle::new(sender),
        shutdown_handle: ShutdownHandle::new(),
        hooks: TestHooks::default(),
    });
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;
    service.start_job(job_id).await?;
    let context = service
        .create_task_instance(
            TEST_SESSION_ID,
            job_id,
            TaskId::Index(0),
            ExecutionManagerId::random(),
        )
        .await?;

    service
        .succeed_task_instance(
            TEST_SESSION_ID,
            job_id,
            context.task_instance_id,
            0,
            create_test_serialized_outputs(),
        )
        .await?;

    assert_eq!(
        receiver.try_recv(),
        Ok(job_id),
        "succeeded job should be enqueued for cache GC"
    );
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    Ok(())
}

#[tokio::test]
async fn fail_task_instance_enqueues_terminal_job_for_cache_gc() -> anyhow::Result<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let service = TestServiceState::new(ServiceStateParams {
        db: MockDbConnector::default(),
        session_id: TEST_SESSION_ID,
        job_cache: JobCache::new(),
        inbound_queue_sender: MockInboundQueueSender,
        inbound_queue_receiver: create_inbound_queue_receiver(),
        task_instance_pool_connector: MockTaskInstancePoolConnector,
        job_cache_gc_handle: JobCacheGcHandle::new(sender),
        shutdown_handle: ShutdownHandle::new(),
        hooks: TestHooks::default(),
    });
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;
    service.start_job(job_id).await?;
    let context = service
        .create_task_instance(
            TEST_SESSION_ID,
            job_id,
            TaskId::Index(0),
            ExecutionManagerId::random(),
        )
        .await?;

    service
        .fail_task_instance(
            TEST_SESSION_ID,
            job_id,
            context.task_instance_id,
            TaskId::Index(0),
            "test failure".to_owned(),
        )
        .await?;

    assert_eq!(
        receiver.try_recv(),
        Ok(job_id),
        "failed job should be enqueued for cache GC"
    );
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    Ok(())
}

#[tokio::test]
async fn task_instance_orchestration_return_stale_session_on_mismatch() -> anyhow::Result<()> {
    // Create a service with a higher session ID to simulate a server restart.
    const CURRENT_SESSION_ID: SessionId = 10;
    const STALE_SESSION_ID: SessionId = CURRENT_SESSION_ID - 1;
    const TASK_INDEX: TaskIndex = 0;
    const TASK_INSTANCE_ID: TaskInstanceId = 1;

    let db = MockDbConnector::default();
    let service = create_test_service_with_db_and_session(db, CURRENT_SESSION_ID);

    // Register a job so the JCB is in cache.
    let (compressed_serialized_task_graph, compressed_serialized_task_graph_input) =
        create_test_job_submission();
    let job_id = service
        .register_job(
            ResourceGroupId::random(),
            compressed_serialized_task_graph,
            compressed_serialized_task_graph_input,
        )
        .await?;

    {
        let result = service
            .create_task_instance(
                STALE_SESSION_ID,
                job_id,
                TaskId::Index(TASK_INDEX),
                ExecutionManagerId::random(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageServerError::StaleSession(_))),
            "create_task_instance should return StaleSession on session mismatch"
        );
    }

    {
        let result = service
            .succeed_task_instance(
                STALE_SESSION_ID,
                job_id,
                TASK_INSTANCE_ID,
                TASK_INDEX,
                create_test_serialized_outputs(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageServerError::StaleSession(_))),
            "succeed_task_instance should return StaleSession on session mismatch"
        );
    }

    {
        let result = service
            .fail_task_instance(
                STALE_SESSION_ID,
                job_id,
                TASK_INSTANCE_ID,
                TaskId::Index(TASK_INDEX),
                "error".to_owned(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageServerError::StaleSession(_))),
            "fail_task_instance should return StaleSession on session mismatch"
        );
    }

    Ok(())
}

#[tokio::test]
async fn add_resource_group_returns_id() -> anyhow::Result<()> {
    let service = create_test_service();
    assert!(
        service
            .add_resource_group(ExternalResourceGroupCredentials::new(
                "external_123".to_owned(),
                vec![1, 2, 3],
            ))
            .await
            .is_ok()
    );
    Ok(())
}

#[tokio::test]
async fn test_add_or_verify_resource_group_creates_and_reuses_id() -> anyhow::Result<()> {
    let db = MockDbConnector::default();
    let service = create_test_service_with_db(db.clone());
    let credentials =
        ExternalResourceGroupCredentials::new("external_123".to_owned(), vec![1, 2, 3]);
    let rg_id = service
        .add_or_verify_resource_group(credentials.clone())
        .await?;
    service
        .verify_resource_group(rg_id, credentials.get_password())
        .await?;
    assert_eq!(
        service.add_or_verify_resource_group(credentials).await?,
        rg_id
    );
    assert_eq!(db.resource_groups.len(), 1);
    Ok(())
}

#[tokio::test]
async fn test_add_or_verify_resource_group_rejects_wrong_password() -> anyhow::Result<()> {
    let db = MockDbConnector::default();
    let service = create_test_service_with_db(db.clone());
    let credentials =
        ExternalResourceGroupCredentials::new("external_123".to_owned(), vec![1, 2, 3]);
    let rg_id = service.add_resource_group(credentials.clone()).await?;
    let result = service
        .add_or_verify_resource_group(ExternalResourceGroupCredentials::new(
            credentials.get_external_resource_group_id().to_owned(),
            vec![4, 5, 6],
        ))
        .await;
    assert!(matches!(
        result,
        Err(StorageServerError::Db(DbError::InvalidPassword(id))) if id == rg_id
    ));
    assert_eq!(
        service.add_or_verify_resource_group(credentials).await?,
        rg_id
    );
    assert_eq!(db.resource_groups.len(), 1);
    Ok(())
}

#[tokio::test]
async fn verify_resource_group_succeeds_for_correct_password() -> anyhow::Result<()> {
    let service = create_test_service();
    let password = vec![1, 2, 3];
    let rg_id = service
        .add_resource_group(ExternalResourceGroupCredentials::new(
            "external_123".to_owned(),
            password.clone(),
        ))
        .await?;
    service.verify_resource_group(rg_id, &password).await?;
    Ok(())
}

#[tokio::test]
async fn verify_resource_group_fails_for_wrong_password() -> anyhow::Result<()> {
    let service = create_test_service();
    let rg_id = service
        .add_resource_group(ExternalResourceGroupCredentials::new(
            "external_123".to_owned(),
            vec![1, 2, 3],
        ))
        .await?;
    let result = service.verify_resource_group(rg_id, &[4, 5, 6]).await;
    assert!(result.is_err(), "verify should fail for wrong password");
    Ok(())
}

#[tokio::test]
async fn poll_ready_tasks_returns_entries_from_inbound_queue() -> anyhow::Result<()> {
    const TASK_INDEX: TaskIndex = 0;
    let (service, sender) = create_test_service_with_inbound_queue(MockDbConnector::default());
    let rg_id = ResourceGroupId::random();
    let job_id = JobId::random();
    sender
        .send_task_ready(rg_id, job_id, vec![TASK_INDEX])
        .await
        .expect("send_task_ready should succeed");

    let entries = service
        .poll_ready_tasks(10, Duration::from_millis(100))
        .await?;
    assert_eq!(entries.len(), 1, "should receive one inbound queue entry");
    assert_eq!(entries[0].job_id, job_id);
    assert_eq!(entries[0].task_kind, TASK_INDEX);
    assert_eq!(entries[0].resource_group_id, rg_id);
    Ok(())
}

#[tokio::test]
async fn poll_ready_tasks_returns_empty_when_no_tasks() -> anyhow::Result<()> {
    let (service, _sender) = create_test_service_with_inbound_queue(MockDbConnector::default());
    let entries = service
        .poll_ready_tasks(10, Duration::from_millis(10))
        .await?;
    assert!(
        entries.is_empty(),
        "should receive no entries from empty inbound queue"
    );
    Ok(())
}

#[tokio::test]
async fn poll_commit_ready_tasks_returns_entries_from_inbound_queue() -> anyhow::Result<()> {
    let (service, sender) = create_test_service_with_inbound_queue(MockDbConnector::default());
    let rg_id = ResourceGroupId::random();
    let job_id = JobId::random();
    sender
        .send_commit_ready(rg_id, job_id)
        .await
        .expect("send_commit_ready should succeed");

    let entries = service
        .poll_commit_ready_tasks(10, Duration::from_millis(100))
        .await?;
    assert_eq!(entries.len(), 1, "should receive one commit-ready entry");
    assert_eq!(entries[0].job_id, job_id);
    assert_eq!(entries[0].task_kind, CommitTaskMarker);
    assert_eq!(entries[0].resource_group_id, rg_id);
    Ok(())
}

#[tokio::test]
async fn poll_cleanup_ready_tasks_returns_entries_from_inbound_queue() -> anyhow::Result<()> {
    let (service, sender) = create_test_service_with_inbound_queue(MockDbConnector::default());
    let rg_id = ResourceGroupId::random();
    let job_id = JobId::random();
    sender
        .send_cleanup_ready(rg_id, job_id)
        .await
        .expect("send_cleanup_ready should succeed");

    let entries = service
        .poll_cleanup_ready_tasks(10, Duration::from_millis(100))
        .await?;
    assert_eq!(entries.len(), 1, "should receive one cleanup-ready entry");
    assert_eq!(entries[0].job_id, job_id);
    assert_eq!(entries[0].task_kind, CleanupTaskMarker);
    assert_eq!(entries[0].resource_group_id, rg_id);
    Ok(())
}

#[tokio::test]
async fn register_execution_manager_returns_id() -> anyhow::Result<()> {
    let service = create_test_service();
    let (_, resource_group_id) = service
        .register_execution_manager("127.0.0.1".parse()?, None)
        .await?;
    assert_eq!(resource_group_id, None);
    Ok(())
}

#[tokio::test]
async fn register_execution_manager_resolves_resource_group() -> anyhow::Result<()> {
    let service = create_test_service();
    let password = b"password";
    let resource_group_id = service
        .add_resource_group(ExternalResourceGroupCredentials::new(
            "external_123".to_owned(),
            password.to_vec(),
        ))
        .await?;

    let (_, registered_resource_group_id) = service
        .register_execution_manager(
            "127.0.0.1".parse()?,
            Some(ExternalResourceGroupCredentials::new(
                "external_123".to_owned(),
                password.to_vec(),
            )),
        )
        .await?;

    assert_eq!(registered_resource_group_id, Some(resource_group_id));
    Ok(())
}

#[tokio::test]
async fn register_execution_manager_rejects_unknown_resource_group() -> anyhow::Result<()> {
    let service = create_test_service();

    let result = service
        .register_execution_manager(
            "127.0.0.1".parse()?,
            Some(ExternalResourceGroupCredentials::new(
                "unknown".to_owned(),
                b"password".to_vec(),
            )),
        )
        .await;

    assert!(matches!(
        result,
        Err(StorageServerError::Db(
            DbError::ExternalResourceGroupNotFound(_)
        ))
    ));
    Ok(())
}

#[tokio::test]
async fn register_execution_manager_rejects_invalid_password() -> anyhow::Result<()> {
    let service = create_test_service();
    service
        .add_resource_group(ExternalResourceGroupCredentials::new(
            "external_123".to_owned(),
            b"password".to_vec(),
        ))
        .await?;

    let result = service
        .register_execution_manager(
            "127.0.0.1".parse()?,
            Some(ExternalResourceGroupCredentials::new(
                "external_123".to_owned(),
                b"wrong-password".to_vec(),
            )),
        )
        .await;

    assert!(matches!(
        result,
        Err(StorageServerError::Db(DbError::InvalidPassword(_)))
    ));
    Ok(())
}

#[tokio::test]
async fn register_scheduler_resends_ready_tasks_only_when_replacing_previous_scheduler()
-> anyhow::Result<()> {
    let (service, _sender) = create_test_service_with_inbound_queue(MockDbConnector::default());

    let (task_graph, inputs) = create_test_job_submission();
    let job_id = service
        .register_job(ResourceGroupId::random(), task_graph, inputs)
        .await?;
    service.start_job(job_id).await?;

    // Starting the job enqueues its initial ready task; drain it so the queue is empty before
    // probing whether a registration triggers a resend.
    let initial = service
        .poll_ready_tasks(10, Duration::from_millis(100))
        .await?;
    assert_eq!(
        initial.len(),
        1,
        "starting the job should enqueue its initial ready task"
    );

    // The first registration has no previous scheduler to replace, so it must not resend.
    service
        .register_scheduler(
            Host::new("scheduler-a.example.com".to_owned())
                .expect("scheduler host should not be empty"),
            8080,
        )
        .await?;
    tokio::task::yield_now().await;
    let after_first = service
        .poll_ready_tasks(10, Duration::from_millis(100))
        .await?;
    assert!(
        after_first.is_empty(),
        "the first scheduler registration must not resend ready tasks"
    );

    // The second registration replaces the first scheduler, so it must resend ready tasks. The
    // resend runs in a spawned background task, so yield to let it run before polling.
    service
        .register_scheduler(
            Host::new("scheduler-b.example.com".to_owned())
                .expect("scheduler host should not be empty"),
            8081,
        )
        .await?;
    tokio::task::yield_now().await;
    let after_second = service
        .poll_ready_tasks(10, Duration::from_millis(100))
        .await?;
    assert_eq!(
        after_second.len(),
        1,
        "the second scheduler registration must resend the job's ready tasks"
    );
    assert_eq!(after_second[0].job_id, job_id);
    Ok(())
}

#[tokio::test]
async fn update_execution_manager_heartbeat_succeeds_for_registered_em() -> anyhow::Result<()> {
    let service = create_test_service();
    let em_id = service
        .register_execution_manager("127.0.0.1".parse()?, None)
        .await?
        .0;
    service.update_execution_manager_heartbeat(em_id).await?;
    Ok(())
}

#[tokio::test]
async fn update_execution_manager_heartbeat_fails_for_unknown_em() -> anyhow::Result<()> {
    let service = create_test_service();
    let result = service
        .update_execution_manager_heartbeat(ExecutionManagerId::random())
        .await;

    assert!(
        matches!(
            result,
            Err(StorageServerError::Db(DbError::IllegalExecutionManagerId(
                _
            )))
        ),
        "update heartbeat should fail for unregistered execution manager"
    );
    Ok(())
}

#[tokio::test]
async fn a_body_failing_during_a_graceful_shutdown_is_stopped() {
    let service = create_test_service();
    let shutdown_handle = service.inner.shutdown_handle.clone();

    let result = service
        .spawn_body(async move {
            shutdown_handle.shut_down_gracefully();
            Err::<(), _>(StorageServerError::from(CacheError::Internal(
                InternalError::InboundQueueChannelClosed,
            )))
        })
        .await;

    assert!(
        matches!(result, Err(StorageServerError::Stopping(_))),
        "a body failing during a graceful shutdown should be stopped, got {result:?}"
    );
    assert!(!service.inner.shutdown_handle.is_fatal());
}

#[tokio::test]
async fn a_body_failing_with_a_fatal_shutdown_keeps_its_error() {
    let service = create_test_service();
    let shutdown_handle = service.inner.shutdown_handle.clone();

    let result = service
        .spawn_body(async move {
            shutdown_handle.shut_down_fatally();
            Err::<(), _>(StorageServerError::from(CacheError::Internal(
                InternalError::InboundQueueChannelClosed,
            )))
        })
        .await;

    assert!(
        matches!(
            result,
            Err(StorageServerError::Cache(CacheError::Internal(
                InternalError::InboundQueueChannelClosed
            )))
        ),
        "a body failing with a fatal shutdown should keep its error, got {result:?}"
    );
}

#[tokio::test]
async fn a_body_failing_fatally_after_its_caller_is_dropped_shuts_down_fatally() {
    let service = create_test_service();
    let (release_sender, release_receiver) = tokio::sync::oneshot::channel();

    let mut caller = Box::pin(service.spawn_body(async move {
        release_receiver
            .await
            .expect("the release sender should not be dropped");
        Err::<(), _>(StorageServerError::from(CacheError::Internal(
            InternalError::InboundQueueChannelClosed,
        )))
    }));
    assert!(std::future::poll_fn(|cx| Poll::Ready(caller.as_mut().poll(cx).is_pending())).await);
    drop(caller);
    release_sender
        .send(())
        .expect("the body should still wait for the release");
    service.inner.task_tracker.close();
    service.inner.task_tracker.wait().await;

    assert!(service.inner.shutdown_handle.is_fatal());
}

#[tokio::test]
async fn a_body_panicking_after_its_caller_is_dropped_shuts_down_fatally() {
    let service = create_test_service();
    let (release_sender, release_receiver) = tokio::sync::oneshot::channel();

    let mut caller = Box::pin(service.spawn_body::<()>(async move {
        release_receiver
            .await
            .expect("the release sender should not be dropped");
        panic!("injected handler body panic")
    }));
    assert!(std::future::poll_fn(|cx| Poll::Ready(caller.as_mut().poll(cx).is_pending())).await);
    drop(caller);
    release_sender
        .send(())
        .expect("the body should still wait for the release");
    service.inner.task_tracker.close();
    service.inner.task_tracker.wait().await;

    assert!(service.inner.shutdown_handle.is_fatal());
}

#[tokio::test]
async fn a_body_failing_fatally_without_a_caller_during_a_graceful_shutdown_is_not_fatal() {
    let service = create_test_service();
    let shutdown_handle = service.inner.shutdown_handle.clone();
    let (release_sender, release_receiver) = tokio::sync::oneshot::channel();

    let mut caller = Box::pin(service.spawn_body(async move {
        release_receiver
            .await
            .expect("the release sender should not be dropped");
        shutdown_handle.shut_down_gracefully();
        Err::<(), _>(StorageServerError::from(CacheError::Internal(
            InternalError::InboundQueueChannelClosed,
        )))
    }));
    assert!(std::future::poll_fn(|cx| Poll::Ready(caller.as_mut().poll(cx).is_pending())).await);
    drop(caller);
    release_sender
        .send(())
        .expect("the body should still wait for the release");
    service.inner.task_tracker.close();
    service.inner.task_tracker.wait().await;

    assert!(service.inner.shutdown_handle.is_cancelled());
    assert!(!service.inner.shutdown_handle.is_fatal());
}

#[tokio::test]
async fn a_body_requested_after_shutdown_starts_is_not_run() {
    let service = create_test_service();
    service.inner.shutdown_handle.shut_down_gracefully();
    let body_ran = Arc::new(AtomicBool::new(false));
    let body_ran_in_body = Arc::clone(&body_ran);

    let result = service
        .spawn_body(async move {
            body_ran_in_body.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await;

    assert!(
        matches!(result, Err(StorageServerError::Stopping(_))),
        "a body requested after shutdown starts should be stopped, got {result:?}"
    );
    assert_eq!(service.inner.task_tracker.len(), 0);
    service.inner.task_tracker.close();
    service.inner.task_tracker.wait().await;
    assert!(!body_ran.load(Ordering::SeqCst));
}
