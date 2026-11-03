//! The fixtures shared by the storage service state's tests.

use spider_core::types::id::SessionId;
use spider_core::types::io::TaskOutputsSerializer;
use spider_utils::shutdown::ShutdownHandle;

use crate::inbound_queue::InboundQueueReceiverHandle;
use crate::job_submission::compress_job_inputs;
use crate::job_submission::compress_task_graph;
use crate::state::JobCache;
use crate::state::JobCacheGcHandle;
use crate::state::service::ServiceState;
use crate::state::service::ServiceStateParams;
use crate::state::test_utils::MockDbConnector;
use crate::state::test_utils::MockInboundQueueSender;
use crate::state::test_utils::MockTaskInstancePoolConnector;
use crate::state::test_utils::create_test_task_graph;
use crate::state::test_utils::create_test_task_graph_input;
use crate::test_hooks::TestHooks;

pub(super) type TestServiceState =
    ServiceState<MockInboundQueueSender, MockDbConnector, MockTaskInstancePoolConnector>;

pub(super) const TEST_SESSION_ID: SessionId = 0;

pub(super) fn create_test_service() -> TestServiceState {
    create_test_service_with_db(MockDbConnector::default())
}

pub(super) fn create_test_service_with_db(db: MockDbConnector) -> TestServiceState {
    create_test_service_with_db_and_session(db, TEST_SESSION_ID)
}

pub(super) fn create_test_service_with_db_and_session(
    db: MockDbConnector,
    session_id: SessionId,
) -> TestServiceState {
    TestServiceState::new(ServiceStateParams {
        db,
        session_id,
        job_cache: JobCache::new(),
        inbound_queue_sender: MockInboundQueueSender,
        inbound_queue_receiver: create_inbound_queue_receiver(),
        task_instance_pool_connector: MockTaskInstancePoolConnector,
        job_cache_gc_handle: JobCacheGcHandle::new(tokio::sync::mpsc::unbounded_channel().0),
        shutdown_handle: ShutdownHandle::new(),
        hooks: TestHooks::default(),
    })
}

pub(super) fn create_inbound_queue_receiver() -> InboundQueueReceiverHandle {
    use crate::inbound_queue::create_inbound_queue;
    let (_sender, receiver) = create_inbound_queue();
    receiver
}

pub(super) fn create_test_job_submission() -> (Vec<u8>, Vec<u8>) {
    let task_graph = create_test_task_graph();
    (
        compress_task_graph(&task_graph),
        compress_job_inputs(&create_test_task_graph_input()),
    )
}

pub(super) fn create_test_serialized_outputs() -> Vec<u8> {
    let output_tuple = (1,);
    TaskOutputsSerializer::from_tuple(&output_tuple).expect("output serialization should succeed")
}
