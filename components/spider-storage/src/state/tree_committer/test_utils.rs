//! The fixtures shared by the tree committer's tests.

use std::time::Duration;

use spider_core::job::JobState;
use spider_core::task::TdlContext;
use spider_core::task::TerminationTaskDescriptor;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_utils::shutdown::ShutdownHandle;
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;

use crate::cache::job::SharedJobControlBlock;
use crate::cache::tree::TreeControlBlock;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::TreeEventKind;
use crate::cache::tree::TreeEventReply;
use crate::inbound_queue::InboundQueueSender;
use crate::job_submission::create_validated_submission;
use crate::state::JobCacheGcHandle;
use crate::state::test_utils::MockDbConnector;
use crate::state::test_utils::MockTaskInstancePoolConnector;
use crate::state::test_utils::create_test_task_graph_input;
use crate::state::test_utils::create_test_task_graph_with_termination_tasks;
use crate::state::tree_committer::CommitterEnv;
use crate::state::tree_committer::spawn_committer;
use crate::test_hooks::TestHooks;

pub(super) const ROOT_ID: JobId = JobId::from(1);

pub(super) const TIMEOUT: Duration = Duration::from_secs(5);

/// A one-job tree whose committer runs in its own task tracker.
pub(super) struct TestTree<InboundQueueSenderType: InboundQueueSender + 'static> {
    pub(super) control_block: TreeControlBlock,
    pub(super) jcb: SharedJobControlBlock<
        InboundQueueSenderType,
        MockDbConnector,
        MockTaskInstancePoolConnector,
    >,
    pub(super) owner_id: ResourceGroupId,
    pub(super) db: MockDbConnector,
    pub(super) task_tracker: TaskTracker,
    pub(super) shutdown_handle: ShutdownHandle,
    #[cfg(feature = "test-hooks")]
    pub(super) hooks: TestHooks,
    pub(super) gc_receiver: mpsc::UnboundedReceiver<JobId>,
}

impl<InboundQueueSenderType: InboundQueueSender + 'static> TestTree<InboundQueueSenderType> {
    /// Factory function.
    ///
    /// Registers a one-task root job as [`JobState::Ready`] in a mock database, with a commit
    /// task if `has_commit` and a cleanup task if `has_cleanup`, and spawns the committer of
    /// its tree, which publishes through `inbound_queue_sender`.
    ///
    /// # Returns
    ///
    /// The tree.
    ///
    /// # Panics
    ///
    /// Panics if the root's control block cannot be created.
    pub(super) async fn spawn(
        has_commit: bool,
        has_cleanup: bool,
        inbound_queue_sender: InboundQueueSenderType,
    ) -> Self {
        let owner_id = ResourceGroupId::random();
        let db = MockDbConnector::default();
        db.states.insert(ROOT_ID, JobState::Ready);
        let shutdown_handle = ShutdownHandle::new();
        let hooks = TestHooks::default();
        hooks.register_runtime(&shutdown_handle);
        let task_graph = create_test_task_graph_with_termination_tasks(
            has_commit.then(termination_task),
            has_cleanup.then(termination_task),
        );
        let jcb = SharedJobControlBlock::create(
            ROOT_ID,
            owner_id,
            create_validated_submission(task_graph, create_test_task_graph_input()),
            inbound_queue_sender.clone(),
            db.clone(),
            MockTaskInstancePoolConnector,
            hooks.clone(),
        )
        .await
        .expect("the root's control block should be created");
        let (gc_sender, gc_receiver) = mpsc::unbounded_channel();
        let (control_block, receivers) = TreeControlBlock::new(shutdown_handle.clone());
        let task_tracker = TaskTracker::new();
        spawn_committer(
            &task_tracker,
            CommitterEnv {
                db: db.clone(),
                job_cache_gc_handle: JobCacheGcHandle::new(gc_sender),
                inbound_queue_sender,
                shutdown_handle: shutdown_handle.clone(),
                hooks: hooks.clone(),
            },
            jcb.clone(),
            receivers,
        );
        Self {
            control_block,
            jcb,
            owner_id,
            db,
            task_tracker,
            shutdown_handle,
            #[cfg(feature = "test-hooks")]
            hooks,
            gc_receiver,
        }
    }

    /// Submits an event of `kind` of the root.
    ///
    /// # Returns
    ///
    /// A future that resolves to the event's reply.
    pub(super) fn submit(
        &self,
        kind: TreeEventKind,
    ) -> impl Future<Output = TreeEventReply> + use<InboundQueueSenderType> {
        self.control_block.submit(TreeEvent {
            job_id: ROOT_ID,
            kind,
        })
    }

    /// Waits until the tree's committer exits.
    ///
    /// # Panics
    ///
    /// Panics if the committer does not exit within 5 seconds.
    pub(super) async fn wait_for_exit(&self) {
        self.task_tracker.close();
        tokio::time::timeout(TIMEOUT, self.task_tracker.wait())
            .await
            .expect("the committer should exit within 5 seconds");
    }
}

pub(super) fn termination_task() -> TerminationTaskDescriptor {
    TerminationTaskDescriptor {
        tdl_context: TdlContext {
            package: "test_pkg".to_owned(),
            task_func: "test_termination_fn".to_owned(),
        },
        execution_policy: None,
    }
}

pub(super) fn start() -> TreeEventKind {
    TreeEventKind::Start {
        ready_task_indices: vec![0],
    }
}

pub(super) fn regular_tasks_done() -> TreeEventKind {
    TreeEventKind::RegularTasksDone {
        outputs: vec![vec![1]],
    }
}

/// Awaits the reply to an event.
///
/// # Returns
///
/// The reply that `future` resolves to.
///
/// # Panics
///
/// Panics if `future` does not resolve within 5 seconds.
pub(super) async fn reply(future: impl Future<Output = TreeEventReply>) -> TreeEventReply {
    tokio::time::timeout(TIMEOUT, future)
        .await
        .expect("the event should be answered within 5 seconds")
}
