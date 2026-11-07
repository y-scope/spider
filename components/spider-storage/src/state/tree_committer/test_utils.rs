//! The fixtures shared by the tree committer's tests.

use std::sync::Arc;
use std::time::Duration;

use spider_core::job::JobState;
use spider_core::task::TdlContext;
use spider_core::task::TerminationTaskDescriptor;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_utils::shutdown::ShutdownHandle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
#[cfg(feature = "test-hooks")]
use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;

use crate::cache::job::SharedJobControlBlock;
use crate::cache::task::PreparedChild;
use crate::cache::tree::Pending;
use crate::cache::tree::TreeControlBlock;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::TreeEventKind;
use crate::cache::tree::TreeEventReply;
use crate::db::DbError;
use crate::inbound_queue::InboundQueueSender;
use crate::job_submission::create_validated_submission;
use crate::state::JobCache;
use crate::state::JobCacheGcHandle;
use crate::state::test_utils::MockDbConnector;
use crate::state::test_utils::MockTaskInstancePoolConnector;
use crate::state::test_utils::create_test_task_graph_input;
use crate::state::test_utils::create_test_task_graph_with_termination_tasks;
use crate::state::tree_committer::CommitterEnv;
use crate::state::tree_committer::spawn_committer;
#[cfg(feature = "test-hooks")]
use crate::test_hooks::ArmedHook;
#[cfg(feature = "test-hooks")]
use crate::test_hooks::HookAction;
#[cfg(feature = "test-hooks")]
use crate::test_hooks::HookCount;
#[cfg(feature = "test-hooks")]
use crate::test_hooks::HookFilter;
#[cfg(feature = "test-hooks")]
use crate::test_hooks::HookPoint;
use crate::test_hooks::TestHooks;

pub(super) const ROOT_ID: JobId = JobId::from(1);

pub(super) const TIMEOUT: Duration = Duration::from_secs(5);

/// A one-job tree whose committer runs in its own task tracker.
pub(super) struct TestTree<InboundQueueSenderType: InboundQueueSender + 'static> {
    pub(super) control_block: TreeControlBlock,
    pub(super) jcb: SharedJobControlBlock<InboundQueueSenderType, MockTaskInstancePoolConnector>,
    pub(super) owner_id: ResourceGroupId,
    pub(super) db: MockDbConnector,
    pub(super) job_cache: JobCache<InboundQueueSenderType, MockTaskInstancePoolConnector>,
    pub(super) task_tracker: TaskTracker,
    pub(super) shutdown_handle: ShutdownHandle,
    #[cfg(feature = "test-hooks")]
    pub(super) hooks: TestHooks,
    pub(super) gc_receiver: mpsc::UnboundedReceiver<JobId>,
}

impl<InboundQueueSenderType: InboundQueueSender + 'static> TestTree<InboundQueueSenderType> {
    /// Factory function.
    ///
    /// Registers a one-task root job as [`JobState::Ready`] in a mock database and a job cache,
    /// with a commit task if `has_commit` and a cleanup task if `has_cleanup`, and spawns the
    /// committer of its tree, which publishes through `inbound_queue_sender`.
    ///
    /// # Returns
    ///
    /// The tree.
    ///
    /// # Panics
    ///
    /// Panics if the root's control block cannot be created or cached.
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
        let (control_block, receivers) = TreeControlBlock::new(shutdown_handle.clone());
        let jcb = SharedJobControlBlock::create(
            ROOT_ID,
            owner_id,
            create_validated_submission(task_graph, create_test_task_graph_input()),
            inbound_queue_sender.clone(),
            MockTaskInstancePoolConnector,
            hooks.clone(),
            control_block.clone(),
        )
        .await
        .expect("the root's control block should be created");
        let job_cache = JobCache::new();
        job_cache
            .insert(jcb.clone())
            .await
            .expect("the root's control block should be cached");
        let (gc_sender, gc_receiver) = mpsc::unbounded_channel();
        let task_tracker = TaskTracker::new();
        spawn_committer(
            &task_tracker,
            Arc::new(CommitterEnv {
                db: db.clone(),
                job_cache: job_cache.clone(),
                job_cache_gc_handle: JobCacheGcHandle::new(gc_sender),
                inbound_queue_sender,
                shutdown_handle: shutdown_handle.clone(),
                hooks: hooks.clone(),
            }),
            jcb.clone(),
            receivers,
        );
        Self {
            control_block,
            jcb,
            owner_id,
            db,
            job_cache,
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

    /// Makes the next tree transition of the mock database return `error` before it checks or
    /// writes anything.
    ///
    /// # Panics
    ///
    /// Panics if the mutex of the injected errors is poisoned.
    pub(super) fn inject_transition_error(&self, error: DbError) {
        self.db
            .injected_transition_errors
            .lock()
            .expect("the injected transition errors' mutex should not be poisoned")
            .push_back(error);
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

    /// Arms a pause, for one hit, of the tree's committer at `point`.
    ///
    /// # Returns
    ///
    /// The armed pause.
    #[cfg(feature = "test-hooks")]
    pub(super) fn arm_pause(&self, point: HookPoint) -> ArmedHook {
        self.hooks.arm(
            point,
            HookFilter::job(ROOT_ID),
            HookAction::Pause,
            HookCount::Once,
        )
    }

    /// Submits an event of each of `kinds` of the root, in order, while the tree's committer is
    /// paused before it receives the queued events, so that one round receives them all.
    ///
    /// # Returns
    ///
    /// The tasks that wait for the events' replies, in the order of `kinds`.
    ///
    /// # Panics
    ///
    /// Panics if the committer does not wake for the first event within 5 seconds.
    #[cfg(feature = "test-hooks")]
    pub(super) async fn submit_in_one_round(
        &self,
        kinds: Vec<TreeEventKind>,
    ) -> Vec<JoinHandle<TreeEventReply>> {
        let pause = self.arm_pause(HookPoint::CommitterBeforeReceive);
        let reply_join_handles = kinds
            .into_iter()
            .map(|kind| tokio::spawn(self.submit(kind)))
            .collect();
        pause
            .reached(1, TIMEOUT)
            .await
            .expect("the committer should wake for the first event within 5 seconds");
        pause.release();
        reply_join_handles
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
    registration(Vec::new())
}

/// # Returns
///
/// The regular tasks of a job being done with one output and `children`.
pub(super) fn registration(children: Vec<PreparedChild>) -> TreeEventKind {
    TreeEventKind::RegularTasksDone {
        outputs: vec![vec![1]],
        children,
    }
}

/// # Returns
///
/// A rejection of a child insert's payload.
pub(super) fn payload_rejected() -> DbError {
    DbError::PayloadRejected(sqlx::Error::Protocol(
        "the child payload exceeds the packet size".to_owned(),
    ))
}

/// # Returns
///
/// An event of `kind` of the job `job_id`, whose reply nobody waits for.
pub(super) fn pending(job_id: u64, kind: TreeEventKind) -> Pending {
    Pending {
        event: TreeEvent {
            job_id: JobId::from(job_id),
            kind,
        },
        reply: oneshot::channel().0,
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

/// Awaits the reply to an event that a task waits for.
///
/// # Returns
///
/// The reply that the task of `reply_join_handle` returns.
///
/// # Panics
///
/// Panics if the task does not return within 5 seconds or panics.
#[cfg(feature = "test-hooks")]
pub(super) async fn join_reply(reply_join_handle: JoinHandle<TreeEventReply>) -> TreeEventReply {
    reply(async {
        reply_join_handle
            .await
            .expect("the task waiting for a reply should not panic")
    })
    .await
}
