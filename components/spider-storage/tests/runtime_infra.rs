//! An in-process storage runtime on its own isolated test database, with test hooks.

use std::collections::HashMap;
use std::collections::HashSet;
use std::net::IpAddr;
use std::time::Duration;

use spider_core::job::JobState;
use spider_core::task::DataTypeDescriptor;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TaskDescriptor;
use spider_core::task::TaskIndex;
use spider_core::task::TaskInputOutputIndex;
use spider_core::task::TdlContext;
use spider_core::task::TerminationTaskDescriptor;
use spider_core::task::ValueTypeDescriptor;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::SerializedTaskOutputs;
use spider_core::types::io::TaskGraphInput;
use spider_core::types::io::TaskGraphInputBuilder;
use spider_core::types::io::TaskOutput;
use spider_core::types::io::TaskOutputsSerializer;
use spider_core::types::resource_group::ExternalResourceGroupCredentials;
use spider_storage::cache::error::CacheError;
use spider_storage::cache::error::StaleStateError;
use spider_storage::db::MariaDbStorageConnector;
use spider_storage::inbound_queue::InboundQueueSenderHandle;
use spider_storage::job_submission::CompressedJobSubmission;
use spider_storage::state::JobCacheGcConfig;
use spider_storage::state::Runtime;
use spider_storage::state::ServiceState;
use spider_storage::state::StorageServerError;
use spider_storage::state::create_runtime_with_hooks;
use spider_storage::state::runtime::RuntimeConfig;
use spider_storage::task_instance_pool::TaskInstancePoolConfig;
use spider_storage::task_instance_pool::TaskInstancePoolHandle;
use spider_storage::test_hooks::ArmedHook;
use spider_storage::test_hooks::HookAction;
use spider_storage::test_hooks::HookCount;
use spider_storage::test_hooks::HookFilter;
use spider_storage::test_hooks::HookPoint;
use spider_storage::test_hooks::TestHooks;
use sqlx::MySqlPool;
use tokio::task::JoinHandle;

use crate::mariadb_infra::IsolatedTestDb;
use crate::mariadb_infra::create_raw_pool;
use crate::task_graph_builder::SubmittedTaskGraph;
use crate::task_graph_builder::compress_job_inputs;
use crate::task_graph_builder::compress_task_graph;

/// How long a test waits for a request to reach a paused hook, or for a request that must not wait
/// on a paused hook to return.
pub const HOOK_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a test waits for published work to arrive on a lane.
pub const PUBLICATION_TIMEOUT: Duration = Duration::from_secs(10);

/// The runtime type that [`TestEnv`] runs.
pub type TestRuntime =
    Runtime<InboundQueueSenderHandle, MariaDbStorageConnector, TaskInstancePoolHandle>;

/// The service type of a [`TestRuntime`].
pub type TestService =
    ServiceState<InboundQueueSenderHandle, MariaDbStorageConnector, TaskInstancePoolHandle>;

/// The result of a job RPC that returns the job's state.
pub type JobResult = Result<JobState, StorageServerError>;

/// A storage runtime on its own isolated test database, with its test hooks, a raw pool for row
/// assertions, a resource group and an execution manager.
pub struct TestEnv {
    pub isolated_test_db: IsolatedTestDb,
    pub config: RuntimeConfig,
    pub runtime: TestRuntime,
    pub service: TestService,
    pub hooks: TestHooks,
    pub pool: MySqlPool,
    pub resource_group_id: ResourceGroupId,
    pub execution_manager_id: ExecutionManagerId,
}

impl TestEnv {
    /// Creates an isolated test database and starts a runtime on it, then resends the ready tasks,
    /// as the storage binary does after it starts serving.
    ///
    /// # Returns
    ///
    /// The started environment.
    ///
    /// # Panics
    ///
    /// Forwards [`Self::start_with_task_instance_pool`]'s panics.
    pub async fn start() -> Self {
        Self::start_with_task_instance_pool(TaskInstancePoolConfig::default()).await
    }

    /// Like [`Self::start`], with `task_instance_pool` as the runtime's task instance pool
    /// configuration.
    ///
    /// # Returns
    ///
    /// The started environment.
    ///
    /// # Panics
    ///
    /// Panics if any step fails.
    pub async fn start_with_task_instance_pool(task_instance_pool: TaskInstancePoolConfig) -> Self {
        let isolated_test_db = IsolatedTestDb::create().await;
        let config = RuntimeConfig {
            db: isolated_test_db.config(),
            task_instance_pool,
            job_cache_gc: JobCacheGcConfig::default(),
        };
        let pool = create_raw_pool(&config.db).await;
        let hooks = TestHooks::default();
        let runtime = Self::start_runtime(&config, hooks.clone()).await;
        let service = runtime.get_service_state();
        let resource_group_id = service
            .add_resource_group(ExternalResourceGroupCredentials::new(
                format!("runtime-test-{}", rand::random::<u64>()),
                b"test-password".to_vec(),
            ))
            .await
            .expect("resource group creation should succeed");
        let execution_manager_id = service
            .register_execution_manager(IpAddr::from([127, 0, 0, 1]), None)
            .await
            .expect("execution manager registration should succeed")
            .0;
        Self {
            isolated_test_db,
            config,
            runtime,
            service,
            hooks,
            pool,
            resource_group_id,
            execution_manager_id,
        }
    }

    /// Like [`Self::start`], with a task instance pool that never considers the test's execution
    /// manager stale, although it sends no heartbeats, so that the pool never removes the test's
    /// task instances or re-enqueues their tasks, however long the test runs.
    ///
    /// # Returns
    ///
    /// The started environment.
    pub async fn start_keeping_task_instances() -> Self {
        Self::start_with_task_instance_pool(TaskInstancePoolConfig {
            execution_manager_stale_cutoff_sec: 24 * 60 * 60,
            ..TaskInstancePoolConfig::default()
        })
        .await
    }

    /// Releases the hooks and stops the runtime, then starts a new runtime with fresh hooks on the
    /// same database and resends the ready tasks, as a restart of the storage binary does.
    ///
    /// # Returns
    ///
    /// The restarted environment.
    ///
    /// # Panics
    ///
    /// Panics if the runtime fails to stop cleanly, or forwards [`Self::start_runtime`]'s panics.
    pub async fn restart(self) -> Self {
        let Self {
            isolated_test_db,
            config,
            runtime,
            service,
            hooks,
            pool,
            resource_group_id,
            execution_manager_id,
        } = self;
        hooks.release_all();
        drop(service);
        runtime
            .stop()
            .await
            .expect("the runtime should stop cleanly");
        let hooks = TestHooks::default();
        let runtime = Self::start_runtime(&config, hooks.clone()).await;
        let service = runtime.get_service_state();
        Self {
            isolated_test_db,
            config,
            runtime,
            service,
            hooks,
            pool,
            resource_group_id,
            execution_manager_id,
        }
    }

    /// # Returns
    ///
    /// Whether the runtime has shut down on a fatal error.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        self.runtime.get_shutdown_handle().is_fatal()
    }

    /// Releases the hooks, stops the runtime and drops the isolated test database.
    ///
    /// # Panics
    ///
    /// Panics if the runtime fails to stop or the database fails to drop.
    pub async fn finish(self) {
        self.hooks.release_all();
        drop(self.service);
        self.runtime
            .stop()
            .await
            .expect("the runtime should stop cleanly");
        self.pool.close().await;
        self.isolated_test_db.drop_database().await;
    }

    /// Registers a job.
    ///
    /// # Returns
    ///
    /// The ID of the registered job.
    ///
    /// # Panics
    ///
    /// Panics if the registration fails.
    pub async fn register_job(
        &self,
        task_graph: &SubmittedTaskGraph,
        task_graph_input: &TaskGraphInput,
    ) -> JobId {
        self.service
            .register_job(
                self.resource_group_id,
                compress_task_graph(task_graph).expect("task graph compression should succeed"),
                compress_job_inputs(task_graph_input).expect("input compression should succeed"),
            )
            .await
            .expect("job registration should succeed")
    }

    /// Registers a chain job of `num_tasks` tasks and starts it.
    ///
    /// # Returns
    ///
    /// The job ID.
    ///
    /// # Panics
    ///
    /// Panics if the registration or the start fails, or if the first task is not published.
    pub async fn start_chain_job(&self, num_tasks: usize, max_num_instances: u32) -> JobId {
        let (graph, inputs) = build_chain_task_graph(
            num_tasks,
            ExecutionPolicy {
                max_num_instances,
                ..ExecutionPolicy::default()
            },
        );
        self.start_job(&graph, &inputs).await
    }

    /// Registers a job and starts it.
    ///
    /// # Returns
    ///
    /// The job ID.
    ///
    /// # Panics
    ///
    /// Panics if the registration or the start fails, or if exactly task 0 is not published.
    pub async fn start_job(
        &self,
        task_graph: &SubmittedTaskGraph,
        task_graph_input: &TaskGraphInput,
    ) -> JobId {
        let job_id = self.register_job(task_graph, task_graph_input).await;
        self.service
            .start_job(job_id)
            .await
            .expect("the job should start");
        assert_eq!(self.poll_ready_tasks(job_id, 1).await, vec![0]);
        job_id
    }

    /// Registers and starts a job of [`build_independent_job`].
    ///
    /// # Returns
    ///
    /// The job ID.
    ///
    /// # Panics
    ///
    /// Panics if the registration or the start fails, or if the job's tasks are not published.
    pub async fn start_independent_job(
        &self,
        num_tasks: usize,
        execution_policy: ExecutionPolicy,
        with_cleanup: bool,
    ) -> JobId {
        let (graph, inputs) = build_independent_job(num_tasks, execution_policy, with_cleanup);
        let job_id = self.register_job(&graph, &inputs).await;
        self.service
            .start_job(job_id)
            .await
            .expect("the job should start");
        let mut task_indices = self.poll_ready_tasks(job_id, num_tasks).await;
        task_indices.sort_unstable();
        assert_eq!(task_indices, (0..num_tasks).collect::<Vec<_>>());
        job_id
    }

    /// Fails the only task of `job_id`, which has no retry left, cancels the job while the failure
    /// is paused before its submit, and then lets the failure go on.
    ///
    /// # Returns
    ///
    /// The job state that the failure returns on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ServiceState::fail_task_instance`]'s return values on failure.
    ///
    /// # Panics
    ///
    /// Panics if the failure does not reach its hook or panics, or if the cancel does not move the
    /// job to `cancelled_state`.
    pub async fn race_terminal_failure_with_cancel(
        &self,
        job_id: JobId,
        cancelled_state: JobState,
    ) -> Result<JobState, StorageServerError> {
        let task_instance_id = self.create_instance(job_id, TaskId::Index(0)).await;
        let pause = self.hooks.arm(
            HookPoint::TaskFailureBeforeSubmit,
            HookFilter::job(job_id),
            HookAction::Pause,
            HookCount::Once,
        );

        let service = self.service.clone();
        let failure = tokio::spawn(async move {
            service
                .fail_task_instance(
                    service.session_id(),
                    job_id,
                    task_instance_id,
                    TaskId::Index(0),
                    "failure".to_owned(),
                )
                .await
        });
        pause.reached(1, HOOK_TIMEOUT).await.expect(
            "the hook should be reached within the timeout; the request may have failed before \
             reaching the hook point",
        );
        assert_eq!(
            self.service
                .cancel_job(job_id)
                .await
                .expect("the cancel should succeed"),
            cancelled_state
        );

        pause.release();
        let result = failure.await.expect("the failure's task should not panic");
        drop(pause);
        result
    }

    /// # Returns
    ///
    /// The ID of the created instance.
    ///
    /// # Panics
    ///
    /// Panics if the instance creation fails.
    pub async fn create_instance(&self, job_id: JobId, task_id: TaskId) -> u64 {
        self.service
            .create_task_instance(
                self.service.session_id(),
                job_id,
                task_id,
                self.execution_manager_id,
            )
            .await
            .expect("the instance creation should succeed")
            .task_instance_id
    }

    /// Reports the success of the task instance `task_instance_id` of the task `task_index` of
    /// `job_id`, whose only output is `output`, with `children`.
    ///
    /// # Returns
    ///
    /// The job state that the report returns on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`ServiceState::succeed_task_instance`]'s return values on failure.
    pub async fn report_success(
        &self,
        job_id: JobId,
        task_index: TaskIndex,
        task_instance_id: u64,
        output: Vec<u8>,
        children: Vec<CompressedJobSubmission>,
    ) -> Result<JobState, StorageServerError> {
        self.task_success(job_id, task_index, task_instance_id, &output, children)
            .await
    }

    /// # Returns
    ///
    /// A future that reports the success of the task instance `task_instance_id` of the task
    /// `task_index` of `job_id`, whose only output is `value`, with `children`, and that forwards
    /// [`ServiceState::succeed_task_instance`]'s return values.
    pub fn task_success(
        &self,
        job_id: JobId,
        task_index: TaskIndex,
        task_instance_id: TaskInstanceId,
        value: &[u8],
        children: Vec<CompressedJobSubmission>,
    ) -> impl Future<Output = JobResult> + Send + use<> {
        let service = self.service.clone();
        let serialized_outputs = serialized_output(value);
        async move {
            service
                .succeed_task_instance(
                    service.session_id(),
                    job_id,
                    task_instance_id,
                    task_index,
                    serialized_outputs,
                    children,
                )
                .await
        }
    }

    /// # Returns
    ///
    /// A future that reports the failure of the task instance `task_instance_id` of `task_id` of
    /// `job_id` with `message`, and that forwards [`ServiceState::fail_task_instance`]'s return
    /// values.
    pub fn task_failure(
        &self,
        job_id: JobId,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
        message: &str,
    ) -> impl Future<Output = JobResult> + Send + use<> {
        let service = self.service.clone();
        let message = message.to_owned();
        async move {
            service
                .fail_task_instance(
                    service.session_id(),
                    job_id,
                    task_instance_id,
                    task_id,
                    message,
                )
                .await
        }
    }

    /// # Returns
    ///
    /// A future that reports the success of the instance `task_instance_id` of `job_id`'s commit
    /// task, and that forwards [`ServiceState::succeed_commit_task_instance`]'s return values.
    pub fn commit_success(
        &self,
        job_id: JobId,
        task_instance_id: TaskInstanceId,
    ) -> impl Future<Output = JobResult> + Send + use<> {
        let service = self.service.clone();
        async move {
            service
                .succeed_commit_task_instance(service.session_id(), job_id, task_instance_id)
                .await
        }
    }

    /// # Returns
    ///
    /// A future that cancels `job_id`, and that forwards [`ServiceState::cancel_job`]'s return
    /// values.
    pub fn cancel(&self, job_id: JobId) -> impl Future<Output = JobResult> + Send + use<> {
        let service = self.service.clone();
        async move { service.cancel_job(job_id).await }
    }

    /// Registers and starts a root job of `shape`, and drains its ready tasks.
    ///
    /// # Returns
    ///
    /// The job ID.
    ///
    /// # Panics
    ///
    /// Panics if the start fails.
    pub async fn start_root(&self, shape: JobShape) -> JobId {
        let (graph, inputs) = shape.build();
        let job_id = self.register_job(&graph, &inputs).await;
        self.service
            .start_job(job_id)
            .await
            .expect("the job should start");
        self.drain_lane(Lane::Task, &shape.ready_tasks(job_id))
            .await;
        job_id
    }

    /// Runs a new instance of task 0 of `parent_id` to success with the output `value` and one
    /// child per shape of `shapes`, and drains the children's ready tasks.
    ///
    /// # Returns
    ///
    /// The IDs of the registered children, by ascending ID, which is the order of `shapes`.
    ///
    /// # Panics
    ///
    /// Panics if the report fails, or if it doesn't block the parent on one child per shape.
    pub async fn register_children(
        &self,
        parent_id: JobId,
        value: &[u8],
        shapes: &[JobShape],
    ) -> Vec<JobId> {
        let task_instance_id = self.create_instance(parent_id, TaskId::Index(0)).await;
        let children = shapes.iter().map(|shape| shape.child()).collect();
        assert_eq!(
            self.task_success(parent_id, 0, task_instance_id, value, children)
                .await
                .expect("the registering report should succeed"),
            JobState::BlockingOnChildren
        );
        let child_ids = self.children_of(parent_id).await;
        assert_eq!(child_ids.len(), shapes.len());
        let ready_tasks: Vec<_> = child_ids
            .iter()
            .zip(shapes)
            .flat_map(|(child_id, shape)| shape.ready_tasks(*child_id))
            .collect();
        self.drain_lane(Lane::Task, &ready_tasks).await;
        child_ids
    }

    /// Runs a new instance of the task `task_index` of `job_id` to success with the output `value`.
    ///
    /// # Returns
    ///
    /// The job state that the report returns.
    ///
    /// # Panics
    ///
    /// Panics if the report fails.
    pub async fn run_task(&self, job_id: JobId, task_index: TaskIndex, value: &[u8]) -> JobState {
        let task_instance_id = self
            .create_instance(job_id, TaskId::Index(task_index))
            .await;
        self.task_success(job_id, task_index, task_instance_id, value, Vec::new())
            .await
            .expect("the report should succeed")
    }

    /// Drains the published cleanup tasks of exactly `job_ids`, then runs a new instance of each to
    /// success.
    ///
    /// # Panics
    ///
    /// Panics if a report fails or doesn't cancel its job.
    pub async fn run_cleanups(&self, job_ids: &[JobId]) {
        let cleanup_tasks: Vec<_> = job_ids
            .iter()
            .map(|job_id| (*job_id, TaskId::Cleanup))
            .collect();
        self.drain_lane(Lane::Cleanup, &cleanup_tasks).await;
        for job_id in job_ids {
            let task_instance_id = self.create_instance(*job_id, TaskId::Cleanup).await;
            assert_eq!(
                self.service
                    .succeed_cleanup_task_instance(
                        self.service.session_id(),
                        *job_id,
                        task_instance_id
                    )
                    .await
                    .expect("the cleanup report should succeed"),
                JobState::Cancelled,
                "the cleanup of job {job_id}"
            );
        }
    }

    /// Drains the published commit task of `job_id` and registers an instance of it.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The ID of the instance.
    /// * The outputs that the instance's execution context hands to the commit task.
    ///
    /// # Panics
    ///
    /// Panics if the instance creation fails, or if the context doesn't hold task outputs.
    pub async fn commit_instance(&self, job_id: JobId) -> (TaskInstanceId, Vec<TaskOutput>) {
        self.drain_lane(Lane::Commit, &[(job_id, TaskId::Commit)])
            .await;
        let execution_context = self
            .service
            .create_task_instance(
                self.service.session_id(),
                job_id,
                TaskId::Commit,
                self.execution_manager_id,
            )
            .await
            .expect("the commit instance creation should succeed");
        let outputs =
            SerializedTaskOutputs::deserialize_from_raw(&execution_context.serialized_task_io)
                .expect("the commit task's context should hold task outputs");
        (execution_context.task_instance_id, outputs)
    }

    /// Polls the task lane until it has returned `num_entries` ready tasks of `job_id`, ignoring
    /// other jobs' entries.
    ///
    /// # Returns
    ///
    /// The task indices, in the order they were polled.
    ///
    /// # Panics
    ///
    /// Panics if the entries do not arrive within 10 seconds.
    pub async fn poll_ready_tasks(&self, job_id: JobId, num_entries: usize) -> Vec<TaskIndex> {
        let mut task_indices = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), async {
            while task_indices.len() < num_entries {
                let entries = self
                    .service
                    .poll_ready_tasks(num_entries - task_indices.len(), Duration::from_secs(1))
                    .await
                    .expect("polling should succeed");
                task_indices.extend(
                    entries
                        .into_iter()
                        .filter(|entry| entry.job_id == job_id)
                        .map(|entry| entry.task_kind),
                );
            }
        })
        .await
        .expect("the ready tasks should be published");
        task_indices
    }

    /// Polls the task lane until every job of `job_ids` has published its task 0, then checks that
    /// none of them publishes another task, ignoring other jobs' entries.
    ///
    /// # Panics
    ///
    /// Panics if the entries do not arrive within 10 seconds, or if a job of `job_ids` publishes
    /// any other task.
    pub async fn poll_first_task_of_each(&self, job_ids: &[JobId]) {
        let mut remaining_job_ids: HashSet<JobId> = job_ids.iter().copied().collect();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !remaining_job_ids.is_empty() {
                let entries = self
                    .service
                    .poll_ready_tasks(remaining_job_ids.len(), Duration::from_secs(1))
                    .await
                    .expect("polling should succeed");
                for entry in entries {
                    if !job_ids.contains(&entry.job_id) {
                        continue;
                    }
                    assert_eq!(entry.task_kind, 0);
                    assert!(
                        remaining_job_ids.remove(&entry.job_id),
                        "job {:?} published its first task twice",
                        entry.job_id
                    );
                }
            }
        })
        .await
        .expect("every job's first task should be published");
        let entries = self
            .service
            .poll_ready_tasks(job_ids.len(), Duration::from_millis(200))
            .await
            .expect("polling should succeed");
        assert_eq!(
            entries
                .into_iter()
                .filter(|entry| job_ids.contains(&entry.job_id))
                .map(|entry| (entry.job_id, entry.task_kind))
                .collect::<Vec<_>>(),
            Vec::new()
        );
    }

    /// Polls the commit lane until it has returned the commit task of `job_id`, ignoring other
    /// jobs' entries.
    ///
    /// # Panics
    ///
    /// Panics if the entry does not arrive within 10 seconds.
    pub async fn poll_commit_ready_task(&self, job_id: JobId) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let entries = self
                    .service
                    .poll_commit_ready_tasks(1, Duration::from_secs(1))
                    .await
                    .expect("polling should succeed");
                if entries.iter().any(|entry| entry.job_id == job_id) {
                    return;
                }
            }
        })
        .await
        .expect("the commit task should be published");
    }

    /// Polls the cleanup lane until it has returned the cleanup task of `job_id`, ignoring other
    /// jobs' entries.
    ///
    /// # Panics
    ///
    /// Panics if the entry does not arrive within 10 seconds.
    pub async fn poll_cleanup_ready_task(&self, job_id: JobId) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let entries = self
                    .service
                    .poll_cleanup_ready_tasks(1, Duration::from_secs(1))
                    .await
                    .expect("polling should succeed");
                if entries.iter().any(|entry| entry.job_id == job_id) {
                    return;
                }
            }
        })
        .await
        .expect("the cleanup task should be published");
    }

    /// # Returns
    ///
    /// The entries that one poll of `lane` returns, at most `max_entries` within `wait`.
    ///
    /// # Panics
    ///
    /// Panics if the poll fails.
    pub async fn poll_lane(
        &self,
        lane: Lane,
        max_entries: usize,
        wait: Duration,
    ) -> Vec<(JobId, TaskId)> {
        match lane {
            Lane::Task => self
                .service
                .poll_ready_tasks(max_entries, wait)
                .await
                .expect("the task lane poll should succeed")
                .into_iter()
                .map(|entry| (entry.job_id, TaskId::Index(entry.task_kind)))
                .collect(),
            Lane::Commit => self
                .service
                .poll_commit_ready_tasks(max_entries, wait)
                .await
                .expect("the commit lane poll should succeed")
                .into_iter()
                .map(|entry| (entry.job_id, TaskId::Commit))
                .collect(),
            Lane::Cleanup => self
                .service
                .poll_cleanup_ready_tasks(max_entries, wait)
                .await
                .expect("the cleanup lane poll should succeed")
                .into_iter()
                .map(|entry| (entry.job_id, TaskId::Cleanup))
                .collect(),
        }
    }

    /// Polls `lane` until it has returned exactly the entries of `expected`, in any order.
    ///
    /// # Panics
    ///
    /// Panics if the lane returns an entry that `expected` doesn't hold, or one more time than
    /// `expected` holds it, or if the entries don't arrive within [`PUBLICATION_TIMEOUT`].
    pub async fn drain_lane(&self, lane: Lane, expected: &[(JobId, TaskId)]) {
        let mut remaining: HashMap<(JobId, TaskId), usize> = HashMap::new();
        for entry in expected {
            *remaining.entry(*entry).or_default() += 1;
        }
        let mut num_remaining = expected.len();
        tokio::time::timeout(PUBLICATION_TIMEOUT, async {
            while num_remaining > 0 {
                for entry in self
                    .poll_lane(lane, num_remaining, Duration::from_secs(1))
                    .await
                {
                    let Some(count) = remaining.get_mut(&entry).filter(|count| **count > 0) else {
                        panic!("unexpected entry {entry:?} in the {lane:?} lane");
                    };
                    *count -= 1;
                    num_remaining -= 1;
                }
            }
        })
        .await
        .expect("the expected lane entries should be published");
    }

    /// Asserts that no lane holds an entry.
    pub async fn assert_lanes_empty(&self) {
        for lane in [Lane::Task, Lane::Commit, Lane::Cleanup] {
            assert_eq!(
                self.poll_lane(lane, 1, Duration::from_millis(100)).await,
                Vec::new(),
                "the {lane:?} lane should be empty"
            );
        }
    }

    /// Asserts that `job_id` is not in the job cache: an instance creation for its task 0 finds no
    /// job.
    pub async fn assert_not_cached(&self, job_id: JobId, what: &str) {
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

    /// # Returns
    ///
    /// A hook that observes every hit of `point` that `filter` matches.
    #[must_use]
    pub fn observe(&self, point: HookPoint, filter: HookFilter) -> ArmedHook {
        self.hooks
            .arm(point, filter, HookAction::Observe, HookCount::Always)
    }

    /// # Returns
    ///
    /// A hook that pauses the first hit of `point` that `filter` matches.
    #[must_use]
    pub fn pause(&self, point: HookPoint, filter: HookFilter) -> ArmedHook {
        self.hooks
            .arm(point, filter, HookAction::Pause, HookCount::Once)
    }

    /// Waits until the job's cached state is `expected`.
    ///
    /// # Panics
    ///
    /// Panics if the job state is unreadable, or if it does not become `expected` within 10
    /// seconds.
    pub async fn wait_for_job_state(&self, job_id: JobId, expected: JobState) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self
                .service
                .get_job_state(job_id)
                .await
                .expect("the job state should be readable")
                != expected
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the job should reach the expected state");
    }

    /// # Returns
    ///
    /// The state of `job_id`'s row in the database.
    ///
    /// # Panics
    ///
    /// Panics if the query fails.
    pub async fn row_state(&self, job_id: JobId) -> JobState {
        sqlx::query_scalar::<_, JobState>("SELECT `state` FROM `jobs` WHERE `id` = ?;")
            .bind(job_id)
            .fetch_one(&self.pool)
            .await
            .expect("the row state query should succeed")
    }

    /// # Returns
    ///
    /// Whether `job_id`'s row in the database has job outputs.
    ///
    /// # Panics
    ///
    /// Panics if the query fails.
    pub async fn has_outputs(&self, job_id: JobId) -> bool {
        sqlx::query_scalar::<_, bool>(
            "SELECT `serialized_job_outputs` IS NOT NULL FROM `jobs` WHERE `id` = ?;",
        )
        .bind(job_id)
        .fetch_one(&self.pool)
        .await
        .expect("the outputs query should succeed")
    }

    /// # Returns
    ///
    /// The rows of the children of `parent_id` in the database, by ascending ID.
    ///
    /// # Panics
    ///
    /// Panics if the query fails.
    pub async fn child_rows(&self, parent_id: JobId) -> Vec<ChildRow> {
        sqlx::query_as::<_, (JobId, Option<JobId>, Option<JobId>, JobState, Vec<u8>)>(
            "SELECT `id`, `parent_id`, `root_id`, `state`, `compressed_serialized_task_graph` \
             FROM `jobs` WHERE `parent_id` = ? ORDER BY `id`;",
        )
        .bind(parent_id)
        .fetch_all(&self.pool)
        .await
        .expect("the child row query should succeed")
        .into_iter()
        .map(
            |(id, parent_id, root_id, state, compressed_serialized_task_graph)| ChildRow {
                id,
                parent_id,
                root_id,
                state,
                compressed_serialized_task_graph,
            },
        )
        .collect()
    }

    /// # Returns
    ///
    /// The IDs of the children of `parent_id` in the database, by ascending ID.
    pub async fn children_of(&self, parent_id: JobId) -> Vec<JobId> {
        self.child_rows(parent_id)
            .await
            .into_iter()
            .map(|row| row.id)
            .collect()
    }

    /// # Returns
    ///
    /// The outputs of `job_id`'s row in the database, or [`None`] if it has none.
    ///
    /// # Panics
    ///
    /// Panics if the query fails or the outputs fail to deserialize.
    pub async fn row_outputs(&self, job_id: JobId) -> Option<Vec<TaskOutput>> {
        sqlx::query_scalar::<_, Option<Vec<u8>>>(
            "SELECT `serialized_job_outputs` FROM `jobs` WHERE `id` = ?;",
        )
        .bind(job_id)
        .fetch_one(&self.pool)
        .await
        .expect("the outputs query should succeed")
        .map(|serialized_outputs| {
            SerializedTaskOutputs::deserialize_from_raw(&serialized_outputs)
                .expect("the stored outputs should deserialize")
        })
    }

    /// # Returns
    ///
    /// The states of the rows of the tree rooted at `root_id` in the database, by job.
    pub async fn tree_rows(&self, root_id: JobId) -> HashMap<JobId, JobState> {
        tree_rows(&self.pool, root_id).await
    }

    /// # Returns
    ///
    /// The error messages of the rows of the tree rooted at `root_id` in the database, by job.
    ///
    /// # Panics
    ///
    /// Panics if the query fails.
    pub async fn tree_errors(&self, root_id: JobId) -> HashMap<JobId, String> {
        sqlx::query_as::<_, (JobId, String)>(
            "SELECT `id`, `error_message` FROM `jobs` WHERE (`id` = ? OR `root_id` = ?) AND \
             `error_message` IS NOT NULL;",
        )
        .bind(root_id)
        .bind(root_id)
        .fetch_all(&self.pool)
        .await
        .expect("the error query should succeed")
        .into_iter()
        .collect()
    }

    /// Asserts that every job of `expected` is in its state in its row, whose end time is set
    /// exactly when the state is terminal, and, for a root, in what [`ServiceState::get_job_state`]
    /// answers, and that the service has not shut down fatally.
    ///
    /// # Panics
    ///
    /// Panics if the query or a root's state read fails, or if an assertion fails.
    pub async fn assert_states(&self, expected: &HashMap<JobId, JobState>, what: &str) {
        for (job_id, state) in expected {
            let (row_state, has_ended, is_root) = sqlx::query_as::<_, (JobState, bool, bool)>(
                "SELECT `state`, `ended_at` IS NOT NULL, `parent_id` IS NULL FROM `jobs` WHERE \
                 `id` = ?;",
            )
            .bind(job_id)
            .fetch_one(&self.pool)
            .await
            .expect("the row state query should succeed");
            assert_eq!(row_state, *state, "{what}: the row state of job {job_id}");
            assert_eq!(
                has_ended,
                state.is_terminal(),
                "{what}: the end time of job {job_id} in state {state:?}"
            );
            if is_root {
                assert_eq!(
                    self.service
                        .get_job_state(*job_id)
                        .await
                        .expect("the root's state should be readable"),
                    *state,
                    "{what}: the cached state of the root {job_id}"
                );
            }
        }
        assert!(
            !self.is_fatal(),
            "{what}: the service should not have shut down fatally"
        );
    }

    /// Asserts that the rows of the tree rooted at `root_id` are exactly the jobs of `expected`, in
    /// their states.
    pub async fn assert_tree_rows(
        &self,
        root_id: JobId,
        expected: &HashMap<JobId, JobState>,
        what: &str,
    ) {
        assert_eq!(
            self.tree_rows(root_id).await,
            *expected,
            "{what}: the rows of the tree"
        );
    }

    /// Asserts that the tree rooted at `root_id` has no non-terminal row.
    ///
    /// # Panics
    ///
    /// Panics if the query fails or a row is not terminal.
    pub async fn assert_quiescent(&self, root_id: JobId) {
        let non_terminal_rows: HashMap<JobId, JobState> = self
            .tree_rows(root_id)
            .await
            .into_iter()
            .filter(|(_, state)| !state.is_terminal())
            .collect();
        assert_eq!(
            non_terminal_rows,
            HashMap::new(),
            "the tree of {root_id} should be quiescent"
        );
    }

    /// Asserts that the job's cached and persisted states are both `expected`, and that the service
    /// has not shut down fatally.
    pub async fn assert_job_state(&self, job_id: JobId, expected: JobState) {
        assert_eq!(
            self.service
                .get_job_state(job_id)
                .await
                .expect("the job state should be readable"),
            expected
        );
        assert_eq!(self.row_state(job_id).await, expected);
        assert!(!self.is_fatal());
    }

    /// Starts a runtime with `hooks` under `config`, then resends the ready tasks.
    ///
    /// # Returns
    ///
    /// The started runtime.
    ///
    /// # Panics
    ///
    /// Panics if the runtime fails to start or the ready-task resend fails.
    async fn start_runtime(config: &RuntimeConfig, hooks: TestHooks) -> TestRuntime {
        let runtime = create_runtime_with_hooks(config, hooks)
            .await
            .expect("the runtime should start");
        runtime
            .get_service_state()
            .resend_ready_tasks()
            .await
            .expect("the ready-task resend should succeed");
        runtime
    }
}

/// A row of the `jobs` table that has a parent.
#[derive(Debug, PartialEq, Eq)]
pub struct ChildRow {
    pub id: JobId,
    pub parent_id: Option<JobId>,
    pub root_id: Option<JobId>,
    pub state: JobState,
    pub compressed_serialized_task_graph: Vec<u8>,
}

/// The shape of a test job of independent tasks, each taking and returning one byte-typed value,
/// whose tasks, commit task and cleanup task each run at most `max_num_instances` instances at once
/// and are retried at most `max_num_retry` times.
#[derive(Clone, Copy, Debug)]
pub struct JobShape {
    pub num_tasks: usize,
    pub with_commit: bool,
    pub with_cleanup: bool,
    pub max_num_instances: u32,
    pub max_num_retry: u32,
}

impl JobShape {
    /// A job of one task, without a commit or a cleanup task, whose task runs one instance at once
    /// and is never retried.
    pub const ONE_TASK: Self = Self {
        num_tasks: 1,
        with_commit: false,
        with_cleanup: false,
        max_num_instances: 1,
        max_num_retry: 0,
    };

    /// Builds a job of this shape.
    ///
    /// # Returns
    ///
    /// The task graph and its inputs.
    #[must_use]
    pub fn build(self) -> (SubmittedTaskGraph, TaskGraphInput) {
        self.build_with_execution_policy(ExecutionPolicy {
            max_num_instances: self.max_num_instances,
            max_num_retry: self.max_num_retry,
            ..ExecutionPolicy::default()
        })
    }

    /// # Returns
    ///
    /// The submission of a child job of this shape, as [`Self::build`] builds it.
    ///
    /// # Panics
    ///
    /// Panics if the job fails to compress.
    #[must_use]
    pub fn child(self) -> CompressedJobSubmission {
        let (graph, inputs) = self.build();
        CompressedJobSubmission {
            compressed_serialized_task_graph: compress_task_graph(&graph)
                .expect("task graph compression should succeed"),
            compressed_serialized_job_inputs: compress_job_inputs(&inputs)
                .expect("input compression should succeed"),
        }
    }

    /// # Returns
    ///
    /// The task-lane entries that the start of `job_id`, a job of this shape, publishes.
    #[must_use]
    pub fn ready_tasks(self, job_id: JobId) -> Vec<(JobId, TaskId)> {
        (0..self.num_tasks)
            .map(|task_index| (job_id, TaskId::Index(task_index)))
            .collect()
    }

    /// Builds a job of this shape whose tasks, commit task and cleanup task run under
    /// `execution_policy` instead of the shape's own limits.
    ///
    /// # Returns
    ///
    /// The task graph and its inputs.
    ///
    /// # Panics
    ///
    /// Panics if the graph fails to build.
    fn build_with_execution_policy(
        self,
        execution_policy: ExecutionPolicy,
    ) -> (SubmittedTaskGraph, TaskGraphInput) {
        let execution_policy = Some(execution_policy);
        let termination_task = |task_func: &str| TerminationTaskDescriptor {
            tdl_context: TdlContext {
                package: "test".to_owned(),
                task_func: task_func.to_owned(),
            },
            execution_policy: execution_policy.clone(),
        };
        let mut graph = SubmittedTaskGraph::new(
            self.with_commit.then(|| termination_task("commit")),
            self.with_cleanup.then(|| termination_task("cleanup")),
        )
        .expect("task graph creation should succeed");
        let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
        let mut inputs = TaskGraphInputBuilder::new();
        for task_index in 0..self.num_tasks {
            graph
                .insert_task(TaskDescriptor {
                    tdl_context: TdlContext {
                        package: "test".to_owned(),
                        task_func: format!("task{task_index}"),
                    },
                    execution_policy: execution_policy.clone(),
                    inputs: vec![bytes_type.clone()],
                    outputs: vec![bytes_type.clone()],
                    input_sources: None,
                })
                .expect("task insertion should succeed");
            inputs
                .append_task_input(&[0u8; 4])
                .expect("task input appending should succeed");
        }
        (graph, inputs.build())
    }
}

/// A lane of the inbound queue.
#[derive(Clone, Copy, Debug)]
pub enum Lane {
    Task,
    Commit,
    Cleanup,
}

/// Observers of the committer of one tree: its rounds, the database writes of its rounds, and the
/// rounds that write a transition of each of some of its jobs.
pub struct TreeProbes {
    rounds: ArmedHook,
    writes: ArmedHook,
    transitions: HashMap<JobId, ArmedHook>,
}

impl TreeProbes {
    /// Arms the probes of the tree rooted at `root_id`, which also observe the rounds that write a
    /// transition of each job of `job_ids`. The writes are those of every tree of `env`: the tests
    /// that count them run one tree at a time.
    ///
    /// # Returns
    ///
    /// The armed probes.
    #[must_use]
    pub fn arm(env: &TestEnv, root_id: JobId, job_ids: &[JobId]) -> Self {
        Self {
            rounds: env.observe(HookPoint::CommitterBeforeReceive, HookFilter::job(root_id)),
            writes: env.observe(HookPoint::CommitterRoundAfterDbWrite, HookFilter::default()),
            transitions: job_ids
                .iter()
                .map(|job_id| {
                    (
                        *job_id,
                        env.observe(
                            HookPoint::CommitterRoundAfterDbWrite,
                            HookFilter::job(*job_id),
                        ),
                    )
                })
                .collect(),
        }
    }

    /// # Returns
    ///
    /// The number of rounds that have written a transition of `job_id`.
    ///
    /// # Panics
    ///
    /// Panics if the probes don't observe `job_id`.
    #[must_use]
    pub fn transitions(&self, job_id: JobId) -> usize {
        self.transitions
            .get(&job_id)
            .expect("the probes should observe the job")
            .hits()
    }

    /// Asserts the number of rounds of the tree and the number of database writes.
    pub fn assert_rounds(&self, num_rounds: usize, num_writes: usize, what: &str) {
        assert_eq!(
            self.rounds.hits(),
            num_rounds,
            "{what}: the number of rounds"
        );
        assert_eq!(
            self.writes.hits(),
            num_writes,
            "{what}: the number of database writes"
        );
    }

    /// Asserts, for each job of `expected`, the number of rounds that have written a transition of
    /// it.
    pub fn assert_transitions(&self, expected: &HashMap<JobId, usize>, what: &str) {
        for (job_id, num_transitions) in expected {
            assert_eq!(
                self.transitions(*job_id),
                *num_transitions,
                "{what}: the rounds that transition job {job_id}"
            );
        }
    }
}

/// Two RPCs whose events race in one tree: the first RPC's round is paused at a hook point until
/// the second RPC has submitted its event.
///
/// # Type Parameters
///
/// * `FirstType` - The output type of the first RPC.
/// * `SecondType` - The output type of the second RPC.
pub struct Race<FirstType, SecondType> {
    paused: ArmedHook,
    first: JoinHandle<FirstType>,
    second: JoinHandle<SecondType>,
}

impl<FirstType: Send + 'static, SecondType: Send + 'static> Race<FirstType, SecondType> {
    /// Runs `first` until a round of its tree pauses at `point` on a hit that `filter` matches,
    /// then runs `second` until `second_job_id` has submitted its event:
    ///
    /// * At [`HookPoint::CommitterBeforeReceive`], with the filter of the tree's root, both events
    ///   are planned in one round.
    /// * At [`HookPoint::CommitterRoundAfterDbWrite`], with the filter of a job that `first`'s
    ///   round transitions, `second` runs while that round is written but not applied, and its
    ///   event is planned in a later round.
    ///
    /// # Returns
    ///
    /// The race, with `first`'s round still paused.
    pub async fn start(
        env: &TestEnv,
        point: HookPoint,
        filter: HookFilter,
        first: impl Future<Output = FirstType> + Send + 'static,
        second: impl Future<Output = SecondType> + Send + 'static,
        second_job_id: JobId,
    ) -> Self {
        let paused = env.pause(point, filter);
        let first = tokio::spawn(first);
        reached(&paused, 1).await;
        let submitted = env.observe(
            HookPoint::JobEventAfterSubmit,
            HookFilter::job(second_job_id),
        );
        let second = tokio::spawn(second);
        reached(&submitted, 1).await;
        Self {
            paused,
            first,
            second,
        }
    }

    /// Like [`Self::start`], for two RPCs whose events are both submitted by `job_id`: waits until
    /// `first` has submitted its event before running `second`, so that `first`'s submission is
    /// never taken for `second`'s.
    ///
    /// # Returns
    ///
    /// The race, with `first`'s round still paused.
    pub async fn start_on_one_job(
        env: &TestEnv,
        point: HookPoint,
        filter: HookFilter,
        first: impl Future<Output = FirstType> + Send + 'static,
        second: impl Future<Output = SecondType> + Send + 'static,
        job_id: JobId,
    ) -> Self {
        let submitted = env.observe(HookPoint::JobEventAfterSubmit, HookFilter::job(job_id));
        let paused = env.pause(point, filter);
        let first = tokio::spawn(first);
        reached(&paused, 1).await;
        reached(&submitted, 1).await;
        let second = tokio::spawn(second);
        reached(&submitted, 2).await;
        Self {
            paused,
            first,
            second,
        }
    }

    /// Releases the first RPC's round.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The result of the first RPC.
    /// * The result of the second RPC.
    ///
    /// # Panics
    ///
    /// Panics if the paused hook fired more than once.
    pub async fn finish(self) -> (FirstType, SecondType) {
        self.paused.release();
        let first = join(self.first).await;
        let second = join(self.second).await;
        assert_eq!(self.paused.hits(), 1);
        (first, second)
    }
}

/// The rows of a tree at the database write of one of its rounds, read while the round is paused
/// after its write.
pub struct CommitSnapshot {
    reader: JoinHandle<HashMap<JobId, JobState>>,
}

impl CommitSnapshot {
    /// Arms a snapshot of the rows of the tree rooted at `root_id` at the `nth` round, counted from
    /// now, that writes a transition of `job_id`.
    ///
    /// # Returns
    ///
    /// The armed snapshot.
    #[must_use]
    pub fn arm(env: &TestEnv, root_id: JobId, job_id: JobId, nth: usize) -> Self {
        let paused = env.hooks.arm(
            HookPoint::CommitterRoundAfterDbWrite,
            HookFilter::job(job_id),
            HookAction::Pause,
            HookCount::Times(nth),
        );
        for _ in 1..nth {
            paused.release();
        }
        let pool = env.pool.clone();
        let reader = tokio::spawn(async move {
            reached(&paused, nth).await;
            let rows = tree_rows(&pool, root_id).await;
            paused.release();
            rows
        });
        Self { reader }
    }

    /// # Returns
    ///
    /// The states of the rows of the tree at the write, by job.
    pub async fn rows(self) -> HashMap<JobId, JobState> {
        join(self.reader).await
    }
}

/// Builds a graph of `num_tasks` tasks under `execution_policy`, where every task but the first
/// consumes the previous task's output, with one byte-typed input for the first task.
///
/// # Returns
///
/// The task graph and its inputs.
///
/// # Panics
///
/// Panics if the graph fails to build.
#[must_use]
pub fn build_chain_task_graph(
    num_tasks: usize,
    execution_policy: ExecutionPolicy,
) -> (SubmittedTaskGraph, TaskGraphInput) {
    let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
    let execution_policy = Some(execution_policy);
    let mut graph =
        SubmittedTaskGraph::new(None, None).expect("chain task graph creation should succeed");
    for task_index in 0..num_tasks {
        graph
            .insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: "test".to_owned(),
                    task_func: format!("n{task_index}"),
                },
                execution_policy: execution_policy.clone(),
                inputs: vec![bytes_type.clone()],
                outputs: vec![bytes_type.clone()],
                input_sources: task_index.checked_sub(1).map(|parent| {
                    vec![TaskInputOutputIndex {
                        task_idx: parent,
                        position: 0,
                    }]
                }),
            })
            .expect("chain task insertion should succeed");
    }
    let mut inputs = TaskGraphInputBuilder::new();
    inputs
        .append_task_input(&[0u8; 4])
        .expect("task input appending should succeed");
    (graph, inputs.build())
}

/// Builds a job of `num_tasks` independent tasks under `execution_policy`, each taking and
/// returning one byte-typed value, with a cleanup task if `with_cleanup`.
///
/// # Returns
///
/// The task graph and its inputs.
///
/// # Panics
///
/// Panics if the graph fails to build.
#[must_use]
pub fn build_independent_job(
    num_tasks: usize,
    execution_policy: ExecutionPolicy,
    with_cleanup: bool,
) -> (SubmittedTaskGraph, TaskGraphInput) {
    JobShape {
        num_tasks,
        with_cleanup,
        ..JobShape::ONE_TASK
    }
    .build_with_execution_policy(execution_policy)
}

/// # Returns
///
/// A task output value that is unique for `tag` and `index`.
///
/// # Panics
///
/// Panics if `index` doesn't fit in 16 bits.
#[must_use]
pub fn value(tag: u8, index: usize) -> Vec<u8> {
    let index = u16::try_from(index).expect("test indices should fit in 16 bits");
    let [low, high] = index.to_le_bytes();
    vec![tag, low, high, 0x5a]
}

/// # Returns
///
/// The outputs that the storage service stores for the one-output tasks that reported `values`, in
/// order.
///
/// # Type Parameters
///
/// * `'value_lifetime` - The lifetime of the borrowed values.
///
/// # Panics
///
/// Panics if the outputs fail to deserialize.
#[must_use]
pub fn task_outputs<'value_lifetime>(
    values: impl IntoIterator<Item = &'value_lifetime Vec<u8>>,
) -> Vec<TaskOutput> {
    values
        .into_iter()
        .flat_map(|value| {
            TaskOutputsSerializer::deserialize(&serialized_output(value))
                .expect("output deserialization should succeed")
        })
        .collect()
}

/// Waits until `hook` has fired `num_hits` times.
///
/// # Panics
///
/// Panics if the hook has not fired that often within [`HOOK_TIMEOUT`].
pub async fn reached(hook: &ArmedHook, num_hits: usize) {
    hook.reached(num_hits, HOOK_TIMEOUT)
        .await
        .expect("the hook should be reached within the timeout");
}

/// Waits for the spawned task of `handle`.
///
/// # Type Parameters
///
/// * `OutputType` - The type of the task's output.
///
/// # Returns
///
/// The task's output.
///
/// # Panics
///
/// Panics if the task doesn't finish within [`HOOK_TIMEOUT`], or if it panics.
pub async fn join<OutputType>(mut handle: JoinHandle<OutputType>) -> OutputType {
    tokio::time::timeout(HOOK_TIMEOUT, &mut handle)
        .await
        .expect("the spawned task should finish within the timeout")
        .expect("the spawned task should not panic")
}

/// Asserts that `result` is the job state `expected`.
pub fn assert_job_result(result: &JobResult, expected: JobState, what: &str) {
    assert!(
        matches!(result, Ok(state) if *state == expected),
        "{what}: expected `Ok({expected:?})`, got {result:?}"
    );
}

/// Asserts that `result` is the stale state error `expected`.
///
/// # Type Parameters
///
/// * `ValueType` - The type of `result`'s success value.
pub fn assert_stale<ValueType: std::fmt::Debug>(
    result: &Result<ValueType, StorageServerError>,
    expected: &StaleStateError,
    what: &str,
) {
    assert!(
        matches!(
            result,
            Err(StorageServerError::Cache(CacheError::StaleState(error)))
                if format!("{error:?}") == format!("{expected:?}")
        ),
        "{what}: expected the stale error {expected:?}, got {result:?}"
    );
}

/// # Returns
///
/// The serialized outputs of a task whose only output is `value`.
///
/// # Panics
///
/// Panics if the outputs fail to serialize.
fn serialized_output(value: &[u8]) -> Vec<u8> {
    TaskOutputsSerializer::from_tuple(&(value.to_vec(),))
        .expect("output serialization should succeed")
}

/// # Returns
///
/// The states of the rows of the tree rooted at `root_id` in the database of `pool`, by job.
///
/// # Panics
///
/// Panics if the query fails.
async fn tree_rows(pool: &MySqlPool, root_id: JobId) -> HashMap<JobId, JobState> {
    sqlx::query_as::<_, (JobId, JobState)>(
        "SELECT `id`, `state` FROM `jobs` WHERE `id` = ? OR `root_id` = ?;",
    )
    .bind(root_id)
    .bind(root_id)
    .fetch_all(pool)
    .await
    .expect("the tree query should succeed")
    .into_iter()
    .collect()
}
