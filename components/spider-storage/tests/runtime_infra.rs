//! An in-process storage runtime on its own isolated test database, with test hooks.

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
use spider_core::types::io::TaskGraphInput;
use spider_core::types::io::TaskGraphInputBuilder;
use spider_core::types::resource_group::ExternalResourceGroupCredentials;
use spider_storage::db::MariaDbStorageConnector;
use spider_storage::inbound_queue::InboundQueueSenderHandle;
use spider_storage::state::JobCacheGcConfig;
use spider_storage::state::Runtime;
use spider_storage::state::ServiceState;
use spider_storage::state::StorageServerError;
use spider_storage::state::create_runtime_with_hooks;
use spider_storage::state::runtime::RuntimeConfig;
use spider_storage::task_instance_pool::TaskInstancePoolConfig;
use spider_storage::task_instance_pool::TaskInstancePoolHandle;
use spider_storage::test_hooks::HookAction;
use spider_storage::test_hooks::HookCount;
use spider_storage::test_hooks::HookFilter;
use spider_storage::test_hooks::HookPoint;
use spider_storage::test_hooks::TestHooks;
use sqlx::MySqlPool;

use crate::mariadb_infra::IsolatedTestDb;
use crate::mariadb_infra::create_raw_pool;
use crate::task_graph_builder::SubmittedTaskGraph;
use crate::task_graph_builder::compress_job_inputs;
use crate::task_graph_builder::compress_task_graph;

/// How long a test waits for a request to reach a paused hook, or for a request that must not wait
/// on a paused hook to return.
pub const HOOK_TIMEOUT: Duration = Duration::from_secs(10);

/// The runtime type that [`TestEnv`] runs.
pub type TestRuntime =
    Runtime<InboundQueueSenderHandle, MariaDbStorageConnector, TaskInstancePoolHandle>;

/// The service type of a [`TestRuntime`].
pub type TestService =
    ServiceState<InboundQueueSenderHandle, MariaDbStorageConnector, TaskInstancePoolHandle>;

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
    let execution_policy = Some(execution_policy);
    let cleanup_task = with_cleanup.then(|| TerminationTaskDescriptor {
        tdl_context: TdlContext {
            package: "test".to_owned(),
            task_func: "cleanup".to_owned(),
        },
        execution_policy: execution_policy.clone(),
    });
    let mut graph =
        SubmittedTaskGraph::new(None, cleanup_task).expect("task graph creation should succeed");
    let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
    let mut inputs = TaskGraphInputBuilder::new();
    for task_index in 0..num_tasks {
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
