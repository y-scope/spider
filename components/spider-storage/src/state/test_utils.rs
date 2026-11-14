use std::collections::VecDeque;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use rand::Rng;
use rand::SeedableRng;
use spider_core::job::JobState;
use spider_core::task::DataTypeDescriptor;
use spider_core::task::ExecutionPolicy;
use spider_core::task::TaskDescriptor;
use spider_core::task::TaskGraph as SubmittedTaskGraph;
use spider_core::task::TdlContext;
use spider_core::task::TerminationTaskDescriptor;
use spider_core::task::ValueTypeDescriptor;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SchedulerId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::io::TaskGraphInput;
use spider_core::types::io::TaskGraphInputBuilder;
use spider_core::types::io::TaskOutput;
use spider_core::types::resource_group::ExternalResourceGroupCredentials;
use spider_core::types::scheduler::RegisteredScheduler;
use spider_utils::shutdown::ShutdownHandle;

use crate::cache::error::InternalError;
use crate::cache::job::SharedJobControlBlock;
use crate::cache::task::PreparedChild;
use crate::cache::task::SharedTaskControlBlock;
use crate::cache::task::SharedTerminationTaskControlBlock;
use crate::cache::tree::EventReceivers;
use crate::cache::tree::TreeControlBlock;
use crate::db::DbError;
use crate::db::DbStorage;
use crate::db::ExecutionManagerLivenessManagement;
use crate::db::ExternalJobOrchestration;
use crate::db::InternalJobOrchestration;
use crate::db::RecoverableJobContext;
use crate::db::ResourceGroupManagement;
use crate::db::SchedulerRegistrationManagement;
use crate::db::SessionManagement;
use crate::db::TreeTransition;
use crate::inbound_queue::InboundQueueSender;
use crate::job_submission::CompressedJobSubmission;
use crate::job_submission::ValidatedCompressedJobSubmission;
use crate::job_submission::compress_job_inputs;
use crate::job_submission::compress_task_graph;
use crate::job_submission::create_validated_submission;
use crate::task_instance_pool::TaskInstanceMetadata;
use crate::task_instance_pool::TaskInstancePoolConnector;
use crate::test_hooks::TestHooks;

/// The maximum total size of one job's outputs that [`MockDbConnector`] accepts in a tree
/// transition.
pub const MOCK_MAX_OUTPUTS_SIZE: u64 = 1024 * 1024;

/// The maximum payload size of one child insertion that [`MockDbConnector`] accepts in a tree
/// transition.
pub const MOCK_MAX_CHILD_SIZE: u64 = 64 * 1024;

/// The maximum statement payload that [`MockDbConnector`] reports.
pub const MOCK_MAX_STATEMENT_PAYLOAD: u64 = 4 * 1024 * 1024;

/// The first job ID that [`MockDbConnector`] allocates, above the small fixed IDs that tests use.
pub const MOCK_FIRST_JOB_ID: u64 = 1_000_000;

pub type TestJcb = SharedJobControlBlock<MockInboundQueueSender, MockTaskInstancePoolConnector>;

/// A mock inbound queue sender for testing.
#[derive(Clone, Default)]
pub struct MockInboundQueueSender;

#[async_trait::async_trait]
impl InboundQueueSender for MockInboundQueueSender {
    async fn send_task_ready(
        &self,
        _rg_id: ResourceGroupId,
        _job_id: JobId,
        _task_indices: Vec<usize>,
    ) -> Result<(), InternalError> {
        Ok(())
    }

    async fn send_commit_ready(
        &self,
        _rg_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<(), InternalError> {
        Ok(())
    }

    async fn send_cleanup_ready(
        &self,
        _rg_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<(), InternalError> {
        Ok(())
    }
}

/// A mock DB connector for testing that implements [`DbStorage`].
#[derive(Clone)]
pub struct MockDbConnector {
    pub states: Arc<DashMap<JobId, JobState>>,
    pub errors: Arc<DashMap<JobId, String>>,
    pub outputs: Arc<DashMap<JobId, Vec<TaskOutput>>>,

    /// The parent of every inserted child job.
    pub parents: Arc<DashMap<JobId, JobId>>,

    pub next_job_id: Arc<AtomicU64>,

    /// The errors that the next tree transitions return, in order, before they check or write
    /// anything.
    pub injected_transition_errors: Arc<Mutex<VecDeque<DbError>>>,

    pub resource_groups: Arc<DashMap<ResourceGroupId, ExternalResourceGroupCredentials>>,
    pub resource_group_ids: Arc<DashMap<String, ResourceGroupId>>,
    pub next_resource_group_id: Arc<AtomicUsize>,
    pub execution_managers: Arc<DashMap<ExecutionManagerId, (IpAddr, Option<ResourceGroupId>)>>,
    pub next_execution_manager_id: Arc<AtomicUsize>,
    pub next_scheduler_id: Arc<AtomicUsize>,
    pub session_id: SessionId,
}

impl MockDbConnector {
    /// # Returns
    ///
    /// A job ID larger than every job ID allocated before.
    fn allocate_job_id(&self) -> JobId {
        JobId::from(self.next_job_id.fetch_add(1, Ordering::Relaxed))
    }
}

impl Default for MockDbConnector {
    fn default() -> Self {
        Self {
            states: Arc::new(DashMap::new()),
            errors: Arc::new(DashMap::new()),
            outputs: Arc::new(DashMap::new()),
            parents: Arc::new(DashMap::new()),
            next_job_id: Arc::new(AtomicU64::new(MOCK_FIRST_JOB_ID)),
            injected_transition_errors: Arc::new(Mutex::new(VecDeque::new())),
            resource_groups: Arc::new(DashMap::new()),
            resource_group_ids: Arc::new(DashMap::new()),
            next_resource_group_id: Arc::new(AtomicUsize::new(1)),
            execution_managers: Arc::new(DashMap::new()),
            next_execution_manager_id: Arc::new(AtomicUsize::new(1)),
            next_scheduler_id: Arc::new(AtomicUsize::new(1)),
            session_id: 0,
        }
    }
}

#[async_trait::async_trait]
impl ExternalJobOrchestration for MockDbConnector {
    async fn register(
        &self,
        _resource_group_id: ResourceGroupId,
        _compressed_job_submission: &ValidatedCompressedJobSubmission,
    ) -> Result<JobId, DbError> {
        let job_id = self.allocate_job_id();
        self.states.insert(job_id, JobState::Ready);
        Ok(job_id)
    }

    async fn get_state(&self, job_id: JobId) -> Result<JobState, DbError> {
        self.states
            .get(&job_id)
            .map(|v| *v)
            .ok_or(DbError::JobNotFound(job_id))
    }

    async fn get_outputs(&self, job_id: JobId) -> Result<Vec<TaskOutput>, DbError> {
        self.outputs
            .get(&job_id)
            .map(|v| v.clone())
            .ok_or(DbError::JobNotFound(job_id))
    }

    async fn get_error(&self, job_id: JobId) -> Result<String, DbError> {
        self.errors
            .get(&job_id)
            .map(|v| v.clone())
            .ok_or(DbError::JobNotFound(job_id))
    }
}

#[async_trait::async_trait]
impl InternalJobOrchestration for MockDbConnector {
    async fn apply_tree_transition(
        &self,
        transition: TreeTransition<'_>,
    ) -> Result<Vec<JobId>, DbError> {
        let injected_error = self
            .injected_transition_errors
            .lock()
            .expect("the injected transition errors' mutex should not be poisoned")
            .pop_front();
        if let Some(error) = injected_error {
            return Err(error);
        }
        for update in &transition.updates {
            let size: u64 = update
                .outputs
                .iter()
                .flatten()
                .map(|output| output.len() as u64)
                .sum();
            if size > MOCK_MAX_OUTPUTS_SIZE {
                return Err(DbError::OutputsTooLarge {
                    job_id: update.job_id,
                    size,
                    limit: MOCK_MAX_OUTPUTS_SIZE,
                });
            }
        }
        if let Some(insertion) = transition
            .child_insertions
            .iter()
            .find(|insertion| insertion.payload_size() > MOCK_MAX_CHILD_SIZE)
        {
            return Err(DbError::ChildTooLarge {
                parent_id: insertion.parent_id,
                size: insertion.payload_size(),
                limit: MOCK_MAX_CHILD_SIZE,
            });
        }
        for update in &transition.updates {
            let current = self.states.get(&update.job_id).map(|state| *state);
            if current != Some(update.from) {
                return Err(DbError::CorruptedDbState(format!(
                    "expected job {:?} in state {}, found {current:?}",
                    update.job_id, update.from
                )));
            }
        }
        for update in transition.updates {
            self.states.insert(update.job_id, update.to);
            if let Some(outputs) = update.outputs {
                self.outputs.insert(update.job_id, outputs);
            }
            if let Some(error_message) = update.error_message {
                self.errors.insert(update.job_id, error_message);
            }
        }
        Ok(transition
            .child_insertions
            .iter()
            .map(|insertion| {
                let child_id = self.allocate_job_id();
                self.states.insert(child_id, JobState::Running);
                self.parents.insert(child_id, insertion.parent_id);
                child_id
            })
            .collect())
    }

    fn max_statement_payload(&self) -> u64 {
        MOCK_MAX_STATEMENT_PAYLOAD
    }

    async fn delete_expired_terminated_jobs(
        &self,
        _expire_after_sec: u64,
    ) -> Result<Vec<JobId>, DbError> {
        Ok(Vec::new())
    }

    async fn get_recoverable_jobs(&self) -> Result<Vec<RecoverableJobContext>, DbError> {
        Ok(Vec::new())
    }
}

#[async_trait::async_trait]
impl ResourceGroupManagement for MockDbConnector {
    async fn add(
        &self,
        credentials: ExternalResourceGroupCredentials,
    ) -> Result<ResourceGroupId, DbError> {
        let counter = self.next_resource_group_id.fetch_add(1, Ordering::Relaxed);
        let id = ResourceGroupId::from(counter as u64);
        let external_resource_group_id = credentials.get_external_resource_group_id().to_owned();
        self.resource_groups.insert(id, credentials);
        self.resource_group_ids
            .insert(external_resource_group_id, id);
        Ok(id)
    }

    async fn add_or_verify(
        &self,
        credentials: ExternalResourceGroupCredentials,
    ) -> Result<ResourceGroupId, DbError> {
        match self
            .resource_group_ids
            .entry(credentials.get_external_resource_group_id().to_owned())
        {
            Entry::Occupied(entry) => {
                let id = *entry.get();
                let stored = self
                    .resource_groups
                    .get(&id)
                    .expect("indexed resource group must exist in the mock");
                let matches = stored.get_password() == credentials.get_password();
                drop(stored);
                if !matches {
                    return Err(DbError::InvalidPassword(id));
                }
                Ok(id)
            }
            Entry::Vacant(entry) => {
                let counter = self.next_resource_group_id.fetch_add(1, Ordering::Relaxed);
                let id = ResourceGroupId::from(counter as u64);
                self.resource_groups.insert(id, credentials);
                entry.insert(id);
                Ok(id)
            }
        }
    }

    async fn verify(
        &self,
        resource_group_id: ResourceGroupId,
        password: &[u8],
    ) -> Result<(), DbError> {
        let stored = self
            .resource_groups
            .get(&resource_group_id)
            .ok_or(DbError::ResourceGroupNotFound(resource_group_id))?;
        let matches = stored.get_password() == password;
        drop(stored);
        if !matches {
            return Err(DbError::InvalidPassword(resource_group_id));
        }
        Ok(())
    }

    async fn delete(&self, resource_group_id: ResourceGroupId) -> Result<(), DbError> {
        self.resource_groups
            .remove(&resource_group_id)
            .ok_or(DbError::ResourceGroupNotFound(resource_group_id))?;
        self.resource_group_ids
            .retain(|_, stored_resource_group_id| *stored_resource_group_id != resource_group_id);
        Ok(())
    }
}

#[async_trait::async_trait]
impl ExecutionManagerLivenessManagement for MockDbConnector {
    async fn register_execution_manager(
        &self,
        ip_address: IpAddr,
        resource_group_credentials: Option<ExternalResourceGroupCredentials>,
    ) -> Result<(ExecutionManagerId, Option<ResourceGroupId>), DbError> {
        let resource_group_id = match resource_group_credentials {
            Some(credentials) => {
                let resource_group_id = self
                    .resource_group_ids
                    .get(credentials.get_external_resource_group_id())
                    .map(|entry| *entry.value())
                    .ok_or_else(|| {
                        DbError::ExternalResourceGroupNotFound(
                            credentials.get_external_resource_group_id().to_owned(),
                        )
                    })?;
                ResourceGroupManagement::verify(
                    self,
                    resource_group_id,
                    credentials.get_password(),
                )
                .await?;
                Some(resource_group_id)
            }
            None => None,
        };
        let counter = self
            .next_execution_manager_id
            .fetch_add(1, Ordering::Relaxed);
        let id = ExecutionManagerId::from(counter as u64);
        self.execution_managers
            .insert(id, (ip_address, resource_group_id));
        Ok((id, resource_group_id))
    }

    async fn update_execution_manager_heartbeat(
        &self,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<(), DbError> {
        if self.execution_managers.contains_key(&execution_manager_id) {
            Ok(())
        } else {
            Err(DbError::IllegalExecutionManagerId(execution_manager_id))
        }
    }

    async fn is_execution_manager_alive(
        &self,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<bool, DbError> {
        Ok(self.execution_managers.contains_key(&execution_manager_id))
    }

    async fn get_dead_execution_managers(
        &self,
        _stale_after_sec: u64,
    ) -> Result<Vec<ExecutionManagerId>, DbError> {
        Ok(Vec::new())
    }
}

#[async_trait::async_trait]
impl SchedulerRegistrationManagement for MockDbConnector {
    async fn register_scheduler(&self, _host: &str, _port: u16) -> Result<SchedulerId, DbError> {
        let counter = self.next_scheduler_id.fetch_add(1, Ordering::Relaxed);
        Ok(SchedulerId::from(counter as u64))
    }

    async fn get_schedulers(&self) -> Result<Vec<RegisteredScheduler>, DbError> {
        unreachable!("not implemented for mock connector")
    }

    async fn is_scheduler_registered(&self, _scheduler_id: SchedulerId) -> Result<bool, DbError> {
        unreachable!("not implemented for mock connector")
    }
}

impl SessionManagement for MockDbConnector {
    fn session_id(&self) -> SessionId {
        self.session_id
    }
}

impl DbStorage for MockDbConnector {}

/// A mock task instance pool connector for testing.
#[derive(Clone, Default)]
pub struct MockTaskInstancePoolConnector;

#[async_trait::async_trait]
impl TaskInstancePoolConnector for MockTaskInstancePoolConnector {
    fn get_next_available_task_instance_id(&self) -> TaskInstanceId {
        1
    }

    async fn register_task_instance(
        &self,
        _tcb: SharedTaskControlBlock,
        _registration: TaskInstanceMetadata,
    ) -> Result<(), InternalError> {
        Ok(())
    }

    async fn register_termination_task_instance(
        &self,
        _termination_tcb: SharedTerminationTaskControlBlock,
        _registration: TaskInstanceMetadata,
    ) -> Result<(), InternalError> {
        Ok(())
    }
}

pub fn create_test_task_graph() -> SubmittedTaskGraph {
    create_test_task_graph_with_termination_tasks(None, None)
}

pub fn create_test_task_graph_with_termination_tasks(
    commit_task: Option<TerminationTaskDescriptor>,
    cleanup_task: Option<TerminationTaskDescriptor>,
) -> SubmittedTaskGraph {
    let mut task_graph = SubmittedTaskGraph::new(commit_task, cleanup_task)
        .expect("task graph creation should succeed");
    task_graph
        .insert_task(create_test_task_descriptor())
        .expect("task insertion should succeed");
    task_graph
}

/// # Returns
///
/// A task graph of `num_tasks` independent tasks, each like [`create_test_task_graph`]'s only task.
///
/// # Panics
///
/// Panics if the task graph creation or a task insertion fails.
pub fn create_test_task_graph_with_independent_tasks(num_tasks: usize) -> SubmittedTaskGraph {
    let mut task_graph =
        SubmittedTaskGraph::new(None, None).expect("task graph creation should succeed");
    for _ in 0..num_tasks {
        task_graph
            .insert_task(create_test_task_descriptor())
            .expect("task insertion should succeed");
    }
    task_graph
}

/// # Returns
///
/// A task graph input whose only positional input is the msgpack serialization of a 4-byte zero
/// array.
pub fn create_test_task_graph_input() -> TaskGraphInput {
    create_test_task_graph_input_with_num_inputs(1)
}

/// # Returns
///
/// A task graph input of `num_inputs` positional inputs, each the msgpack serialization of a
/// 4-byte zero array.
///
/// # Panics
///
/// Panics if the input serialization fails.
pub fn create_test_task_graph_input_with_num_inputs(num_inputs: usize) -> TaskGraphInput {
    let mut builder = TaskGraphInputBuilder::new();
    for _ in 0..num_inputs {
        builder
            .append_task_input(&[0u8; 4])
            .expect("task input appending should succeed");
    }
    builder.build()
}

/// # Returns
///
/// The submission of a child job with [`create_test_task_graph`]'s task graph, whose only
/// positional input is the msgpack serialization of a 4-byte array of `input_byte`.
///
/// # Panics
///
/// Panics if the input serialization or the compression fails.
pub fn create_test_child_submission(input_byte: u8) -> CompressedJobSubmission {
    let mut builder = TaskGraphInputBuilder::new();
    builder
        .append_task_input(&[input_byte; 4])
        .expect("task input appending should succeed");
    CompressedJobSubmission {
        compressed_serialized_task_graph: compress_task_graph(&create_test_task_graph()),
        compressed_serialized_job_inputs: compress_job_inputs(&builder.build()),
    }
}

/// # Returns
///
/// The submission of a child job with [`create_test_task_graph_with_independent_tasks`]'s task
/// graph of `num_tasks` tasks and [`create_test_task_graph_input_with_num_inputs`]'s inputs.
///
/// # Panics
///
/// Panics if the compression fails.
pub fn create_test_child_submission_with_independent_tasks(
    num_tasks: usize,
) -> CompressedJobSubmission {
    CompressedJobSubmission {
        compressed_serialized_task_graph: compress_task_graph(
            &create_test_task_graph_with_independent_tasks(num_tasks),
        ),
        compressed_serialized_job_inputs: compress_job_inputs(
            &create_test_task_graph_input_with_num_inputs(num_tasks),
        ),
    }
}

/// # Returns
///
/// The prepared child of [`create_test_child_submission`]'s submission for `input_byte`.
///
/// # Panics
///
/// Panics if the child preparation fails.
pub async fn create_test_prepared_child(input_byte: u8) -> PreparedChild {
    PreparedChild::create(create_test_child_submission(input_byte))
        .await
        .expect("child preparation should succeed")
}

/// # Returns
///
/// A prepared child with [`create_test_task_graph`]'s task graph whose only positional input is
/// `input_size` pseudo-random bytes, so that its payload exceeds `input_size`.
///
/// # Panics
///
/// Panics if:
///
/// * `input_size` does not fit in `usize`.
/// * The input serialization, the compression or the child preparation fails.
/// * The child's payload does not exceed `input_size`.
pub async fn create_test_incompressible_prepared_child(input_size: u64) -> PreparedChild {
    let mut input = vec![0u8; usize::try_from(input_size).expect("the size should fit in `usize`")];
    rand::rngs::StdRng::seed_from_u64(0).fill(input.as_mut_slice());
    let mut builder = TaskGraphInputBuilder::new();
    builder
        .append_task_input(&input)
        .expect("task input appending should succeed");
    let child = PreparedChild::create(CompressedJobSubmission {
        compressed_serialized_task_graph: compress_task_graph(&create_test_task_graph()),
        compressed_serialized_job_inputs: compress_job_inputs(&builder.build()),
    })
    .await
    .expect("child preparation should succeed");
    assert!(
        child.submission().size() > input_size,
        "the child's payload should not compress below its input size"
    );
    child
}

/// # Returns
///
/// The prepared children of [`create_test_prepared_child`] for the input bytes `0` to
/// `num_children - 1`, in order.
pub async fn create_test_prepared_children(num_children: u8) -> Vec<PreparedChild> {
    let mut children = Vec::with_capacity(usize::from(num_children));
    for input_byte in 0..num_children {
        children.push(create_test_prepared_child(input_byte).await);
    }
    children
}

pub async fn create_test_jcb(job_id: JobId) -> (TestJcb, EventReceivers) {
    create_test_jcb_with_independent_tasks(job_id, 1).await
}

/// # Returns
///
/// A JCB of `job_id` whose task graph is [`create_test_task_graph_with_independent_tasks`]'s graph
/// of `num_tasks` tasks, with the receivers of its tree's events.
///
/// # Panics
///
/// Panics if the JCB creation fails.
pub async fn create_test_jcb_with_independent_tasks(
    job_id: JobId,
    num_tasks: usize,
) -> (TestJcb, EventReceivers) {
    let job_submission = create_validated_submission(
        create_test_task_graph_with_independent_tasks(num_tasks),
        create_test_task_graph_input_with_num_inputs(num_tasks),
    );
    let (tree, receivers) = TreeControlBlock::new(ShutdownHandle::new());

    let jcb = SharedJobControlBlock::create(
        job_id,
        ResourceGroupId::random(),
        job_submission,
        MockInboundQueueSender,
        MockTaskInstancePoolConnector,
        TestHooks::default(),
        tree,
    )
    .await
    .expect("JCB creation should succeed");
    (jcb, receivers)
}

/// # Returns
///
/// The descriptor of a test task with one bytes input and one bytes output, and no input sources.
fn create_test_task_descriptor() -> TaskDescriptor {
    let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
    TaskDescriptor {
        tdl_context: TdlContext {
            package: "test_pkg".to_owned(),
            task_func: "test_fn".to_owned(),
        },
        execution_policy: Some(ExecutionPolicy::default()),
        inputs: vec![bytes_type.clone()],
        outputs: vec![bytes_type],
        input_sources: None,
    }
}
