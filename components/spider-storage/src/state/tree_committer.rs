//! The per-tree committer: one long-running task per job tree that is the tree's only writer.
//!
//! The committer receives the tree's job-level events, plans each round of them, writes the round
//! to the database, applies it to the cached jobs, and replies to every event.

use std::collections::HashMap;
use std::collections::VecDeque;

use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_utils::shutdown::ShutdownHandle;
use tokio_util::task::TaskTracker;

use crate::cache::error::CacheError;
use crate::cache::error::InternalError;
use crate::cache::job::SharedJobControlBlock;
use crate::cache::tree::EventReceivers;
use crate::cache::tree::Lane;
use crate::cache::tree::Pending;
use crate::cache::tree::Plan;
use crate::cache::tree::PlannerInput;
use crate::cache::tree::Publication;
use crate::cache::tree::TreeControlBlock;
use crate::cache::tree::TreeEventKind;
use crate::cache::tree::TreeNode;
use crate::cache::tree::TreeOutcome;
use crate::cache::tree::TreeReply;
use crate::cache::tree::TreeStructure;
use crate::cache::tree::plan_event;
use crate::db::DbError;
use crate::db::InternalJobOrchestration;
use crate::db::JobStateUpdate;
use crate::db::RecoverableJobContext;
use crate::db::TreeTransition;
use crate::inbound_queue::InboundQueueSender;
use crate::job_submission::ValidatedJobSubmission;
use crate::state::JobCache;
use crate::state::JobCacheGcHandle;
use crate::state::StorageServerError;
use crate::task_instance_pool::TaskInstancePoolConnector;
use crate::test_hooks::HookContext;
use crate::test_hooks::HookPoint;
use crate::test_hooks::TestHooks;

/// Everything a tree's committer uses besides its tree.
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender that publishes ready work.
/// * `DbConnectorType` - The type of the DB-layer connector.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
#[derive(Clone)]
pub struct TreeEnv<
    InboundQueueSenderType: InboundQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    pub db: DbConnectorType,
    pub job_cache: JobCache<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    pub job_cache_gc_handle: JobCacheGcHandle,
    pub inbound_queue_sender: InboundQueueSenderType,
    pub task_instance_pool_connector: TaskInstancePoolConnectorType,
    pub shutdown_handle: ShutdownHandle,
    pub task_tracker: TaskTracker,
    pub hooks: TestHooks,
}

/// Creates a new root job's tree: its control block, its cached job, and its committer.
///
/// # Returns
///
/// The root's control block on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`SharedJobControlBlock::create`]'s return values on failure.
/// * Forwards [`JobCache::insert`]'s return values on failure.
pub async fn create_root_tree<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: InternalJobOrchestration + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
>(
    env: &TreeEnv<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    job_id: JobId,
    owner_id: ResourceGroupId,
    job_submission: ValidatedJobSubmission,
) -> Result<
    SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    StorageServerError,
> {
    let (tree, receivers) = new_tree_control_block(env, job_id);
    let jcb = SharedJobControlBlock::create(
        job_id,
        owner_id,
        job_submission,
        env.inbound_queue_sender.clone(),
        env.task_instance_pool_connector.clone(),
        tree,
    )
    .await?;
    env.job_cache.insert(jcb.clone()).await?;
    spawn_committer(env, CommitterState::new_root(jcb.clone()), receivers);
    Ok(jcb)
}

/// Recovers the tree of one job loaded from the database: the job gets a control block as the root
/// of its own tree, is cached, and gets its tree's committer.
///
/// # Returns
///
/// The recovered job's control block on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`SharedJobControlBlock::recover`]'s return values on failure.
/// * Forwards [`JobCache::insert_many`]'s return values on failure.
pub async fn create_recovered_tree<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: InternalJobOrchestration + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
>(
    env: &TreeEnv<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    recoverable_job_context: RecoverableJobContext,
) -> Result<
    SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    StorageServerError,
> {
    let (tree, receivers) = new_tree_control_block(env, recoverable_job_context.id);
    let jcb = SharedJobControlBlock::recover(
        recoverable_job_context,
        env.inbound_queue_sender.clone(),
        env.task_instance_pool_connector.clone(),
        tree,
    )
    .await?;
    env.job_cache.insert_many(vec![jcb.clone()]).await?;
    spawn_committer(env, CommitterState::new_root(jcb.clone()), receivers);
    Ok(jcb)
}

/// The state that a tree's committer owns: the tree's structure and its non-terminal jobs.
struct CommitterState<
    InboundQueueSenderType: InboundQueueSender,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> {
    structure: TreeStructure,
    jcbs: HashMap<
        JobId,
        SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    >,
}

impl<
    InboundQueueSenderType: InboundQueueSender,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
> CommitterState<InboundQueueSenderType, TaskInstancePoolConnectorType>
{
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The state of the one-job tree rooted at `root`.
    fn new_root(
        root: SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    ) -> Self {
        let root_id = root.id();
        let structure = TreeStructure::new_root(
            root_id,
            TreeNode {
                has_commit: root.has_commit_task(),
                has_cleanup: root.has_cleanup_task(),
            },
        );
        Self {
            structure,
            jcbs: HashMap::from([(root_id, root)]),
        }
    }

    /// Drops the jobs that an applied plan made terminal from the tree.
    fn update_structure(&mut self, plan: &Plan) {
        for job_id in &plan.terminated {
            self.structure.remove(*job_id);
            self.jcbs.remove(job_id);
        }
    }

    /// # Returns
    ///
    /// The control block of the tree's non-terminal job `job_id` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError::JobNotFound`] if the job is not a non-terminal job of the tree.
    fn jcb(
        &self,
        job_id: JobId,
    ) -> Result<
        &SharedJobControlBlock<InboundQueueSenderType, TaskInstancePoolConnectorType>,
        CacheError,
    > {
        self.jcbs
            .get(&job_id)
            .ok_or_else(|| InternalError::JobNotFound(job_id).into())
    }
}

/// An event in the committer's buffers, with its input once collected.
struct Queued {
    pending: Pending,
    input: Option<PlannerInput>,
}

impl Queued {
    const fn new(pending: Pending) -> Self {
        Self {
            pending,
            input: None,
        }
    }
}

/// The committer's buffers of received events, one per event channel. Each holds its requeued
/// events first, then its arrivals.
#[derive(Default)]
struct Buffers {
    /// The `CancelRoot` and `TerminalFailure` events.
    fast_forward: VecDeque<Queued>,

    /// Every other event.
    regular: VecDeque<Queued>,
}

impl Buffers {
    /// # Returns
    ///
    /// Whether neither buffer holds an event.
    fn is_empty(&self) -> bool {
        self.fast_forward.is_empty() && self.regular.is_empty()
    }

    /// # Returns
    ///
    /// The buffered events: the fast-forward buffer's, then the regular buffer's.
    fn iter(&self) -> impl Iterator<Item = &Queued> {
        self.fast_forward.iter().chain(&self.regular)
    }

    /// # Returns
    ///
    /// The buffer of the events of `kind`.
    const fn buffer_of(&mut self, kind: &TreeEventKind) -> &mut VecDeque<Queued> {
        if kind.is_fast_forward() {
            &mut self.fast_forward
        } else {
            &mut self.regular
        }
    }

    /// Takes the event of the next round: the oldest fast-forward event, or else the oldest regular
    /// event.
    ///
    /// # Returns
    ///
    /// The event, or [`None`] if both buffers are empty.
    fn take(&mut self) -> Option<Queued> {
        self.fast_forward
            .pop_front()
            .or_else(|| self.regular.pop_front())
    }

    /// Puts `queued` back at the front of the buffer of its kind.
    fn requeue(&mut self, queued: Queued) {
        self.buffer_of(&queued.pending.event.kind)
            .push_front(queued);
    }
}

/// Owns the tree's event receivers. Unless disarmed, dropping it marks the service's shutdown as
/// fatal, so that every exit of the committer other than a quiescent one or a shutdown poisons the
/// service.
struct PoisonOnAbnormalExit {
    receivers: EventReceivers,
    shutdown_handle: ShutdownHandle,
    root_id: JobId,
    disarmed: bool,
}

impl PoisonOnAbnormalExit {
    /// Disarms the guard, then marks the service's shutdown as fatal.
    fn poison(&mut self) {
        self.disarmed = true;
        tracing::error!(
            root_id = ? self.root_id,
            "Tree committer exited abnormally. Shutting down the service."
        );
        self.shutdown_handle.shut_down_fatally();
    }
}

impl Drop for PoisonOnAbnormalExit {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        if !std::thread::panicking() && self.shutdown_handle.is_cancelled() {
            tracing::debug!(
                root_id = ? self.root_id,
                "Tree committer dropped during the runtime's teardown."
            );
            return;
        }
        self.poison();
    }
}

/// # Returns
///
/// A new tree control block for the tree rooted at `root_id`, and its event receivers.
fn new_tree_control_block<
    InboundQueueSenderType: InboundQueueSender,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    env: &TreeEnv<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    root_id: JobId,
) -> (TreeControlBlock, EventReceivers) {
    TreeControlBlock::new(root_id, env.shutdown_handle.clone(), env.hooks.clone())
}

/// Spawns the committer of the tree whose committer state is `state` into the environment's
/// task tracker.
fn spawn_committer<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: InternalJobOrchestration + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
>(
    env: &TreeEnv<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
    state: CommitterState<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    receivers: EventReceivers,
) {
    env.task_tracker
        .spawn(run_committer(state, receivers, env.clone()));
}

/// Runs a tree's committer until the tree is quiescent or the service shuts down, one event per
/// round, the fast-forward events first.
///
/// Any other exit, an error or a panic, poisons the service through [`PoisonOnAbnormalExit`]: the
/// fatal flag is set, the service is shut down, and every waiting event's reply channel is dropped.
async fn run_committer<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: InternalJobOrchestration + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
>(
    mut state: CommitterState<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    receivers: EventReceivers,
    env: TreeEnv<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
) {
    let root_id = state.structure.root_id();
    let shutdown_handle = env.shutdown_handle.clone();
    let mut guard = PoisonOnAbnormalExit {
        receivers,
        shutdown_handle: shutdown_handle.clone(),
        root_id,
        disarmed: false,
    };
    let mut buffers = Buffers::default();
    loop {
        if buffers.is_empty() {
            let arrival = tokio::select! {
                biased;
                pending = guard.receivers.fast_forward.recv() => pending,
                pending = guard.receivers.regular.recv() => pending,
                () = shutdown_handle.cancelled() => None,
            };
            match arrival {
                Some(pending) => buffers
                    .buffer_of(&pending.event.kind)
                    .push_back(Queued::new(pending)),
                None if shutdown_handle.is_cancelled() => {}
                None => {
                    tracing::error!(
                        root_id = ? root_id,
                        "An event channel of a live tree closed."
                    );
                    guard.poison();
                    return;
                }
            }
        }
        receive(&mut guard.receivers, &mut buffers);

        let hit = env
            .hooks
            .hit(HookPoint::CommitterAfterReceive, || {
                HookContext::round(root_id, buffers.iter().map(|queued| &queued.pending.event))
            })
            .await;
        match hit {
            Ok(()) if cfg!(feature = "test-hooks") => {
                receive(&mut guard.receivers, &mut buffers);
            }
            Ok(()) => {}
            Err(fault) if fault.is_stopping() => {}
            Err(fault) => {
                tracing::error!(fault = ? fault, "Injected committer fault.");
                guard.poison();
                return;
            }
        }

        if shutdown_handle.is_cancelled() {
            close_and_answer(&mut guard.receivers, &mut buffers, || {
                Err(CacheError::ServiceStopping)
            });
            break;
        }

        let Some(queued) = buffers.take() else {
            continue;
        };
        match Box::pin(run_tree_state_transaction(&mut state, queued, &env)).await {
            Ok(Some(requeued)) => buffers.requeue(requeued),
            Ok(None) => {}
            Err(e) => {
                tracing::error!(
                    root_id = ? root_id,
                    error = % e,
                    "Tree state transaction failed."
                );
                guard.poison();
                return;
            }
        }

        if state.structure.is_quiescent() {
            close_and_answer(&mut guard.receivers, &mut buffers, || {
                Ok(TreeOutcome::Untracked)
            });
            break;
        }
    }
    guard.disarmed = true;
}

/// Runs one tree state transaction over the event of `queued`: collects its inputs, plans it,
/// writes the plan to the database, applies it to the cached jobs and the tree's structure,
/// enqueues the newly terminal jobs for cache GC, replies to the event, and publishes the ready
/// work.
///
/// # Returns
///
/// On success, the event to put back at the front of its buffer, with its input to be collected
/// again, if the service started shutting down before the database write or if [`fail_culprit`]
/// turns the database's error into a failure of the event's job; [`None`] if the event was
/// answered.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`collect_inputs`]'s return values on failure.
/// * Forwards [`TestHooks::hit`]'s return values on failure.
/// * Forwards [`fail_culprit`]'s return values on failure.
/// * Forwards [`hit_after_commit`]'s return values on failure.
/// * Forwards [`apply_plan`]'s return values on failure.
/// * Forwards [`CommitterState::jcb`]'s return values on failure.
/// * Forwards [`publish`]'s return values on failure.
///
/// # Panics
///
/// Panics if [`plan_round`] panics.
async fn run_tree_state_transaction<
    InboundQueueSenderType: InboundQueueSender + 'static,
    DbConnectorType: InternalJobOrchestration + 'static,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector + 'static,
>(
    state: &mut CommitterState<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    mut queued: Queued,
    env: &TreeEnv<InboundQueueSenderType, DbConnectorType, TaskInstancePoolConnectorType>,
) -> Result<Option<Queued>, StorageServerError> {
    let root_id = state.structure.root_id();
    let collected = collect_inputs(state, &mut queued).await?;
    match env
        .hooks
        .hit(HookPoint::CommitterBeforePlan, || {
            round_context(root_id, &queued)
        })
        .await
    {
        Ok(()) => {}
        Err(fault) if fault.is_stopping() => return Ok(Some(requeue_all(queued))),
        Err(fault) => return Err(CacheError::from(fault).into()),
    }

    let mut plan = plan_round(&state.structure, &collected, &mut queued);

    match env
        .hooks
        .hit(HookPoint::CommitterBeforeDbTransaction, || {
            round_context(root_id, &queued)
        })
        .await
    {
        Ok(()) => {}
        Err(fault) if fault.is_stopping() => return Ok(Some(requeue_all(queued))),
        Err(fault) => return Err(CacheError::from(fault).into()),
    }

    if !plan.is_empty() {
        if let Err(error) = write_plan(std::mem::take(&mut plan.updates), &env.db).await {
            return fail_culprit(queued, error).map(Some);
        }
        hit_after_commit(&env.hooks, HookPoint::CommitterAfterCommit, || {
            round_context(root_id, &queued)
        })
        .await?;
        apply_plan(state, &plan, &env.hooks, || round_context(root_id, &queued)).await?;
    }

    let mut resource_group_ids = HashMap::new();
    for publication in &plan.publications {
        let job_id = publication.job_id();
        resource_group_ids.insert(job_id, state.jcb(job_id)?.owner_id());
    }
    state.update_structure(&plan);
    for job_id in &plan.terminated {
        env.job_cache_gc_handle.enqueue_terminated_job(*job_id);
    }

    reply(queued, plan.outcome);

    for publication in plan.publications {
        let resource_group_id = resource_group_ids[&publication.job_id()];
        publish(&env.inbound_queue_sender, resource_group_id, publication).await?;
    }
    Ok(None)
}

/// Collects the states of the jobs that the planner reads for the event of `queued`, and the
/// event's input if it has none yet. An event whose job is no longer in the tree gets no input.
///
/// # Returns
///
/// The collected job states on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`CommitterState::jcb`]'s return values on failure.
/// * Forwards [`SharedJobControlBlock::collect_ready_task_indices`]'s return values on failure.
/// * Forwards [`SharedJobControlBlock::collect_regular_done_inputs`]'s return values on failure.
async fn collect_inputs<
    InboundQueueSenderType: InboundQueueSender,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    state: &CommitterState<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    queued: &mut Queued,
) -> Result<HashMap<JobId, JobState>, CacheError> {
    let event = &queued.pending.event;
    let job_ids = state.structure.jobs_to_collect(event);
    let mut collected = HashMap::with_capacity(job_ids.len());
    for job_id in job_ids {
        collected.insert(job_id, state.jcb(job_id)?.state().await);
    }

    if queued.input.is_some() {
        return Ok(collected);
    }
    let input = match state.jcbs.get(&event.job_id) {
        None => PlannerInput::Empty,
        Some(jcb) => match event.kind {
            TreeEventKind::Start => jcb
                .collect_ready_task_indices()
                .await?
                .map_or(PlannerInput::Empty, |ready_task_indices| {
                    PlannerInput::Start { ready_task_indices }
                }),
            TreeEventKind::RegularTasksDone => jcb
                .collect_regular_done_inputs()
                .await?
                .map_or(PlannerInput::Empty, |outputs| {
                    PlannerInput::RegularTasksDone { outputs }
                }),
            _ => PlannerInput::Empty,
        },
    };
    queued.input = Some(input);
    Ok(collected)
}

/// Plans the event of `queued` against the tree's `structure` and the `collected` job states,
/// taking the event's input.
///
/// # Returns
///
/// Forwards [`plan_event`]'s return values.
///
/// # Panics
///
/// Panics if the event's input was not collected, or if [`plan_event`] panics.
fn plan_round(
    structure: &TreeStructure,
    collected: &HashMap<JobId, JobState>,
    queued: &mut Queued,
) -> Plan {
    let input = queued
        .input
        .take()
        .expect("the event's input should be collected before planning");
    plan_event(structure, collected, &queued.pending.event, input)
}

/// Writes `updates` to the database in one tree transition.
///
/// # Type Parameters
///
/// * `DbConnectorType` - The type of the DB-layer connector.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`InternalJobOrchestration::apply_tree_transition`]'s return values on failure.
async fn write_plan<DbConnectorType: InternalJobOrchestration>(
    updates: Vec<JobStateUpdate>,
    db: &DbConnectorType,
) -> Result<(), DbError> {
    db.apply_tree_transition(TreeTransition { updates }).await
}

/// Applies `plan`'s state changes to the cached jobs, in its apply order. After each job's apply,
/// `hooks` fire at [`HookPoint::CommitterAfterApplyJob`] with the job in the context that
/// `round_context` builds.
///
/// # Type Parameters
///
/// * `InboundQueueSenderType` - The type of the inbound queue sender of the tree's jobs.
/// * `TaskInstancePoolConnectorType` - The type of the task instance pool connector.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`CommitterState::jcb`]'s return values on failure.
/// * Forwards [`SharedJobControlBlock::apply_tree_state`]'s return values on failure.
/// * Forwards [`hit_after_commit`]'s return values on failure.
async fn apply_plan<
    InboundQueueSenderType: InboundQueueSender,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    state: &CommitterState<InboundQueueSenderType, TaskInstancePoolConnectorType>,
    plan: &Plan,
    hooks: &TestHooks,
    round_context: impl Fn() -> HookContext + Send + Sync,
) -> Result<(), CacheError> {
    for transition in &plan.apply_order {
        state
            .jcb(transition.job_id)?
            .apply_tree_state(transition.from, transition.to)
            .await?;
        hit_after_commit(hooks, HookPoint::CommitterAfterApplyJob, || HookContext {
            job_ids: vec![transition.job_id],
            ..round_context()
        })
        .await?;
    }
    Ok(())
}

/// Answers the event of `queued` with `outcome`.
fn reply(queued: Queued, outcome: TreeOutcome) {
    if let (TreeOutcome::Stale(error), TreeEventKind::TerminalFailure { message, .. }) =
        (&outcome, &queued.pending.event.kind)
    {
        tracing::info!(
            job_id = ? queued.pending.event.job_id,
            error = % error,
            failure = message.as_str(),
            "Discarded a stale task failure."
        );
    }
    // The event's waiter may have dropped its future, which is not an error of the committer.
    let _ = queued.pending.reply.send(Ok(outcome));
}

/// Sends one publication into its lane of the inbound queue through `inbound_queue_sender`.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`InboundQueueSender::send_task_ready`]'s return values on failure.
/// * Forwards [`InboundQueueSender::send_commit_ready`]'s return values on failure.
/// * Forwards [`InboundQueueSender::send_cleanup_ready`]'s return values on failure.
async fn publish<InboundQueueSenderType: InboundQueueSender>(
    inbound_queue_sender: &InboundQueueSenderType,
    resource_group_id: ResourceGroupId,
    publication: Publication,
) -> Result<(), CacheError> {
    match publication {
        Publication::Ready {
            job_id,
            task_indices,
        } => {
            inbound_queue_sender
                .send_task_ready(resource_group_id, job_id, task_indices)
                .await?;
        }
        Publication::Commit(job_id) => {
            inbound_queue_sender
                .send_commit_ready(resource_group_id, job_id)
                .await?;
        }
        Publication::Cleanup(job_id) => {
            inbound_queue_sender
                .send_cleanup_ready(resource_group_id, job_id)
                .await?;
        }
    }
    Ok(())
}

/// Turns a database error that the event of `queued` causes deterministically into a terminal
/// failure of the event's job on the regular lane, which keeps the event's reply channel:
/// [`DbError::OutputsTooLarge`] fails the job whose `RegularTasksDone` event stores the outputs.
///
/// # Returns
///
/// Forwards [`requeue_all`]'s return values on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards `error` if it is not [`DbError::OutputsTooLarge`] of the event's `RegularTasksDone`.
fn fail_culprit(mut queued: Queued, error: DbError) -> Result<Queued, StorageServerError> {
    let is_culprit = matches!(
        &error,
        DbError::OutputsTooLarge { job_id, .. }
            if *job_id == queued.pending.event.job_id
                && queued.pending.event.kind == TreeEventKind::RegularTasksDone
    );
    if !is_culprit {
        return Err(error.into());
    }
    tracing::error!(
        job_id = ? queued.pending.event.job_id,
        error = % error,
        "The database rejected the payload of a job's event. Failing the job."
    );
    queued.pending.event.kind = TreeEventKind::TerminalFailure {
        lane: Lane::Regular,
        message: error.to_string(),
    };
    Ok(requeue_all(queued))
}

/// # Returns
///
/// The round's event, `queued`, with its input cleared so that the next round collects it again.
fn requeue_all(mut queued: Queued) -> Queued {
    queued.input = None;
    queued
}

/// Hits a hook point that lies after the round's database write. A pause that ends because the
/// service is stopping lets the round finish, as the round would without hooks.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`TestHooks::hit`]'s return values on failure, except a pause that ended because the
///   service is stopping.
async fn hit_after_commit(
    hooks: &TestHooks,
    point: HookPoint,
    context: impl FnOnce() -> HookContext,
) -> Result<(), CacheError> {
    match hooks.hit(point, context).await {
        Ok(()) => Ok(()),
        Err(fault) if fault.is_stopping() => Ok(()),
        Err(fault) => Err(fault.into()),
    }
}

/// # Returns
///
/// The hook context of a round of the tree rooted at `root_id` over the event of `queued`.
fn round_context(root_id: JobId, queued: &Queued) -> HookContext {
    HookContext::round(root_id, std::iter::once(&queued.pending.event))
}

/// Moves every event queued on either channel to the back of its buffer, without waiting.
fn receive(receivers: &mut EventReceivers, buffers: &mut Buffers) {
    while let Ok(pending) = receivers.fast_forward.try_recv() {
        buffers.fast_forward.push_back(Queued::new(pending));
    }
    while let Ok(pending) = receivers.regular.try_recv() {
        buffers.regular.push_back(Queued::new(pending));
    }
}

/// Closes both receivers, so that every later send fails, then answers every event still buffered
/// or queued with `reply`.
fn close_and_answer(
    receivers: &mut EventReceivers,
    buffers: &mut Buffers,
    reply: impl Fn() -> TreeReply,
) {
    receivers.fast_forward.close();
    receivers.regular.close();
    let mut pendings: Vec<Pending> = buffers
        .fast_forward
        .drain(..)
        .chain(buffers.regular.drain(..))
        .map(|queued| queued.pending)
        .collect();
    while let Ok(pending) = receivers.fast_forward.try_recv() {
        pendings.push(pending);
    }
    while let Ok(pending) = receivers.regular.try_recv() {
        pendings.push(pending);
    }
    for pending in pendings {
        // The event's waiter may have dropped its future, which is not an error of the committer.
        let _ = pending.reply.send(reply());
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
    use spider_core::types::id::TaskId;
    use spider_core::types::io::TaskGraphInput;
    use spider_core::types::io::TaskGraphInputBuilder;

    use super::*;
    use crate::cache::error::StaleStateError;
    use crate::cache::tree::TreeEvent;
    use crate::db::ExternalJobOrchestration;
    use crate::job_submission::create_validated_submission;
    use crate::state::test_utils::MockDbConnector;
    use crate::state::test_utils::MockInboundQueueSender;
    use crate::state::test_utils::MockTaskInstancePoolConnector;
    use crate::state::test_utils::create_test_tree_env;

    type TestEnv = TreeEnv<MockInboundQueueSender, MockDbConnector, MockTaskInstancePoolConnector>;

    type TestJcb = SharedJobControlBlock<MockInboundQueueSender, MockTaskInstancePoolConnector>;

    /// # Returns
    ///
    /// A one-task graph whose task takes and returns one byte-typed value, with a cleanup task if
    /// `with_cleanup`, and its inputs.
    fn one_task_graph(with_cleanup: bool) -> (SubmittedTaskGraph, TaskGraphInput) {
        let tdl_context = TdlContext {
            package: "test_pkg".to_owned(),
            task_func: "test_fn".to_owned(),
        };
        let cleanup_task = with_cleanup.then(|| TerminationTaskDescriptor {
            tdl_context: tdl_context.clone(),
            execution_policy: None,
        });
        let bytes_type = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
        let mut task_graph = SubmittedTaskGraph::new(None, cleanup_task)
            .expect("task graph creation should succeed");
        task_graph
            .insert_task(TaskDescriptor {
                tdl_context,
                execution_policy: Some(ExecutionPolicy::default()),
                inputs: vec![bytes_type.clone()],
                outputs: vec![bytes_type],
                input_sources: None,
            })
            .expect("task insertion should succeed");
        let mut inputs = TaskGraphInputBuilder::new();
        inputs
            .append_task_input(&[0u8; 4])
            .expect("task input appending should succeed");
        (task_graph, inputs.build())
    }

    /// Registers a one-task job in the mock database and creates its tree.
    ///
    /// # Returns
    ///
    /// The job's control block.
    ///
    /// # Panics
    ///
    /// Panics if the registration or the tree creation fails.
    async fn create_job(env: &TestEnv, with_cleanup: bool) -> TestJcb {
        let (task_graph, inputs) = one_task_graph(with_cleanup);
        let submission = create_validated_submission(task_graph, inputs);
        let owner_id = ResourceGroupId::random();
        let job_id = env
            .db
            .register(owner_id, &submission)
            .await
            .expect("mock registration should succeed");
        create_root_tree(env, job_id, owner_id, submission)
            .await
            .expect("tree creation should succeed")
    }

    fn event(jcb: &TestJcb, kind: TreeEventKind) -> TreeEvent {
        TreeEvent {
            job_id: jcb.id(),
            kind,
        }
    }

    /// Waits until every task of the environment's task tracker has exited.
    ///
    /// # Panics
    ///
    /// Panics if a task is still running after 5 seconds.
    async fn wait_for_committers(env: &TestEnv) {
        env.task_tracker.close();
        tokio::time::timeout(Duration::from_secs(5), env.task_tracker.wait())
            .await
            .expect("every committer should exit");
    }

    #[tokio::test]
    async fn committer_exits_when_its_root_becomes_terminal() -> anyhow::Result<()> {
        let (env, mut job_cache_gc_receiver) =
            create_test_tree_env(MockDbConnector::default(), MockInboundQueueSender);
        let jcb = create_job(&env, false).await;

        jcb.start().await?;
        let context = jcb
            .create_task_instance(TaskId::Index(0), ExecutionManagerId::random())
            .await?;
        let state = jcb
            .succeed_task_instance(context.task_instance_id, 0, vec![vec![1, 2]])
            .await?;
        assert_eq!(state, JobState::Succeeded);
        assert_eq!(job_cache_gc_receiver.try_recv(), Ok(jcb.id()));
        assert_eq!(
            env.db.states.get(&jcb.id()).map(|state| *state),
            Some(JobState::Succeeded)
        );
        wait_for_committers(&env).await;

        let start = jcb.start().await;
        assert!(
            matches!(
                start,
                Err(CacheError::StaleState(StaleStateError::JobAlreadyStarted))
            ),
            "a start after the committer exited should be stale, got {start:?}"
        );
        let cancel = jcb.cancel().await;
        assert!(
            matches!(
                cancel,
                Err(CacheError::StaleState(
                    StaleStateError::JobAlreadyTerminated(JobState::Succeeded)
                ))
            ),
            "a cancel after the committer exited should be stale, got {cancel:?}"
        );
        assert!(!env.shutdown_handle.is_fatal());
        Ok(())
    }

    #[tokio::test]
    async fn committer_stays_while_its_root_is_not_terminal() -> anyhow::Result<()> {
        let (env, _job_cache_gc_receiver) =
            create_test_tree_env(MockDbConnector::default(), MockInboundQueueSender);
        let jcb = create_job(&env, true).await;

        jcb.start().await?;
        assert_eq!(jcb.cancel().await?, JobState::CleanupReady);
        let reply = jcb
            .tree()
            .submit(event(&jcb, TreeEventKind::CancelRoot))
            .await?;
        assert!(
            matches!(
                reply,
                Ok(TreeOutcome::Stale(
                    StaleStateError::JobCancellationAlreadyRequested
                ))
            ),
            "a second cancel should be planned stale, got {reply:?}"
        );
        assert!(!jcb.tree().is_stopping());
        assert!(
            !env.task_tracker.is_empty(),
            "the committer should still run"
        );
        Ok(())
    }

    #[test]
    fn a_guard_dropped_after_the_token_is_cancelled_does_not_poison() {
        for cancels_first in [false, true] {
            let shutdown_handle = ShutdownHandle::new();
            let (_tree, receivers) = TreeControlBlock::new(
                JobId::from(1),
                shutdown_handle.clone(),
                TestHooks::default(),
            );
            let guard = PoisonOnAbnormalExit {
                receivers,
                shutdown_handle: shutdown_handle.clone(),
                root_id: JobId::from(1),
                disarmed: false,
            };
            if cancels_first {
                shutdown_handle.shut_down_gracefully();
            }
            drop(guard);
            assert_eq!(shutdown_handle.is_fatal(), !cancels_first);
            assert!(shutdown_handle.is_cancelled());
        }
    }

    #[cfg(feature = "test-hooks")]
    mod hooked {
        use super::*;
        use crate::test_hooks::HookAction;
        use crate::test_hooks::HookCount;
        use crate::test_hooks::HookFilter;

        /// Arms `action` at `point`, submits a start and a cancel, and checks that the committer
        /// poisons the service and wakes both waiters.
        ///
        /// # Panics
        ///
        /// Panics if any check fails.
        async fn assert_fault_poisons_the_tree(point: HookPoint, action: HookAction) {
            let (env, _job_cache_gc_receiver) =
                create_test_tree_env(MockDbConnector::default(), MockInboundQueueSender);
            let jcb = create_job(&env, false).await;
            let _hook = env
                .hooks
                .arm(point, HookFilter::root(jcb.id()), action, HookCount::Once);

            let start = jcb.tree().submit(event(&jcb, TreeEventKind::Start));
            let cancel = jcb.tree().submit(event(&jcb, TreeEventKind::CancelRoot));
            for reply in [start.await, cancel.await] {
                assert!(
                    reply.is_err(),
                    "the reply channel should close without a reply, got {reply:?}"
                );
            }
            wait_for_committers(&env).await;
            assert!(env.shutdown_handle.is_fatal());
            assert!(env.shutdown_handle.is_cancelled());
            let later = jcb.start().await;
            assert!(
                matches!(later, Err(CacheError::ServiceStopping)),
                "a later event should be answered Stopping, got {later:?}"
            );
        }

        #[tokio::test]
        async fn shutdown_finishes_the_round_in_flight_and_answers_the_queued_event_with_stopping()
        -> anyhow::Result<()> {
            let (env, _job_cache_gc_receiver) =
                create_test_tree_env(MockDbConnector::default(), MockInboundQueueSender);
            let jcb = create_job(&env, false).await;
            let pause = env.hooks.arm(
                HookPoint::CommitterAfterCommit,
                HookFilter::root(jcb.id()),
                HookAction::Pause,
                HookCount::Once,
            );

            let start = jcb.tree().submit(event(&jcb, TreeEventKind::Start));
            pause.reached(1).await;
            let cancel = jcb.tree().submit(event(&jcb, TreeEventKind::CancelRoot));
            env.shutdown_handle.shut_down_gracefully();

            let start = start.await;
            assert!(
                matches!(start, Ok(Ok(TreeOutcome::Applied(JobState::Running)))),
                "the round in flight should be applied, got {start:?}"
            );
            let cancel = cancel.await;
            assert!(
                matches!(cancel, Ok(Err(CacheError::ServiceStopping))),
                "the queued event should be answered Stopping, got {cancel:?}"
            );
            wait_for_committers(&env).await;
            let late = jcb
                .tree()
                .submit(event(&jcb, TreeEventKind::CancelRoot))
                .await;
            assert!(
                matches!(late, Ok(Ok(TreeOutcome::Untracked))),
                "an event after the committer exited should be untracked, got {late:?}"
            );
            assert!(!env.shutdown_handle.is_fatal());
            assert_eq!(jcb.state().await, JobState::Running);
            assert_eq!(
                env.db.states.get(&jcb.id()).map(|state| *state),
                Some(JobState::Running)
            );
            Ok(())
        }

        #[tokio::test]
        async fn a_round_requeued_before_the_db_transaction_collects_its_inputs_again()
        -> anyhow::Result<()> {
            for point in [
                HookPoint::CommitterBeforePlan,
                HookPoint::CommitterBeforeDbTransaction,
            ] {
                let (env, _job_cache_gc_receiver) =
                    create_test_tree_env(MockDbConnector::default(), MockInboundQueueSender);
                let jcb = create_job(&env, false).await;
                let mut state = CommitterState::new_root(jcb.clone());
                let (reply, mut reply_receiver) = tokio::sync::oneshot::channel();
                let pending = Pending {
                    event: event(&jcb, TreeEventKind::Start),
                    reply,
                };
                let _pause = env.hooks.arm(
                    point,
                    HookFilter::root(jcb.id()),
                    HookAction::Pause,
                    HookCount::Once,
                );
                env.shutdown_handle.shut_down_gracefully();

                let requeued = run_tree_state_transaction(&mut state, Queued::new(pending), &env)
                    .await?
                    .expect("a round stopped before its DB transaction should be requeued");
                assert_eq!(requeued.input, None, "{point:?}");
                assert_eq!(
                    env.db.states.get(&jcb.id()).map(|state| *state),
                    Some(JobState::Ready),
                    "{point:?}"
                );

                let requeued = run_tree_state_transaction(&mut state, requeued, &env).await?;
                assert!(
                    requeued.is_none(),
                    "{point:?}: the replanned round should be answered"
                );
                let reply = reply_receiver.try_recv();
                assert!(
                    matches!(reply, Ok(Ok(TreeOutcome::Applied(JobState::Running)))),
                    "{point:?}: the replanned start should be applied, got {reply:?}"
                );
                assert_eq!(jcb.state().await, JobState::Running, "{point:?}");
                assert_eq!(
                    env.db.states.get(&jcb.id()).map(|state| *state),
                    Some(JobState::Running),
                    "{point:?}"
                );
            }
            Ok(())
        }

        #[tokio::test]
        async fn crash_before_the_db_transaction_poisons_the_tree() {
            assert_fault_poisons_the_tree(
                HookPoint::CommitterBeforeDbTransaction,
                HookAction::Crash,
            )
            .await;
        }

        #[tokio::test]
        async fn panic_in_the_apply_poisons_the_tree() {
            assert_fault_poisons_the_tree(HookPoint::CommitterAfterApplyJob, HookAction::Panic)
                .await;
        }
    }
}
