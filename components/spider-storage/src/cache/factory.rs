use std::{collections::HashSet, sync::Arc};

use spider_core::{
    task::{self as core_task, TaskIndex, TaskState},
    types::{
        id::{JobId, ResourceGroupId},
        io::TaskInput,
    },
};
use tokio::sync::RwLock;

use crate::{
    cache::{
        error::{CacheError, InternalError},
        job::{Job, JobControlBlock, ReadyQueueConnector, RwJob, TaskInstancePoolConnector},
        task::{
            BaseTaskControlBlock,
            InputReader,
            OutputReader,
            RetryCounter,
            SharedTaskControlBlock,
            SharedTerminationTaskControlBlock,
            TaskControlBlock,
            TaskGraph,
            TerminationTaskControlBlock,
            ValuePayload,
        },
        types::{Reader, Shared, TdlContext, Writer},
    },
    db::InternalJobOrchestration,
};

/// The result type of [`build_job`].
type BuildJobResult<RQ, DB, TIP> =
    Result<(JobControlBlock<RQ, DB, TIP>, Vec<TaskIndex>), CacheError>;

/// Builds a [`JobControlBlock`] from a user-facing [`core_task::TaskGraph`] and job inputs.
///
/// Returns the job control block and a list of initially ready task indices (input tasks).
///
/// # Errors
///
/// Returns [`CacheError`] if the task graph is corrupted or the job inputs do not match the
/// expected graph inputs.
///
/// # Panics
///
/// Panics if a task control block mutex cannot be acquired during construction (indicates a bug).
pub fn build_job<
    ReadyQueueConnectorType: ReadyQueueConnector,
    DbConnectorType: InternalJobOrchestration,
    TaskInstancePoolConnectorType: TaskInstancePoolConnector,
>(
    job_id: JobId,
    owner_id: ResourceGroupId,
    core_graph: &core_task::TaskGraph,
    job_inputs: Vec<TaskInput>,
    ready_queue: ReadyQueueConnectorType,
    db: DbConnectorType,
    pool: TaskInstancePoolConnectorType,
) -> BuildJobResult<ReadyQueueConnectorType, DbConnectorType, TaskInstancePoolConnectorType> {
    let data_buffers = create_data_buffers(core_graph, job_inputs)?;
    let cache_tcbs = build_task_control_blocks(core_graph, &data_buffers);
    populate_children(core_graph, &cache_tcbs);
    let output_readers = collect_job_outputs(core_graph, &data_buffers);

    let commit_task = core_graph
        .get_commit_task_descriptor()
        .map(build_termination_tcb);
    let cleanup_task = core_graph
        .get_cleanup_task_descriptor()
        .map(build_termination_tcb);

    let cache_task_graph = TaskGraph {
        tasks: cache_tcbs,
        outputs: output_readers,
        commit_task,
        cleanup_task,
    };

    let num_tasks = core_graph.get_num_tasks();
    let job = Job::new(
        spider_core::job::JobState::Running,
        cache_task_graph,
        num_tasks,
    );
    let rw_job = RwJob::new(job);
    let jcb = JobControlBlock::new(job_id, owner_id, rw_job, ready_queue, db, pool);

    let ready_indices: Vec<TaskIndex> = core_graph
        .get_tasks()
        .iter()
        .filter(|t| t.is_input_task())
        .map(spider_core::task::Task::get_index)
        .collect();

    Ok((jcb, ready_indices))
}

/// Creates shared data buffers for all dataflow dependencies and pre-populates job inputs.
fn create_data_buffers(
    core_graph: &core_task::TaskGraph,
    job_inputs: Vec<TaskInput>,
) -> Result<Vec<Shared<ValuePayload>>, CacheError> {
    let num_deps = core_graph.get_num_dataflow_deps();
    let mut data_buffers: Vec<Shared<ValuePayload>> =
        (0..num_deps).map(|_| Arc::new(RwLock::new(None))).collect();

    let mut graph_input_dep_indices: Vec<usize> = Vec::new();
    for dep_idx in 0..num_deps {
        let dep = core_graph.get_dataflow_dep(dep_idx).ok_or_else(|| {
            InternalError::TaskGraphCorrupted("dataflow dep index out of bounds".to_owned())
        })?;
        if dep.get_src().is_none() {
            graph_input_dep_indices.push(dep_idx);
        }
    }

    if graph_input_dep_indices.len() != job_inputs.len() {
        return Err(InternalError::TaskGraphCorrupted(format!(
            "expected {} graph inputs, got {} job inputs",
            graph_input_dep_indices.len(),
            job_inputs.len()
        ))
        .into());
    }

    for (dep_idx, job_input) in graph_input_dep_indices.iter().zip(job_inputs.into_iter()) {
        let TaskInput::ValuePayload(payload) = job_input;
        data_buffers[*dep_idx] = Arc::new(RwLock::new(Some(payload)));
    }

    Ok(data_buffers)
}

/// Builds `SharedTaskControlBlock`s for each task (without children populated).
fn build_task_control_blocks(
    core_graph: &core_task::TaskGraph,
    data_buffers: &[Shared<ValuePayload>],
) -> Vec<SharedTaskControlBlock> {
    let core_tasks = core_graph.get_tasks();
    let mut cache_tcbs: Vec<SharedTaskControlBlock> = Vec::with_capacity(core_tasks.len());

    for core_task in core_tasks {
        let inputs: Vec<InputReader> = core_task
            .get_input_dep_indices()
            .iter()
            .map(|&dep_idx| InputReader::Value(Reader::new(data_buffers[dep_idx].clone())))
            .collect();

        let outputs: Vec<_> = core_task
            .get_output_dep_indices()
            .iter()
            .map(|&dep_idx| Writer::new(data_buffers[dep_idx].clone()))
            .collect();

        let num_parents = core_task.get_num_parents();
        let state = if num_parents == 0 {
            TaskState::Ready
        } else {
            TaskState::Pending
        };

        let execution_policy = core_task.get_execution_policy();

        let tcb = TaskControlBlock {
            base: BaseTaskControlBlock {
                state,
                tdl_context: TdlContext {
                    package: core_task.get_tdl_package().to_owned(),
                    func: core_task.get_tdl_function().to_owned(),
                },
                instance_ids: HashSet::new(),
                max_num_instances: execution_policy.max_num_instances,
                retry_counter: RetryCounter::new(execution_policy.max_num_retries),
            },
            index: core_task.get_index(),
            num_parents,
            num_unfinished_parents: num_parents,
            inputs,
            outputs,
            children: Vec::new(),
        };

        cache_tcbs.push(SharedTaskControlBlock::new(tcb));
    }

    cache_tcbs
}

/// Populates child references for each task control block (second pass).
///
/// # Panics
///
/// Panics if a mutex cannot be acquired (should be impossible during single-threaded construction).
fn populate_children(core_graph: &core_task::TaskGraph, cache_tcbs: &[SharedTaskControlBlock]) {
    for core_task in core_graph.get_tasks() {
        let children: Vec<SharedTaskControlBlock> = core_task
            .get_child_indices()
            .iter()
            .map(|&child_idx| cache_tcbs[child_idx].clone())
            .collect();

        if !children.is_empty() {
            let mut tcb_guard = cache_tcbs[core_task.get_index()]
                .try_lock_for_construction()
                .expect("lock should not be contended during construction");
            tcb_guard.children = children;
        }
    }
}

/// Collects job-level outputs (dangling dataflow outputs not consumed by any task).
fn collect_job_outputs(
    core_graph: &core_task::TaskGraph,
    data_buffers: &[Shared<ValuePayload>],
) -> Vec<OutputReader> {
    let mut output_readers: Vec<OutputReader> = Vec::new();
    for (dep_idx, buffer) in data_buffers.iter().enumerate() {
        if let Some(dep) = core_graph.get_dataflow_dep(dep_idx)
            && dep.get_src().is_some()
            && dep.get_dst().is_empty()
        {
            output_readers.push(Reader::new(buffer.clone()));
        }
    }
    output_readers
}

fn build_termination_tcb(
    desc: &core_task::TerminationTaskDescriptor,
) -> SharedTerminationTaskControlBlock {
    let tcb = TerminationTaskControlBlock {
        base: BaseTaskControlBlock {
            state: TaskState::Ready,
            tdl_context: TdlContext {
                package: desc.tdl_package.clone(),
                func: desc.tdl_function.clone(),
            },
            instance_ids: HashSet::new(),
            max_num_instances: desc.execution_policy.max_num_instances,
            retry_counter: RetryCounter::new(desc.execution_policy.max_num_retries),
        },
    };
    SharedTerminationTaskControlBlock::new(tcb)
}
