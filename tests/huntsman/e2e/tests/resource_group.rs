//! End-to-end test that a resource-group-dedicated worker upholds two isolation properties:
//!
//! 1. It executes only tasks from the resource group it is pinned to.
//! 2. It never executes another resource group's tasks, even while those tasks are actively
//!    scheduled onto the general workers.
//!
//! Requires a deployment with a general pool plus a pool dedicated to `rg-dedicated`, both loading
//! the `integration_test_tasks` package under the `resource_group_round_robin` scheduler policy.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context;
use anyhow::bail;
use e2e::JobSubmission;
use e2e::SpiderTestDriver;
use e2e::TerminationResult;
use e2e::decode_output;
use e2e::encode_input;
use spider_core::task::DataTypeDescriptor;
use spider_core::task::TaskDescriptor;
use spider_core::task::TaskGraph;
use spider_core::task::TdlContext;
use spider_core::task::ValueTypeDescriptor;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_resource_group_isolation() -> anyhow::Result<()> {
    /// External id of the resource group a dedicated worker pool is pinned to.
    const DEDICATED_RG: &str = "rg-dedicated";

    /// External id of a resource group with no dedicated pool; its tasks run on general workers.
    const OTHER_RG: &str = "rg-other";

    /// Number of jobs submitted per resource group.
    const JOBS_PER_GROUP: usize = 60;

    /// Placement reported by a task that ran on a general (unpinned) worker.
    const GENERAL_PLACEMENT: &str = "general";

    /// Placement reported by a task that ran on its own group's dedicated worker.
    const DEDICATED_PLACEMENT: &str = "dedicated";

    /// Placement reported by a task that ran on another group's dedicated worker.
    const FOREIGN_PLACEMENT: &str = "foreign";

    let placements: Arc<Mutex<Vec<(&'static str, String)>>> = Arc::new(Mutex::new(Vec::new()));

    let mut jobs = JoinSet::new();
    for _ in 0..JOBS_PER_GROUP {
        for resource_group in [DEDICATED_RG, OTHER_RG] {
            let placements = Arc::clone(&placements);
            jobs.spawn(async move { run_report_job(resource_group, &placements).await });
        }
    }
    while let Some(result) = jobs.join_next().await {
        result.context("report-job task panicked")??;
    }

    let placements = placements.lock().expect("placements mutex poisoned");

    let foreign: Vec<&(&str, String)> = placements
        .iter()
        .filter(|(_, placement)| placement == FOREIGN_PLACEMENT)
        .collect();
    anyhow::ensure!(
        foreign.is_empty(),
        "a dedicated worker executed {} task(s) from another resource group: {foreign:?}",
        foreign.len(),
    );

    let dedicated_ran_own = placements
        .iter()
        .filter(|(rg, placement)| *rg == DEDICATED_RG && placement == DEDICATED_PLACEMENT)
        .count();
    anyhow::ensure!(
        dedicated_ran_own > 0,
        "dedicated worker executed none of its own resource group's tasks; the dedicated pool may \
         not have registered",
    );

    for (_, placement) in placements.iter().filter(|(rg, _)| *rg == OTHER_RG) {
        anyhow::ensure!(
            placement == GENERAL_PLACEMENT,
            "an {OTHER_RG} task ran on pool {placement:?}, expected {GENERAL_PLACEMENT:?}",
        );
    }

    Ok(())
}

/// Submits one single-task `report_worker_pool` job under `resource_group` and records the pool
/// that executed it.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`TaskGraph::new`]'s return values on failure.
/// * Forwards [`TaskGraph::insert_task`]'s return values on failure.
/// * Forwards [`encode_input`]'s return values on failure.
/// * Forwards [`SpiderTestDriver::run`]'s return values on failure.
async fn run_report_job(
    resource_group: &'static str,
    placements: &Arc<Mutex<Vec<(&'static str, String)>>>,
) -> anyhow::Result<()> {
    /// TDL package the deployment's workers load.
    const TASK_PACKAGE: &str = "integration_test_tasks";

    /// Task that reports the pool that executed it.
    const TASK_FUNC: &str = "report_worker_pool";

    /// Per-task sleep so tasks from both groups stay in flight together while the scheduler places
    /// them across workers.
    const TASK_SLEEP_MILLIS: i64 = 500;

    /// Upper bound on how long a single job may take to reach a terminal state.
    const JOB_TIMEOUT: Duration = Duration::from_secs(180);

    let int64 = DataTypeDescriptor::Value(ValueTypeDescriptor::int64());
    let bytes = DataTypeDescriptor::Value(ValueTypeDescriptor::bytes());
    let mut task_graph = TaskGraph::new(None, None)?;
    task_graph.insert_task(TaskDescriptor {
        tdl_context: TdlContext {
            package: TASK_PACKAGE.to_owned(),
            task_func: TASK_FUNC.to_owned(),
        },
        execution_policy: None,
        inputs: vec![int64],
        outputs: vec![bytes],
        input_sources: None,
    })?;
    let job = JobSubmission {
        resource_group_id: resource_group.to_owned(),
        task_graph,
        inputs: vec![encode_input(&TASK_SLEEP_MILLIS)?],
    };

    let placements = Arc::clone(placements);
    SpiderTestDriver::run(job, JOB_TIMEOUT, async move |_job_id, result| {
        let outputs = match result {
            TerminationResult::Success(outputs) => outputs,
            TerminationResult::Failure(message) => bail!("job failed: {message}"),
            TerminationResult::Cancelled => bail!("job cancelled"),
        };
        anyhow::ensure!(
            outputs.len() == 1,
            "expected exactly one output, got {}",
            outputs.len(),
        );
        let placement = String::from_utf8(decode_output::<Vec<u8>>(&outputs[0])?)
            .context("worker pool placement is not valid UTF-8")?;
        placements
            .lock()
            .expect("placements mutex poisoned")
            .push((resource_group, placement));
        Ok(())
    })
    .await?;

    Ok(())
}
