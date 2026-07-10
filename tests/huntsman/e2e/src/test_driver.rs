//! The shared end-to-end test driver that submits jobs to a live Spider deployment and drives
//! them to a terminal state.
//!
//! The driver is a lazily initialized process-wide singleton. It reads its Spider endpoint and
//! concurrency configuration from environment variables and serializes access to the underlying
//! client so that shared scenarios run concurrently up to a configured limit while exclusive
//! scenarios run in isolation.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;

use anyhow::Context;
use spider_client::SpiderClient;
use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;
use tokio::sync::RwLock;
use tokio::sync::Semaphore;
use tonic::transport::Endpoint;

use crate::types::JobSubmission;
use crate::types::TerminationResult;

/// A process-wide harness for running end-to-end Spider job scenarios.
pub struct SpiderTestDriver {
    client: RwLock<SpiderClient>,
    concurrency_limiter: Semaphore,
    resource_groups: Mutex<HashMap<String, ResourceGroupId>>,
}

impl SpiderTestDriver {
    /// Runs a job scenario concurrently with other shared scenarios, up to the configured
    /// concurrency limit.
    ///
    /// # Type Parameters
    ///
    /// * `OutcomeAssertionType` - The callback for asserting the terminal outcome of the job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if the concurrency limiter has been closed.
    /// * Forwards [`Self::instance`]'s return values on failure.
    /// * Forwards [`Self::resolve_resource_group`]'s return values on failure.
    /// * Forwards [`run_scenario`]'s return values on failure.
    pub async fn run<OutcomeAssertionType>(
        job_submission: JobSubmission,
        timeout: Duration,
        outcome_assertion: OutcomeAssertionType,
    ) -> anyhow::Result<()>
    where
        OutcomeAssertionType: AsyncFnOnce(JobId, TerminationResult) -> anyhow::Result<()>, {
        let driver = Self::instance().await?;
        let client_guard = driver.client.read().await;
        let _permit = driver
            .concurrency_limiter
            .acquire()
            .await
            .context("concurrency limiter closed")?;
        let client = client_guard.clone();
        let resource_group_id = driver
            .resolve_resource_group(&client, &job_submission.resource_group_id)
            .await?;
        let result = run_scenario(
            client,
            resource_group_id,
            job_submission,
            timeout,
            async |_job_id: JobId| -> anyhow::Result<()> { Ok(()) },
            outcome_assertion,
        )
        .await;
        drop(client_guard);
        result
    }

    /// Runs a job scenario in exclusive isolation.
    ///
    /// # Type Parameters
    ///
    /// * `FailureInjectionType` - A background task for injecting a failure into the running job.
    /// * `OutcomeAssertionType` - The callback for asserting the terminal outcome of the job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::instance`]'s return values on failure.
    /// * Forwards [`Self::resolve_resource_group`]'s return values on failure.
    /// * Forwards [`run_scenario`]'s return values on failure.
    pub async fn run_exclusive<FailureInjectionType, OutcomeAssertionType>(
        job_submission: JobSubmission,
        timeout: Duration,
        failure_injection: FailureInjectionType,
        outcome_assertion: OutcomeAssertionType,
    ) -> anyhow::Result<()>
    where
        FailureInjectionType: AsyncFnOnce(JobId) -> anyhow::Result<()>,
        OutcomeAssertionType: AsyncFnOnce(JobId, TerminationResult) -> anyhow::Result<()>, {
        let driver = Self::instance().await?;
        let client_guard = driver.client.write().await;
        let client = client_guard.clone();
        let resource_group_id = driver
            .resolve_resource_group(&client, &job_submission.resource_group_id)
            .await?;
        let result = run_scenario(
            client,
            resource_group_id,
            job_submission,
            timeout,
            failure_injection,
            outcome_assertion,
        )
        .await;
        drop(client_guard);
        result
    }

    /// Returns the process-wide driver instance, initializing it on first access.
    ///
    /// # Returns
    ///
    /// A reference to the process-wide driver instance on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::init`]'s return values on failure.
    async fn instance() -> anyhow::Result<&'static Self> {
        static INSTANCE: OnceCell<SpiderTestDriver> = OnceCell::const_new();
        INSTANCE.get_or_try_init(Self::init).await
    }

    /// Initializes the driver from the environment, connecting to the configured Spider endpoint.
    ///
    /// # Returns
    ///
    /// A newly initialized driver on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if:
    ///  * The endpoint environment variable is unset.
    ///  * The endpoint value is not a valid Spider endpoint.
    /// * Forwards [`read_concurrency`]'s return values on failure.
    /// * Forwards [`SpiderClient::connect`]'s return values on failure.
    async fn init() -> anyhow::Result<Self> {
        const ENDPOINT_ENV_VAR: &str = "SPIDER_ENDPOINT";
        let endpoint_string = std::env::var(ENDPOINT_ENV_VAR)
            .with_context(|| format!("{ENDPOINT_ENV_VAR} is not set"))?;
        let endpoint = Endpoint::from_shared(endpoint_string).context("invalid spider endpoint")?;
        let concurrency = read_concurrency()?;
        let client = SpiderClient::connect(endpoint, concurrency).await?;
        Ok(Self {
            client: RwLock::new(client),
            concurrency_limiter: Semaphore::new(concurrency.get()),
            resource_groups: Mutex::new(HashMap::new()),
        })
    }

    /// Resolves an external resource-group id to a Spider-assigned id, registering the resource
    /// group on first use and caching the result.
    ///
    /// # Returns
    ///
    /// The Spider-assigned resource-group id on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`SpiderClient::add_resource_group`]'s return values on failure.
    async fn resolve_resource_group(
        &self,
        client: &SpiderClient,
        external_resource_group_id: &str,
    ) -> anyhow::Result<ResourceGroupId> {
        let mut resource_groups = self.resource_groups.lock().await;
        if let Some(resource_group_id) = resource_groups.get(external_resource_group_id) {
            return Ok(*resource_group_id);
        }
        let resource_group_id = client
            .add_resource_group(external_resource_group_id.to_owned(), Vec::new())
            .await?;
        resource_groups.insert(external_resource_group_id.to_owned(), resource_group_id);
        drop(resource_groups);
        Ok(resource_group_id)
    }
}

/// Runs a single job scenario: submits and starts the job, drives it to a terminal state while
/// concurrently running the failure injection, and forwards the outcome to the assertion.
///
/// # Type Parameters
///
/// * `FailureInjectionType` - A background task for injecting a failure into the running job.
/// * `OutcomeAssertionType` - The callback for asserting the terminal outcome of the job.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if the job does not reach a terminal state within `timeout`.
/// * Forwards [`submit_and_start_job`]'s return values on failure.
/// * Forwards [`poll_until_terminal`]'s return values on failure.
/// * Forwards `failure_injection`'s return values on failure.
/// * Forwards `outcome_assertion`'s return values on failure.
async fn run_scenario<FailureInjectionType, OutcomeAssertionType>(
    client: SpiderClient,
    resource_group_id: ResourceGroupId,
    job_submission: JobSubmission,
    timeout: Duration,
    failure_injection: FailureInjectionType,
    outcome_assertion: OutcomeAssertionType,
) -> anyhow::Result<()>
where
    FailureInjectionType: AsyncFnOnce(JobId) -> anyhow::Result<()>,
    OutcomeAssertionType: AsyncFnOnce(JobId, TerminationResult) -> anyhow::Result<()>, {
    let job_id = submit_and_start_job(&client, resource_group_id, job_submission).await?;
    let result = match tokio::time::timeout(timeout, async {
        let ((), termination) = tokio::try_join!(
            failure_injection(job_id),
            poll_until_terminal(&client, job_id),
        )?;
        anyhow::Result::<TerminationResult>::Ok(termination)
    })
    .await
    {
        Ok(termination) => termination?,
        Err(_elapsed) => anyhow::bail!("job did not reach a terminal state within {timeout:?}"),
    };
    outcome_assertion(job_id, result).await
}

/// Submits and starts the described job.
///
/// # Returns
///
/// The id of the submitted job on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`SpiderClient::submit_job`]'s return values on failure.
/// * Forwards [`SpiderClient::start_job`]'s return values on failure.
async fn submit_and_start_job(
    client: &SpiderClient,
    resource_group_id: ResourceGroupId,
    job_submission: JobSubmission,
) -> anyhow::Result<JobId> {
    let job_id = client
        .submit_job(
            resource_group_id,
            &job_submission.task_graph,
            job_submission.inputs,
        )
        .await?;
    client.start_job(job_id).await?;
    Ok(job_id)
}

/// Polls the job's state until it reaches a terminal state.
///
/// # Returns
///
/// The terminal outcome of the job on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`SpiderClient::get_job_state`]'s return values on failure.
/// * Forwards [`SpiderClient::get_job_outputs`]'s return values on failure.
/// * Forwards [`SpiderClient::get_job_error`]'s return values on failure.
async fn poll_until_terminal(
    client: &SpiderClient,
    job_id: JobId,
) -> anyhow::Result<TerminationResult> {
    const POLL_INTERVAL: Duration = Duration::from_millis(10);

    loop {
        match client.get_job_state(job_id).await? {
            JobState::Succeeded => {
                let outputs = client.get_job_outputs(job_id).await?;
                return Ok(TerminationResult::Success(outputs));
            }
            JobState::Failed => {
                let error_message = client.get_job_error(job_id).await?;
                return Ok(TerminationResult::Failure(error_message));
            }
            JobState::Cancelled => return Ok(TerminationResult::Cancelled),
            _ => tokio::time::sleep(POLL_INTERVAL).await,
        }
    }
}

/// Reads the configured shared-scenario concurrency from the environment, falling back to a default
/// value when unset.
///
/// # Returns
///
/// The configured concurrency on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if the concurrency value is invalid.
///
/// # Panics
///
/// Panics if the default value is zero. This shouldn't be reachable at runtime: the default number
/// is a non-zero compile-time constant.
fn read_concurrency() -> anyhow::Result<NonZeroUsize> {
    const CONCURRENCY_ENV_VAR: &str = "SPIDER_CONCURRENCY";
    const DEFAULT_CONCURRENCY: usize = 8;

    match std::env::var(CONCURRENCY_ENV_VAR) {
        Ok(value) => value.parse::<NonZeroUsize>().with_context(|| {
            format!("{CONCURRENCY_ENV_VAR} must be a positive integer, got {value:?}")
        }),
        Err(std::env::VarError::NotPresent) => {
            Ok(NonZeroUsize::new(DEFAULT_CONCURRENCY).expect("default concurrency is non-zero"))
        }
        Err(error) => {
            Err(error).with_context(|| format!("{CONCURRENCY_ENV_VAR} is not valid unicode"))
        }
    }
}
