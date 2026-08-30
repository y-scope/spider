//! End-to-end test: a layered `neuron::dense_*` task graph run through Spider must match the
//! in-process simulation.

use std::time::Duration;

use anyhow::Context;
use anyhow::bail;
use e2e::JobSubmission;
use e2e::SpiderTestDriver;
use e2e::TerminationResult;
use e2e::decode_output;
use e2e::encode_input;
use e2e::nn::NeuralNetwork;
use e2e::nn::Neuron;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_nn() -> anyhow::Result<()> {
    /// Number of neural-network job batches.
    const NUM_BATCHES: usize = 3;

    /// Number of concurrent neural-network jobs in each batch.
    const NUM_JOBS_PER_BATCH: usize = 8;

    for batch_index in 0..NUM_BATCHES {
        let mut jobs = JoinSet::new();
        for job_index in 0..NUM_JOBS_PER_BATCH {
            let seed = u64::try_from(batch_index * NUM_JOBS_PER_BATCH + job_index)
                .expect("neural-network job index does not fit in u64");
            jobs.spawn(async move {
                run_neural_network_job(seed).await.with_context(|| {
                    format!(
                        "neural-network job {job_index} in batch {batch_index} with seed {seed} \
                         failed"
                    )
                })
            });
        }
        while let Some(result) = jobs.join_next().await {
            result.context("neural-network job task panicked")??;
        }
    }

    Ok(())
}

/// Runs a neural-network job and validates its outputs against the in-process simulation.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`NeuralNetwork::new`]'s return values on failure.
/// * Forwards [`NeuralNetwork::simulate`]'s return values on failure.
/// * Forwards [`NeuralNetwork::to_task_graph`]'s return values on failure.
/// * Forwards [`encode_input`]'s return values on failure.
/// * Forwards [`SpiderTestDriver::run`]'s return values on failure.
async fn run_neural_network_job(seed: u64) -> anyhow::Result<()> {
    /// Relative-tolerance float comparison.
    const REL_TOL: f64 = 1.0e-12;

    /// Number of layers in the test network.
    const NUM_LAYERS: usize = 10;

    /// Neurons per layer in the test network.
    const LAYER_SIZE: usize = 1000;

    /// Maximum duration of one neural-network job.
    const JOB_TIMEOUT: Duration = Duration::from_secs(600);

    let layer_specs = (0..NUM_LAYERS)
        .map(|i| {
            (
                LAYER_SIZE,
                match i % 3 {
                    0 => Neuron::Relu,
                    1 => Neuron::Sigmoid,
                    _ => Neuron::Identity,
                },
            )
        })
        .collect::<Vec<_>>();
    let nn = NeuralNetwork::new(layer_specs, seed)?;
    let inputs = random_f64s(nn.num_graph_inputs(), seed);
    let expected = nn.simulate(&inputs)?;
    let task_graph = nn.to_task_graph()?;
    let job = JobSubmission {
        resource_group_id: "e2e-nn".to_owned(),
        task_graph,
        inputs: inputs
            .iter()
            .map(encode_input)
            .collect::<anyhow::Result<Vec<_>>>()?,
    };

    SpiderTestDriver::run(job, JOB_TIMEOUT, async move |_job_id, result| {
        let outputs = match result {
            TerminationResult::Success(outputs) => outputs,
            TerminationResult::Failure(message) => bail!("job failed: {message}"),
            TerminationResult::Cancelled => bail!("job cancelled"),
        };
        let actual: Vec<f64> = outputs
            .iter()
            .map(decode_output)
            .collect::<anyhow::Result<Vec<_>>>()?;
        anyhow::ensure!(
            actual.len() == expected.len(),
            "expected {} outputs, got {}",
            expected.len(),
            actual.len(),
        );
        for (&got, &exp) in actual.iter().zip(expected.iter()) {
            let diff = (got - exp).abs();
            let tol = REL_TOL * (1.0 + exp.abs());
            assert!(
                got.is_finite() && exp.is_finite() && diff <= tol,
                "output mismatch: got={got}, expected={exp}, diff={diff}, tol={tol}",
            );
        }
        Ok(())
    })
    .await?;

    Ok(())
}

/// # Returns
///
/// `count` number of deterministic random `f64` values seeded by `seed`.
fn random_f64s(count: usize, seed: u64) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count).map(|_| rng.random::<f64>()).collect()
}
