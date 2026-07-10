//! End-to-end test: a layered `neuron::dense_*` task graph run through Spider must match the
//! in-process simulation.

use std::time::Duration;

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

/// Relative-tolerance float comparison.
const REL_TOL: f64 = 1.0e-12;

/// Number of layers in the test network.
const NUM_LAYERS: usize = 10;

/// Neurons per layer in the test network.
const LAYER_SIZE: usize = 1000;

#[tokio::test]
async fn test_nn() -> anyhow::Result<()> {
    if std::env::var("SPIDER_ENDPOINT").is_err() {
        bail!("SPIDER_ENDPOINT is not set");
    }

    let layer_specs = (0..NUM_LAYERS)
        .map(|i| {
            (
                LAYER_SIZE,
                if i % 2 == 0 {
                    Neuron::Relu
                } else {
                    Neuron::Sigmoid
                },
            )
        })
        .collect::<Vec<_>>();
    let nn = NeuralNetwork::new(layer_specs, 0)?;
    let inputs = random_f64s(nn.num_graph_inputs(), 0);
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

    SpiderTestDriver::run(
        job,
        Duration::from_secs(300),
        async move |_job_id, result| {
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
        },
    )
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
