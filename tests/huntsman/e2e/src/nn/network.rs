//! The neural-network model: a layered topology of `neuron::dense_*` neurons whose Spider
//! [`TaskGraph`] and in-process simulation describe the same DAG.

use huntsman_nn_core::NUM_INPUTS;
use rand::SeedableRng;
use rand::rngs::StdRng;
use spider_core::task::DataTypeDescriptor;
use spider_core::task::TaskDescriptor;
use spider_core::task::TaskGraph;
use spider_core::task::TaskIndex;
use spider_core::task::TaskInputOutputIndex;
use spider_core::task::TdlContext;
use spider_core::task::ValueTypeDescriptor;

use crate::nn::Neuron;
use crate::nn::wiring;

/// Name of the TDL package supplying the `neuron::dense_*` tasks.
const PACKAGE: &str = "nn";

/// One layer of the network: its neuron count, activation, and per-neuron fan-in.
struct Layer {
    /// Number of neurons in this layer.
    neuron_count: usize,
    /// Activation applied by every neuron in this layer.
    activation: Neuron,
    /// Per-neuron fan-in, listing previous-layer output indices feeding each neuron.
    /// Empty for layer 0, which reads graph inputs directly.
    fan_in: Vec<Vec<usize>>,
}

/// A randomly-wired, layered neural network of `neuron::dense_*` neurons.
pub struct NeuralNetwork {
    /// The layers in layer order.
    layers: Vec<Layer>,
}

impl NeuralNetwork {
    /// Factory function.
    ///
    /// Validates `layer_specs` via [`wiring::validate`] and generates the inner-layer fan-in
    /// wiring deterministically from `seed`.
    ///
    /// # Returns
    ///
    /// The newly created [`NeuralNetwork`] on success.
    ///
    /// # Errors
    ///
    /// Forwards [`wiring::validate`]'s return values on failure.
    pub fn new(layer_specs: Vec<(usize, Neuron)>, seed: u64) -> anyhow::Result<Self> {
        let sizes: Vec<usize> = layer_specs.iter().map(|(size, _)| *size).collect();
        wiring::validate(&sizes)?;
        let mut rng = StdRng::seed_from_u64(seed);
        let fan_ins = wiring::build_wiring(&sizes, &mut rng);
        let layers = layer_specs
            .into_iter()
            .zip(fan_ins)
            .map(|((neuron_count, activation), fan_in)| Layer {
                neuron_count,
                activation,
                fan_in,
            })
            .collect();
        Ok(Self { layers })
    }

    /// # Returns
    ///
    /// The number of graph inputs.
    #[must_use]
    pub fn num_graph_inputs(&self) -> usize {
        self.layers[0].neuron_count * NUM_INPUTS
    }

    /// Builds the Spider [`TaskGraph`] for this network.
    ///
    /// # Returns
    ///
    /// The [`TaskGraph`] for this network on success.
    ///
    /// # Errors
    ///
    /// Forwards [`TaskGraph::new`]'s return values on failure.
    /// Forwards [`TaskGraph::insert_task`]'s return values on failure.
    pub fn to_task_graph(&self) -> anyhow::Result<TaskGraph> {
        let float64 = DataTypeDescriptor::Value(ValueTypeDescriptor::float64());
        let mut graph = TaskGraph::new(None, None)?;
        let first = &self.layers[0];
        let mut prev_layer: Vec<TaskIndex> = Vec::with_capacity(first.neuron_count);

        for _ in 0..first.neuron_count {
            let task_idx = graph.insert_task(TaskDescriptor {
                tdl_context: TdlContext {
                    package: PACKAGE.to_owned(),
                    task_func: first.activation.task_name().to_owned(),
                },
                execution_policy: None,
                inputs: vec![float64.clone(); NUM_INPUTS],
                outputs: vec![float64.clone()],
                input_sources: None,
            })?;
            prev_layer.push(task_idx);
        }

        for layer in self.layers.iter().skip(1) {
            let mut curr_layer = Vec::with_capacity(layer.neuron_count);
            for j in 0..layer.neuron_count {
                let input_sources: Vec<TaskInputOutputIndex> = layer.fan_in[j]
                    .iter()
                    .map(|&src| TaskInputOutputIndex {
                        task_idx: prev_layer[src],
                        position: 0,
                    })
                    .collect();
                let task_idx = graph.insert_task(TaskDescriptor {
                    tdl_context: TdlContext {
                        package: PACKAGE.to_owned(),
                        task_func: layer.activation.task_name().to_owned(),
                    },
                    execution_policy: None,
                    inputs: vec![float64.clone(); NUM_INPUTS],
                    outputs: vec![float64.clone()],
                    input_sources: Some(input_sources),
                })?;
                curr_layer.push(task_idx);
            }
            prev_layer = curr_layer;
        }

        Ok(graph)
    }

    /// Computes the network's outputs from graph inputs.
    ///
    /// # Returns
    ///
    /// The network's outputs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`anyhow::Error`] if `inputs` length is not [`Self::num_graph_inputs`].
    pub fn simulate(&self, inputs: &[f64]) -> anyhow::Result<Vec<f64>> {
        let expected = self.num_graph_inputs();
        anyhow::ensure!(
            inputs.len() == expected,
            "expected {expected} graph inputs, got {}",
            inputs.len(),
        );

        let first = &self.layers[0];
        let mut layer_outputs: Vec<f64> = (0..first.neuron_count)
            .map(|i| {
                let start = i * NUM_INPUTS;
                let mut neuron_inputs = [0.0_f64; NUM_INPUTS];
                neuron_inputs.copy_from_slice(&inputs[start..start + NUM_INPUTS]);
                first.activation.evaluate_func()(&neuron_inputs)
            })
            .collect();

        for layer in self.layers.iter().skip(1) {
            layer_outputs = (0..layer.neuron_count)
                .map(|i| {
                    let neuron_inputs: [f64; NUM_INPUTS] =
                        std::array::from_fn(|j| layer_outputs[layer.fan_in[i][j]]);
                    layer.activation.evaluate_func()(&neuron_inputs)
                })
                .collect();
        }

        Ok(layer_outputs)
    }
}
