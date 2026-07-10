//! Self-contained neural-network model for the end-to-end test.
//!
//! [`NeuralNetwork`] builds a layered `neuron::dense_*` task graph and reproduces it in-process via
//! [`NeuralNetwork::simulate`].

mod network;
mod neuron;
mod wiring;

pub use network::NeuralNetwork;
pub use neuron::Neuron;
