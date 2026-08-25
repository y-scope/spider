//! Activation functions for the end-to-end neural-network test workload.
//!
//! Each [`Neuron`] pairs a Spider `neuron::dense_*` task with the in-process
//! `huntsman_nn_core::dense_*` evaluation function so the task graph and [`super::NeuralNetwork`]'s
//! simulation share one source of truth.

use huntsman_nn_core::NUM_INPUTS;

/// A dense-layer neuron activation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Neuron {
    /// Rectified-linear activation.
    Relu,

    /// Logistic-sigmoid activation.
    Sigmoid,

    /// Identity (no-op) activation.
    Identity,
}

impl Neuron {
    /// # Returns
    ///
    /// The `neuron::dense_*` task function name that evaluates this activation.
    #[must_use]
    pub const fn task_name(self) -> &'static str {
        match self {
            Self::Relu => "neuron::dense_relu",
            Self::Sigmoid => "neuron::dense_sigmoid",
            Self::Identity => "neuron::dense_identity",
        }
    }

    /// # Returns
    ///
    /// The `huntsman_nn_core::dense_*` function that evaluates this activation.
    #[must_use]
    pub fn evaluate_func(self) -> fn(&[f64; NUM_INPUTS]) -> f64 {
        match self {
            Self::Relu => huntsman_nn_core::dense_relu,
            Self::Sigmoid => huntsman_nn_core::dense_sigmoid,
            Self::Identity => huntsman_nn_core::dense_identity,
        }
    }
}
