//! Pure neuron math for the Spider end-to-end neural-network test workload.
//!
//! A dense-layer neuron computes `activation(weighted_sum(inputs) + bias)` over a fixed fan-in of
//! 25 scalar `double` inputs.

/// The fixed neuron fan-in: each neuron consumes exactly this many scalar inputs.
pub const NUM_INPUTS: usize = 25;

/// The fixed per-input weights, one per input position. Deterministic values calculated as
/// (`WEIGHTS[k] = (k + 1) * 0.01 * (-1)^k`), alternating in sign starting positive.
pub const WEIGHTS: [f64; NUM_INPUTS] = [
    0.01, -0.02, 0.03, -0.04, 0.05, -0.06, 0.07, -0.08, 0.09, -0.10, 0.11, -0.12, 0.13, -0.14,
    0.15, -0.16, 0.17, -0.18, 0.19, -0.20, 0.21, -0.22, 0.23, -0.24, 0.25,
];

/// The fixed bias added to the weighted sum before the activation.
pub const BIAS: f64 = 0.5;

/// # Returns
///
/// The rectified-linear activation `max(0.0, x)`.
#[must_use]
pub const fn relu(x: f64) -> f64 {
    f64::max(0.0, x)
}

/// # Returns
///
/// The logistic sigmoid activation `1.0 / (1.0 + exp(-x))`.
#[must_use]
pub fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + f64::exp(-x))
}

/// # Returns
///
/// The identity activation `x`.
#[must_use]
pub const fn identity(x: f64) -> f64 {
    x
}

/// # Returns
///
/// The rectified-linear activation of the weighted sum of `inputs` plus [`BIAS`].
#[must_use]
pub fn dense_relu(inputs: &[f64; NUM_INPUTS]) -> f64 {
    relu(weighted_sum(inputs))
}

/// # Returns
///
/// The logistic sigmoid of the weighted sum of `inputs` plus [`BIAS`].
#[must_use]
pub fn dense_sigmoid(inputs: &[f64; NUM_INPUTS]) -> f64 {
    sigmoid(weighted_sum(inputs))
}

/// # Returns
///
/// The weighted sum of `inputs` plus [`BIAS`], unchanged by the activation.
#[must_use]
pub fn dense_identity(inputs: &[f64; NUM_INPUTS]) -> f64 {
    identity(weighted_sum(inputs))
}

/// # Returns
///
/// The weighted sum `sum(WEIGHTS[k] * inputs[k]) + BIAS`.
fn weighted_sum(inputs: &[f64; NUM_INPUTS]) -> f64 {
    let mut acc = BIAS;
    for (w, x) in WEIGHTS.iter().zip(inputs.iter()) {
        acc += w * x;
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Relative-tolerance float equality used to compare hand-computed and computed values.
    fn assert_approx_eq(actual: f64, expected: f64) {
        let diff = (actual - expected).abs();
        let tol = 1.0e-12_f64 * (1.0 + expected.abs());
        assert!(
            diff <= tol,
            "actual={actual}, expected={expected}, diff={diff}, tol={tol}",
        );
    }

    #[test]
    fn test_relu() {
        assert_approx_eq(relu(-1.0), 0.0);
        assert_approx_eq(relu(0.0), 0.0);
        assert_approx_eq(relu(2.5), 2.5);
    }

    #[test]
    fn test_sigmoid() {
        assert_approx_eq(sigmoid(0.0), 0.5);
        assert_approx_eq(sigmoid(100.0), 1.0);
        assert_approx_eq(sigmoid(-100.0), 0.0);
        assert!(sigmoid(-1.0) < sigmoid(0.0));
        assert!(sigmoid(0.0) < sigmoid(1.0));
    }

    #[test]
    fn test_identity() {
        assert_approx_eq(identity(-3.0), -3.0);
        assert_approx_eq(identity(0.0), 0.0);
        assert_approx_eq(identity(7.25), 7.25);
    }

    #[test]
    fn test_weighted_sum_all_zero_inputs_equals_bias() {
        let inputs = [0.0_f64; NUM_INPUTS];
        assert_approx_eq(weighted_sum(&inputs), BIAS);
    }

    #[test]
    fn test_weighted_sum_all_one_inputs() {
        let inputs = [1.0_f64; NUM_INPUTS];
        assert_approx_eq(weighted_sum(&inputs), 0.63);
    }

    #[test]
    fn test_dense_relu() {
        let zero = [0.0_f64; NUM_INPUTS];
        assert_approx_eq(dense_relu(&zero), 0.5);

        let ones = [1.0_f64; NUM_INPUTS];
        assert_approx_eq(dense_relu(&ones), 0.63);

        // Negative weighted sum (large negative inputs) clamps to 0 under relu.
        let neg = [-1000.0_f64; NUM_INPUTS];
        assert_approx_eq(dense_relu(&neg), 0.0);
    }

    #[test]
    fn test_dense_sigmoid() {
        let zero = [0.0_f64; NUM_INPUTS];
        assert_approx_eq(dense_sigmoid(&zero), sigmoid(BIAS));

        let ones = [1.0_f64; NUM_INPUTS];
        assert_approx_eq(dense_sigmoid(&ones), sigmoid(0.63));
    }

    #[test]
    fn test_dense_identity() {
        let zero = [0.0_f64; NUM_INPUTS];
        assert_approx_eq(dense_identity(&zero), 0.5);

        let ones = [1.0_f64; NUM_INPUTS];
        assert_approx_eq(dense_identity(&ones), 0.63);

        let neg = [-1000.0_f64; NUM_INPUTS];
        assert_approx_eq(dense_identity(&neg), weighted_sum(&neg));
    }
}
