//! Topology wiring for the end-to-end neural-network test workload.
//!
//! Layer 0 neurons read graph inputs directly; inner-layer neurons each draw a fixed fan-in
//! from the previous layer's outputs.

use huntsman_nn_core::NUM_INPUTS;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

/// Validates the layer sizes against the following invariants:
///
/// * The layer list is non-empty.
/// * Each layer's size is at least the neuron fan-in [`NUM_INPUTS`].
/// * Each consecutive pair of layers fully covers the previous layer's outputs (`next_size *
///   NUM_INPUTS >= prev_size`).
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if an invariant is violated.
pub fn validate(sizes: &[usize]) -> anyhow::Result<()> {
    anyhow::ensure!(!sizes.is_empty(), "at least one layer is required");
    for (i, &size) in sizes.iter().enumerate() {
        anyhow::ensure!(
            size >= NUM_INPUTS,
            "layer {i} size {size} is smaller than the neuron fan-in {NUM_INPUTS}",
        );
    }
    for (i, window) in sizes.windows(2).enumerate() {
        let layer_index = i + 1;
        let prev_size = window[0];
        let next_size = window[1];
        anyhow::ensure!(
            next_size * NUM_INPUTS >= prev_size,
            "layer {layer_index}'s {next_size} * fan-in {NUM_INPUTS} cannot cover previous size \
             {prev_size}",
        );
    }
    Ok(())
}

/// Builds the per-neuron fan-in wiring for every layer.
///
/// # Returns
///
/// The per-neuron fan-in wiring, indexed `[layer][neuron][fan_in]`. `wiring[0]` is empty since
/// layer 0 reads graph inputs directly.
///
/// # Panics
///
/// Panics if the layer invariants do not hold.
pub fn build_wiring(sizes: &[usize], rng: &mut StdRng) -> Vec<Vec<Vec<usize>>> {
    let mut wiring = Vec::with_capacity(sizes.len());
    wiring.push(Vec::new());
    for window in sizes.windows(2) {
        let prev_size = window[0];
        let next_size = window[1];
        wiring.push(generate_layer_wiring(rng, prev_size, next_size));
    }
    wiring
}

/// Deals numbers from a shuffled deck, reshuffling a fresh permutation whenever the current deck
/// runs out.
struct Dealer<'a> {
    rng: &'a mut StdRng,
    range_size: usize,
    deck: Vec<usize>,
}

impl<'a> Dealer<'a> {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The created [`Dealer`] with a freshly shuffled deck of 0..`range_size`.
    fn new(rng: &'a mut StdRng, range_size: usize) -> Self {
        let deck = shuffled_range(rng, range_size);
        Self {
            rng,
            range_size,
            deck,
        }
    }

    /// Draws the next number, reshuffling when the current deck is exhausted.
    ///
    /// # Returns
    ///
    /// The drawn number.
    ///
    /// # Panics
    ///
    /// Panics if a reshuffled deck is empty.
    fn draw(&mut self) -> usize {
        if self.deck.is_empty() {
            self.deck = shuffled_range(self.rng, self.range_size);
        }
        self.deck.pop().expect("reshuffled deck must be non-empty")
    }
}

/// Generates the fan-in for each neuron of one inner layer.
///
/// # Returns
///
/// One fan-in vector per neuron in the layer, each containing previous-layer output indices.
///
/// # Panics
///
/// Panics if the layer invariants do not hold, i.e. `next_size * NUM_INPUTS < prev_size` or
/// `prev_size < NUM_INPUTS`.
fn generate_layer_wiring(rng: &mut StdRng, prev_size: usize, next_size: usize) -> Vec<Vec<usize>> {
    assert!(
        prev_size >= NUM_INPUTS && next_size * NUM_INPUTS >= prev_size,
        "layer invariants do not hold",
    );
    let mut slots: Vec<Vec<usize>> = vec![Vec::new(); next_size];
    let mut dealer = Dealer::new(rng, prev_size);

    for neuron_slots in &mut slots {
        for _ in 0..NUM_INPUTS {
            let output = loop {
                let candidate = dealer.draw();
                if !neuron_slots.contains(&candidate) {
                    break candidate;
                }
            };
            neuron_slots.push(output);
        }
    }

    slots
}

/// Produces a random permutation of the numbers in 0..`range_size`.
///
/// # Returns
///
/// The random permutation of 0..`range_size`.
fn shuffled_range(rng: &mut StdRng, range_size: usize) -> Vec<usize> {
    let mut deck: Vec<usize> = (0..range_size).collect();
    deck.shuffle(rng);
    deck
}
