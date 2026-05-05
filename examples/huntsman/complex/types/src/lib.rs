//! Wire-compatible data types shared between the `huntsman-complex` cdylib (which exposes the
//! `complex::*` tasks) and any downstream consumer that builds task inputs / decodes outputs.
//!
//! Splitting the types out of the cdylib lets the integration test crate depend on this rlib and
//! reuse the canonical struct definitions instead of declaring a parallel mirror.

use serde::{Deserialize, Serialize};
use spider_tdl::r#std::{List, double};

/// A complex number with [`double`] components.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Complex {
    pub re: double,
    pub im: double,
}

/// A list of [`Complex`] values, used as the input/output type for every vector arithmetic task
/// exported by the `huntsman-complex` cdylib.
///
/// Wrapping the [`List`] in a named struct keeps the wire-format payload one positional element per
/// task parameter regardless of how many complex numbers it carries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComplexVec {
    pub items: List<Complex>,
}
