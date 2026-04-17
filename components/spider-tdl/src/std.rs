//! Standard TDL type aliases for cross-language compatibility.
//!
//! Rust task functions accept any type that implements the appropriate serde traits (`Serialize`,
//! `Deserialize`). However, for task signatures that must remain compatible with other language
//! front-ends (e.g., a future Python or C++ Spider TDL), it is recommended to use the aliases
//! defined in this module (`spider_tdl::std::int32`, `spider_tdl::std::List<T>`, etc.). These names
//! map one-to-one with the TDL type system and guarantee compatibility across languages through
//! TDL-compiler-generated serde wrapper.
//!
//! # Example
//!
//! ```ignore
//! use spider_tdl::std::{int32, List};
//!
//! #[task]
//! fn sum(ctx: TaskContext, values: List<int32>) -> Result<int32, TdlError> {
//!     Ok(values.iter().sum())
//! }
//! ```

// The lowercase primitive aliases (`int8`, `float`, `boolean`, ...) intentionally mirror the TDL
// language's primitive type spelling, so that TDL package source reads like TDL rather than Rust.
#![allow(non_camel_case_types)]

/// Signed 8-bit integer.
pub type int8 = i8;

/// Signed 16-bit integer.
pub type int16 = i16;

/// Signed 32-bit integer.
pub type int32 = i32;

/// Signed 64-bit integer.
pub type int64 = i64;

/// 32-bit IEEE-754 floating-point number.
pub type float = f32;

/// 64-bit IEEE-754 floating-point number.
pub type double = f64;

/// Boolean value.
pub type boolean = bool;

/// Opaque byte string.
pub type Bytes = Vec<u8>;

/// Homogeneous list of values.
pub type List<ElementType> = Vec<ElementType>;

/// Homogeneous key-value map.
///
/// The key type must satisfy [`MapKey`]. Because Rust does not enforce `where` clauses on type
/// aliases on stable, the bound is enforced by the `#[task]` proc-macro at parse time rather than
/// by the type alias itself.
pub type Map<KeyType, ValueType> = std::collections::HashMap<KeyType, ValueType>;
