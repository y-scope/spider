//! Type aliases and marker traits that spell TDL primitive types in the Rust source.
//!
//! These names (`int8`, `float`, `List`, `Map`, ...) are the user-facing surface for authors of
//! TDL packages: the `#[task]` proc-macro inspects parameter types by name, so users should write
//! the aliases directly in their task signatures rather than the underlying Rust primitives.

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

mod private {
    pub trait Sealed {}
}

/// Marker trait restricting which types may appear as keys of a TDL [`Map`].
///
/// The trait is sealed: downstream crates cannot implement it for their own types. This guarantees
/// that every permitted key type has a stable, well-defined encoding at the wire layer.
pub trait MapKey: Eq + std::hash::Hash + private::Sealed {}

impl private::Sealed for i8 {}
impl private::Sealed for i16 {}
impl private::Sealed for i32 {}
impl private::Sealed for i64 {}
impl private::Sealed for Vec<u8> {}

impl MapKey for i8 {}
impl MapKey for i16 {}
impl MapKey for i32 {}
impl MapKey for i64 {}
impl MapKey for Vec<u8> {}
