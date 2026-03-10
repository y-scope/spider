use std::fmt::Display;

use strum::IntoEnumIterator;

/// Returns the inner part of a SQL `ENUM(...)` definition from a Rust enum,
/// e.g. `'Ready','Running','CommitReady'`.
pub fn sql_enum_values<T: IntoEnumIterator + Display>() -> String {
    T::iter()
        .map(|s| format!("'{s}'"))
        .collect::<Vec<_>>()
        .join(",")
}
