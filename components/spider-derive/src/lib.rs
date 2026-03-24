mod mysql;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

/// Derives MySQL-compatible enum support for use with [`sqlx`].
///
/// This derive macro generates the necessary trait implementations and helper methods to store a
/// Rust enum as a `MySQL` `ENUM` column. It uses each variant's identifier name as the
/// corresponding `MySQL` enum string value (e.g., variant `Ready` maps to the string `'Ready'`).
///
/// # Generated methods
///
/// The macro adds the following `const` methods to the annotated enum:
///
/// ## `as_mysql_enum_decl() -> &'static str`
///
/// Returns the `MySQL` `ENUM(...)` type declaration string for use in DDL statements. For example,
/// an enum with variants `Red`, `Green`, `Blue` produces `"ENUM('Red','Green','Blue')"`.
///
/// ## `as_str(&self) -> &'static str`
///
/// Returns the unquoted variant name (e.g., `"Red"`).
///
/// ## `as_quoted_str(&self) -> &'static str`
///
/// Returns the single-quoted variant name for use in SQL literals (e.g., `"'Red'"`).
///
/// # Generated trait implementations
///
/// * [`sqlx::Type<sqlx::MySql>`] — Reports the `MySQL` type info as `str`, so that `sqlx` treats
///   the enum value as a text-compatible type.
/// * [`sqlx::Encode<sqlx::MySql>`] — Encodes the enum by delegating to `&str` encoding using the
///   variant name.
/// * [`sqlx::Decode<sqlx::MySql>`] — Decodes the enum by matching the database string against known
///   variant names. Returns an error for unknown values.
///
/// # Limitations
///
/// * Can only be derived for enums — applying it to a struct or union produces a compile-time
///   error.
/// * All variants must be unit variants (no tuple or struct fields).
/// * Variant names are used as-is for the `MySQL` values — there is no support for renaming via
///   attributes (e.g., `#[sql(rename = "...")]`).
/// * The generated [`sqlx::Decode`] implementation returns a runtime error (not a compile-time
///   error) if the database contains a value that does not match any variant name.
///
/// # Example
#[proc_macro_derive(MySqlEnum)]
pub fn derive_mysql_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    mysql::derive_mysql_enum(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
