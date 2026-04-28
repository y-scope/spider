//! Procedural macros that support the [`spider_tdl`](../spider_tdl/index.html) crate.
//!
//! Currently, the only macro provided is the [`macro@task`] attribute macro, which transforms a
//! user-authored function into the marker struct, params struct, and `Task` trait implementation
//! consumed by the runtime.

mod task_macro;

use proc_macro::TokenStream;
use syn::{ItemFn, parse_macro_input};
use task_macro::TaskAttr;

/// Attribute macro that transforms a function into a Spider TDL–compatible task.
///
/// The annotated function must:
///
/// * Be a free-standing function (i.e., no `self` parameter).
/// * Accept `spider_tdl::TaskContext` as its first parameter. Type aliases and fully qualified
///   paths are allowed. This requirement is enforced at compile time via a generated assertion, so
///   any type that does not resolve to `spider_tdl::TaskContext` will produce a type error.
/// * Return `Result<(T1, T2, ...), spider_tdl::TdlError>`. A return type of
///   `Result<T, spider_tdl::TdlError>` is automatically promoted to
///   `Result<(T,), spider_tdl::TdlError>`.
///
/// # Arguments
///
/// * `name = "..."` (optional): Overrides the registered task name. If omitted, the function name
///   is used as-is (no module or namespace prefix is added).
///
/// # Example
///
/// ```ignore
/// use spider_tdl::{r#std::int32, task, TaskContext, TdlError};
///
/// #[task]
/// fn add(_ctx: TaskContext, a: int32, b: int32) -> Result<(int32,), TdlError> {
///     Ok((a + b,))
/// }
///
/// #[task(name = "math::sub")]
/// fn sub(_ctx: TaskContext, a: int32, b: int32) -> Result<int32, TdlError> {
///     Ok(a - b)
/// }
/// ```
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as TaskAttr);
    let item = parse_macro_input!(item as ItemFn);
    task_macro::expand(&attr, &item)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
