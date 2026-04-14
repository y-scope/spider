mod task_macro;

use proc_macro::TokenStream;
use syn::{ItemFn, parse_macro_input};
use task_macro::TaskAttr;

/// Attribute macro that transforms a task function into a marker struct with a [`spider_tdl::Task`]
/// implementation.
///
/// # Usage
///
/// ```ignore
/// #[task]
/// fn my_task(ctx: TaskContext, a: int32, b: int32) -> Result<(int32,), TdlError> {
///     Ok((a + b,))
/// }
/// ```
///
/// An optional `name` argument overrides the registered task name:
///
/// ```ignore
/// #[task(name = "my_namespace::my_task")]
/// fn my_task(ctx: TaskContext) -> Result<(int32,), TdlError> { ... }
/// ```
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as TaskAttr);
    let item = parse_macro_input!(item as ItemFn);
    task_macro::expand(&attr, &item)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
