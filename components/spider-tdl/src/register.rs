//! Task registration macro and supporting helpers.
//!
//! The [`register_tdl_package!`] macro is invoked once per TDL package. It expands to:
//!
//! * A `LazyLock<HashMap<&'static str, Box<dyn TaskHandler>>>` dispatch table populated on first
//!   lookup, mapping each task's `NAME` to a [`TaskHandler`](crate::TaskHandler) trait object.
//!   Runtime lookups are O(1) and dispatch goes through the trait vtable.
//! * A single assertion that rejects duplicate `NAME`s with a `const_eval` panic at build time. The
//!   hash map itself is built only once at runtime; uniqueness is enforced at compile time and is
//!   independent of the runtime structure.
//! * Three `extern "C"` entry points consumed by the package manager via `dlsym`:
//!   * `__spider_tdl_package_get_version`
//!   * `__spider_tdl_package_get_name`
//!   * `__spider_tdl_package_execute`
//!
//! The helpers in this module are public only so they are reachable from macro expansions in
//! downstream crates. They are not part of the user-facing API. As a result, their docstrings are
//! marked as `hidden`.

use crate::TdlError;

/// Registers a TDL package's tasks and exports the three C-FFI entry points consumed by the task
/// executor.
///
/// Invoke once per package, at module scope:
///
/// ```ignore
/// spider_tdl::register_tdl_package! {
///     package_name: "complex-number",
///     tasks: [add, sub, mul, div, always_fail],
/// }
/// ```
///
/// Each entry in `tasks` must name a type that implements [`Task`](crate::Task) (typically a marker
/// struct produced by the `#[task]` attribute macro).
///
/// # Name Uniqueness
///
/// All tasks registered within a single package must have distinct `NAME`s. This check happens in
/// compile time.
///
/// # Generated FFI symbols
///
/// * `__spider_tdl_package_get_version` returns the [`Version`](crate::Version) of `spider-tdl` the
///   package was compiled against.
/// * `__spider_tdl_package_get_name` returns the package name passed to the macro, as a borrowed
///   [`CCharArray`](crate::ffi::CCharArray).
/// * `__spider_tdl_package_execute` dispatches a task by name for execution and returns a
///   [`TaskExecutionResult`](crate::ffi::TaskExecutionResult).
#[macro_export]
macro_rules! register_tdl_package {
    (
        package_name: $package_name:expr,
        tasks: [$($task:path),* $(,)?] $(,)?
    ) => {
        const __SPIDER_TDL_PACKAGE_NAME: &str = $package_name;

        const _TASK_NAME_UNIQUENESS_CHECK: () = $crate::register::assert_unique_task_names(&[
            $(
                <$task as $crate::Task>::NAME,
            )*
        ]);

        static __SPIDER_TDL_REGISTRY: ::std::sync::LazyLock<
            ::std::collections::HashMap<
                &'static str,
                ::std::boxed::Box<dyn $crate::TaskHandler>,
            >,
        > = ::std::sync::LazyLock::new(|| {
            ::std::collections::HashMap::from([
                $(
                    (
                        <$task as $crate::Task>::NAME,
                        ::std::boxed::Box::new($crate::TaskHandlerImpl::<$task>::new())
                            as ::std::boxed::Box<dyn $crate::TaskHandler>,
                    ),
                )*
            ])
        });

        #[unsafe(no_mangle)]
        pub extern "C" fn __spider_tdl_package_get_version() -> $crate::Version {
            $crate::Version::SPIDER_TDL
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn __spider_tdl_package_get_name() -> $crate::ffi::CCharArray<'static> {
            $crate::ffi::CCharArray::from_utf8(__SPIDER_TDL_PACKAGE_NAME)
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn __spider_tdl_package_execute(
            name: $crate::ffi::CCharArray<'_>,
            raw_ctx: $crate::ffi::CByteArray<'_>,
            raw_inputs: $crate::ffi::CByteArray<'_>,
        ) -> $crate::ffi::TaskExecutionResult {
            let name_str: &str = match name.as_utf8() {
                ::std::result::Result::Ok(s) => s,
                ::std::result::Result::Err(_) => {
                    let err = $crate::TdlError::DeserializationError(
                        "task name is not valid UTF-8".to_owned(),
                    );
                    let bytes = $crate::register::serialize_error_payload(&err);
                    return $crate::ffi::TaskExecutionResult::from_error(bytes);
                }
            };

            let raw_ctx_slice: &[u8] = &raw_ctx;
            let raw_inputs_slice: &[u8] = &raw_inputs;

            match __SPIDER_TDL_REGISTRY.get(name_str) {
                ::std::option::Option::Some(handler) => {
                    match handler.execute_raw(raw_ctx_slice, raw_inputs_slice) {
                        ::std::result::Result::Ok(output_bytes) => {
                            $crate::ffi::TaskExecutionResult::from_outputs(output_bytes)
                        }
                        ::std::result::Result::Err(error_bytes) => {
                            $crate::ffi::TaskExecutionResult::from_error(error_bytes)
                        }
                    }
                }
                ::std::option::Option::None => {
                    let err = $crate::TdlError::TaskNotFound(name_str.to_owned());
                    let bytes = $crate::register::serialize_error_payload(&err);
                    $crate::ffi::TaskExecutionResult::from_error(bytes)
                }
            }
        }
    };
}

/// Compile-time check that all task `NAME`s are distinct.
///
/// The [`register_tdl_package!`] macro emits a single `const _: () = assert_unique_task_names(...)`
/// call passing every registered task's `NAME`. The helper runs an O(N²) nested loop entirely
/// during const evaluation; a duplicate `NAME` aborts the build with a `const_eval` panic.
///
/// The panic message is static (can't include the conflicting `NAME` value) because multi-arg
/// formatting in const panic — `const_format_args!` — is still unstable. The single-arg form is
/// stable, but doesn't help here since we'd need to name both colliding tasks.
///
/// # Panics
///
/// Panics in const evaluation if any two `names` are equal.
#[doc(hidden)]
pub const fn assert_unique_task_names(names: &[&'static str]) {
    let mut i = 0;
    while i < names.len() {
        let mut j = i + 1;
        while j < names.len() {
            assert!(
                !const_str_eq(names[i], names[j]),
                "two registered tasks share the same NAME — check the `#[task(name = ...)]` \
                 attributes in the most recent `register_tdl_package!` invocation",
            );
            j += 1;
        }
        i += 1;
    }
}

/// Serializes a [`TdlError`] into the byte payload returned across the FFI boundary.
///
/// Used by the `register_tdl_package!` expansion when reporting `TaskNotFound` and other errors
/// that originate inside the FFI dispatcher rather than inside a user task.
///
/// # Returns
///
/// The msgpack-encoded [`TdlError`] bytes.
///
/// # Panics
///
/// Panics if [`rmp_serde::to_vec`] fails to serialize the error. Msgpack encoding of [`TdlError`]
/// (which only contains a [`String`] payload) should not fail in practice.
#[doc(hidden)]
#[must_use]
pub fn serialize_error_payload(err: &TdlError) -> Vec<u8> {
    rmp_serde::to_vec(err).expect("failed to serialize `TdlError` as msgpack")
}

/// `const`-friendly byte-wise string equality.
///
/// `str::eq` is not yet `const`, so the `register_tdl_package!` uniqueness assertions cannot rely
/// on it. This helper compares the underlying byte slices.
///
/// # Returns
///
/// Whether `a` and `b` contain the same bytes.
const fn const_str_eq(a: &'static str, b: &'static str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    // Since `iter` and `zip` are not `const` methods yet, we have to implement the byte-by-byte
    // check explicitly using a C-style comparison.
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use std::panic;

    use super::*;

    #[test]
    fn const_str_eq_basic() {
        assert!(const_str_eq("", ""));
        assert!(const_str_eq("foo", "foo"));
        assert!(!const_str_eq("foo", "bar"));
        assert!(!const_str_eq("foo", "foobar"));
        assert!(!const_str_eq("foobar", "foo"));
    }

    #[test]
    fn assert_unique_task_names_passes_when_all_distinct() {
        let names: &[&str] = &["foo::a", "foo::b", "foo::c"];
        let result = panic::catch_unwind(|| assert_unique_task_names(names));
        assert!(result.is_ok(), "expected no panic for distinct names");
    }

    #[test]
    fn assert_unique_task_names_passes_for_empty_input() {
        let names: &[&str] = &[];
        let result = panic::catch_unwind(|| assert_unique_task_names(names));
        assert!(result.is_ok(), "expected no panic for empty input");
    }

    #[test]
    fn assert_unique_task_names_panics_on_duplicate() {
        let names: &[&str] = &["foo::a", "foo::dup", "foo::dup"];
        let payload = panic::catch_unwind(|| assert_unique_task_names(names))
            .expect_err("expected panic on duplicate NAME");
        let msg = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&'static str>().copied())
            .expect("panic payload was neither `String` nor `&'static str`");
        assert!(
            msg.contains("two registered tasks share the same NAME"),
            "unexpected panic payload: {msg}",
        );
    }

    #[test]
    fn assert_unique_task_names_is_const_callable() {
        /// Confirms `assert_unique_task_names` is usable in a `const` context (i.e. compile-time).
        /// If this fails to compile, the macro's `const _` invocation is also broken.
        const _: () = assert_unique_task_names(&["foo::a", "foo::b"]);
    }
}
