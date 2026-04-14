//! Registration macro for TDL task packages.
//!
//! The [`register_tasks!`] macro generates the static dispatch table and C-FFI entry points that
//! the task executor uses to discover and invoke tasks within a compiled TDL package.

/// Generates the TDL package's task registry and C-FFI entry points.
///
/// # Usage
///
/// ```ignore
/// spider_tdl::register_tasks! {
///     package_name: "my_package",
///     tasks: [my_task, another_task]
/// }
/// ```
///
/// # Generated items
///
/// * `__SPIDER_TDL_REGISTRY` — a `LazyLock<HashMap<&'static str, Box<dyn TaskHandler>>>` dispatch
///   table populated from the listed task types.
/// * `__SPIDER_TDL_PACKAGE_NAME` — a `&'static str` holding the package name.
/// * `__spider_tdl_package_get_name` — an `extern "C"` function returning the package name as a
///   [`CCharArray`][crate::ffi::CCharArray].
/// * `__spider_tdl_package_execute` — an `extern "C"` function that looks up a task by name in the
///   registry and executes it, returning a
///   [`TaskExecutionResult`][crate::ffi::TaskExecutionResult].
#[macro_export]
macro_rules! register_tasks {
    (
        package_name: $package_name:expr,
        tasks: [$($task:ty),* $(,)?]
    ) => {
        static __SPIDER_TDL_REGISTRY: std::sync::LazyLock<
            std::collections::HashMap<&'static str, Box<dyn $crate::TaskHandler>>,
        > = std::sync::LazyLock::new(|| {
            let mut map = std::collections::HashMap::new();
            $(
                map.insert(
                    <$task as $crate::Task>::NAME,
                    Box::new($crate::TaskHandlerImpl::<$task>::new())
                        as Box<dyn $crate::TaskHandler>,
                );
            )*
            map
        });

        static __SPIDER_TDL_PACKAGE_NAME: &str = $package_name;

        #[unsafe(no_mangle)]
        pub extern "C" fn __spider_tdl_package_get_name<'a>() -> $crate::ffi::CCharArray<'a> {
            $crate::ffi::CCharArray::from_str(__SPIDER_TDL_PACKAGE_NAME)
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn __spider_tdl_package_execute(
            name: $crate::ffi::CCharArray<'_>,
            ctx: $crate::ffi::CByteArray<'_>,
            inputs: $crate::ffi::CByteArray<'_>,
        ) -> $crate::ffi::TaskExecutionResult {
            let task_name: &str = unsafe { name.as_str() };
            let raw_ctx: &[u8] = unsafe { ctx.as_slice() };
            let raw_inputs: &[u8] = unsafe { inputs.as_slice() };

            let result = match __SPIDER_TDL_REGISTRY.get(task_name) {
                Some(handler) => handler.execute_raw(raw_ctx, raw_inputs),
                None => {
                    let err = $crate::TdlError::TaskNotFound(task_name.to_string());
                    $crate::ExecutionResult::from_tdl_error(&err)
                }
            };

            $crate::ffi::TaskExecutionResult::from_execution_result(result)
        }
    };
}
