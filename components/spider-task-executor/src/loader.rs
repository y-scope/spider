//! TDL package loader.
//!
//! [`TdlPackageLoader`] manages a registry of loaded TDL packages (cdylibs), indexed by package
//! name. Each package is loaded via `dlopen` and its name is discovered by calling the
//! `__spider_tdl_package_get_name` C-FFI entry point. Callers look up a package by name via
//! [`TdlPackageLoader::get`] and invoke tasks directly on the returned [`TdlPackage`] reference.

use std::{collections::HashMap, path::Path};

use spider_tdl::{
    TdlError,
    ffi::{CByteArray, CCharArray, TaskExecutionResult},
};

use crate::error::ExecutorError;

type GetNameFn = unsafe extern "C" fn() -> CCharArray<'static>;
type ExecuteFn =
    unsafe extern "C" fn(CCharArray<'_>, CByteArray<'_>, CByteArray<'_>) -> TaskExecutionResult;

/// A single loaded TDL package backed by a `dlopen`-ed shared library.
///
/// Obtained from [`TdlPackageLoader::get`]. Provides [`Self::execute_task`] to invoke a task
/// inside the package by name.
pub struct TdlPackage {
    library: libloading::Library,
}

impl TdlPackage {
    /// Returns the package name declared by the loaded library.
    ///
    /// # Returns
    ///
    /// The package name as a `&str` on success.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::LibraryLoad`] if the `__spider_tdl_package_get_name` symbol is
    /// not found.
    pub fn get_name(&self) -> Result<&str, ExecutorError> {
        // SAFETY: the library is a valid TDL package produced by `register_tasks!`, so the
        // symbol exists and returns a CCharArray pointing to a static string inside the library.
        unsafe {
            let func: libloading::Symbol<GetNameFn> =
                self.library.get(b"__spider_tdl_package_get_name")?;
            let name_arr = func();
            // SAFETY: the package name is a Rust `&'static str` produced by `register_tasks!`,
            // so it is guaranteed to be valid UTF-8.
            Ok(name_arr.as_str())
        }
    }

    /// Executes a task by name with raw serialized context and inputs.
    ///
    /// Both `raw_ctx` (msgpack-encoded `TaskContext`) and `raw_inputs` (wire-format task inputs)
    /// are passed through opaquely to the C-FFI entry point. The executor does not interpret
    /// their contents.
    ///
    /// # Returns
    ///
    /// The wire-format output bytes on success (opaque to the executor, decoded by the storage
    /// layer).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ExecutorError::LibraryLoad`] if the `__spider_tdl_package_execute` symbol is not found.
    /// * [`ExecutorError::TaskError`] if the task returned a [`TdlError`].
    /// * [`ExecutorError::ErrorPayloadDeserialization`] if the error payload cannot be decoded.
    pub fn execute_task(
        &self,
        task_name: &str,
        raw_ctx: &[u8],
        raw_inputs: &[u8],
    ) -> Result<Vec<u8>, ExecutorError> {
        // SAFETY: the library is a valid TDL package produced by `register_tasks!`. The
        // CCharArray/CByteArray values borrow from the caller's stack and remain valid for the
        // duration of the synchronous FFI call. The returned TaskExecutionResult owns a buffer
        // allocated by the same global allocator.
        unsafe {
            let func: libloading::Symbol<ExecuteFn> =
                self.library.get(b"__spider_tdl_package_execute")?;
            let name_arr = CCharArray::from_str(task_name);
            let ctx_arr = CByteArray::from_slice(raw_ctx);
            let input_arr = CByteArray::from_slice(raw_inputs);
            let result = func(name_arr, ctx_arr, input_arr);

            match result.into_result() {
                Ok(output_bytes) => Ok(output_bytes),
                Err(error_bytes) => {
                    let tdl_error: TdlError = rmp_serde::from_slice(&error_bytes)?;
                    Err(ExecutorError::TaskError(tdl_error))
                }
            }
        }
    }
}

/// Registry of loaded TDL packages, keyed by package name.
///
/// Each package is loaded from a cdylib at runtime. The loader discovers the package name by
/// calling the library's `__spider_tdl_package_get_name` entry point and rejects duplicates.
/// Callers look up a package by name via [`Self::get`] and invoke tasks on the returned
/// [`TdlPackage`] reference.
pub struct TdlPackageLoader {
    packages: HashMap<String, TdlPackage>,
}

impl Default for TdlPackageLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl TdlPackageLoader {
    /// Creates an empty loader with no packages.
    #[must_use]
    pub fn new() -> Self {
        Self {
            packages: HashMap::new(),
        }
    }

    /// Loads a TDL package from the shared library at `path` and registers it by its declared
    /// package name.
    ///
    /// The package name is discovered by calling `__spider_tdl_package_get_name` inside the
    /// loaded library.
    ///
    /// # Returns
    ///
    /// The package name on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ExecutorError::LibraryLoad`] if `dlopen` fails or the name symbol is missing.
    /// * [`ExecutorError::DuplicatePackage`] if a package with the same name is already loaded.
    ///
    /// # Panics
    ///
    /// Panics if the just-inserted package cannot be found in the internal map -- this indicates
    /// a logic error and cannot occur in practice.
    pub fn load(&mut self, path: &Path) -> Result<&str, ExecutorError> {
        // SAFETY: loading a shared library runs its init routines, but the safety contract is
        // between the deployment environment and the library.
        let library = unsafe { libloading::Library::new(path) }?;
        let package = TdlPackage { library };

        let name = package.get_name()?.to_owned();
        if self.packages.contains_key(&name) {
            return Err(ExecutorError::DuplicatePackage(name));
        }
        self.packages.insert(name.clone(), package);

        Ok(self.packages.get_key_value(&name).expect("just inserted").0)
    }

    /// Returns a reference to the loaded package with the given name, or `None` if no such
    /// package is loaded.
    #[must_use]
    pub fn get(&self, package_name: &str) -> Option<&TdlPackage> {
        self.packages.get(package_name)
    }
}
