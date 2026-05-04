//! Loads and indexes TDL packages compiled as cdylibs.
//!
//! See [`TdlPackage`] for the per-library wrapper and [`TdlPackageManager`] for the top-level
//! collection that enforces unique package names.

use std::{collections::HashMap, path::Path};

use libloading::{Library, Symbol};
use spider_tdl::{
    TdlError,
    Version,
    ffi::{CByteArray, CCharArray, TaskExecutionResult},
};

use crate::error::ExecutorError;

/// A single dlopen'd TDL package.
///
/// Owns the [`Library`] handle for the lifetime of the value; the dylib stays mapped until the
/// `TdlPackage` is dropped. The package's name and version are queried at load time and cached to
/// avoid repeating the FFI round trip on every call. The execute fn pointer is also resolved once
/// at load time and cached so each [`Self::execute_task`] call doesn't require `dlsym` per
/// dispatch.
pub struct TdlPackage {
    /// The name of the package.
    name: String,

    /// The TDL version used to generate the package.
    version: Version,

    /// Cached fn pointer for `__spider_tdl_package_execute`, resolved once at load time. Valid
    /// for the lifetime of `library` (i.e., for the lifetime of `Self`).
    execute: ExecuteFn,

    /// Holds the dylib mapped in memory. Never read directly after construction, but its `Drop`
    /// impl unmaps the library, which would invalidate `execute`. `library` must outlive
    /// `execute`; field-declaration order ensures `execute` is dropped first.
    _library: Library,
}

impl TdlPackage {
    /// Loads a TDL package from a filesystem path and verifies its `spider-tdl` ABI version.
    ///
    /// The load sequence runs in the following order. Failure at any step aborts the load and drops
    /// the library before returning, leaving the caller with no resources to clean up:
    ///
    /// 1. `dlopen` the library at `path`.
    /// 2. Look up [`SYM_GET_VERSION`], call it, and verify the returned [`Version`] is compatible
    ///    with [`Version::SPIDER_TDL`].
    /// 3. Look up [`SYM_GET_NAME`], call it, and decode the returned bytes as UTF-8.
    /// 4. Look up [`SYM_EXECUTE`] and cache the fn pointer for per-task dispatch.
    ///
    /// # Returns
    ///
    /// The loaded package on success, with its name, version, and execute fn pointer cached.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ExecutorError::IncompatibleVersion`] if the package was built against an incompatible
    ///   `spider-tdl` release.
    /// * Forwards [`Library::new`]'s return values on failure.
    /// * Forwards [`Library::get`]'s return values on failure for loading [`SYM_GET_VERSION`],
    ///   [`SYM_GET_NAME`], or [`SYM_EXECUTE`].
    /// * Forwards [`CCharArray::as_utf8`]'s return values on failure.
    pub fn load(path: &Path) -> Result<Self, ExecutorError> {
        // SAFETY: `Library::new` runs the dylib's initializers. Spider's design treats every TDL
        // package as trusted code installed by the operator, so this is the unsafety boundary for
        // the whole executor.
        let library = unsafe { Library::new(path) }?;

        // SAFETY: the symbol is read with the exact `extern "C"` signature the registration macro
        // emits. A mismatch is impossible if the package was built with `spider-tdl`'s
        // `register_tdl_package!`. If the symbol is missing or the dylib is not a TDL package, the
        // `library.get` call returns an error.
        let version = unsafe {
            let get_version: Symbol<GetVersionFn> = library.get(SYM_GET_VERSION)?;
            get_version()
        };

        let executor_version = Version::SPIDER_TDL;
        if !executor_version.is_compatible_with(&version) {
            return Err(ExecutorError::incompatible_version(
                version,
                executor_version,
            ));
        }

        // SAFETY: see the SAFETY comment on the version lookup above.
        let name_array = unsafe {
            let get_name: Symbol<GetNameFn> = library.get(SYM_GET_NAME)?;
            get_name()
        };
        let name = name_array.as_utf8()?.to_owned();

        // SAFETY: see the SAFETY comment on the version lookup above. We deref the borrowed
        // `Symbol<ExecuteFn>` to copy out the underlying fn pointer (`ExecuteFn` is `Copy`); the
        // pointer remains valid for as long as `library` stays loaded, which is the entire
        // lifetime of `Self`.
        let execute = unsafe {
            let symbol: Symbol<ExecuteFn> = library.get(SYM_EXECUTE)?;
            *symbol
        };

        Ok(Self {
            name,
            version,
            execute,
            _library: library,
        })
    }

    /// # Returns
    ///
    /// The package's declared name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// # Returns
    ///
    /// The `spider-tdl` ABI [`Version`] the package was compiled against.
    #[must_use]
    pub const fn version(&self) -> Version {
        self.version
    }

    /// Dispatches a task by name and returns the wire-format-encoded output buffer.
    ///
    /// `raw_ctx` is a msgpack-encoded
    /// [`TaskContext`](spider_tdl::TaskContext); `raw_inputs` is a wire-format-encoded
    /// [`TaskInputsSerializer`](spider_tdl::wire::TaskInputsSerializer) buffer.
    ///
    /// # Returns
    ///
    /// The wire-format-encoded outputs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ExecutorError::TaskError`] if the user task returned a [`TdlError`] (including
    ///   `TaskNotFound` when `task_name` is unknown to the package).
    /// * Forwards [`rmp_serde::from_slice`]'s return values on failure.
    pub fn execute_task(
        &self,
        task_name: &str,
        raw_ctx: &[u8],
        raw_inputs: &[u8],
    ) -> Result<Vec<u8>, ExecutorError> {
        let name_view = CCharArray::from_utf8(task_name);
        let ctx_view = CByteArray::from_slice(raw_ctx);
        let inputs_view = CByteArray::from_slice(raw_inputs);

        // SAFETY: `self.execute` was extracted at load time from a `Symbol<ExecuteFn>` resolved
        // against `self.library`. The library is still mapped (we own it) and the package protocol
        // is fixed by the version handshake at load time, so the call signature matches.
        let result = unsafe { (self.execute)(name_view, ctx_view, inputs_view) };

        match result.into_result() {
            Ok(output_bytes) => Ok(output_bytes),
            Err(error_bytes) => {
                let err: TdlError = rmp_serde::from_slice(&error_bytes)?;
                Err(ExecutorError::TaskError(err))
            }
        }
    }
}

/// Indexes loaded [`TdlPackage`]s by their declared name and rejects duplicates.
#[derive(Default)]
pub struct TdlPackageManager {
    packages: HashMap<String, TdlPackage>,
}

impl TdlPackageManager {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// An empty package manager.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads the package at `path` and indexes it by its declared name.
    ///
    /// # Returns
    ///
    /// The newly loaded package's name on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`ExecutorError::DuplicatePackage`] if a package with the same name is already loaded. The
    ///   freshly loaded library will be dropped (unloaded).
    /// * Forwards [`TdlPackage::load`]'s return values on failure.
    pub fn load(&mut self, path: &Path) -> Result<String, ExecutorError> {
        let package = TdlPackage::load(path)?;
        if self.packages.contains_key(package.name()) {
            return Err(ExecutorError::DuplicatePackage(package.name().to_owned()));
        }
        let name_key = package.name().to_owned();
        let inserted = self.packages.entry(name_key).or_insert(package);
        Ok(inserted.name().to_owned())
    }

    /// # Returns
    ///
    /// The package registered under `package_name`, or `None` if no such package is loaded.
    #[must_use]
    pub fn get(&self, package_name: &str) -> Option<&TdlPackage> {
        self.packages.get(package_name)
    }

    /// # Returns
    ///
    /// An iterator over the names of all currently loaded packages, in unspecified order.
    pub fn package_names(&self) -> impl Iterator<Item = &str> {
        self.packages.keys().map(String::as_str)
    }
}

/// FFI signature of `__spider_tdl_package_get_version`.
type GetVersionFn = unsafe extern "C" fn() -> Version;

/// FFI signature of `__spider_tdl_package_get_name`.
type GetNameFn = unsafe extern "C" fn() -> CCharArray<'static>;

/// FFI signature of `__spider_tdl_package_execute`.
type ExecuteFn =
    unsafe extern "C" fn(CCharArray<'_>, CByteArray<'_>, CByteArray<'_>) -> TaskExecutionResult;

/// FFI symbol name (NUL-terminated) for the package's compile-time `spider-tdl` version.
const SYM_GET_VERSION: &[u8] = b"__spider_tdl_package_get_version\0";

/// FFI symbol name (NUL-terminated) for the package's declared name.
const SYM_GET_NAME: &[u8] = b"__spider_tdl_package_get_name\0";

/// FFI symbol name (NUL-terminated) for the per-task dispatcher.
const SYM_EXECUTE: &[u8] = b"__spider_tdl_package_execute\0";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manager_load_nonexistent_path_returns_library_load_error() {
        let mut manager = TdlPackageManager::new();
        let result = manager.load(Path::new("/this/path/does/not/exist.so"));
        let err = result.expect_err("expected `InvalidLibrary` error for missing file");
        assert!(
            matches!(err, ExecutorError::InvalidLibrary(_)),
            "unexpected error variant: {err:?}",
        );
        assert_eq!(manager.package_names().count(), 0);
    }
}
