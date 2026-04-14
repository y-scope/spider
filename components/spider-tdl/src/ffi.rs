//! `#[repr(C)]` types shared across the TDL package / task executor C-FFI boundary.
//!
//! Both sides of the boundary live in the same process and share the same Rust global allocator,
//! so buffers allocated on one side can be reclaimed on the other via `Box::into_raw` /
//! `Box::from_raw`. These types are intentionally thin: they carry pointers and lengths only.

use std::{ffi::c_char, fmt, marker::PhantomData, mem::ManuallyDrop};

/// Borrowed, C-ABI-compatible view of a contiguous slice `&'borrow_lifetime [ElementType]`.
///
/// The lifetime parameter is tracked via [`PhantomData`] so that a [`CArray`] cannot outlive the
/// slice it was constructed from when it stays on the Rust side. Once the value is passed across
/// the C-FFI boundary, the lifetime is erased and safety falls to the caller.
#[repr(C)]
pub struct CArray<'borrow_lifetime, ElementType> {
    pointer: *const ElementType,
    length: usize,
    _lifetime: PhantomData<&'borrow_lifetime [ElementType]>,
}

// Manual `Copy`/`Clone` impls avoid the auto-derived `ElementType: Copy` / `ElementType: Clone`
// bounds: a borrowed pointer/length pair is always trivially copyable regardless of the element
// type.
impl<ElementType> Copy for CArray<'_, ElementType> {}

impl<ElementType> Clone for CArray<'_, ElementType> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<ElementType> fmt::Debug for CArray<'_, ElementType> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CArray")
            .field("pointer", &self.pointer)
            .field("length", &self.length)
            .finish()
    }
}

impl<'borrow_lifetime, ElementType> CArray<'borrow_lifetime, ElementType> {
    /// Borrows `slice` as a C-ABI array view.
    ///
    /// The returned [`CArray`] is tied to the lifetime of `slice`; the pointer remains valid as
    /// long as the original slice is not moved or dropped.
    pub const fn from_slice(slice: &'borrow_lifetime [ElementType]) -> Self {
        Self {
            pointer: slice.as_ptr(),
            length: slice.len(),
            _lifetime: PhantomData,
        }
    }

    /// Returns the number of elements in the borrowed view.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.length
    }

    /// Returns `true` if the view contains no elements.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Reconstructs a Rust slice from the raw pointer and length.
    ///
    /// # Returns
    ///
    /// A slice of `length` elements starting at `pointer`.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that:
    ///
    /// * `pointer` points to a single, contiguous allocation of at least `length` elements of
    ///   `ElementType`, properly initialized.
    /// * The memory remains valid and immutable for the returned lifetime.
    /// * `length * size_of::<ElementType>()` does not exceed `isize::MAX`.
    #[must_use]
    pub const unsafe fn as_slice(&self) -> &'borrow_lifetime [ElementType] {
        // SAFETY: the caller upholds the invariants documented above.
        unsafe { std::slice::from_raw_parts(self.pointer, self.length) }
    }
}

/// Borrowed view of a UTF-8 string as a `char`-typed C array.
pub type CCharArray<'borrow_lifetime> = CArray<'borrow_lifetime, c_char>;

/// Borrowed view of a raw byte buffer.
pub type CByteArray<'borrow_lifetime> = CArray<'borrow_lifetime, u8>;

impl<'borrow_lifetime> CCharArray<'borrow_lifetime> {
    /// Borrows `s` as a C-ABI char array view.
    ///
    /// The returned view is **not** NUL-terminated; both sides of the FFI boundary rely on the
    /// explicit `length` field rather than a terminator.
    #[must_use]
    pub const fn from_str(s: &'borrow_lifetime str) -> Self {
        Self {
            pointer: s.as_ptr().cast::<c_char>(),
            length: s.len(),
            _lifetime: PhantomData,
        }
    }

    /// Reconstructs a Rust `&str` from the raw pointer and length.
    ///
    /// # Returns
    ///
    /// A `&str` view of the underlying bytes.
    ///
    /// # Safety
    ///
    /// In addition to the invariants required by [`CArray::as_slice`], the caller must guarantee
    /// that the bytes are valid UTF-8. No validation is performed.
    #[must_use]
    pub const unsafe fn as_str(&self) -> &'borrow_lifetime str {
        // SAFETY: the caller guarantees pointer validity and UTF-8 correctness.
        let bytes = unsafe { std::slice::from_raw_parts(self.pointer.cast::<u8>(), self.length) };
        // SAFETY: the caller guarantees the bytes are valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }
}

/// Owned, C-ABI-compatible result buffer returned from a TDL package's `execute` entry point.
///
/// The buffer is allocated on the TDL-package side by leaking a `Box<[u8]>` via [`Box::into_raw`]
/// and reclaimed on the executor side via [`Box::from_raw`]. This only works because both sides
/// share the same global allocator, which is true when the package is loaded via `dlopen` into
/// the executor process.
///
/// Instances are produced by [`TaskExecutionResult::from_outputs`] /
/// [`TaskExecutionResult::from_error`] and consumed exactly once by
/// [`TaskExecutionResult::into_result`]; using any other lifecycle risks a double free or leak.
#[repr(C)]
pub struct TaskExecutionResult {
    is_error: bool,
    pointer: *mut u8,
    length: usize,
}

impl TaskExecutionResult {
    /// Constructs a successful result wrapping wire-format-encoded output bytes.
    #[must_use]
    pub fn from_outputs(bytes: Vec<u8>) -> Self {
        Self::from_buffer(false, bytes)
    }

    /// Constructs a failing result wrapping msgpack-encoded `TdlError` bytes.
    #[must_use]
    pub fn from_error(bytes: Vec<u8>) -> Self {
        Self::from_buffer(true, bytes)
    }

    /// Reclaims ownership of the leaked buffer and returns it.
    ///
    /// This must be called **exactly once** for every value produced by [`Self::from_outputs`] or
    /// [`Self::from_error`]; the reclaimed `Vec<u8>` takes back allocator ownership and will free
    /// the buffer when dropped.
    ///
    /// # Returns
    ///
    /// `Ok(bytes)` on success, where `bytes` is the wire-format output payload produced by the
    /// user task.
    ///
    /// # Errors
    ///
    /// Returns `Err(bytes)` if the result represented failure, where `bytes` is a
    /// msgpack-encoded `TdlError` produced inside the TDL package. The caller is responsible for
    /// decoding it via `rmp_serde::from_slice`.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that:
    ///
    /// * `self.pointer` / `self.length` originated from a prior call to [`Self::from_outputs`] or
    ///   [`Self::from_error`] in a component that shares this process's global allocator.
    /// * The buffer has not already been reclaimed.
    pub unsafe fn into_result(self) -> Result<Vec<u8>, Vec<u8>> {
        // Prevent the destructor from running after we reconstruct the `Box`.
        let this = ManuallyDrop::new(self);
        // SAFETY: the caller upholds the invariants documented above. The buffer was produced by
        // `Box::<[u8]>::into_raw`, and the same length is recorded in `this.length`, so
        // reconstructing the slice and boxing it is sound.
        let boxed: Box<[u8]> = unsafe {
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                this.pointer,
                this.length,
            ))
        };
        let vec = boxed.into_vec();
        if this.is_error { Err(vec) } else { Ok(vec) }
    }

    /// Converts an [`ExecutionResult`] into its C-ABI-compatible form.
    ///
    /// This is the primary conversion used by the `register_tasks!` macro's generated
    /// `__spider_tdl_package_execute` entry point.
    #[must_use]
    pub fn from_execution_result(result: crate::ExecutionResult) -> Self {
        match result {
            crate::ExecutionResult::Outputs(bytes) => Self::from_outputs(bytes),
            crate::ExecutionResult::Error(bytes) => Self::from_error(bytes),
        }
    }

    fn from_buffer(is_error: bool, buffer: Vec<u8>) -> Self {
        let boxed: Box<[u8]> = buffer.into_boxed_slice();
        let length = boxed.len();
        let pointer = Box::into_raw(boxed).cast::<u8>();
        Self {
            is_error,
            pointer,
            length,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CByteArray, CCharArray, TaskExecutionResult};

    #[test]
    fn c_byte_array_round_trip() {
        let data: [u8; 5] = [1, 2, 3, 4, 5];
        let view = CByteArray::from_slice(&data);
        assert_eq!(view.len(), 5);
        assert!(!view.is_empty());
        // SAFETY: `data` is still alive, so the borrowed view is valid.
        let reconstructed = unsafe { view.as_slice() };
        assert_eq!(reconstructed, &data[..]);
    }

    #[test]
    fn c_byte_array_empty() {
        let data: [u8; 0] = [];
        let view = CByteArray::from_slice(&data);
        assert_eq!(view.len(), 0);
        assert!(view.is_empty());
    }

    #[test]
    fn c_char_array_round_trip() {
        let original = "hello, TDL";
        let view = CCharArray::from_str(original);
        assert_eq!(view.len(), original.len());
        // SAFETY: `original` is still alive and is valid UTF-8.
        let reconstructed = unsafe { view.as_str() };
        assert_eq!(reconstructed, original);
    }

    #[test]
    fn task_execution_result_success_round_trip() {
        let payload = vec![10u8, 20, 30, 40];
        let expected = payload.clone();
        let result = TaskExecutionResult::from_outputs(payload);
        // SAFETY: the result was freshly produced by `from_outputs` and not reclaimed yet.
        let reclaimed = unsafe { result.into_result() };
        assert_eq!(reclaimed, Ok(expected));
    }

    #[test]
    fn task_execution_result_error_round_trip() {
        let payload = vec![0xdeu8, 0xad, 0xbe, 0xef];
        let expected = payload.clone();
        let result = TaskExecutionResult::from_error(payload);
        // SAFETY: the result was freshly produced by `from_error` and not reclaimed yet.
        let reclaimed = unsafe { result.into_result() };
        assert_eq!(reclaimed, Err(expected));
    }

    #[test]
    fn task_execution_result_empty_buffer() {
        let result = TaskExecutionResult::from_outputs(Vec::new());
        // SAFETY: freshly produced, single consumption.
        let reclaimed = unsafe { result.into_result() };
        assert_eq!(reclaimed, Ok(Vec::new()));
    }
}
