//! `#[repr(C)]` types shared across the TDL package / task executor C-FFI boundary.
//!
//! Both sides of the boundary live in the same process and share the same Rust global allocator,
//! so buffers allocated on one side can be reclaimed on the other via [`Box::into_raw`] /
//! [`Box::from_raw`].

use std::{ffi::c_char, fmt, marker::PhantomData, mem::ManuallyDrop, str::Utf8Error};

/// Borrowed, C-ABI-compatible view of a contiguous slice `&'borrow_lifetime [ElementType]`.
///
/// # Type Parameters
///
/// * `'borrow_lifetime` - The lifetime of the borrowed slice.
/// * `ElementType` - The type of the element inside the slice.
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
    ///
    /// # Returns
    ///
    /// The constructed C array from the given slice.
    pub const fn from_slice(slice: &'borrow_lifetime [ElementType]) -> Self {
        Self {
            pointer: slice.as_ptr(),
            length: slice.len(),
            _lifetime: PhantomData,
        }
    }

    /// # Returns
    ///
    /// The length of the array (the number of elements).
    #[must_use]
    pub const fn len(&self) -> usize {
        self.length
    }

    /// # Returns
    ///
    /// Whether the array is empty.
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
    #[must_use]
    pub const fn as_slice(&self) -> &'borrow_lifetime [ElementType] {
        unsafe { std::slice::from_raw_parts(self.pointer, self.length) }
    }
}

/// Borrowed view of a UTF-8 string as a `char`-typed C array.
pub type CCharArray<'borrow_lifetime> = CArray<'borrow_lifetime, c_char>;

impl<'borrow_lifetime> CCharArray<'borrow_lifetime> {
    /// Borrows UTF8-encoded string `s` as a C-ABI char array view.
    ///
    /// The returned view is **not** NUL-terminated; both sides of the FFI boundary rely on the
    /// explicit `length` field rather than a terminator.
    ///
    /// Equivalent to `std::string_view` in C++.
    ///
    /// # Returns
    ///
    /// The constructed C char array from the given `&str`.
    #[must_use]
    pub const fn from_utf8(s: &'borrow_lifetime str) -> Self {
        Self {
            pointer: s.as_ptr().cast::<c_char>(),
            length: s.len(),
            _lifetime: PhantomData,
        }
    }

    /// Reconstructs a UTF8-encoded Rust `&str` from the raw pointer and length.
    ///
    /// # Returns
    ///
    /// A `&str` view of the underlying C char array on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`str::from_utf8`]'s return values on failure.
    pub const fn as_utf8(&self) -> Result<&'borrow_lifetime str, Utf8Error> {
        let bytes: &[u8] =
            unsafe { std::slice::from_raw_parts(self.pointer.cast::<u8>(), self.length) };
        str::from_utf8(bytes)
    }
}

/// Borrowed view of a raw byte buffer.
pub type CByteArray<'borrow_lifetime> = CArray<'borrow_lifetime, u8>;

/// Owned, C-ABI-compatible result buffer returned from a TDL task execution.
///
/// The buffer is allocated on the TDL-package side by leaking a `Box<[u8]>` via [`Box::into_raw`]
/// and reclaimed on the executor side via [`Box::from_raw`]. This only works because both sides
/// share the same global allocator, which is true when the package is loaded via `dlopen` into
/// the executor process.
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

    /// Constructs a failing result wrapping msgpack-encoded [`TdlError`](crate::TdlError) bytes.
    #[must_use]
    pub fn from_error(bytes: Vec<u8>) -> Self {
        Self::from_buffer(true, bytes)
    }

    /// Reclaims ownership of the leaked buffer and returns it.
    ///
    /// # Returns
    ///
    /// `Ok(bytes)` on success, where `bytes` is the wire-format output payload produced by the
    /// user task.
    ///
    /// # Errors
    ///
    /// Returns `Err(bytes)` if the result represented failure, where `bytes` is a msgpack-encoded
    /// [`crate::TdlError`] produced inside the TDL package. The caller is responsible for decoding
    /// it via [`rmp_serde::from_slice`].
    ///
    /// # Safety
    ///
    /// The caller must guarantee that `self.pointer` / `self.length` originated from a prior call
    /// to [`Self::from_outputs`] or [`Self::from_error`] in a component that shares this process's
    /// global allocator.
    pub fn into_result(self) -> Result<Vec<u8>, Vec<u8>> {
        // Prevent the destructor from running after we reconstruct the `Box`.
        let this = ManuallyDrop::new(self);
        let boxed: Box<[u8]> = unsafe {
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                this.pointer,
                this.length,
            ))
        };
        let vec = boxed.into_vec();
        if this.is_error { Err(vec) } else { Ok(vec) }
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
    use crate::TdlError;

    #[test]
    fn c_byte_array_round_trip() {
        let data: [u8; 5] = [1, 2, 3, 4, 5];
        let view = CByteArray::from_slice(&data);
        assert_eq!(view.len(), 5);
        assert!(!view.is_empty());
        let reconstructed = view.as_slice();
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
    fn c_char_array_round_trip() -> anyhow::Result<()> {
        let original = "hello, TDL";
        let view = CCharArray::from_utf8(original);
        assert_eq!(view.len(), original.len());
        let reconstructed = view.as_utf8()?;
        assert_eq!(reconstructed, original);
        Ok(())
    }

    #[test]
    fn c_char_array_invalid_utf8() {
        // 0xFF is never valid in any position of a UTF-8 sequence.
        let invalid_bytes: &[u8] = &[0x68, 0x65, 0xff, 0x6c, 0x6f];
        // Reinterpret as `&[c_char]` to go through `CArray::from_slice` rather than `from_utf8`.
        let c_chars: &[std::ffi::c_char] = unsafe {
            std::slice::from_raw_parts(invalid_bytes.as_ptr().cast(), invalid_bytes.len())
        };
        let view = CCharArray::from_slice(c_chars);
        assert_eq!(view.len(), 5);
        assert!(view.as_utf8().is_err());
    }

    #[test]
    fn task_execution_result_success_round_trip() {
        let payload = vec![10u8, 20, 30, 40];
        let expected = payload.clone();
        let result = TaskExecutionResult::from_outputs(payload);
        let reclaimed = result.into_result();
        assert_eq!(reclaimed, Ok(expected));
    }

    #[test]
    fn task_execution_result_error_round_trip() -> anyhow::Result<()> {
        let error = TdlError::Custom("custom task execution error".to_owned());
        let payload = rmp_serde::to_vec(&error)?;
        let result = TaskExecutionResult::from_error(payload);
        let reclaimed = result.into_result();
        if let Err(payload) = reclaimed {
            let decoded: TdlError = rmp_serde::from_slice(&payload)?;
            assert_eq!(decoded.to_string(), error.to_string());
        } else {
            panic!("reclaimed payload did not match original");
        }
        Ok(())
    }

    #[test]
    fn task_execution_result_empty_buffer() {
        let result = TaskExecutionResult::from_outputs(Vec::new());
        let reclaimed = result.into_result();
        assert_eq!(reclaimed, Ok(Vec::new()));
    }
}
