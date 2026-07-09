//! Wire-format framing for length-prefixed payload sequences.
//!
//! The wire format is a thin, length-prefixed framing layer that wraps an ordered sequence of
//! opaque byte payloads:
//!
//! ```text
//!   [count: u32 LE] [len_0: u32 LE][payload_0 ...] [len_1: u32 LE][payload_1 ...] ...
//! ```
//!
//! The wire layer never interprets the payload bytes -- that responsibility belongs to the payload
//! layer (msgpack, via `rmp-serde`). Field-level deserialization is zero-copy: each payload is
//! handed to `rmp_serde` as a borrowed slice into the original wire buffer.
//!
//! This module provides the generic framing primitives. Higher-level, domain-specific serializers
//! (such as `spider_core`'s task input/output serializers) are layered on top of these.

use std::fmt;

use serde::de::DeserializeSeed;
use serde::de::SeqAccess;
use serde::de::Visitor;
use serde::de::{self};
use serde::ser;

/// Errors produced while framing or unframing a wire buffer.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// The encoded payload count does not match the destination struct's field count.
    #[error("`{type_name}`: expected {expected} payloads, got {actual}")]
    LengthMismatch {
        type_name: &'static str,
        expected: usize,
        actual: usize,
    },

    /// A single payload failed to decode from its msgpack bytes.
    #[error(
        "`{type_name}::{field}` (position {position}): failed to decode msgpack payload: {source}"
    )]
    FieldDeserialization {
        type_name: &'static str,
        field: &'static str,
        position: usize,
        #[source]
        source: rmp_serde::decode::Error,
    },

    /// The wire buffer is malformed -- truncated, corrupted, or otherwise not a valid framing
    /// of a payload sequence.
    #[error("invalid wire format: {0}")]
    InvalidFormat(&'static str),

    /// A value exceeds the wire format's `u32` size limit during serialization.
    #[error("wire format overflow: {0}")]
    Overflow(String),

    /// Catch-all bucket required by [`serde::de::Error`] for deserializer-reported errors that
    /// do not fit any specific variant.
    #[error("{0}")]
    Custom(String),
}

impl de::Error for WireError {
    fn custom<MessageType: fmt::Display>(msg: MessageType) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl ser::Error for WireError {
    fn custom<MessageType: fmt::Display>(msg: MessageType) -> Self {
        Self::Custom(msg.to_string())
    }
}

/// Serializes a tuple by decomposing it into individual elements, encoding each element as
/// msgpack, and framing them in the wire format.
///
/// This drives serde's `Serialize` impl for the tuple: serde calls `serialize_tuple(len)`
/// followed by `serialize_element` for each element. A custom [`TupleOutputSerializer`]
/// intercepts these calls and frames each element into the wire buffer.
///
/// # Type Parameters
///
/// * `TupleType` - The tuple type to serialize. Must implement [`serde::Serialize`].
///
/// # Returns
///
/// The wire-format byte stream on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`WireFrameBuilder::append_serialize`]'s return values on failure.
/// * Forwards [`TupleOutputSerializer`]'s return values on failure to reject non-tuple type inputs
///   at runtime.
pub fn serialize_tuple<TupleType: serde::Serialize>(
    value: &TupleType,
) -> Result<Vec<u8>, WireError> {
    value.serialize(TupleOutputSerializer {
        builder: WireFrameBuilder::new(),
    })
}

/// Parses a wire-format byte stream and extracts each payload as an owned `Vec<u8>`.
///
/// # Returns
///
/// The unframed wire-format buffer with each element extracted as raw bytes.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`WireFrameBuilder::unframe_payloads`]'s return values on failure.
pub fn unframe(buf: &[u8]) -> Result<Vec<Vec<u8>>, WireError> {
    WireFrameBuilder::unframe_payloads(buf)
}

/// Generates `serialize_*` methods on a `serde::Serializer` impl that all return the same error
/// forwarded from [`unsupported_type_error`]. Covers the two method shapes in the trait:
///
/// * `primitive`: `fn method(self, _: Type) -> Result<Self::Ok, Self::Error>`
/// * `compound`: `fn method(self, ...) -> Result<Self::AssocType, Self::Error>`
macro_rules! reject_non_tuple {
    (primitive: $($method:ident($prim:ty)),* $(,)?) => {
        $(
            fn $method(self, _: $prim) -> Result<Self::Ok, Self::Error> {
                Err(unsupported_type_error())
            }
        )*
    };

    (compound: $($method:ident($($arg:ident: $ty:ty),*) -> $assoc:ty),* $(,)?) => {
        $(
            fn $method(self, $($arg: $ty),*) -> Result<$assoc, Self::Error> {
                Err(unsupported_type_error())
            }
        )*
    };
}

/// Length of the wire header recording the payload count, in bytes.
const COUNT_HEADER_LEN: usize = 4;

/// Length of the per-payload length prefix, in bytes.
const FIELD_LEN_PREFIX_LEN: usize = 4;

/// Streaming wire-format builder that frames a sequence of byte payloads.
///
/// Reserves space for the `u32` count header upfront and patches it in [`Self::release`] once
/// the final count is known. Each [`Self::append_payload`] call writes a length-prefixed payload
/// directly into the buffer.
pub struct WireFrameBuilder {
    buffer: Vec<u8>,
    count: u32,
}

impl Default for WireFrameBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl WireFrameBuilder {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The newly created wire frame builder.
    #[must_use]
    pub fn new() -> Self {
        let buffer = vec![0u8; COUNT_HEADER_LEN];
        Self { buffer, count: 0 }
    }

    /// Appends the given byte payload as a new frame to the underlying buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`WireError::Overflow`] if the length of `payload` exceeds [`u32::MAX`].
    /// * Forwards [`WireFrameBuilder::increment_count`]'s return values on failure.
    pub fn append_payload(&mut self, payload: &[u8]) -> Result<(), WireError> {
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            WireError::Overflow(format!(
                "payload length {} bytes exceeds u32::MAX",
                payload.len()
            ))
        })?;
        self.increment_count()?;
        self.buffer.extend_from_slice(&payload_len.to_le_bytes());
        self.buffer.extend_from_slice(payload);
        Ok(())
    }

    /// Serializes `value` into msgpack directly into the buffer with a length prefix.
    ///
    /// Writes a placeholder `u32` length, serializes the value in-place, then back-patches the
    /// length with the actual payload size.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`WireError::Custom`] if [`rmp_serde::encode::write`] fails to serialize `value`.
    /// * [`WireError::Overflow`] if the serialized payload length exceeds [`u32::MAX`].
    /// * Forwards [`WireFrameBuilder::increment_count`]'s return values on failure.
    pub fn append_serialize<ValueType: serde::Serialize + ?Sized>(
        &mut self,
        value: &ValueType,
    ) -> Result<(), WireError> {
        self.increment_count()?;

        // Reserve space for the length prefix.
        let len_offset = self.buffer.len();
        self.buffer.extend_from_slice(&0u32.to_le_bytes());

        // Serialize directly into the buffer.
        rmp_serde::encode::write(&mut self.buffer, value)
            .map_err(|e| WireError::Custom(format!("msgpack serialization failed: {e}")))?;

        // Back-patch the length prefix.
        let payload_len = self.buffer.len() - len_offset - FIELD_LEN_PREFIX_LEN;
        let payload_len_u32 = u32::try_from(payload_len).map_err(|_| {
            WireError::Overflow(format!(
                "payload length {payload_len} bytes exceeds u32::MAX"
            ))
        })?;
        self.buffer[len_offset..len_offset + FIELD_LEN_PREFIX_LEN]
            .copy_from_slice(&payload_len_u32.to_le_bytes());
        Ok(())
    }

    /// Increments the count for the next frame.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`WireError::Overflow`] if the total count exceeds [`u32::MAX`].
    fn increment_count(&mut self) -> Result<(), WireError> {
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| WireError::Overflow("payload count exceeds u32::MAX".to_owned()))?;
        Ok(())
    }

    /// Finalizes the count header and returns the completed wire-format buffer.
    ///
    /// # Returns
    ///
    /// Completed wire-format buffer.
    #[must_use]
    pub fn release(mut self) -> Vec<u8> {
        self.buffer[..COUNT_HEADER_LEN].copy_from_slice(&self.count.to_le_bytes());
        self.buffer
    }

    /// Parses a wire-format byte stream and extracts each payload as an owned `Vec<u8>`.
    ///
    /// # Returns
    ///
    /// A vector of payloads on success, one per wire-format element.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`WireError::InvalidFormat`] if the buffer is too small to contain the count header, or if
    ///   the declared field length extends past the end of the buffer.
    pub fn unframe_payloads(data: &[u8]) -> Result<Vec<Vec<u8>>, WireError> {
        let count_bytes =
            data.first_chunk::<COUNT_HEADER_LEN>()
                .ok_or(WireError::InvalidFormat(
                    "buffer too small for the payload count header",
                ))?;
        let count = u32::from_le_bytes(*count_bytes) as usize;

        let mut pos = COUNT_HEADER_LEN;
        let mut payloads = Vec::with_capacity(count);
        for _ in 0..count {
            let len_bytes = data
                .get(pos..)
                .and_then(<[u8]>::first_chunk::<FIELD_LEN_PREFIX_LEN>)
                .ok_or(WireError::InvalidFormat(
                    "unexpected end of buffer reading payload length",
                ))?;
            let field_len = u32::from_le_bytes(*len_bytes) as usize;
            pos += FIELD_LEN_PREFIX_LEN;

            if pos + field_len > data.len() {
                return Err(WireError::InvalidFormat(
                    "unexpected end of buffer reading payload data",
                ));
            }
            payloads.push(data[pos..pos + field_len].to_vec());
            pos += field_len;
        }
        Ok(payloads)
    }
}

/// Single-pass, zero-copy cursor over a wire-format byte stream.
///
/// Holds a borrowed slice of the wire buffer and a position cursor. The cursor advances each
/// time a field is consumed, yielding borrowed slices into the buffer that can be handed to
/// `rmp_serde` for payload deserialization.
///
/// The cursor implements [`serde::Deserializer`] (on `&mut StreamDeserializer`), driving struct
/// deserialization where each struct field positionally consumes one wire-format payload.
///
/// # Type Parameters
///
/// * `'de` - The lifetime of the borrowed wire buffer slice. Using `'de` instead of
///   `'deserializer_lifetime` because this is required by [`serde::forward_to_deserialize_any`].
pub struct StreamDeserializer<'de> {
    data: &'de [u8],
    pos: usize,
    count: usize,
    current_field: usize,
    type_name: &'static str,
    field_names: &'static [&'static str],
}

impl<'de> StreamDeserializer<'de> {
    /// Factory function.
    ///
    /// Parses the wire-format count header and initializes a cursor positioned immediately after
    /// it. The `type_name` and `field_names` fields are left as placeholders and are populated by
    /// [`serde::Deserializer::deserialize_struct`] once the target struct's metadata is known.
    ///
    /// # Returns
    ///
    /// A cursor over `data` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`WireError::InvalidFormat`] if `data` is shorter than the 4-byte count header.
    pub fn new(data: &'de [u8]) -> Result<Self, WireError> {
        let count_bytes =
            data.first_chunk::<COUNT_HEADER_LEN>()
                .ok_or(WireError::InvalidFormat(
                    "buffer too small for the payload count header",
                ))?;
        let count = u32::from_le_bytes(*count_bytes) as usize;
        Ok(Self {
            data,
            pos: COUNT_HEADER_LEN,
            count,
            current_field: 0,
            type_name: "<unknown>",
            field_names: &[],
        })
    }

    /// Advances the cursor past the next length-prefixed payload and returns a borrowed slice
    /// of the payload bytes.
    ///
    /// The returned slice is a zero-copy view into the original wire buffer; it can be handed
    /// directly to `rmp_serde` without an intermediate allocation.
    ///
    /// # Returns
    ///
    /// A slice of the payload bytes on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`WireError::InvalidFormat`] if:
    ///   * The buffer ends before the payload length prefix can be read.
    ///   * The declared payload length extends past the end of the buffer.
    fn next_field_bytes(&mut self) -> Result<&'de [u8], WireError> {
        let len_bytes = self
            .data
            .get(self.pos..)
            .and_then(<[u8]>::first_chunk::<FIELD_LEN_PREFIX_LEN>)
            .ok_or(WireError::InvalidFormat(
                "unexpected end of buffer reading payload length",
            ))?;
        let field_len = u32::from_le_bytes(*len_bytes) as usize;
        self.pos += FIELD_LEN_PREFIX_LEN;

        if self.pos + field_len > self.data.len() {
            return Err(WireError::InvalidFormat(
                "unexpected end of buffer reading payload data",
            ));
        }
        let bytes = &self.data[self.pos..self.pos + field_len];
        self.pos += field_len;
        Ok(bytes)
    }
}

/// Serde [`serde::Deserializer`] impl that drives struct deserialization from a wire-format
/// byte stream.
///
/// Only [`Self::deserialize_struct`] is meaningful: it validates the payload count against the
/// struct's field count, stores the struct name and field names for error reporting, and
/// delegates to [`FieldSeqAccess`] to visit each field positionally. All other `deserialize_*`
/// methods route through [`Self::deserialize_any`], which always returns [`WireError::Custom`]
/// since the wire format only carries struct-shaped data.
impl<'de> serde::Deserializer<'de> for &mut StreamDeserializer<'de> {
    type Error = WireError;

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map enum identifier ignored_any
    }

    fn deserialize_struct<VisitorType: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: VisitorType,
    ) -> Result<VisitorType::Value, WireError> {
        if self.count != fields.len() {
            return Err(WireError::LengthMismatch {
                type_name: name,
                expected: fields.len(),
                actual: self.count,
            });
        }
        self.type_name = name;
        self.field_names = fields;
        visitor.visit_seq(FieldSeqAccess {
            stream_deserializer: self,
        })
    }

    fn deserialize_any<VisitorType: Visitor<'de>>(
        self,
        _: VisitorType,
    ) -> Result<VisitorType::Value, WireError> {
        Err(WireError::Custom(
            "wire stream can only be deserialized into a struct".to_owned(),
        ))
    }
}

/// Per-field adapter that hands the next payload of a wire frame to a serde visitor.
///
/// # Type Parameters
///
/// * `'borrow_lifetime` - The borrowed lifetime of [`StreamDeserializer`].
/// * `'de` - The lifetime of the wire buffer slice, passed through from [`StreamDeserializer`].
struct FieldSeqAccess<'borrow_lifetime, 'de> {
    stream_deserializer: &'borrow_lifetime mut StreamDeserializer<'de>,
}

impl<'de> SeqAccess<'de> for FieldSeqAccess<'_, 'de> {
    type Error = WireError;

    fn next_element_seed<SeedType: DeserializeSeed<'de>>(
        &mut self,
        seed: SeedType,
    ) -> Result<Option<SeedType::Value>, WireError> {
        if self.stream_deserializer.current_field == self.stream_deserializer.count {
            return Ok(None);
        }
        if self.stream_deserializer.current_field > self.stream_deserializer.count {
            return Err(WireError::LengthMismatch {
                type_name: self.stream_deserializer.type_name,
                expected: self.stream_deserializer.count,
                actual: self.stream_deserializer.current_field + 1,
            });
        }

        let idx = self.stream_deserializer.current_field;
        let field_name = self
            .stream_deserializer
            .field_names
            .get(idx)
            .copied()
            .unwrap_or("<unknown>");
        let type_name = self.stream_deserializer.type_name;

        let bytes = self.stream_deserializer.next_field_bytes()?;
        self.stream_deserializer.current_field += 1;

        let mut rmp_deserializer = rmp_serde::Deserializer::from_read_ref(bytes);
        seed.deserialize(&mut rmp_deserializer)
            .map(Some)
            .map_err(|source| WireError::FieldDeserialization {
                type_name,
                field: field_name,
                position: idx,
                source,
            })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.stream_deserializer.count - self.stream_deserializer.current_field)
    }
}

/// Custom serde [`serde::Serializer`] that decomposes a tuple into individually encoded wire
/// payloads via a [`WireFrameBuilder`].
///
/// Only `serialize_tuple` (and `serialize_unit` for the empty-tuple case) are supported. All other
/// serialization methods return an error to indicate type rejection at runtime.
struct TupleOutputSerializer {
    builder: WireFrameBuilder,
}

impl serde::Serializer for TupleOutputSerializer {
    type Error = WireError;
    type Ok = Vec<u8>;
    type SerializeMap = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeSeq = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Self;
    type SerializeTupleStruct = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = ser::Impossible<Self::Ok, Self::Error>;

    reject_non_tuple! { primitive:
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char),
        serialize_str(&str),
        serialize_bytes(&[u8]),
    }

    reject_non_tuple! { compound:
        serialize_unit_struct(_n: &'static str) -> Self::Ok,
        serialize_unit_variant(_n: &'static str, _i: u32, _v: &'static str) -> Self::Ok,
        serialize_seq(_len: Option<usize>) -> Self::SerializeSeq,
        serialize_tuple_struct(_n: &'static str, _len: usize) -> Self::SerializeTupleStruct,
        serialize_map(_len: Option<usize>) -> Self::SerializeMap,
        serialize_struct(_n: &'static str, _len: usize) -> Self::SerializeStruct,
        serialize_tuple_variant(
            _n: &'static str, _i: u32, _v: &'static str, _len: usize
        ) -> Self::SerializeTupleVariant,
        serialize_struct_variant(
            _n: &'static str, _i: u32, _v: &'static str, _len: usize
        ) -> Self::SerializeStructVariant,
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.builder.release())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(unsupported_type_error())
    }

    fn serialize_some<ValueType: serde::Serialize + ?Sized>(
        self,
        _: &ValueType,
    ) -> Result<Self::Ok, Self::Error> {
        Err(unsupported_type_error())
    }

    fn serialize_newtype_struct<ValueType: serde::Serialize + ?Sized>(
        self,
        _: &'static str,
        _: &ValueType,
    ) -> Result<Self::Ok, Self::Error> {
        Err(unsupported_type_error())
    }

    fn serialize_newtype_variant<ValueType: serde::Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &ValueType,
    ) -> Result<Self::Ok, Self::Error> {
        Err(unsupported_type_error())
    }
}

impl ser::SerializeTuple for TupleOutputSerializer {
    type Error = WireError;
    type Ok = Vec<u8>;

    fn serialize_element<ValueType: serde::Serialize + ?Sized>(
        &mut self,
        value: &ValueType,
    ) -> Result<(), Self::Error> {
        self.builder.append_serialize(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.builder.release())
    }
}

/// # Returns
///
/// A [`WireError`] indicating that the value must be a tuple.
fn unsupported_type_error() -> WireError {
    WireError::Custom("task output must be a tuple".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_length_mismatch() {
        let err = WireError::LengthMismatch {
            type_name: "Foo",
            expected: 2,
            actual: 5,
        };
        assert_eq!(err.to_string(), "`Foo`: expected 2 payloads, got 5");
    }

    #[test]
    fn error_display_field_deserialization() {
        let source =
            rmp_serde::from_slice::<u32>(&[0xc1u8]).expect_err("expected rmp_serde decode error");
        let err = WireError::FieldDeserialization {
            type_name: "Foo",
            field: "bar",
            position: 1,
            source,
        };
        let msg = err.to_string();
        assert!(msg.contains("Foo::bar"));
        assert!(msg.contains("position 1"));
    }
}
