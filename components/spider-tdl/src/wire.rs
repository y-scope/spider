//! Wire-format framing for TDL task inputs and task outputs.
//!
//! The wire format is a thin, length-prefixed framing layer that wraps an ordered sequence of
//! opaque byte payloads. It is used on both sides of the TDL package boundary:
//!
//! * Task inputs, originating in the storage layer, frame a `Vec<TaskInput>` into a byte stream
//!   which the TDL package then deserializes directly into the task's parameter struct.
//! * Task outputs, produced inside the TDL package, frame the elements of the return tuple into a
//!   byte stream which the storage layer later unframes into a `Vec<Vec<u8>>` of per-element
//!   msgpack payloads.
//!
//! ```text
//!   [count: u32 LE] [len_0: u32 LE][payload_0 ...] [len_1: u32 LE][payload_1 ...] ...
//! ```
//!
//! The wire layer never interprets the payload bytes -- that responsibility belongs to the payload
//! layer (msgpack, via `rmp-serde`). Field-level deserialization is zero-copy: each payload is
//! handed to `rmp_serde` as a borrowed slice into the original wire buffer.

use std::fmt;

use serde::de::{self, DeserializeSeed, SeqAccess, Visitor};
use spider_core::types::io::{TaskInput, TaskOutput};

/// Errors produced while framing or unframing a TDL wire buffer.
///
/// [`WireError`] is local to the crate: it describes failures of the wire/payload layer
/// specifically. Higher-level callers translate it into a [`crate::TdlError`] before the error
/// crosses the C-FFI edge.
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

/// Streaming wire-format serializer for task inputs.
///
/// Appends [`TaskInput`] payloads one at a time into an internal buffer, writing each payload's
/// length prefix inline. The count header at the front of the buffer is patched by
/// [`Self::release`] once all inputs have been appended.
///
/// # Example (conceptual)
///
/// ```ignore
/// let mut inputs = TaskInputsSerializer::new();
/// inputs.append(TaskInput::ValuePayload(msgpack_bytes_0))?;
/// inputs.append(TaskInput::ValuePayload(msgpack_bytes_1))?;
/// let wire: Vec<u8> = inputs.release();
/// ```
pub struct TaskInputsSerializer {
    builder: WireFrameBuilder,
}

impl Default for TaskInputsSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskInputsSerializer {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// Newly created serializer with an empty buffer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            builder: WireFrameBuilder::new(),
        }
    }

    /// Appends a single task input to the wire buffer.
    ///
    /// The payload bytes inside the [`TaskInput::ValuePayload`] variant are written directly; no
    /// re-encoding takes place.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`WireFrameBuilder::append_payload`]'s return values on failure.
    pub fn append(&mut self, input: TaskInput) -> Result<(), WireError> {
        let TaskInput::ValuePayload(bytes) = input;
        self.builder.append_payload(&bytes)
    }

    /// Finalizes the count header and returns the completed wire-format buffer.
    ///
    /// # Returns
    ///
    /// Completed wire-format buffer.
    #[must_use]
    pub fn release(self) -> Vec<u8> {
        self.builder.release()
    }

    /// Deserializes a wire-format byte stream directly into a struct of type `TargetType`.
    ///
    /// This is the deserialization counterpart to [`Self::append`] + [`Self::release`]. Each
    /// field of `TargetType` positionally consumes one wire-format payload, which is then
    /// deserialized from msgpack via a zero-copy borrowed slice into `data`.
    ///
    /// # Type Parameters
    ///
    /// * `'deserializer_lifetime` - The lifetime of the wire buffer `data`.
    /// * `TargetType` - The struct to produce. Must implement [`serde::Deserialize`].
    ///
    /// # Returns
    ///
    /// The deserialized struct on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`StreamDeserializer::new`]'s return values on failure.
    /// * Forwards [`TargetType::deserialize`]'s return values on failure.
    pub fn deserialize<
        'deserializer_lifetime,
        TargetType: serde::Deserialize<'deserializer_lifetime>,
    >(
        data: &'deserializer_lifetime [u8],
    ) -> Result<TargetType, WireError> {
        let mut deserializer = StreamDeserializer::new(data)?;
        TargetType::deserialize(&mut deserializer)
    }
}

/// Streaming wire-format serializer and deserializer for task outputs.
///
/// On the serialization side, pre-encoded msgpack payloads (one per tuple element) are appended
/// via [`Self::append`], and the final wire buffer is obtained from [`Self::release`].
///
/// On the deserialization side, [`Self::deserialize`] extracts each payload as an opaque
/// `Vec<u8>` without decoding the msgpack contents.
pub struct TaskOutputsSerializer {
    builder: WireFrameBuilder,
}

impl Default for TaskOutputsSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskOutputsSerializer {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// Newly created serializer with an empty buffer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            builder: WireFrameBuilder::new(),
        }
    }

    /// Serializes `value` into msgpack and appends the encoded bytes directly to the wire buffer.
    ///
    /// The msgpack encoding is written in-place: a placeholder length prefix is reserved, the
    /// value is serialized into the buffer, and the prefix is back-patched with the actual
    /// payload size. This avoids allocating an intermediate `Vec<u8>` per element.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`WireFrameBuilder::append_serialize`]'s return values on failure.
    pub fn append<ValueType: serde::Serialize>(
        &mut self,
        value: &ValueType,
    ) -> Result<(), WireError> {
        self.builder.append_serialize(value)
    }

    /// Finalizes the count header and returns the completed wire-format buffer.
    ///
    /// # Returns
    ///
    /// Completed wire-format buffer.
    #[must_use]
    pub fn release(self) -> Vec<u8> {
        self.builder.release()
    }

    /// Deserializes a wire-format byte stream into a vector of [`TaskOutput`] values.
    ///
    /// Each payload is extracted as an opaque `Vec<u8>`. The msgpack contents are **not**
    /// decoded here since the storage layer only stores the raw bytes.
    ///
    /// # Returns
    ///
    /// A vector of output payloads on success, one per wire-format element.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`WireFrameBuilder::unframe_payloads`]' return values on failure.
    pub fn deserialize(data: &[u8]) -> Result<Vec<TaskOutput>, WireError> {
        WireFrameBuilder::unframe_payloads(data)
    }
}

/// Length of the wire header recording the payload count, in bytes.
const COUNT_HEADER_LEN: usize = 4;

/// Length of the per-payload length prefix, in bytes.
const FIELD_LEN_PREFIX_LEN: usize = 4;

/// Streaming wire-format builder shared by [`TaskInputsSerializer`] and [`TaskOutputsSerializer`].
///
/// Reserves space for the `u32` count header upfront and patches it in [`Self::release`] once
/// the final count is known. Each [`Self::append_payload`] call writes a length-prefixed payload
/// directly into the buffer.
struct WireFrameBuilder {
    buffer: Vec<u8>,
    count: u32,
}

impl WireFrameBuilder {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// The newly created wire frame builder.
    #[must_use]
    fn new() -> Self {
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
    fn append_payload(&mut self, payload: &[u8]) -> Result<(), WireError> {
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
    fn append_serialize<ValueType: serde::Serialize>(
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
    fn release(mut self) -> Vec<u8> {
        self.buffer[..COUNT_HEADER_LEN].copy_from_slice(&self.count.to_le_bytes());
        self.buffer
    }

    /// Parses a wire-format byte stream and extracts each payload as an owned `Vec<u8>`.
    ///
    /// # Returns
    ///
    /// A vector of output payloads on success, one per wire-format element.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`WireError::InvalidFormat`] if the buffer is too small to contain the count header, or if
    ///   the declared field length extends past the end of the buffer.
    fn unframe_payloads(data: &[u8]) -> Result<Vec<Vec<u8>>, WireError> {
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
/// # Type Parameters
///
/// * `'de` - The lifetime of the borrowed wire buffer slice. Using `'de` instead of
///   `'deserializer_lifetime` because this is required by [`serde::forward_to_deserialize_any`].
struct StreamDeserializer<'de> {
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
    fn new(data: &'de [u8]) -> Result<Self, WireError> {
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

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use spider_core::types::io::{TaskInput, TaskOutput};

    use super::*;
    use crate::{
        std::{int32, int64},
        r#std::{Bytes, List, Map, int8, int16},
    };

    #[derive(Debug, PartialEq, Deserialize)]
    struct Job {
        name: String,
        priority: int32,
        payload: List<int8>,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct Single {
        value: int64,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct Empty {}

    #[derive(Debug, PartialEq, serde::Serialize, Deserialize)]
    struct Inner {
        x: int64,
        y: int64,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct Outer {
        label: String,
        point: Inner,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct Params {
        greeting: String,
        count: int64,
    }

    #[derive(Debug, PartialEq, serde::Serialize, Deserialize)]
    struct A {
        map: Map<int8, Bytes>,
        list: List<int8>,
    }

    #[derive(Debug, PartialEq, serde::Serialize, Deserialize)]
    struct B {
        a: A,
        value: int16,
        list_map: Map<int8, List<int8>>,
    }

    #[derive(Debug, PartialEq, serde::Serialize, Deserialize)]
    struct EmptyInner {}

    #[derive(Debug, PartialEq, serde::Serialize, Deserialize)]
    struct C {
        a: A,
        empty: EmptyInner,
        b: int16,
    }

    /// msgpack-encodes a single value as a payload.
    fn encode<ValueType: serde::Serialize>(value: &ValueType) -> Vec<u8> {
        rmp_serde::to_vec(value).expect("msgpack encoding failed")
    }

    #[test]
    fn wire_frame_byte_layout() -> anyhow::Result<()> {
        const FIRST_PAYLOAD: u8 = 42;
        const SECOND_PAYLOAD: &str = "hi";

        let mut outputs = TaskOutputsSerializer::new();
        outputs.append(&FIRST_PAYLOAD)?;
        outputs.append(&SECOND_PAYLOAD)?;
        let wire = outputs.release();

        let encoded_first = encode(&FIRST_PAYLOAD);
        let encoded_second = encode(&SECOND_PAYLOAD);

        let mut expected = Vec::new();
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&u32::try_from(encoded_first.len())?.to_le_bytes());
        expected.extend_from_slice(&encoded_first);
        expected.extend_from_slice(&u32::try_from(encoded_second.len())?.to_le_bytes());
        expected.extend_from_slice(&encoded_second);
        assert_eq!(wire, expected);
        Ok(())
    }

    #[test]
    fn task_inputs_streaming_round_trip() -> anyhow::Result<()> {
        let mut inputs = TaskInputsSerializer::new();
        inputs.append(TaskInput::ValuePayload(encode(&"hello".to_owned())))?;
        inputs.append(TaskInput::ValuePayload(encode(&42u32)))?;
        let wire = inputs.release();

        let params: Params = TaskInputsSerializer::deserialize(&wire)?;
        assert_eq!(params.greeting, "hello");
        assert_eq!(params.count, 42);
        Ok(())
    }

    #[test]
    fn task_inputs_empty() -> anyhow::Result<()> {
        let wire = TaskInputsSerializer::new().release();
        assert_eq!(wire, 0u32.to_le_bytes());

        let value: Empty = TaskInputsSerializer::deserialize(&wire)?;
        assert_eq!(value, Empty {});
        Ok(())
    }

    #[test]
    fn task_inputs_nested_struct() -> anyhow::Result<()> {
        const LABEL: &str = "origin";
        const POINT: Inner = Inner { x: -10, y: 42 };

        let mut inputs = TaskInputsSerializer::new();
        inputs.append(TaskInput::ValuePayload(encode(&LABEL.to_owned())))?;
        inputs.append(TaskInput::ValuePayload(encode(&POINT)))?;
        let wire = inputs.release();

        let outer: Outer = TaskInputsSerializer::deserialize(&wire)?;
        assert_eq!(
            outer,
            Outer {
                label: LABEL.to_owned(),
                point: POINT,
            }
        );
        Ok(())
    }

    #[test]
    fn task_inputs_deserialize_length_mismatch() {
        let mut inputs = TaskInputsSerializer::new();
        inputs
            .append(TaskInput::ValuePayload(encode(&"only-one".to_owned())))
            .expect("append failed");
        let wire = inputs.release();

        let err =
            TaskInputsSerializer::deserialize::<Job>(&wire).expect_err("expected length mismatch");
        match err {
            WireError::LengthMismatch {
                type_name,
                expected,
                actual,
            } => {
                assert_eq!(type_name, "Job");
                assert_eq!(expected, 3);
                assert_eq!(actual, 1);
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn task_inputs_deserialize_field_error() {
        let mut inputs = TaskInputsSerializer::new();
        inputs
            .append(TaskInput::ValuePayload(encode(&"name".to_owned())))
            .expect("append failed");
        // 0xC1 is a reserved/invalid msgpack byte.
        inputs
            .append(TaskInput::ValuePayload(vec![0xc1u8]))
            .expect("append failed");
        inputs
            .append(TaskInput::ValuePayload(encode(&vec![0u8])))
            .expect("append failed");
        let wire = inputs.release();

        let err = TaskInputsSerializer::deserialize::<Job>(&wire)
            .expect_err("expected field deserialization error");
        match err {
            WireError::FieldDeserialization {
                type_name,
                field,
                position,
                ..
            } => {
                assert_eq!(type_name, "Job");
                assert_eq!(field, "priority");
                assert_eq!(position, 1);
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn task_inputs_deserialize_truncated_header() {
        let err =
            TaskInputsSerializer::deserialize::<Job>(&[0x01]).expect_err("expected invalid format");
        assert!(matches!(err, WireError::InvalidFormat(_)));
    }

    #[test]
    fn task_inputs_deserialize_truncated_field() {
        // Header declares 1 payload of 100 bytes but supplies only 2 bytes of data.
        let mut wire = Vec::new();
        wire.extend_from_slice(&1u32.to_le_bytes());
        wire.extend_from_slice(&100u32.to_le_bytes());
        wire.extend_from_slice(&[0u8, 1]);

        let err = TaskInputsSerializer::deserialize::<Single>(&wire)
            .expect_err("expected invalid format");
        assert!(matches!(err, WireError::InvalidFormat(_)));
    }

    #[test]
    fn task_outputs_streaming_round_trip() -> anyhow::Result<()> {
        const RESULT: &str = "result";
        const VALUE: i64 = 99;

        let mut outputs = TaskOutputsSerializer::new();
        outputs.append(&RESULT.to_owned())?;
        outputs.append(&VALUE)?;
        let wire = outputs.release();

        let decoded: Vec<TaskOutput> = TaskOutputsSerializer::deserialize(&wire)?;
        assert_eq!(decoded.len(), 2);
        // Each payload is the msgpack encoding of the original value.
        assert_eq!(decoded[0], encode(&RESULT.to_owned()));
        assert_eq!(decoded[1], encode(&VALUE));
        Ok(())
    }

    #[test]
    fn task_outputs_empty() -> anyhow::Result<()> {
        let wire = TaskOutputsSerializer::new().release();
        assert_eq!(wire, 0u32.to_le_bytes());

        let decoded: Vec<TaskOutput> = TaskOutputsSerializer::deserialize(&wire)?;
        assert!(decoded.is_empty());
        Ok(())
    }

    #[test]
    fn task_outputs_deserialize_truncated() {
        let err = TaskOutputsSerializer::deserialize(&[0x01]).expect_err("expected invalid format");
        assert!(matches!(err, WireError::InvalidFormat(_)));
    }

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

    #[test]
    fn compound_type_round_trip() -> anyhow::Result<()> {
        let original = B {
            a: A {
                map: Map::from([(1i8, vec![0xabu8, 0xcd]), (-3i8, vec![])]),
                list: vec![10, 20, -1],
            },
            value: 1234,
            list_map: Map::from([(0i8, vec![1, 2, 3]), (5i8, vec![])]),
        };

        // Serialize each field of B as a separate TaskInput.
        let mut inputs = TaskInputsSerializer::new();
        inputs.append(TaskInput::ValuePayload(encode(&original.a)))?;
        inputs.append(TaskInput::ValuePayload(encode(&original.value)))?;
        inputs.append(TaskInput::ValuePayload(encode(&original.list_map)))?;
        let wire = inputs.release();

        let decoded: B = TaskInputsSerializer::deserialize(&wire)?;
        assert_eq!(decoded, original);
        Ok(())
    }

    #[test]
    fn compound_type_with_empty_inner_round_trip() -> anyhow::Result<()> {
        let original = C {
            a: A {
                map: Map::from([(42i8, vec![0xffu8])]),
                list: vec![-128, 0, 127],
            },
            empty: EmptyInner {},
            b: -1,
        };

        let mut inputs = TaskInputsSerializer::new();
        inputs.append(TaskInput::ValuePayload(encode(&original.a)))?;
        inputs.append(TaskInput::ValuePayload(encode(&original.empty)))?;
        inputs.append(TaskInput::ValuePayload(encode(&original.b)))?;
        let wire = inputs.release();

        let decoded: C = TaskInputsSerializer::deserialize(&wire)?;
        assert_eq!(decoded, original);
        Ok(())
    }

    #[test]
    fn compound_type_output_round_trip() -> anyhow::Result<()> {
        let original = B {
            a: A {
                map: Map::from([(0i8, vec![1u8, 2, 3])]),
                list: vec![],
            },
            value: 0,
            list_map: Map::new(),
        };

        // Simulate proc-macro: serialize each tuple element directly into TaskOutputs.
        let mut outputs = TaskOutputsSerializer::new();
        outputs.append(&original.a)?;
        outputs.append(&original.value)?;
        outputs.append(&original.list_map)?;
        let wire = outputs.release();

        // Storage-layer side: unframe into Vec<TaskOutput>.
        let payloads: Vec<TaskOutput> = TaskOutputsSerializer::deserialize(&wire)?;
        assert_eq!(payloads.len(), 3);

        // Verify each payload decodes to the expected value.
        let decoded_a: A = rmp_serde::from_slice(&payloads[0])?;
        let decoded_value: int16 = rmp_serde::from_slice(&payloads[1])?;
        let decoded_list_map: Map<int8, List<int8>> = rmp_serde::from_slice(&payloads[2])?;
        assert_eq!(decoded_a, original.a);
        assert_eq!(decoded_value, original.value);
        assert_eq!(decoded_list_map, original.list_map);
        Ok(())
    }

    #[test]
    fn empty_params_round_trip() -> anyhow::Result<()> {
        // A task with only TaskContext and no user-supplied inputs produces an empty wire frame.
        let wire = TaskInputsSerializer::new().release();
        let decoded: Empty = TaskInputsSerializer::deserialize(&wire)?;
        assert_eq!(decoded, Empty {});
        Ok(())
    }
}
