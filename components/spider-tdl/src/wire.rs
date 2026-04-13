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

/// Length of the wire header recording the payload count, in bytes.
const COUNT_HEADER_LEN: usize = 4;

/// Length of the per-payload length prefix, in bytes.
const FIELD_LEN_PREFIX_LEN: usize = 4;

/// Errors produced while framing or unframing a TDL wire buffer.
///
/// [`WireError`] is module-local: it describes failures of the wire/payload layer specifically.
/// Higher-level call sites (for example, `TaskHandlerImpl` once it is implemented) translate it
/// into a [`crate::TdlError`] before the error crosses the C-FFI edge.
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

    /// Catch-all bucket required by [`serde::de::Error`] and [`serde::ser::Error`] for errors
    /// that do not fit any specific variant.
    #[error("{0}")]
    Custom(String),
}

impl de::Error for WireError {
    fn custom<MessageType: fmt::Display>(msg: MessageType) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl serde::ser::Error for WireError {
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
/// let mut inputs = TaskInputs::new();
/// inputs.append(TaskInput::ValuePayload(msgpack_bytes_0))?;
/// inputs.append(TaskInput::ValuePayload(msgpack_bytes_1))?;
/// let wire: Vec<u8> = inputs.release();
/// ```
pub struct TaskInputs {
    builder: WireFrameBuilder,
}

impl Default for TaskInputs {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskInputs {
    /// Creates a new streaming serializer with an empty buffer.
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
    /// * [`WireError::Overflow`] if the payload count would exceed [`u32::MAX`].
    /// * [`WireError::Overflow`] if the payload is longer than [`u32::MAX`] bytes.
    pub fn append(&mut self, input: TaskInput) -> Result<(), WireError> {
        let TaskInput::ValuePayload(bytes) = input;
        self.builder.append_payload(&bytes)
    }

    /// Finalizes the count header and returns the completed wire-format buffer.
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
    /// * `'de` - The lifetime of the wire buffer `data`.
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
    /// * [`WireError::InvalidFormat`] if the buffer is truncated or malformed.
    /// * [`WireError::LengthMismatch`] if the payload count does not match `TargetType`'s field
    ///   count.
    /// * [`WireError::FieldDeserialization`] if any payload fails to decode.
    /// * [`WireError::Custom`] if `TargetType` is not a struct.
    pub fn deserialize<'de, TargetType>(data: &'de [u8]) -> Result<TargetType, WireError>
    where
        TargetType: serde::Deserialize<'de>, {
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
pub struct TaskOutputs {
    builder: WireFrameBuilder,
}

impl Default for TaskOutputs {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskOutputs {
    /// Creates a new streaming serializer with an empty buffer.
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
    /// * [`WireError::Overflow`] if the payload count would exceed [`u32::MAX`].
    /// * [`WireError::Overflow`] if the serialized payload is longer than [`u32::MAX`] bytes.
    /// * [`WireError::Custom`] if msgpack serialization of `value` fails.
    pub fn append<ValueType: serde::Serialize + ?Sized>(
        &mut self,
        value: &ValueType,
    ) -> Result<(), WireError> {
        self.builder.append_serialize(value)
    }

    /// Finalizes the count header and returns the completed wire-format buffer.
    #[must_use]
    pub fn release(self) -> Vec<u8> {
        self.builder.release()
    }

    /// Deserializes a wire-format byte stream into a vector of [`TaskOutput`] values.
    ///
    /// Each payload is extracted as an opaque `Vec<u8>`. The msgpack contents are **not**
    /// decoded here -- the storage layer is responsible for interpreting each output downstream.
    ///
    /// # Returns
    ///
    /// A vector of output payloads on success, one per wire-format element.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`WireError::InvalidFormat`] if the buffer is truncated or malformed.
    pub fn deserialize(data: &[u8]) -> Result<Vec<TaskOutput>, WireError> {
        unframe_payloads(data)
    }

    /// Serializes a tuple by decomposing it into individual elements, encoding each element as
    /// msgpack, and framing them in the wire format.
    ///
    /// This drives serde's `Serialize` impl for the tuple: serde calls `serialize_tuple(len)`
    /// followed by `serialize_element` for each element. A custom [`TupleOutputSerializer`]
    /// intercepts these calls and routes each element through [`TaskOutputs::append`].
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
    /// * [`WireError::Overflow`] if any payload exceeds [`u32::MAX`] bytes.
    /// * [`WireError::Custom`] if msgpack serialization of any element fails.
    /// * [`WireError::Custom`] if the value is not a tuple.
    pub fn serialize_from<TupleType: serde::Serialize>(
        value: &TupleType,
    ) -> Result<Vec<u8>, WireError> {
        value.serialize(TupleOutputSerializer {
            outputs: Self::new(),
        })
    }
}

/// Custom serde [`serde::Serializer`] that decomposes a tuple into individually-encoded wire
/// payloads via [`TaskOutputs`].
///
/// Only `serialize_tuple` (and `serialize_unit` for the empty-tuple case) are supported. All
/// other serialization methods return an error.
struct TupleOutputSerializer {
    outputs: TaskOutputs,
}

/// Generates `serialize_*` methods on a `serde::Serializer` impl that all return the same
/// error. Covers the three method shapes in the trait:
///
/// - `primitive`: `fn method(self, _: Type) -> Result<Self::Ok, Self::Error>`
/// - `compound`: `fn method(self, ...) -> Result<Self::AssocType, Self::Error>`
/// - `generic`: `fn method<T: Serialize + ?Sized>(self, ...) -> Result<Self::Ok, Self::Error>`
macro_rules! reject_non_tuple {
    // fn method(self, _: PrimType)
    (primitive: $($method:ident($prim:ty)),* $(,)?) => {
        $(
            fn $method(self, _: $prim) -> Result<Self::Ok, Self::Error> {
                Err(unsupported_type_error())
            }
        )*
    };
    // fn method(self, ...) -> Result<Self::AssocType, ...>  (compound starters)
    (compound: $($method:ident($($arg:ident: $ty:ty),*) -> $assoc:ty),* $(,)?) => {
        $(
            fn $method(self, $($arg: $ty),*) -> Result<$assoc, Self::Error> {
                Err(unsupported_type_error())
            }
        )*
    };
}

impl serde::Serializer for TupleOutputSerializer {
    type Error = WireError;
    type Ok = Vec<u8>;
    type SerializeMap = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeSeq = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Self;
    type SerializeTupleStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = serde::ser::Impossible<Self::Ok, Self::Error>;

    reject_non_tuple! { primitive:
        serialize_bool(bool),
        serialize_i8(i8), serialize_i16(i16), serialize_i32(i32), serialize_i64(i64),
        serialize_u8(u8), serialize_u16(u16), serialize_u32(u32), serialize_u64(u64),
        serialize_f32(f32), serialize_f64(f64),
        serialize_char(char), serialize_str(&str), serialize_bytes(&[u8]),
    }

    reject_non_tuple! { compound:
        serialize_unit_struct(
            _n: &'static str
        ) -> Self::Ok,
        serialize_unit_variant(
            _n: &'static str, _i: u32, _v: &'static str
        ) -> Self::Ok,
        serialize_seq(
            _len: Option<usize>
        ) -> Self::SerializeSeq,
        serialize_tuple_struct(
            _n: &'static str, _len: usize
        ) -> Self::SerializeTupleStruct,
        serialize_tuple_variant(
            _n: &'static str, _i: u32, _v: &'static str, _len: usize
        ) -> Self::SerializeTupleVariant,
        serialize_map(
            _len: Option<usize>
        ) -> Self::SerializeMap,
        serialize_struct(
            _n: &'static str, _len: usize
        ) -> Self::SerializeStruct,
        serialize_struct_variant(
            _n: &'static str, _i: u32, _v: &'static str, _len: usize
        ) -> Self::SerializeStructVariant,
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.outputs.release())
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

impl serde::ser::SerializeTuple for TupleOutputSerializer {
    type Error = WireError;
    type Ok = Vec<u8>;

    fn serialize_element<ValueType: serde::Serialize + ?Sized>(
        &mut self,
        value: &ValueType,
    ) -> Result<(), Self::Error> {
        self.outputs.append(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.outputs.release())
    }
}

fn unsupported_type_error() -> WireError {
    WireError::Custom("task output must be a tuple".to_owned())
}

/// Streaming wire-format builder shared by [`TaskInputs`] and [`TaskOutputs`].
///
/// Reserves space for the `u32` count header upfront and patches it in [`Self::release`] once
/// the final count is known. Each [`Self::append_payload`] call writes a length-prefixed payload
/// directly into the buffer.
struct WireFrameBuilder {
    buffer: Vec<u8>,
    count: u32,
}

impl WireFrameBuilder {
    fn new() -> Self {
        let buffer = vec![0u8; COUNT_HEADER_LEN];
        Self { buffer, count: 0 }
    }

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
    fn append_serialize<ValueType: serde::Serialize + ?Sized>(
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

    fn increment_count(&mut self) -> Result<(), WireError> {
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| WireError::Overflow("payload count exceeds u32::MAX".to_owned()))?;
        Ok(())
    }

    fn release(mut self) -> Vec<u8> {
        self.buffer[..COUNT_HEADER_LEN].copy_from_slice(&self.count.to_le_bytes());
        self.buffer
    }
}

/// Parses the wire-format framing and extracts each payload as an owned `Vec<u8>`.
///
/// Shared deserialization core for [`TaskOutputs::deserialize`]. Each payload is copied out of
/// the wire buffer into its own allocation.
fn unframe_payloads(data: &[u8]) -> Result<Vec<Vec<u8>>, WireError> {
    if data.len() < COUNT_HEADER_LEN {
        return Err(WireError::InvalidFormat(
            "buffer too small for the payload count header",
        ));
    }
    let count_bytes: [u8; COUNT_HEADER_LEN] = data[..COUNT_HEADER_LEN]
        .try_into()
        .expect("slice length checked above");
    let count = u32::from_le_bytes(count_bytes) as usize;

    let mut pos = COUNT_HEADER_LEN;
    let mut payloads = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + FIELD_LEN_PREFIX_LEN > data.len() {
            return Err(WireError::InvalidFormat(
                "unexpected end of buffer reading payload length",
            ));
        }
        let len_bytes: [u8; FIELD_LEN_PREFIX_LEN] = data[pos..pos + FIELD_LEN_PREFIX_LEN]
            .try_into()
            .expect("slice length checked above");
        let field_len = u32::from_le_bytes(len_bytes) as usize;
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

/// Single-pass, zero-copy cursor over a wire-format byte stream.
///
/// Holds a borrowed slice of the wire buffer and a position cursor. The cursor advances each
/// time a field is consumed, yielding borrowed slices into the buffer that can be handed to
/// `rmp_serde` for payload deserialization.
struct StreamDeserializer<'de> {
    data: &'de [u8],
    pos: usize,
    count: usize,
    current_field: usize,
    type_name: &'static str,
    field_names: &'static [&'static str],
}

impl<'de> StreamDeserializer<'de> {
    fn new(data: &'de [u8]) -> Result<Self, WireError> {
        if data.len() < COUNT_HEADER_LEN {
            return Err(WireError::InvalidFormat(
                "buffer too small for the payload count header",
            ));
        }
        let count_bytes: [u8; COUNT_HEADER_LEN] = data[..COUNT_HEADER_LEN]
            .try_into()
            .expect("slice length checked above");
        let count = u32::from_le_bytes(count_bytes) as usize;
        Ok(Self {
            data,
            pos: COUNT_HEADER_LEN,
            count,
            current_field: 0,
            type_name: "<unknown>",
            field_names: &[],
        })
    }

    fn next_field_bytes(&mut self) -> Result<&'de [u8], WireError> {
        if self.pos + FIELD_LEN_PREFIX_LEN > self.data.len() {
            return Err(WireError::InvalidFormat(
                "unexpected end of buffer reading payload length",
            ));
        }
        let len_bytes: [u8; FIELD_LEN_PREFIX_LEN] = self.data
            [self.pos..self.pos + FIELD_LEN_PREFIX_LEN]
            .try_into()
            .expect("slice length checked above");
        let field_len = u32::from_le_bytes(len_bytes) as usize;
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

impl<'de> serde::Deserializer<'de> for &mut StreamDeserializer<'de> {
    type Error = WireError;

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map enum identifier ignored_any
    }

    fn deserialize_struct<VisitorType>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: VisitorType,
    ) -> Result<VisitorType::Value, WireError>
    where
        VisitorType: Visitor<'de>, {
        if self.count != fields.len() {
            return Err(WireError::LengthMismatch {
                type_name: name,
                expected: fields.len(),
                actual: self.count,
            });
        }
        self.type_name = name;
        self.field_names = fields;
        visitor.visit_seq(FieldSeqAccess { de: self })
    }

    fn deserialize_any<VisitorType>(self, _: VisitorType) -> Result<VisitorType::Value, WireError>
    where
        VisitorType: Visitor<'de>, {
        Err(WireError::Custom(
            "wire stream can only be deserialized into a struct".to_owned(),
        ))
    }
}

/// Per-field adapter that hands the next payload of a wire frame to a serde visitor.
struct FieldSeqAccess<'borrow_lifetime, 'de> {
    de: &'borrow_lifetime mut StreamDeserializer<'de>,
}

impl<'de> SeqAccess<'de> for FieldSeqAccess<'_, 'de> {
    type Error = WireError;

    fn next_element_seed<SeedType>(
        &mut self,
        seed: SeedType,
    ) -> Result<Option<SeedType::Value>, WireError>
    where
        SeedType: DeserializeSeed<'de>, {
        if self.de.current_field == self.de.count {
            return Ok(None);
        }
        if self.de.current_field > self.de.count {
            return Err(WireError::LengthMismatch {
                type_name: self.de.type_name,
                expected: self.de.count,
                actual: self.de.current_field + 1,
            });
        }

        let idx = self.de.current_field;
        let field_name = self.de.field_names.get(idx).copied().unwrap_or("<unknown>");
        let type_name = self.de.type_name;

        let bytes = self.de.next_field_bytes()?;
        self.de.current_field += 1;

        // `bytes` is a borrowed `&'de [u8]` into the original buffer (zero-copy).
        // `rmp_serde` deserializes directly from it in a single step per field.
        let mut rmp_de = rmp_serde::Deserializer::from_read_ref(bytes);
        seed.deserialize(&mut rmp_de)
            .map(Some)
            .map_err(|source| WireError::FieldDeserialization {
                type_name,
                field: field_name,
                position: idx,
                source,
            })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.de.count - self.de.current_field)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use spider_core::types::io::{TaskInput, TaskOutput};

    use super::{TaskInputs, TaskOutputs, WireError};
    use crate::tdl_types::{Bytes, List, Map, int8, int16};

    /// msgpack-encodes a single value as a payload.
    fn encode<ValueType: serde::Serialize>(value: &ValueType) -> Vec<u8> {
        rmp_serde::to_vec(value).expect("msgpack encoding failed")
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct Job {
        name: String,
        priority: u32,
        payload: Vec<u8>,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct Single {
        value: i64,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct Empty {}

    #[derive(Debug, PartialEq, serde::Serialize, Deserialize)]
    struct Inner {
        x: i64,
        y: i64,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct Outer {
        label: String,
        point: Inner,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct Params {
        greeting: String,
        count: u32,
    }

    #[test]
    fn wire_frame_byte_layout() -> anyhow::Result<()> {
        let mut outputs = TaskOutputs::new();
        outputs.append(&42u8)?;
        outputs.append(&"hi")?;
        let wire = outputs.release();

        let encoded_42 = encode(&42u8);
        let encoded_hi = encode(&"hi");

        let mut expected = Vec::new();
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&u32::try_from(encoded_42.len())?.to_le_bytes());
        expected.extend_from_slice(&encoded_42);
        expected.extend_from_slice(&u32::try_from(encoded_hi.len())?.to_le_bytes());
        expected.extend_from_slice(&encoded_hi);
        assert_eq!(wire, expected);
        Ok(())
    }

    #[test]
    fn task_inputs_streaming_round_trip() -> anyhow::Result<()> {
        let mut inputs = TaskInputs::new();
        inputs.append(TaskInput::ValuePayload(encode(&"hello".to_owned())))?;
        inputs.append(TaskInput::ValuePayload(encode(&42u32)))?;
        let wire = inputs.release();

        let params: Params = TaskInputs::deserialize(&wire)?;
        assert_eq!(params.greeting, "hello");
        assert_eq!(params.count, 42);
        Ok(())
    }

    #[test]
    fn task_inputs_empty() -> anyhow::Result<()> {
        let wire = TaskInputs::new().release();
        assert_eq!(wire, 0u32.to_le_bytes());

        let value: Empty = TaskInputs::deserialize(&wire)?;
        assert_eq!(value, Empty {});
        Ok(())
    }

    #[test]
    fn task_inputs_nested_struct() -> anyhow::Result<()> {
        let mut inputs = TaskInputs::new();
        inputs.append(TaskInput::ValuePayload(encode(&"origin".to_owned())))?;
        inputs.append(TaskInput::ValuePayload(encode(&Inner { x: -10, y: 42 })))?;
        let wire = inputs.release();

        let outer: Outer = TaskInputs::deserialize(&wire)?;
        assert_eq!(
            outer,
            Outer {
                label: "origin".to_owned(),
                point: Inner { x: -10, y: 42 },
            }
        );
        Ok(())
    }

    #[test]
    fn task_inputs_deserialize_length_mismatch() {
        let mut inputs = TaskInputs::new();
        inputs
            .append(TaskInput::ValuePayload(encode(&"only-one".to_owned())))
            .expect("append failed");
        let wire = inputs.release();

        let err = TaskInputs::deserialize::<Job>(&wire).expect_err("expected length mismatch");
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
        let mut inputs = TaskInputs::new();
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

        let err = TaskInputs::deserialize::<Job>(&wire)
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
        let err = TaskInputs::deserialize::<Job>(&[0x01]).expect_err("expected invalid format");
        assert!(matches!(err, WireError::InvalidFormat(_)));
    }

    #[test]
    fn task_inputs_deserialize_truncated_field() {
        // Header declares 1 payload of 100 bytes but supplies only 2 bytes of data.
        let mut wire = Vec::new();
        wire.extend_from_slice(&1u32.to_le_bytes());
        wire.extend_from_slice(&100u32.to_le_bytes());
        wire.extend_from_slice(&[0u8, 1]);

        let err = TaskInputs::deserialize::<Single>(&wire).expect_err("expected invalid format");
        assert!(matches!(err, WireError::InvalidFormat(_)));
    }

    #[test]
    fn task_outputs_streaming_round_trip() -> anyhow::Result<()> {
        let mut outputs = TaskOutputs::new();
        outputs.append(&"result".to_owned())?;
        outputs.append(&99i64)?;
        let wire = outputs.release();

        let decoded: Vec<TaskOutput> = TaskOutputs::deserialize(&wire)?;
        assert_eq!(decoded.len(), 2);
        // Each payload is the msgpack encoding of the original value.
        assert_eq!(decoded[0], encode(&"result".to_owned()));
        assert_eq!(decoded[1], encode(&99i64));
        Ok(())
    }

    #[test]
    fn task_outputs_empty() -> anyhow::Result<()> {
        let wire = TaskOutputs::new().release();
        assert_eq!(wire, 0u32.to_le_bytes());

        let decoded: Vec<TaskOutput> = TaskOutputs::deserialize(&wire)?;
        assert!(decoded.is_empty());
        Ok(())
    }

    #[test]
    fn task_outputs_deserialize_truncated() {
        let err = TaskOutputs::deserialize(&[0x01]).expect_err("expected invalid format");
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
        let mut inputs = TaskInputs::new();
        inputs.append(TaskInput::ValuePayload(encode(&original.a)))?;
        inputs.append(TaskInput::ValuePayload(encode(&original.value)))?;
        inputs.append(TaskInput::ValuePayload(encode(&original.list_map)))?;
        let wire = inputs.release();

        let decoded: B = TaskInputs::deserialize(&wire)?;
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

        let mut inputs = TaskInputs::new();
        inputs.append(TaskInput::ValuePayload(encode(&original.a)))?;
        inputs.append(TaskInput::ValuePayload(encode(&original.empty)))?;
        inputs.append(TaskInput::ValuePayload(encode(&original.b)))?;
        let wire = inputs.release();

        let decoded: C = TaskInputs::deserialize(&wire)?;
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
        let mut outputs = TaskOutputs::new();
        outputs.append(&original.a)?;
        outputs.append(&original.value)?;
        outputs.append(&original.list_map)?;
        let wire = outputs.release();

        // Storage-layer side: unframe into Vec<TaskOutput>.
        let payloads: Vec<TaskOutput> = TaskOutputs::deserialize(&wire)?;
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
        let wire = TaskInputs::new().release();
        let decoded: Empty = TaskInputs::deserialize(&wire)?;
        assert_eq!(decoded, Empty {});
        Ok(())
    }
}
