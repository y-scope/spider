use serde::Deserialize;
use serde::Serialize;
use spider_utils::wire::StreamDeserializer;
use spider_utils::wire::WireError;
use spider_utils::wire::WireFrameBuilder;
use spider_utils::wire::{self};

use crate::compression::decode_zstd_bytes;
use crate::compression::encode_zstd_bytes;
use crate::compression::{self};
use crate::task::TdlContext;
use crate::task::TimeoutPolicy;
use crate::types::id::TaskInstanceId;

/// Represents an input of a task.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TaskInput {
    ValuePayload(Vec<u8>),
}

/// Represents an output of a task.
pub type TaskOutput = Vec<u8>;

/// Errors produced while (de)serializing or (un)packing [`SerializedTaskOutputs`].
#[derive(Debug, thiserror::Error)]
pub enum TaskOutputsError {
    /// A wire framing or unframing operation failed.
    #[error("wire framing failed: {0}")]
    Wire(#[from] WireError),

    /// A zstd compression or decompression operation failed.
    #[error("compression failed: {0}")]
    Compression(#[from] compression::Error),

    /// A raw buffer was empty and therefore carried no serialize-option byte.
    #[error("raw task outputs buffer is empty")]
    Empty,

    /// The leading serialize-option byte of a raw buffer did not map to a known option.
    #[error("unknown task outputs serialize option: {0}")]
    UnknownOption(u8),
}

/// Selects how a [`Vec<TaskOutput>`] is encoded into a [`SerializedTaskOutputs`] payload.
///
/// The option is `u8`-convertible and is written as the first byte of the
/// [`SerializedTaskOutputs::to_raw`] buffer so the encoding can be recovered by
/// [`SerializedTaskOutputs::from_raw`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::FromRepr)]
#[repr(u8)]
pub enum TaskOutputsSerializeOption {
    /// Wire-format framing only.
    Wire = 0,

    /// Wire-format framing followed by whole-buffer zstd compression.
    ZstdWire = 1,
}

/// A serialized bundle of task outputs together with the encoding used to produce it.
///
/// Compression, if required, is performed in a single shot over the whole wire buffer; streaming
/// compression is not yet supported.
#[derive(Debug)]
pub struct SerializedTaskOutputs {
    option: TaskOutputsSerializeOption,
    payload: Vec<u8>,
}

impl SerializedTaskOutputs {
    /// Serializes `task_outputs` into the encoding selected by `option`.
    ///
    /// # Returns
    ///
    /// The serialized task outputs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`WireFrameBuilder::append_payload`]'s return values on failure.
    /// * Forwards [`encode_zstd_bytes`]'s return values on failure.
    pub fn serialize(
        task_outputs: &[TaskOutput],
        option: TaskOutputsSerializeOption,
    ) -> Result<Self, TaskOutputsError> {
        let mut builder = WireFrameBuilder::new();
        for output in task_outputs {
            builder.append_payload(output)?;
        }
        let wire_bytes = builder.release();

        let payload = match option {
            TaskOutputsSerializeOption::Wire => wire_bytes,
            TaskOutputsSerializeOption::ZstdWire => encode_zstd_bytes(&wire_bytes)?,
        };
        Ok(Self { option, payload })
    }

    /// Serializes `task_outputs` with a hardcoded size hint heuristic to determine whether to apply
    /// Zstd compression.
    ///
    /// The size hint is a hard coded value: if the total number of bytes of the given task output
    /// payload exceeds 1KiB, the task outputs will be serialized using
    /// [`TaskOutputsSerializeOption::ZstdWire`], otherwise [`TaskOutputsSerializeOption::Wire`].
    ///
    /// # Returns
    ///
    /// The serialized task outputs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::serialize`]'s return values on failure.
    pub fn serialize_with_size_hint(task_outputs: &[TaskOutput]) -> Result<Self, TaskOutputsError> {
        const COMPRESSION_THRESHOLD: usize = 1_024;
        let total_size = task_outputs.iter().map(std::vec::Vec::len).sum::<usize>();
        let option = if total_size > COMPRESSION_THRESHOLD {
            TaskOutputsSerializeOption::ZstdWire
        } else {
            TaskOutputsSerializeOption::Wire
        };
        Self::serialize(task_outputs, option)
    }

    /// Packs the bundle into a raw byte buffer for storage.
    ///
    /// The first byte is the [`TaskOutputsSerializeOption`] encoded as `u8`; the remaining bytes
    /// are the encoded payload.
    ///
    /// # Returns
    ///
    /// The raw byte buffer.
    #[must_use]
    pub fn to_raw(&self) -> Vec<u8> {
        let mut raw = Vec::with_capacity(1 + self.payload.len());
        raw.push(self.option as u8);
        raw.extend_from_slice(&self.payload);
        raw
    }

    /// Unpacks a raw byte buffer produced by [`Self::to_raw`].
    ///
    /// # Returns
    ///
    /// The reconstructed bundle on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TaskOutputsError::Empty`] if `raw_bytes` is empty.
    /// * [`TaskOutputsError::UnknownOption`] if the leading option byte maps to no known option.
    pub fn from_raw(raw_bytes: &[u8]) -> Result<Self, TaskOutputsError> {
        let (option, payload_bytes) = Self::split_raw(raw_bytes)?;
        Ok(Self {
            option,
            payload: payload_bytes.to_vec(),
        })
    }

    /// Deserializes the bundle back into individual task output payloads.
    ///
    /// # Returns
    ///
    /// A vector of output payloads on success, one per wire-format element.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`decode_zstd_bytes`]'s return values on failure.
    /// * Forwards [`wire::unframe`]'s return values on failure.
    pub fn deserialize(self) -> Result<Vec<TaskOutput>, TaskOutputsError> {
        let wire_bytes = match self.option {
            TaskOutputsSerializeOption::Wire => self.payload,
            TaskOutputsSerializeOption::ZstdWire => decode_zstd_bytes(&self.payload)?,
        };
        Ok(wire::unframe(&wire_bytes)?)
    }

    /// Unpacks a raw byte buffer produced by [`Self::to_raw`] and deserializes it back into
    /// individual task output payloads in one step.
    ///
    /// # Returns
    ///
    /// A vector of output payloads on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`Self::split_raw`]'s return values on failure.
    /// * Forwards [`decode_zstd_bytes`]'s return values on failure.
    /// * Forwards [`wire::unframe`]'s return values on failure.
    pub fn deserialize_from_raw(raw_bytes: &[u8]) -> Result<Vec<TaskOutput>, TaskOutputsError> {
        let (option, payload_bytes) = Self::split_raw(raw_bytes)?;
        let outputs = match option {
            // Unframe directly from the borrowed slice to avoid copying the payload.
            TaskOutputsSerializeOption::Wire => wire::unframe(payload_bytes)?,
            TaskOutputsSerializeOption::ZstdWire => {
                wire::unframe(&decode_zstd_bytes(payload_bytes)?)?
            }
        };
        Ok(outputs)
    }

    /// Splits a raw byte buffer produced by [`Self::to_raw`] into its serialize option and a
    /// borrowed slice of the payload bytes that follow it.
    ///
    /// # Returns
    ///
    /// A tuple on success, containing:
    ///
    /// * The serialize option.
    /// * The slice of the payload bytes borrowed from the given `raw_bytes`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TaskOutputsError::Empty`] if `raw_bytes` is empty.
    /// * [`TaskOutputsError::UnknownOption`] if the leading option byte maps to no known option.
    fn split_raw(
        raw_bytes: &[u8],
    ) -> Result<(TaskOutputsSerializeOption, &[u8]), TaskOutputsError> {
        let (&option_byte, payload_bytes) =
            raw_bytes.split_first().ok_or(TaskOutputsError::Empty)?;
        let option = TaskOutputsSerializeOption::from_repr(option_byte)
            .ok_or(TaskOutputsError::UnknownOption(option_byte))?;
        Ok((option, payload_bytes))
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

    /// Finalizes the count header and returns the completed wire-format buffer.
    ///
    /// # Returns
    ///
    /// Completed wire-format buffer.
    #[must_use]
    pub fn release(self) -> Vec<u8> {
        self.builder.release()
    }

    /// Serializes a tuple by decomposing it into individual elements, encoding each element as
    /// msgpack, and framing them in the wire format.
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
    /// * Forwards [`wire::serialize_tuple`]'s return values on failure.
    pub fn from_tuple<TupleType: serde::Serialize>(
        value: &TupleType,
    ) -> Result<Vec<u8>, WireError> {
        wire::serialize_tuple(value)
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
    /// * Forwards [`wire::unframe`]'s return values on failure.
    pub fn deserialize(data: &[u8]) -> Result<Vec<TaskOutput>, WireError> {
        wire::unframe(data)
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
    #[cfg(test)]
    fn append<ValueType: serde::Serialize + ?Sized>(
        &mut self,
        value: &ValueType,
    ) -> Result<(), WireError> {
        self.builder.append_serialize(value)
    }
}

/// The execution context for a task instance.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecutionContext {
    pub task_instance_id: TaskInstanceId,
    pub tdl_context: TdlContext,
    pub timeout_policy: TimeoutPolicy,
    pub serialized_inputs: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde::Deserialize;

    use super::*;

    #[derive(Debug, PartialEq, Deserialize)]
    struct Job {
        name: String,
        priority: i32,
        payload: Vec<i8>,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct Single {
        value: i64,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct Empty {}

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
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
        count: i64,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct A {
        map: HashMap<i8, Vec<u8>>,
        list: Vec<i8>,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct B {
        a: A,
        value: i16,
        list_map: HashMap<i8, Vec<i8>>,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct EmptyInner {}

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct C {
        a: A,
        empty: EmptyInner,
        b: i16,
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
    fn compound_type_round_trip() -> anyhow::Result<()> {
        let original = B {
            a: A {
                map: HashMap::from([(1i8, vec![0xabu8, 0xcd]), (-3i8, vec![])]),
                list: vec![10, 20, -1],
            },
            value: 1234,
            list_map: HashMap::from([(0i8, vec![1, 2, 3]), (5i8, vec![])]),
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
                map: HashMap::from([(42i8, vec![0xffu8])]),
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
                map: HashMap::from([(0i8, vec![1u8, 2, 3])]),
                list: vec![],
            },
            value: 0,
            list_map: HashMap::new(),
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
        let decoded_value: i16 = rmp_serde::from_slice(&payloads[1])?;
        let decoded_list_map: HashMap<i8, Vec<i8>> = rmp_serde::from_slice(&payloads[2])?;
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

    #[test]
    fn serialize_from_tuple_multi_element() -> anyhow::Result<()> {
        const FIRST: i32 = 7;
        const SECOND: &str = "hello";
        const THIRD: i64 = -42;

        let wire = TaskOutputsSerializer::from_tuple(&(FIRST, SECOND.to_owned(), THIRD))?;
        let payloads: Vec<TaskOutput> = TaskOutputsSerializer::deserialize(&wire)?;
        assert_eq!(payloads.len(), 3);
        let decoded_first: i32 = rmp_serde::from_slice(&payloads[0])?;
        let decoded_second: String = rmp_serde::from_slice(&payloads[1])?;
        let decoded_third: i64 = rmp_serde::from_slice(&payloads[2])?;
        assert_eq!(decoded_first, FIRST);
        assert_eq!(decoded_second, SECOND);
        assert_eq!(decoded_third, THIRD);
        Ok(())
    }

    #[test]
    fn serialize_from_tuple_single_element() -> anyhow::Result<()> {
        const ONLY: i32 = 99;

        let wire = TaskOutputsSerializer::from_tuple(&(ONLY,))?;
        let payloads: Vec<TaskOutput> = TaskOutputsSerializer::deserialize(&wire)?;
        assert_eq!(payloads.len(), 1);
        let decoded: i32 = rmp_serde::from_slice(&payloads[0])?;
        assert_eq!(decoded, ONLY);
        Ok(())
    }

    #[test]
    fn serialize_from_empty_tuple() -> anyhow::Result<()> {
        let wire = TaskOutputsSerializer::from_tuple(&())?;
        let payloads: Vec<TaskOutput> = TaskOutputsSerializer::deserialize(&wire)?;
        assert!(payloads.is_empty());
        Ok(())
    }

    #[test]
    fn serialize_from_rejects_non_tuple() {
        let err = TaskOutputsSerializer::from_tuple(&42i32)
            .expect_err("expected non-tuple to be rejected");
        assert!(matches!(err, WireError::Custom(_)));
    }

    #[test]
    fn serialize_option_u8_round_trip() {
        for option in [
            TaskOutputsSerializeOption::Wire,
            TaskOutputsSerializeOption::ZstdWire,
        ] {
            assert_eq!(
                TaskOutputsSerializeOption::from_repr(option as u8),
                Some(option)
            );
        }
    }

    #[test]
    fn serialize_option_unknown_byte_rejected() {
        let err = SerializedTaskOutputs::from_raw(&[42u8])
            .expect_err("expected unknown option byte to be rejected");
        assert!(matches!(err, TaskOutputsError::UnknownOption(42)));
    }

    #[test]
    fn serialized_task_outputs_round_trip() -> anyhow::Result<()> {
        let datasets: [Vec<TaskOutput>; 3] = [
            Vec::new(),
            vec![vec![7u8; 4096], vec![0u8; 4096]],
            vec![encode(&"result"), encode(&99i64)],
        ];

        for option in [
            TaskOutputsSerializeOption::Wire,
            TaskOutputsSerializeOption::ZstdWire,
        ] {
            for outputs in &datasets {
                let raw = SerializedTaskOutputs::serialize(outputs, option)?.to_raw();
                assert_eq!(raw[0], option as u8);
                assert_eq!(
                    SerializedTaskOutputs::from_raw(&raw)?.deserialize()?,
                    *outputs
                );
                assert_eq!(SerializedTaskOutputs::deserialize_from_raw(&raw)?, *outputs);
            }
        }

        Ok(())
    }

    #[test]
    fn serialized_task_outputs_from_empty_raw_rejected() {
        let err =
            SerializedTaskOutputs::from_raw(&[]).expect_err("expected empty buffer to be rejected");
        assert!(matches!(err, TaskOutputsError::Empty));
    }
}
