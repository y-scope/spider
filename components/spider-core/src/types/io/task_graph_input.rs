//! Task-graph-level inputs, where each input is either a standalone payload or a reference to a
//! payload that is serialized once and shared across inputs.

use serde::Serialize;
use spider_utils::wire::WireError;
use spider_utils::wire::WireFrameBuilder;

use crate::compression::decode_zstd_bytes;
use crate::compression::encode_zstd_bytes;
use crate::compression::{self};

/// Represents a shared input payload in its msgpack-serialized form.
pub type SharedInputPayload = Vec<u8>;

/// Represents the ID of a shared input payload within a [`TaskGraphInput`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SharedInputId(u32);

impl SharedInputId {
    /// # Returns
    ///
    /// The underlying raw ID.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Errors produced while building or (de)serializing a [`TaskGraphInput`].
#[derive(Debug, thiserror::Error)]
pub enum TaskGraphInputError {
    /// A shared input ID does not refer to any shared input payload.
    #[error("shared input ID out of bounds: {0}")]
    SharedInputIdOutOfBounds(u32),

    /// The number of shared input payloads exceeds the range of [`SharedInputId`].
    #[error("shared input ID overflow")]
    SharedInputIdOverflow,

    /// An input failed to serialize into msgpack.
    #[error("msgpack serialization failed: {0}")]
    SerializationFailure(#[from] rmp_serde::encode::Error),

    /// A wire framing or unframing operation failed.
    #[error("wire framing failed: {0}")]
    Wire(#[from] WireError),

    /// A zstd compression or decompression operation failed.
    #[error("compression failed: {0}")]
    Compression(#[from] compression::Error),

    /// A serialized entry was empty and therefore carried no tag byte.
    #[error("task graph input entry is empty")]
    EmptyEntry,

    /// The leading tag byte of a serialized entry did not map to a known entry tag.
    #[error("unknown task graph input entry tag: {0}")]
    UnknownEntryTag(u8),

    /// The body of a serialized shared entry is not a 4-byte shared input ID.
    #[error("invalid shared task graph input entry length: expected 4 bytes, got {0}")]
    InvalidSharedEntryLength(usize),

    /// Unexpected bytes follow the serialized positional inputs.
    #[error("unexpected {0} trailing bytes after task graph positional inputs")]
    TrailingBytes(usize),
}

/// Represents a single input of a task graph.
#[derive(Debug, Clone, PartialEq, Eq, strum::EnumDiscriminants)]
#[strum_discriminants(
    name(EntryTag),
    vis(),
    derive(strum::FromRepr),
    repr(u8),
    doc = "The tag byte that prefixes each serialized [`TaskGraphInputEntry`] to identify its \
           variant."
)]
pub enum TaskGraphInputEntry {
    ValuePayload(Vec<u8>),
    SharedPayload(SharedInputId),
}

impl TaskGraphInputEntry {
    /// Decodes an entry from its serialized wire payload.
    ///
    /// # Returns
    ///
    /// The decoded entry on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TaskGraphInputError::EmptyEntry`] if `payload` is empty.
    /// * [`TaskGraphInputError::UnknownEntryTag`] if the leading tag byte maps to no known tag.
    /// * [`TaskGraphInputError::InvalidSharedEntryLength`] if a shared entry's body is not exactly
    ///   4 bytes.
    /// * [`TaskGraphInputError::SharedInputIdOutOfBounds`] if a shared entry's ID is not less than
    ///   `num_shared_input_payloads`.
    fn from_wire_payload(
        payload: &[u8],
        num_shared_input_payloads: usize,
    ) -> Result<Self, TaskGraphInputError> {
        let (&tag_byte, body) = payload
            .split_first()
            .ok_or(TaskGraphInputError::EmptyEntry)?;
        let tag =
            EntryTag::from_repr(tag_byte).ok_or(TaskGraphInputError::UnknownEntryTag(tag_byte))?;
        match tag {
            EntryTag::ValuePayload => Ok(Self::ValuePayload(body.to_vec())),
            EntryTag::SharedPayload => {
                let raw_id = u32::from_le_bytes(
                    body.try_into()
                        .map_err(|_| TaskGraphInputError::InvalidSharedEntryLength(body.len()))?,
                );
                if raw_id as usize >= num_shared_input_payloads {
                    return Err(TaskGraphInputError::SharedInputIdOutOfBounds(raw_id));
                }
                Ok(Self::SharedPayload(SharedInputId(raw_id)))
            }
        }
    }
}

/// The positional inputs of a task graph, together with the shared input payloads they reference.
///
/// Every [`TaskGraphInputEntry::SharedPayload`] positional input is guaranteed to reference an
/// existing shared input payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskGraphInput {
    shared_input_payloads: Vec<SharedInputPayload>,
    positional_inputs: Vec<TaskGraphInputEntry>,
}

impl TaskGraphInput {
    /// Loads a task graph input from its zstd-compressed serialized form produced by
    /// [`Self::to_zstd_compressed_bytes`].
    ///
    /// # Returns
    ///
    /// The deserialized task graph input on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TaskGraphInputError::TrailingBytes`] if unexpected bytes follow the serialized
    ///   positional inputs.
    /// * Forwards [`decode_zstd_bytes`]'s return values on failure.
    /// * Forwards [`WireFrameBuilder::unframe_payload_slices`]'s return values on failure.
    /// * Forwards [`TaskGraphInputEntry::from_wire_payload`]'s return values on failure.
    pub fn from_zstd_compressed_bytes(bytes: &[u8]) -> Result<Self, TaskGraphInputError> {
        let wire_bytes = decode_zstd_bytes(bytes)?;
        let (shared_input_payload_slices, remaining) =
            WireFrameBuilder::unframe_payload_slices(&wire_bytes)?;
        let (positional_input_slices, remaining) =
            WireFrameBuilder::unframe_payload_slices(remaining)?;
        if !remaining.is_empty() {
            return Err(TaskGraphInputError::TrailingBytes(remaining.len()));
        }

        let positional_inputs = positional_input_slices
            .into_iter()
            .map(|positional_input_slice| {
                TaskGraphInputEntry::from_wire_payload(
                    positional_input_slice,
                    shared_input_payload_slices.len(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            shared_input_payloads: shared_input_payload_slices
                .into_iter()
                .map(<[u8]>::to_vec)
                .collect(),
            positional_inputs,
        })
    }

    /// Serializes the task graph input into zstd-compressed bytes.
    ///
    /// # Returns
    ///
    /// The zstd-compressed serialized task graph input on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`WireFrameBuilder::append_payload`]'s return values on failure.
    /// * Forwards [`WireFrameBuilder::append_payload_parts`]'s return values on failure.
    /// * Forwards [`encode_zstd_bytes`]'s return values on failure.
    pub fn to_zstd_compressed_bytes(&self) -> Result<Vec<u8>, TaskGraphInputError> {
        let mut shared_frame_builder = WireFrameBuilder::new();
        for shared_input_payload in &self.shared_input_payloads {
            shared_frame_builder.append_payload(shared_input_payload)?;
        }

        let mut positional_input_frame_builder =
            WireFrameBuilder::with_prefix(shared_frame_builder.release());
        for positional_input in &self.positional_inputs {
            let tag = [EntryTag::from(positional_input) as u8];
            match positional_input {
                TaskGraphInputEntry::ValuePayload(payload) => {
                    positional_input_frame_builder.append_payload_parts(&[&tag, payload])?;
                }
                TaskGraphInputEntry::SharedPayload(id) => {
                    positional_input_frame_builder
                        .append_payload_parts(&[&tag, &id.get().to_le_bytes()])?;
                }
            }
        }
        Ok(encode_zstd_bytes(
            &positional_input_frame_builder.release(),
        )?)
    }

    /// # Returns
    ///
    /// The positional inputs of the task graph input.
    #[must_use]
    pub fn get_positional_inputs(&self) -> &[TaskGraphInputEntry] {
        &self.positional_inputs
    }

    /// # Returns
    ///
    /// The shared input payload with the given `id`, or `None` if `id` is out of bounds.
    #[must_use]
    pub fn get_shared_input_payload(&self, id: SharedInputId) -> Option<&SharedInputPayload> {
        self.shared_input_payloads.get(id.0 as usize)
    }

    /// # Returns
    ///
    /// The number of shared input payloads.
    #[must_use]
    pub const fn get_num_shared_input_payloads(&self) -> usize {
        self.shared_input_payloads.len()
    }

    /// Converts the task graph input into its positional inputs.
    ///
    /// `create_input` takes ownership of each payload and is called exactly once per shared input
    /// payload and once per value payload.
    ///
    /// # Type Parameters
    ///
    /// * `InputType` - The type of a positional input created from a msgpack-serialized payload.
    ///
    /// # Returns
    ///
    /// The created positional inputs, in order. Positional inputs referencing the same shared input
    /// payload yield clones of the input created for that shared input payload.
    ///
    /// # Panics
    ///
    /// Panics if a shared input ID is out of bounds, which cannot happen for a validly constructed
    /// task graph input.
    #[must_use]
    pub fn into_positional_inputs<InputType: Clone>(
        self,
        mut create_input: impl FnMut(Vec<u8>) -> InputType,
    ) -> Vec<InputType> {
        let shared_inputs: Vec<InputType> = self
            .shared_input_payloads
            .into_iter()
            .map(&mut create_input)
            .collect();
        self.positional_inputs
            .into_iter()
            .map(|positional_input| match positional_input {
                TaskGraphInputEntry::ValuePayload(payload) => create_input(payload),
                TaskGraphInputEntry::SharedPayload(id) => shared_inputs
                    .get(id.0 as usize)
                    .expect("shared input ID should be in bounds")
                    .clone(),
            })
            .collect()
    }
}

/// Builder for [`TaskGraphInput`].
pub struct TaskGraphInputBuilder {
    task_graph_input: TaskGraphInput,
}

impl TaskGraphInputBuilder {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created builder with no shared input payloads and no positional inputs.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            task_graph_input: TaskGraphInput {
                shared_input_payloads: Vec::new(),
                positional_inputs: Vec::new(),
            },
        }
    }

    /// Serializes `input` into msgpack as a new shared input payload.
    ///
    /// # Type Parameters
    ///
    /// * `InputType` - The type of the input to serialize.
    ///
    /// # Returns
    ///
    /// The ID of the newly created shared input payload on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TaskGraphInputError::SharedInputIdOverflow`] if the new shared input payload's ID
    ///   exceeds [`u32::MAX`].
    /// * Forwards [`rmp_serde::to_vec`]'s return values on failure.
    pub fn create_shared_input_payload<InputType: Serialize + ?Sized>(
        &mut self,
        input: &InputType,
    ) -> Result<SharedInputId, TaskGraphInputError> {
        let id = u32::try_from(self.task_graph_input.shared_input_payloads.len())
            .map_err(|_| TaskGraphInputError::SharedInputIdOverflow)?;
        self.task_graph_input
            .shared_input_payloads
            .push(rmp_serde::to_vec(input)?);
        Ok(SharedInputId(id))
    }

    /// Appends a positional input referencing the shared input payload with the given `id`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`TaskGraphInputError::SharedInputIdOutOfBounds`] if `id` is out of bounds of the shared
    ///   input payloads created by this builder.
    pub fn append_shared_task_input(
        &mut self,
        id: SharedInputId,
    ) -> Result<(), TaskGraphInputError> {
        if self.task_graph_input.get_shared_input_payload(id).is_none() {
            return Err(TaskGraphInputError::SharedInputIdOutOfBounds(id.get()));
        }
        self.task_graph_input
            .positional_inputs
            .push(TaskGraphInputEntry::SharedPayload(id));
        Ok(())
    }

    /// Serializes `input` into msgpack and appends it as a value payload positional input.
    ///
    /// # Type Parameters
    ///
    /// * `InputType` - The type of the input to serialize.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`rmp_serde::to_vec`]'s return values on failure.
    pub fn append_task_input<InputType: Serialize + ?Sized>(
        &mut self,
        input: &InputType,
    ) -> Result<(), TaskGraphInputError> {
        self.task_graph_input
            .positional_inputs
            .push(TaskGraphInputEntry::ValuePayload(rmp_serde::to_vec(input)?));
        Ok(())
    }

    /// Finalizes the builder.
    ///
    /// # Returns
    ///
    /// The built task graph input.
    #[must_use]
    pub fn build(self) -> TaskGraphInput {
        self.task_graph_input
    }
}

impl Default for TaskGraphInputBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Frames the given shared input payloads and raw positional input payloads into two
    /// consecutive wire frames, independently of [`TaskGraphInput::to_zstd_compressed_bytes`].
    ///
    /// # Returns
    ///
    /// The uncompressed serialized task graph input.
    ///
    /// # Panics
    ///
    /// Panics if any payload fails to be framed.
    fn frame_task_graph_input(
        shared_input_payloads: &[&[u8]],
        positional_inputs: &[&[u8]],
    ) -> Vec<u8> {
        let mut shared_frame_builder = WireFrameBuilder::new();
        for shared_input_payload in shared_input_payloads {
            shared_frame_builder
                .append_payload(shared_input_payload)
                .expect("framing a shared input payload should succeed");
        }
        let mut positional_input_frame_builder = WireFrameBuilder::new();
        for positional_input in positional_inputs {
            positional_input_frame_builder
                .append_payload(positional_input)
                .expect("framing a positional input should succeed");
        }

        let mut wire_bytes = shared_frame_builder.release();
        wire_bytes.extend_from_slice(&positional_input_frame_builder.release());
        wire_bytes
    }

    #[test]
    fn round_trip_with_mixed_positional_inputs() -> anyhow::Result<()> {
        const FIRST_SHARED_INPUT: &str = "first-shared";
        const SECOND_SHARED_INPUT: [i64; 3] = [1, -2, 3];
        const VALUE_INPUT: u32 = 42;
        const VALUE_PAYLOAD_TAG: u8 = 0;
        const SHARED_PAYLOAD_TAG: u8 = 1;

        let mut builder = TaskGraphInputBuilder::new();
        let first_shared_id = builder.create_shared_input_payload(FIRST_SHARED_INPUT)?;
        let second_shared_id = builder.create_shared_input_payload(&SECOND_SHARED_INPUT)?;
        builder.append_shared_task_input(second_shared_id)?;
        builder.append_task_input(&VALUE_INPUT)?;
        builder.append_shared_task_input(first_shared_id)?;
        builder.append_shared_task_input(second_shared_id)?;
        let task_graph_input = builder.build();

        let encoded_first_shared_input = rmp_serde::to_vec(FIRST_SHARED_INPUT)?;
        let encoded_second_shared_input = rmp_serde::to_vec(&SECOND_SHARED_INPUT)?;
        let encoded_value_input = rmp_serde::to_vec(&VALUE_INPUT)?;
        assert_eq!(
            task_graph_input.get_positional_inputs(),
            &[
                TaskGraphInputEntry::SharedPayload(second_shared_id),
                TaskGraphInputEntry::ValuePayload(encoded_value_input.clone()),
                TaskGraphInputEntry::SharedPayload(first_shared_id),
                TaskGraphInputEntry::SharedPayload(second_shared_id),
            ]
        );

        let expected_value_entry = [&[VALUE_PAYLOAD_TAG], encoded_value_input.as_slice()].concat();
        let expected_wire_bytes = frame_task_graph_input(
            &[&encoded_first_shared_input, &encoded_second_shared_input],
            &[
                &[SHARED_PAYLOAD_TAG, 1, 0, 0, 0],
                &expected_value_entry,
                &[SHARED_PAYLOAD_TAG, 0, 0, 0, 0],
                &[SHARED_PAYLOAD_TAG, 1, 0, 0, 0],
            ],
        );
        let compressed_bytes = task_graph_input.to_zstd_compressed_bytes()?;
        assert_eq!(decode_zstd_bytes(&compressed_bytes)?, expected_wire_bytes);

        let decoded = TaskGraphInput::from_zstd_compressed_bytes(&compressed_bytes)?;
        assert_eq!(decoded, task_graph_input);
        assert_eq!(
            decoded.get_shared_input_payload(first_shared_id),
            Some(&encoded_first_shared_input)
        );
        assert_eq!(
            decoded.get_shared_input_payload(second_shared_id),
            Some(&encoded_second_shared_input)
        );
        Ok(())
    }

    #[test]
    fn round_trip_without_shared_input_payloads() -> anyhow::Result<()> {
        const FIRST_INPUT: &str = "first";
        const SECOND_INPUT: u64 = 7;

        let mut builder = TaskGraphInputBuilder::new();
        builder.append_task_input(FIRST_INPUT)?;
        builder.append_task_input(&SECOND_INPUT)?;
        let task_graph_input = builder.build();

        let decoded = TaskGraphInput::from_zstd_compressed_bytes(
            &task_graph_input.to_zstd_compressed_bytes()?,
        )?;
        assert_eq!(decoded, task_graph_input);
        assert_eq!(decoded.get_num_shared_input_payloads(), 0);
        assert_eq!(
            decoded.get_positional_inputs(),
            &[
                TaskGraphInputEntry::ValuePayload(rmp_serde::to_vec(FIRST_INPUT)?),
                TaskGraphInputEntry::ValuePayload(rmp_serde::to_vec(&SECOND_INPUT)?),
            ]
        );
        Ok(())
    }

    #[test]
    fn round_trip_empty() -> anyhow::Result<()> {
        let task_graph_input = TaskGraphInputBuilder::default().build();

        let decoded = TaskGraphInput::from_zstd_compressed_bytes(
            &task_graph_input.to_zstd_compressed_bytes()?,
        )?;
        assert_eq!(decoded, task_graph_input);
        assert_eq!(decoded.get_num_shared_input_payloads(), 0);
        assert_eq!(decoded.get_positional_inputs(), &[]);
        Ok(())
    }

    #[test]
    fn round_trip_with_unused_shared_input_payload() -> anyhow::Result<()> {
        const SHARED_INPUT: &str = "unused";

        let mut builder = TaskGraphInputBuilder::new();
        let shared_id = builder.create_shared_input_payload(SHARED_INPUT)?;
        let task_graph_input = builder.build();

        let decoded = TaskGraphInput::from_zstd_compressed_bytes(
            &task_graph_input.to_zstd_compressed_bytes()?,
        )?;
        assert_eq!(decoded, task_graph_input);
        assert_eq!(decoded.get_positional_inputs(), &[]);
        assert_eq!(
            decoded.get_shared_input_payload(shared_id),
            Some(&rmp_serde::to_vec(SHARED_INPUT)?)
        );
        Ok(())
    }

    #[test]
    fn builder_rejects_shared_input_id_from_another_builder() -> anyhow::Result<()> {
        let mut other_builder = TaskGraphInputBuilder::new();
        other_builder.create_shared_input_payload("other-first")?;
        let foreign_id = other_builder.create_shared_input_payload("other-second")?;

        let mut builder = TaskGraphInputBuilder::new();
        builder.create_shared_input_payload("first")?;
        let err = builder
            .append_shared_task_input(foreign_id)
            .expect_err("foreign shared input ID should be rejected");
        assert!(matches!(
            err,
            TaskGraphInputError::SharedInputIdOutOfBounds(id) if id == foreign_id.get()
        ));
        assert_eq!(builder.build().get_positional_inputs(), &[]);
        Ok(())
    }

    #[test]
    fn getters() -> anyhow::Result<()> {
        const SHARED_INPUT: &str = "shared";
        const VALUE_INPUT: bool = true;

        let mut other_builder = TaskGraphInputBuilder::new();
        other_builder.create_shared_input_payload("other-first")?;
        let foreign_id = other_builder.create_shared_input_payload("other-second")?;

        let mut builder = TaskGraphInputBuilder::new();
        let shared_id = builder.create_shared_input_payload(SHARED_INPUT)?;
        builder.append_shared_task_input(shared_id)?;
        builder.append_task_input(&VALUE_INPUT)?;
        let task_graph_input = builder.build();

        let expected_positional_inputs = [
            TaskGraphInputEntry::SharedPayload(shared_id),
            TaskGraphInputEntry::ValuePayload(rmp_serde::to_vec(&VALUE_INPUT)?),
        ];
        assert_eq!(shared_id.get(), 0);
        assert_eq!(task_graph_input.get_num_shared_input_payloads(), 1);
        assert_eq!(
            task_graph_input.get_shared_input_payload(shared_id),
            Some(&rmp_serde::to_vec(SHARED_INPUT)?)
        );
        assert_eq!(task_graph_input.get_shared_input_payload(foreign_id), None);
        assert_eq!(
            task_graph_input.get_positional_inputs(),
            &expected_positional_inputs
        );
        Ok(())
    }

    #[test]
    fn into_positional_inputs_resolves_shared_input_payloads_in_order() -> anyhow::Result<()> {
        const FIRST_SHARED_INPUT: &str = "first-shared";
        const SECOND_SHARED_INPUT: [i64; 3] = [1, -2, 3];
        const FIRST_VALUE_INPUT: u32 = 42;
        const SECOND_VALUE_INPUT: &str = "second-value";

        let mut builder = TaskGraphInputBuilder::new();
        let first_shared_id = builder.create_shared_input_payload(FIRST_SHARED_INPUT)?;
        let second_shared_id = builder.create_shared_input_payload(&SECOND_SHARED_INPUT)?;
        builder.append_shared_task_input(second_shared_id)?;
        builder.append_task_input(&FIRST_VALUE_INPUT)?;
        builder.append_shared_task_input(first_shared_id)?;
        builder.append_shared_task_input(second_shared_id)?;
        builder.append_task_input(SECOND_VALUE_INPUT)?;
        let task_graph_input = builder.build();

        let encoded_first_shared_input = rmp_serde::to_vec(FIRST_SHARED_INPUT)?;
        let encoded_second_shared_input = rmp_serde::to_vec(&SECOND_SHARED_INPUT)?;
        assert_eq!(
            task_graph_input.into_positional_inputs(|bytes| bytes),
            vec![
                encoded_second_shared_input.clone(),
                rmp_serde::to_vec(&FIRST_VALUE_INPUT)?,
                encoded_first_shared_input,
                encoded_second_shared_input,
                rmp_serde::to_vec(SECOND_VALUE_INPUT)?,
            ]
        );
        Ok(())
    }

    #[test]
    fn into_positional_inputs_creates_each_shared_input_once() -> anyhow::Result<()> {
        use std::sync::Arc;

        const FIRST_SHARED_INPUT: &str = "first-shared";
        const SECOND_SHARED_INPUT: &str = "second-shared";
        const VALUE_INPUT: u32 = 42;

        let mut builder = TaskGraphInputBuilder::new();
        let first_shared_id = builder.create_shared_input_payload(FIRST_SHARED_INPUT)?;
        let second_shared_id = builder.create_shared_input_payload(SECOND_SHARED_INPUT)?;
        builder.append_shared_task_input(first_shared_id)?;
        builder.append_shared_task_input(second_shared_id)?;
        builder.append_task_input(&VALUE_INPUT)?;
        builder.append_shared_task_input(first_shared_id)?;
        builder.append_shared_task_input(second_shared_id)?;
        let task_graph_input = builder.build();

        let num_shared_input_payloads = task_graph_input.get_num_shared_input_payloads();
        let num_value_payloads = task_graph_input
            .get_positional_inputs()
            .iter()
            .filter(|positional_input| {
                matches!(positional_input, TaskGraphInputEntry::ValuePayload(_))
            })
            .count();
        let mut num_created_inputs = 0;
        let positional_inputs = task_graph_input.into_positional_inputs(|bytes| {
            num_created_inputs += 1;
            Arc::new(bytes)
        });
        assert_eq!(
            num_created_inputs,
            num_shared_input_payloads + num_value_payloads
        );

        let [
            first_shared,
            second_shared,
            value,
            first_shared_again,
            second_shared_again,
        ] = positional_inputs.as_slice()
        else {
            anyhow::bail!(
                "unexpected number of positional inputs: {}",
                positional_inputs.len()
            );
        };
        assert_eq!(**first_shared, rmp_serde::to_vec(FIRST_SHARED_INPUT)?);
        assert_eq!(**second_shared, rmp_serde::to_vec(SECOND_SHARED_INPUT)?);
        assert_eq!(**value, rmp_serde::to_vec(&VALUE_INPUT)?);
        assert!(Arc::ptr_eq(first_shared, first_shared_again));
        assert!(Arc::ptr_eq(second_shared, second_shared_again));
        assert!(!Arc::ptr_eq(first_shared, second_shared));
        Ok(())
    }

    #[test]
    fn into_positional_inputs_empty() {
        let task_graph_input = TaskGraphInputBuilder::new().build();
        assert_eq!(
            task_graph_input.into_positional_inputs(|bytes| bytes),
            Vec::<Vec<u8>>::new()
        );
    }

    #[test]
    fn into_positional_inputs_after_round_trip() -> anyhow::Result<()> {
        const UNUSED_SHARED_INPUT: &str = "unused-shared";
        const SHARED_INPUT: &str = "shared";
        const VALUE_INPUT: u64 = 7;

        let mut builder = TaskGraphInputBuilder::new();
        builder.create_shared_input_payload(UNUSED_SHARED_INPUT)?;
        let shared_id = builder.create_shared_input_payload(SHARED_INPUT)?;
        builder.append_task_input(&VALUE_INPUT)?;
        builder.append_shared_task_input(shared_id)?;
        builder.append_shared_task_input(shared_id)?;
        let task_graph_input = builder.build();
        let decoded = TaskGraphInput::from_zstd_compressed_bytes(
            &task_graph_input.to_zstd_compressed_bytes()?,
        )?;

        let encoded_shared_input = rmp_serde::to_vec(SHARED_INPUT)?;
        let expected_positional_inputs = vec![
            rmp_serde::to_vec(&VALUE_INPUT)?,
            encoded_shared_input.clone(),
            encoded_shared_input,
        ];
        assert_eq!(
            task_graph_input.into_positional_inputs(|bytes| bytes),
            expected_positional_inputs
        );
        assert_eq!(
            decoded.into_positional_inputs(|bytes| bytes),
            expected_positional_inputs
        );
        Ok(())
    }

    #[test]
    fn decode_rejects_unknown_entry_tag() -> anyhow::Result<()> {
        const UNKNOWN_TAG: u8 = u8::MAX;

        let wire_bytes = frame_task_graph_input(&[], &[&[UNKNOWN_TAG]]);
        let err = TaskGraphInput::from_zstd_compressed_bytes(&encode_zstd_bytes(&wire_bytes)?)
            .expect_err("unknown entry tag should be rejected");
        assert!(matches!(
            err,
            TaskGraphInputError::UnknownEntryTag(UNKNOWN_TAG)
        ));
        Ok(())
    }

    #[test]
    fn decode_rejects_empty_entry() -> anyhow::Result<()> {
        let wire_bytes = frame_task_graph_input(&[], &[&[]]);
        let err = TaskGraphInput::from_zstd_compressed_bytes(&encode_zstd_bytes(&wire_bytes)?)
            .expect_err("empty entry should be rejected");
        assert!(matches!(err, TaskGraphInputError::EmptyEntry));
        Ok(())
    }

    #[test]
    fn decode_rejects_invalid_shared_entry_length() -> anyhow::Result<()> {
        for body in [
            [0u8; 0].as_slice(),
            [0u8; 3].as_slice(),
            [0u8; 5].as_slice(),
        ] {
            let entry = [&[EntryTag::SharedPayload as u8], body].concat();
            let wire_bytes = frame_task_graph_input(&[b"shared"], &[&entry]);
            let err = TaskGraphInput::from_zstd_compressed_bytes(&encode_zstd_bytes(&wire_bytes)?)
                .expect_err("shared entry with an invalid length should be rejected");
            assert!(matches!(
                err,
                TaskGraphInputError::InvalidSharedEntryLength(len) if len == body.len()
            ));
        }
        Ok(())
    }

    #[test]
    fn decode_rejects_out_of_bounds_shared_input_id() -> anyhow::Result<()> {
        const OUT_OF_BOUNDS_ID: u32 = 1;

        let shared_entry = [
            &[EntryTag::SharedPayload as u8],
            OUT_OF_BOUNDS_ID.to_le_bytes().as_slice(),
        ]
        .concat();
        let wire_bytes = frame_task_graph_input(
            &[b"shared"],
            &[&shared_entry, &[EntryTag::ValuePayload as u8]],
        );
        let err = TaskGraphInput::from_zstd_compressed_bytes(&encode_zstd_bytes(&wire_bytes)?)
            .expect_err("out-of-bounds shared input ID should be rejected");
        assert!(matches!(
            err,
            TaskGraphInputError::SharedInputIdOutOfBounds(OUT_OF_BOUNDS_ID)
        ));
        Ok(())
    }

    #[test]
    fn decode_rejects_trailing_bytes() -> anyhow::Result<()> {
        const TRAILING_BYTES: &[u8] = &[0, 0];

        let mut wire_bytes = frame_task_graph_input(&[], &[&[EntryTag::ValuePayload as u8]]);
        wire_bytes.extend_from_slice(TRAILING_BYTES);
        let err = TaskGraphInput::from_zstd_compressed_bytes(&encode_zstd_bytes(&wire_bytes)?)
            .expect_err("trailing bytes should be rejected");
        assert!(matches!(
            err,
            TaskGraphInputError::TrailingBytes(len) if len == TRAILING_BYTES.len()
        ));
        Ok(())
    }

    #[test]
    fn decode_rejects_malformed_frames() -> anyhow::Result<()> {
        let empty_frame = WireFrameBuilder::new().release();
        let mut truncated_wire_bytes =
            frame_task_graph_input(&[b"shared"], &[&[EntryTag::ValuePayload as u8]]);
        truncated_wire_bytes.pop();
        let oversized_count_header = u32::MAX.to_le_bytes();
        let oversized_positional_input_frame =
            [empty_frame.as_slice(), &oversized_count_header].concat();

        for wire_bytes in [
            empty_frame.as_slice(),
            truncated_wire_bytes.as_slice(),
            oversized_count_header.as_slice(),
            oversized_positional_input_frame.as_slice(),
        ] {
            let err = TaskGraphInput::from_zstd_compressed_bytes(&encode_zstd_bytes(wire_bytes)?)
                .expect_err("malformed frames should be rejected");
            assert!(matches!(
                err,
                TaskGraphInputError::Wire(WireError::InvalidFormat(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn decode_rejects_corrupted_bytes() {
        let err = TaskGraphInput::from_zstd_compressed_bytes(b"not a zstd payload")
            .expect_err("corrupted bytes should be rejected");
        assert!(matches!(err, TaskGraphInputError::Compression(_)));
    }
}
