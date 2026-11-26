//! Wire encoding for task execution metadata and optional task-graph outputs.

use spider_utils::wire::WireError;
use spider_utils::wire::WireFrameBuilder;

use crate::task::Error;
use crate::task::TaskContext;
use crate::types::id::TaskId;

/// The metadata and optional serialized task-graph outputs for one task execution.
///
/// Metadata is the first payload in the frame. Commit tasks also carry a second payload for their
/// serialized task-graph outputs.
///
/// # Type Parameters
///
/// * `'outputs_lifetime` - The lifetime of the borrowed serialized task-graph outputs.
pub struct TaskContextFrame<'outputs_lifetime> {
    task_context: TaskContext,
    serialized_task_graph_outputs: Option<&'outputs_lifetime [u8]>,
}

impl<'outputs_lifetime> TaskContextFrame<'outputs_lifetime> {
    /// Pairs task metadata with any serialized task-graph outputs.
    ///
    /// # Returns
    ///
    /// The task context frame on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`validate_output_presence`]'s return values on failure.
    pub fn new(
        task_context: TaskContext,
        serialized_task_graph_outputs: Option<&'outputs_lifetime [u8]>,
    ) -> Result<Self, Error> {
        validate_output_presence(
            task_context.task_id(),
            serialized_task_graph_outputs.is_some(),
        )?;
        Ok(Self {
            task_context,
            serialized_task_graph_outputs,
        })
    }

    /// Decodes the metadata and borrows the serialized task-graph outputs from the frame.
    ///
    /// # Returns
    ///
    /// The decoded task context frame on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`WireError::InvalidFormat`] if the frame has the wrong number of payloads or contains
    ///   trailing bytes.
    /// * Forwards [`WireFrameBuilder::unframe_payload_slices`]'s return values on failure.
    /// * Forwards [`rmp_serde::from_read`]'s return values on failure.
    /// * Forwards [`Self::new`]'s return values on failure.
    pub fn from_wire(bytes: &'outputs_lifetime [u8]) -> Result<Self, Error> {
        let (payloads, remaining) = WireFrameBuilder::unframe_payload_slices(bytes)?;
        if !remaining.is_empty() {
            return Err(WireError::InvalidFormat("bytes after task context frame").into());
        }
        let (mut metadata, outputs) = match payloads.as_slice() {
            [metadata] => (*metadata, None),
            [metadata, outputs] => (*metadata, Some(*outputs)),
            _ => {
                return Err(
                    WireError::InvalidFormat("expected one or two task context payloads").into(),
                );
            }
        };
        let task_context: TaskContext = rmp_serde::from_read(&mut metadata)?;
        if !metadata.is_empty() {
            return Err(WireError::InvalidFormat("bytes after task context metadata").into());
        }
        Self::new(task_context, outputs)
    }

    /// Encodes the metadata and serialized task-graph outputs as separate payloads in a wire frame.
    ///
    /// # Returns
    ///
    /// The wire-format frame on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`WireFrameBuilder::append_serialize`]'s return values on failure.
    /// * Forwards [`WireFrameBuilder::append_payload`]'s return values on failure.
    pub fn to_wire(&self) -> Result<Vec<u8>, Error> {
        let mut builder = WireFrameBuilder::new();
        builder.append_serialize(&self.task_context)?;
        if let Some(outputs) = self.serialized_task_graph_outputs {
            builder.append_payload(outputs)?;
        }
        Ok(builder.release())
    }

    /// Consumes the frame and returns its contents.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    ///
    /// * The task execution metadata.
    /// * The serialized task-graph outputs, if present.
    #[must_use]
    pub const fn into_parts(self) -> (TaskContext, Option<&'outputs_lifetime [u8]>) {
        (self.task_context, self.serialized_task_graph_outputs)
    }
}

/// Checks that serialized task-graph outputs are present exactly for commit tasks.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`Error::InvalidTaskContext`] if the presence of outputs does not match the task kind.
fn validate_output_presence(task_id: TaskId, has_outputs: bool) -> Result<(), Error> {
    match (task_id, has_outputs) {
        (TaskId::Commit, false) => Err(Error::InvalidTaskContext(
            "task-graph outputs are required for a commit task but were not provided".to_owned(),
        )),
        (TaskId::Index(_) | TaskId::Cleanup, true) => Err(Error::InvalidTaskContext(format!(
            "task-graph outputs are only valid for a commit task, but the current task is \
             {task_id}"
        ))),
        (TaskId::Commit, true) | (TaskId::Index(_) | TaskId::Cleanup, false) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use spider_utils::wire::WireError;
    use spider_utils::wire::WireFrameBuilder;

    use super::TaskContextFrame;
    use crate::task::Error;
    use crate::task::TaskContext;
    use crate::types::id::JobId;
    use crate::types::id::ResourceGroupId;
    use crate::types::id::TaskId;

    /// # Returns
    ///
    /// Task metadata with fixture IDs and the given task kind.
    fn task_context(task_id: TaskId) -> TaskContext {
        TaskContext::new(JobId::random(), task_id, 1, ResourceGroupId::random())
    }

    #[test]
    fn to_wire_places_outputs_in_a_separate_payload() -> anyhow::Result<()> {
        let metadata = task_context(TaskId::Commit);
        let encoded_metadata = rmp_serde::to_vec(&(
            metadata.job_id(),
            metadata.task_id(),
            metadata.task_instance_id(),
            metadata.resource_group_id(),
        ))?;
        let outputs = [0, 128, 255];
        let wire = TaskContextFrame::new(metadata, Some(&outputs))?.to_wire()?;
        let (payloads, remaining) = WireFrameBuilder::unframe_payload_slices(&wire)?;
        assert_eq!(payloads, [encoded_metadata.as_slice(), &outputs]);
        assert_eq!(remaining, [] as [u8; 0]);
        Ok(())
    }

    #[test]
    fn from_wire_rejects_mismatched_task_and_outputs() -> anyhow::Result<()> {
        for (task_id, has_outputs) in [
            (TaskId::Commit, false),
            (TaskId::Index(0), true),
            (TaskId::Cleanup, true),
        ] {
            let mut builder = WireFrameBuilder::new();
            builder.append_serialize(&task_context(task_id))?;
            if has_outputs {
                builder.append_payload(&[])?;
            }
            assert!(matches!(
                TaskContextFrame::from_wire(&builder.release()),
                Err(Error::InvalidTaskContext(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn from_wire_rejects_wrong_payload_count() -> anyhow::Result<()> {
        let mut builder = WireFrameBuilder::new();
        builder.append_serialize(&task_context(TaskId::Commit))?;
        builder.append_payload(&[])?;
        builder.append_payload(&[])?;
        for wire in [WireFrameBuilder::new().release(), builder.release()] {
            assert!(matches!(
                TaskContextFrame::from_wire(&wire),
                Err(Error::TaskContextWireError(WireError::InvalidFormat(_)))
            ));
        }
        Ok(())
    }

    #[test]
    fn from_wire_rejects_trailing_bytes() -> anyhow::Result<()> {
        let metadata = task_context(TaskId::Commit);
        let mut encoded_metadata = rmp_serde::to_vec(&metadata)?;
        encoded_metadata.push(0);
        let mut builder = WireFrameBuilder::new();
        builder.append_payload(&encoded_metadata)?;
        builder.append_payload(&[1, 2, 3])?;
        let trailing_metadata = builder.release();

        let mut wire = TaskContextFrame::new(metadata, Some(&[1, 2, 3]))?.to_wire()?;
        wire.push(0);
        for invalid in [wire, trailing_metadata] {
            assert!(matches!(
                TaskContextFrame::from_wire(&invalid),
                Err(Error::TaskContextWireError(WireError::InvalidFormat(_)))
            ));
        }
        Ok(())
    }
}
