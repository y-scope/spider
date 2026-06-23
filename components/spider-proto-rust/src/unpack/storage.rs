//! [`RequestUnpack`] implementations for `storage.proto` requests.

use spider_core::types::id::{JobId, ResourceGroupId};
use tonic::Code;

use crate::{
    storage::{JobIdRequest, RegisterJobRequest},
    unpack::{RequestUnpack, UnpackError},
};

/// Unpacks [`RegisterJobRequest`] into a tuple containing:
///
/// * The resource group ID.
/// * The serialized task graph.
/// * The serialized inputs.
impl RequestUnpack for RegisterJobRequest {
    type Unpacked = (ResourceGroupId, String, Vec<u8>);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        let serialized_task_graph =
            String::from_utf8(self.serialized_task_graph).map_err(|error| {
                tracing::error!(error = % error, "Invalid UTF-8 in serialized task graph.");
                UnpackError {
                    code: Code::InvalidArgument,
                    message: format!("invalid UTF-8 in serialized task graph: {error}"),
                }
            })?;
        Ok((
            ResourceGroupId::from(self.resource_group_id),
            serialized_task_graph,
            self.serialized_inputs,
        ))
    }
}

/// Unpacks [`JobIdRequest`] into a [`JobId`].
impl RequestUnpack for JobIdRequest {
    type Unpacked = JobId;

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        Ok(JobId::from(self.job_id))
    }
}
