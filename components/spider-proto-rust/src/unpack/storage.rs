//! [`RequestUnpack`] implementations for `storage.proto` requests.

use spider_core::types::id::{JobId, ResourceGroupId};

use crate::{
    storage::{JobIdRequest, RegisterJobRequest},
    unpack::{RequestUnpack, UnpackError},
};

/// Unpacks [`RegisterJobRequest`] into a tuple containing:
///
/// * The resource group ID.
/// * The zstd-compressed serialized task graph.
/// * The zstd-compressed serialized inputs.
impl RequestUnpack for RegisterJobRequest {
    type Unpacked = (ResourceGroupId, Vec<u8>, Vec<u8>);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        Ok((
            ResourceGroupId::from(self.resource_group_id),
            self.compressed_serialized_task_graph,
            self.compressed_serialized_inputs,
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
