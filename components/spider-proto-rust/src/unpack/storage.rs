//! [`RequestUnpack`] implementations for `storage.proto` requests.

use spider_core::types::id::{
    ExecutionManagerId,
    JobId,
    ResourceGroupId,
    SessionId,
    TaskId,
    TaskInstanceId,
};

use crate::{
    storage::{
        JobIdRequest,
        RegisterJobRequest,
        RegisterTaskInstanceRequest,
        ReportTaskFailureRequest,
        ReportTaskSuccessRequest,
    },
    unpack::{RequestUnpack, UnpackError, common::unpack_task_id},
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

/// Unpacks [`RegisterTaskInstanceRequest`] into a tuple containing:
///
/// * The session ID.
/// * The job ID.
/// * The task ID.
/// * The execution manager ID.
impl RequestUnpack for RegisterTaskInstanceRequest {
    type Unpacked = (SessionId, JobId, TaskId, ExecutionManagerId);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        let task_id = unpack_task_id(self.task_id).inspect_err(|error| {
            tracing::error!(
                error = % error.message,
                request = "RegisterTaskInstance",
                em_id = self.execution_manager_id,
                "Failed to unpack request."
            );
        })?;
        Ok((
            self.session_id,
            JobId::from(self.job_id),
            task_id,
            ExecutionManagerId::from(self.execution_manager_id),
        ))
    }
}

/// Unpacks [`ReportTaskSuccessRequest`] into a tuple containing:
///
/// * The session ID.
/// * The job ID.
/// * The task ID.
/// * The task instance ID.
/// * The serialized task outputs.
impl RequestUnpack for ReportTaskSuccessRequest {
    type Unpacked = (SessionId, JobId, TaskId, TaskInstanceId, Vec<u8>);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        let task_id = unpack_task_id(self.task_id).inspect_err(|error| {
            tracing::error!(
                error = % error.message,
                request = "ReportTaskSuccess",
                em_id = self.execution_manager_id,
                task_instance_id = self.task_instance_id,
                "Failed to unpack request."
            );
        })?;

        Ok((
            self.session_id,
            JobId::from(self.job_id),
            task_id,
            self.task_instance_id,
            self.serialized_outputs,
        ))
    }
}

/// Unpacks [`ReportTaskFailureRequest`] into a tuple containing:
///
/// * The session ID.
/// * The job ID.
/// * The task ID.
/// * The task instance ID.
/// * The error message.
impl RequestUnpack for ReportTaskFailureRequest {
    type Unpacked = (SessionId, JobId, TaskId, TaskInstanceId, String);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        let task_id = unpack_task_id(self.task_id).inspect_err(|error| {
            tracing::error!(
                error = % error.message,
                request = "ReportTaskFailure",
                em_id = self.execution_manager_id,
                task_instance_id = self.task_instance_id,
                "Failed to unpack request."
            );
        })?;

        Ok((
            self.session_id,
            JobId::from(self.job_id),
            task_id,
            self.task_instance_id,
            self.error_message,
        ))
    }
}
