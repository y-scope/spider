//! [`RequestUnpack`] implementations for `storage.proto` requests.

use std::net::IpAddr;
use std::time::Duration;

use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use tonic::Code;

use crate::storage::AddResourceGroupRequest;
use crate::storage::ExecutionManagerIdRequest;
use crate::storage::JobIdRequest;
use crate::storage::PollReadyTasksRequest;
use crate::storage::RegisterExecutionManagerRequest;
use crate::storage::RegisterJobRequest;
use crate::storage::RegisterSchedulerRequest;
use crate::storage::RegisterTaskInstanceRequest;
use crate::storage::ReportTaskFailureRequest;
use crate::storage::ReportTaskSuccessRequest;
use crate::storage::VerifyResourceGroupRequest;
use crate::unpack::RequestUnpack;
use crate::unpack::UnpackError;
use crate::unpack::common::unpack_task_id;

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

/// Unpacks [`AddResourceGroupRequest`] into a tuple containing:
///
/// * The external resource group ID.
/// * The password.
impl RequestUnpack for AddResourceGroupRequest {
    type Unpacked = (String, Vec<u8>);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        Ok((self.external_resource_group_id, self.password))
    }
}

/// Unpacks [`VerifyResourceGroupRequest`] into a tuple containing:
///
/// * The resource group ID.
/// * The password.
impl RequestUnpack for VerifyResourceGroupRequest {
    type Unpacked = (ResourceGroupId, Vec<u8>);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        Ok((ResourceGroupId::from(self.resource_group_id), self.password))
    }
}

/// Unpacks [`RegisterExecutionManagerRequest`] into the execution manager's IP address.
impl RequestUnpack for RegisterExecutionManagerRequest {
    type Unpacked = IpAddr;

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        self.ip_address
            .parse::<IpAddr>()
            .map_err(|error| invalid_argument(format!("invalid IP address: {error}")))
    }
}

/// Unpacks [`ExecutionManagerIdRequest`] into an [`ExecutionManagerId`].
impl RequestUnpack for ExecutionManagerIdRequest {
    type Unpacked = ExecutionManagerId;

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        Ok(ExecutionManagerId::from(self.execution_manager_id))
    }
}

/// Unpacks [`RegisterSchedulerRequest`] into a tuple containing:
///
/// * The scheduler IP address.
/// * The scheduler port.
impl RequestUnpack for RegisterSchedulerRequest {
    type Unpacked = (IpAddr, u16);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        let ip_address = self
            .ip_address
            .parse::<IpAddr>()
            .map_err(|error| invalid_argument(format!("invalid IP address: {error}")))?;
        let port = u16::try_from(self.port)
            .map_err(|_| invalid_argument(format!("port does not fit in `u16`: {}", self.port)))?;
        Ok((ip_address, port))
    }
}

/// Unpacks [`PollReadyTasksRequest`] into a tuple containing:
///
/// * The maximum number of entries to return.
/// * The maximum duration to block waiting for entries.
impl RequestUnpack for PollReadyTasksRequest {
    type Unpacked = (usize, Duration);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        let max_items = usize::try_from(self.max_items).map_err(|_| {
            invalid_argument(format!(
                "max_items does not fit in `usize`: {}",
                self.max_items
            ))
        })?;
        Ok((max_items, Duration::from_millis(self.wait_ms)))
    }
}

/// Builds an [`UnpackError`] carrying [`Code::InvalidArgument`] and the given message.
///
/// # Returns
///
/// An [`UnpackError`] whose [`Code`] is [`Code::InvalidArgument`] and whose message is `message`.
const fn invalid_argument(message: String) -> UnpackError {
    UnpackError {
        code: Code::InvalidArgument,
        message,
    }
}
