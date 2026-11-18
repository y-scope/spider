//! [`RequestUnpack`] implementations for `storage.proto` requests.

use std::net::IpAddr;
use std::time::Duration;

use spider_core::job::submission::TaskSuccessOutcome;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SessionId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_core::types::resource_group::ExternalResourceGroupCredentials;
use spider_utils::config::Host;
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
/// * The zstd-compressed serialized task graph input.
impl RequestUnpack for RegisterJobRequest {
    type Unpacked = (ResourceGroupId, Vec<u8>, Vec<u8>);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        Ok((
            ResourceGroupId::from(self.resource_group_id),
            self.compressed_serialized_task_graph,
            self.compressed_serialized_task_graph_input,
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
/// * The successful task outcome. Regular tasks preserve outputs and child submission order; commit
///   and cleanup tasks must carry neither outputs nor children.
impl RequestUnpack for ReportTaskSuccessRequest {
    type Unpacked = (SessionId, JobId, TaskId, TaskInstanceId, TaskSuccessOutcome);

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

        let outcome = match task_id {
            TaskId::Index(_) => TaskSuccessOutcome::Regular {
                serialized_outputs: self.serialized_outputs,
                children: self.children.into_iter().map(Into::into).collect(),
            },
            TaskId::Commit | TaskId::Cleanup
                if self.serialized_outputs.is_empty() && self.children.is_empty() =>
            {
                TaskSuccessOutcome::Finalizing
            }
            TaskId::Commit | TaskId::Cleanup => {
                return Err(invalid_argument(
                    "termination tasks cannot report outputs or child jobs".to_owned(),
                ));
            }
        };

        Ok((
            self.session_id,
            JobId::from(self.job_id),
            task_id,
            self.task_instance_id,
            outcome,
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

/// Unpacks [`AddResourceGroupRequest`] into external resource group credentials.
impl RequestUnpack for AddResourceGroupRequest {
    type Unpacked = ExternalResourceGroupCredentials;

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        self.credentials
            .map(Into::into)
            .ok_or_else(|| invalid_argument("resource group credentials are missing".to_owned()))
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

/// Unpacks [`RegisterExecutionManagerRequest`] into a tuple containing:
///
/// * The execution manager's IP address.
/// * The external resource group credentials, if present.
impl RequestUnpack for RegisterExecutionManagerRequest {
    type Unpacked = (IpAddr, Option<ExternalResourceGroupCredentials>);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        let ip_address = self
            .ip_address
            .parse::<IpAddr>()
            .map_err(|error| invalid_argument(format!("invalid IP address: {error}")))?;
        let resource_group_credentials = self.resource_group_credentials.map(Into::into);
        Ok((ip_address, resource_group_credentials))
    }
}

/// Unpacks [`ExecutionManagerIdRequest`] into an [`ExecutionManagerId`].
impl RequestUnpack for ExecutionManagerIdRequest {
    type Unpacked = ExecutionManagerId;

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        Ok(ExecutionManagerId::from(self.execution_manager_id))
    }
}

/// Unpacks [`RegisterSchedulerRequest`] into the scheduler host and port.
impl RequestUnpack for RegisterSchedulerRequest {
    type Unpacked = (Host, u16);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        let host = Host::new(self.host)
            .map_err(|_| invalid_argument("host must not be empty".to_owned()))?;
        let port = u16::try_from(self.port)
            .map_err(|_| invalid_argument(format!("port does not fit in `u16`: {}", self.port)))?;
        Ok((host, port))
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

#[cfg(test)]
mod tests {
    use spider_core::job::submission::CompressedJobSubmission;
    use tonic::Status;

    use super::*;
    use crate::common;
    use crate::storage;

    /// Creates a success report with distinct identifiers and the given task result.
    ///
    /// # Returns
    ///
    /// The success report.
    fn success_request(
        task_id: TaskId,
        serialized_outputs: Vec<u8>,
        children: Vec<storage::CompressedJobSubmission>,
    ) -> ReportTaskSuccessRequest {
        ReportTaskSuccessRequest {
            session_id: 11,
            job_id: 13,
            task_id: Some(common::TaskId::from(task_id)),
            task_instance_id: 17,
            execution_manager_id: 19,
            serialized_outputs,
            children,
        }
    }

    #[test]
    fn regular_success_unpacks_outputs_children_and_identifiers() -> Result<(), Status> {
        let children = vec![
            CompressedJobSubmission {
                compressed_serialized_task_graph: vec![1, 2],
                compressed_serialized_job_inputs: vec![3],
            },
            CompressedJobSubmission {
                compressed_serialized_task_graph: vec![4],
                compressed_serialized_job_inputs: vec![5, 6],
            },
        ];
        for (outputs, children) in [(Vec::new(), Vec::new()), (vec![7, 8], children)] {
            let request = success_request(
                TaskId::Index(23),
                outputs.clone(),
                children.clone().into_iter().map(Into::into).collect(),
            );
            assert_eq!(
                request.unpack()?,
                (
                    11,
                    JobId::from(13),
                    TaskId::Index(23),
                    17,
                    TaskSuccessOutcome::Regular {
                        serialized_outputs: outputs,
                        children,
                    },
                )
            );
        }
        Ok(())
    }

    #[test]
    fn finalizing_success_unpacks_empty_report() -> Result<(), Status> {
        for task_id in [TaskId::Commit, TaskId::Cleanup] {
            assert_eq!(
                success_request(task_id, Vec::new(), Vec::new()).unpack()?,
                (
                    11,
                    JobId::from(13),
                    task_id,
                    17,
                    TaskSuccessOutcome::Finalizing
                )
            );
        }
        Ok(())
    }

    #[test]
    fn finalizing_success_rejects_outputs_and_children() {
        let child = storage::CompressedJobSubmission {
            compressed_serialized_task_graph: vec![1],
            compressed_serialized_task_graph_input: vec![2],
        };
        for task_id in [TaskId::Commit, TaskId::Cleanup] {
            for (outputs, children) in [
                (vec![3], Vec::new()),
                (Vec::new(), vec![child.clone()]),
                (vec![3], vec![child.clone()]),
            ] {
                let error = success_request(task_id, outputs, children)
                    .unpack()
                    .expect_err("finalizing reports must carry neither outputs nor children");
                assert_eq!(Status::from(error).code(), Code::InvalidArgument);
            }
        }
    }
}
