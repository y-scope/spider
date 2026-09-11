//! [`RequestUnpack`] implementations for `scheduler.proto` requests.

use std::time::Duration;

use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::scheduler::TaskAssignmentRecord;

use crate::scheduler::HeartbeatRequest;
use crate::scheduler::NextTaskRequest;
use crate::scheduler::ShutdownRequest;
use crate::scheduler::TaskAssignmentRecord as ProtoTaskAssignmentRecord;
use crate::unpack::RequestUnpack;
use crate::unpack::UnpackError;

/// The unpacked form of [`NextTaskRequest`].
pub struct NextTaskRequestPayload {
    /// The execution manager requesting a task assignment.
    pub em_id: ExecutionManagerId,

    /// The resource group whose task assignments the execution manager wants to receive, or `None`
    /// to express no preference.
    pub rg_id: Option<ResourceGroupId>,

    /// The maximum duration to wait for an assignment.
    pub wait_time: Duration,

    /// The previously consumed assignment record, if any.
    pub prev_assignment: Option<TaskAssignmentRecord>,
}

/// Unpacks [`NextTaskRequest`] into a [`NextTaskRequestPayload`].
impl RequestUnpack for NextTaskRequest {
    type Unpacked = NextTaskRequestPayload;

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        Ok(NextTaskRequestPayload {
            em_id: ExecutionManagerId::from(self.execution_manager_id),
            rg_id: self.resource_group_id.map(ResourceGroupId::from),
            wait_time: Duration::from_millis(self.wait_time_ms),
            prev_assignment: self.prev_assignment.map(ProtoTaskAssignmentRecord::into),
        })
    }
}

/// Unpacks [`HeartbeatRequest`] into an [`ExecutionManagerId`].
impl RequestUnpack for HeartbeatRequest {
    type Unpacked = ExecutionManagerId;

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        Ok(ExecutionManagerId::from(self.execution_manager_id))
    }
}

/// Unpacks [`ShutdownRequest`] into a tuple containing:
///
/// * The execution manager ID.
/// * The previously consumed assignment records.
impl RequestUnpack for ShutdownRequest {
    type Unpacked = (ExecutionManagerId, Vec<TaskAssignmentRecord>);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        Ok((
            ExecutionManagerId::from(self.execution_manager_id),
            self.prev_assignments
                .into_iter()
                .map(ProtoTaskAssignmentRecord::into)
                .collect(),
        ))
    }
}
