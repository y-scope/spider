//! [`RequestUnpack`] implementations for `scheduler.proto` requests.

use std::time::Duration;

use spider_core::types::id::ExecutionManagerId;
use spider_core::types::scheduler::TaskAssignmentRecord;

use crate::scheduler::HeartbeatRequest;
use crate::scheduler::NextTaskRequest;
use crate::scheduler::ShutdownRequest;
use crate::scheduler::TaskAssignmentRecord as ProtoTaskAssignmentRecord;
use crate::unpack::RequestUnpack;
use crate::unpack::UnpackError;

/// Unpacks [`NextTaskRequest`] into a tuple containing:
///
/// * The execution manager ID.
/// * The previously consumed assignment record, if any.
/// * The maximum duration to wait for an assignment.
impl RequestUnpack for NextTaskRequest {
    type Unpacked = (ExecutionManagerId, Option<TaskAssignmentRecord>, Duration);

    fn unpack(self) -> Result<Self::Unpacked, UnpackError> {
        Ok((
            ExecutionManagerId::from(self.execution_manager_id),
            self.prev_assignment.map(ProtoTaskAssignmentRecord::into),
            Duration::from_millis(self.wait_time_ms),
        ))
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
