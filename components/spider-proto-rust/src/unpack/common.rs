//! Shared [`Unpack`] helpers for types defined in `common.proto`.
//!
//! These conversions are shared across services: any request or response that carries a
//! [`common::TaskId`] funnels through [`unpack_task_id`] rather than duplicating the logic per
//! service.

use spider_core::types::id::TaskId;
use tonic::Code;

use crate::common;
use crate::unpack::UnpackError;

/// Converts a protobuf [`common::TaskId`] into a core [`TaskId`].
///
/// # Returns
///
/// The core [`TaskId`] on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`Code::InvalidArgument`] (as [`UnpackError`]) if the task ID is absent or carries an index
///   that cannot be represented.
pub(super) fn unpack_task_id(task_id: Option<common::TaskId>) -> Result<TaskId, UnpackError> {
    let task_id = task_id.ok_or_else(|| UnpackError {
        code: Code::InvalidArgument,
        message: "task ID is missing".to_owned(),
    })?;
    TaskId::try_from(task_id).map_err(|error| UnpackError {
        code: Code::InvalidArgument,
        message: error.to_string(),
    })
}
