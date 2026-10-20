//! The limits that the storage service enforces on job trees and on task reports.

/// The maximum number of characters of a failure message that the failed job's error stores.
pub const MAX_ERROR_MESSAGE_CHARS: usize = 64 * 1024;
