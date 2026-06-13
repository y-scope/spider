//! Helpers for converting Spider IDs to protobuf fields.

use spider_core::types::id::TaskId;

use crate::{common, error::Error, storage};

impl From<TaskId> for common::TaskId {
    fn from(task_id: TaskId) -> Self {
        let kind = match task_id {
            TaskId::Index(task_index) => common::task_id::Kind::Index(
                u64::try_from(task_index).expect("task index does not fit in u64"),
            ),
            TaskId::Commit => common::task_id::Kind::Commit(common::Void {}),
            TaskId::Cleanup => common::task_id::Kind::Cleanup(common::Void {}),
        };
        Self { kind: Some(kind) }
    }
}

impl From<TaskId> for storage::TaskId {
    fn from(task_id: TaskId) -> Self {
        let kind = match task_id {
            TaskId::Index(task_index) => storage::task_id::Kind::Index(
                u64::try_from(task_index).expect("task index does not fit in u64"),
            ),
            TaskId::Commit => storage::task_id::Kind::Commit(storage::Void {}),
            TaskId::Cleanup => storage::task_id::Kind::Cleanup(storage::Void {}),
        };
        Self { kind: Some(kind) }
    }
}

impl TryFrom<common::TaskId> for TaskId {
    type Error = Error;

    fn try_from(task_id: common::TaskId) -> Result<Self, Self::Error> {
        match task_id.kind {
            Some(common::task_id::Kind::Index(task_index)) => Ok(Self::Index(
                usize::try_from(task_index).map_err(|_| Error::TaskIndexOutOfRange(task_index))?,
            )),
            Some(common::task_id::Kind::Commit(_)) => Ok(Self::Commit),
            Some(common::task_id::Kind::Cleanup(_)) => Ok(Self::Cleanup),
            None => Err(Error::TaskIdKindMissing),
        }
    }
}

impl TryFrom<storage::TaskId> for TaskId {
    type Error = Error;

    fn try_from(task_id: storage::TaskId) -> Result<Self, Self::Error> {
        match task_id.kind {
            Some(storage::task_id::Kind::Index(task_index)) => Ok(Self::Index(
                usize::try_from(task_index).map_err(|_| Error::TaskIndexOutOfRange(task_index))?,
            )),
            Some(storage::task_id::Kind::Commit(_)) => Ok(Self::Commit),
            Some(storage::task_id::Kind::Cleanup(_)) => Ok(Self::Cleanup),
            None => Err(Error::TaskIdKindMissing),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_id_to_protocol_u64_converts_index_task() {
        let task_id = common::TaskId::from(TaskId::Index(7));

        assert!(matches!(
            task_id.kind,
            Some(common::task_id::Kind::Index(7))
        ));
    }

    #[test]
    fn task_id_to_protocol_converts_commit_task() {
        let task_id = common::TaskId::from(TaskId::Commit);

        assert!(matches!(
            task_id.kind,
            Some(common::task_id::Kind::Commit(_))
        ));
    }

    #[test]
    fn task_id_to_protocol_converts_cleanup_task() {
        let task_id = common::TaskId::from(TaskId::Cleanup);

        assert!(matches!(
            task_id.kind,
            Some(common::task_id::Kind::Cleanup(_))
        ));
    }

    #[test]
    fn common_task_id_to_core_converts_index_task() {
        let task_id = TaskId::try_from(common::TaskId {
            kind: Some(common::task_id::Kind::Index(7)),
        })
        .expect("protocol task id conversion should succeed");

        assert_eq!(task_id, TaskId::Index(7));
    }

    #[test]
    fn common_task_id_to_core_rejects_missing_kind() {
        let error = TaskId::try_from(common::TaskId { kind: None })
            .expect_err("missing task id kind should fail");

        assert!(matches!(error, Error::TaskIdKindMissing));
    }

    #[test]
    fn storage_task_id_to_core_converts_index_task() {
        let task_id = TaskId::try_from(storage::TaskId {
            kind: Some(storage::task_id::Kind::Index(7)),
        })
        .expect("protocol task id conversion should succeed");

        assert_eq!(task_id, TaskId::Index(7));
    }
}
