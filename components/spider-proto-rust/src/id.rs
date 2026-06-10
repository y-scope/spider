//! Helpers for converting Spider IDs to protobuf fields.

use spider_core::types::id::TaskId;

use crate::common::{self, task_id};

impl From<TaskId> for common::TaskId {
    fn from(task_id: TaskId) -> Self {
        let kind = match task_id {
            TaskId::Index(task_index) => task_id::Kind::Index(
                u64::try_from(task_index).expect("task index does not fit in u64"),
            ),
            TaskId::Commit => task_id::Kind::Commit(common::Void {}),
            TaskId::Cleanup => task_id::Kind::Cleanup(common::Void {}),
        };
        Self { kind: Some(kind) }
    }
}

impl TryFrom<common::TaskId> for TaskId {
    type Error = String;

    fn try_from(task_id: common::TaskId) -> Result<Self, Self::Error> {
        match task_id.kind {
            Some(task_id::Kind::Index(task_index)) => usize::try_from(task_index)
                .map(TaskId::Index)
                .map_err(|error| format!("task index does not fit in usize: {error}")),
            Some(task_id::Kind::Commit(_)) => Ok(Self::Commit),
            Some(task_id::Kind::Cleanup(_)) => Ok(Self::Cleanup),
            None => Err("task id missing kind".to_owned()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_id_to_protocol_u64_converts_index_task() {
        let task_id = common::TaskId::from(TaskId::Index(7));

        assert!(matches!(task_id.kind, Some(task_id::Kind::Index(7))));
    }

    #[test]
    fn task_id_to_protocol_converts_commit_task() {
        let task_id = common::TaskId::from(TaskId::Commit);

        assert!(matches!(task_id.kind, Some(task_id::Kind::Commit(_))));
    }

    #[test]
    fn task_id_to_protocol_converts_cleanup_task() {
        let task_id = common::TaskId::from(TaskId::Cleanup);

        assert!(matches!(task_id.kind, Some(task_id::Kind::Cleanup(_))));
    }

    #[test]
    fn protocol_task_id_to_core_converts_index_task() {
        let task_id = TaskId::try_from(common::TaskId {
            kind: Some(task_id::Kind::Index(7)),
        })
        .expect("protocol task id conversion should succeed");

        assert_eq!(task_id, TaskId::Index(7));
    }

    #[test]
    fn protocol_task_id_to_core_rejects_missing_kind() {
        let error = TaskId::try_from(common::TaskId { kind: None })
            .expect_err("missing task id kind should fail");

        assert!(error.contains("missing kind"));
    }
}
