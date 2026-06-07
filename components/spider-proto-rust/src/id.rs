//! Helpers for converting Spider IDs to protobuf fields.

use spider_core::types::id::TaskId;

use crate::storage::{self, task_id};

impl From<TaskId> for storage::TaskId {
    fn from(task_id: TaskId) -> Self {
        let kind = match task_id {
            TaskId::Index(task_index) => task_id::Kind::Index(
                u64::try_from(task_index).expect("task index does not fit in u64"),
            ),
            TaskId::Commit => task_id::Kind::Commit(storage::Void {}),
            TaskId::Cleanup => task_id::Kind::Cleanup(storage::Void {}),
        };
        Self { kind: Some(kind) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_id_to_protocol_u64_converts_index_task() {
        let task_id = storage::TaskId::from(TaskId::Index(7));

        assert!(matches!(task_id.kind, Some(task_id::Kind::Index(7))));
    }

    #[test]
    fn task_id_to_protocol_converts_commit_task() {
        let task_id = storage::TaskId::from(TaskId::Commit);

        assert!(matches!(task_id.kind, Some(task_id::Kind::Commit(_))));
    }

    #[test]
    fn task_id_to_protocol_converts_cleanup_task() {
        let task_id = storage::TaskId::from(TaskId::Cleanup);

        assert!(matches!(task_id.kind, Some(task_id::Kind::Cleanup(_))));
    }
}
