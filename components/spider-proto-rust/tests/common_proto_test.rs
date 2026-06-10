use spider_core::types::id::TaskId;
use spider_proto_rust::common::{self, task_id};

#[test]
fn common_task_id_converts_from_core_task_id() {
    let task_id = common::TaskId::from(TaskId::Commit);

    assert!(matches!(task_id.kind, Some(task_id::Kind::Commit(_))));
}

#[test]
fn common_task_id_converts_to_core_task_id() {
    let task_id = TaskId::try_from(common::TaskId {
        kind: Some(task_id::Kind::Cleanup(common::Void {})),
    })
    .expect("common task id conversion should succeed");

    assert_eq!(task_id, TaskId::Cleanup);
}
