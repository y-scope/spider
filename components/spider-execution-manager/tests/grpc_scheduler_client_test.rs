use spider_core::types::id::{ExecutionManagerId, JobId, ResourceGroupId, TaskId};
use spider_execution_manager::client::{SchedulerResponse, grpc::scheduler_response_to_result};
use spider_proto_rust::{
    common,
    scheduler::{NextTaskResponse, SchedulerAssignment, next_task_response},
};

#[test]
fn scheduler_response_to_result_returns_assignment() {
    let response = NextTaskResponse {
        result: Some(next_task_response::Result::Assignment(
            SchedulerAssignment {
                job_id: 11,
                task_id: Some(common::TaskId::from(TaskId::Commit)),
                resource_group_id: 13,
                session_id: 17,
            },
        )),
    };

    let assignment = scheduler_response_to_result(response)
        .expect("scheduler response conversion should succeed")
        .expect("scheduler response should contain an assignment");

    assert_eq!(
        assignment,
        SchedulerResponse {
            job_id: JobId::from(11),
            task_id: TaskId::Commit,
            resource_group_id: ResourceGroupId::from(13),
            session_id: 17,
        }
    );
}

#[test]
fn scheduler_response_to_result_rejects_missing_result() {
    let result = scheduler_response_to_result(NextTaskResponse { result: None });

    assert!(result.is_err());
}

#[test]
fn scheduler_response_to_result_rejects_empty_assignment_task_id() {
    let response = NextTaskResponse {
        result: Some(next_task_response::Result::Assignment(
            SchedulerAssignment {
                job_id: 11,
                task_id: None,
                resource_group_id: 13,
                session_id: 17,
            },
        )),
    };

    let result = scheduler_response_to_result(response);

    assert!(result.is_err());
}

#[test]
fn next_task_request_uses_execution_manager_id() {
    let request =
        spider_execution_manager::client::grpc::next_task_request(ExecutionManagerId::from(23));

    assert_eq!(request.execution_manager_id, 23);
}
