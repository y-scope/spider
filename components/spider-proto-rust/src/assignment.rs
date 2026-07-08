//! Conversions between protobuf task-assignment messages and their Spider core representations.

use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SchedulerId;
use spider_core::types::id::TaskAssignmentId;
use spider_core::types::id::TaskId;
use spider_core::types::scheduler::SchedulerResponse;
use spider_core::types::scheduler::TaskAssignment;
use spider_core::types::scheduler::TaskAssignmentRecord;

use crate::common;
use crate::error::Error;
use crate::scheduler::NextTaskResponse;
use crate::scheduler::TaskAssignmentRecord as ProtoTaskAssignmentRecord;
use crate::scheduler::next_task_response;

impl From<TaskAssignmentRecord> for ProtoTaskAssignmentRecord {
    fn from(record: TaskAssignmentRecord) -> Self {
        Self {
            id: record.id.get(),
            from: record.from.get(),
        }
    }
}

impl From<ProtoTaskAssignmentRecord> for TaskAssignmentRecord {
    fn from(record: ProtoTaskAssignmentRecord) -> Self {
        Self {
            id: TaskAssignmentId::from(record.id),
            from: SchedulerId::from(record.from),
        }
    }
}

impl TryFrom<NextTaskResponse> for Option<SchedulerResponse> {
    type Error = Error;

    fn try_from(response: NextTaskResponse) -> Result<Self, Self::Error> {
        match response.result {
            Some(next_task_response::Result::Assignment(assignment)) => {
                let task_id = TaskId::try_from(assignment.task_id.ok_or(Error::TaskIdMissing)?)?;
                Ok(Some(SchedulerResponse {
                    task_assignment: TaskAssignment {
                        id: TaskAssignmentId::from(assignment.id),
                        resource_group_id: ResourceGroupId::from(assignment.resource_group_id),
                        job_id: JobId::from(assignment.job_id),
                        task_id,
                    },
                    scheduler_id: SchedulerId::from(assignment.scheduler_id),
                    session_id: assignment.session_id,
                }))
            }
            Some(next_task_response::Result::NoTask(common::Void {})) => Ok(None),
            None => Err(Error::NextTaskResultMissing),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::TaskId as ProtoTaskId;
    use crate::common::Void;
    use crate::common::task_id::Kind as ProtoTaskIdKind;
    use crate::scheduler::SchedulerAssignment;
    use crate::scheduler::next_task_response;

    #[test]
    fn assignment_record_converts_to_protocol() {
        let record = ProtoTaskAssignmentRecord::from(TaskAssignmentRecord::new(
            TaskAssignmentId::from(7),
            SchedulerId::from(9),
        ));

        assert_eq!(record.id, 7);
        assert_eq!(record.from, 9);
    }

    #[test]
    fn protocol_assignment_record_converts_to_core() {
        let record = TaskAssignmentRecord::from(ProtoTaskAssignmentRecord { id: 7, from: 9 });

        assert_eq!(record.id, TaskAssignmentId::from(7));
        assert_eq!(record.from, SchedulerId::from(9));
    }

    #[test]
    fn next_task_response_converts_assignment() {
        let response = NextTaskResponse {
            result: Some(next_task_response::Result::Assignment(
                SchedulerAssignment {
                    id: 1,
                    resource_group_id: 2,
                    job_id: 3,
                    task_id: Some(ProtoTaskId {
                        kind: Some(ProtoTaskIdKind::Index(7)),
                    }),
                    scheduler_id: 4,
                    session_id: 5,
                },
            )),
        };

        let scheduler_response = Option::<SchedulerResponse>::try_from(response)
            .expect("assignment response should convert")
            .expect("assignment response should yield a task");

        assert_eq!(
            scheduler_response.task_assignment.id,
            TaskAssignmentId::from(1)
        );
        assert_eq!(
            scheduler_response.task_assignment.resource_group_id,
            ResourceGroupId::from(2)
        );
        assert_eq!(scheduler_response.task_assignment.job_id, JobId::from(3));
        assert_eq!(scheduler_response.task_assignment.task_id, TaskId::Index(7));
        assert_eq!(scheduler_response.scheduler_id, SchedulerId::from(4));
        assert_eq!(scheduler_response.session_id, 5);
    }

    #[test]
    fn next_task_response_no_task_converts_to_none() {
        let response = NextTaskResponse {
            result: Some(next_task_response::Result::NoTask(Void {})),
        };

        let result = Option::<SchedulerResponse>::try_from(response)
            .expect("no_task response should convert");

        assert!(result.is_none());
    }

    #[test]
    fn next_task_response_missing_result_is_error() {
        let response = NextTaskResponse { result: None };

        assert!(matches!(
            Option::<SchedulerResponse>::try_from(response),
            Err(Error::NextTaskResultMissing)
        ));
    }

    #[test]
    fn next_task_response_missing_task_id_is_error() {
        let response = NextTaskResponse {
            result: Some(next_task_response::Result::Assignment(
                SchedulerAssignment {
                    id: 1,
                    resource_group_id: 2,
                    job_id: 3,
                    task_id: None,
                    scheduler_id: 4,
                    session_id: 5,
                },
            )),
        };

        assert!(matches!(
            Option::<SchedulerResponse>::try_from(response),
            Err(Error::TaskIdMissing)
        ));
    }
}
