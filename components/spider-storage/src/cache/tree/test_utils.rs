//! The fixtures shared by the job-tree tests.

use spider_core::types::id::JobId;

use crate::cache::tree::Lane;
use crate::cache::tree::TreeEvent;
use crate::cache::tree::TreeEventKind;

pub(super) const ROOT_ID: JobId = JobId::from(1);

pub(super) const fn root_event(kind: TreeEventKind) -> TreeEvent {
    TreeEvent {
        job_id: ROOT_ID,
        kind,
    }
}

pub(super) const fn start() -> TreeEventKind {
    TreeEventKind::Start {
        ready_task_indices: Vec::new(),
    }
}

pub(super) fn failure(lane: Lane, message: &str) -> TreeEventKind {
    TreeEventKind::TerminalFailure {
        lane,
        message: message.to_owned(),
    }
}
