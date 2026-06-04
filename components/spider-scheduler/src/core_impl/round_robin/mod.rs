//! Round-robin scheduler.
//!
//! This scheduler provides basic fairness across jobs using a round-robin scheduling policy. It
//! polls tasks from the inbound queue (maintained by the storage service) and organizes jobs into
//! two sets:
//!
//! * Active jobs: jobs that participate in round-robin scheduling.
//! * Pending jobs: jobs that are buffered but not yet scheduled. When an active job has no
//!   remaining schedulable tasks, it is replaced by the next pending job in FIFO order.
//!
//! The scheduler operates in discrete ticks. During each tick, it attempts to consume the results
//! of an asynchronous inbound-queue polling operation and loads any newly available tasks into its
//! internal buffers. It then makes scheduling decisions until the dispatch queue reaches capacity.
//!
//! # Properties
//!
//! * Each round-robin cycle may schedule at most one additional commit task and one additional
//!   cleanup task, if available.
//! * All buffered tasks are unique. Tasks loaded from the inbound queue are deduplicated before
//!   entering the scheduler's internal buffers.
//!
//! # Configuration
//!
//! * `active_job_queue_capacity`: Maximum number of active jobs maintained by the scheduler.
//! * `dispatch_queue_capacity`: Maximum number of task assignments in the dispatch queue.
//! * `ready_task_capacity`: Maximum number of ready tasks buffered by the scheduler.
//! * `commit_ready_task_capacity`: Maximum number of buffered commit-ready tasks.
//! * `cleanup_ready_task_capacity`: Maximum number of buffered cleanup-ready tasks.
//! * `storage_poll_timeout_ms`: Maximum time, in milliseconds, that inbound-queue polling may block
//!   on the storage-service side.
//! * `tick_interval_ms`: Interval, in milliseconds, between scheduler ticks (tick execution time
//!   included).

mod implementation;

#[cfg(test)]
mod tests;

pub use implementation::{RoundRobinConfig, RoundRobinCore};
