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
//! Commit-ready and cleanup-ready tasks are buffered separately from regular active-job tasks. They
//! participate in round-robin scheduling through higher-priority tiers: on each round, commit tasks
//! are scheduled before cleanup tasks, and cleanup tasks are scheduled before regular active-job
//! tasks.
//!
//! The scheduler operates in discrete ticks. During each tick, it attempts to consume the results
//! of an asynchronous inbound-queue polling operation and loads any newly available tasks into its
//! internal buffers. It then makes scheduling decisions until the dispatch queue reaches capacity.
//!
//! # Properties
//!
//! * During each round-robin tick, the scheduler enqueues task assignments into the dispatch queue
//!   in the following priority order, subject to the remaining dispatch capacity:
//!   * Up to `active_job_queue_capacity` commit task assignments, in FIFO order.
//!     * This mirrors the fairness bound for active jobs: each job can contribute at most one
//!       commit task assignment per pass.
//!   * Exactly one cleanup task assignment, if any.
//!   * Exactly one regular task assignment for each active job, in FIFO order.
//! * All buffered tasks are unique. Tasks loaded from the inbound queue are deduplicated before
//!   entering the scheduler's internal buffers.
//! * Active job retirement is deferred. When an active job has no remaining schedulable tasks in
//!   the scheduler, it is not immediately replaced by the next pending job. Instead, retirement is
//!   delayed until the round-robin order loops back to that active job and its buffer is still
//!   empty.
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
//! * `finalizing_job_expiration_timeout_sec`: Time, in seconds, that a job may remain in the
//!   finalizing state before the scheduler retires it.
//!
//! # Limitations
//!
//! * The scheduler is not notified when a job terminates if: (1) the job is cancelled and has no
//!   cleanup task; or (2) the job fails. In these cases, the scheduler may continue to buffer tasks
//!   that belong to the terminated job, and those tasks may eventually be dispatched. This does not
//!   affect system correctness, because storage will reject stale task assignments for terminated
//!   jobs. However, it may waste dispatch capacity and execution-manager cycles.
//!   * Related issue: <https://github.com/y-scope/spider/issues/345>
//! * The scheduler does not have job-level metadata for determining when an active job can be
//!   safely retired. As a result, an active job may be retired from the active-job queue even
//!   though additional tasks for that job are still buffered in the inbound queue. This is usually
//!   not an issue for flattened task graphs, where most ready tasks are exposed to the scheduler at
//!   once. However, it may occur frequently for task graphs with complex dependencies, especially
//!   when a large set of upstream tasks is followed by a small number of downstream tasks.
//!   * Related issue: <https://github.com/y-scope/spider/issues/344>

mod implementation;

#[cfg(test)]
mod tests;

pub use implementation::RoundRobinConfig;
pub use implementation::RoundRobinCore;
