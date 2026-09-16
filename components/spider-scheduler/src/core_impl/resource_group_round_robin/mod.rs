//! Resource-group-aware round-robin scheduler.
//!
//! The core is a single-threaded, tick-based loop that makes scheduling decisions using two levels
//! of round-robin: the outer level interleaves resource groups, while the inner level interleaves
//! active jobs within each resource group.

mod dispatch_queue;

// The core has no consumer until the seam that implements `SchedulerCore` over it lands, so every
// item it and the modules it decides with expose reads as dead. `expect` rather than `allow`: once
// the seam constructs the core, this attribute becomes unfulfilled and the compiler flags it for
// removal.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the `SchedulerCore` implementation that runs the core has not landed yet"
    )
)]
mod implementation;

mod inbound_queue_reader;
mod job_registry;
mod scheduling_state;

#[cfg(test)]
mod tests;
