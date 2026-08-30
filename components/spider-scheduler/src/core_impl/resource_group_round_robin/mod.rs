//! Resource-group-aware round-robin scheduler.
//!
//! The core is a single-threaded, tick-based loop that makes scheduling decisions using two levels
//! of round-robin: the outer level interleaves resource groups, while the inner level interleaves
//! active jobs within each resource group.

// The dispatch queues have no consumer outside their own tests until the rest of the core lands, so
// every item they expose reads as dead. `expect` rather than `allow`: once `implementation.rs` uses
// the queues, this attribute becomes unfulfilled and the compiler flags it for removal.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the core and the dispatch service that consume the queues have not landed yet"
    )
)]
mod dispatch_queue;

// The registry has no consumer outside tests until the rest of the core lands, so every item it
// exposes reads as dead. `expect` rather than `allow`: once `implementation.rs` uses the registry,
// this attribute becomes unfulfilled and the compiler flags it for removal.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the core that consumes the registry has not landed yet"
    )
)]
mod job_registry;

// The scheduling state has no consumer until the rest of the core lands, so every item it exposes
// reads as dead. `expect` rather than `allow`: once `implementation.rs` uses the state, this
// attribute becomes unfulfilled and the compiler flags it for removal.
#[expect(
    dead_code,
    reason = "the core that consumes the scheduling state has not landed yet"
)]
mod scheduling_state;
