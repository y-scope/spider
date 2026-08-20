//! Resource-group-aware round-robin scheduler.
//!
//! The core is a single-threaded, tick-based loop that makes scheduling decisions using two levels
//! of round-robin: the outer level interleaves resource groups, while the inner level interleaves
//! active jobs within each resource group.

// The registry has no consumer until the rest of the core lands, so every item it exposes reads as
// dead. `expect` rather than `allow`: once `implementation.rs` uses the registry, this attribute
// becomes unfulfilled and the compiler flags it for removal.
#[expect(
    dead_code,
    reason = "the core that consumes the registry has not landed yet"
)]
mod job_registry;
