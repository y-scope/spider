//! Resource-group-aware round-robin scheduler.
//!
//! The core is a single-threaded, tick-based loop that makes scheduling decisions using two levels
//! of round-robin: the outer level interleaves resource groups, while the inner level interleaves
//! active jobs within each resource group.

mod dispatch_queue;
mod implementation;
mod inbound_queue_reader;
mod job_registry;
mod scheduling_state;

#[cfg(test)]
mod tests;

pub use implementation::ResourceGroupRoundRobinConfig;
pub use implementation::ResourceGroupRoundRobinCore;
