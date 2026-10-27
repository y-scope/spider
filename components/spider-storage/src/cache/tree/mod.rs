//! Job trees: the tree control block and the planner.
//!
//! A job tree is a root job and its descendants. Every job of a tree submits its job-level events
//! through the tree's control block to the tree's committer, the single owner of the tree's event
//! receivers and structure. The committer plans each event against the tree's structure. Cancels
//! and terminal failures travel on the fast-forward channel, which the committer reads first; every
//! other event travels on the regular channel.

mod control_block;
mod event;
mod planner;
mod structure;
#[cfg(test)]
mod test_utils;

pub use control_block::EventReceivers;
pub use control_block::Pending;
pub use control_block::TreeControlBlock;
pub use event::Lane;
pub use event::TreeEvent;
pub use event::TreeEventKind;
pub use event::TreeEventOutcome;
pub use event::TreeEventReply;
pub use planner::Plan;
pub use planner::Publication;
pub use structure::TreeNode;
pub use structure::TreeStructure;
