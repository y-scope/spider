// TODO(spider-storage): Address these clippy lints when stabilizing the cache layer.
#[allow(
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    clippy::option_if_let_else,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]
pub mod error;
#[allow(
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    clippy::option_if_let_else,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]
mod factory;
#[allow(
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    clippy::option_if_let_else,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]
mod job;
#[allow(
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    clippy::option_if_let_else,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]
mod task;
#[allow(
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    clippy::option_if_let_else,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]
mod types;

pub use factory::*;
pub use job::{JobControlBlock, ReadyQueueConnector, TaskInstancePoolConnector};

#[cfg(test)]
mod tests;
