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
#[allow(
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    clippy::option_if_let_else,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::similar_names,
    clippy::needless_pass_by_value,
    clippy::too_many_lines,
    clippy::manual_let_else
)]
mod tests;
