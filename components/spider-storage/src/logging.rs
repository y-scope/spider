//! Logging setup for the storage server binaries.

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

/// Initializes the global tracing subscriber with JSON-formatted, non-blocking output to `stderr`.
///
/// Log events are forwarded to a dedicated background worker so that emitting a log never blocks
/// the async runtime. The returned [`WorkerGuard`] must be held for the lifetime of the program;
/// dropping it flushes any buffered logs and shuts the worker down.
///
/// # Returns
///
/// The [`WorkerGuard`] for the non-blocking writer.
pub fn set_up_logging() -> WorkerGuard {
    let (non_blocking_writer, guard) = tracing_appender::non_blocking(std::io::stderr());
    tracing_subscriber::fmt()
        .event_format(
            tracing_subscriber::fmt::format()
                .with_level(true)
                .with_target(false)
                .with_file(true)
                .with_line_number(true)
                .json(),
        )
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(false)
        .with_writer(non_blocking_writer)
        .init();
    guard
}
