//! Wire protocol between the execution manager and a `spider-task-executor` subprocess.
//!
//! The parent encodes each [`Request`] with `bincode` and writes it as one length-delimited frame
//! over the executor's `stdin`; the executor reads frames, dispatches to the TDL package manager,
//! and writes one [`Response`] frame back over `stdout`.
//!
//! `stderr` is **not** carried over this protocol. The executor writes diagnostics to its own
//! stderr; how those bytes are disposed of (inherited, piped, redirected to a log file) is a choice
//! made by whoever spawned the process.

use serde::Deserialize;
use serde::Serialize;
use spider_core::task::TdlContext;

/// Request from the parent process (execution manager) to the executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Execute {
        /// TDL information for identifying which task to execute.
        tdl_context: TdlContext,

        /// Serialized task context.
        raw_ctx: Vec<u8>,

        /// Serialized task inputs.
        raw_inputs: Vec<u8>,
    },

    Shutdown,
}

/// Reply from the executor to the parent process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Result {
        outcome: ExecutorOutcome,
        /// Wall-clock duration of the FFI call, measured by the executor.
        elapsed_us: u64,
    },
}

/// Outcome of a task execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutorOutcome {
    /// Task outputs serialized in wire-format.
    Success { outputs: Vec<u8> },

    /// [`crate::ExecutorError`] serialized in msgpack.
    Failure { error: Vec<u8> },
}
