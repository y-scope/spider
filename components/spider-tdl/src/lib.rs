pub mod error;
pub mod ffi;
pub mod r#std;
pub mod task;
pub mod task_context;
pub mod wire;

pub use error::TdlError;
pub use task::{ExecutionResult, Task, TaskHandler, TaskHandlerImpl};
pub use task_context::TaskContext;

#[cfg(feature = "derive")]
pub use spider_tdl_derive::task;
