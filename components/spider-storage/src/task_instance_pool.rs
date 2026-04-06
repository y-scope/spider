use async_trait::async_trait;
use spider_core::types::id::TaskInstanceId;

use crate::cache::{
    error::InternalError,
    task::{SharedTaskControlBlock, SharedTerminationTaskControlBlock},
};

/// Connector for creating and registering task instances in the task instance pool.
///
/// This trait is invoked by the cache layer to allocate task instance IDs and register newly
/// created task instances.
#[async_trait]
pub trait TaskInstancePoolConnector: Clone + Send + Sync {
    /// Allocates a new task instance ID.
    ///
    /// Implementations must guarantee that each returned ID is globally unique across all
    /// invocations.
    ///
    /// # Returns
    ///
    /// A unique task instance ID.
    fn get_next_available_task_instance_id(&self) -> TaskInstanceId;

    /// Registers a task instance with the given task control block (TCB).
    ///
    /// # Parameters
    ///
    /// * `task_instance_id` - A task instance ID previously allocated via
    ///   [`TaskInstancePoolConnector::get_next_available_task_instance_id`].
    /// * `tcb` - The task control block associated with the task instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the task instance cannot be registered in the pool.
    async fn register_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        tcb: SharedTaskControlBlock,
    ) -> Result<(), InternalError>;

    /// Registers a termination task instance with the given termination task control block.
    ///
    /// # Parameters
    ///
    /// * `task_instance_id` - A task instance ID previously allocated via
    ///   [`TaskInstancePoolConnector::get_next_available_task_instance_id`].
    /// * `termination_tcb` - The termination task control block associated with the task instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the task instance cannot be registered in the pool.
    async fn register_termination_task_instance(
        &self,
        task_instance_id: TaskInstanceId,
        termination_tcb: SharedTerminationTaskControlBlock,
    ) -> Result<(), InternalError>;
}
