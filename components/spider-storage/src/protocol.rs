use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use spider_core::{
    job::JobState,
    task::{TaskGraph, TaskMetadata},
    types::{
        id::{
            DataId,
            JobId,
            ResourceGroupId,
            SchedulerId,
            SignedJobId,
            SignedTaskId,
            SignedTaskInstanceId,
            TaskId,
            TaskInstanceId,
            WorkerId,
        },
        io::{Data, TaskInput, TaskOutput},
    },
};

use crate::StorageError;

/// Represents all possible owners of a shared [`Data`] object.
pub enum SharedDataOwner {
    ResourceGroup(ResourceGroupId),
    Job(SignedJobId),
}

/// Represents the possible results of a job when queried.
pub enum JobResult {
    NotReady,
    Output(Vec<TaskOutput>),
    Stopped,
}

/// The storage interface for the Spider scheduling framework. The Spider storage backend must
/// implement the following traits:
///
/// * [`JobOrchestration`]
/// * [`TaskOrchestration`]
/// * [`DataManagement`]
/// * [`Scheduling`]
/// * [`LivenessTracking`]
pub trait SpiderStorage:
    JobOrchestration + TaskOrchestration + DataManagement + Scheduling + LivenessTracking {
}

/// Implements [`SpiderStorage`] for any type that implements all the sub-traits.
impl<T> SpiderStorage for T where
    T: JobOrchestration + TaskOrchestration + DataManagement + Scheduling + LivenessTracking
{
}

/// Defines the storage interface for job orchestration.
///
/// In the Spider scheduling framework, every job is associated with a resource group ID.
/// Orchestration operations may only be performed when the provided resource group ID matches the
/// one associated with the target job.
///
/// # NOTE
///
/// All operations defined by this trait **must be transactional**. Implementations are required to
/// guarantee atomicity and consistency for each operation.
#[async_trait]
pub trait JobOrchestration {
    /// Submits a job to the storage backend.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The owner of the created job.
    /// * `task_graph` - The task graph representing the job's tasks and their dependencies.
    /// * `task_inputs` - A vector of task inputs required for the job.
    ///
    /// # Returns
    ///
    /// The ID of the submitted job on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn submit_job(
        &self,
        resource_group_id: ResourceGroupId,
        task_graph: Arc<TaskGraph>,
        task_inputs: Vec<TaskInput>,
    ) -> Result<JobId, StorageError>;

    /// Lists all job IDs owned by the given resource group.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The resource group ID whose jobs are to be listed.
    ///
    /// # Returns
    ///
    /// A vector of job IDs associated with the specified resource group on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn list_jobs(
        &self,
        resource_group_id: ResourceGroupId,
    ) -> Result<Vec<JobId>, StorageError>;

    /// Retrieves the current state of a job.
    ///
    /// # Parameters
    ///
    /// * `signed_id` - The signed ID of the target job.
    ///
    /// # Returns
    ///
    /// The current state of the job on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn get_job_state(&self, signed_id: SignedJobId) -> Result<JobState, StorageError>;

    /// Retrieves the result outputs of a completed job.
    ///
    /// # Parameters
    ///
    /// * `signed_id` - The signed ID of the target job.
    ///
    /// # Returns
    ///
    /// * [`JobResult::NotReady`] if the job is not yet completed.
    /// * [`JobResult::Output`] if the job has completed successfully.
    /// * [`JobResult::Stopped`] if the job has been failed or cancelled.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn get_job_result(&self, signed_id: SignedJobId) -> Result<JobResult, StorageError>;

    /// Cancels a job.
    ///
    /// # Parameters
    ///
    /// * `signed_id` - The signed ID of the target job.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn cancel_job(&self, signed_id: SignedJobId) -> Result<(), StorageError>;

    /// Deletes a job.
    ///
    /// This method is designed for GC and is not expected to be invoked by users. To delete a job,
    /// it must be in one of the following states:
    ///
    /// * [`JobState::Succeeded`]
    /// * [`JobState::Failed`]
    /// * [`JobState::Cancelled`]
    ///
    /// # Parameters
    ///
    /// * `signed_id` - The signed ID of the target job.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn delete_job(&self, signed_id: SignedJobId) -> Result<(), StorageError>;
}

/// Defines the storage interface for task orchestration.
///
/// In the Spider scheduling framework, every task is associated with a resource group ID.
/// Orchestration operations may only be performed when the provided resource group ID matches the
/// one associated with the target task.
///
/// # NOTE
///
/// All operations defined by this trait **must be transactional**. Implementations are required to
/// guarantee atomicity and consistency for each operation.
#[async_trait]
pub trait TaskOrchestration {
    /// Retrieves the input data for a task.
    ///
    /// # Parameters
    ///
    /// * `signed_id` - The signed ID of the target task.
    ///
    /// # Returns
    ///
    /// A vector of task inputs on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn get_task_inputs(
        &self,
        signed_id: SignedTaskId,
    ) -> Result<Vec<TaskInput>, StorageError>;

    /// Retrieves the output data for a task.
    ///
    /// # Parameters
    ///
    /// * `signed_id` - The signed ID of the target task.
    ///
    /// # Returns
    ///
    /// A vector of task outputs on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn get_task_outputs(
        &self,
        signed_id: SignedTaskId,
    ) -> Result<Vec<TaskOutput>, StorageError>;

    /// Creates a new task instance for execution.
    ///
    /// This method is typically invoked by the scheduler when a task is ready to be executed. If
    /// the task is in [`TaskState::Ready`], this method will transition it to
    /// [`TaskState::Running`].
    ///
    /// # Parameters
    ///
    /// * `signed_id` - The signed ID of the target task.
    ///
    /// # Returns
    ///
    /// The ID of the created task instance on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn create_task_instance(
        &self,
        signed_id: SignedTaskId,
    ) -> Result<TaskInstanceId, StorageError>;

    /// Completes a task instance and uploads its outputs.
    ///
    /// On success, this method will transition the task instance to [`TaskState::Succeeded`].
    ///
    /// # Parameters
    ///
    /// * `signed_id` - The signed ID of the target task instance.
    /// * `outputs` - A vector of task outputs produced by the completed task instance.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn complete_task_instance(
        &self,
        signed_id: SignedTaskInstanceId,
        outputs: Vec<TaskOutput>,
    ) -> Result<(), StorageError>;

    /// Cancels a task instance.
    ///
    /// If the cancelled instance is the only task instance associated with the task, this method
    /// will also transition the task to [`TaskState::Cancelled`].
    ///
    /// # Parameters
    ///
    /// * `signed_id` - The signed ID of the target task instance.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn cancel_task_instance(
        &self,
        signed_id: SignedTaskInstanceId,
    ) -> Result<(), StorageError>;

    /// Marks a task instance as failed and records the error message.
    ///
    /// If the failed instance is the only task instance associated with the task, this method
    /// will also transition the task to [`TaskState::Failed`].
    ///
    /// # Parameters
    ///
    /// * `signed_id` - The signed ID of the target task instance.
    /// * `error_message` - A description of the error that caused the task instance to fail.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn fail_task_instance(
        &self,
        signed_id: SignedTaskInstanceId,
        error_message: String,
    ) -> Result<(), StorageError>;
}

/// Defines the storage interface for data management.
///
/// In the Spider scheduling framework, a data object is a shareable value holder that can be shared
/// across jobs and resource groups. The lifecycle of a data object is managed through reference
/// counting. This trait provides operations to create data objects and manages their reference
/// counts.
///
/// # NOTE
///
/// All operations defined by this trait **must be transactional**. Implementations are required to
/// guarantee atomicity and consistency for each operation.
#[async_trait]
pub trait DataManagement {
    /// Creates a new data object.
    ///
    /// An owner must be provided as the initial reference holder of the newly created data object.
    ///
    /// # Parameters
    ///
    /// * `owner` - The owner of the created data object.
    /// * `data` - The data object to store.
    ///
    /// # Returns
    ///
    /// The ID of the created data object on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn create_data(&self, owner: SharedDataOwner, data: Data)
    -> Result<DataId, StorageError>;

    /// Retrieves a data object by its ID on behalf of the given owner.
    ///
    /// To successfully retrieve the data, the specified owner must hold an existing reference to
    /// the data object.
    ///
    /// # Parameters
    ///
    /// * `owner` - The owner requesting access to the data object.
    /// * `id` - The ID of the data object to retrieve.
    ///
    /// # Returns
    ///
    /// The requested data object on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn get_data(&self, owner: SharedDataOwner, id: DataId) -> Result<Data, StorageError>;

    /// Creates a new reference from the given owner to the specified data object.
    ///
    /// This operation grants the owner access to the data object by registering a reference. The
    /// data object must already exist.
    ///
    /// # Parameters
    ///
    /// * `owner` - The owner to register as a reference holder.
    /// * `id` - The ID of the data object to reference.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn add_data_ref(&self, owner: SharedDataOwner, id: DataId) -> Result<(), StorageError>;

    /// Removes an existing reference from the given owner to the specified data object.
    ///
    /// This operation revokes the owner's access to the data object by removing an existing
    /// reference. If this removal causes the total reference count of the data object to reach
    /// zero, the data object should be deleted as part of the same operation.
    ///
    /// # Parameters
    ///
    /// * `owner` - The owner whose reference should be removed.
    /// * `id` - The ID of the data object to un-reference.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn remove_data_ref(&self, owner: SharedDataOwner, id: DataId)
    -> Result<(), StorageError>;
}

/// Defines the storage interface for scheduling operations.
///
/// # NOTE
///
/// All operations defined by this trait **must be transactional**. Implementations are required to
/// guarantee atomicity and consistency for each operation.
#[async_trait]
pub trait Scheduling {
    /// Retrieves tasks that are ready for execution.
    ///
    /// A task is considered ready for execution if it is in [`TaskState::Ready`].
    ///
    /// # Parameters
    ///
    /// * `num_tasks` - The maximum number of ready tasks to retrieve. If `None`, all ready tasks
    ///   are returned.
    ///
    /// # Returns
    ///
    /// A vector of task metadata for ready tasks on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn get_ready_tasks(
        &self,
        num_tasks: Option<usize>,
    ) -> Result<Vec<TaskMetadata>, StorageError>;

    /// Retrieves jobs that have failed.
    ///
    /// A job is considered failed if it is in [`JobState::Failed`].
    ///
    /// # Parameters
    ///
    /// * `num_jobs` - The maximum number of failed jobs to retrieve. If `None`, all failed jobs are
    ///   returned.
    ///
    /// # Returns
    ///
    /// A vector of job IDs for failed jobs on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn get_failed_jobs(&self, num_jobs: Option<usize>) -> Result<Vec<JobId>, StorageError>;

    /// Resets a failed job to allow it to be retried.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the failed job to reset.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn reset_failed_job(&self, job_id: JobId) -> Result<(), StorageError>;

    /// Retrieves tasks that have exceeded their execution timeout.
    ///
    /// # Parameters
    ///
    /// * `timeout` - The duration after which a running task is considered timed out.
    ///
    /// # Returns
    ///
    /// A vector of task IDs for timed-out tasks on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn get_timeout_tasks(&self, timeout: Duration) -> Result<Vec<TaskId>, StorageError>;

    /// Retrieves workers that have not sent a heartbeat within the specified timeout.
    ///
    /// # Parameters
    ///
    /// * `timeout` - The duration after which a worker without a heartbeat is considered timed out.
    ///
    /// # Returns
    ///
    /// A vector of worker IDs for timed-out workers on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn get_timeout_workers(&self, timeout: Duration) -> Result<Vec<WorkerId>, StorageError>;
}

/// Defines the storage interface for liveness tracking operations.
///
/// # NOTE
///
/// All operations defined by this trait **must be transactional**. Implementations are required to
/// guarantee atomicity and consistency for each operation.
#[async_trait]
pub trait LivenessTracking {
    /// Registers a new worker in the scheduling system.
    ///
    /// # Returns
    ///
    /// The unique ID assigned to the newly registered worker on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn register_worker(&self) -> Result<WorkerId, StorageError>;

    /// Registers a new scheduler in the scheduling system.
    ///
    /// # Parameters
    ///
    /// * `address` - The network address of the scheduler.
    /// * `port` - The port number on which the scheduler is listening to.
    ///
    /// # Returns
    ///
    /// The unique ID assigned to the newly registered scheduler on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn register_scheduler(
        &self,
        address: String,
        port: u16,
    ) -> Result<SchedulerId, StorageError>;

    /// Updates the heartbeat timestamp for a worker.
    ///
    /// Workers should call this method periodically to signal that they are still alive and
    /// operational. The scheduler uses these heartbeat timestamps to detect failed or unreachable
    /// workers.
    ///
    /// # Parameters
    ///
    /// * `worker_id` - The ID of the worker whose heartbeat should be updated.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] instance indicating the failures.
    ///
    /// Implementations **must document** the specific error variants they may return and the
    /// conditions under which those errors occur.
    async fn update_worker_heartbeat(&self, worker_id: WorkerId) -> Result<(), StorageError>;
}
