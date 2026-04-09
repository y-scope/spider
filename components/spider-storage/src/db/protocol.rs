use async_trait::async_trait;
use spider_core::{
    job::JobState,
    task::TaskGraph,
    types::{
        id::{JobId, ResourceGroupId},
        io::{TaskInput, TaskOutput},
    },
};

use crate::db::error::DbError;

/// The database storage interface. A database storage must implement the following traits:
///
/// * [`ExternalJobOrchestration`]
/// * [`InternalJobOrchestration`]
/// * [`ResourceGroupManagement`]
#[async_trait]
pub trait DbStorage:
    ExternalJobOrchestration + InternalJobOrchestration + ResourceGroupManagement {
}

/// Defines the user-facing storage interface for job storage in the database.
#[async_trait]
pub trait ExternalJobOrchestration {
    /// Registers a job in the database.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The owner of the created job.
    /// * `task_graph` - The task graph representing the job's tasks and their dependencies.
    /// * `job_inputs` - A slice of job inputs required for the job.
    ///
    /// # Returns
    ///
    /// The ID of the submitted job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::ResourceGroupNotFound`] if the `resource_group_id` does not exist.
    /// * [`DbError::TaskGraphSerializationFailure`] if the `task_graph` serialization fails.
    /// * [`DbError::ValueSerializationFailure`] if the `job_inputs` serialization fails.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    ///
    /// # Note
    ///
    /// This function assumes that the `task_graph` and `job_inputs` are consistent.
    ///
    /// TODO: Fix this when #284 is addressed.
    async fn register(
        &self,
        resource_group_id: ResourceGroupId,
        task_graph: &TaskGraph,
        job_inputs: &[TaskInput],
    ) -> Result<JobId, DbError>;

    /// Gets the state of a job.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    ///
    /// # Returns
    ///
    /// The state of the job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::CorruptedDbState`] if the data in the DB is corrupted.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn get_state(&self, job_id: JobId) -> Result<JobState, DbError>;

    /// Gets the outputs of a job.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    ///
    /// # Returns
    ///
    /// The outputs of the job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::UnexpectedJobState`] if the job is not in [`JobState::Succeeded`] state.
    /// * [`DbError::CorruptedDbState`] if the data in the DB is corrupted.
    /// * [`DbError::ValueDeserializationFailure`] if the job outputs deserialization fails.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn get_outputs(&self, job_id: JobId) -> Result<Vec<TaskOutput>, DbError>;

    /// Gets the error message of a job.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    ///
    /// # Returns
    ///
    /// The error message of the job on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::UnexpectedJobState`] if the job is not in [`JobState::Failed`] state.
    /// * [`DbError::CorruptedDbState`] if the data in the DB is corrupted.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn get_error(&self, job_id: JobId) -> Result<String, DbError>;
}

/// Defines the internal storage interface for job storage in the database.
#[async_trait]
pub trait InternalJobOrchestration: Clone + Send + Sync {
    /// Starts a job.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::UnexpectedJobState`] if the job is not in [`JobState::Ready`] state.
    /// * [`DbError::CorruptedDbState`] if the data in the DB is corrupted.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn start(&self, job_id: JobId) -> Result<(), DbError>;

    /// Sets the state of a job.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    /// * `state` - The new state to set for the job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::InvalidJobStateTransition`] if transition from current state to `state` is
    ///   invalid.
    /// * [`DbError::CorruptedDbState`] if the data in the DB is corrupted.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn set_state(&self, job_id: JobId, state: JobState) -> Result<(), DbError>;

    /// Commits the job outputs.
    ///
    /// A job is ready to commit if all its tasks have been completed successfully. The job outputs
    /// will be persisted in the database. The job enters the state:
    ///
    /// * [`JobState::CommitReady`] if the job has a commit task.
    /// * [`JobState::Succeeded`] otherwise.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    /// * `job_outputs` - The outputs of the job.
    /// * `has_commit_task` - Whether the job has commit task.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::InvalidJobStateTransition`] if the current job state is not
    ///   [`JobState::Running`].
    /// * [`DbError::ValueSerializationFailure`] if the `job_outputs` serialization fails.
    /// * [`DbError::CorruptedDbState`] if the data in the DB is corrupted.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn commit_outputs(
        &self,
        job_id: JobId,
        job_outputs: Vec<TaskOutput>,
        has_commit_task: bool,
    ) -> Result<(), DbError>;

    /// Cancels the job.
    ///
    /// The job enters the state:
    ///
    /// * [`JobState::CleanupReady`] if the job has a cleanup task.
    /// * [`JobState::Cancelled`] otherwise.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    /// * `has_cleanup_task` - Whether the job has cleanup task.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::InvalidJobStateTransition`] if the job is not in a cancellable state.
    /// * [`DbError::CorruptedDbState`] if the data in the DB is corrupted.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn cancel(&self, job_id: JobId, has_cleanup_task: bool) -> Result<(), DbError>;

    /// Fails job execution.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    /// * `error_message` - The error message explaining the failure.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::InvalidJobStateTransition`] if the job is already in a terminal state.
    /// * [`DbError::CorruptedDbState`] if the data in the DB is corrupted.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn fail(&self, job_id: JobId, error_message: String) -> Result<(), DbError>;

    /// Deletes all expired terminated jobs.
    ///
    /// # Parameters
    ///
    /// * `expire_after_sec` - The duration after termination which a job is considered expired, in
    ///   seconds.
    ///
    /// # Returns
    ///
    /// The IDs of the deleted jobs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::CorruptedDbState`] if the data in the DB is corrupted.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn delete_expired_terminated_jobs(
        &self,
        expire_after_sec: u64,
    ) -> Result<Vec<JobId>, DbError>;
}

/// Defines the storage interface for resource group management in the database.
#[async_trait]
pub trait ResourceGroupManagement {
    /// Adds a resource group to the database.
    ///
    /// # Parameters
    ///
    /// * `external_resource_group_id` - The ID of the external resource group to add.
    /// * `password` - The hashed password for the resource group.
    ///
    /// # Returns
    ///
    /// The ID of the created resource group on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::ResourceGroupAlreadyExists`] if the `external_resource_group_id` already
    ///   exists.
    /// * [`DbError::CorruptedDbState`] if the data in the DB is corrupted.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn add(
        &self,
        external_resource_group_id: String,
        password: Vec<u8>,
    ) -> Result<ResourceGroupId, DbError>;

    /// Verifies the password of a resource group.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The ID of the resource group to verify.
    /// * `password` - The hashed password to verify.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::ResourceGroupNotFound`] if the `resource_group_id` does not exist.
    /// * [`DbError::InvalidPassword`] if the password is incorrect.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn verify(
        &self,
        resource_group_id: ResourceGroupId,
        password: &[u8],
    ) -> Result<(), DbError>;

    /// Deletes a resource group from the database.
    ///
    /// This function deletes all jobs belonging to the resource group before deleting the resource
    /// group.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The ID of the resource group to delete.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::ResourceGroupNotFound`] if the `resource_group_id` does not exist.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn delete(&self, resource_group_id: ResourceGroupId) -> Result<(), DbError>;
}
