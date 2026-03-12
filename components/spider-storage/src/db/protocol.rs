use std::sync::Arc;

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
/// * [`ResourceGroupStorage`]
#[async_trait]
pub trait DbStorage:
    ExternalJobOrchestration + InternalJobOrchestration + ResourceGroupStorage {
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
    /// * `job_inputs` - A vector of job inputs required for the job.
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
    /// * [`DbError::DataIntegrity`] if serialization of the task graph or job inputs fails.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    ///
    /// # Note
    ///
    /// This function assumes that the `task_graph` and `job_inputs` are consistent.
    async fn register_job(
        &self,
        resource_group_id: ResourceGroupId,
        task_graph: Arc<TaskGraph>,
        job_inputs: Vec<TaskInput>,
    ) -> Result<JobId, DbError>;

    /// Starts a job.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The owner of the job.
    /// * `job_id` - The ID of the job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::InvalidAccess`] if the `resource_group_id` does not exist or does not have
    ///   access to the job.
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::UnexpectedJobState`] if the job is not in [`JobState::Ready`] state.
    /// * [`DbError::DataIntegrity`] if the data in the database is invalid.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn start_job(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), DbError>;

    /// Cancels a job.
    ///
    /// The cancelled job will move to:
    /// * [`JobState::CleanupReady`] if the job has a `cleanup` function.
    /// * [`JobState::Cancelled`] otherwise.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The owner of the job.
    /// * `job_id` - The ID of the job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::InvalidAccess`] if the `resource_group_id` does not exist or cannot cancel the
    ///   job.
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::UnexpectedJobState`] if the job is in a terminal state.
    /// * [`DbError::DataIntegrity`] if the data in the database is invalid.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn cancel_job(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), DbError>;

    /// Gets the state of a job.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The owner of the job.
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
    /// * [`DbError::InvalidAccess`] if the `resource_group_id` does not exist or does not have
    ///   access to the job.
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::DataIntegrity`] if the data in the database is invalid.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn get_job_state(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<JobState, DbError>;

    /// Gets the outputs of a job.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The owner of the job.
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
    /// * [`DbError::InvalidAccess`] if the `resource_group_id` does not exist or does not have
    ///   access to the job.
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::UnexpectedJobState`] if the job is not in [`JobState::Succeeded`] state.
    /// * [`DbError::DataIntegrity`] if the data in the database is invalid.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn get_job_outputs(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<Vec<TaskOutput>, DbError>;

    /// Gets the error message of a job.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The owner of the job.
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
    /// * [`DbError::InvalidAccess`] if the `resource_group_id` does not exist or does not have
    ///   access to the job.
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::UnexpectedJobState`] if the job is not in [`JobState::Failed`] state.
    /// * [`DbError::DataIntegrity`] if the data in the database is invalid.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn get_job_error(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<String, DbError>;
}

/// Defines the internal storage interface for job storage in database.
#[async_trait]
pub trait InternalJobOrchestration {
    /// Sets the state of a job.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The ID of the job.
    /// * `old_state` - The expected old state of the job. If `None`, the state will be updated
    ///   regardless of the current state.
    /// * `new_state` - The new state to set for the job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::InvalidJobStateTransition`] if the current state of the job does not match any
    ///   of the states in `old_state` (if `old_state` is not `None`).
    /// * [`DbError::DataIntegrity`] if the data in the database is invalid.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn set_job_state(
        &self,
        job_id: JobId,
        old_state: Option<&[JobState]>,
        new_state: JobState,
    ) -> Result<(), DbError>;

    /// Deletes jobs that are in terminal states for a certain duration.
    ///
    /// # Parameters
    ///
    /// * `timeout` - The duration after which jobs in terminal states should be deleted.
    ///
    /// # Returns
    ///
    /// The IDs of the deleted jobs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::DataIntegrity`] if the data in the database is invalid.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn delete_jobs(&self, timeout: std::time::Duration) -> Result<Vec<JobId>, DbError>;

    /// Resets all started jobs that are in non-terminal states.
    ///
    /// # Returns
    ///
    /// The IDs of the reset jobs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::DataIntegrity`] if the data in the database is invalid.
    /// * Forwards a [`sqlx::error::Error`] if database operation fails.
    async fn reset_jobs(&self) -> Result<Vec<JobId>, DbError>;
}

/// Defines the storage interface for resource group management in database.
#[async_trait]
pub trait ResourceGroupStorage {
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
    /// * [`DbError::DataIntegrity`] if the data in the database is invalid.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn add_resource_group(
        &self,
        external_resource_group_id: String,
        password: String,
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
    async fn verify_resource_group(
        &self,
        resource_group_id: ResourceGroupId,
        password: String,
    ) -> Result<(), DbError>;

    /// Deletes a resource group from the database.
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
    async fn delete_resource_group(
        &self,
        resource_group_id: ResourceGroupId,
    ) -> Result<(), DbError>;
}
