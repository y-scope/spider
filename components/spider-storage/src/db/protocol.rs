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

#[async_trait]
pub trait DbStorage {}

/// Defines the user-facing storage interface for job storage in database.
#[async_trait]
pub trait ExternalJobStorage {
    /// Stores a job into the database.
    /// If the `resource_group_id` does not exist in the database, a new resource group will be
    /// registered.
    ///
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
    /// * Forwards a [`sqlx::error::Error`] if database operation fails.
    ///
    /// # Note
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
    /// * `resource_group_id` - The owner of the job.
    /// * `job_id` - The ID of the job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::InvalidAccess`] if the `resource_group_id` does not exist or do not have access
    ///   to the job.
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::WrongJobState`] if the job is not in [`JobState::Ready`] state.
    /// * Forwards a [`sqlx::error::Error`] if database operation fails.
    async fn start_job(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), DbError>;

    /// Cancels a job. The cancelled job will move to
    /// * [`JobState::Cleanup`] if the job has a `cleanup` function.
    /// * [`JobState::Cancelled`] otherwise.
    ///
    /// # Parameters
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
    /// * [`DbError::WrongJobState`] if the job is in one of terminal states:
    ///   * ['JobState::Succeeded']
    ///   * ['JobState::Failed']
    ///   * ['JobState::Cancelled']
    /// * Forwards a [`sqlx::error::Error`] if database operation fails.
    async fn cancel_job(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), DbError>;

    /// Gets the state of a job.
    ///
    /// # Parameters
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
    /// * Forwards a [`sqlx::error::Error`] if database operation fails.
    async fn get_job_state(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<JobState, DbError>;

    /// Gets the outputs of a job.
    ///
    /// # Parameters
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
    /// * [`DbError::WrongJobState`] if the job is not in [`JobState::Succeeded`] state.
    /// * Forwards a [`sqlx::error::Error`] if database operation fails.
    async fn get_job_outputs(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<Vec<TaskOutput>, DbError>;

    /// Gets the error message of a job.
    ///
    /// # Parameters
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
    /// * [`DbError::WrongJobState`] if the job is not in [`JobState::Failed`] state.
    /// * Forwards a [`sqlx::error::Error`] if database operation fails.
    async fn get_job_error(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<String, DbError>;
}

/// Defines the internal storage interface for job storage in database.
#[async_trait]
pub trait InternalJobStorage {
    /// Sets the state of a job.
    ///
    /// # Parameters
    /// * `job_id` - The ID of the job.
    /// * `old_state` - The expected old state of the job. If `None`, the state will be updated
    ///   regardless of the current state.
    /// * `new_state` - The new state to set for the job.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * [`DbError::JobNotFound`] if the `job_id` does not exist.
    /// * [`DbError::WrongJobState`] if the current state of the job does not match any of the
    ///   states in `old_state` (if `old_state` is not `None`).
    /// * Forwards a [`sqlx::error::Error`] if database operation fails.
    async fn set_job_state(
        &self,
        job_id: JobId,
        old_state: Option<Vec<JobState>>,
        new_state: JobState,
    ) -> Result<(), DbError>;

    /// Deletes jobs that are in terminal states (i.e., [`JobState::Succeeded`],
    /// [`JobState::Failed`], or [`JobState::Cancelled`]) for a certain duration.
    ///
    /// # Parameters
    /// * `timeout` - The duration after which jobs in terminal states should be deleted.
    ///
    /// # Returns
    ///
    /// The IDs of the deleted jobs on success.
    ///
    /// # Errors
    ///
    /// * Forwards a [`sqlx::error::Error`] if database operation fails.
    async fn delete_jobs(&self, timeout: std::time::Duration) -> Result<Vec<JobId>, DbError>;

    /// Resets all started jobs that are in non-terminal states.
    ///
    /// # Returns
    ///
    /// The IDs of the reset jobs on success.
    ///
    /// # Errors
    /// * Forwards a [`sqlx::error::Error`] if database operation fails.
    async fn reset_jobs(&self) -> Result<Vec<JobId>, DbError>;
}
