use std::net::IpAddr;

use async_trait::async_trait;
use spider_core::job::JobState;
use spider_core::types::id::ExecutionManagerId;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SchedulerId;
use spider_core::types::id::SessionId;
use spider_core::types::io::TaskOutput;
use spider_core::types::scheduler::RegisteredScheduler;

use crate::db::error::DbError;
use crate::job_submission::ValidatedJobSubmission;

/// A job persisted in the database that should be rebuilt in the storage cache on startup.
///
/// All non-terminal jobs are recoverable.
pub struct RecoverableJobContext {
    pub id: JobId,
    pub resource_group_id: ResourceGroupId,
    pub state: JobState,
    pub submission: ValidatedJobSubmission,
    pub outputs: Option<Vec<TaskOutput>>,
}

/// The database storage interface. A database storage must implement the following traits:
///
/// * [`ExternalJobOrchestration`]
/// * [`InternalJobOrchestration`]
/// * [`ResourceGroupManagement`]
/// * [`ExecutionManagerLivenessManagement`]
/// * [`SchedulerRegistrationManagement`]
/// * [`SessionManagement`]
#[async_trait]
pub trait DbStorage:
    ExternalJobOrchestration
    + InternalJobOrchestration
    + ResourceGroupManagement
    + ExecutionManagerLivenessManagement
    + SchedulerRegistrationManagement
    + SessionManagement {
}

/// Defines the user-facing storage interface for job storage in the database.
#[async_trait]
pub trait ExternalJobOrchestration {
    /// Registers a job in the database.
    ///
    /// # Parameters
    ///
    /// * `resource_group_id` - The owner of the created job.
    /// * `job_submission` - The validated job submission containing the task graph, job inputs, and
    ///   the compressed serializations to persist verbatim.
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
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn register(
        &self,
        resource_group_id: ResourceGroupId,
        job_submission: &ValidatedJobSubmission,
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

    /// Gets all jobs that should be recovered and cached in the storage service startup.
    ///
    /// # Returns
    ///
    /// All persisted jobs in [`JobState::Ready`], [`JobState::Running`],
    /// [`JobState::CommitReady`], or [`JobState::CleanupReady`] on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::TaskGraphDeserializationFailure`] if a persisted task graph is invalid.
    /// * [`DbError::ValueDeserializationFailure`] if persisted inputs or outputs are invalid.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn get_recoverable_jobs(&self) -> Result<Vec<RecoverableJobContext>, DbError>;
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

/// Defines the storage interface for execution manager liveness management in the database.
#[async_trait]
pub trait ExecutionManagerLivenessManagement: Clone + Send + Sync {
    /// Registers an execution manager in the database.
    ///
    /// # Parameters
    ///
    /// * `ip_address` - The execution manager IP address.
    ///
    /// # Returns
    ///
    /// The ID of the registered execution manager on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn register_execution_manager(
        &self,
        ip_address: IpAddr,
    ) -> Result<ExecutionManagerId, DbError>;

    /// Updates the heartbeat of an alive execution manager.
    ///
    /// # Parameters
    ///
    /// * `execution_manager_id` - The ID of the execution manager to update.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::IllegalExecutionManagerId`] if the execution manager ID is illegal.
    /// * [`DbError::ExecutionManagerAlreadyDead`] if the execution manager is dead.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn update_execution_manager_heartbeat(
        &self,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<(), DbError>;

    /// Checks whether the execution manager with the given ID is alive.
    ///
    /// # Parameters
    ///
    /// * `execution_manager_id` - The execution manager ID to check.
    ///
    /// # Returns
    ///
    /// Whether the execution manager is alive on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`DbError::IllegalExecutionManagerId`] if the execution manager ID is illegal.
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn is_execution_manager_alive(
        &self,
        execution_manager_id: ExecutionManagerId,
    ) -> Result<bool, DbError>;

    /// Marks stale execution managers dead and returns their IDs.
    ///
    /// This operation is atomic: once an execution manager is marked dead and returned by a call of
    /// this method, it will not be returned again in subsequent calls.
    ///
    /// # Parameters
    ///
    /// * `stale_after_sec` - The seconds after the last heartbeat which makes an execution manager
    ///   stale.
    ///
    /// # Returns
    ///
    /// A vector of dead execution manager IDs on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn get_dead_execution_managers(
        &self,
        stale_after_sec: u64,
    ) -> Result<Vec<ExecutionManagerId>, DbError>;
}

/// Defines the storage interface for scheduler registration in the database.
#[async_trait]
pub trait SchedulerRegistrationManagement: Clone + Send + Sync {
    /// Registers the scheduler in the database.
    ///
    /// For now, only one scheduler can be registered at a time. Registering a new scheduler removes
    /// any previously registered scheduler before allocating the new scheduler ID.
    ///
    /// # Parameters
    ///
    /// * `ip_address` - The scheduler IP address.
    /// * `port` - The scheduler port.
    ///
    /// # Returns
    ///
    /// The ID of the registered scheduler on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn register_scheduler(
        &self,
        ip_address: IpAddr,
        port: u16,
    ) -> Result<SchedulerId, DbError>;

    /// Gets registered schedulers.
    ///
    /// # Returns
    ///
    /// The registered schedulers on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn get_schedulers(&self) -> Result<Vec<RegisteredScheduler>, DbError>;

    /// Checks whether the scheduler with the given ID is registered.
    ///
    /// # Parameters
    ///
    /// * `scheduler_id` - The scheduler ID to check.
    ///
    /// # Returns
    ///
    /// Whether the scheduler is registered on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`sqlx::error::Error`] on DB operation failure.
    async fn is_scheduler_registered(&self, scheduler_id: SchedulerId) -> Result<bool, DbError>;
}

/// Defines the storage interface for session management.
///
/// A session ID is a monotonically increasing value that bumps each time the storage layer
/// reconnects. Callers can use it to detect and reject stale requests from previous sessions.
pub trait SessionManagement {
    /// # Returns
    ///
    /// The current session ID.
    fn session_id(&self) -> SessionId;
}
