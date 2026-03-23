use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use const_format::formatcp;
use secrecy::ExposeSecret;
use spider_core::{
    job::JobState,
    task::TaskGraph,
    types::{
        id::{JobId, ResourceGroupId},
        io::{TaskInput, TaskOutput},
    },
};
use sqlx::MySqlPool;

use crate::{
    config::DatabaseConfig,
    db::{DbError, ExternalJobOrchestration, InternalJobOrchestration, ResourceGroupManagement},
};

#[derive(Clone)]
pub struct MariaDbStorageConnector {
    pool: MySqlPool,
}

impl MariaDbStorageConnector {
    /// Connects to database and initializes tables.
    ///
    /// # Parameters
    ///
    /// * `config` - Database configuration parameters.
    ///
    /// # Returns
    ///
    /// A new instance of `MariaDbStorageConnector` if connection and initialization succeed.
    ///
    /// # Errors
    ///
    /// Returns an error if
    ///
    /// * Forwards a [`sqlx::error::Error`] if database operation fails.
    pub async fn connect_and_initialize(config: &DatabaseConfig) -> Result<Self, DbError> {
        let mysql_options = sqlx::mysql::MySqlConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .database(&config.name)
            .username(&config.username)
            .password(config.password.expose_secret());

        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(mysql_options)
            .await?;

        let connector = Self { pool };
        connector.initialize().await?;
        Ok(connector)
    }

    /// Initializes the database by creating necessary tables if they do not exist.
    ///
    /// # NOTE
    ///
    /// `MariaDB` does not support transactions for DDL statements. All DDL statements are
    /// automatically committed. Thus, this function executes each table creation query separately,
    /// and does not provide atomicity guarantees.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards a [`sqlx::query::Query::execute`]'s return values on failure.
    async fn initialize(&self) -> Result<(), DbError> {
        sqlx::query(resource_groups_creation_query())
            .execute(&self.pool)
            .await?;

        sqlx::query(jobs_creation_query())
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
#[async_trait]
impl ExternalJobOrchestration for MariaDbStorageConnector {
    async fn register(
        &self,
        _resource_group_id: ResourceGroupId,
        _task_graph: Arc<TaskGraph>,
        _job_inputs: Vec<TaskInput>,
    ) -> Result<JobId, DbError> {
        todo!()
    }

    async fn start(
        &self,
        _resource_group_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<(), DbError> {
        todo!()
    }

    async fn cancel(
        &self,
        _resource_group_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<(), DbError> {
        todo!()
    }

    async fn get_state(
        &self,
        _resource_group_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<JobState, DbError> {
        todo!()
    }

    async fn get_outputs(
        &self,
        _resource_group_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<Vec<TaskOutput>, DbError> {
        todo!()
    }

    async fn get_error(
        &self,
        _resource_group_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<String, DbError> {
        todo!()
    }
}

#[async_trait]
impl InternalJobOrchestration for MariaDbStorageConnector {
    async fn set_state(&self, _job_id: JobId, _state: JobState) -> Result<(), DbError> {
        todo!()
    }

    async fn commit_outputs(
        &self,
        _job_id: JobId,
        _job_outputs: Vec<TaskOutput>,
    ) -> Result<JobState, DbError> {
        todo!()
    }

    async fn cancel(&self, _job_id: JobId) -> Result<JobState, DbError> {
        todo!()
    }

    async fn fail(&self, _job_id: JobId, _error_message: String) -> Result<(), DbError> {
        todo!()
    }

    async fn delete_expired_terminated_jobs(
        &self,
        _expire_after: Duration,
    ) -> Result<Vec<JobId>, DbError> {
        todo!()
    }
}

#[async_trait]
impl ResourceGroupManagement for MariaDbStorageConnector {
    async fn add(
        &self,
        _external_resource_group_id: String,
        _password: String,
    ) -> Result<ResourceGroupId, DbError> {
        todo!()
    }

    async fn verify(
        &self,
        _resource_group_id: ResourceGroupId,
        _password: String,
    ) -> Result<(), DbError> {
        todo!()
    }

    async fn delete(&self, _resource_group_id: ResourceGroupId) -> Result<(), DbError> {
        todo!()
    }
}

const RESOURCE_GROUPS_TABLE_NAME: &str = "resource_groups";
const JOBS_TABLE_NAME: &str = "jobs";

#[must_use]
const fn resource_groups_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{RESOURCE_GROUPS_TABLE_NAME}` (
  `id` UUID NOT NULL DEFAULT UUID_v7(),
  `external_id` VARCHAR(256) NOT NULL,
  `password` VARCHAR(2048) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `external_resource_group_id` (`external_id`)
);"
    )
}

#[must_use]
const fn jobs_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{JOBS_TABLE_NAME}` (
  `id` UUID NOT NULL DEFAULT UUID_v7(),
  `resource_group_id` UUID NOT NULL,
  `state` {state_enum} NOT NULL DEFAULT {default_state},
  `serialized_task_graph` LONGTEXT NOT NULL,
  `serialized_job_inputs` LONGTEXT NOT NULL,
  `serialized_job_outputs` LONGTEXT,
  `error_message` LONGTEXT,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ended_at` TIMESTAMP,
  `max_num_retries` INT UNSIGNED NOT NULL DEFAULT 0,
  `num_retries` INT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  CONSTRAINT `job_resource_group` FOREIGN KEY (`resource_group_id`)
    REFERENCES `{RESOURCE_GROUPS_TABLE_NAME}` (`id`)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);",
        state_enum = JobState::as_mysql_enum_decl(),
        default_state = JobState::Ready.as_quoted_str(),
    )
}
