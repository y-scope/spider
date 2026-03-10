use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use const_format::formatcp;
use spider_core::{
    job::JobState,
    task::TaskGraph,
    types::{
        id::{JobId, ResourceGroupId},
        io::{TaskInput, TaskOutput},
    },
};
use sqlx::MySqlPool;

use super::sql_utils;
use crate::db::{DbError, DbStorage, ExternalJobStorage, InternalJobStorage, UserStorage};

const RESOURCE_GROUPS_TABLE_NAME: &str = "resource_groups";
const JOBS_TABLE_NAME: &str = "jobs";

#[must_use]
const fn resource_groups_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{RESOURCE_GROUPS_TABLE_NAME}` (
  id UUID NOT NULL,
  password VARCHAR(2048) NOT NULL,
  PRIMARY KEY (`id`)
);"
    )
}

#[must_use]
fn jobs_creation_query() -> String {
    format!(
        r"
CREATE TABLE IF NOT EXISTS `{JOBS_TABLE_NAME}` (
  id UUID NOT NULL DEFAULT UUID_v7(),
  resource_group_id UUID NOT NULL,
  state ENUM({state_enum}) NOT NULL DEFAULT 'Ready',
  serialized_task_graph LONGTEXT NOT NULL,
  serialized_job_inputs LONGTEXT NOT NULL,
  serialized_job_outputs LONGTEXT,
  PRIMARY KEY (`id`),
  CONSTRAINT `job_resource_group` FOREIGN KEY (`resource_group_id`)
    REFERENCES `{RESOURCE_GROUPS_TABLE_NAME}` (`id`)
);",
        state_enum = sql_utils::sql_enum_values::<JobState>()
    )
}

pub struct MariaDbStorage {
    pool: MySqlPool,
}

impl MariaDbStorage {
    #[must_use]
    pub const fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DbStorage for MariaDbStorage {
    async fn initialize(&self) -> Result<(), DbError> {
        sqlx::query(resource_groups_creation_query())
            .execute(&self.pool)
            .await?;

        sqlx::query(jobs_creation_query().as_str())
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
#[async_trait]
impl ExternalJobStorage for MariaDbStorage {
    async fn register_job(
        &self,
        _resource_group_id: ResourceGroupId,
        _task_graph: Arc<TaskGraph>,
        _job_inputs: Vec<TaskInput>,
    ) -> Result<JobId, DbError> {
        todo!()
    }

    async fn start_job(
        &self,
        _resource_group_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<(), DbError> {
        todo!()
    }

    async fn cancel_job(
        &self,
        _resource_group_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<(), DbError> {
        todo!()
    }

    async fn get_job_state(
        &self,
        _resource_group_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<JobState, DbError> {
        todo!()
    }

    async fn get_job_outputs(
        &self,
        _resource_group_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<Vec<TaskOutput>, DbError> {
        todo!()
    }

    async fn get_job_error(
        &self,
        _resource_group_id: ResourceGroupId,
        _job_id: JobId,
    ) -> Result<String, DbError> {
        todo!()
    }
}

#[async_trait]
impl InternalJobStorage for MariaDbStorage {
    async fn set_job_state(
        &self,
        _job_id: JobId,
        _old_state: Option<&[JobState]>,
        _new_state: JobState,
    ) -> Result<(), DbError> {
        todo!()
    }

    async fn delete_jobs(&self, _timeout: Duration) -> Result<Vec<JobId>, DbError> {
        todo!()
    }

    async fn reset_jobs(&self) -> Result<Vec<JobId>, DbError> {
        todo!()
    }
}

#[async_trait]
impl UserStorage for MariaDbStorage {
    async fn add_resource_group(
        &self,
        _resource_group_id: ResourceGroupId,
        _password: String,
    ) -> Result<(), DbError> {
        todo!()
    }

    async fn verify_resource_group(
        &self,
        _resource_group_id: ResourceGroupId,
        _password: String,
    ) -> Result<(), DbError> {
        todo!()
    }

    async fn delete_resource_group(
        &self,
        _resource_group_id: ResourceGroupId,
    ) -> Result<(), DbError> {
        todo!()
    }
}
