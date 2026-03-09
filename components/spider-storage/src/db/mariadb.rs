use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use spider_core::{
    job::JobState,
    task::TaskGraph,
    types::{
        id::{JobId, ResourceGroupId},
        io::{TaskInput, TaskOutput},
    },
};
use sqlx::MySqlPool;

use crate::db::{DbError, DbStorage, ExternalJobStorage, InternalJobStorage, UserStorage};

pub struct MariaDbStorage {
    pool: MySqlPool,
}

impl MariaDbStorage {
    #[must_use] 
    pub const fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }
}

const TABLE_CREATION_QUERIES: &[&str] = &[
    r"
CREATE TABLE IF NOT EXISTS `resource_groups` (
  id UUID NOT NULL,
  password VARCHAR(2048) NOT NULL,
  PRIMARY KEY (`id`)
)
    ",
    r"
CREATE TABLE IF NOT EXISTS `jobs` (
  id UUID NOT NULL DEFAULT UUIV_v7 (),
  resource_group_id UUID NOT NULL,
  serailized_task_graph LONGTEXT NOT NULL,
  serialized_job_inputs LONGTEXT NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `job_resource_group` FOREIGN KEY (`resource_group_id`)
    REFERENCES (`resource_groups`.`id`)
)
    ",
];

#[async_trait]
impl DbStorage for MariaDbStorage {
    async fn initialize(&self) -> Result<(), DbError> {
        for table_creation_query in TABLE_CREATION_QUERIES {
            sqlx::query(table_creation_query)
                .execute(&self.pool)
                .await?;
        }
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
