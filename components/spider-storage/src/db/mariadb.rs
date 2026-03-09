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

#[async_trait]
impl ExternalJobStorage for MariaDbStorage {
    async fn register_job(
        &self,
        resource_group_id: ResourceGroupId,
        task_graph: Arc<TaskGraph>,
        job_inputs: Vec<TaskInput>,
    ) -> Result<JobId, DbError> {
        todo!()
    }

    async fn start_job(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), DbError> {
        todo!()
    }

    async fn cancel_job(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<(), DbError> {
        todo!()
    }

    async fn get_job_state(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<JobState, DbError> {
        todo!()
    }

    async fn get_job_outputs(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<Vec<TaskOutput>, DbError> {
        todo!()
    }

    async fn get_job_error(
        &self,
        resource_group_id: ResourceGroupId,
        job_id: JobId,
    ) -> Result<String, DbError> {
        todo!()
    }
}

#[async_trait]
impl InternalJobStorage for MariaDbStorage {
    async fn set_job_state(
        &self,
        job_id: JobId,
        old_state: Option<&[JobState]>,
        new_state: JobState,
    ) -> Result<(), DbError> {
        todo!()
    }

    async fn delete_jobs(&self, timeout: Duration) -> Result<Vec<JobId>, DbError> {
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
        resource_group_id: ResourceGroupId,
        password: String,
    ) -> Result<(), DbError> {
        todo!()
    }

    async fn verify_resource_group(
        &self,
        resource_group_id: ResourceGroupId,
        password: String,
    ) -> Result<(), DbError> {
        todo!()
    }

    async fn delete_resource_group(
        &self,
        resource_group_id: ResourceGroupId,
    ) -> Result<(), DbError> {
        todo!()
    }
}

#[async_trait]
impl DbStorage for MariaDbStorage {}
