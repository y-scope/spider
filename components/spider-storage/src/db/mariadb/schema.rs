use const_format::formatcp;
use spider_core::job::JobState;
use sqlx::MySqlPool;

use crate::db::DbError;
use crate::db::mariadb::execution_manager::ExecutionManagerState;

pub(super) const RESOURCE_GROUPS_TABLE_NAME: &str = "resource_groups";
pub(super) const JOBS_TABLE_NAME: &str = "jobs";
pub(super) const EXECUTION_MANAGERS_TABLE_NAME: &str = "execution_managers";
pub(super) const SCHEDULERS_TABLE_NAME: &str = "schedulers";
pub(super) const SESSIONS_TABLE_NAME: &str = "sessions";
pub(super) const STORAGE_FENCE_TABLE_NAME: &str = "storage_fence";

/// Creates every storage table that does not exist yet.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
pub(super) async fn create_tables(pool: &MySqlPool) -> Result<(), DbError> {
    // MariaDB does not support transactions for DDL statements. All DDL statements are
    // automatically committed. Thus, each table creation query is executed separately, and
    // atomicity is not guaranteed.
    sqlx::query(resource_groups_creation_query())
        .execute(pool)
        .await?;
    sqlx::query(jobs_creation_query()).execute(pool).await?;
    sqlx::query(sessions_creation_query()).execute(pool).await?;
    sqlx::query(execution_managers_creation_query())
        .execute(pool)
        .await?;
    sqlx::query(schedulers_creation_query())
        .execute(pool)
        .await?;
    sqlx::query(storage_fence_creation_query())
        .execute(pool)
        .await?;
    Ok(())
}

#[must_use]
const fn resource_groups_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{RESOURCE_GROUPS_TABLE_NAME}` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `external_id` VARCHAR(256) NOT NULL,
  `password` VARBINARY(2048) NOT NULL,
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
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `resource_group_id` BIGINT UNSIGNED NOT NULL,
  `state` {state_enum} NOT NULL DEFAULT {default_state},
  `compressed_serialized_task_graph` LONGBLOB NOT NULL,
  `compressed_serialized_job_inputs` LONGBLOB NOT NULL,
  `serialized_job_outputs` LONGBLOB,
  `error_message` LONGTEXT,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ended_at` TIMESTAMP,
  `max_num_retries` INT UNSIGNED NOT NULL DEFAULT 0,
  `num_retries` INT UNSIGNED NOT NULL DEFAULT 0,
  `parent_id` BIGINT UNSIGNED DEFAULT NULL,
  `root_id` BIGINT UNSIGNED DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `job_state` (`state`),
  INDEX `job_parent` (`parent_id`),
  CONSTRAINT `job_resource_group` FOREIGN KEY (`resource_group_id`)
    REFERENCES `{RESOURCE_GROUPS_TABLE_NAME}` (`id`)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);",
        state_enum = JobState::as_mysql_enum_decl(),
        default_state = JobState::Ready.as_quoted_str(),
    )
}

#[must_use]
const fn execution_managers_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{EXECUTION_MANAGERS_TABLE_NAME}` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `ip_address` VARCHAR(45) NOT NULL,
  `resource_group_id` BIGINT UNSIGNED,
  `state` {state_enum} NOT NULL DEFAULT {default_state},
  `last_heartbeat_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `death_confirmed_at` TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `execution_manager_liveness` (`state`, `last_heartbeat_at`),
  CONSTRAINT `execution_manager_resource_group` FOREIGN KEY (`resource_group_id`)
    REFERENCES `{RESOURCE_GROUPS_TABLE_NAME}` (`id`)
    ON UPDATE RESTRICT ON DELETE RESTRICT
);",
        state_enum = ExecutionManagerState::as_mysql_enum_decl(),
        default_state = ExecutionManagerState::Alive.as_quoted_str(),
    )
}

#[must_use]
const fn schedulers_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{SCHEDULERS_TABLE_NAME}` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `host` VARCHAR(255) NOT NULL,
  `port` SMALLINT UNSIGNED NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
);"
    )
}

#[must_use]
const fn sessions_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{SESSIONS_TABLE_NAME}` (
  `session_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`session_id`)
);"
    )
}

#[must_use]
const fn storage_fence_creation_query() -> &'static str {
    formatcp!(
        r"
CREATE TABLE IF NOT EXISTS `{STORAGE_FENCE_TABLE_NAME}` (
  `id` BIGINT UNSIGNED NOT NULL,
  `session_id` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`)
);"
    )
}
