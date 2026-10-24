use async_trait::async_trait;
use const_format::formatcp;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::resource_group::ExternalResourceGroupCredentials;
use sqlx::Connection;
use subtle::ConstantTimeEq;

use crate::db::DbError;
use crate::db::ResourceGroupManagement;
use crate::db::mariadb::MariaDbStorageConnector;
use crate::db::mariadb::mysql_error::is_duplicate_entry;
use crate::db::mariadb::run_read_committed_tx;
use crate::db::mariadb::schema::RESOURCE_GROUPS_TABLE_NAME;

#[async_trait]
impl ResourceGroupManagement for MariaDbStorageConnector {
    async fn add(
        &self,
        credentials: ExternalResourceGroupCredentials,
    ) -> Result<ResourceGroupId, DbError> {
        const QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`external_id`, `password`) VALUES (?, ?);",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );

        let resource_group_id = sqlx::query(QUERY)
            .bind(credentials.get_external_resource_group_id())
            .bind(credentials.get_password())
            .execute(&self.pool)
            .await
            .map_err(|e| match e {
                e if is_duplicate_entry(&e) => DbError::ResourceGroupAlreadyExists(
                    credentials.get_external_resource_group_id().to_owned(),
                ),
                e => e.into(),
            })?
            .last_insert_id();

        Ok(ResourceGroupId::from(resource_group_id))
    }

    async fn add_or_verify(
        &self,
        credentials: ExternalResourceGroupCredentials,
    ) -> Result<ResourceGroupId, DbError> {
        const INSERT_QUERY: &str = formatcp!(
            "INSERT INTO `{table}` (`external_id`, `password`) VALUES (?, ?);",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );
        const SELECT_QUERY: &str = formatcp!(
            "SELECT `id`, `password` FROM `{table}` WHERE `external_id` = ? LOCK IN SHARE MODE;",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );

        run_read_committed_tx(self.pool.clone(), async move |connection| {
            let mut tx = connection.begin().await?;
            if let Some((resource_group_id, stored_password)) =
                sqlx::query_as::<_, (ResourceGroupId, Vec<u8>)>(SELECT_QUERY)
                    .bind(credentials.get_external_resource_group_id())
                    .fetch_optional(&mut *tx)
                    .await?
            {
                if !bool::from(stored_password.ct_eq(credentials.get_password())) {
                    return Err(DbError::InvalidPassword(resource_group_id));
                }
                tx.commit().await?;
                return Ok(resource_group_id);
            }

            let inserted_resource_group_id = match sqlx::query(INSERT_QUERY)
                .bind(credentials.get_external_resource_group_id())
                .bind(credentials.get_password())
                .execute(&mut *tx)
                .await
            {
                Ok(result) => Some(ResourceGroupId::from(result.last_insert_id())),
                Err(error) if is_duplicate_entry(&error) => None,
                Err(error) => return Err(error.into()),
            };

            if let Some(resource_group_id) = inserted_resource_group_id {
                tx.commit().await?;
                return Ok(resource_group_id);
            }

            let (resource_group_id, stored_password) =
                sqlx::query_as::<_, (ResourceGroupId, Vec<u8>)>(SELECT_QUERY)
                    .bind(credentials.get_external_resource_group_id())
                    .fetch_one(&mut *tx)
                    .await?;
            if !bool::from(stored_password.ct_eq(credentials.get_password())) {
                return Err(DbError::InvalidPassword(resource_group_id));
            }
            tx.commit().await?;
            Ok(resource_group_id)
        })
        .await
    }

    async fn verify(
        &self,
        resource_group_id: ResourceGroupId,
        password: &[u8],
    ) -> Result<(), DbError> {
        const QUERY: &str = formatcp!(
            "SELECT `password` FROM `{table}` WHERE `id` = ?;",
            table = RESOURCE_GROUPS_TABLE_NAME,
        );

        let Some(stored_password) = sqlx::query_scalar::<_, Vec<u8>>(QUERY)
            .bind(resource_group_id)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Err(DbError::ResourceGroupNotFound(resource_group_id));
        };

        if stored_password.ct_eq(password).into() {
            Ok(())
        } else {
            Err(DbError::InvalidPassword(resource_group_id))
        }
    }

    async fn delete(&self, _resource_group_id: ResourceGroupId) -> Result<(), DbError> {
        todo!("not implemented")
    }
}
