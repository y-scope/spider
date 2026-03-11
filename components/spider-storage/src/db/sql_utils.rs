use std::{fmt::Display, str::FromStr};

use spider_core::{job::JobState, types::id::ResourceGroupId};
use strum::IntoEnumIterator;

use crate::db::DbError;

/// Returns the inner part of a SQL `ENUM(...)` definition from a Rust enum,
/// e.g. `'Ready','Running','CommitReady'`.
#[must_use]
pub fn sql_enum_values<T: IntoEnumIterator + Display>() -> String {
    T::iter()
        .map(|s| format!("'{s}'"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Returns a comma-separated list of SQL-quoted strings for use in `IN (...)` clauses,
/// e.g. `'Succeeded','Failed','Cancelled'`.
pub fn sql_quoted_list<T: Display>(values: &[T]) -> String {
    values
        .iter()
        .map(|s| format!("'{s}'"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Validates that the `resource_group_id` column from a job row matches the expected value.
///
/// # Errors
///
/// * [`DbError::InvalidAccess`] if the resource group IDs do not match.
/// * [`DbError::DataIntegrity`] if the UUID column is invalid.
pub fn validate_resource_group_access(
    rg_id_str: &str,
    expected: ResourceGroupId,
) -> Result<(), DbError> {
    let actual: ResourceGroupId = rg_id_str.parse().map_err(|e: uuid::Error| {
        DbError::DataIntegrity(format!("invalid resource group UUID: {e}"))
    })?;
    if actual != expected {
        return Err(DbError::InvalidAccess(expected));
    }
    Ok(())
}

/// Parses a [`JobState`] from a database column string.
///
/// # Errors
///
/// * [`DbError::DataIntegrity`] if the state string is not a valid [`JobState`] variant.
pub fn parse_job_state(state_str: &str) -> Result<JobState, DbError> {
    JobState::from_str(state_str)
        .map_err(|e| DbError::DataIntegrity(format!("invalid job state: {e}")))
}

/// Converts a JDBC `MariaDB` URL to a sqlx-compatible `MySQL` URL.
///
/// Example:
/// ```text
/// jdbc:mariadb://127.0.0.1:3306/spider-db?user=spider-user&password=spider-password
/// ```
/// becomes:
/// ```text
/// mysql://spider-user:spider-password@127.0.0.1:3306/spider-db
/// ```
///
/// # Errors
///
/// Returns [`DbError::DataIntegrity`] if the URL is malformed or missing required parameters.
pub fn jdbc_url_to_sqlx(jdbc_url: &str) -> Result<String, DbError> {
    let rest = jdbc_url.strip_prefix("jdbc:mariadb://").ok_or_else(|| {
        DbError::DataIntegrity("JDBC URL must start with 'jdbc:mariadb://'".to_string())
    })?;

    let (host_port_db, query) = rest.split_once('?').ok_or_else(|| {
        DbError::DataIntegrity("JDBC URL must contain query parameters after '?'".to_string())
    })?;

    let params: Vec<(&str, &str)> = query.split('&').filter_map(|p| p.split_once('=')).collect();

    let user = params
        .iter()
        .find(|(k, _)| *k == "user")
        .map(|(_, v)| *v)
        .ok_or_else(|| DbError::DataIntegrity("JDBC URL missing 'user' parameter".to_string()))?;

    let password = params
        .iter()
        .find(|(k, _)| *k == "password")
        .map(|(_, v)| *v)
        .ok_or_else(|| {
            DbError::DataIntegrity("JDBC URL missing 'password' parameter".to_string())
        })?;

    Ok(format!("mysql://{user}:{password}@{host_port_db}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jdbc_url_to_sqlx() {
        let jdbc =
            "jdbc:mariadb://127.0.0.1:3306/spider-db?user=spider-user&password=spider-password";
        let result = jdbc_url_to_sqlx(jdbc).unwrap();
        assert_eq!(
            result,
            "mysql://spider-user:spider-password@127.0.0.1:3306/spider-db"
        );
    }

    #[test]
    fn test_jdbc_url_to_sqlx_missing_prefix() {
        let jdbc = "mysql://127.0.0.1:3306/spider-db?user=u&password=p";
        assert!(jdbc_url_to_sqlx(jdbc).is_err());
    }

    #[test]
    fn test_jdbc_url_to_sqlx_missing_user() {
        let jdbc = "jdbc:mariadb://127.0.0.1:3306/spider-db?password=p";
        assert!(jdbc_url_to_sqlx(jdbc).is_err());
    }

    #[test]
    fn test_jdbc_url_to_sqlx_missing_password() {
        let jdbc = "jdbc:mariadb://127.0.0.1:3306/spider-db?user=u";
        assert!(jdbc_url_to_sqlx(jdbc).is_err());
    }

    #[test]
    fn test_jdbc_url_to_sqlx_missing_query() {
        let jdbc = "jdbc:mariadb://127.0.0.1:3306/spider-db";
        assert!(jdbc_url_to_sqlx(jdbc).is_err());
    }
}
