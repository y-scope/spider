use std::{fmt::Display, str::FromStr};

use spider_core::{job::JobState, types::id::ResourceGroupId};
use strum::IntoEnumIterator;

use crate::db::DbError;

/// Returns the inner part of a SQL `ENUM(...)` definition from a Rust enum,
/// e.g. `'Ready','Running','CommitReady'`.
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
/// * Forwards a [`sqlx::error::Error`] if the UUID column is invalid.
pub fn validate_resource_group_access(
    rg_id_str: &str,
    expected: ResourceGroupId,
) -> Result<(), DbError> {
    let actual: ResourceGroupId = rg_id_str
        .parse()
        .map_err(|e: uuid::Error| sqlx::Error::Protocol(e.to_string()))?;
    if actual != expected {
        return Err(DbError::InvalidAccess(expected));
    }
    Ok(())
}

/// Parses a [`JobState`] from a database column string.
///
/// # Errors
///
/// * Forwards a [`sqlx::error::Error`] if the state string is not a valid [`JobState`] variant.
pub fn parse_job_state(state_str: &str) -> Result<JobState, sqlx::Error> {
    JobState::from_str(state_str).map_err(|e| sqlx::Error::Protocol(e.to_string()))
}
