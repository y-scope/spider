use sqlx::mysql::MySqlDatabaseError;

/// `MySQL` error number for foreign key constraint violation.
pub(super) const MYSQL_ER_FK_CONSTRAINT: u16 = 1452;

/// `MySQL` error number for duplicate entry.
const MYSQL_ER_DUP_ENTRY: u16 = 1062;

/// `MySQL` error number for a lock wait timeout.
const MYSQL_ER_LOCK_WAIT_TIMEOUT: u16 = 1205;

/// `MySQL` error number for a missing privilege on an operation.
const MYSQL_ER_SPECIFIC_ACCESS_DENIED: u16 = 1227;

/// `MySQL` error number for a missing privilege on a table.
const MYSQL_ER_TABLE_ACCESS_DENIED: u16 = 1142;

/// `MySQL` error number for a `NULL` written to a `NOT NULL` column.
pub(super) const MYSQL_ER_BAD_NULL_ERROR: u16 = 1048;

/// # Returns
///
/// Whether `error` is a duplicate-entry error reported by the database.
pub(super) fn is_duplicate_entry(error: &sqlx::Error) -> bool {
    has_mysql_error_number(error, &[MYSQL_ER_DUP_ENTRY])
}

/// # Returns
///
/// Whether `error` is a lock wait timeout reported by the database.
pub(super) fn is_lock_wait_timeout(error: &sqlx::Error) -> bool {
    has_mysql_error_number(error, &[MYSQL_ER_LOCK_WAIT_TIMEOUT])
}

/// # Returns
///
/// Whether `error` is a missing-privilege error reported by the database.
pub(super) fn is_access_denied(error: &sqlx::Error) -> bool {
    has_mysql_error_number(
        error,
        &[
            MYSQL_ER_SPECIFIC_ACCESS_DENIED,
            MYSQL_ER_TABLE_ACCESS_DENIED,
        ],
    )
}

/// # Returns
///
/// Whether `error` is a database error whose `MySQL` error number is one of `numbers`.
pub(super) fn has_mysql_error_number(error: &sqlx::Error, numbers: &[u16]) -> bool {
    let sqlx::Error::Database(error) = error else {
        return false;
    };
    error
        .try_downcast_ref::<MySqlDatabaseError>()
        .is_some_and(|mysql_error| numbers.contains(&mysql_error.number()))
}
