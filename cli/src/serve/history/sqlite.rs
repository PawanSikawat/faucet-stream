//! SQLite-backed run history (`serve-history-sqlite`, Phase 5 of #127).
//! Connection setup only — the schema, statements, and `RunHistory` impl are
//! shared with Postgres via [`impl_sql_history!`](super::sql).

use super::HistoryError;
use super::sql::{DDL, Dialect, Stmts, impl_sql_history};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::str::FromStr;
use std::time::Duration;

impl_sql_history!(SqliteHistory, sqlx::SqlitePool);

impl SqliteHistory {
    /// Connect (creating the database file if missing), create the schema if
    /// absent, and return the backend. WAL + a busy timeout let the connection
    /// pool tolerate concurrent run writes. `lease_ttl` and `instance_id` drive
    /// instance-fenced orphan recovery (#146 H7).
    pub async fn connect(
        url: &str,
        idem_retention: Duration,
        lease_ttl: Duration,
        instance_id: String,
    ) -> Result<Self, HistoryError> {
        let opts = SqliteConnectOptions::from_str(url)
            .map_err(|e| HistoryError::Backend(format!("invalid sqlite url '{url}': {e}")))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await
            .map_err(|e| HistoryError::Backend(format!("SQLite connection failed: {e}")))?;
        for stmt in DDL {
            sqlx::query(stmt)
                .execute(&pool)
                .await
                .map_err(|e| HistoryError::Backend(format!("creating run-history schema: {e}")))?;
        }
        Ok(Self::from_parts(
            pool,
            idem_retention,
            lease_ttl,
            instance_id,
            Stmts::new(Dialect::Sqlite),
        ))
    }
}
