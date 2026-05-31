//! Postgres-backed run history (`serve-history-postgres`, Phase 5 of #127).
//! Connection setup only — the schema, statements, and `RunHistory` impl are
//! shared with SQLite via [`impl_sql_history!`](super::sql).

use super::HistoryError;
use super::sql::{DDL, Dialect, Stmts, impl_sql_history};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

impl_sql_history!(PostgresHistory, sqlx::PgPool);

impl PostgresHistory {
    /// Connect, create the schema if absent, and return the backend.
    pub async fn connect(url: &str, idem_retention: Duration) -> Result<Self, HistoryError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .map_err(|e| HistoryError::Backend(format!("Postgres connection failed: {e}")))?;
        for stmt in DDL {
            sqlx::query(stmt)
                .execute(&pool)
                .await
                .map_err(|e| HistoryError::Backend(format!("creating run-history schema: {e}")))?;
        }
        Ok(Self::from_parts(
            pool,
            idem_retention,
            Stmts::new(Dialect::Postgres),
        ))
    }
}
