//! SQLite source implementation.

use crate::config::SqliteSourceConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Column, Row, SqlitePool};

/// A source that executes a SQL query against SQLite and returns rows as JSON.
pub struct SqliteSource {
    config: SqliteSourceConfig,
    pool: SqlitePool,
}

impl SqliteSource {
    /// Create a new SQLite source. Establishes a connection pool.
    pub async fn new(config: SqliteSourceConfig) -> Result<Self, FaucetError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.database_url)
            .await
            .map_err(|e| FaucetError::Config(format!("SQLite connection failed: {e}")))?;

        Ok(Self { config, pool })
    }
}

/// Convert a SQLite row column value to a `serde_json::Value`.
///
/// SQLite has dynamic typing — values are stored as INTEGER, REAL, TEXT,
/// BLOB, or NULL. We try each type in order of specificity.
fn sqlite_value_to_json(row: &sqlx::sqlite::SqliteRow, col_name: &str) -> Value {
    // Try JSON first (TEXT that parses as JSON)
    if let Ok(v) = row.try_get::<Value, _>(col_name) {
        return v;
    }

    if let Ok(v) = row.try_get::<String, _>(col_name) {
        return Value::String(v);
    }
    if let Ok(v) = row.try_get::<i64, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<i32, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<f64, _>(col_name) {
        return serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<bool, _>(col_name) {
        return Value::Bool(v);
    }

    Value::Null
}

#[async_trait]
impl faucet_core::Source for SqliteSource {
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        let rows = sqlx::query(&self.config.query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FaucetError::Config(format!("SQLite query failed: {e}")))?;

        let mut records = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut map = serde_json::Map::new();
            for col in row.columns() {
                let name = col.name().to_string();
                let value = sqlite_value_to_json(row, &name);
                map.insert(name, value);
            }
            records.push(Value::Object(map));
        }

        tracing::info!(
            rows = records.len(),
            query = %self.config.query,
            "SQLite source fetch complete"
        );
        Ok(records)
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(SqliteSourceConfig))
            .expect("schema serialization")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Source;

    #[tokio::test]
    async fn fetch_from_memory_db() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1 AS val, 'hello' AS msg");
        let source = SqliteSource::new(config).await.unwrap();
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["val"], 1);
        assert_eq!(records[0]["msg"], "hello");
    }

    #[tokio::test]
    async fn fetch_from_table() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1");
        let source = SqliteSource::new(config).await.unwrap();

        // Create a table and insert data.
        sqlx::query("CREATE TABLE test_items (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
            .execute(&source.pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO test_items (id, name, score) VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.0)",
        )
        .execute(&source.pool)
        .await
        .unwrap();

        // Reuse the same pool by creating a new source pointing to same in-memory db.
        // For in-memory DBs, each connection gets its own DB, so we query through the existing pool.
        let rows = sqlx::query("SELECT * FROM test_items ORDER BY id")
            .fetch_all(&source.pool)
            .await
            .unwrap();

        assert_eq!(rows.len(), 2);
        let row0 = &rows[0];
        assert_eq!(row0.try_get::<i64, _>("id").unwrap(), 1);
        assert_eq!(row0.try_get::<String, _>("name").unwrap(), "Alice");
    }

    #[tokio::test]
    async fn empty_result() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1 AS x WHERE 1 = 0");
        let source = SqliteSource::new(config).await.unwrap();
        let records = source.fetch_all().await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn invalid_query_returns_error() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "INVALID SQL");
        let source = SqliteSource::new(config).await.unwrap();
        let result = source.fetch_all().await;
        assert!(result.is_err());
    }
}
