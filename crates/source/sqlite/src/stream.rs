//! SQLite source implementation.

use crate::config::SqliteSourceConfig;
use async_trait::async_trait;
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::TryStreamExt;
use serde_json::Value;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Column, Row, SqlitePool};
use std::pin::Pin;

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

/// Build the effective SQL query and ordered context-bind values for a given
/// parent context. Returns the literal query when there is no context.
///
/// SQLite uses positional `?` placeholders (not the `$N` form used by
/// PostgreSQL), so the bind-marker formatter ignores the index.
fn resolve_query(
    config: &SqliteSourceConfig,
    context: &std::collections::HashMap<String, Value>,
) -> (String, Vec<Value>) {
    if context.is_empty() {
        (config.query.clone(), Vec::new())
    } else {
        faucet_core::util::substitute_context_bind_params(&config.query, context, 1, |_| {
            "?".to_string()
        })
    }
}

/// Apply context-derived bind values onto a sqlx query.
fn bind_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    bind_values: &'q [Value],
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    for value in bind_values {
        query = match value {
            Value::String(s) => query.bind(s.clone()),
            Value::Number(n) if n.is_i64() => query.bind(n.as_i64().unwrap()),
            Value::Number(n) => query.bind(n.as_f64().unwrap_or(0.0)),
            Value::Bool(b) => query.bind(*b),
            Value::Null => query.bind(None::<String>),
            _ => query.bind(value.to_string()),
        };
    }
    query
}

/// Convert a single `SqliteRow` into a JSON object whose keys are the row's
/// column names.
fn row_to_json(row: &sqlx::sqlite::SqliteRow) -> Value {
    let mut map = serde_json::Map::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let value = sqlite_value_to_json(row, &name);
        map.insert(name, value);
    }
    Value::Object(map)
}

#[async_trait]
impl faucet_core::Source for SqliteSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (query_str, bind_values) = resolve_query(&self.config, context);
        let query = bind_params(sqlx::query(&query_str), &bind_values);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FaucetError::Config(format!("SQLite query failed: {e}")))?;

        let records: Vec<Value> = rows.iter().map(row_to_json).collect();
        tracing::info!(
            rows = records.len(),
            query = %self.config.query,
            "SQLite source fetch complete"
        );
        Ok(records)
    }

    /// Stream rows from the underlying sqlx cursor without buffering the full
    /// result set. Each emitted [`StreamPage`] holds up to
    /// [`SqliteSourceConfig::batch_size`] rows.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// `batch_size = 0` drains the entire cursor into a single page. SQLite
    /// is an in-process engine with no server-side cursor concept, so this
    /// streams rows page-by-page off the local file rather than across a
    /// network wire. The sqlite query source has no incremental-replication
    /// mode today, so every emitted page carries `bookmark: None`.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let (query_str, bind_values) = resolve_query(&self.config, context);
            let query = bind_params(sqlx::query(&query_str), &bind_values);

            let mut rows = query.fetch(&self.pool);
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            while let Some(row) = rows
                .try_next()
                .await
                .map_err(|e| FaucetError::Config(format!("SQLite query failed: {e}")))?
            {
                buffer.push(row_to_json(&row));
                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(initial_capacity));
                    total += page.len();
                    yield StreamPage { records: page, bookmark: None };
                }
            }
            if !buffer.is_empty() {
                total += buffer.len();
                yield StreamPage { records: buffer, bookmark: None };
            }

            tracing::info!(
                rows = total,
                batch_size,
                query = %self.config.query,
                "SQLite source stream complete",
            );
        })
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

    #[tokio::test]
    async fn fetch_with_context_substitutes_query_placeholders() {
        let config =
            SqliteSourceConfig::new("sqlite::memory:", "SELECT {val} AS result, {name} AS name");
        let source = SqliteSource::new(config).await.unwrap();

        let mut context = std::collections::HashMap::new();
        context.insert("val".to_string(), serde_json::json!(42));
        context.insert("name".to_string(), serde_json::json!("hello"));

        let records = source.fetch_with_context(&context).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["result"], 42);
        assert_eq!(records[0]["name"], "hello");
    }

    #[tokio::test]
    async fn fetch_with_context_prevents_sql_injection() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT {val} AS result");
        let source = SqliteSource::new(config).await.unwrap();

        let mut context = std::collections::HashMap::new();
        context.insert(
            "val".to_string(),
            serde_json::json!("1; DROP TABLE test; --"),
        );

        // Value is bound as a parameter, not interpolated — no injection possible
        let records = source.fetch_with_context(&context).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["result"], "1; DROP TABLE test; --");
    }
}
