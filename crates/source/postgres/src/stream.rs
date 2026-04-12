//! PostgreSQL source implementation.

use crate::config::PostgresSourceConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Column, PgPool, Row};

/// A source that executes a SQL query against PostgreSQL and returns rows as JSON.
pub struct PostgresSource {
    config: PostgresSourceConfig,
    pool: PgPool,
}

impl PostgresSource {
    /// Create a new PostgreSQL source. Establishes a connection pool.
    pub async fn new(config: PostgresSourceConfig) -> Result<Self, FaucetError> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.connection_url)
            .await
            .map_err(|e| FaucetError::Config(format!("PostgreSQL connection failed: {e}")))?;

        Ok(Self { config, pool })
    }
}

/// Convert a raw sqlx column value to a `serde_json::Value`.
///
/// Uses `try_get_raw` to inspect the type info and convert accordingly.
/// Falls back to `Value::Null` for unsupported or null columns.
fn pg_value_to_json(row: &sqlx::postgres::PgRow, col_name: &str) -> Value {
    // Try JSON/JSONB first — this is the most flexible
    if let Ok(v) = row.try_get::<Value, _>(col_name) {
        return v;
    }

    // Try common scalar types
    if let Ok(v) = row.try_get::<String, _>(col_name) {
        return Value::String(v);
    }
    if let Ok(v) = row.try_get::<i64, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<i32, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<i16, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<f64, _>(col_name) {
        return serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<f32, _>(col_name) {
        return serde_json::Number::from_f64(v as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<bool, _>(col_name) {
        return Value::Bool(v);
    }

    Value::Null
}

#[async_trait]
impl faucet_core::Source for PostgresSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (query_str, bind_values) = if context.is_empty() {
            (self.config.query.clone(), Vec::new())
        } else {
            faucet_core::util::substitute_context_bind_params(
                &self.config.query,
                context,
                self.config.params.len() + 1,
                |i| format!("${i}"),
            )
        };
        let mut query = sqlx::query(&query_str);

        for param in &self.config.params {
            query = query.bind(param);
        }
        for value in &bind_values {
            query = match value {
                Value::String(s) => query.bind(s.clone()),
                Value::Number(n) if n.is_i64() => query.bind(n.as_i64().unwrap()),
                Value::Number(n) => query.bind(n.as_f64().unwrap_or(0.0)),
                Value::Bool(b) => query.bind(*b),
                Value::Null => query.bind(None::<String>),
                _ => query.bind(value.to_string()),
            };
        }

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FaucetError::Config(format!("PostgreSQL query failed: {e}")))?;

        let mut records = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut map = serde_json::Map::new();
            for col in row.columns() {
                let name = col.name().to_string();
                let value = pg_value_to_json(row, &name);
                map.insert(name, value);
            }
            records.push(Value::Object(map));
        }

        tracing::info!(rows = records.len(), query = %self.config.query, "PostgreSQL source fetch complete");
        Ok(records)
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(PostgresSourceConfig))
            .expect("schema serialization")
    }
}
