//! SQLite sink implementation.

use crate::config::{SqliteColumnMapping, SqliteSinkConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_core::util::quote_ident;
use serde_json::Value;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Row, SqlitePool};

/// A sink that writes JSON records to a SQLite table.
pub struct SqliteSink {
    config: SqliteSinkConfig,
    pool: SqlitePool,
}

impl SqliteSink {
    /// Create a new SQLite sink. Establishes a connection pool.
    pub async fn new(config: SqliteSinkConfig) -> Result<Self, FaucetError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&config.database_url)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite connection failed: {e}")))?;

        Ok(Self { config, pool })
    }

    /// Insert a batch of records using JSON column mode.
    async fn insert_json(&self, records: &[Value], column: &str) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES (?)",
            quote_ident(&self.config.table_name),
            quote_ident(column)
        );

        let mut total = 0;
        for record in records {
            let json_str = serde_json::to_string(record)
                .map_err(|e| FaucetError::Sink(format!("failed to serialize record: {e}")))?;

            sqlx::query(&insert_sql)
                .bind(&json_str)
                .execute(&self.pool)
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite insert failed: {e}")))?;

            total += 1;
        }

        Ok(total)
    }

    /// Insert a batch of records using auto-mapped columns.
    ///
    /// Discovers column names from `pragma_table_info` and maps
    /// top-level JSON fields to columns.
    async fn insert_auto_map(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Get column names from the table using pragma_table_info.
        let columns: Vec<String> = sqlx::query(&format!(
            "PRAGMA table_info({})",
            quote_ident(&self.config.table_name)
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("failed to query table columns: {e}")))?
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();

        if columns.is_empty() {
            return Err(FaucetError::Sink(format!(
                "table '{}' has no columns or does not exist",
                self.config.table_name
            )));
        }

        let mut total = 0;
        for record in records {
            let obj = record
                .as_object()
                .ok_or_else(|| FaucetError::Sink("AutoMap requires JSON object records".into()))?;

            // Only insert keys that match existing columns.
            let matching: Vec<(&String, &Value)> = columns
                .iter()
                .filter_map(|col| obj.get(col).map(|v| (col, v)))
                .collect();

            if matching.is_empty() {
                tracing::warn!(
                    record_keys = ?obj.keys().collect::<Vec<_>>(),
                    table_columns = ?columns,
                    "record has no keys matching table columns, skipping"
                );
                continue;
            }

            let col_names: Vec<String> = matching.iter().map(|(c, _)| quote_ident(c)).collect();
            let placeholders: Vec<&str> = matching.iter().map(|_| "?").collect();

            let query = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                quote_ident(&self.config.table_name),
                col_names.join(", "),
                placeholders.join(", ")
            );

            let mut q = sqlx::query(&query);
            for (_, val) in &matching {
                // Bind as JSON string for SQLite compatibility.
                let s = serde_json::to_string(val).map_err(|e| {
                    FaucetError::Sink(format!("failed to serialize column value: {e}"))
                })?;
                q = q.bind(s);
            }

            q.execute(&self.pool)
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite insert failed: {e}")))?;

            total += 1;
        }

        Ok(total)
    }
}

#[async_trait]
impl faucet_core::Sink for SqliteSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let mut total = 0;
        for chunk in records.chunks(self.config.batch_size) {
            total += match &self.config.column_mapping {
                SqliteColumnMapping::Json { column } => self.insert_json(chunk, column).await?,
                SqliteColumnMapping::AutoMap => self.insert_auto_map(chunk).await?,
            };
        }

        tracing::info!(
            table = %self.config.table_name,
            rows = total,
            "SQLite write complete"
        );
        Ok(total)
    }
}
