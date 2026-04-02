//! PostgreSQL sink implementation.

use crate::config::{PostgresColumnMapping, PostgresSinkConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_core::util::quote_ident;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

/// A sink that writes JSON records to a PostgreSQL table.
pub struct PostgresSink {
    config: PostgresSinkConfig,
    pool: PgPool,
}

impl PostgresSink {
    /// Create a new PostgreSQL sink. Establishes a connection pool.
    pub async fn new(config: PostgresSinkConfig) -> Result<Self, FaucetError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&config.connection_url)
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL connection failed: {e}")))?;

        Ok(Self { config, pool })
    }

    /// Insert a batch of records using JSONB column mode.
    async fn insert_jsonb(&self, records: &[Value], column: &str) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Use a single INSERT with unnest for efficiency.
        let json_values: Vec<serde_json::Value> = records.to_vec();
        let query = format!(
            "INSERT INTO {} ({}) SELECT * FROM unnest($1::jsonb[])",
            quote_ident(&self.config.table_name),
            quote_ident(column)
        );

        sqlx::query(&query)
            .bind(json_values)
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL insert failed: {e}")))?;

        Ok(records.len())
    }

    /// Insert a batch of records using auto-mapped columns.
    ///
    /// Discovers column names from the table schema and maps
    /// top-level JSON fields to columns. Values are inserted as JSONB.
    async fn insert_auto_map(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Get column names from the table.
        let columns: Vec<String> = sqlx::query(
            "SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position"
        )
        .bind(&self.config.table_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("failed to query table columns: {e}")))?
        .iter()
        .map(|row| row.get::<String, _>("column_name"))
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
            let placeholders: Vec<String> = (1..=matching.len()).map(|i| format!("${i}")).collect();

            let query = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                quote_ident(&self.config.table_name),
                col_names.join(", "),
                placeholders.join(", ")
            );

            let mut q = sqlx::query(&query);
            for (_, val) in &matching {
                q = q.bind(*val);
            }

            q.execute(&self.pool)
                .await
                .map_err(|e| FaucetError::Sink(format!("PostgreSQL insert failed: {e}")))?;

            total += 1;
        }

        Ok(total)
    }
}

#[async_trait]
impl faucet_core::Sink for PostgresSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let mut total = 0;
        for chunk in records.chunks(self.config.batch_size) {
            total += match &self.config.column_mapping {
                PostgresColumnMapping::Jsonb { column } => self.insert_jsonb(chunk, column).await?,
                PostgresColumnMapping::AutoMap => self.insert_auto_map(chunk).await?,
            };
        }

        tracing::info!(
            table = %self.config.table_name,
            rows = total,
            "PostgreSQL write complete"
        );
        Ok(total)
    }
}
