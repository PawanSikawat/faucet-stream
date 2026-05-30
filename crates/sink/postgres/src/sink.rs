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
            .max_connections(config.max_connections)
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
    /// Uses a single multi-row INSERT for efficiency.
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

        // Pre-validate all records and collect matched column values.
        // Each entry is the list of (column_index, value) for one record.
        let mut matched_rows: Vec<Vec<(&String, &Value)>> = Vec::with_capacity(records.len());

        // Determine the set of columns used across all records by using
        // the columns from the first valid record. All rows in a single
        // multi-row INSERT must share the same column list.
        let mut insert_columns: Option<Vec<String>> = None;

        for record in records {
            let obj = record
                .as_object()
                .ok_or_else(|| FaucetError::Sink("AutoMap requires JSON object records".into()))?;

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

            // Fix the column set from the first valid record.
            if insert_columns.is_none() {
                insert_columns = Some(matching.iter().map(|(c, _)| (*c).clone()).collect());
            }

            matched_rows.push(matching);
        }

        let insert_columns = match insert_columns {
            Some(cols) => cols,
            None => return Ok(0),
        };

        if matched_rows.is_empty() {
            return Ok(0);
        }

        let num_cols = insert_columns.len();
        let num_rows = matched_rows.len();
        let col_names: Vec<String> = insert_columns.iter().map(|c| quote_ident(c)).collect();

        // PostgreSQL caps bind parameters per statement at 65535. A multi-row
        // INSERT binds `rows × num_cols` parameters, so a wide table at a large
        // batch_size can exceed it and fail at runtime (#78/#21). Split into
        // sub-INSERTs of at most floor(MAX_PARAMS / num_cols) rows.
        const MAX_PG_PARAMS: usize = 65535;
        let max_rows_per_insert = (MAX_PG_PARAMS / num_cols).max(1);

        for sub in matched_rows.chunks(max_rows_per_insert) {
            // Build multi-row VALUES clause: ($1, $2), ($3, $4), ...
            let mut value_tuples: Vec<String> = Vec::with_capacity(sub.len());
            for row_idx in 0..sub.len() {
                let start = row_idx * num_cols + 1;
                let placeholders: Vec<String> =
                    (start..start + num_cols).map(|i| format!("${i}")).collect();
                value_tuples.push(format!("({})", placeholders.join(", ")));
            }

            let query = format!(
                "INSERT INTO {} ({}) VALUES {}",
                quote_ident(&self.config.table_name),
                col_names.join(", "),
                value_tuples.join(", ")
            );

            let mut q = sqlx::query(&query);
            for matched in sub {
                // Bind values in the fixed column order. If a record is missing
                // a column that appeared in the first record, bind null.
                for col in &insert_columns {
                    let val = matched.iter().find(|(c, _)| *c == col).map(|(_, v)| *v);
                    q = q.bind(val.cloned());
                }
            }

            q.execute(&self.pool)
                .await
                .map_err(|e| FaucetError::Sink(format!("PostgreSQL insert failed: {e}")))?;
        }

        Ok(num_rows)
    }
}

#[async_trait]
impl faucet_core::Sink for PostgresSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(PostgresSinkConfig))
            .expect("schema serialization")
    }

    /// Preflight connectivity probe (`faucet doctor`).
    ///
    /// Acquires a connection from the existing pool and runs `SELECT 1`. This
    /// is non-mutating and idempotent — it validates that the database is
    /// reachable and the credentials are accepted without writing anything.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let probe =
            match tokio::time::timeout(ctx.timeout, sqlx::query("SELECT 1").execute(&self.pool))
                .await
            {
                Ok(Ok(_)) => Probe::pass("auth", started.elapsed()),
                Ok(Err(e)) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    e.to_string(),
                    "check connection_url / credentials / that the database is reachable",
                ),
                Err(_) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    "timed out",
                    "check connection_url / credentials / that the database is reachable",
                ),
            };
        Ok(CheckReport::single(probe))
    }

    /// Write records to PostgreSQL.
    ///
    /// When `config.batch_size > 0` and the input slice is larger than
    /// `batch_size`, the slice is split into chunks of `batch_size` rows and
    /// each chunk is sent as a separate multi-row `INSERT`. When
    /// `config.batch_size == 0`, the entire slice is sent in a single
    /// `INSERT` — useful when upstream `StreamPage`s are already sized for
    /// Postgres' per-statement bind-parameter limit (~65 535 / num_columns
    /// in AutoMap mode).
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            // Sentinel: pass the entire upstream page through in a single
            // INSERT statement. Subject to Postgres' 65 535 bind-parameter
            // limit in AutoMap mode; JSONB mode binds a single array.
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut total = 0;
        for chunk in chunks {
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
