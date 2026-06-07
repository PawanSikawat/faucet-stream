//! SQLite sink implementation.

use crate::config::{SqliteColumnMapping, SqliteSinkConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_core::util::quote_ident;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;
use std::time::Duration;

/// A sink that writes JSON records to a SQLite table.
pub struct SqliteSink {
    config: SqliteSinkConfig,
    pool: SqlitePool,
}

impl SqliteSink {
    /// Create a new SQLite sink. Establishes a connection pool.
    ///
    /// The pool opens each connection with `journal_mode = WAL` and a 5-second
    /// `busy_timeout`. WAL lets a writer and readers proceed concurrently
    /// instead of locking each other out, and the busy timeout makes a
    /// connection wait-and-retry for the write lock rather than failing
    /// immediately with `SQLITE_BUSY` under contention. `create_if_missing`
    /// preserves the previous behaviour of creating the database file on first
    /// open. WAL on a `sqlite::memory:` database is a harmless no-op.
    pub async fn new(config: SqliteSinkConfig) -> Result<Self, FaucetError> {
        let options = SqliteConnectOptions::from_str(&config.database_url)
            .map_err(|e| FaucetError::Sink(format!("invalid SQLite database_url: {e}")))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(options)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite connection failed: {e}")))?;

        Ok(Self { config, pool })
    }

    /// Insert a batch of records using JSON column mode.
    /// Uses a single multi-row INSERT wrapped in a transaction.
    async fn insert_json(&self, records: &[Value], column: &str) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Build multi-row INSERT: INSERT INTO t (col) VALUES (?), (?), ...
        let placeholders: Vec<&str> = records.iter().map(|_| "(?)").collect();
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            quote_ident(&self.config.table_name),
            quote_ident(column),
            placeholders.join(", ")
        );

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        let mut q = sqlx::query(&insert_sql);
        for record in records {
            let json_str = serde_json::to_string(record)
                .map_err(|e| FaucetError::Sink(format!("failed to serialize record: {e}")))?;
            q = q.bind(json_str);
        }

        q.execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite insert failed: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;

        Ok(records.len())
    }

    /// Insert a batch of records using auto-mapped columns.
    ///
    /// Discovers column names from `pragma_table_info` and maps
    /// top-level JSON fields to columns. Uses a single multi-row INSERT
    /// wrapped in a transaction.
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

        // Pre-validate all records and collect matched column values. The
        // INSERT column set is the UNION of table columns present in ANY record
        // (in declared table order), not just the first record's keys —
        // otherwise a field present only in a later record of the batch would be
        // silently dropped (audit #146 H1). A row missing a unioned column binds
        // SQL NULL.
        let mut matched_rows: Vec<Vec<(&String, &Value)>> = Vec::with_capacity(records.len());
        let mut used: std::collections::HashSet<&str> = std::collections::HashSet::new();

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

            for (c, _) in &matching {
                used.insert(c.as_str());
            }
            matched_rows.push(matching);
        }

        if matched_rows.is_empty() {
            return Ok(0);
        }

        // Table columns (in declared order) that appear in at least one record.
        let insert_columns: Vec<String> = columns
            .iter()
            .filter(|c| used.contains(c.as_str()))
            .cloned()
            .collect();

        let num_cols = insert_columns.len();
        let num_rows = matched_rows.len();
        let col_names: Vec<String> = insert_columns.iter().map(|c| quote_ident(c)).collect();

        // SQLite caps bind parameters per statement at SQLITE_MAX_VARIABLE_NUMBER
        // (32766 since 3.32). A multi-row INSERT binds `rows × num_cols`
        // parameters, so a wide table at a large batch_size can exceed it and
        // fail at runtime with "too many SQL variables" (#78/#21). Split into
        // sub-INSERTs of at most floor(MAX_VARS / num_cols) rows.
        const MAX_SQLITE_VARS: usize = 32766;
        let max_rows_per_insert = (MAX_SQLITE_VARS / num_cols).max(1);

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        for sub in matched_rows.chunks(max_rows_per_insert) {
            // Build multi-row VALUES clause: (?, ?), (?, ?), ...
            let row_placeholder = format!("({})", vec!["?"; num_cols].join(", "));
            let value_tuples: Vec<&str> =
                (0..sub.len()).map(|_| row_placeholder.as_str()).collect();
            let query = format!(
                "INSERT INTO {} ({}) VALUES {}",
                quote_ident(&self.config.table_name),
                col_names.join(", "),
                value_tuples.join(", ")
            );

            let mut q = sqlx::query(&query);
            for matched in sub {
                for col in &insert_columns {
                    let val = matched.iter().find(|(c, _)| *c == col).map(|(_, v)| *v);
                    // Bind native SQLite types so column affinity and typed reads
                    // round-trip correctly. Binding every value as a JSON string
                    // (the old behaviour) stored `"Bob"` with embedded quotes,
                    // turned `true` into the text "true", and bound the literal
                    // text "null" for absent columns instead of SQL NULL (#78/#4).
                    q = match val {
                        None | Some(Value::Null) => q.bind(None::<String>),
                        Some(Value::Bool(b)) => q.bind(*b),
                        Some(Value::Number(n)) => {
                            if let Some(i) = n.as_i64() {
                                q.bind(i)
                            } else if let Some(f) = n.as_f64() {
                                q.bind(f)
                            } else {
                                // u64 above i64::MAX — preserve exact text.
                                q.bind(n.to_string())
                            }
                        }
                        Some(Value::String(s)) => q.bind(s.clone()),
                        // Arrays/objects have no scalar SQL representation — store
                        // their JSON text (suitable for TEXT / JSON columns).
                        Some(v) => q.bind(v.to_string()),
                    };
                }
            }

            q.execute(&mut *tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite insert failed: {e}")))?;
        }

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;

        Ok(num_rows)
    }
}

#[async_trait]
impl faucet_core::Sink for SqliteSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(SqliteSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        let path = self
            .config
            .database_url
            .trim_start_matches("sqlite://")
            .trim_start_matches("sqlite:");
        format!("sqlite://{}?table={}", path, self.config.table_name)
    }

    /// Preflight connectivity probe (`faucet doctor`).
    ///
    /// Acquires a connection from the existing pool and runs `SELECT 1`. This
    /// is non-mutating and idempotent — it validates that the database file /
    /// connection opens without writing anything.
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
                    "check database_url / that the database file is reachable and openable",
                ),
                Err(_) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    "timed out",
                    "check database_url / that the database file is reachable and openable",
                ),
            };
        Ok(CheckReport::single(probe))
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // `batch_size = 0` is the "no batching" sentinel: write the entire
        // upstream slice as a single multi-row INSERT inside one
        // `BEGIN`/`COMMIT` transaction, preserving `StreamPage` framing.
        // Otherwise re-chunk into `batch_size` slices so each transaction
        // stays near SQLite's sweet spot (~1000 rows per multi-row INSERT).
        let effective_chunk = if self.config.batch_size == 0 {
            records.len()
        } else {
            self.config.batch_size
        };

        let mut total = 0;
        for chunk in records.chunks(effective_chunk) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SqliteSinkConfig;
    use faucet_core::Sink as _;

    #[tokio::test]
    async fn dataset_uri_strips_sqlite_prefix_and_includes_table() {
        let config = SqliteSinkConfig::new("sqlite:///tmp/test.db", "events");
        let sink = SqliteSink::new(config).await.unwrap();
        assert_eq!(sink.dataset_uri(), "sqlite:///tmp/test.db?table=events");
    }

    #[tokio::test]
    async fn dataset_uri_with_memory_db() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "logs");
        let sink = SqliteSink::new(config).await.unwrap();
        assert_eq!(sink.dataset_uri(), "sqlite://:memory:?table=logs");
    }
}
