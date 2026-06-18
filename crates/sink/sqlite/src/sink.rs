//! SQLite sink implementation.

use crate::config::{SqliteColumnMapping, SqliteSinkConfig};
use async_trait::async_trait;
use faucet_core::util::quote_ident;
use faucet_core::{FaucetError, SchemaEvolution, SqlBaseType, json_schema_base_type};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;
use std::time::Duration;

/// Map a [`SqlBaseType`] to the SQLite column-type keyword used when adding a
/// column during schema evolution (issue #194). SQLite uses dynamic typing
/// (type affinity), so these are advisory affinities rather than strict types:
/// `Boolean` maps to `INTEGER` (SQLite has no native boolean) and `Json` to
/// `TEXT` (JSON is stored as text).
fn sqlite_keyword(t: SqlBaseType) -> &'static str {
    match t {
        SqlBaseType::Integer => "INTEGER",
        SqlBaseType::Double => "REAL",
        SqlBaseType::Boolean => "INTEGER",
        SqlBaseType::Text => "TEXT",
        SqlBaseType::Json => "TEXT",
    }
}

/// `ALTER TABLE <table> ADD COLUMN "<col>" <kw>` — SQLite has no
/// `ADD COLUMN IF NOT EXISTS`, so [`SqliteSink::evolve_schema`] only emits this
/// for columns it has already verified are absent (idempotency by pre-check).
/// `table` is the unquoted table name; it is quoted here via [`quote_ident`].
fn build_add_column_sql(table: &str, col: &str, t: SqlBaseType) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        quote_ident(table),
        quote_ident(col),
        sqlite_keyword(t)
    )
}

/// Map a SQLite column affinity string (`PRAGMA table_info.type`, e.g. `INTEGER`,
/// `REAL`, `VARCHAR(255)`, `TEXT`) to a JSON-Schema type fragment so
/// [`SqliteSink::current_schema`] round-trips with [`faucet_core::diff_schema`].
///
/// SQLite determines affinity by a tolerant, case-insensitive substring match on
/// the declared type (the rules in <https://www.sqlite.org/datatype3.html>), so
/// this mirrors that: contains `INT` → integer; `CHAR`/`CLOB`/`TEXT` → string;
/// `REAL`/`FLOA`/`DOUB` (and the loose `NUMERIC`/`DECIMAL`) → number; everything
/// else falls back to string. `nullable` reflects `PRAGMA table_info.notnull == 0`.
fn sqlite_affinity_to_json_schema(declared: &str, nullable: bool) -> serde_json::Value {
    let up = declared.to_ascii_uppercase();
    let contains = |needle: &str| up.contains(needle);
    let base = if contains("INT") {
        "integer"
    } else if contains("CHAR") || contains("CLOB") || contains("TEXT") {
        "string"
    } else if contains("REAL")
        || contains("FLOA")
        || contains("DOUB")
        || contains("NUMERIC")
        || contains("DECIMAL")
    {
        "number"
    } else {
        "string"
    };
    if nullable {
        serde_json::json!({ "type": [base, "null"] })
    } else {
        serde_json::json!({ "type": base })
    }
}

/// Build the `ON CONFLICT(key) DO UPDATE …` tail for an upsert INSERT.
/// Non-key columns are SET from `excluded`. If every column is a key column,
/// emit `DO NOTHING`.
fn on_conflict_clause(key: &[String], all_cols: &[String]) -> String {
    let key_list = key
        .iter()
        .map(|k| quote_ident(k))
        .collect::<Vec<_>>()
        .join(", ");
    let updates: Vec<String> = all_cols
        .iter()
        .filter(|c| !key.iter().any(|k| k == *c))
        .map(|c| format!("{q} = excluded.{q}", q = quote_ident(c)))
        .collect();
    if updates.is_empty() {
        format!("ON CONFLICT({key_list}) DO NOTHING")
    } else {
        format!(
            "ON CONFLICT({key_list}) DO UPDATE SET {}",
            updates.join(", ")
        )
    }
}

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
        config.write.validate()?;
        if !matches!(config.write.write_mode, faucet_core::WriteMode::Append)
            && !matches!(config.column_mapping, SqliteColumnMapping::AutoMap)
        {
            return Err(FaucetError::Config(
                "sqlite sink: write_mode upsert/delete requires column_mapping: auto_map \
                 (key columns must be real columns, not inside a JSON blob)"
                    .into(),
            ));
        }

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

    /// Insert JSON-column records within an existing transaction, sub-chunking
    /// at SQLite's bind-variable cap. JSON mode binds one variable per row.
    async fn insert_json_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        records: &[Value],
        column: &str,
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        // SQLite caps bind params per statement at 32766 (>=3.32). JSON mode
        // binds one variable per row, so chunk at that cap.
        const MAX_SQLITE_VARS: usize = 32766;
        for chunk in records.chunks(MAX_SQLITE_VARS) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "(?)").collect();
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                quote_ident(&self.config.table_name),
                quote_ident(column),
                placeholders.join(", ")
            );
            let mut q = sqlx::query(&insert_sql);
            for record in chunk {
                let json_str = serde_json::to_string(record)
                    .map_err(|e| FaucetError::Sink(format!("failed to serialize record: {e}")))?;
                q = q.bind(json_str);
            }
            q.execute(&mut **tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite insert failed: {e}")))?;
        }
        Ok(records.len())
    }

    /// Insert a batch of records using JSON column mode.
    /// Opens its own `BEGIN`/`COMMIT` transaction and delegates to
    /// [`Self::insert_json_tx`], which sub-chunks at SQLite's bind-variable cap.
    async fn insert_json(&self, records: &[Value], column: &str) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;
        let n = self.insert_json_tx(&mut tx, records, column).await?;
        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;
        Ok(n)
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

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        let written = self.insert_auto_map_tx(&mut tx, records).await?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;

        Ok(written)
    }

    /// Auto-map insert against an in-progress transaction.
    ///
    /// This is the reusable core shared by [`Self::insert_auto_map`] (which
    /// opens its own `BEGIN`/`COMMIT`) and [`faucet_core::Sink::write_batch_idempotent`]
    /// (which folds the insert and the commit-token upsert into one
    /// transaction). The read-only `PRAGMA table_info` column-discovery query
    /// runs on the transaction's own connection (`&mut **tx`), not on
    /// `&self.pool` — otherwise, with the default single-connection pool, it
    /// would deadlock waiting for a connection the open transaction is holding.
    ///
    /// When `conflict_key` is `Some(key)`, each sub-chunk's INSERT is given an
    /// `ON CONFLICT(key) DO UPDATE …` tail so it upserts by the key columns
    /// (last-write-wins within the batch is handled by the planner's dedup,
    /// so a single sub-chunk never double-hits the same conflict target).
    async fn insert_auto_map_with_conflict_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        records: &[Value],
        conflict_key: Option<&[String]>,
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Get column names from the table using pragma_table_info. Use the
        // transaction's connection so a single-connection pool doesn't deadlock.
        let columns: Vec<String> = sqlx::query(&format!(
            "PRAGMA table_info({})",
            quote_ident(&self.config.table_name)
        ))
        .fetch_all(&mut **tx)
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

        for sub in matched_rows.chunks(max_rows_per_insert) {
            // Build multi-row VALUES clause: (?, ?), (?, ?), ...
            let row_placeholder = format!("({})", vec!["?"; num_cols].join(", "));
            let value_tuples: Vec<&str> =
                (0..sub.len()).map(|_| row_placeholder.as_str()).collect();
            let base_query = format!(
                "INSERT INTO {} ({}) VALUES {}",
                quote_ident(&self.config.table_name),
                col_names.join(", "),
                value_tuples.join(", ")
            );
            let query = match conflict_key {
                Some(key) => format!("{base_query} {}", on_conflict_clause(key, &insert_columns)),
                None => base_query,
            };

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

            q.execute(&mut **tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite insert failed: {e}")))?;
        }

        Ok(num_rows)
    }

    /// Auto-map insert against an in-progress transaction with plain append
    /// semantics (no `ON CONFLICT` tail).
    ///
    /// Thin wrapper over
    /// [`insert_auto_map_with_conflict_tx`](Self::insert_auto_map_with_conflict_tx)
    /// so the append path and the idempotent-write path keep their original
    /// signature.
    async fn insert_auto_map_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        self.insert_auto_map_with_conflict_tx(tx, records, None)
            .await
    }

    /// Delete rows whose key columns match any of `deletes`, using
    /// `DELETE FROM t WHERE (k1, …) IN ((?, …), …)`, chunked at
    /// SQLite's bind-variable cap. Runs inside the caller's transaction.
    async fn delete_by_keys(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        deletes: &[faucet_core::KeyTuple],
    ) -> Result<usize, FaucetError> {
        if deletes.is_empty() {
            return Ok(0);
        }
        let key = &self.config.write.key;
        let table_ref = quote_ident(&self.config.table_name);
        let col_list = key
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");

        const MAX_SQLITE_VARS: usize = 32766;
        let per = (MAX_SQLITE_VARS / key.len().max(1)).max(1);
        let mut total = 0usize;

        for chunk in deletes.chunks(per) {
            let tuples: Vec<String> = chunk
                .iter()
                .map(|_| format!("({})", vec!["?"; key.len()].join(", ")))
                .collect();
            let sql = format!(
                "DELETE FROM {table_ref} WHERE ({col_list}) IN ({})",
                tuples.join(", ")
            );
            let mut q = sqlx::query(&sql);
            for kt in chunk {
                for (_, v) in &kt.0 {
                    // Bind native SQLite types — same logic as in the INSERT path.
                    q = match v {
                        Value::Null => q.bind(None::<String>),
                        Value::Bool(b) => q.bind(*b),
                        Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                q.bind(i)
                            } else if let Some(f) = n.as_f64() {
                                q.bind(f)
                            } else {
                                q.bind(n.to_string())
                            }
                        }
                        Value::String(s) => q.bind(s.clone()),
                        other => q.bind(other.to_string()),
                    };
                }
            }
            let res = q
                .execute(&mut **tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite delete failed: {e}")))?;
            total += res.rows_affected() as usize;
        }
        Ok(total)
    }

    /// Apply a planned upsert/delete batch inside one `BEGIN`/`COMMIT`
    /// transaction. Upserts and deletes are wrapped together so they commit
    /// atomically.
    async fn apply_plan(&self, plan: &faucet_core::WritePlan) -> Result<usize, FaucetError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        let mut affected = 0usize;
        if !plan.upserts.is_empty() {
            affected += self
                .insert_auto_map_with_conflict_tx(
                    &mut tx,
                    &plan.upserts,
                    Some(&self.config.write.key),
                )
                .await?;
        }
        if !plan.deletes.is_empty() {
            affected += self.delete_by_keys(&mut tx, &plan.deletes).await?;
        }

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;
        Ok(affected)
    }

    /// Ensure the commit-token watermark table exists.
    async fn ensure_commit_table(&self) -> Result<(), FaucetError> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {t} ({s} TEXT PRIMARY KEY, {k} TEXT NOT NULL, updated_at TEXT DEFAULT (datetime('now')))",
            t = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            s = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
            k = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
        );
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite commit-table create failed: {e}")))?;
        Ok(())
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

    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        &[
            faucet_core::WriteMode::Append,
            faucet_core::WriteMode::Upsert,
            faucet_core::WriteMode::Delete,
        ]
    }

    fn supports_schema_evolution(&self) -> bool {
        true
    }

    /// Read the live destination schema via `PRAGMA table_info`, shaped as an
    /// `infer_schema`-compatible object (`{"type":"object","properties":{…}}`),
    /// or `None` when the target table does not exist yet (issue #194).
    ///
    /// `PRAGMA table_info` returns one row per column with `name`, `type` (the
    /// declared affinity string), and `notnull`. The affinity string is mapped
    /// to a JSON-Schema base type via [`sqlite_affinity_to_json_schema`], and
    /// `notnull == 0` surfaces the column as nullable. The PRAGMA runs on a
    /// connection acquired from the pool (a standalone read — not inside an open
    /// transaction).
    async fn current_schema(&self) -> Result<Option<serde_json::Value>, FaucetError> {
        let rows = sqlx::query(&format!(
            "PRAGMA table_info({})",
            quote_ident(&self.config.table_name)
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("sqlite current_schema query failed: {e}")))?;

        if rows.is_empty() {
            return Ok(None); // table does not exist yet (or has no columns)
        }

        let mut props = serde_json::Map::new();
        for row in &rows {
            let name: String = row.get("name");
            let declared: String = row.get("type");
            let notnull: i64 = row.get("notnull");
            props.insert(
                name,
                sqlite_affinity_to_json_schema(&declared, notnull == 0),
            );
        }
        Ok(Some(
            serde_json::json!({ "type": "object", "properties": props }),
        ))
    }

    /// Apply an additive schema evolution to the destination table (issue #194).
    ///
    /// - **Additions** — `ALTER TABLE … ADD COLUMN`. SQLite has no
    ///   `ADD COLUMN IF NOT EXISTS`, so the current columns are read first and a
    ///   column already present is silently skipped (idempotency by pre-check).
    /// - **Widenings** — a no-op under SQLite's dynamic typing: a column already
    ///   accepts a value of any type, so there is nothing to ALTER. Logged once
    ///   at `debug`.
    /// - **Nullability relaxations** — a no-op: SQLite cannot drop a `NOT NULL`
    ///   constraint in place (it requires a full table rebuild, which is out of
    ///   scope here). Logged once at `debug`; the column is left as-is.
    async fn evolve_schema(&self, evolution: &SchemaEvolution) -> Result<(), FaucetError> {
        // Read the current column set so additions are idempotent (no
        // `ADD COLUMN IF NOT EXISTS` in SQLite).
        let existing: std::collections::HashSet<String> = sqlx::query(&format!(
            "PRAGMA table_info({})",
            quote_ident(&self.config.table_name)
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("sqlite evolve table_info failed: {e}")))?
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();

        for c in &evolution.additions {
            if existing.contains(&c.name) {
                continue; // already present — ADD COLUMN would error
            }
            let t = json_schema_base_type(&c.to).unwrap_or(SqlBaseType::Text);
            sqlx::query(&build_add_column_sql(&self.config.table_name, &c.name, t))
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    FaucetError::Sink(format!("sqlite ADD COLUMN {} failed: {e}", c.name))
                })?;
        }

        if !evolution.widenings.is_empty() {
            tracing::debug!("sqlite: type widening is a no-op under dynamic typing");
        }
        for col in &evolution.relax_nullability {
            tracing::debug!("sqlite cannot relax NOT NULL in place; column {col} left as-is");
        }

        Ok(())
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Non-append modes: plan the writes and apply atomically.
        if !matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "sqlite {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            return self.apply_plan(&plan).await;
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

    /// Write a batch and report per-row outcomes.
    ///
    /// In append mode this delegates to [`write_batch`](faucet_core::Sink::write_batch) and
    /// maps a single success onto an all-`Ok(())` vector (the trait default).
    /// In upsert/delete mode the good rows are applied (upserts + deletes), and
    /// only the rows whose key could not be extracted (missing / null key) are
    /// reported as `Err` so the pipeline routes them to the DLQ per-row instead
    /// of sending the whole page.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            self.write_batch(records).await?;
            return Ok(records.iter().map(|_| Ok(())).collect());
        }

        let plan = faucet_core::plan_writes(records, &self.config.write);
        self.apply_plan(&plan).await?;

        let mut outcomes: Vec<faucet_core::RowOutcome> = records.iter().map(|_| Ok(())).collect();
        for (idx, msg) in &plan.failed {
            outcomes[*idx] = Err(FaucetError::Sink(format!(
                "sqlite {}: {msg}",
                self.config.write.write_mode.as_str()
            )));
        }
        Ok(outcomes)
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.ensure_commit_table().await?;
        let sql = format!(
            "SELECT {k} FROM {t} WHERE {s} = ?",
            t = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            k = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
            s = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
        );
        let row = sqlx::query(&sql)
            .bind(scope)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite token read failed: {e}")))?;
        Ok(row.map(|r| r.get::<String, _>(0)))
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.ensure_commit_table().await?;

        // For upsert/delete modes, plan the page before opening the transaction
        // so a key-extraction failure aborts without leaving an open tx.
        let plan = if matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            None
        } else {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "sqlite {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            Some(plan)
        };

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        // Data write and the commit-token upsert share ONE transaction so the
        // page is committed atomically with its watermark: on crash either both
        // land or neither does, which is what makes a replay skip-on-resume
        // produce zero duplicates. For upsert/delete the planned upserts/deletes
        // commit together with the watermark in this same tx (no nested tx —
        // we reuse `apply_plan`'s helpers directly on this transaction).
        let written = match &plan {
            Some(plan) => {
                let mut affected = 0usize;
                if !plan.upserts.is_empty() {
                    affected += self
                        .insert_auto_map_with_conflict_tx(
                            &mut tx,
                            &plan.upserts,
                            Some(&self.config.write.key),
                        )
                        .await?;
                }
                if !plan.deletes.is_empty() {
                    affected += self.delete_by_keys(&mut tx, &plan.deletes).await?;
                }
                affected
            }
            None => match &self.config.column_mapping {
                SqliteColumnMapping::Json { column } => {
                    self.insert_json_tx(&mut tx, records, column).await?
                }
                SqliteColumnMapping::AutoMap => self.insert_auto_map_tx(&mut tx, records).await?,
            },
        };

        let upsert = format!(
            "INSERT INTO {t} ({s}, {k}) VALUES (?, ?) ON CONFLICT({s}) DO UPDATE SET {k} = excluded.{k}, updated_at = datetime('now')",
            t = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            s = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
            k = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
        );
        sqlx::query(&upsert)
            .bind(scope)
            .bind(token)
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite token upsert failed: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;
        Ok(written)
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

    #[test]
    fn sqlite_on_conflict_clause() {
        let clause =
            on_conflict_clause(&["id".to_string()], &["id".to_string(), "name".to_string()]);
        assert_eq!(
            clause,
            r#"ON CONFLICT("id") DO UPDATE SET "name" = excluded."name""#
        );
    }

    #[test]
    fn sqlite_on_conflict_all_keys_does_nothing() {
        let clause = on_conflict_clause(&["id".to_string()], &["id".to_string()]);
        assert_eq!(clause, r#"ON CONFLICT("id") DO NOTHING"#);
    }

    #[test]
    fn sqlite_on_conflict_composite_key() {
        let clause = on_conflict_clause(
            &["a".to_string(), "b".to_string()],
            &["a".to_string(), "b".to_string(), "v".to_string()],
        );
        assert_eq!(
            clause,
            r#"ON CONFLICT("a", "b") DO UPDATE SET "v" = excluded."v""#
        );
    }

    #[test]
    fn sqlite_add_column_ddl() {
        assert_eq!(
            build_add_column_sql("t", "email", SqlBaseType::Text),
            r#"ALTER TABLE "t" ADD COLUMN "email" TEXT"#
        );
        assert_eq!(
            build_add_column_sql("t", "age", SqlBaseType::Integer),
            r#"ALTER TABLE "t" ADD COLUMN "age" INTEGER"#
        );
        assert_eq!(
            build_add_column_sql("t", "score", SqlBaseType::Double),
            r#"ALTER TABLE "t" ADD COLUMN "score" REAL"#
        );
        // Boolean has no native SQLite type → INTEGER affinity; JSON → TEXT.
        assert_eq!(
            build_add_column_sql("t", "ok", SqlBaseType::Boolean),
            r#"ALTER TABLE "t" ADD COLUMN "ok" INTEGER"#
        );
        assert_eq!(
            build_add_column_sql("t", "meta", SqlBaseType::Json),
            r#"ALTER TABLE "t" ADD COLUMN "meta" TEXT"#
        );
    }

    #[test]
    fn sqlite_keyword_mapping() {
        assert_eq!(sqlite_keyword(SqlBaseType::Integer), "INTEGER");
        assert_eq!(sqlite_keyword(SqlBaseType::Double), "REAL");
        assert_eq!(sqlite_keyword(SqlBaseType::Boolean), "INTEGER");
        assert_eq!(sqlite_keyword(SqlBaseType::Text), "TEXT");
        assert_eq!(sqlite_keyword(SqlBaseType::Json), "TEXT");
    }

    #[test]
    fn sqlite_affinity_round_trips_to_json_schema() {
        use serde_json::json;
        // Tolerant case-insensitive substring matching, SQLite affinity rules.
        assert_eq!(
            sqlite_affinity_to_json_schema("INTEGER", false),
            json!({"type":"integer"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("BIGINT", false),
            json!({"type":"integer"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("REAL", false),
            json!({"type":"number"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("DOUBLE PRECISION", false),
            json!({"type":"number"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("DECIMAL(10,2)", false),
            json!({"type":"number"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("TEXT", false),
            json!({"type":"string"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("VARCHAR(255)", false),
            json!({"type":"string"})
        );
        // Unknown / empty affinity falls back to string.
        assert_eq!(
            sqlite_affinity_to_json_schema("BLOB", false),
            json!({"type":"string"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("", false),
            json!({"type":"string"})
        );
        // Nullable columns widen the type array.
        assert_eq!(
            sqlite_affinity_to_json_schema("integer", true),
            json!({"type":["integer","null"]})
        );
    }
}
