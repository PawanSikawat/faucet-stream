//! `IcebergSink` — `faucet_core::Sink` implementation for Apache Iceberg.
//!
//! Records are accumulated as Arrow `RecordBatch`es, written via the iceberg
//! writer pipeline to Parquet data files, and committed in one
//! `Transaction::fast_append` snapshot per `flush()`.
//!
//! ## Flush contract
//!
//! Callers **must** call `flush()` when they are done writing. Unflushed data
//! files are abandoned: they are written to object storage but never committed
//! as an Iceberg snapshot. The pipeline (via `faucet-core`) calls `flush()`
//! automatically at the end of each `StreamPage`.
//!
//! ## Commit failure
//!
//! If the snapshot commit itself fails (after iceberg's internal retry of
//! retryable conflicts), the already-uploaded data files are orphaned — written
//! to object storage but never referenced by any snapshot. The error
//! propagates so the run aborts without advancing the bookmark; the re-run
//! writes fresh files and commits them. Orphaned files accumulate until you run
//! Iceberg's standard `remove_orphan_files` maintenance (e.g. via Spark /
//! pyiceberg). The sink does **not** auto-delete on failure (re-committing
//! after an ambiguous commit could duplicate data).
//!
//! ## Schema management
//!
//! When `create_if_missing: true` (the default) and no table exists yet, the
//! Iceberg schema is inferred from the first batch using `infer_arrow_schema`
//! and `arrow_to_iceberg_schema`. On subsequent batches the table's existing
//! schema (converted back to Arrow with `iceberg_to_arrow_schema`) is used so
//! the writer and the table stay in sync.
//!
//! When `create_if_missing: false` the table is loaded at `new()` time; a
//! missing table produces a `FaucetError::Sink` immediately.

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use faucet_core::FaucetError;
use iceberg::spec::{DataFile, Transform, UnboundPartitionSpec};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::catalog::build_catalog;
use crate::config::{IcebergSinkConfig, PartitionField};
use crate::schema::{
    arrow_to_iceberg_schema, arrow_to_json_schema, iceberg_to_arrow_schema, infer_arrow_schema,
    json_to_record_batch,
};
use crate::writer::{TableWriter, compression_from_str};

// ── Interior state ────────────────────────────────────────────────────────────

/// Interior-mutable state shared across `Sink` method calls.
///
/// All mutation goes through `Mutex<SinkState>`. The `Mutex` is `tokio::sync::Mutex`
/// so `await` inside the guard compiles without issue.
struct SinkState {
    /// `None` until the first `write_batch` call (deferred when `create_if_missing`).
    /// Set to `Some` on first write or at `new()` when not deferred.
    table: Option<Table>,

    /// Open writer, if any. `None` between rollovers and between flushes.
    writer: Option<TableWriter>,

    /// `DataFile`s accumulated from closed writers, awaiting the next commit.
    pending_files: Vec<DataFile>,

    /// Exactly-once commit token to stamp onto the next committed snapshot.
    /// Set by `write_batch_idempotent` and consumed (cleared) by `commit_pending`.
    pending_commit: Option<(String, String)>,
}

impl SinkState {
    fn new(preloaded: Option<Table>) -> Self {
        Self {
            table: preloaded,
            writer: None,
            pending_files: Vec::new(),
            pending_commit: None,
        }
    }
}

// ── IcebergSink ───────────────────────────────────────────────────────────────

/// An Apache Iceberg sink.
///
/// Writes records to an Iceberg table via the `fast_append` transaction action,
/// creating one snapshot per `flush()` call.
pub struct IcebergSink {
    config: IcebergSinkConfig,
    catalog: Arc<dyn Catalog>,
    state: Mutex<SinkState>,
}

impl std::fmt::Debug for IcebergSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSink")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl IcebergSink {
    /// Create a new sink from the given configuration.
    ///
    /// Validates the config, builds the catalog client, and — when
    /// `create_if_missing: false` — loads and validates the target table.
    pub async fn new(config: IcebergSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;

        if config.write_mode != faucet_core::WriteMode::Append {
            return Err(FaucetError::Config(format!(
                "iceberg sink: write_mode '{}' is not supported (append only; \
                 upsert is a version-gated follow-up tracked in #179 / #190)",
                config.write_mode.as_str()
            )));
        }

        let catalog = build_catalog(&config.catalog).await?;

        let preloaded: Option<Table> = if !config.create_if_missing {
            // `create_if_missing = false`: load now so a missing table is caught
            // immediately at startup rather than silently on first write.
            let ns = NamespaceIdent::from_strs(config.namespace.iter().map(String::as_str))
                .map_err(|e| FaucetError::Sink(format!("iceberg: invalid namespace: {e}")))?;
            let tid = TableIdent::new(ns, config.table.clone());
            let table = catalog.load_table(&tid).await.map_err(|e| {
                FaucetError::Sink(format!(
                    "iceberg: table '{}' does not exist and create_if_missing is false: {e}",
                    config.table
                ))
            })?;
            Some(table)
        } else {
            None
        };

        Ok(Self {
            config,
            catalog,
            state: Mutex::new(SinkState::new(preloaded)),
        })
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Build an `UnboundPartitionSpec` from the config's `partition_spec` list
    /// by looking up field IDs in `iceberg_schema`. Returns `None` when the
    /// partition spec is empty (unpartitioned table).
    fn build_partition_spec(
        pfs: &[PartitionField],
        iceberg_schema: &iceberg::spec::Schema,
    ) -> Result<Option<UnboundPartitionSpec>, FaucetError> {
        if pfs.is_empty() {
            return Ok(None);
        }

        let struct_type = iceberg_schema.as_struct();
        let mut builder = UnboundPartitionSpec::builder();

        for pf in pfs {
            // Look up the source field ID from the iceberg schema by name.
            let field_ref = struct_type.field_by_name(&pf.source).ok_or_else(|| {
                FaucetError::Config(format!(
                    "iceberg: partition source column {:?} not found in schema",
                    pf.source
                ))
            })?;

            let transform = Transform::from_str(&pf.transform).map_err(|e| {
                FaucetError::Config(format!(
                    "iceberg: invalid transform {:?}: {e}",
                    pf.transform
                ))
            })?;

            builder = builder
                .add_partition_field(field_ref.id, pf.source.clone(), transform)
                .map_err(|e| {
                    FaucetError::Config(format!(
                        "iceberg: could not add partition field {:?}: {e}",
                        pf.source
                    ))
                })?;
        }

        Ok(Some(builder.build()))
    }

    /// Resolve the table from state, creating it when `create_if_missing` is
    /// set and no table exists yet.
    ///
    /// Returns a reference-counted clone of the resolved `Table`. Mutates
    /// `state.table` in place on first creation.
    async fn resolve_table(
        &self,
        state: &mut SinkState,
        records: &[Value],
    ) -> Result<Table, FaucetError> {
        if let Some(ref table) = state.table {
            return Ok(table.clone());
        }

        // Table not yet resolved — infer + create.
        let arrow_schema = infer_arrow_schema(records, records.len().min(100))?;
        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema)?;

        let ns = NamespaceIdent::from_strs(self.config.namespace.iter().map(String::as_str))
            .map_err(|e| FaucetError::Sink(format!("iceberg: invalid namespace: {e}")))?;
        let table_name = self.config.table.clone();

        let table_ident = TableIdent::new(ns.clone(), table_name.clone());

        let table =
            if self.catalog.table_exists(&table_ident).await.map_err(|e| {
                FaucetError::Sink(format!("iceberg: table_exists check failed: {e}"))
            })? {
                // Table was created between `new()` and first write — just load it.
                self.catalog
                    .load_table(&table_ident)
                    .await
                    .map_err(|e| FaucetError::Sink(format!("iceberg: load_table failed: {e}")))?
            } else {
                // Ensure the namespace exists before creating the table.
                // Some catalogs (e.g. REST when pre-configured, Glue) auto-create
                // namespaces; others (SQL, HMS) require an explicit
                // `create_namespace` call.  We call it unconditionally when
                // `create_if_missing: true` and swallow `AlreadyExists` errors so
                // the sink is idempotent whether or not the namespace pre-exists.
                let ns_exists = self.catalog.namespace_exists(&ns).await.map_err(|e| {
                    FaucetError::Sink(format!("iceberg: namespace_exists check failed: {e}"))
                })?;
                if !ns_exists {
                    self.catalog
                        .create_namespace(&ns, std::collections::HashMap::new())
                        .await
                        .map_err(|e| {
                            FaucetError::Sink(format!(
                                "iceberg: create_namespace {:?} failed: {e}",
                                self.config.namespace
                            ))
                        })?;
                }

                // Build partition spec from config, resolving source column IDs.
                let partition_spec =
                    Self::build_partition_spec(&self.config.partition_spec, &iceberg_schema)?;

                // The `TableCreation` TypedBuilder uses type-state, so the two
                // branches (with/without partition_spec) produce different builder
                // types. We fully build in each arm rather than trying to hold a
                // partially-built builder in a variable.
                let creation = if let Some(ps) = partition_spec {
                    TableCreation::builder()
                        .name(table_name)
                        .schema(iceberg_schema)
                        .partition_spec(ps)
                        .properties(self.config.snapshot_properties.clone())
                        .build()
                } else {
                    TableCreation::builder()
                        .name(table_name)
                        .schema(iceberg_schema)
                        .properties(self.config.snapshot_properties.clone())
                        .build()
                };

                self.catalog
                    .create_table(&ns, creation)
                    .await
                    .map_err(|e| FaucetError::Sink(format!("iceberg: create_table failed: {e}")))?
            };

        state.table = Some(table.clone());
        Ok(table)
    }

    /// Ensure the writer in `state` is open for `table`. Opens a new
    /// `TableWriter` if none is currently open.
    async fn ensure_writer(&self, state: &mut SinkState, table: &Table) -> Result<(), FaucetError> {
        if state.writer.is_none() {
            let compression = compression_from_str(&self.config.parquet.compression)?;
            let writer =
                TableWriter::new(table, compression, self.config.target_file_size_mb).await?;
            state.writer = Some(writer);
        }
        Ok(())
    }

    /// Write a single chunk of `Value` records to the open writer.
    ///
    /// Converts `records` to an Arrow `RecordBatch` against the table's arrow
    /// schema, then calls `writer.write(batch)`.
    async fn write_chunk(
        &self,
        state: &mut SinkState,
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let table = self.resolve_table(state, records).await?;
        self.ensure_writer(state, &table).await?;

        // Convert to Arrow using the table's current schema.
        let arrow_schema = iceberg_to_arrow_schema(table.metadata().current_schema())?;
        let batch = json_to_record_batch(records, &arrow_schema)?;
        let row_count = batch.num_rows();

        let writer = state.writer.as_mut().expect("writer is set above");
        writer.write(batch).await?;

        Ok(row_count)
    }

    /// Close the open writer (if any) and collect its `DataFile`s into
    /// `state.pending_files`. Does nothing when no writer is open.
    async fn close_writer(state: &mut SinkState) -> Result<(), FaucetError> {
        if let Some(writer) = state.writer.take() {
            let files = writer.close().await?;
            state.pending_files.extend(files);
        }
        Ok(())
    }

    /// Load the table read-only from the catalog, without creating it.
    ///
    /// Returns `Ok(None)` if the table does not exist yet (first run before any
    /// data has been written), `Ok(Some(table))` if it exists, or `Err` on a
    /// catalog communication failure.
    async fn load_table_readonly(&self) -> Result<Option<Table>, FaucetError> {
        let ns = NamespaceIdent::from_strs(self.config.namespace.iter().map(String::as_str))
            .map_err(|e| FaucetError::Sink(format!("iceberg: invalid namespace: {e}")))?;
        let tid = TableIdent::new(ns, self.config.table.clone());

        let exists =
            self.catalog.table_exists(&tid).await.map_err(|e| {
                FaucetError::Sink(format!("iceberg: table_exists check failed: {e}"))
            })?;

        if !exists {
            return Ok(None);
        }

        let table = self
            .catalog
            .load_table(&tid)
            .await
            .map_err(|e| FaucetError::Sink(format!("iceberg: load_table failed: {e}")))?;

        Ok(Some(table))
    }

    /// Commit all pending data files as a single `fast_append` snapshot.
    ///
    /// `Transaction::commit` in iceberg 0.9.1 already includes an internal
    /// retry loop (exponential back-off on retryable commit conflicts), so we
    /// do not add an outer retry.  Returns `Ok(())` when the commit succeeds.
    async fn commit_pending(&self, state: &mut SinkState) -> Result<(), FaucetError> {
        let files = std::mem::take(&mut state.pending_files);

        if files.is_empty() {
            // No data files — do not emit an empty snapshot.
            return Ok(());
        }

        let table = state
            .table
            .as_ref()
            .ok_or_else(|| {
                FaucetError::Sink(
                    "iceberg: flush called with pending files but no table loaded".to_string(),
                )
            })?
            .clone();

        let tx = Transaction::new(&table);

        // Merge config snapshot properties with the exactly-once commit token (if any).
        // The token is taken out of state so it is stamped exactly once, atomically
        // with the data files in this fast_append commit.
        let mut props = self.config.snapshot_properties.clone();
        if let Some((scope, token)) = state.pending_commit.take() {
            props.insert(
                faucet_core::idempotency::ICEBERG_SCOPE_PROP.to_string(),
                scope,
            );
            props.insert(
                faucet_core::idempotency::ICEBERG_TOKEN_PROP.to_string(),
                token,
            );
        }

        let mut action = tx.fast_append().add_data_files(files);
        if !props.is_empty() {
            action = action.set_snapshot_properties(props);
        }

        let tx = action
            .apply(tx)
            .map_err(|e| FaucetError::Sink(format!("iceberg: fast_append apply failed: {e}")))?;

        let updated_table = tx
            .commit(self.catalog.as_ref())
            .await
            .map_err(|e| FaucetError::Sink(format!("iceberg: transaction commit failed: {e}")))?;

        // Update the stored table handle so subsequent writes use the latest
        // metadata (snapshot ID, manifest list, etc.).
        state.table = Some(updated_table);
        Ok(())
    }
}

// ── Sink trait ────────────────────────────────────────────────────────────────

#[async_trait]
impl faucet_core::Sink for IcebergSink {
    fn connector_name(&self) -> &'static str {
        "iceberg"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(IcebergSinkConfig))
            .expect("schema serialization infallible")
    }

    fn dataset_uri(&self) -> String {
        use crate::config::CatalogConfig;
        let kind = match &self.config.catalog {
            CatalogConfig::Rest(_) => "rest",
            CatalogConfig::Glue(_) => "glue",
            CatalogConfig::Sql(_) => "sql",
            CatalogConfig::Hms(_) => "hms",
        };
        format!(
            "iceberg://{}/{}.{}",
            kind,
            self.config.namespace.join("."),
            self.config.table
        )
    }

    /// Preflight check (`faucet doctor`).
    ///
    /// Probes catalog connectivity and table existence without writing any data.
    /// Builds the namespace + table ident from config and calls `table_exists`,
    /// bounded by `ctx.timeout`. A catalog connection failure surfaces as `Fail`.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();

        let ns_result = NamespaceIdent::from_strs(self.config.namespace.iter().map(String::as_str));
        let tid_result = ns_result.map(|ns| TableIdent::new(ns, self.config.table.clone()));

        let tid = match tid_result {
            Err(e) => {
                return Ok(CheckReport::single(Probe::fail(
                    "catalog",
                    started.elapsed(),
                    format!("iceberg: invalid namespace config: {e}"),
                )));
            }
            Ok(t) => t,
        };

        let probe_result = tokio::time::timeout(ctx.timeout, self.catalog.table_exists(&tid)).await;

        let probe = match probe_result {
            // The catalog responded — it's reachable, so the probe passes
            // whether or not the target table exists yet (create_if_missing
            // handles a missing table at run time).
            Ok(Ok(_exists)) => Probe::pass("catalog", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "catalog",
                started.elapsed(),
                format!("iceberg catalog probe failed: {e}"),
                "Verify the catalog URI, credentials, and network reachability.",
            ),
            Err(_elapsed) => Probe::fail_hint(
                "catalog",
                started.elapsed(),
                format!("iceberg catalog probe timed out after {:?}", ctx.timeout),
                "Check network reachability to the catalog endpoint.",
            ),
        };

        Ok(CheckReport::single(probe))
    }

    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        // Append only — equality-delete upsert is version-gated on iceberg-rust
        // (tracked as a follow-up to #190 / #179).
        &[faucet_core::WriteMode::Append]
    }

    /// Report the live table schema as an `infer_schema`-shaped JSON object so
    /// the schema-drift policy (issue #194) can diff each page against the real
    /// destination.
    ///
    /// Loads the table read-only via the catalog; a not-yet-created table (or
    /// any "table absent" case) returns `Ok(None)` so drift handling stays inert
    /// until the table exists — only a genuine catalog communication failure
    /// surfaces as `Err`. On success the table's `current_schema()` is converted
    /// to Arrow (via `iceberg_to_arrow_schema`) and then to the JSON shape.
    ///
    /// Schema *evolution* is intentionally NOT implemented for this sink:
    /// `supports_schema_evolution` stays `false` (trait default) and
    /// `evolve_schema` keeps the trait's typed "unsupported" error. iceberg-rust
    /// 0.9.1 (pinned in this crate) exposes no schema-evolution transaction API
    /// (no `UpdateSchema` / `add_column` / type-promotion — only `fast_append`),
    /// so evolution is blocked upstream. Tracked in issue #255; the CLI rejects
    /// `on_drift: evolve` against iceberg at config-load.
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        let table = match self.load_table_readonly().await? {
            Some(t) => t,
            None => return Ok(None),
        };
        let arrow_schema = iceberg_to_arrow_schema(table.metadata().current_schema())?;
        Ok(Some(arrow_to_json_schema(&arrow_schema)))
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    /// Write `records` and durably record the exactly-once `(scope, token)` in
    /// the same atomic `fast_append` snapshot commit.
    ///
    /// The token is stashed on `SinkState::pending_commit` here and merged into
    /// the snapshot's summary properties inside `commit_pending` (called from
    /// `flush`). Both the data files and the token properties land in the same
    /// `Transaction::fast_append` commit, so they are atomic by Iceberg's
    /// optimistic-concurrency guarantee.
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let n = self.write_batch(records).await?;
        let mut state = self.state.lock().await;
        state.pending_commit = Some((scope.to_string(), token.to_string()));
        Ok(n)
    }

    /// Return the last commit token recorded for `scope` in this table's
    /// snapshot history, or `None` if no token has been committed yet.
    ///
    /// Snapshots are scanned newest-first; the first snapshot whose
    /// `faucet.commit-scope` property matches `scope` wins.  If the table does
    /// not yet exist `Ok(None)` is returned immediately.
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        let table = match self.load_table_readonly().await? {
            Some(t) => t,
            None => return Ok(None),
        };

        let meta = table.metadata();
        let mut snaps: Vec<_> = meta.snapshots().collect();
        // Sort newest-first so we return the most recent matching token.
        snaps.sort_by_key(|s| std::cmp::Reverse(s.timestamp_ms()));

        for snap in snaps {
            let summary = snap.summary();
            if summary
                .additional_properties
                .get(faucet_core::idempotency::ICEBERG_SCOPE_PROP)
                .map(String::as_str)
                == Some(scope)
            {
                return Ok(summary
                    .additional_properties
                    .get(faucet_core::idempotency::ICEBERG_TOKEN_PROP)
                    .cloned());
            }
        }

        Ok(None)
    }

    /// Write a batch of records to the Iceberg table.
    ///
    /// When `config.batch_size > 0` the records are re-chunked before writing.
    /// `batch_size = 0` passes the entire page through as a single chunk.
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut state = self.state.lock().await;

        let chunk_size = self.config.batch_size;
        let mut total = 0usize;

        if chunk_size == 0 || records.len() <= chunk_size {
            total += self.write_chunk(&mut state, records).await?;
        } else {
            for chunk in records.chunks(chunk_size) {
                total += self.write_chunk(&mut state, chunk).await?;
            }
        }

        tracing::debug!(
            table = %self.config.table,
            rows = total,
            "iceberg: write_batch complete"
        );

        Ok(total)
    }

    /// Flush buffered data files to a committed Iceberg snapshot.
    ///
    /// 1. Close the open writer (if any) → collect `DataFile`s.
    /// 2. If no `DataFile`s accumulated → **no-op** (empty snapshot not committed).
    /// 3. Commit via `Transaction::fast_append`.
    async fn flush(&self) -> Result<(), FaucetError> {
        let mut state = self.state.lock().await;

        // Step 1: close the open writer and collect its data files.
        Self::close_writer(&mut state).await?;

        // Step 2 + 3: commit pending files (or no-op when empty).
        self.commit_pending(&mut state).await?;

        tracing::debug!(table = %self.config.table, "iceberg: flush complete");
        Ok(())
    }

    /// Write records, returning a per-row outcome for DLQ routing.
    ///
    /// Individual rows that fail Arrow conversion (type mismatch against the
    /// table schema) become `Err(FaucetError::Sink(...))` outcomes so the DLQ
    /// router can quarantine them without aborting the batch.
    ///
    /// A writer or commit failure (transport-level) fails the whole call as an
    /// outer `Err` because no rows are committed in that case.
    ///
    /// Note: unlike BigQuery's `skipInvalidRows` API, the iceberg writer
    /// processes a whole `RecordBatch` atomically — the only granularity at
    /// which we can surface per-row errors is the JSON→Arrow conversion step.
    /// Rows that fail that step are routed to DLQ; the remainder are written
    /// as a batch.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let mut state = self.state.lock().await;

        // Resolve table (and potentially create it) from the full record set so
        // schema inference uses all records.
        let table = self.resolve_table(&mut state, records).await?;
        let arrow_schema = iceberg_to_arrow_schema(table.metadata().current_schema())?;

        // Try to convert each record individually so we can give per-row errors.
        let mut outcomes: Vec<faucet_core::RowOutcome> = Vec::with_capacity(records.len());
        let mut good_records: Vec<Value> = Vec::with_capacity(records.len());
        let mut good_indices: Vec<usize> = Vec::with_capacity(records.len());

        for (i, record) in records.iter().enumerate() {
            match json_to_record_batch(std::slice::from_ref(record), &arrow_schema) {
                Ok(_) => {
                    good_records.push(record.clone());
                    good_indices.push(i);
                    // We'll fill in Ok(()) after the batch write succeeds.
                    outcomes.push(Ok(()));
                }
                Err(e) => {
                    outcomes.push(Err(FaucetError::Sink(format!(
                        "iceberg: row {i} failed Arrow conversion: {e}"
                    ))));
                }
            }
        }

        // Write the good records as a batch (outer-Err on transport failure).
        if !good_records.is_empty() {
            self.ensure_writer(&mut state, &table).await?;
            let batch = json_to_record_batch(&good_records, &arrow_schema)?;
            let writer = state.writer.as_mut().expect("writer set above");
            writer.write(batch).await?;
        }

        Ok(outcomes)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::FaucetError;

    // dataset_uri test is skipped: IcebergSink::new() requires a live catalog
    // connection (build_catalog in new()), and no offline constructor exists.

    fn minimal_config() -> IcebergSinkConfig {
        serde_json::from_value(serde_json::json!({
            "catalog": { "type": "rest", "uri": "http://localhost:8181" },
            "namespace": ["analytics"],
            "table": "events",
            "create_if_missing": true
        }))
        .unwrap()
    }

    // Verify that attempting to build a sink with a disabled catalog type
    // (Glue, SQL, HMS in default feature set) returns a Config error, not a
    // panic. This tests the `new()` code path without network.
    #[cfg(not(feature = "catalog-glue"))]
    #[tokio::test]
    async fn new_with_disabled_catalog_returns_config_error() {
        let config: IcebergSinkConfig = serde_json::from_value(serde_json::json!({
            "catalog": { "type": "glue", "warehouse": "s3://lake/wh" },
            "namespace": ["analytics"],
            "table": "events"
        }))
        .unwrap();

        let err = IcebergSink::new(config).await.unwrap_err();
        assert!(
            matches!(err, FaucetError::Config(_)),
            "expected Config error, got {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("catalog-glue"),
            "should mention the missing feature: {msg}"
        );
    }

    // Verify validate() catches an empty namespace before catalog init.
    #[tokio::test]
    async fn new_rejects_empty_namespace() {
        let config: IcebergSinkConfig = serde_json::from_value(serde_json::json!({
            "catalog": { "type": "rest", "uri": "http://localhost:8181" },
            "namespace": [],
            "table": "events"
        }))
        .unwrap();

        let err = IcebergSink::new(config).await.unwrap_err();
        assert!(
            matches!(err, FaucetError::Config(_)),
            "expected Config error, got {err:?}"
        );
    }

    // Verify validate() catches an empty table name.
    #[tokio::test]
    async fn new_rejects_empty_table_name() {
        let config: IcebergSinkConfig = serde_json::from_value(serde_json::json!({
            "catalog": { "type": "rest", "uri": "http://localhost:8181" },
            "namespace": ["ns"],
            "table": ""
        }))
        .unwrap();

        let err = IcebergSink::new(config).await.unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    // The compression helper is already tested in writer.rs; this test
    // ensures config→sink uses the right codec label.
    #[test]
    fn default_compression_parses_ok() {
        let cfg = minimal_config();
        assert_eq!(cfg.parquet.compression, "snappy");
        assert!(crate::writer::compression_from_str("snappy").is_ok());
    }

    // Verify the partition spec builder returns None on an empty spec.
    #[test]
    fn build_partition_spec_empty_returns_none() {
        use crate::schema::{arrow_to_iceberg_schema, infer_arrow_schema};
        use serde_json::json;

        let records = vec![json!({"id": 1, "name": "alice"})];
        let arrow_schema = infer_arrow_schema(&records, 10).unwrap();
        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema).unwrap();

        let result = IcebergSink::build_partition_spec(&[], &iceberg_schema).unwrap();
        assert!(result.is_none(), "empty partition_spec should yield None");
    }

    // Verify the partition spec builder catches an unknown source column.
    #[test]
    fn build_partition_spec_unknown_column_errors() {
        use crate::schema::{arrow_to_iceberg_schema, infer_arrow_schema};
        use serde_json::json;

        let records = vec![json!({"id": 1})];
        let arrow_schema = infer_arrow_schema(&records, 10).unwrap();
        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema).unwrap();

        let pfs = vec![PartitionField {
            source: "nonexistent_col".to_string(),
            transform: "identity".to_string(),
        }];

        let err = IcebergSink::build_partition_spec(&pfs, &iceberg_schema).unwrap_err();
        assert!(
            matches!(err, FaucetError::Config(_)),
            "unknown column should give Config error: {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("nonexistent_col"),
            "error should name the bad column: {msg}"
        );
    }

    // Verify the partition spec builder succeeds on a valid identity field.
    #[test]
    fn build_partition_spec_identity_succeeds() {
        use crate::schema::{arrow_to_iceberg_schema, infer_arrow_schema};
        use serde_json::json;

        let records = vec![json!({"id": 1, "ts": "2024-01-01"})];
        let arrow_schema = infer_arrow_schema(&records, 10).unwrap();
        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema).unwrap();

        let pfs = vec![PartitionField {
            source: "id".to_string(),
            transform: "identity".to_string(),
        }];

        let result = IcebergSink::build_partition_spec(&pfs, &iceberg_schema).unwrap();
        assert!(result.is_some(), "should produce a partition spec");
    }
}
