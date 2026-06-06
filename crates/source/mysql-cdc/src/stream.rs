//! `MysqlCdcSource` — public `Source` implementation.
//!
//! Tails the MySQL binary log via `mysql_async`'s async [`BinlogStream`] and
//! emits per-row change events as CDC envelopes.  Transactions are buffered
//! in memory (BEGIN → ROWS → COMMIT) and emitted atomically as one
//! [`StreamPage`] per commit, matching the postgres-cdc durability contract.
//!
//! **Target engine:** primarily InnoDB / transactional tables, where commits
//! arrive as `XidEvent`.  Explicit `COMMIT` statements (emitted by
//! non-transactional / mixed-engine workloads as `QueryEvent("COMMIT")`) are
//! also handled as commit boundaries with identical durability semantics.
//!
//! **Bookmark strategy:** all persisted bookmarks use `{file, pos}` (the
//! end-position of the commit event).  Even when `start_position` is
//! `GtidSet`, the session resume is via file/pos after the first commit.
//! Assembling the full executed-GTID set from raw `GtidEvent` messages
//! across multiple sessions is fiddly (needs SID→interval accumulation
//! across runs), whereas file/pos is always available, unambiguous, and
//! fully resumable — the server still honours `gtid_mode` ordering
//! guarantees on the other side.  This choice is documented in the crate
//! README.

use crate::config::{CdcTls, MysqlCdcSourceConfig, StartPosition};
use crate::convert::binlog_row_to_json;
use crate::state::{Bookmark, state_key};
use async_trait::async_trait;
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use mysql_async::binlog::events::{EventData, RowsEventData};
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStreamRequest, Conn, Opts, OptsBuilder, Row, SslOpts};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::sync::Mutex;

/// A configured MySQL CDC (binlog replication) source.
///
/// Bookmarks are file/pos coordinates — see module-level note for the
/// rationale behind the always-file/pos bookmark strategy.
pub struct MysqlCdcSource {
    config: MysqlCdcSourceConfig,
    opts: Opts,
    state_key_value: String,
    /// Bookmark provided by `apply_start_bookmark`, applied at the start of
    /// the next fetch cycle to skip already-consumed events.
    pending_bookmark: Mutex<Option<Bookmark>>,
}

impl MysqlCdcSource {
    /// Build and preflight-check the source.
    ///
    /// Runs `config.validate()`, builds TLS-aware `Opts`, then opens a
    /// throwaway connection to verify binlog variables and user grants.
    pub async fn new(config: MysqlCdcSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;

        let opts = build_opts(&config)?;
        let key = state_key(config.server_id);

        // Preflight: open + query + drop a throwaway connection.
        let mut conn = Conn::new(opts.clone())
            .await
            .map_err(|e| FaucetError::Source(format!("mysql-cdc: cannot connect: {e}")))?;

        run_preflight(&mut conn, &config).await?;
        drop(conn);

        Ok(Self {
            config,
            opts,
            state_key_value: key,
            pending_bookmark: Mutex::new(None),
        })
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Source impl
// ──────────────────────────────────────────────────────────────────────────────

#[async_trait]
impl Source for MysqlCdcSource {
    /// Drain the stream using the per-source `batch_size = 0` sentinel so
    /// all transactions are accumulated into a single trailing page —
    /// matching the historical convenience API contract.
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut pages = self.stream_pages_impl(ctx, 0);
        let mut all = Vec::new();
        while let Some(page) = pages.next().await {
            all.extend(page?.records);
        }
        Ok(all)
    }

    /// Per-transaction streaming.  Each committed transaction is emitted as
    /// its own [`StreamPage`] with `bookmark = Some(file_pos)`.  The
    /// trait-level `batch_size` argument is ignored in favour of the config
    /// field.
    fn stream_pages<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        self.stream_pages_impl(ctx, self.config.batch_size)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(schemars::schema_for!(MysqlCdcSourceConfig)).unwrap_or(Value::Null)
    }

    fn state_key(&self) -> Option<String> {
        Some(self.state_key_value.clone())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        let b = Bookmark::from_value(bookmark)?;
        *self.pending_bookmark.lock().await = Some(b);
        Ok(())
    }

    fn connector_name(&self) -> &'static str {
        "mysql-cdc"
    }

    /// Preflight probe that does **not** open the binlog stream.
    ///
    /// Runs two probes bounded by `ctx.timeout`:
    /// - `connection`: can we connect + authenticate?
    /// - `binlog-config`: are the required server variables set?
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let start = std::time::Instant::now();

        let probe_result = tokio::time::timeout(ctx.timeout, async {
            let mut conn = Conn::new(self.opts.clone()).await.map_err(|e| {
                Probe::fail_hint(
                    "connection",
                    start.elapsed(),
                    format!("could not connect: {e}"),
                    "verify the host is reachable and credentials are valid",
                )
            })?;

            let connection = Probe::pass("connection", start.elapsed());

            let binlog_config = match run_preflight_probes(&mut conn, &self.config).await {
                Ok(_summary) => Probe::pass("binlog-config", start.elapsed()),
                Err(msg) => Probe::fail_hint(
                    "binlog-config",
                    start.elapsed(),
                    msg,
                    "Set binlog_format=ROW, binlog_row_image=FULL, binlog_row_metadata=FULL \
                     and grant REPLICATION SLAVE + REPLICATION CLIENT",
                ),
            };

            Ok::<(Probe, Probe), Probe>((connection, binlog_config))
        })
        .await;

        match probe_result {
            Ok(Ok((conn_probe, cfg_probe))) => Ok(CheckReport {
                probes: vec![conn_probe, cfg_probe],
            }),
            Ok(Err(probe)) => Ok(CheckReport::single(probe)),
            Err(_elapsed) => Ok(CheckReport::single(Probe::fail_hint(
                "connection",
                start.elapsed(),
                "connection timed out",
                "the database did not respond within the check timeout",
            ))),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Stream loop
// ──────────────────────────────────────────────────────────────────────────────

impl MysqlCdcSource {
    fn stream_pages_impl<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let idle_timeout = self.config.idle_timeout;
        let per_transaction = batch_size != 0;

        Box::pin(async_stream::try_stream! {
            use futures::StreamExt;

            // 1. Resolve start position for this fetch cycle.
            let pending = self.pending_bookmark.lock().await.take();
            let resolved = resolve_start(&self.config.start_position, pending.as_ref());

            // 2. Open a connection and build the binlog stream request.
            let mut conn = Conn::new(self.opts.clone())
                .await
                .map_err(|e| FaucetError::Source(format!("mysql-cdc: connect failed: {e}")))?;

            // Resolve Current → FilePos by querying SHOW MASTER STATUS (fills in the
            // actual file/pos before we build the request that borrows from `resolved`).
            let resolved = resolve_current(resolved, &mut conn).await?;
            let req = build_request(self.config.server_id, &resolved)?;

            // 3. Start the binlog stream.
            let binlog_stream = conn
                .get_binlog_stream(req)
                .await
                .map_err(|e| FaucetError::Source(format!("mysql-cdc: get_binlog_stream: {e}")))?;
            let mut stream = std::pin::pin!(binlog_stream);

            // 4. Per-event tracking state.
            let mut current_file = match &resolved {
                ResolvedStart::FilePos { file, .. } => file.clone(),
                _ => String::new(),
            };
            let mut buffer: Vec<Value> = Vec::new();
            let mut in_txn = false;
            let mut txid: u64 = 0;
            // Bookmark of the last successfully committed transaction.
            let mut last_commit_bookmark: Option<Bookmark> = None;
            // Aggregate buffer used when batch_size == 0.
            let mut agg_records: Vec<Value> = Vec::new();

            // commit_buffer!($bm) — factors out the emit/accumulate logic shared by
            // XidEvent (InnoDB), QueryEvent("COMMIT") (non-transactional/mixed), and
            // the desync-guard flush that runs when a new transaction starts while the
            // buffer is unexpectedly non-empty.
            //
            // The macro does NOT update `in_txn` or `txid`; the caller is responsible
            // for those so that desync-guard callers (which immediately set `in_txn =
            // true` afterward) don't trigger a dead-assignment lint.
            //
            // Behaviour:
            //  - per_transaction mode: yields one StreamPage per commit (even if the
            //    buffer is empty, so the bookmark still advances).
            //  - aggregate mode (batch_size == 0): extends agg_records and records the
            //    bookmark for the trailing flush at stream end; skips empty buffers.
            //
            // Must be a macro (not a closure) because it needs to `yield` inside the
            // async_stream::try_stream! block and capture locals by mutable reference.
            macro_rules! commit_buffer {
                ($bm:expr) => {{
                    let bm: Bookmark = $bm;
                    if per_transaction {
                        // Always yield — even an empty page advances the bookmark.
                        yield StreamPage {
                            records: std::mem::take(&mut buffer),
                            bookmark: Some(bm.to_value()?),
                        };
                    } else if !buffer.is_empty() {
                        // Aggregate mode: accumulate; last_commit_bookmark records
                        // the bookmark for the trailing flush at stream end.
                        // Only guard assignment in aggregate mode — per_transaction
                        // already yields the bookmark directly.
                        last_commit_bookmark = Some(bm);
                        agg_records.extend(std::mem::take(&mut buffer));
                    }
                }};
            }

            // 5. Drain loop.
            loop {
                match tokio::time::timeout(idle_timeout, stream.next()).await {
                    Ok(Some(Ok(event))) => {
                        let header = event.header();
                        let ts_ms = u64::from(header.timestamp()) * 1_000;
                        let log_pos = u64::from(header.log_pos());

                        let event_data = event
                            .read_data()
                            .map_err(|e| FaucetError::Source(format!(
                                "mysql-cdc: read_data failed: {e}"
                            )))?;

                        match event_data {
                            Some(EventData::RotateEvent(re)) => {
                                current_file = re.name().into_owned();
                            }
                            Some(EventData::GtidEvent(_g)) => {
                                // A GtidEvent precedes BEGIN in GTID mode.
                                // We don't need the SID/GNO here because we
                                // bookmark with file/pos on commit (see module note).
                                //
                                // Desync guard: if the buffer is non-empty when a new
                                // transaction starts, the previous transaction ended
                                // without an explicit commit boundary event — flush it
                                // now to prevent silent conflation into the next txid.
                                if !buffer.is_empty() {
                                    let bm = Bookmark::FilePos {
                                        file: current_file.clone(),
                                        pos: log_pos,
                                    };
                                    commit_buffer!(bm);
                                    txid = txid.wrapping_add(1);
                                }
                                in_txn = true;
                            }
                            Some(EventData::QueryEvent(qe)) => {
                                let q = qe.query();
                                let q_upper = q.trim().to_ascii_uppercase();
                                if q_upper == "BEGIN" {
                                    // Desync guard: same reasoning as GtidEvent.
                                    if !buffer.is_empty() {
                                        let bm = Bookmark::FilePos {
                                            file: current_file.clone(),
                                            pos: log_pos,
                                        };
                                        commit_buffer!(bm);
                                        txid = txid.wrapping_add(1);
                                    }
                                    in_txn = true;
                                } else if q_upper == "COMMIT" {
                                    // Non-transactional / mixed-engine explicit COMMIT.
                                    // MySQL emits QueryEvent("COMMIT") instead of XidEvent
                                    // for non-InnoDB engines; treat it identically.
                                    let bm = Bookmark::FilePos {
                                        file: current_file.clone(),
                                        pos: log_pos,
                                    };
                                    commit_buffer!(bm);
                                    in_txn = false;
                                    txid = txid.wrapping_add(1);
                                } else {
                                    // DDL statement — auto-commits implicitly.
                                    if self.config.emit_schema_changes {
                                        let envelope = build_ddl_envelope(
                                            q.as_ref(),
                                            ts_ms,
                                            &current_file,
                                            log_pos,
                                        );
                                        let bm = Bookmark::FilePos {
                                            file: current_file.clone(),
                                            pos: log_pos,
                                        };
                                        if per_transaction {
                                            yield StreamPage {
                                                records: vec![envelope],
                                                bookmark: Some(bm.to_value()?),
                                            };
                                        } else {
                                            last_commit_bookmark = Some(bm);
                                            agg_records.push(envelope);
                                        }
                                    }
                                    in_txn = false;
                                }
                            }
                            Some(EventData::RowsEvent(re)) => {
                                let table_id = re.table_id();
                                let tme = stream
                                    .get_tme(table_id)
                                    .ok_or_else(|| FaucetError::Source(format!(
                                        "mysql-cdc: missing TableMapEvent for table_id={table_id}"
                                    )))?;

                                let db = tme.database_name().into_owned();
                                let table = tme.table_name().into_owned();

                                if !self.config.table_included(&db, &table) {
                                    // Skip filtered tables but still mark as in_txn.
                                    continue;
                                }

                                let op = op_from_rows_event(&re);
                                let lsn = json!({ "file": &current_file, "pos": log_pos });

                                for row_result in re.rows(tme) {
                                    let (before_row, after_row) = row_result.map_err(|e| {
                                        FaucetError::Source(format!(
                                            "mysql-cdc: row decode error: {e}"
                                        ))
                                    })?;

                                    let before_json = if self.config.include_columns {
                                        match &before_row {
                                            Some(r) => binlog_row_to_json(r)?,
                                            None => Value::Null,
                                        }
                                    } else {
                                        Value::Null
                                    };
                                    let after_json = match &after_row {
                                        Some(r) => binlog_row_to_json(r)?,
                                        None => Value::Null,
                                    };

                                    let envelope = build_envelope(
                                        op, ts_ms, &db, &table,
                                        before_json, after_json,
                                        lsn.clone(), txid,
                                    );

                                    // Fix 4: use let-chain (stable in edition 2024).
                                    if let Some(max) = self.config.max_staged_records
                                        && buffer.len() >= max
                                    {
                                        Err(FaucetError::Source(format!(
                                            "mysql-cdc: in-progress transaction exceeded \
                                             max_staged_records ({max}); aborting to avoid \
                                             unbounded memory growth. Raise \
                                             max_staged_records or split the source transaction."
                                        )))?;
                                    }
                                    buffer.push(envelope);
                                }
                            }
                            Some(EventData::XidEvent(_xid)) => {
                                // InnoDB COMMIT — emit the buffered transaction.
                                let bm = Bookmark::FilePos {
                                    file: current_file.clone(),
                                    pos: log_pos,
                                };
                                commit_buffer!(bm);
                                in_txn = false;
                                txid = txid.wrapping_add(1);
                            }
                            _ => {
                                // FormatDescriptionEvent, PreviousGtidsEvent, etc. — ignored.
                            }
                        }
                    }
                    Ok(Some(Err(e))) => {
                        Err(FaucetError::Source(format!("mysql-cdc: stream error: {e}")))?;
                    }
                    // Idle timeout or stream closed: flush remaining buffer and end.
                    Ok(None) | Err(_) => {
                        // Drop any uncommitted partial transaction — the server will
                        // redeliver it from the last persisted bookmark on the next run.
                        let _ = in_txn;

                        // Aggregate mode: emit one trailing page with all accumulated
                        // records across the fetch window.  If every transaction was
                        // filtered (all rows excluded by table_included), agg_records
                        // stays empty and last_commit_bookmark is None, so the bookmark
                        // is not advanced — those transactions are harmlessly re-scanned
                        // on the next cycle (aggregate mode is for test/snapshot use only).
                        if !per_transaction
                            && let Some(bm) = last_commit_bookmark.take()
                            && !agg_records.is_empty()
                        {
                            yield StreamPage {
                                records: std::mem::take(&mut agg_records),
                                bookmark: Some(bm.to_value()?),
                            };
                        }
                        break;
                    }
                }
            }

            tracing::info!(
                connector = "mysql-cdc",
                server_id = self.config.server_id,
                "binlog fetch cycle complete",
            );
        })
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers (pure, unit-testable)
// ──────────────────────────────────────────────────────────────────────────────

/// Resolved start position for a single fetch cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolvedStart {
    /// Start at the server's current position (fresh run, no history).
    Current { file: String, pos: u64 },
    /// Start at the oldest available binlog (errors if purged).
    Earliest,
    /// Resume from a persisted file/pos bookmark.
    FilePos { file: String, pos: u64 },
    /// Start after a specific GTID set.  The string is parsed into `Sid`s
    /// for the `BinlogStreamRequest`.
    GtidSet { value: String },
}

/// Determine the effective start for this fetch cycle.
///
/// **Precedence:** a persisted bookmark (set via `apply_start_bookmark`) always
/// wins over the config's `start_position` — this is the CDC durability
/// invariant: we only advance past a position once the pipeline has persisted
/// the bookmark downstream.
///
/// - `FilePos` bookmark → `ResolvedStart::FilePos`
/// - `GtidSet` bookmark (from a previous session that used GTID start) →
///   treated as `FilePos` since all our bookmarks are file/pos after the first commit.
///
/// Note: all persisted bookmarks are `Bookmark::FilePos` (see module-level
/// note on the bookmark strategy), so the `GtidSet` arm is defensive.
pub(crate) fn resolve_start(
    start_position: &StartPosition,
    pending: Option<&Bookmark>,
) -> ResolvedStart {
    if let Some(bm) = pending {
        // A persisted bookmark always wins.
        return match bm {
            Bookmark::FilePos { file, pos } => ResolvedStart::FilePos {
                file: file.clone(),
                pos: *pos,
            },
            // Defensive: a GtidSet bookmark from a previous session that
            // DID persist GTID coordinates — start from the GTID set.
            Bookmark::GtidSet { gtid_set } => ResolvedStart::GtidSet {
                value: gtid_set.clone(),
            },
        };
    }

    // No persisted bookmark — use the config.
    match start_position {
        StartPosition::Current => {
            // Placeholder; real file/pos filled in by `build_request` via SHOW MASTER STATUS.
            ResolvedStart::Current {
                file: String::new(),
                pos: 0,
            }
        }
        StartPosition::Earliest => ResolvedStart::Earliest,
        StartPosition::FilePos { file, pos } => ResolvedStart::FilePos {
            file: file.clone(),
            pos: *pos,
        },
        StartPosition::GtidSet { value } => ResolvedStart::GtidSet {
            value: value.clone(),
        },
    }
}

/// If `resolved` is `Current`, query the server for its current binlog
/// position and return a `FilePos` variant with the real coordinates.
/// All other variants pass through unchanged.
///
/// Splitting this out of `build_request` ensures `resolved` is fully owned
/// before we build a request that borrows from it, avoiding the need to leak
/// any byte buffers.
async fn resolve_current(
    resolved: ResolvedStart,
    conn: &mut Conn,
) -> Result<ResolvedStart, FaucetError> {
    if !matches!(resolved, ResolvedStart::Current { .. }) {
        return Ok(resolved);
    }
    let row: Option<Row> = conn
        .query_first("SHOW MASTER STATUS")
        .await
        .map_err(|e| {
            FaucetError::Source(format!("mysql-cdc: SHOW MASTER STATUS failed: {e}"))
        })?;
    let row = row.ok_or_else(|| {
        FaucetError::Source(
            "mysql-cdc: SHOW MASTER STATUS returned no rows; \
             is binary logging enabled?"
                .into(),
        )
    })?;
    let file: String = row.get(0).ok_or_else(|| {
        FaucetError::Source("mysql-cdc: SHOW MASTER STATUS: missing File column".into())
    })?;
    let pos: u64 = row.get(1).ok_or_else(|| {
        FaucetError::Source("mysql-cdc: SHOW MASTER STATUS: missing Position column".into())
    })?;
    Ok(ResolvedStart::FilePos { file, pos })
}

/// Build a `BinlogStreamRequest` from the resolved start position.
///
/// No heap memory is leaked: filenames borrow from `resolved` (which the
/// caller holds for the duration of the call), and GTID SIDs are parsed into
/// fully-owned data structures — `Sid::from_str` produces `Sid<'static>`
/// because all internal fields (`Seq` / `Tag`) are stored as `Cow::Owned`.
fn build_request<'r>(
    server_id: u32,
    resolved: &'r ResolvedStart,
) -> Result<BinlogStreamRequest<'r>, FaucetError> {
    use mysql_async::Sid;
    use std::str::FromStr;

    match resolved {
        // resolve_current() converts Current → FilePos before we get here;
        // this arm is only hit if a caller skips that step (defensive).
        ResolvedStart::Current { .. } => {
            Ok(BinlogStreamRequest::new(server_id))
        }
        ResolvedStart::Earliest => {
            // No filename/pos — server starts from the oldest available binlog.
            Ok(BinlogStreamRequest::new(server_id))
        }
        ResolvedStart::FilePos { file, pos } => {
            // Borrow the filename bytes directly from the owned `String` in
            // `resolved` — no copy, no leak.
            Ok(BinlogStreamRequest::new(server_id)
                .with_filename(file.as_bytes())
                .with_pos(*pos))
        }
        ResolvedStart::GtidSet { value } => {
            // `Sid::from_str` parses into fully-owned data (intervals as
            // `Cow::Owned`, tag as `Tag<'static>` via `to_owned()`), so the
            // resulting `Sid<'static>` does not borrow the input string.
            // No leaking required.
            let sids: Vec<Sid<'static>> = value
                .split(',')
                .map(|part| {
                    let trimmed = part.trim();
                    Sid::from_str(trimmed).map_err(|e| {
                        FaucetError::Source(format!(
                            "mysql-cdc: invalid GTID set '{trimmed}': {e}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(BinlogStreamRequest::new(server_id)
                .with_gtid()
                .with_gtid_set(sids))
        }
    }
}

/// Map a `RowsEventData` variant to its CDC operation string.
pub(crate) fn op_from_rows_event(re: &RowsEventData<'_>) -> &'static str {
    match re {
        RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => "c",
        RowsEventData::UpdateRowsEvent(_)
        | RowsEventData::UpdateRowsEventV1(_)
        | RowsEventData::PartialUpdateRowsEvent(_) => "u",
        RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => "d",
    }
}

/// Build a CDC change-event envelope.
///
/// ```json
/// { "op": "c", "ts_ms": 1234, "schema": "mydb", "table": "users",
///   "before": null, "after": {"id": 1, "name": "alice"},
///   "lsn": {"file": "binlog.000001", "pos": 4567}, "txid": 0 }
/// ```
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_envelope(
    op: &str,
    ts_ms: u64,
    schema: &str,
    table: &str,
    before: Value,
    after: Value,
    lsn: Value,
    txid: u64,
) -> Value {
    let mut obj = Map::new();
    obj.insert("op".into(), json!(op));
    obj.insert("ts_ms".into(), json!(ts_ms));
    obj.insert("schema".into(), json!(schema));
    obj.insert("table".into(), json!(table));
    obj.insert("before".into(), before);
    obj.insert("after".into(), after);
    obj.insert("lsn".into(), lsn);
    obj.insert("txid".into(), json!(txid));
    Value::Object(obj)
}

/// Build a DDL change-event envelope.
fn build_ddl_envelope(statement: &str, ts_ms: u64, file: &str, pos: u64) -> Value {
    json!({
        "op": "ddl",
        "ts_ms": ts_ms,
        "statement": statement,
        "lsn": { "file": file, "pos": pos },
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// TLS + Opts construction
// ──────────────────────────────────────────────────────────────────────────────

fn build_opts(config: &MysqlCdcSourceConfig) -> Result<Opts, FaucetError> {
    let base = Opts::from_url(&config.connection_url)
        .map_err(|e| FaucetError::Config(format!("mysql-cdc: invalid connection URL: {e}")))?;

    let ssl = match &config.tls {
        CdcTls::Disable => return Ok(base),
        CdcTls::Require => SslOpts::default()
            .with_danger_accept_invalid_certs(true)
            .with_danger_skip_domain_validation(true),
        CdcTls::VerifyCa { ca_path } => {
            let mut s = SslOpts::default().with_danger_skip_domain_validation(true);
            if let Some(p) = ca_path {
                s = s.with_root_certs(vec![PathBuf::from(p).into()]);
            }
            s
        }
        CdcTls::VerifyFull { ca_path } => {
            let mut s = SslOpts::default();
            if let Some(p) = ca_path {
                s = s.with_root_certs(vec![PathBuf::from(p).into()]);
            }
            s
        }
    };

    Ok(OptsBuilder::from_opts(base).ssl_opts(ssl).into())
}

// ──────────────────────────────────────────────────────────────────────────────
// Preflight helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Run preflight checks and return a human-readable summary on success, or an
/// error message on the first failing check.
async fn run_preflight_probes(
    conn: &mut Conn,
    config: &MysqlCdcSourceConfig,
) -> Result<String, String> {
    // Check binlog_format = ROW
    let fmt: Option<(String, String)> = conn
        .query_first("SHOW VARIABLES LIKE 'binlog_format'")
        .await
        .map_err(|e| format!("SHOW VARIABLES LIKE 'binlog_format' failed: {e}"))?;
    match fmt.as_ref() {
        Some((_, v)) if !v.eq_ignore_ascii_case("ROW") => {
            return Err(format!(
                "binlog_format is '{v}', must be ROW. \
                 Set binlog_format=ROW in your MySQL config."
            ));
        }
        None => {
            return Err(
                "binlog_format variable not found. Is binary logging enabled?".into(),
            );
        }
        _ => {}
    }

    // Check binlog_row_image = FULL
    let img: Option<(String, String)> = conn
        .query_first("SHOW VARIABLES LIKE 'binlog_row_image'")
        .await
        .map_err(|e| format!("SHOW VARIABLES LIKE 'binlog_row_image' failed: {e}"))?;
    match img.as_ref() {
        Some((_, v)) if !v.eq_ignore_ascii_case("FULL") => {
            return Err(format!(
                "binlog_row_image is '{v}', must be FULL. \
                 Set binlog_row_image=FULL in your MySQL config."
            ));
        }
        None => {
            return Err("binlog_row_image variable not found.".into());
        }
        _ => {}
    }

    // Check binlog_row_metadata = FULL (required for column names)
    let meta: Option<(String, String)> = conn
        .query_first("SHOW VARIABLES LIKE 'binlog_row_metadata'")
        .await
        .map_err(|e| format!("SHOW VARIABLES LIKE 'binlog_row_metadata' failed: {e}"))?;
    match meta.as_ref() {
        Some((_, v)) if !v.eq_ignore_ascii_case("FULL") => {
            return Err(format!(
                "binlog_row_metadata is '{v}', must be FULL. \
                 Set binlog_row_metadata=FULL in your MySQL config."
            ));
        }
        None => {
            return Err("binlog_row_metadata variable not found.".into());
        }
        _ => {}
    }

    // Check REPLICATION grants
    let grants: Vec<String> = conn
        .query("SHOW GRANTS FOR CURRENT_USER()")
        .await
        .map_err(|e| format!("SHOW GRANTS failed: {e}"))?;
    let grants_combined = grants.join(" ").to_uppercase();
    let has_replication = grants_combined.contains("ALL PRIVILEGES")
        || (grants_combined.contains("REPLICATION SLAVE")
            && grants_combined.contains("REPLICATION CLIENT"));
    if !has_replication {
        return Err(
            "user lacks REPLICATION SLAVE and/or REPLICATION CLIENT privileges. \
             Grant them with: GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'host';"
                .into(),
        );
    }

    // If start_position is GtidSet, gtid_mode must be ON. Checked here (rather
    // than only in `new()`) so `faucet doctor`'s binlog-config probe catches it
    // too.
    if matches!(config.start_position, StartPosition::GtidSet { .. }) {
        let gtid: Option<(String, String)> = conn
            .query_first("SHOW VARIABLES LIKE 'gtid_mode'")
            .await
            .map_err(|e| format!("SHOW VARIABLES LIKE 'gtid_mode' failed: {e}"))?;
        match gtid.as_ref() {
            Some((_, v)) if !v.eq_ignore_ascii_case("ON") => {
                return Err(format!(
                    "start_position is GtidSet but gtid_mode is '{v}' (must be ON). \
                     Enable GTID mode: --gtid-mode=ON --enforce-gtid-consistency=ON"
                ));
            }
            None => {
                return Err("gtid_mode variable not found".into());
            }
            _ => {}
        }
    }

    Ok("binlog_format=ROW, binlog_row_image=FULL, binlog_row_metadata=FULL, grants OK".into())
}

/// Run preflight checks, mapping a failure to a typed `FaucetError::Source`.
async fn run_preflight(conn: &mut Conn, config: &MysqlCdcSourceConfig) -> Result<(), FaucetError> {
    run_preflight_probes(conn, config)
        .await
        .map(|_| ())
        .map_err(|m| FaucetError::Source(format!("mysql-cdc: {m}")))
}

// ──────────────────────────────────────────────────────────────────────────────
// Unit tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Bookmark;
    use serde_json::json;

    // ── resolve_start precedence ──────────────────────────────────────────────

    #[test]
    fn file_pos_bookmark_overrides_current() {
        let bm = Bookmark::FilePos {
            file: "binlog.000003".into(),
            pos: 4567,
        };
        let resolved = resolve_start(&StartPosition::Current, Some(&bm));
        assert_eq!(
            resolved,
            ResolvedStart::FilePos {
                file: "binlog.000003".into(),
                pos: 4567
            }
        );
    }

    #[test]
    fn file_pos_bookmark_overrides_gtid_config() {
        let bm = Bookmark::FilePos {
            file: "binlog.000010".into(),
            pos: 123,
        };
        let resolved = resolve_start(
            &StartPosition::GtidSet {
                value: "abc:1-100".into(),
            },
            Some(&bm),
        );
        assert_eq!(
            resolved,
            ResolvedStart::FilePos {
                file: "binlog.000010".into(),
                pos: 123
            }
        );
    }

    #[test]
    fn gtid_bookmark_overrides_current() {
        let bm = Bookmark::GtidSet {
            gtid_set: "abc:1-100".into(),
        };
        let resolved = resolve_start(&StartPosition::Current, Some(&bm));
        assert_eq!(
            resolved,
            ResolvedStart::GtidSet {
                value: "abc:1-100".into()
            }
        );
    }

    #[test]
    fn no_bookmark_current_yields_current() {
        let resolved = resolve_start(&StartPosition::Current, None);
        assert!(matches!(resolved, ResolvedStart::Current { .. }));
    }

    #[test]
    fn no_bookmark_earliest_yields_earliest() {
        let resolved = resolve_start(&StartPosition::Earliest, None);
        assert_eq!(resolved, ResolvedStart::Earliest);
    }

    #[test]
    fn no_bookmark_file_pos_config_passes_through() {
        let resolved = resolve_start(
            &StartPosition::FilePos {
                file: "binlog.000001".into(),
                pos: 4,
            },
            None,
        );
        assert_eq!(
            resolved,
            ResolvedStart::FilePos {
                file: "binlog.000001".into(),
                pos: 4
            }
        );
    }

    // ── op_from_rows_event ────────────────────────────────────────────────────
    //
    // `op_from_rows_event` maps a `RowsEventData` variant → "c"/"u"/"d". A
    // `RowsEventData` cannot be constructed without raw binlog bytes, so this
    // mapping is exercised end-to-end by the Docker integration test
    // (`tests/integration.rs`), which asserts an INSERT/UPDATE/DELETE produce
    // ops c/u/d. A unit test asserting string literals against themselves would
    // give false confidence, so it is intentionally omitted here.

    // ── envelope assembly ─────────────────────────────────────────────────────

    #[test]
    fn envelope_shape_insert() {
        let lsn = json!({ "file": "binlog.000001", "pos": 4567_u64 });
        let after = json!({ "id": 1, "name": "alice" });
        let env = build_envelope("c", 1_000, "mydb", "users", Value::Null, after.clone(), lsn.clone(), 0);

        assert_eq!(env["op"], "c");
        assert_eq!(env["ts_ms"], 1_000_u64);
        assert_eq!(env["schema"], "mydb");
        assert_eq!(env["table"], "users");
        assert_eq!(env["before"], Value::Null);
        assert_eq!(env["after"], after);
        assert_eq!(env["lsn"], lsn);
        assert_eq!(env["txid"], 0_u64);
    }

    #[test]
    fn envelope_shape_update() {
        let before = json!({ "id": 1, "name": "alice" });
        let after = json!({ "id": 1, "name": "bob" });
        let lsn = json!({ "file": "binlog.000002", "pos": 9999_u64 });
        let env = build_envelope("u", 2_000, "db", "tbl", before.clone(), after.clone(), lsn, 3);

        assert_eq!(env["op"], "u");
        assert_eq!(env["before"], before);
        assert_eq!(env["after"], after);
        assert_eq!(env["txid"], 3_u64);
    }

    #[test]
    fn envelope_shape_delete() {
        let before = json!({ "id": 42 });
        let lsn = json!({ "file": "binlog.000003", "pos": 100_u64 });
        let env = build_envelope("d", 3_000, "db", "tbl", before.clone(), Value::Null, lsn, 7);

        assert_eq!(env["op"], "d");
        assert_eq!(env["before"], before);
        assert_eq!(env["after"], Value::Null);
    }

    #[test]
    fn envelope_has_all_expected_keys() {
        let env = build_envelope(
            "c", 0, "s", "t", Value::Null, Value::Null,
            json!({ "file": "f", "pos": 0_u64 }), 0,
        );
        let obj = env.as_object().unwrap();
        for key in &["op", "ts_ms", "schema", "table", "before", "after", "lsn", "txid"] {
            assert!(obj.contains_key(*key), "missing key: {key}");
        }
    }

    // ── build_opts TLS ────────────────────────────────────────────────────────

    #[test]
    fn build_opts_disable_succeeds() {
        let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mysql://repl:pass@localhost:3306/db",
            "server_id": 1001
        }))
        .unwrap();
        assert!(build_opts(&config).is_ok());
    }

    #[test]
    fn build_opts_require_tls_succeeds() {
        let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mysql://repl:pass@localhost:3306/db",
            "server_id": 1002,
            "tls": { "mode": "require" }
        }))
        .unwrap();
        assert!(build_opts(&config).is_ok());
    }

    #[test]
    fn build_opts_verify_ca_no_path() {
        let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mysql://repl:pass@localhost:3306/db",
            "server_id": 1003,
            "tls": { "mode": "verify_ca" }
        }))
        .unwrap();
        assert!(build_opts(&config).is_ok());
    }

    #[test]
    fn build_opts_invalid_url_errors() {
        let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "not-a-valid-url",
            "server_id": 1
        }))
        .unwrap();
        assert!(build_opts(&config).is_err());
    }
}
