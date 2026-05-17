//! `PostgresCdcSource` — public `Source` implementation.

use crate::config::PostgresCdcSourceConfig;
use crate::pgoutput::decoder::decode_message;
use crate::pgoutput::messages::{
    Delete, Insert, Message, Relation, Truncate, TupleCell, TupleData, Update,
};
use crate::pgoutput::registry::RelationRegistry;
use crate::pgoutput::values::text_to_json;
use crate::replication::{
    self, ReplicationEvent, ReplicationParams, postgres_clock_to_unix_ms, recv, send_status_update,
};
use crate::state::{Bookmark, format_lsn, parse_lsn, state_key};
use async_trait::async_trait;
use faucet_core::{FaucetError, Source};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub struct PostgresCdcSource {
    config: PostgresCdcSourceConfig,
    state_key_value: String,
    /// Bookmark provided by `apply_start_bookmark`, applied at the start of
    /// the next fetch cycle. Becomes the new "confirmed_flush_lsn" we
    /// advertise to Postgres.
    pending_bookmark: Mutex<Option<Bookmark>>,
    /// Last LSN we have told Postgres is durable. Advanced by
    /// `apply_start_bookmark` (after the state store persists a bookmark)
    /// and by the end of `fetch_with_context_incremental` if any txn
    /// committed during the drain.
    confirmed_lsn: Mutex<u64>,
}

impl PostgresCdcSource {
    pub async fn new(config: PostgresCdcSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let key = state_key(&config.slot_name);
        let initial_lsn = match config.start_lsn.as_deref() {
            Some(s) => parse_lsn(s)?,
            None => 0,
        };
        Ok(Self {
            config,
            state_key_value: key,
            pending_bookmark: Mutex::new(None),
            confirmed_lsn: Mutex::new(initial_lsn),
        })
    }
}

#[async_trait]
impl Source for PostgresCdcSource {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (records, _bookmark) = self.fetch_with_context_incremental(ctx).await?;
        Ok(records)
    }

    async fn fetch_with_context_incremental(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        // 1. Resolve start_lsn for THIS fetch cycle.
        let bookmark = {
            let mut g = self.pending_bookmark.lock().await;
            g.take()
        };
        let start_lsn = if let Some(b) = bookmark.as_ref() {
            let lsn = b.as_u64()?;
            *self.confirmed_lsn.lock().await = lsn;
            Some(lsn)
        } else {
            self.config
                .start_lsn
                .as_deref()
                .map(parse_lsn)
                .transpose()?
        };

        // 2. Open replication connection + ensure slot + START_REPLICATION.
        let params = ReplicationParams {
            connection_url: &self.config.connection_url,
            slot_name: &self.config.slot_name,
            publication_name: &self.config.publication_name,
            proto_version: self.config.proto_version,
            create_slot_if_missing: self.config.create_slot_if_missing,
            start_lsn,
            status_update_interval: self.config.status_update_interval,
            tcp_keepalive: self.config.tcp_keepalive,
        };
        let client = replication::connect(&params).await?;
        replication::ensure_slot(
            &client,
            &self.config.connection_url,
            &self.config.slot_name,
            self.config.create_slot_if_missing,
        )
        .await?;
        let mut duplex = replication::start_replication(&client, &params).await?;

        // 3. Advance the slot's confirmed_flush_lsn to the bookmarked LSN.
        let initial_confirmed = *self.confirmed_lsn.lock().await;
        send_status_update(&mut duplex, initial_confirmed, false).await?;

        // 4. Drain the replication stream until idle_timeout or max_messages.
        let mut records: Vec<Value> = Vec::new();
        let mut registry = RelationRegistry::new();
        let mut state = TxnState::default();
        let max_messages = self.config.max_messages.unwrap_or(usize::MAX);
        let idle_timeout = self.config.idle_timeout;
        let mut last_message_at = Instant::now();

        loop {
            let idle_deadline = last_message_at + idle_timeout;
            let budget = idle_deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);

            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("postgres-cdc: ctrl_c received, stopping cleanly");
                    break;
                }
                ev = tokio::time::timeout(budget, recv(&mut duplex)) => {
                    match ev {
                        Ok(Ok(Some(event))) => {
                            // Reset on any server activity, not just committed output. This avoids
                            // firing idle_timeout mid-transaction when the server is actively
                            // streaming WAL we haven't yet COMMITTED. Compare with
                            // `faucet-source-kafka`, which only resets on deliverable records.
                            last_message_at = Instant::now();
                            handle_event(event, &mut registry, &mut state, &mut records)?;
                            if records.len() >= max_messages {
                                break;
                            }
                        }
                        Ok(Ok(None)) => {
                            return Err(FaucetError::Source(
                                "postgres-cdc: replication stream ended unexpectedly".into(),
                            ));
                        }
                        Ok(Err(e)) => return Err(e),
                        Err(_timeout) => {
                            tracing::debug!("postgres-cdc: idle_timeout reached, stopping");
                            break;
                        }
                    }
                }
            }
        }

        // 5. Compute the new bookmark. It's the commit_lsn of the LAST fully-
        //    applied transaction. None means: this drain didn't see any COMMIT,
        //    so nothing new to persist.
        let bookmark_value = if let Some(lsn) = state.last_committed {
            *self.confirmed_lsn.lock().await = lsn;
            Some(Bookmark::from_u64(lsn).to_value()?)
        } else {
            None
        };
        Ok((records, bookmark_value))
    }

    fn config_schema(&self) -> Value {
        let schema = schemars::schema_for!(PostgresCdcSourceConfig);
        serde_json::to_value(&schema).unwrap_or(Value::Null)
    }

    fn state_key(&self) -> Option<String> {
        Some(self.state_key_value.clone())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        let parsed = Bookmark::from_value(bookmark)?;
        // Update confirmed_lsn so the next initial status update (in the next
        // fetch cycle) advertises the correct position.
        *self.confirmed_lsn.lock().await = parsed.as_u64()?;
        *self.pending_bookmark.lock().await = Some(parsed);
        Ok(())
    }
}

/// One decoded tuple, split into its values and the names of any unchanged
/// (large/TOAST) columns whose value the server didn't re-send.
struct TupleRow {
    values: Map<String, Value>,
    unchanged_toast: Vec<String>,
}

/// In-flight transaction state while draining the replication stream.
#[derive(Default)]
struct TxnState {
    /// Records produced inside the current BEGIN..COMMIT, buffered until
    /// COMMIT is seen so partial transactions never leak into the output.
    staged: Vec<Value>,
    /// commit_lsn of the most recently fully-applied transaction.
    last_committed: Option<u64>,
    /// commit_ts (Postgres epoch micros) of the in-progress transaction,
    /// set by BEGIN.
    in_progress_ts: i64,
    /// commit_lsn announced by the in-progress BEGIN (== final_lsn).
    in_progress_lsn: u64,
    /// Whether we are currently inside a BEGIN..COMMIT pair.
    in_txn: bool,
}

fn handle_event(
    event: ReplicationEvent,
    registry: &mut RelationRegistry,
    state: &mut TxnState,
    out: &mut Vec<Value>,
) -> Result<(), FaucetError> {
    match event {
        ReplicationEvent::Begin {
            final_lsn,
            commit_time_micros,
            xid: _,
        } => {
            if state.in_txn {
                tracing::warn!(
                    "postgres-cdc: BEGIN received while previous transaction was \
                     still in progress — discarding {} staged records",
                    state.staged.len()
                );
            }
            state.in_txn = true;
            state.in_progress_lsn = final_lsn.as_u64();
            state.in_progress_ts = commit_time_micros;
            state.staged.clear();
        }
        ReplicationEvent::Commit {
            lsn,
            commit_time_micros: _,
            end_lsn: _,
        } => {
            if !state.in_txn {
                return Err(FaucetError::Source(
                    "postgres-cdc: COMMIT without BEGIN".into(),
                ));
            }
            out.append(&mut state.staged);
            state.last_committed = Some(lsn.as_u64());
            state.in_txn = false;
        }
        ReplicationEvent::XLogData { data, .. } => {
            let msg = decode_message(&data)?;
            handle_pgoutput(msg, registry, state)?;
        }
        ReplicationEvent::Message { .. } => {
            // pg_logical_emit_message — ignore for v1.
        }
        // KeepAlive and StoppedAt are filtered inside recv(); if they appear
        // here it's a bug in the lib upgrade — surface explicitly.
        other => {
            tracing::warn!(?other, "postgres-cdc: unexpected ReplicationEvent variant");
        }
    }
    Ok(())
}

fn handle_pgoutput(
    msg: Message,
    registry: &mut RelationRegistry,
    state: &mut TxnState,
) -> Result<(), FaucetError> {
    match msg {
        Message::Relation(r) => registry.insert(r),
        Message::Origin | Message::Type => {} // ignored
        Message::Insert(i) => stage_insert(state, registry, i)?,
        Message::Update(u) => stage_update(state, registry, u)?,
        Message::Delete(d) => stage_delete(state, registry, d)?,
        Message::Truncate(t) => stage_truncate(state, registry, t)?,
        // Begin/Commit pgoutput messages should never arrive here — the
        // pgwire-replication library decodes them into structured
        // ReplicationEvent::Begin / Commit variants, handled in handle_event.
        // If we see one, log a warning and ignore.
        Message::Begin(_) | Message::Commit(_) => {
            tracing::warn!(
                "postgres-cdc: pgoutput Begin/Commit reached pgoutput decoder; \
                 pgwire-replication should have intercepted it"
            );
        }
    }
    Ok(())
}

fn stage_insert(
    state: &mut TxnState,
    registry: &RelationRegistry,
    i: Insert,
) -> Result<(), FaucetError> {
    let rel = registry.get(i.relation_oid)?;
    let after = tuple_to_object(rel, &i.new)?;
    let r = record(rel, "insert", state, None, Some(after));
    state.staged.push(r);
    Ok(())
}

fn stage_update(
    state: &mut TxnState,
    registry: &RelationRegistry,
    u: Update,
) -> Result<(), FaucetError> {
    let rel = registry.get(u.relation_oid)?;
    let before = match &u.old {
        Some(t) => Some(tuple_to_object(rel, t)?),
        None => None,
    };
    let after = tuple_to_object(rel, &u.new)?;
    let r = record(rel, "update", state, before, Some(after));
    state.staged.push(r);
    Ok(())
}

fn stage_delete(
    state: &mut TxnState,
    registry: &RelationRegistry,
    d: Delete,
) -> Result<(), FaucetError> {
    let rel = registry.get(d.relation_oid)?;
    let before = Some(tuple_to_object(rel, &d.old)?);
    let r = record(rel, "delete", state, before, None);
    state.staged.push(r);
    Ok(())
}

fn stage_truncate(
    state: &mut TxnState,
    registry: &RelationRegistry,
    t: Truncate,
) -> Result<(), FaucetError> {
    for oid in &t.relation_oids {
        let rel = registry.get(*oid)?;
        let r = record(rel, "truncate", state, None, None);
        state.staged.push(r);
    }
    Ok(())
}

fn record(
    rel: &Relation,
    op: &str,
    state: &TxnState,
    before: Option<TupleRow>,
    after: Option<TupleRow>,
) -> Value {
    fn to_value(row: TupleRow) -> Value {
        let mut o = row.values;
        if !row.unchanged_toast.is_empty() {
            o.insert("__unchanged_toast__".into(), json!(row.unchanged_toast));
        }
        Value::Object(o)
    }
    let mut obj = Map::new();
    obj.insert("op".into(), json!(op));
    obj.insert("schema".into(), json!(rel.namespace));
    obj.insert("table".into(), json!(rel.name));
    obj.insert("lsn".into(), json!(format_lsn(state.in_progress_lsn)));
    obj.insert(
        "ts_ms".into(),
        json!(postgres_clock_to_unix_ms(state.in_progress_ts)),
    );
    obj.insert("before".into(), before.map(to_value).unwrap_or(Value::Null));
    obj.insert("after".into(), after.map(to_value).unwrap_or(Value::Null));
    Value::Object(obj)
}

/// Convert a tuple's text cells to a [`TupleRow`].
fn tuple_to_object(rel: &Relation, tup: &TupleData) -> Result<TupleRow, FaucetError> {
    if tup.cells.len() != rel.columns.len() {
        return Err(FaucetError::Source(format!(
            "postgres-cdc: tuple has {} cells but relation {}.{} has {} columns",
            tup.cells.len(),
            rel.namespace,
            rel.name,
            rel.columns.len()
        )));
    }
    let mut values = Map::with_capacity(rel.columns.len());
    let mut unchanged_toast = Vec::new();
    for (col, cell) in rel.columns.iter().zip(&tup.cells) {
        match cell {
            TupleCell::Null => {
                values.insert(col.name.clone(), Value::Null);
            }
            TupleCell::UnchangedToast => {
                unchanged_toast.push(col.name.clone());
            }
            TupleCell::Text(s) => {
                values.insert(col.name.clone(), text_to_json(col.type_oid, s)?);
            }
        }
    }
    Ok(TupleRow {
        values,
        unchanged_toast,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::messages::{ColumnDesc, ReplicaIdentity};
    use crate::replication::ReplicationEvent;
    use pgwire_replication::Lsn;

    fn rel_users() -> Relation {
        Relation {
            oid: 16384,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                ColumnDesc {
                    flags: 1,
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                ColumnDesc {
                    flags: 0,
                    name: "name".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        }
    }

    fn xlogdata(payload: Vec<u8>) -> ReplicationEvent {
        ReplicationEvent::XLogData {
            wal_start: Lsn::from_u64(0),
            wal_end: Lsn::from_u64(0x16A_4F88),
            server_time_micros: 0,
            data: bytes::Bytes::from(payload),
        }
    }

    fn insert_payload(relation_oid: u32, cells: &[(&str, &str)]) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.push(b'I');
        buf.extend_from_slice(&relation_oid.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&(cells.len() as u16).to_be_bytes());
        for (_, val) in cells {
            text_cell(&mut buf, val);
        }
        buf
    }

    /// 'U' relation 'O' fullold 'N' new — exercises REPLICA IDENTITY FULL.
    fn update_full_payload(
        relation_oid: u32,
        old_cells: &[(&str, &str)],
        new_cells: &[(&str, &str)],
    ) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.push(b'U');
        buf.extend_from_slice(&relation_oid.to_be_bytes());
        buf.push(b'O');
        buf.extend_from_slice(&(old_cells.len() as u16).to_be_bytes());
        for (_, val) in old_cells {
            text_cell(&mut buf, val);
        }
        buf.push(b'N');
        buf.extend_from_slice(&(new_cells.len() as u16).to_be_bytes());
        for (_, val) in new_cells {
            text_cell(&mut buf, val);
        }
        buf
    }

    /// 'D' relation 'O' fullold — REPLICA IDENTITY FULL delete.
    fn delete_full_payload(relation_oid: u32, old_cells: &[(&str, &str)]) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.push(b'D');
        buf.extend_from_slice(&relation_oid.to_be_bytes());
        buf.push(b'O');
        buf.extend_from_slice(&(old_cells.len() as u16).to_be_bytes());
        for (_, val) in old_cells {
            text_cell(&mut buf, val);
        }
        buf
    }

    /// 'T' flags=0 oids... — truncate without cascade/restart_identity.
    fn truncate_payload(relation_oids: &[u32]) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.push(b'T');
        buf.extend_from_slice(&(relation_oids.len() as u32).to_be_bytes());
        buf.push(0u8); // flags
        for oid in relation_oids {
            buf.extend_from_slice(&oid.to_be_bytes());
        }
        buf
    }

    fn text_cell(buf: &mut Vec<u8>, val: &str) {
        buf.push(b't');
        buf.extend_from_slice(&(val.len() as u32).to_be_bytes());
        buf.extend_from_slice(val.as_bytes());
    }

    fn begin_event(final_lsn: u64) -> ReplicationEvent {
        ReplicationEvent::Begin {
            final_lsn: Lsn::from_u64(final_lsn),
            xid: 1,
            commit_time_micros: 0,
        }
    }

    fn commit_event(lsn: u64) -> ReplicationEvent {
        ReplicationEvent::Commit {
            lsn: Lsn::from_u64(lsn),
            end_lsn: Lsn::from_u64(lsn + 0x10),
            commit_time_micros: 0,
        }
    }

    #[test]
    fn full_transaction_promotes_to_output_on_commit() {
        let mut registry = RelationRegistry::new();
        registry.insert(rel_users());
        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(0x16A_4F88), &mut registry, &mut state, &mut out).unwrap();
        assert!(out.is_empty());

        handle_event(
            xlogdata(insert_payload(16384, &[("id", "1"), ("name", "alice")])),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();
        assert!(out.is_empty(), "records stay staged until COMMIT");

        handle_event(
            commit_event(0x16A_4F88),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["op"], "insert");
        assert_eq!(out[0]["schema"], "public");
        assert_eq!(out[0]["table"], "users");
        assert_eq!(out[0]["lsn"], "0/16A4F88");
        assert_eq!(out[0]["after"]["id"], 1);
        assert_eq!(out[0]["after"]["name"], "alice");
        assert_eq!(out[0]["before"], Value::Null);

        assert_eq!(state.last_committed, Some(0x16A_4F88));
    }

    #[test]
    fn commit_without_begin_errors() {
        let mut registry = RelationRegistry::new();
        let mut state = TxnState::default();
        let mut out = vec![];

        let err = handle_event(
            ReplicationEvent::Commit {
                lsn: Lsn::from_u64(1),
                end_lsn: Lsn::from_u64(2),
                commit_time_micros: 0,
            },
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("COMMIT without BEGIN"));
    }

    #[test]
    fn unknown_relation_in_insert_errors() {
        let mut registry = RelationRegistry::new();
        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(1), &mut registry, &mut state, &mut out).unwrap();
        // Insert references relation 99999 which is not in the registry.
        let err = handle_event(
            xlogdata(insert_payload(99999, &[("id", "1"), ("name", "alice")])),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("99999"));
    }

    #[test]
    fn update_with_replica_identity_full_emits_before_and_after() {
        let mut registry = RelationRegistry::new();
        registry.insert(rel_users());
        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(0x16A_4F88), &mut registry, &mut state, &mut out).unwrap();
        handle_event(
            xlogdata(update_full_payload(
                16384,
                &[("id", "1"), ("name", "alice")],
                &[("id", "1"), ("name", "alice2")],
            )),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();
        handle_event(
            commit_event(0x16A_4F88),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["op"], "update");
        assert_eq!(out[0]["before"]["id"], 1);
        assert_eq!(out[0]["before"]["name"], "alice");
        assert_eq!(out[0]["after"]["name"], "alice2");
    }

    #[test]
    fn delete_with_replica_identity_full_emits_before_only() {
        let mut registry = RelationRegistry::new();
        registry.insert(rel_users());
        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(0x16A_4F88), &mut registry, &mut state, &mut out).unwrap();
        handle_event(
            xlogdata(delete_full_payload(
                16384,
                &[("id", "1"), ("name", "alice")],
            )),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();
        handle_event(
            commit_event(0x16A_4F88),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["op"], "delete");
        assert_eq!(out[0]["before"]["id"], 1);
        assert_eq!(out[0]["before"]["name"], "alice");
        assert_eq!(out[0]["after"], Value::Null);
    }

    #[test]
    fn truncate_emits_one_record_per_relation() {
        let mut registry = RelationRegistry::new();
        registry.insert(rel_users());
        // Build a second relation so the truncate-list has two known OIDs.
        let mut second = rel_users();
        second.oid = 16385;
        second.name = "orders".into();
        registry.insert(second);

        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(0x16A_4F88), &mut registry, &mut state, &mut out).unwrap();
        handle_event(
            xlogdata(truncate_payload(&[16384, 16385])),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();
        handle_event(
            commit_event(0x16A_4F88),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();

        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|r| r["op"] == "truncate"));
        let tables: Vec<_> = out.iter().map(|r| r["table"].as_str().unwrap()).collect();
        assert!(tables.contains(&"users"));
        assert!(tables.contains(&"orders"));
    }

    #[test]
    fn unchanged_toast_in_before_surfaces_via_metadata() {
        // Exercise Fix 1: a REPLICA IDENTITY FULL update with an UnchangedToast
        // cell in the old tuple must record the column name in
        // before.__unchanged_toast__.
        let mut registry = RelationRegistry::new();
        registry.insert(rel_users());
        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(0x16A_4F88), &mut registry, &mut state, &mut out).unwrap();
        // Hand-build an UPDATE where the OLD tuple's `name` cell is 'u' (unchanged TOAST).
        let mut buf: Vec<u8> = Vec::new();
        buf.push(b'U');
        buf.extend_from_slice(&16384u32.to_be_bytes());
        buf.push(b'O');
        buf.extend_from_slice(&2u16.to_be_bytes());
        // id = text "1"
        text_cell(&mut buf, "1");
        // name = unchanged TOAST
        buf.push(b'u');
        // New tuple: id=1, name="alice2"
        buf.push(b'N');
        buf.extend_from_slice(&2u16.to_be_bytes());
        text_cell(&mut buf, "1");
        text_cell(&mut buf, "alice2");
        handle_event(xlogdata(buf), &mut registry, &mut state, &mut out).unwrap();
        handle_event(
            commit_event(0x16A_4F88),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["before"]["__unchanged_toast__"], json!(["name"]));
        assert!(out[0]["before"].get("name").is_none());
        assert_eq!(out[0]["before"]["id"], 1);
        assert_eq!(out[0]["after"]["name"], "alice2");
    }
}
