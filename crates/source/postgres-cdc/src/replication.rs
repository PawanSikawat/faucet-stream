//! Low-level replication-connection wrapper.
//!
//! This module wraps [`pgwire_replication`] to provide the slot lifecycle
//! (`ensure_slot`) and streaming helpers (`start_replication`, `recv`,
//! `send_status_update`) used by the rest of the CDC source.
//!
//! # Design
//!
//! `pgwire_replication` handles everything from TCP connect through auth,
//! `START_REPLICATION`, keepalive replies, and `StandbyStatusUpdate` — all
//! internally.  The library delivers events as a typed enum; the
//! [`ReplicationEvent::XLogData`] variant carries the raw pgoutput payload
//! bytes (header already stripped) that our [`crate::pgoutput`] decoder
//! consumes.
//!
//! Slot creation (`CREATE_REPLICATION_SLOT`) is a control-plane operation that
//! requires an ordinary (non-replication) SQL connection, so `ensure_slot`
//! uses [`sqlx`] for that single query.
//!
//! # Type aliases
//!
//! The plan requires stable names `Client` and `Duplex` so that Tasks 9+ can
//! refer to concrete types.  We define:
//!
//! - [`Client`] — a lightweight holder of `ReplicationParams` used to verify
//!   connectivity and create the replication slot, before the stream is
//!   opened.
//! - [`Duplex`] — the live replication stream; a thin wrapper around
//!   [`pgwire_replication::ReplicationClient`].

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use faucet_core::FaucetError;
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};
use sqlx::postgres::PgConnectOptions;
use sqlx::{Executor, PgConnection};
use tracing::debug;

/// Microseconds between the Unix epoch (1970-01-01) and the Postgres epoch
/// (2000-01-01).  Used for converting between Postgres timestamps and Unix time.
pub const POSTGRES_EPOCH_MICROS: i64 = 946_684_800_000_000;

// ── Public type aliases ────────────────────────────────────────────────────

/// Pre-stream handle.  Holds the validated `ReplicationParams` needed to open
/// a replication slot or start streaming.  Obtained from [`connect`].
pub struct Client {
    // Will be read by `stream.rs` in a later task.
    #[allow(dead_code)]
    pub(crate) params: ReplicationParams<'static>,
}

/// Live replication stream.  Wraps [`pgwire_replication::ReplicationClient`].
/// Obtained from [`start_replication`].
pub struct Duplex {
    inner: ReplicationClient,
    /// Latest WAL end seen from XLogData / KeepAlive, used by `update_applied`.
    last_wal_end: Lsn,
}

// ── Parameters ────────────────────────────────────────────────────────────

/// All parameters required to establish a logical replication connection.
///
/// This struct is accepted by every function in this module.
#[derive(Clone, Debug)]
pub struct ReplicationParams<'a> {
    /// `postgres://user:pass@host:port/db` style URL.
    pub connection_url: &'a str,
    /// Name of the replication slot (must already exist, or `create_slot_if_missing = true`).
    pub slot_name: &'a str,
    /// Publication name — must already exist on the server.
    pub publication_name: &'a str,
    /// pgoutput protocol version (1 or 2).
    pub proto_version: u32,
    /// Create the slot if it does not already exist.
    pub create_slot_if_missing: bool,
    /// Optional LSN to resume from.  `None` means "start from the slot's
    /// `confirmed_flush_lsn`".
    pub start_lsn: Option<u64>,
    /// TCP keepalive interval for the replication connection.
    pub tcp_keepalive: Duration,
}

// ── Helper: parse a postgres URL into (host, port, user, password, dbname) ─

struct PgCoords {
    host: String,
    port: u16,
    user: String,
    password: String,
    dbname: String,
}

fn parse_url(url: &str) -> Result<PgCoords, FaucetError> {
    // Parse via the standard `url` crate so we can extract all components
    // without relying on sqlx accessor methods that may not be public.
    let parsed = url::Url::parse(url)
        .map_err(|e| FaucetError::Config(format!("postgres-cdc: invalid connection URL: {e}")))?;

    let host = parsed.host_str().unwrap_or("localhost").to_owned();
    let port = parsed.port().unwrap_or(5432);
    let user = parsed.username().to_owned();
    let password = parsed.password().unwrap_or("").to_owned();
    let dbname = parsed.path().trim_start_matches('/').to_owned();
    let dbname = if dbname.is_empty() {
        "postgres".to_owned()
    } else {
        dbname
    };

    Ok(PgCoords {
        host,
        port,
        user,
        password,
        dbname,
    })
}

// ── Public API ─────────────────────────────────────────────────────────────

/// Validate connectivity and return a [`Client`] handle.
///
/// This function parses the connection URL and records the parameters.
/// It does **not** open a TCP connection; actual connectivity is verified
/// lazily when [`ensure_slot`] or [`start_replication`] is called.
pub async fn connect(params: &ReplicationParams<'_>) -> Result<Client, FaucetError> {
    // Eagerly validate the URL so bad configs fail fast.
    let _ = parse_url(params.connection_url)?;
    Ok(Client {
        params: ReplicationParams {
            connection_url: params.connection_url.to_owned().leak(),
            slot_name: params.slot_name.to_owned().leak(),
            publication_name: params.publication_name.to_owned().leak(),
            proto_version: params.proto_version,
            create_slot_if_missing: params.create_slot_if_missing,
            start_lsn: params.start_lsn,
            tcp_keepalive: params.tcp_keepalive,
        },
    })
}

/// Ensure the replication slot exists.
///
/// If the slot already exists this is a no-op.  If it does not exist and
/// `create_if_missing` is `true`, the slot is created via
/// `pg_create_logical_replication_slot`.  If `create_if_missing` is `false`
/// and the slot is absent, an error is returned.
pub async fn ensure_slot(
    _client: &Client,
    connection_url: &str,
    slot_name: &str,
    create_if_missing: bool,
) -> Result<(), FaucetError> {
    // Use sqlx for the control-plane query (not a replication connection).
    let opts: PgConnectOptions = connection_url
        .parse()
        .map_err(|e| FaucetError::Config(format!("postgres-cdc: invalid connection URL: {e}")))?;

    use sqlx::ConnectOptions as _;
    let mut conn: PgConnection = opts
        .connect()
        .await
        .map_err(|e| FaucetError::Source(format!("postgres-cdc ensure_slot connect: {e}")))?;

    // Check whether the slot already exists.
    let row: Option<(String,)> =
        sqlx::query_as("SELECT slot_name::text FROM pg_replication_slots WHERE slot_name = $1")
            .bind(slot_name)
            .fetch_optional(&mut conn)
            .await
            .map_err(|e| FaucetError::Source(format!("postgres-cdc slot lookup: {e}")))?;

    if row.is_some() {
        debug!("postgres-cdc: replication slot '{slot_name}' already exists");
        return Ok(());
    }

    if !create_if_missing {
        return Err(FaucetError::Source(format!(
            "postgres-cdc: replication slot '{slot_name}' does not exist \
             and create_slot_if_missing = false"
        )));
    }

    // Create the slot using the pgoutput plugin.
    // `escape_simple` prevents injection via the slot name (already validated
    // to [a-z0-9_] by config, but defence-in-depth doesn't hurt).
    let sql = format!(
        "SELECT pg_create_logical_replication_slot({}, 'pgoutput')",
        quote_literal(slot_name)
    );
    conn.execute(sql.as_str())
        .await
        .map_err(|e| FaucetError::Source(format!("postgres-cdc create slot: {e}")))?;

    debug!("postgres-cdc: created replication slot '{slot_name}'");
    Ok(())
}

/// Open a logical replication stream and return a [`Duplex`] handle.
///
/// Internally this calls `pgwire_replication::ReplicationClient::connect`
/// which handles TCP, TLS negotiation, auth, and `START_REPLICATION` in one
/// shot.
pub async fn start_replication(
    _client: &Client,
    params: &ReplicationParams<'_>,
) -> Result<Duplex, FaucetError> {
    let coords = parse_url(params.connection_url)?;

    let start_lsn = Lsn::from_u64(params.start_lsn.unwrap_or(0));

    let cfg = ReplicationConfig {
        host: coords.host,
        port: coords.port,
        user: coords.user,
        password: coords.password,
        database: coords.dbname,
        tls: TlsConfig::disabled(),
        slot: params.slot_name.to_owned(),
        publication: params.publication_name.to_owned(),
        start_lsn,
        stop_at_lsn: None,
        // Use the caller-supplied keepalive as the status update interval.
        status_interval: params.tcp_keepalive,
        // Wake up at least every keepalive interval when idle.
        idle_wakeup_interval: params.tcp_keepalive,
        buffer_events: 8192,
    };

    let inner = ReplicationClient::connect(cfg)
        .await
        .map_err(|e| FaucetError::Source(format!("postgres-cdc start_replication: {e}")))?;

    Ok(Duplex {
        inner,
        last_wal_end: start_lsn,
    })
}

/// Report progress to the server (Standby Status Update).
///
/// `confirmed_lsn` is the highest LSN whose changes have been durably
/// written to the sink.  The underlying library sends this feedback on its
/// own keepalive schedule; calling this function additionally marks the
/// progress so the next automatic feedback includes the latest position.
///
/// `reply_requested` mirrors the flag from the server's KeepAlive message
/// (no-op here since the library handles immediate replies internally).
pub async fn send_status_update(
    duplex: &mut Duplex,
    confirmed_lsn: u64,
    _reply_requested: bool,
) -> Result<(), FaucetError> {
    duplex
        .inner
        .update_applied_lsn(Lsn::from_u64(confirmed_lsn));
    Ok(())
}

/// Receive the next raw pgoutput payload from the server.
///
/// Returns:
/// - `Ok(Some(bytes))` — an [`XLogData`][ReplicationEvent::XLogData] message
///   with its 24-byte wire header already stripped.  These bytes are the exact
///   input expected by [`crate::pgoutput::decode_message`].
/// - `Ok(None)` — stream ended cleanly (slot stopped, stop LSN reached, or
///   `Duplex` was shut down).
/// - `Err(_)` — network / protocol error.
///
/// [`KeepAlive`][ReplicationEvent::KeepAlive] events are absorbed here and
/// used to advance the applied-LSN watermark so the library's internal
/// feedback loop stays current.  [`Begin`][ReplicationEvent::Begin] and
/// [`Commit`][ReplicationEvent::Commit] are absorbed too — the caller's
/// decoder re-derives those from the pgoutput payload.
pub async fn recv(duplex: &mut Duplex) -> Result<Option<Bytes>, FaucetError> {
    loop {
        match duplex
            .inner
            .recv()
            .await
            .map_err(|e| FaucetError::Source(format!("postgres-cdc recv: {e}")))?
        {
            None => return Ok(None),

            Some(ReplicationEvent::XLogData { data, wal_end, .. }) => {
                if wal_end > duplex.last_wal_end {
                    duplex.last_wal_end = wal_end;
                }
                return Ok(Some(data));
            }

            Some(ReplicationEvent::KeepAlive { wal_end, .. }) => {
                // Advance watermark so the worker's periodic feedback is
                // current even when we have nothing to ack ourselves.
                if wal_end > duplex.last_wal_end {
                    duplex.last_wal_end = wal_end;
                    duplex.inner.update_applied_lsn(wal_end);
                }
                // Continue the loop — do not surface keepalives to the caller.
            }

            Some(ReplicationEvent::Begin { .. })
            | Some(ReplicationEvent::Commit { .. })
            | Some(ReplicationEvent::Message { .. }) => {
                // Begin/Commit/Message are decoded from the XLogData payload
                // by our pgoutput decoder upstream; absorb here.
            }

            Some(ReplicationEvent::StoppedAt { .. }) => {
                return Ok(None);
            }
        }
    }
}

// ── Clock helpers ──────────────────────────────────────────────────────────

/// Current time as a Postgres-epoch timestamp (µs since 2000-01-01 UTC).
///
/// Used in Standby Status Update messages.
pub fn postgres_clock_now() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let unix_micros = (now.as_secs() as i64) * 1_000_000 + (now.subsec_micros() as i64);
    unix_micros - POSTGRES_EPOCH_MICROS
}

/// Convert a Postgres-epoch timestamp (µs since 2000-01-01) to Unix
/// milliseconds (ms since 1970-01-01).
pub fn postgres_clock_to_unix_ms(ts: i64) -> i64 {
    (POSTGRES_EPOCH_MICROS.saturating_add(ts)) / 1_000
}

// ── Private SQL helpers ────────────────────────────────────────────────────

/// Wrap `s` in double-quotes for use as a Postgres identifier.
/// Any embedded double-quote is doubled (`"` → `""`).
/// Reserved for DDL statements (e.g. `DROP REPLICATION SLOT`); used in tests.
#[allow(dead_code)]
fn quote_slot(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Escape a string for use in a Postgres literal (single-quote context).
/// Any embedded single-quote is doubled (`'` → `''`).
fn escape_simple(s: &str) -> String {
    s.replace('\'', "''")
}

/// Produce a single-quoted Postgres string literal.
fn quote_literal(s: &str) -> String {
    format!("'{}'", escape_simple(s))
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    /// Convenience: turn `postgres_clock_to_unix_ms`-compatible math into a
    /// `DateTime<Utc>`.  Used by tests only.
    fn postgres_clock_to_datetime(ts: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_micros(POSTGRES_EPOCH_MICROS.saturating_add(ts))
            .single()
            .unwrap_or_else(Utc::now)
    }

    #[test]
    fn postgres_clock_round_trip() {
        let dt = Utc.with_ymd_and_hms(2026, 5, 17, 12, 0, 0).unwrap();
        let pg_ts = dt.timestamp_micros() - POSTGRES_EPOCH_MICROS;
        let back = postgres_clock_to_datetime(pg_ts);
        assert_eq!(back, dt);
    }

    #[test]
    fn unix_ms_conversion() {
        let dt = Utc.with_ymd_and_hms(2026, 5, 17, 12, 0, 0).unwrap();
        let pg_ts = dt.timestamp_micros() - POSTGRES_EPOCH_MICROS;
        assert_eq!(postgres_clock_to_unix_ms(pg_ts), 1_779_019_200_000);
    }

    #[test]
    fn quote_slot_simple() {
        assert_eq!(quote_slot("faucet_slot"), "\"faucet_slot\"");
    }

    #[test]
    fn escape_simple_doubles_quotes() {
        assert_eq!(escape_simple("foo'bar"), "foo''bar");
    }
}
