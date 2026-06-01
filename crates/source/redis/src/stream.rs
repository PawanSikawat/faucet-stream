//! Redis source stream executor.

use crate::config::{RedisSourceConfig, RedisSourceType};
use async_trait::async_trait;
use faucet_core::{FaucetError, Stream, StreamPage};
use redis::AsyncCommands;
use serde_json::{Value, json};
use std::pin::Pin;

/// A configured Redis source that reads records from Redis data structures.
pub struct RedisSource {
    config: RedisSourceConfig,
    /// Lazily-opened multiplexed connection, reused across every `fetch_all`
    /// and `stream_pages` call instead of opening a fresh client + TCP/AUTH
    /// handshake per call (#78/#22). `MultiplexedConnection` is cheap to clone
    /// (it shares one underlying socket), so each call clones the cached one.
    conn: tokio::sync::OnceCell<redis::aio::MultiplexedConnection>,
}

impl RedisSource {
    /// Create a new Redis source from the given configuration. The connection
    /// is opened lazily on first use, so construction stays synchronous and does
    /// no I/O; it fails only on an invalid config (an out-of-range `batch_size`).
    pub fn new(config: RedisSourceConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        Ok(Self {
            config,
            conn: tokio::sync::OnceCell::new(),
        })
    }

    /// Return a clone of the shared multiplexed connection, opening it once on
    /// first call.
    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, FaucetError> {
        let conn = self
            .conn
            .get_or_try_init(|| async {
                let client = redis::Client::open(self.config.url.as_str())
                    .map_err(|e| FaucetError::Config(format!("invalid Redis URL: {e}")))?;
                client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| FaucetError::Config(format!("Redis connection failed: {e}")))
            })
            .await?;
        Ok(conn.clone())
    }

    /// Fetch all records from the configured Redis source.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        let mut conn = self.connection().await?;

        let mut records = match &self.config.source_type {
            RedisSourceType::List { key } => self.fetch_list(&mut conn, key).await?,
            RedisSourceType::Stream {
                key,
                group,
                consumer,
                count,
            } => {
                self.fetch_stream(&mut conn, key, group, consumer, count)
                    .await?
            }
            RedisSourceType::Keys { pattern } => self.fetch_keys(&mut conn, pattern).await?,
        };

        if let Some(max) = self.config.max_records {
            records.truncate(max);
        }

        tracing::info!(records = records.len(), "Redis fetch complete");
        Ok(records)
    }

    /// Read all elements from a Redis list.
    async fn fetch_list(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        key: &str,
    ) -> Result<Vec<Value>, FaucetError> {
        let values: Vec<String> = conn
            .lrange(key, 0, -1)
            .await
            .map_err(|e| FaucetError::Config(format!("LRANGE failed on '{key}': {e}")))?;

        let records = values
            .into_iter()
            .map(|v| serde_json::from_str::<Value>(&v).unwrap_or_else(|_| Value::String(v.clone())))
            .collect();

        Ok(records)
    }

    /// Read entries from a Redis stream.
    async fn fetch_stream(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        key: &str,
        group: &Option<String>,
        consumer: &Option<String>,
        count: &Option<usize>,
    ) -> Result<Vec<Value>, FaucetError> {
        let entries: redis::streams::StreamReadReply = match (group, consumer) {
            (Some(group_name), Some(consumer_name)) => {
                let opts = redis::streams::StreamReadOptions::default().count(count.unwrap_or(100));
                conn.xread_options(&[key], &[">"], &opts.group(group_name, consumer_name))
                    .await
                    .map_err(|e| {
                        FaucetError::Config(format!("XREADGROUP failed on '{key}': {e}"))
                    })?
            }
            _ => {
                let mut opts = redis::streams::StreamReadOptions::default();
                if let Some(c) = count {
                    opts = opts.count(*c);
                }
                conn.xread_options(&[key], &["0"], &opts)
                    .await
                    .map_err(|e| FaucetError::Config(format!("XREAD failed on '{key}': {e}")))?
            }
        };

        let mut records = Vec::new();
        for stream_key in &entries.keys {
            for entry in &stream_key.ids {
                records.push(stream_entry_to_json(&entry.id, &entry.map));
            }
        }

        Ok(records)
    }

    /// Scan for keys matching a pattern, then MGET all keys in a single round-trip.
    async fn fetch_keys(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        pattern: &str,
    ) -> Result<Vec<Value>, FaucetError> {
        let keys: Vec<String> = {
            let mut collected = Vec::new();
            let mut iter: redis::AsyncIter<String> =
                conn.scan_match(pattern).await.map_err(|e| {
                    FaucetError::Config(format!("SCAN failed with pattern '{pattern}': {e}"))
                })?;

            while let Some(key) = iter.next_item().await {
                collected.push(key);
            }
            collected
        };

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&keys)
            .query_async(conn)
            .await
            .map_err(|e| FaucetError::Config(format!("MGET failed: {e}")))?;

        let mut records = Vec::new();
        for (key, value) in keys.iter().zip(values.into_iter()) {
            if let Some(v) = value {
                let parsed =
                    serde_json::from_str::<Value>(&v).unwrap_or_else(|_| Value::String(v.clone()));
                records.push(json!({
                    "key": key,
                    "value": parsed,
                }));
            }
        }

        Ok(records)
    }
}

/// Convert a single XRANGE/XREAD stream entry into the JSON record shape used
/// by both [`RedisSource::fetch_all`] and [`RedisSource::stream_pages`].
fn stream_entry_to_json(id: &str, map: &std::collections::HashMap<String, redis::Value>) -> Value {
    let mut fields = serde_json::Map::new();
    for (field_name, field_value) in map {
        let val = match field_value {
            redis::Value::BulkString(bytes) => {
                let s = String::from_utf8_lossy(bytes);
                serde_json::from_str::<Value>(&s).unwrap_or_else(|_| Value::String(s.into_owned()))
            }
            redis::Value::SimpleString(s) => {
                serde_json::from_str::<Value>(s).unwrap_or_else(|_| Value::String(s.clone()))
            }
            redis::Value::Int(n) => json!(n),
            redis::Value::Double(n) => json!(n),
            redis::Value::Boolean(b) => json!(b),
            redis::Value::Nil => Value::Null,
            other => Value::String(format!("{other:?}")),
        };
        fields.insert(field_name.clone(), val);
    }
    json!({
        "id": id,
        "fields": Value::Object(fields),
    })
}

/// Parse a Redis stream entry ID (`ms-seq`) and return the immediate
/// successor ID, used to advance the `start` argument of the next `XRANGE`
/// call without re-emitting the last entry of the previous page.
fn next_stream_id(id: &str) -> String {
    // Stream IDs are `<ms>-<seq>`. The "next" ID after `a-b` is `a-(b+1)`,
    // wrapping to `(a+1)-0` on `u64::MAX` (which we treat as terminal).
    if let Some((ms, seq)) = id.split_once('-')
        && let (Ok(ms), Ok(seq)) = (ms.parse::<u64>(), seq.parse::<u64>())
    {
        return match seq.checked_add(1) {
            Some(next_seq) => format!("{ms}-{next_seq}"),
            None => format!("{}-0", ms.saturating_add(1)),
        };
    }
    // Fall back to appending `\x00` — XRANGE treats this as "just after".
    // Reachable only if Redis ever returns a malformed ID, which it does not
    // in practice, but we degrade safely.
    format!("{id}\u{0}")
}

#[async_trait]
impl faucet_core::Source for RedisSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        if context.is_empty() {
            return RedisSource::fetch_all(self).await;
        }

        let mut conn = self.connection().await?;

        // Substitute context into the key/pattern of each source type variant.
        let mut records = match &self.config.source_type {
            RedisSourceType::List { key } => {
                let resolved_key = faucet_core::util::substitute_context(key, context);
                self.fetch_list(&mut conn, &resolved_key).await?
            }
            RedisSourceType::Stream {
                key,
                group,
                consumer,
                count,
            } => {
                let resolved_key = faucet_core::util::substitute_context(key, context);
                self.fetch_stream(&mut conn, &resolved_key, group, consumer, count)
                    .await?
            }
            RedisSourceType::Keys { pattern } => {
                let resolved_pattern = faucet_core::util::substitute_context(pattern, context);
                self.fetch_keys(&mut conn, &resolved_pattern).await?
            }
        };

        if let Some(max) = self.config.max_records {
            records.truncate(max);
        }

        tracing::info!(
            records = records.len(),
            "Redis fetch complete (with context)"
        );
        Ok(records)
    }

    /// Stream records page-by-page so the pipeline can write to the sink as
    /// pages arrive instead of buffering the full result set. Each mode maps
    /// [`RedisSourceConfig::batch_size`] onto its native paging primitive
    /// (see the type-level doc on [`RedisSourceConfig::batch_size`]).
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// `batch_size = 0` drains the underlying primitive into a single page.
    /// The Redis source has no incremental-replication mode today, so every
    /// emitted page carries `bookmark: None`.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        let max_records = self.config.max_records;

        Box::pin(async_stream::try_stream! {
            let mut conn = self.connection().await?;

            let mut emitted: usize = 0;

            match &self.config.source_type {
                RedisSourceType::List { key } => {
                    let resolved = if context.is_empty() {
                        key.clone()
                    } else {
                        faucet_core::util::substitute_context(key, context)
                    };
                    let pages = stream_list(&mut conn, &resolved, batch_size, max_records);
                    futures::pin_mut!(pages);
                    while let Some(page) = futures::StreamExt::next(&mut pages).await {
                        let page = page?;
                        emitted += page.records.len();
                        yield page;
                    }
                }
                RedisSourceType::Stream { key, .. } => {
                    // Streaming intentionally uses XRANGE — consumer-group
                    // semantics (XREADGROUP) don't compose with "drain to a
                    // bookmarked checkpoint" because acknowledgement state
                    // would have to be deferred until the sink succeeds, and
                    // the source has no incremental mode today.
                    let resolved = if context.is_empty() {
                        key.clone()
                    } else {
                        faucet_core::util::substitute_context(key, context)
                    };
                    let pages = stream_xrange(&mut conn, &resolved, batch_size, max_records);
                    futures::pin_mut!(pages);
                    while let Some(page) = futures::StreamExt::next(&mut pages).await {
                        let page = page?;
                        emitted += page.records.len();
                        yield page;
                    }
                }
                RedisSourceType::Keys { pattern } => {
                    let resolved = if context.is_empty() {
                        pattern.clone()
                    } else {
                        faucet_core::util::substitute_context(pattern, context)
                    };
                    let pages = stream_keys(&mut conn, &resolved, batch_size, max_records);
                    futures::pin_mut!(pages);
                    while let Some(page) = futures::StreamExt::next(&mut pages).await {
                        let page = page?;
                        emitted += page.records.len();
                        yield page;
                    }
                }
            }

            tracing::info!(
                records = emitted,
                batch_size,
                "Redis source stream complete",
            );
        })
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(RedisSourceConfig))
            .expect("schema serialization")
    }
}

/// Stream a Redis list via `LRANGE start stop`, sliding the window by
/// `batch_size`. With `batch_size == 0`, drains the list in a single
/// `LRANGE 0 -1` round-trip.
///
/// **Consistency caveat (#78 LOW):** index-based `LRANGE` paging is only
/// stable if the list is not mutated mid-scan. A concurrent `LPUSH` / `LPOP`
/// shifts every element's index, so a writer pushing/popping while this drains
/// can make the source skip or duplicate elements across page boundaries. For
/// a queue-style workload where the list is being consumed concurrently,
/// prefer a Redis Stream (`XRANGE`/consumer groups) over a list.
fn stream_list<'a>(
    conn: &'a mut redis::aio::MultiplexedConnection,
    key: &'a str,
    batch_size: usize,
    max_records: Option<usize>,
) -> impl Stream<Item = Result<StreamPage, FaucetError>> + 'a {
    async_stream::try_stream! {
        if batch_size == 0 {
            let values: Vec<String> = conn
                .lrange(key, 0, -1)
                .await
                .map_err(|e| FaucetError::Config(format!("LRANGE failed on '{key}': {e}")))?;
            let mut records: Vec<Value> = values
                .into_iter()
                .map(|v| serde_json::from_str::<Value>(&v).unwrap_or_else(|_| Value::String(v.clone())))
                .collect();
            if let Some(max) = max_records {
                records.truncate(max);
            }
            yield StreamPage { records, bookmark: None };
            return;
        }

        let mut start: isize = 0;
        let mut emitted: usize = 0;
        loop {
            let stop: isize = start + batch_size as isize - 1;
            let values: Vec<String> = conn
                .lrange(key, start, stop)
                .await
                .map_err(|e| FaucetError::Config(format!("LRANGE failed on '{key}': {e}")))?;
            if values.is_empty() {
                break;
            }
            let mut records: Vec<Value> = values
                .into_iter()
                .map(|v| serde_json::from_str::<Value>(&v).unwrap_or_else(|_| Value::String(v.clone())))
                .collect();
            let returned = records.len();
            // Respect max_records — truncate the final page and stop.
            let mut stop_after_yield = false;
            if let Some(max) = max_records
                && emitted + records.len() >= max
            {
                records.truncate(max - emitted);
                stop_after_yield = true;
            }
            emitted += records.len();
            yield StreamPage { records, bookmark: None };
            if stop_after_yield || returned < batch_size {
                break;
            }
            start += batch_size as isize;
        }
    }
}

/// Stream a Redis stream via `XRANGE start + COUNT batch_size`, advancing the
/// start ID on each page. With `batch_size == 0`, drains via a single
/// `XRANGE - +` round-trip.
fn stream_xrange<'a>(
    conn: &'a mut redis::aio::MultiplexedConnection,
    key: &'a str,
    batch_size: usize,
    max_records: Option<usize>,
) -> impl Stream<Item = Result<StreamPage, FaucetError>> + 'a {
    async_stream::try_stream! {
        if batch_size == 0 {
            let reply: redis::streams::StreamRangeReply = conn
                .xrange_all(key)
                .await
                .map_err(|e| FaucetError::Config(format!("XRANGE failed on '{key}': {e}")))?;
            let mut records: Vec<Value> = reply
                .ids
                .iter()
                .map(|entry| stream_entry_to_json(&entry.id, &entry.map))
                .collect();
            if let Some(max) = max_records {
                records.truncate(max);
            }
            yield StreamPage { records, bookmark: None };
            return;
        }

        let mut start: String = "-".to_string();
        let mut emitted: usize = 0;
        loop {
            let reply: redis::streams::StreamRangeReply = conn
                .xrange_count(key, &start, "+", batch_size)
                .await
                .map_err(|e| FaucetError::Config(format!("XRANGE failed on '{key}': {e}")))?;

            if reply.ids.is_empty() {
                break;
            }

            // Capture the last returned ID before consuming the reply so we
            // can advance the cursor (`next_stream_id`) without re-emitting it.
            let last_id = reply
                .ids
                .last()
                .expect("non-empty checked above")
                .id
                .clone();
            let returned = reply.ids.len();
            let mut records: Vec<Value> = reply
                .ids
                .into_iter()
                .map(|entry| stream_entry_to_json(&entry.id, &entry.map))
                .collect();

            let mut stop_after_yield = false;
            if let Some(max) = max_records
                && emitted + records.len() >= max
            {
                records.truncate(max - emitted);
                stop_after_yield = true;
            }
            emitted += records.len();
            yield StreamPage { records, bookmark: None };

            if stop_after_yield || returned < batch_size {
                break;
            }
            start = next_stream_id(&last_id);
        }
    }
}

/// Stream keys matching `pattern`. The `SCAN` cursor is iterated server-side
/// (with `COUNT` set to a sensible hint), keys are buffered up to
/// `batch_size`, then `MGET`'d in one round-trip per page. With
/// `batch_size == 0`, drains the entire scan and emits one page after a
/// single `MGET`.
fn stream_keys<'a>(
    conn: &'a mut redis::aio::MultiplexedConnection,
    pattern: &'a str,
    batch_size: usize,
    max_records: Option<usize>,
) -> impl Stream<Item = Result<StreamPage, FaucetError>> + 'a {
    use faucet_core::DEFAULT_BATCH_SIZE;
    async_stream::try_stream! {
        // Drive the SCAN cursor manually (one `SCAN cursor MATCH .. COUNT ..`
        // round-trip at a time) rather than via the buffering `AsyncIter`, so
        // we can MGET + yield a page as soon as `batch_size` keys accumulate
        // instead of materialising the entire matched keyset first (#78 LOW).
        // SCAN COUNT is only a per-round-trip hint; a call may return more or
        // fewer keys than the hint, so we still buffer until a full page.
        let scan_hint = if batch_size == 0 { DEFAULT_BATCH_SIZE } else { batch_size };
        // `batch_size == 0` is the "no batching" sentinel — accumulate the
        // whole scan and emit one page (still one MGET).
        let chunk_size = if batch_size == 0 { usize::MAX } else { batch_size };
        let cap = max_records.unwrap_or(usize::MAX);

        let mut cursor: u64 = 0;
        let mut buffer: Vec<String> = Vec::new();
        let mut emitted: usize = 0;

        'scan: loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(scan_hint)
                .query_async(conn)
                .await
                .map_err(|e| FaucetError::Config(format!("SCAN failed with pattern '{pattern}': {e}")))?;
            cursor = next_cursor;
            buffer.extend(keys);

            // Flush as many full pages as the buffer now holds.
            while emitted < cap && buffer.len() >= chunk_size {
                let take = chunk_size.min(cap - emitted);
                let page_keys: Vec<String> = buffer.drain(..take).collect();
                let records = mget_records(conn, &page_keys).await?;
                emitted += records.len();
                yield StreamPage { records, bookmark: None };
            }

            if cursor == 0 || emitted >= cap {
                break 'scan;
            }
        }

        // Trailing partial page (and the single page in the batch_size==0 case).
        if emitted < cap && !buffer.is_empty() {
            let take = (cap - emitted).min(buffer.len());
            let page_keys: Vec<String> = buffer.drain(..take).collect();
            let records = mget_records(conn, &page_keys).await?;
            yield StreamPage { records, bookmark: None };
        }
    }
}

/// `MGET` a slice of keys and pair them with their values via
/// [`collect_kv_records`].
async fn mget_records(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
) -> Result<Vec<Value>, FaucetError> {
    let values: Vec<Option<String>> = redis::cmd("MGET")
        .arg(keys)
        .query_async(conn)
        .await
        .map_err(|e| FaucetError::Config(format!("MGET failed: {e}")))?;
    Ok(collect_kv_records(keys, values))
}

/// Pair `keys` with their `MGET`-returned values into `{ "key", "value" }`
/// records. Missing values (deleted between `SCAN` and `MGET`) are dropped,
/// matching [`RedisSource::fetch_keys`].
fn collect_kv_records(keys: &[String], values: Vec<Option<String>>) -> Vec<Value> {
    keys.iter()
        .zip(values)
        .filter_map(|(key, value)| {
            value.map(|v| {
                let parsed =
                    serde_json::from_str::<Value>(&v).unwrap_or_else(|_| Value::String(v.clone()));
                json!({ "key": key, "value": parsed })
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RedisSourceConfig;

    #[test]
    fn creates_source() {
        let config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::List { key: "test".into() },
        );
        let _source = RedisSource::new(config).unwrap();
    }

    #[test]
    fn new_rejects_out_of_range_batch_size() {
        let mut config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::List { key: "test".into() },
        );
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match RedisSource::new(config) {
            Err(FaucetError::Config(m)) => assert!(m.contains("batch_size"), "got: {m}"),
            other => panic!(
                "expected a batch_size Config error, got {:?}",
                other.is_ok()
            ),
        }
    }

    #[test]
    fn next_stream_id_increments_sequence() {
        assert_eq!(next_stream_id("1234-0"), "1234-1");
        assert_eq!(next_stream_id("1234-99"), "1234-100");
    }

    #[test]
    fn next_stream_id_wraps_seq_overflow() {
        let id = format!("5-{}", u64::MAX);
        assert_eq!(next_stream_id(&id), "6-0");
    }

    #[test]
    fn next_stream_id_falls_back_on_malformed_id() {
        // Not a real Redis ID — fallback path appends NUL.
        let next = next_stream_id("not-a-real-id");
        assert!(next.starts_with("not-a-real-id"));
        assert!(next.ends_with('\u{0}'));
    }

    #[test]
    fn stream_entry_to_json_extracts_id_and_fields() {
        let mut map = std::collections::HashMap::new();
        map.insert(
            "field1".to_string(),
            redis::Value::BulkString(b"value1".to_vec()),
        );
        map.insert("field2".to_string(), redis::Value::Int(42));
        let json = stream_entry_to_json("100-0", &map);
        assert_eq!(json["id"], "100-0");
        assert_eq!(json["fields"]["field1"], "value1");
        assert_eq!(json["fields"]["field2"], 42);
    }
}
