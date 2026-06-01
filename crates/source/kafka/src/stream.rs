//! `KafkaSource` — the Kafka consumer implementation.

use crate::config::KafkaSourceConfig;
use crate::context::BookmarkContext;
use crate::decode;
use crate::state::{Bookmark, PartitionOffset, state_key};
use async_trait::async_trait;
use base64::Engine;
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use faucet_common_kafka::OnDecodeError;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Headers;
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(feature = "schema-registry")]
use faucet_common_kafka::KafkaValueFormat;
#[cfg(feature = "schema-registry")]
use faucet_common_kafka::schema_registry::client::SchemaRegistryClient;

pub struct KafkaSource {
    config: KafkaSourceConfig,
    consumer: Arc<StreamConsumer<BookmarkContext>>,
    context: BookmarkContext,
    state_key_value: String,
    /// Cache of watermark-derived floor offsets for assigned partitions that
    /// have produced no message (so `position()` reports no concrete offset).
    /// Resolved once per partition and reused so the streaming bookmark build
    /// doesn't issue a broker `fetch_watermarks` every page (#146 H9).
    assigned_floor: std::sync::Mutex<HashMap<(String, i32), i64>>,
    #[cfg(feature = "schema-registry")]
    sr_client: Option<SchemaRegistryClient>,
}

impl KafkaSource {
    pub async fn new(config: KafkaSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;

        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &config.brokers);
        client_config.set("group.id", &config.group_id);
        client_config.set("enable.auto.commit", "false");
        client_config.set("auto.offset.reset", config.auto_offset_reset.as_str());
        client_config.set(
            "session.timeout.ms",
            config.session_timeout.as_millis().to_string(),
        );
        client_config.set_log_level(RDKafkaLogLevel::Warning);

        config.auth.apply(&mut client_config)?;

        for (k, v) in &config.extra_client_config {
            client_config.set(k, v);
        }

        let context = BookmarkContext::new();
        let consumer: StreamConsumer<BookmarkContext> = client_config
            .create_with_context(context.clone())
            .map_err(|e| FaucetError::Source(format!("kafka consumer init: {e}")))?;

        let topic_refs: Vec<&str> = config.topics.iter().map(String::as_str).collect();
        consumer
            .subscribe(&topic_refs)
            .map_err(|e| FaucetError::Source(format!("kafka subscribe: {e}")))?;

        let state_key_value = state_key(&config.group_id, &config.topics);

        #[cfg(feature = "schema-registry")]
        let sr_client = build_sr_client(&config.value_format, config.key_format.as_ref())?;

        Ok(Self {
            config,
            consumer: Arc::new(consumer),
            context,
            state_key_value,
            assigned_floor: std::sync::Mutex::new(HashMap::new()),
            #[cfg(feature = "schema-registry")]
            sr_client,
        })
    }

    /// Drain the callback-error slot. Called once per poll iteration so
    /// rebalance-callback failures surface before any further messages are
    /// processed (matches the batch-mode invariant).
    fn check_callback_error(&self) -> Result<(), FaucetError> {
        let mut guard =
            self.context.callback_error.lock().map_err(|e| {
                FaucetError::State(format!("kafka callback_error mutex poisoned: {e}"))
            })?;
        if let Some(e) = guard.take() {
            return Err(e);
        }
        Ok(())
    }

    /// Resolve a starting offset for **every assigned partition**, so the
    /// persisted bookmark records partitions that produced no message this run
    /// — otherwise such a partition is absent on resume and resets to
    /// `auto.offset.reset` (default `latest`), silently skipping any records
    /// that arrived in the meantime (the H9 fix).
    ///
    /// For a partition that delivered messages, `position()` reports a concrete
    /// next-offset (cheap, local). For one that produced nothing, `position()`
    /// reports `Offset::Invalid` (librdkafka only tracks *consumed* offsets), so
    /// we fall back to its watermark via `fetch_watermarks`: the **low**
    /// watermark under `earliest`, the **high** watermark under `latest` — i.e.
    /// exactly where the consumer is positioned for that reset policy. Those
    /// watermark lookups are a broker round-trip, so each empty partition's
    /// floor is resolved once and cached.
    async fn resolve_assigned_offsets(&self) -> Vec<PartitionOffset> {
        let assigned = match self.consumer.assignment() {
            Ok(tpl) => tpl,
            Err(e) => {
                tracing::warn!(error = %e, "kafka source: assignment() failed; bookmark falls back to consumed/carry-forward offsets");
                return Vec::new();
            }
        };
        let positions: HashMap<(String, i32), rdkafka::Offset> = self
            .consumer
            .position()
            .map(|tpl| {
                tpl.elements()
                    .iter()
                    .map(|e| ((e.topic().to_string(), e.partition()), e.offset()))
                    .collect()
            })
            .unwrap_or_default();

        let mut out: Vec<PartitionOffset> = Vec::new();
        let mut need_watermark: Vec<(String, i32)> = Vec::new();
        {
            let cache = self.assigned_floor.lock().ok();
            for elem in assigned.elements() {
                let key = (elem.topic().to_string(), elem.partition());
                match positions.get(&key) {
                    // A delivered partition: its concrete next-offset.
                    Some(rdkafka::Offset::Offset(n)) => out.push(PartitionOffset {
                        topic: key.0,
                        partition: key.1,
                        offset: *n,
                    }),
                    // No concrete position → use a cached watermark floor, or
                    // schedule a lookup for it.
                    _ => match cache.as_ref().and_then(|c| c.get(&key)) {
                        Some(&floor) => out.push(PartitionOffset {
                            topic: key.0,
                            partition: key.1,
                            offset: floor,
                        }),
                        None => need_watermark.push(key),
                    },
                }
            }
        }

        if !need_watermark.is_empty() {
            let earliest = matches!(
                self.config.auto_offset_reset,
                crate::config::OffsetReset::Earliest
            );
            let consumer = Arc::clone(&self.consumer);
            let to_fetch = need_watermark.clone();
            // `fetch_watermarks` is a blocking librdkafka broker call — run it
            // off the async runtime.
            let resolved = tokio::task::spawn_blocking(move || {
                to_fetch
                    .into_iter()
                    .filter_map(|(topic, partition)| {
                        consumer
                            .fetch_watermarks(&topic, partition, Duration::from_secs(5))
                            .ok()
                            .map(|(low, high)| {
                                (topic, partition, if earliest { low } else { high })
                            })
                    })
                    .collect::<Vec<_>>()
            })
            .await
            .unwrap_or_default();

            if let Ok(mut cache) = self.assigned_floor.lock() {
                for (topic, partition, floor) in resolved {
                    cache.insert((topic.clone(), partition), floor);
                    out.push(PartitionOffset {
                        topic,
                        partition,
                        offset: floor,
                    });
                }
            }
        }
        out
    }

    /// The start bookmark applied via [`apply_start_bookmark`], retained for
    /// carry-forward (cloned, not consumed). `None` on a fresh run.
    fn start_bookmark(&self) -> Option<Bookmark> {
        self.context
            .start_offsets
            .lock()
            .ok()
            .and_then(|g| g.clone())
    }

    /// Build the bookmark to persist from the offsets consumed this page/run,
    /// merging in the assigned-partition offsets and the carry-forward start
    /// bookmark. Returns `None` only when there is nothing at all to record (no
    /// partition assigned, nothing consumed, no prior state).
    async fn build_bookmark(
        &self,
        consumed: &HashMap<(String, i32), i64>,
    ) -> Result<Option<Value>, FaucetError> {
        let assigned = self.resolve_assigned_offsets().await;
        let merged = Bookmark::merged(self.start_bookmark().as_ref(), &assigned, consumed);
        if merged.partition_offsets.is_empty() {
            Ok(None)
        } else {
            Ok(Some(merged.to_value()?))
        }
    }

    async fn message_to_value(
        &self,
        msg: &rdkafka::message::BorrowedMessage<'_>,
    ) -> Result<Value, FaucetError> {
        let value = decode::decode(
            msg.payload(),
            &self.config.value_format,
            #[cfg(feature = "schema-registry")]
            self.sr_client.as_ref(),
        )
        .await?;

        let key = match &self.config.key_format {
            Some(fmt) => {
                decode::decode(
                    msg.key(),
                    fmt,
                    #[cfg(feature = "schema-registry")]
                    self.sr_client.as_ref(),
                )
                .await?
            }
            None => match msg.key() {
                Some(bytes) => Value::String(
                    std::str::from_utf8(bytes)
                        .map_err(|e| FaucetError::Source(format!("kafka key utf-8: {e}")))?
                        .to_string(),
                ),
                None => Value::Null,
            },
        };

        let mut headers_obj = Map::new();
        if let Some(headers) = msg.headers() {
            for h in headers.iter() {
                if let Some(value_bytes) = h.value {
                    if let Ok(s) = std::str::from_utf8(value_bytes) {
                        headers_obj.insert(h.key.to_string(), Value::String(s.to_string()));
                    } else {
                        let encoded = base64::engine::general_purpose::STANDARD.encode(value_bytes);
                        headers_obj.insert(h.key.to_string(), Value::String(encoded));
                    }
                }
            }
        }

        Ok(json!({
            "key": key,
            "value": value,
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": msg.timestamp().to_millis().unwrap_or(0),
            "headers": Value::Object(headers_obj),
        }))
    }
}

#[cfg(feature = "schema-registry")]
fn build_sr_client(
    value_format: &KafkaValueFormat,
    key_format: Option<&KafkaValueFormat>,
) -> Result<Option<SchemaRegistryClient>, FaucetError> {
    fn extract_cfg(f: &KafkaValueFormat) -> Option<&faucet_common_kafka::SchemaRegistryConfig> {
        match f {
            KafkaValueFormat::ConfluentAvro { schema_registry }
            | KafkaValueFormat::ConfluentProtobuf { schema_registry } => Some(schema_registry),
            KafkaValueFormat::ConfluentJsonSchema {
                schema_registry, ..
            } => Some(schema_registry),
            _ => None,
        }
    }
    let cfg = extract_cfg(value_format).or_else(|| key_format.and_then(extract_cfg));
    cfg.map(SchemaRegistryClient::new).transpose()
}

#[async_trait]
impl Source for KafkaSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (records, _bookmark) = self.fetch_with_context_incremental(context).await?;
        Ok(records)
    }

    async fn fetch_with_context_incremental(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let mut records: Vec<Value> = Vec::new();
        let mut pending_offsets: HashMap<(String, i32), i64> = HashMap::new();
        let mut last_message_at = Instant::now();
        let max_messages = self.config.max_messages.unwrap_or(usize::MAX);
        let idle_timeout = self.config.idle_timeout;

        loop {
            // Surface any error raised by the rebalance callback (e.g. a
            // failed seek). Done before the next poll so we don't process
            // additional messages after a bookmark application failure.
            self.check_callback_error()?;

            let idle_deadline = idle_timeout.map(|t| last_message_at + t);
            let poll_budget = match idle_deadline {
                Some(deadline) => deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or(Duration::ZERO),
                None => self.config.poll_timeout,
            };

            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("kafka source: ctrl_c received, stopping cleanly");
                    break;
                }
                recv = tokio::time::timeout(poll_budget, self.consumer.recv()) => {
                    match recv {
                        Ok(Ok(msg)) => {
                            match self.message_to_value(&msg).await {
                                Ok(record) => {
                                    pending_offsets.insert(
                                        (msg.topic().to_string(), msg.partition()),
                                        msg.offset() + 1,
                                    );
                                    records.push(record);
                                    last_message_at = Instant::now();
                                    if records.len() >= max_messages {
                                        break;
                                    }
                                }
                                Err(e) => match self.config.on_decode_error {
                                    OnDecodeError::Skip => {
                                        tracing::warn!(error = %e, "kafka source: decode failed, skipping message");
                                    }
                                    OnDecodeError::Fail => return Err(e),
                                },
                            }
                        }
                        Ok(Err(e)) => {
                            return Err(FaucetError::Source(format!("kafka recv: {e}")));
                        }
                        Err(_timeout) => {
                            if let Some(deadline) = idle_deadline
                                && Instant::now() >= deadline
                            {
                                tracing::debug!("kafka source: idle_timeout reached, stopping");
                                break;
                            }
                        }
                    }
                }
            }
        }

        let bookmark_value = self.build_bookmark(&pending_offsets).await?;
        Ok((records, bookmark_value))
    }

    /// Stream Kafka messages page-by-page. Mirrors the
    /// [`fetch_with_context_incremental`](Self::fetch_with_context_incremental)
    /// poll loop but emits a [`StreamPage`] each time the in-memory buffer
    /// reaches [`KafkaSourceConfig::batch_size`] (or whenever the idle window
    /// flushes a partially-filled buffer), and continues polling until the
    /// configured `max_messages` / `idle_timeout` termination conditions are
    /// hit.
    ///
    /// The trait-level `batch_size` argument is intentionally ignored in
    /// favour of the config field — the config is the user-facing knob the
    /// README documents and routing the pipeline-supplied hint through it
    /// would silently override an explicit config value.
    ///
    /// **Per-page bookmark:** each yielded page carries a snapshot of the
    /// cumulative `(topic, partition) -> next_offset` map seen so far. The
    /// streaming pipeline persists this via the configured `StateStore`
    /// *after* the sink confirms the write, giving at-least-once delivery
    /// with per-page durability — a crash between pages re-reads only the
    /// uncommitted page on resume (the rebalance callback seeds the assigned
    /// partitions with the bookmarked offsets before any fetch happens).
    ///
    /// **`batch_size = 0`:** drains the entire run window (until
    /// `max_messages` or `idle_timeout` fires) into a single page. This
    /// negates the streaming benefit; prefer a finite `batch_size` in
    /// production so the state store advances with each successful sink
    /// write.
    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        let max_messages = self.config.max_messages.unwrap_or(usize::MAX);
        let idle_timeout = self.config.idle_timeout;
        let poll_timeout = self.config.poll_timeout;
        let on_decode_error = self.config.on_decode_error;

        // batch_size == 0 means "drain entire run window into one page" —
        // effectively no per-page flush boundary. Otherwise the page flushes
        // as soon as it accumulates `batch_size` messages.
        let page_chunk = if batch_size == 0 {
            usize::MAX
        } else {
            batch_size
        };
        let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };

        Box::pin(async_stream::try_stream! {
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut pending_offsets: HashMap<(String, i32), i64> = HashMap::new();
            let mut last_message_at = Instant::now();
            let mut total: usize = 0;

            loop {
                // Surface any error raised by the rebalance callback (e.g. a
                // failed seek) before processing the next poll batch.
                self.check_callback_error()?;

                let idle_deadline = idle_timeout.map(|t| last_message_at + t);
                let poll_budget = match idle_deadline {
                    Some(deadline) => deadline
                        .checked_duration_since(Instant::now())
                        .unwrap_or(Duration::ZERO),
                    None => poll_timeout,
                };

                // Accumulators set by the select arm — `?` cannot cross the
                // select's match boundary into the outer `try_stream!`, so we
                // collect errors and termination flags here and act on them
                // after the select returns.
                let mut stop = false;
                let mut fatal: Option<FaucetError> = None;
                tokio::select! {
                    biased;
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("kafka source: ctrl_c received, stopping cleanly");
                        stop = true;
                    }
                    recv = tokio::time::timeout(poll_budget, self.consumer.recv()) => {
                        match recv {
                            Ok(Ok(msg)) => {
                                match self.message_to_value(&msg).await {
                                    Ok(record) => {
                                        pending_offsets.insert(
                                            (msg.topic().to_string(), msg.partition()),
                                            msg.offset() + 1,
                                        );
                                        buffer.push(record);
                                        last_message_at = Instant::now();
                                        total += 1;
                                        if total >= max_messages {
                                            stop = true;
                                        }
                                    }
                                    Err(e) => match on_decode_error {
                                        OnDecodeError::Skip => {
                                            tracing::warn!(error = %e, "kafka source: decode failed, skipping message");
                                        }
                                        OnDecodeError::Fail => fatal = Some(e),
                                    },
                                }
                            }
                            Ok(Err(e)) => {
                                fatal = Some(FaucetError::Source(format!("kafka recv: {e}")));
                            }
                            Err(_timeout) => {
                                if let Some(deadline) = idle_deadline
                                    && Instant::now() >= deadline
                                {
                                    tracing::debug!("kafka source: idle_timeout reached, stopping");
                                    stop = true;
                                }
                            }
                        }
                    }
                }

                if let Some(e) = fatal {
                    Err(e)?;
                }

                // Yield a full page as soon as the buffer hits `page_chunk`.
                // Bookmark = cumulative snapshot of pending_offsets after this
                // page's last message. Snapshot before flushing the buffer so
                // a crash between yield and the next loop iteration re-reads
                // only the uncommitted messages, not those already in this
                // page.
                if !buffer.is_empty() && buffer.len() >= page_chunk {
                    let page_records = std::mem::replace(
                        &mut buffer,
                        Vec::with_capacity(initial_capacity),
                    );
                    let bookmark = self.build_bookmark(&pending_offsets).await?;
                    yield StreamPage { records: page_records, bookmark };
                }

                if stop {
                    break;
                }
            }

            // Flush the trailing buffer (may be empty if the run terminated
            // exactly on a page boundary). When non-empty, emit one final
            // page carrying the cumulative bookmark.
            if !buffer.is_empty() {
                let bookmark = self.build_bookmark(&pending_offsets).await?;
                yield StreamPage { records: buffer, bookmark };
            }

            tracing::info!(
                messages = total,
                batch_size,
                "kafka source: stream complete",
            );
        })
    }

    fn config_schema(&self) -> Value {
        let schema = schemars::schema_for!(KafkaSourceConfig);
        serde_json::to_value(&schema).unwrap_or(Value::Null)
    }

    fn state_key(&self) -> Option<String> {
        Some(self.state_key_value.clone())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        let parsed = Bookmark::from_value(bookmark)?;
        // `pending_bookmark` is consumed by the rebalance callback to seed the
        // assigned partitions' starting offsets. `start_offsets` keeps a
        // retained copy so previously-known partitions that are empty this run
        // carry their offset forward into the next bookmark (the H9 fix).
        {
            let mut guard = self.context.start_offsets.lock().map_err(|e| {
                FaucetError::State(format!("kafka start_offsets mutex poisoned: {e}"))
            })?;
            *guard = Some(parsed.clone());
        }
        let mut guard = self.context.pending_bookmark.lock().map_err(|e| {
            FaucetError::State(format!("kafka pending_bookmark mutex poisoned: {e}"))
        })?;
        *guard = Some(parsed);
        Ok(())
    }

    fn connector_name(&self) -> &'static str {
        "kafka"
    }

    /// Preflight probe that does **not** consume any message.
    ///
    /// The default `Source::check` would call `stream_pages`, which polls for
    /// a message and would block on an empty topic until the idle/max-message
    /// terminator fires. Instead we fetch cluster metadata
    /// (`fetch_metadata(None, timeout)`), which validates broker connectivity +
    /// auth without consuming or committing anything.
    ///
    /// `fetch_metadata` is a blocking librdkafka call, so it runs on a blocking
    /// thread; the whole probe is additionally bounded by `ctx.timeout`.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        use rdkafka::util::Timeout;

        let start = std::time::Instant::now();
        let consumer = Arc::clone(&self.consumer);
        let rd_timeout = Timeout::After(ctx.timeout);

        // Run the blocking fetch_metadata off the async runtime, bounded by the
        // same wall-clock budget so an unreachable broker can't hang the probe.
        let fetch = tokio::task::spawn_blocking(move || {
            consumer
                .fetch_metadata(None, rd_timeout)
                .map(|md| md.brokers().len())
                .map_err(|e| e.to_string())
        });

        let probe = match tokio::time::timeout(ctx.timeout, fetch).await {
            Ok(Ok(Ok(broker_count))) => {
                tracing::debug!(broker_count, "kafka check: fetched cluster metadata");
                Probe::pass("metadata", start.elapsed())
            }
            Ok(Ok(Err(e))) => Probe::fail_hint(
                "metadata",
                start.elapsed(),
                e,
                "verify brokers, network reachability, and auth (SASL/TLS) settings",
            ),
            Ok(Err(join_err)) => Probe::fail(
                "metadata",
                start.elapsed(),
                format!("metadata fetch task failed: {join_err}"),
            ),
            Err(_elapsed) => Probe::fail_hint(
                "metadata",
                start.elapsed(),
                "metadata fetch timed out",
                "no broker responded within the check timeout",
            ),
        };
        Ok(CheckReport::single(probe))
    }
}
