//! `KafkaSource` — the Kafka consumer implementation.

use crate::config::KafkaSourceConfig;
use crate::decode;
use crate::state::{Bookmark, state_key};
use async_trait::async_trait;
use base64::Engine;
use faucet_core::{FaucetError, Source};
use faucet_kafka_common::OnDecodeError;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Headers;
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[cfg(feature = "schema-registry")]
use faucet_kafka_common::KafkaValueFormat;
#[cfg(feature = "schema-registry")]
use faucet_kafka_common::schema_registry::client::SchemaRegistryClient;

pub struct KafkaSource {
    config: KafkaSourceConfig,
    consumer: Arc<StreamConsumer>,
    pending_bookmark: Mutex<Option<Bookmark>>,
    state_key_value: String,
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

        let consumer: StreamConsumer = client_config
            .create()
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
            pending_bookmark: Mutex::new(None),
            state_key_value,
            #[cfg(feature = "schema-registry")]
            sr_client,
        })
    }

    /// Once the consumer has received its partition assignment, apply any
    /// pending bookmark by seeking each partition to the stored offset.
    async fn maybe_apply_seek(&self) -> Result<(), FaucetError> {
        let bookmark = {
            let mut guard = self.pending_bookmark.lock().await;
            guard.take()
        };
        let Some(bookmark) = bookmark else {
            return Ok(());
        };
        for entry in &bookmark.partition_offsets {
            self.consumer
                .seek(
                    &entry.topic,
                    entry.partition,
                    rdkafka::Offset::Offset(entry.offset),
                    Some(Duration::from_secs(5)),
                )
                .map_err(|e| {
                    FaucetError::State(format!(
                        "kafka seek topic={} partition={} offset={}: {e}",
                        entry.topic, entry.partition, entry.offset
                    ))
                })?;
        }
        Ok(())
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
    fn extract_cfg(f: &KafkaValueFormat) -> Option<&faucet_kafka_common::SchemaRegistryConfig> {
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
        let mut seek_applied = false;
        let max_messages = self.config.max_messages.unwrap_or(usize::MAX);
        let idle_timeout = self.config.idle_timeout;

        loop {
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
                            if !seek_applied {
                                self.maybe_apply_seek().await?;
                                seek_applied = true;
                            }
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

        let bookmark_value = if pending_offsets.is_empty() {
            None
        } else {
            Some(Bookmark::from_map(pending_offsets).to_value()?)
        };
        Ok((records, bookmark_value))
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
        let mut guard = self.pending_bookmark.lock().await;
        *guard = Some(parsed);
        Ok(())
    }
}
