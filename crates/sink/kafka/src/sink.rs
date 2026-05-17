//! KafkaSink: the producer implementation.

use crate::config::{KafkaSinkConfig, KafkaSinkTopic};
use crate::encode;
use crate::extract;
use async_trait::async_trait;
use faucet_core::{FaucetError, Sink};
#[cfg(feature = "schema-registry")]
use faucet_kafka_common::KafkaValueFormat;
use faucet_kafka_common::OnKeyError;
use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "schema-registry")]
use faucet_kafka_common::schema_registry::client::SchemaRegistryClient;

pub struct KafkaSink {
    config: KafkaSinkConfig,
    producer: Arc<FutureProducer>,
    #[cfg(feature = "schema-registry")]
    sr_client: Option<SchemaRegistryClient>,
}

impl KafkaSink {
    pub async fn new(config: KafkaSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;

        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &config.brokers);
        client_config.set("acks", config.acks.as_str());
        client_config.set(
            "enable.idempotence",
            if config.idempotent { "true" } else { "false" },
        );
        client_config.set("compression.type", config.compression.as_str());
        client_config.set("linger.ms", config.linger.as_millis().to_string());
        client_config.set("batch.size", config.batch_size.to_string());
        client_config.set(
            "message.timeout.ms",
            config.message_timeout.as_millis().to_string(),
        );

        config.auth.apply(&mut client_config)?;
        for (k, v) in &config.extra_client_config {
            client_config.set(k, v);
        }

        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| FaucetError::Sink(format!("kafka producer init: {e}")))?;

        #[cfg(feature = "schema-registry")]
        let sr_client = build_sr_client(&config.value_format, config.key_format.as_ref())?;

        Ok(Self {
            config,
            producer: Arc::new(producer),
            #[cfg(feature = "schema-registry")]
            sr_client,
        })
    }

    fn resolve_topic(&self, record: &Value) -> Result<String, FaucetError> {
        match &self.config.topic {
            KafkaSinkTopic::Fixed { name } => Ok(name.clone()),
            KafkaSinkTopic::FromPath { path } => {
                extract::string_at(record, path)?.ok_or_else(|| {
                    FaucetError::Sink(format!("topic.path '{path}' did not resolve for record"))
                })
            }
        }
    }

    async fn build_record_bytes(
        &self,
        record: &Value,
        topic: &str,
    ) -> Result<(Vec<u8>, Option<Vec<u8>>), FaucetError> {
        #[cfg(feature = "schema-registry")]
        let value_ctx = encode::SchemaContext {
            subject: format!("{topic}-value"),
            schema_text: None,
        };
        #[cfg(feature = "schema-registry")]
        let key_ctx = encode::SchemaContext {
            subject: format!("{topic}-key"),
            schema_text: None,
        };

        let value_bytes = encode::encode(
            record,
            &self.config.value_format,
            #[cfg(feature = "schema-registry")]
            self.sr_client.as_ref(),
            #[cfg(feature = "schema-registry")]
            &value_ctx,
        )
        .await?;

        let key_bytes = match (&self.config.key_path, &self.config.key_format) {
            (Some(path), Some(fmt)) => {
                // Extract the JSON sub-value at key_path and encode per key_format.
                let sub = extract::value_at(record, path)?;
                match self.handle_key_extract(sub).await? {
                    Some(v) => Some(
                        encode::encode(
                            &v,
                            fmt,
                            #[cfg(feature = "schema-registry")]
                            self.sr_client.as_ref(),
                            #[cfg(feature = "schema-registry")]
                            &key_ctx,
                        )
                        .await?,
                    ),
                    None => None,
                }
            }
            (Some(path), None) => {
                // Extract a string at key_path and use it directly as the key bytes.
                match extract::string_at(record, path)? {
                    Some(s) => Some(s.into_bytes()),
                    None => match self.config.on_key_error {
                        OnKeyError::Fail => {
                            return Err(FaucetError::Sink(format!(
                                "key_path '{path}' did not resolve and on_key_error=fail"
                            )));
                        }
                        OnKeyError::Skip | OnKeyError::RoundRobin => None,
                    },
                }
            }
            (None, _) => None,
        };

        let _ = topic; // used in feature-gated branch
        Ok((value_bytes, key_bytes))
    }

    async fn handle_key_extract(
        &self,
        extracted: Option<Value>,
    ) -> Result<Option<Value>, FaucetError> {
        match extracted {
            Some(v) => Ok(Some(v)),
            None => match self.config.on_key_error {
                OnKeyError::Fail => Err(FaucetError::Sink(
                    "key_path did not resolve and on_key_error=fail".into(),
                )),
                OnKeyError::Skip | OnKeyError::RoundRobin => Ok(None),
            },
        }
    }
}

#[cfg(feature = "schema-registry")]
fn build_sr_client(
    value_format: &KafkaValueFormat,
    key_format: Option<&KafkaValueFormat>,
) -> Result<Option<SchemaRegistryClient>, FaucetError> {
    fn cfg(f: &KafkaValueFormat) -> Option<&faucet_kafka_common::SchemaRegistryConfig> {
        match f {
            KafkaValueFormat::ConfluentAvro { schema_registry }
            | KafkaValueFormat::ConfluentProtobuf { schema_registry } => Some(schema_registry),
            KafkaValueFormat::ConfluentJsonSchema {
                schema_registry, ..
            } => Some(schema_registry),
            _ => None,
        }
    }
    let c = cfg(value_format).or_else(|| key_format.and_then(cfg));
    c.map(SchemaRegistryClient::new).transpose()
}

#[async_trait]
impl Sink for KafkaSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        let mut produced = 0usize;
        let mut skipped = 0usize;

        for record in records {
            // Drain one if we're at capacity.
            if in_flight.len() >= self.config.max_in_flight
                && let Some(res) = in_flight.next().await
            {
                match res {
                    Ok(()) => produced += 1,
                    Err(e) => return Err(e),
                }
            }

            let topic = self.resolve_topic(record)?;
            let (value_bytes, key_bytes) = self.build_record_bytes(record, &topic).await?;

            // Honour OnKeyError::Skip: if key was required but missing, the
            // extract step returned None and on_key_error decided to skip.
            if self.config.key_path.is_some()
                && key_bytes.is_none()
                && matches!(self.config.on_key_error, OnKeyError::Skip)
            {
                skipped += 1;
                continue;
            }

            let partition = match &self.config.partition_path {
                Some(p) => extract::partition_at(record, p)?,
                None => None,
            };

            let producer = self.producer.clone();
            let topic_owned = topic.clone();
            let message_timeout = self.config.message_timeout;
            let retries = self.config.queue_full_max_retries;
            let backoff = self.config.queue_full_backoff;

            in_flight.push(async move {
                send_with_queue_full_retry(
                    &producer,
                    &topic_owned,
                    value_bytes,
                    key_bytes,
                    partition,
                    message_timeout,
                    retries,
                    backoff,
                )
                .await
            });
        }

        while let Some(res) = in_flight.next().await {
            match res {
                Ok(()) => produced += 1,
                Err(e) => return Err(e),
            }
        }

        if skipped > 0 {
            tracing::warn!(
                skipped,
                "kafka sink: dropped records due to OnKeyError::Skip"
            );
        }
        Ok(produced)
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        self.producer
            .flush(self.config.message_timeout)
            .map_err(|e| FaucetError::Sink(format!("kafka producer flush: {e}")))?;
        Ok(())
    }

    fn config_schema(&self) -> Value {
        let schema = schemars::schema_for!(KafkaSinkConfig);
        serde_json::to_value(&schema).unwrap_or(Value::Null)
    }
}

/// Send a single record, retrying on `QueueFull` up to `max_retries` times
/// with `backoff` delay between attempts.
///
/// Uses `send_result()` (non-blocking enqueue) so we can apply our own retry
/// schedule rather than the librdkafka built-in queue timeout.
/// `_message_timeout` is kept in the signature for callers that track it but
/// is not needed with `send_result()`.
#[allow(clippy::too_many_arguments)]
async fn send_with_queue_full_retry(
    producer: &FutureProducer,
    topic: &str,
    value_bytes: Vec<u8>,
    key_bytes: Option<Vec<u8>>,
    partition: Option<i32>,
    _message_timeout: Duration,
    max_retries: u32,
    backoff: Duration,
) -> Result<(), FaucetError> {
    let mut attempts: u32 = 0;
    loop {
        // Build a fresh FutureRecord each iteration because send_result()
        // returns the record back on QueueFull so we can reconstruct it.
        let mut record: FutureRecord<'_, [u8], [u8]> =
            FutureRecord::to(topic).payload(value_bytes.as_slice());
        if let Some(k) = key_bytes.as_deref() {
            record = record.key(k);
        }
        if let Some(p) = partition {
            record = record.partition(p);
        }

        match producer.send_result(record) {
            Ok(delivery_future) => {
                // Record enqueued — await the delivery confirmation.
                match delivery_future.await {
                    Ok(Ok(_delivery)) => return Ok(()),
                    Ok(Err((
                        KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge),
                        _,
                    ))) => {
                        return Err(FaucetError::Sink(
                            "kafka send: record exceeds broker max.message.bytes".into(),
                        ));
                    }
                    Ok(Err((e, _msg))) => {
                        return Err(FaucetError::Sink(format!("kafka send: {e}")));
                    }
                    Err(_canceled) => {
                        return Err(FaucetError::Sink(
                            "kafka send: delivery future canceled (producer dropped)".into(),
                        ));
                    }
                }
            }
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _record)) => {
                if attempts >= max_retries {
                    return Err(FaucetError::Sink(format!(
                        "kafka send: QueueFull after {max_retries} retries"
                    )));
                }
                tracing::warn!(attempts, "kafka send: QueueFull, backing off");
                tokio::time::sleep(backoff).await;
                attempts += 1;
            }
            Err((e, _record)) => {
                return Err(FaucetError::Sink(format!("kafka send: {e}")));
            }
        }
    }
}
