//! Kafka consumer source stream.

use crate::config::KafkaSourceConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde_json::Value;
use std::time::Duration;

/// A source that consumes messages from a Kafka topic and returns them as JSON records.
///
/// Each message payload is deserialized as JSON. If deserialization fails, the
/// raw bytes are returned as a `Value::String`.
///
/// The consumer is created fresh on each `fetch_all()` call (no long-lived
/// connection is held between calls).
pub struct KafkaSource {
    config: KafkaSourceConfig,
}

impl KafkaSource {
    /// Create a new Kafka source with the given configuration.
    pub fn new(config: KafkaSourceConfig) -> Self {
        Self { config }
    }

    /// Build a `StreamConsumer` from the current configuration.
    fn build_consumer(&self) -> Result<StreamConsumer, FaucetError> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &self.config.group_id)
            .set("auto.offset.reset", &self.config.offset_reset)
            .set("enable.auto.commit", "true");

        for (key, value) in &self.config.additional_config {
            client_config.set(key, value);
        }

        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| FaucetError::Config(format!("failed to create Kafka consumer: {e}")))?;

        consumer
            .subscribe(&[&self.config.topic])
            .map_err(|e| FaucetError::Config(format!("failed to subscribe to topic: {e}")))?;

        Ok(consumer)
    }
}

#[async_trait]
impl faucet_core::Source for KafkaSource {
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        let consumer = self.build_consumer()?;
        let timeout = Duration::from_millis(self.config.timeout_ms);
        let mut records = Vec::new();

        loop {
            // Check if we've reached the message limit.
            if let Some(max) = self.config.max_messages
                && records.len() >= max
            {
                tracing::debug!(count = records.len(), "reached max_messages limit");
                break;
            }

            // Wait for the next message with a timeout.
            let message_result = tokio::time::timeout(timeout, consumer.recv()).await;

            match message_result {
                Err(_elapsed) => {
                    // Timeout expired — no more messages available within the window.
                    tracing::debug!(
                        count = records.len(),
                        timeout_ms = self.config.timeout_ms,
                        "timeout reached, stopping consumption"
                    );
                    break;
                }
                Ok(Err(e)) => {
                    return Err(FaucetError::Config(format!("Kafka consumer error: {e}")));
                }
                Ok(Ok(msg)) => {
                    let value = match msg.payload() {
                        Some(bytes) => match serde_json::from_slice::<Value>(bytes) {
                            Ok(v) => v,
                            Err(_) => {
                                // Not valid JSON — wrap raw bytes as a string.
                                let raw = String::from_utf8_lossy(bytes).into_owned();
                                tracing::warn!(
                                    offset = msg.offset(),
                                    "message payload is not valid JSON, wrapping as string"
                                );
                                Value::String(raw)
                            }
                        },
                        None => {
                            // Null/empty payload — skip or represent as null.
                            tracing::debug!(
                                offset = msg.offset(),
                                "skipping message with null payload"
                            );
                            Value::Null
                        }
                    };
                    records.push(value);
                }
            }
        }

        tracing::info!(
            topic = %self.config.topic,
            count = records.len(),
            "Kafka consumption complete"
        );
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_source_new() {
        let config = KafkaSourceConfig::new("localhost:9092", "test-topic", "test-group");
        let source = KafkaSource::new(config);
        assert_eq!(source.config.topic, "test-topic");
        assert_eq!(source.config.group_id, "test-group");
    }
}
