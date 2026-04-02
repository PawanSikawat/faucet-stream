//! Kafka producer sink.

use crate::config::KafkaSinkConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde_json::Value;
use std::time::Duration;

/// A sink that produces JSON records to a Kafka topic.
///
/// Each record is serialized as a JSON string and sent as the message payload.
/// If `key_field` is configured, the corresponding field value is used as the
/// message key.
pub struct KafkaSink {
    config: KafkaSinkConfig,
    producer: FutureProducer,
}

impl KafkaSink {
    /// Create a new Kafka sink, establishing the producer connection.
    pub fn new(config: KafkaSinkConfig) -> Result<Self, FaucetError> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &config.brokers);

        for (key, value) in &config.additional_config {
            client_config.set(key, value);
        }

        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| FaucetError::Config(format!("failed to create Kafka producer: {e}")))?;

        Ok(Self { config, producer })
    }

    /// Extract the message key from a record, if `key_field` is configured.
    fn extract_key(&self, record: &Value) -> Option<String> {
        let key_field = self.config.key_field.as_deref()?;
        let key_value = record.get(key_field)?;
        // Use the raw JSON representation for non-string types, bare string for strings.
        match key_value {
            Value::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        }
    }
}

#[async_trait]
impl faucet_core::Sink for KafkaSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut success_count = 0usize;
        let delivery_timeout = Duration::from_secs(5);

        for record in records {
            let payload = serde_json::to_string(record)
                .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?;

            let key = self.extract_key(record);

            let mut future_record = FutureRecord::to(&self.config.topic).payload(&payload);
            if let Some(ref k) = key {
                future_record = future_record.key(k);
            }

            match self.producer.send(future_record, delivery_timeout).await {
                Ok(_) => {
                    success_count += 1;
                }
                Err((e, _)) => {
                    return Err(FaucetError::Sink(format!(
                        "failed to produce message to {}: {e}",
                        self.config.topic
                    )));
                }
            }
        }

        tracing::debug!(
            topic = %self.config.topic,
            count = success_count,
            "Kafka batch produced"
        );
        Ok(success_count)
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        self.producer
            .flush(Duration::from_secs(10))
            .map_err(|e| FaucetError::Sink(format!("Kafka producer flush failed: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_key_with_string_field() {
        let config = KafkaSinkConfig::new("localhost:9092", "test").key_field("id");
        // We can't create a real KafkaSink without a broker, so test the key
        // extraction logic directly via a helper instance pattern.
        // Instead, test the config and key extraction logic.
        let record = json!({"id": "abc-123", "name": "Alice"});
        let key_field = config.key_field.as_deref().unwrap();
        let key_value = record.get(key_field).unwrap();
        match key_value {
            Value::String(s) => assert_eq!(s, "abc-123"),
            _ => panic!("expected string key"),
        }
    }

    #[test]
    fn extract_key_with_numeric_field() {
        let record = json!({"id": 42, "name": "Bob"});
        let key_value = record.get("id").unwrap();
        assert_eq!(key_value.to_string(), "42");
    }

    #[test]
    fn extract_key_missing_field() {
        let record = json!({"name": "Charlie"});
        assert!(record.get("id").is_none());
    }
}
