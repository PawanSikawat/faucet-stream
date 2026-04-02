//! Kafka sink configuration.

use std::collections::HashMap;

/// Configuration for the Kafka producer sink.
#[derive(Debug, Clone)]
pub struct KafkaSinkConfig {
    /// Comma-separated list of Kafka broker addresses (e.g. `"localhost:9092"`).
    pub brokers: String,
    /// Topic to produce to.
    pub topic: String,
    /// Optional JSON field name to use as the Kafka message key.
    ///
    /// If set, each record's value at this field is serialized as the message key.
    /// If the field is missing from a record, the message is produced without a key.
    pub key_field: Option<String>,
    /// Additional librdkafka configuration key-value pairs.
    pub additional_config: HashMap<String, String>,
}

impl KafkaSinkConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(brokers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            topic: topic.into(),
            key_field: None,
            additional_config: HashMap::new(),
        }
    }

    /// Set the JSON field to use as the Kafka message key.
    pub fn key_field(mut self, field: impl Into<String>) -> Self {
        self.key_field = Some(field.into());
        self
    }

    /// Add an additional librdkafka configuration parameter.
    pub fn additional_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.additional_config.insert(key.into(), value.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = KafkaSinkConfig::new("localhost:9092", "my-topic");
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.topic, "my-topic");
        assert!(config.key_field.is_none());
        assert!(config.additional_config.is_empty());
    }

    #[test]
    fn builder_methods() {
        let config = KafkaSinkConfig::new("broker1:9092", "events")
            .key_field("id")
            .additional_config("compression.type", "lz4");
        assert_eq!(config.key_field.unwrap(), "id");
        assert_eq!(
            config.additional_config.get("compression.type").unwrap(),
            "lz4"
        );
    }
}
