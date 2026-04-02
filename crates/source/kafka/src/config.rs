//! Kafka source configuration.

use std::collections::HashMap;

/// Configuration for the Kafka consumer source.
#[derive(Debug, Clone)]
pub struct KafkaSourceConfig {
    /// Comma-separated list of Kafka broker addresses (e.g. `"localhost:9092"`).
    pub brokers: String,
    /// Topic to consume from.
    pub topic: String,
    /// Consumer group ID.
    pub group_id: String,
    /// Maximum number of messages to consume. `None` means consume until timeout.
    pub max_messages: Option<usize>,
    /// How long (in milliseconds) to wait for messages before stopping (default: 5000).
    pub timeout_ms: u64,
    /// Auto offset reset policy (default: `"earliest"`).
    pub offset_reset: String,
    /// Additional librdkafka configuration key-value pairs.
    pub additional_config: HashMap<String, String>,
}

impl KafkaSourceConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(
        brokers: impl Into<String>,
        topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            topic: topic.into(),
            group_id: group_id.into(),
            max_messages: None,
            timeout_ms: 5000,
            offset_reset: "earliest".into(),
            additional_config: HashMap::new(),
        }
    }

    /// Set the maximum number of messages to consume.
    pub fn max_messages(mut self, max: usize) -> Self {
        self.max_messages = Some(max);
        self
    }

    /// Set the timeout in milliseconds for waiting on messages.
    pub fn timeout_ms(mut self, ms: u64) -> Self {
        self.timeout_ms = ms;
        self
    }

    /// Set the auto offset reset policy (e.g. `"earliest"`, `"latest"`).
    pub fn offset_reset(mut self, policy: impl Into<String>) -> Self {
        self.offset_reset = policy.into();
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
        let config = KafkaSourceConfig::new("localhost:9092", "my-topic", "my-group");
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.topic, "my-topic");
        assert_eq!(config.group_id, "my-group");
        assert!(config.max_messages.is_none());
        assert_eq!(config.timeout_ms, 5000);
        assert_eq!(config.offset_reset, "earliest");
        assert!(config.additional_config.is_empty());
    }

    #[test]
    fn builder_methods() {
        let config = KafkaSourceConfig::new("broker1:9092,broker2:9092", "events", "consumer-1")
            .max_messages(100)
            .timeout_ms(10000)
            .offset_reset("latest")
            .additional_config("security.protocol", "SASL_SSL");
        assert_eq!(config.max_messages, Some(100));
        assert_eq!(config.timeout_ms, 10000);
        assert_eq!(config.offset_reset, "latest");
        assert_eq!(
            config.additional_config.get("security.protocol").unwrap(),
            "SASL_SSL"
        );
    }
}
