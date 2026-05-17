//! Configuration types for the Kafka source.

use faucet_core::FaucetError;
use faucet_kafka_common::{KafkaAuth, KafkaValueFormat, OnDecodeError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KafkaSourceConfig {
    /// Comma-separated bootstrap server list, e.g. `"broker1:9092,broker2:9092"`.
    pub brokers: String,
    /// One or more topics to subscribe to.
    pub topics: Vec<String>,
    /// Consumer group ID; required by librdkafka and used for partition assignment.
    pub group_id: String,
    #[serde(default)]
    pub auth: KafkaAuth,
    #[serde(default)]
    pub value_format: KafkaValueFormat,
    /// Format for the message key. `None` (the default) decodes key bytes as
    /// UTF-8 (or `null` if there is no key on the message).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_format: Option<KafkaValueFormat>,
    #[serde(default)]
    pub auto_offset_reset: OffsetReset,
    /// Stop after this many messages have been consumed.
    /// At least one of `max_messages` and `idle_timeout` must be set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<usize>,
    /// Stop after this many seconds of no new messages.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "faucet_core::config::duration_secs_option"
    )]
    #[schemars(with = "Option<u64>")]
    pub idle_timeout: Option<Duration>,
    /// Single-poll timeout (advisory). Default 1 second.
    #[serde(
        default = "default_poll_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub poll_timeout: Duration,
    /// Kafka `session.timeout.ms`. Default 30 seconds.
    #[serde(
        default = "default_session_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub session_timeout: Duration,
    #[serde(default)]
    pub on_decode_error: OnDecodeError,
    /// Raw librdkafka client properties to pass through. Use with care —
    /// these can override anything set by `auth` or the typed fields above.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra_client_config: BTreeMap<String, String>,
}

fn default_poll_timeout() -> Duration {
    Duration::from_secs(1)
}

fn default_session_timeout() -> Duration {
    Duration::from_secs(30)
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OffsetReset {
    Earliest,
    #[default]
    Latest,
}

impl OffsetReset {
    #[allow(dead_code)] // used by KafkaSource::new in Task 13
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            OffsetReset::Earliest => "earliest",
            OffsetReset::Latest => "latest",
        }
    }
}

impl KafkaSourceConfig {
    /// Validate the config at construction time. Called by `KafkaSource::new`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.brokers.trim().is_empty() {
            return Err(FaucetError::Config(
                "kafka source: brokers must not be empty".into(),
            ));
        }
        if self.topics.is_empty() {
            return Err(FaucetError::Config(
                "kafka source: topics must contain at least one entry".into(),
            ));
        }
        if self.group_id.trim().is_empty() {
            return Err(FaucetError::Config(
                "kafka source: group_id must not be empty".into(),
            ));
        }
        if self.max_messages.is_none() && self.idle_timeout.is_none() {
            return Err(FaucetError::Config(
                "kafka source: at least one of max_messages or idle_timeout must be set".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn minimal_config() -> KafkaSourceConfig {
        KafkaSourceConfig {
            brokers: "localhost:9092".into(),
            topics: vec!["orders".into()],
            group_id: "test-group".into(),
            auth: KafkaAuth::None,
            value_format: KafkaValueFormat::Json,
            key_format: None,
            auto_offset_reset: OffsetReset::Latest,
            max_messages: Some(10),
            idle_timeout: None,
            poll_timeout: Duration::from_secs(1),
            session_timeout: Duration::from_secs(30),
            on_decode_error: OnDecodeError::Fail,
            extra_client_config: BTreeMap::new(),
        }
    }

    #[test]
    fn validate_accepts_minimal_config() {
        assert!(minimal_config().validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_brokers() {
        let mut c = minimal_config();
        c.brokers = "".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_topics() {
        let mut c = minimal_config();
        c.topics = vec![];
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_group_id() {
        let mut c = minimal_config();
        c.group_id = "  ".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_no_termination_condition() {
        let mut c = minimal_config();
        c.max_messages = None;
        c.idle_timeout = None;
        let err = c.validate().unwrap_err();
        assert!(format!("{err}").contains("max_messages or idle_timeout"));
    }

    #[test]
    fn validate_accepts_idle_timeout_only() {
        let mut c = minimal_config();
        c.max_messages = None;
        c.idle_timeout = Some(Duration::from_secs(5));
        assert!(c.validate().is_ok());
    }

    #[test]
    fn deserialize_from_yaml_like_json() {
        let j = json!({
            "brokers": "broker:9092",
            "topics": ["t1"],
            "group_id": "g",
            "value_format": {"type": "json"},
            "max_messages": 100
        });
        let parsed: KafkaSourceConfig = serde_json::from_value(j).unwrap();
        assert_eq!(parsed.topics, vec!["t1"]);
        assert_eq!(parsed.max_messages, Some(100));
        assert!(matches!(parsed.auth, KafkaAuth::None));
        assert_eq!(parsed.auto_offset_reset, OffsetReset::Latest);
    }

    #[test]
    fn schema_for_config_compiles() {
        let _ = schemars::schema_for!(KafkaSourceConfig);
    }

    #[test]
    fn offset_reset_as_str() {
        assert_eq!(OffsetReset::Earliest.as_str(), "earliest");
        assert_eq!(OffsetReset::Latest.as_str(), "latest");
    }
}
