//! Configuration for the Kafka sink.

use faucet_core::FaucetError;
use faucet_kafka_common::{CompressionType, KafkaAuth, KafkaValueFormat, OnKeyError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KafkaSinkConfig {
    pub brokers: String,
    pub topic: KafkaSinkTopic,
    #[serde(default)]
    pub auth: KafkaAuth,
    #[serde(default)]
    pub value_format: KafkaValueFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_format: Option<KafkaValueFormat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers_path: Option<String>,
    #[serde(default)]
    pub on_key_error: OnKeyError,
    #[serde(default)]
    pub compression: CompressionType,
    #[serde(default = "default_acks")]
    pub acks: Acks,
    #[serde(default = "default_idempotent")]
    pub idempotent: bool,
    #[serde(
        default = "default_linger",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub linger: Duration,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(
        default = "default_message_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub message_timeout: Duration,
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: usize,
    #[serde(
        default = "default_queue_full_backoff",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub queue_full_backoff: Duration,
    #[serde(default = "default_queue_full_max_retries")]
    pub queue_full_max_retries: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra_client_config: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KafkaSinkTopic {
    Fixed { name: String },
    FromPath { path: String },
}

impl Default for KafkaSinkTopic {
    fn default() -> Self {
        Self::Fixed {
            name: String::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Acks {
    None,
    Leader,
    #[default]
    All,
}

impl Acks {
    // Used by KafkaSink (Task 20) when building the rdkafka producer config.
    #[allow(dead_code)]
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Acks::None => "0",
            Acks::Leader => "1",
            Acks::All => "all",
        }
    }
}

fn default_acks() -> Acks {
    Acks::All
}
fn default_idempotent() -> bool {
    true
}
fn default_linger() -> Duration {
    Duration::from_millis(5)
}
fn default_batch_size() -> usize {
    16_384
}
fn default_message_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_max_in_flight() -> usize {
    100
}
fn default_queue_full_backoff() -> Duration {
    Duration::from_millis(100)
}
fn default_queue_full_max_retries() -> u32 {
    3
}

impl KafkaSinkConfig {
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.brokers.trim().is_empty() {
            return Err(FaucetError::Config(
                "kafka sink: brokers must not be empty".into(),
            ));
        }
        match &self.topic {
            KafkaSinkTopic::Fixed { name } if name.trim().is_empty() => {
                return Err(FaucetError::Config(
                    "kafka sink: topic.name must not be empty".into(),
                ));
            }
            KafkaSinkTopic::FromPath { path } if path.trim().is_empty() => {
                return Err(FaucetError::Config(
                    "kafka sink: topic.path must not be empty".into(),
                ));
            }
            _ => {}
        }
        if self.idempotent && self.acks != Acks::All {
            return Err(FaucetError::Config(
                "kafka sink: idempotent=true requires acks=all".into(),
            ));
        }
        if self.max_in_flight == 0 {
            return Err(FaucetError::Config(
                "kafka sink: max_in_flight must be at least 1".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> KafkaSinkConfig {
        KafkaSinkConfig {
            brokers: "b:9092".into(),
            topic: KafkaSinkTopic::Fixed { name: "out".into() },
            auth: KafkaAuth::None,
            value_format: KafkaValueFormat::Json,
            key_format: None,
            key_path: None,
            partition_path: None,
            headers_path: None,
            on_key_error: OnKeyError::Fail,
            compression: CompressionType::None,
            acks: Acks::All,
            idempotent: true,
            linger: Duration::from_millis(5),
            batch_size: 16_384,
            message_timeout: Duration::from_secs(30),
            max_in_flight: 100,
            queue_full_backoff: Duration::from_millis(100),
            queue_full_max_retries: 3,
            extra_client_config: BTreeMap::new(),
        }
    }

    #[test]
    fn validate_accepts_minimal() {
        assert!(minimal().validate().is_ok());
    }

    #[test]
    fn validate_rejects_idempotent_without_acks_all() {
        let mut c = minimal();
        c.idempotent = true;
        c.acks = Acks::Leader;
        let err = c.validate().unwrap_err();
        assert!(format!("{err}").contains("idempotent"));
    }

    #[test]
    fn validate_rejects_empty_brokers() {
        let mut c = minimal();
        c.brokers = String::new();
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_fixed_topic() {
        let mut c = minimal();
        c.topic = KafkaSinkTopic::Fixed {
            name: String::new(),
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_from_path() {
        let mut c = minimal();
        c.topic = KafkaSinkTopic::FromPath {
            path: String::new(),
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_max_in_flight() {
        let mut c = minimal();
        c.max_in_flight = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_non_idempotent_with_acks_leader() {
        let mut c = minimal();
        c.idempotent = false;
        c.acks = Acks::Leader;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn acks_as_str_returns_librdkafka_values() {
        assert_eq!(Acks::None.as_str(), "0");
        assert_eq!(Acks::Leader.as_str(), "1");
        assert_eq!(Acks::All.as_str(), "all");
    }

    #[test]
    fn from_path_topic_round_trips() {
        let t = KafkaSinkTopic::FromPath {
            path: "$.dest".into(),
        };
        let v = serde_json::to_value(&t).unwrap();
        assert_eq!(v["type"], "from_path");
        assert_eq!(v["path"], "$.dest");
    }

    #[test]
    fn schema_compiles() {
        let _ = schemars::schema_for!(KafkaSinkConfig);
    }
}
