//! # faucet-source-kafka
//!
//! Apache Kafka consumer source for `faucet-stream`. Subscribes to one or
//! more topics, drains messages until `max_messages` or `idle_timeout`, and
//! yields each message as a JSON object with key/value/topic/partition/
//! offset/timestamp/headers fields.

pub mod config;
pub mod decode;
pub mod state;
pub mod stream;

pub use faucet_core::{FaucetError, Source};
#[cfg(feature = "schema-registry")]
pub use faucet_kafka_common::SchemaRegistryConfig;
pub use faucet_kafka_common::{
    BasicAuth, CompressionType, KafkaAuth, KafkaValueFormat, OnDecodeError, OnKeyError,
    ScramMechanism,
};

pub use config::{KafkaSourceConfig, OffsetReset};
pub use stream::KafkaSource;
