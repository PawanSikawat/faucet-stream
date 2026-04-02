//! # faucet-source-kafka
//!
//! Kafka consumer source connector for the faucet-stream ecosystem.
//!
//! Consumes messages from a Kafka topic, deserializes payloads as JSON,
//! and returns them as `serde_json::Value` records.

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::KafkaSourceConfig;
pub use stream::KafkaSource;
