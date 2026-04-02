//! # faucet-sink-kafka
//!
//! Kafka producer sink connector for the faucet-stream ecosystem.
//!
//! Serializes `serde_json::Value` records as JSON and produces them to a Kafka topic.

pub mod config;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::KafkaSinkConfig;
pub use sink::KafkaSink;
