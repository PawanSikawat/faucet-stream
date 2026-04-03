//! # faucet-sink-elasticsearch
//!
//! Elasticsearch bulk index sink connector for the faucet-stream ecosystem.
//!
//! Writes JSON records to an Elasticsearch index using the `_bulk` API.

pub mod config;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{ElasticsearchSinkAuth, ElasticsearchSinkConfig};
pub use sink::ElasticsearchSink;
