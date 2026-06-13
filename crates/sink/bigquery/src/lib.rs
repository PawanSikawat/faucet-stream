#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-bigquery
//!
//! BigQuery sink connector for the faucet-stream ecosystem.
//!
//! Writes `serde_json::Value` records to a Google BigQuery table using
//! the BigQuery streaming insert API.

pub mod config;
mod idempotent;
mod merge;
pub mod sink;

// Re-export core types.
pub use faucet_core::{FaucetError, Sink};

pub use config::{BigQueryCredentials, BigQuerySinkConfig};
pub use sink::BigQuerySink;
