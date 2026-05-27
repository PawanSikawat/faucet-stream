#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-s3
//!
//! AWS S3 sink connector for the faucet-stream ecosystem.
//!
//! Writes `serde_json::Value` records to S3 as JSON Lines files.

pub mod config;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::S3SinkConfig;
pub use sink::S3Sink;
