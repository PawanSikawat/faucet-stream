#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-postgres
//!
//! PostgreSQL sink connector for the faucet-stream ecosystem.
//!
//! Writes `serde_json::Value` records to a PostgreSQL table using `jsonb`
//! columns or dynamic column mapping.

pub mod config;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{PostgresColumnMapping, PostgresSinkConfig};
pub use sink::PostgresSink;
