#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-http
//!
//! An HTTP POST sink connector that sends records to an HTTP endpoint,
//! either individually or as a JSON array batch.

pub mod config;
pub mod serde_helpers;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{HttpBatchMode, HttpSinkAuth, HttpSinkConfig};
pub use sink::HttpSink;
