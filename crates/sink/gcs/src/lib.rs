#![cfg_attr(docsrs, feature(doc_cfg))]

//! Google Cloud Storage sink connector.

mod config;
mod sink;

pub use config::GcsSinkConfig;
pub use faucet_gcs_common::GcsCredentials;
pub use sink::GcsSink;
