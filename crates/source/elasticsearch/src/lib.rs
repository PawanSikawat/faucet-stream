#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-elasticsearch
//!
//! Elasticsearch search source connector for the faucet-stream ecosystem.
//!
//! Uses the scroll API to paginate through large result sets efficiently.

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{ElasticsearchAuth, ElasticsearchSourceConfig};
pub use stream::ElasticsearchSource;
