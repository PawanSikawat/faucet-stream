#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-graphql
//!
//! A config-driven GraphQL API source with cursor-based pagination,
//! JSONPath record extraction, and pluggable authentication.

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source, TlsClientConfig};

pub use config::{GraphqlAuth, GraphqlPagination, GraphqlStreamConfig};
pub use stream::GraphqlStream;
