#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-graphql
//!
//! A config-driven GraphQL API source with cursor-based and
//! offset-into-variable pagination, JSONPath record extraction, and pluggable
//! authentication.

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source, TlsClientConfig};

pub use config::{
    GraphqlAuth, GraphqlOffsetPagination, GraphqlPagination, GraphqlPaginationSpec,
    GraphqlStreamConfig, OffsetPaginationKind,
};
pub use stream::GraphqlStream;
