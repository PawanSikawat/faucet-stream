//! # faucet-source-graphql
//!
//! A config-driven GraphQL API source with cursor-based pagination,
//! JSONPath record extraction, and pluggable authentication.

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::GraphqlStreamConfig;
pub use stream::GraphqlStream;
