#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-rest
//!
//! A declarative, config-driven REST API client with pluggable authentication,
//! pagination, record transforms, schema inference, and incremental replication.

pub mod auth;
pub mod config;
pub mod extract;
pub mod pagination;
pub mod retry;
pub mod serde_helpers;
pub mod stream;

// Re-export core types so users don't need a separate faucet-core dependency.
pub use faucet_core::{
    FaucetError, RecordTransform, ReplicationMethod, Sink, Source, replication, schema, transform,
};

pub use auth::oauth2::DEFAULT_EXPIRY_RATIO;
pub use auth::token_endpoint::DEFAULT_TOKEN_ENDPOINT_EXPIRY_RATIO;
pub use auth::{Auth, ResponseValidator, fetch_oauth2_token, fetch_token_from_endpoint};
pub use config::RestStreamConfig;
pub use pagination::PaginationStyle;
pub use stream::RestStream;
