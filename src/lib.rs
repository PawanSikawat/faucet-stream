//! # faucet-stream
//!
//! A declarative, config-driven REST API client for Rust with pluggable
//! authentication, pagination, and JSONPath extraction.

pub mod auth;
pub mod config;
pub mod error;
pub mod extract;
pub mod pagination;
pub mod replication;
pub mod retry;
pub mod schema;
pub mod stream;
pub mod transform;

pub use auth::oauth2::DEFAULT_EXPIRY_RATIO;
pub use auth::{Auth, fetch_oauth2_token};
pub use config::RestStreamConfig;
pub use error::FaucetError;
pub use pagination::PaginationStyle;
pub use replication::ReplicationMethod;
pub use stream::RestStream;
pub use transform::RecordTransform;
