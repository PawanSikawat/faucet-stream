//! # faucet-stream
//!
//! A declarative, config-driven REST API client for Rust with pluggable
//! authentication, pagination, record transforms, schema inference,
//! and incremental replication.
//!
//! ## Overview
//!
//! Configure a [`RestStreamConfig`] once, call [`RestStream::fetch_all()`] or
//! [`RestStream::fetch_all_as::<T>()`], and get all records — no manual
//! pagination loop, no auth boilerplate.
//!
//! ## Key capabilities
//!
//! - **Authentication**: Bearer, Basic, API Key (header or query), OAuth2
//!   (client credentials with automatic token caching), or custom headers
//! - **Pagination**: cursor/token, page number, offset/limit, Link header,
//!   or next-link-in-body — all with automatic loop detection
//! - **JSONPath extraction**: point at where records live in any JSON response
//! - **Record transforms**: flatten nested objects, regex key renaming,
//!   snake_case normalisation, or custom closures (feature-gated)
//! - **Schema inference**: derive a JSON Schema from sampled records via
//!   [`RestStream::infer_schema()`]
//! - **Incremental replication**: bookmark-based filtering via
//!   [`RestStream::fetch_all_incremental()`]
//! - **Partitions**: run the same config across multiple contexts
//!   (e.g. per-org, per-repo) with path placeholder substitution
//! - **Retries with backoff**: exponential backoff with 429 rate-limit handling
//! - **Streaming**: process pages as they arrive with [`RestStream::stream_pages()`]

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
