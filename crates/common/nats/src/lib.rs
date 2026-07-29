#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-nats
//!
//! Shared configuration types for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
//! NATS source and sink connectors.
//!
//! - [`NatsAuth`] — authentication modes (None, Token, UserPassword, NKey,
//!   CredsFile) with a secret-safe [`std::fmt::Debug`].
//! - [`NatsConnectionConfig`] — the connection surface (`servers`, `auth`,
//!   `tls`, `name`) that both connectors `#[serde(flatten)]` into their config.
//! - [`connect`] — the single client builder both connectors use.
//!
//! All types derive `Serialize`, `Deserialize`, and `JsonSchema` so they
//! round-trip through YAML/JSON configs and CLI introspection.

pub mod auth;
pub mod connection;

pub use auth::NatsAuth;
pub use connection::{NatsConnectionConfig, connect};
