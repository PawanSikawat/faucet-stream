#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-websocket
//!
//! A WebSocket streaming source connector. Connects to a `ws://` or `wss://`
//! endpoint, optionally sends one or more subscription frames, and streams
//! each incoming message as a record until `max_messages`, `idle_timeout`, or
//! Ctrl-C terminates the run.

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{OnParseError, WebsocketAuth, WebsocketSourceConfig, WsMessageFormat};
pub use stream::WebsocketSource;
