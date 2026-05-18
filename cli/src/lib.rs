//! # faucet-cli
//!
//! A config-driven runner for [`faucet-stream`](https://docs.rs/faucet-stream)
//! pipelines.  Define a source, optional transforms, a sink, and (optionally)
//! a state store in a YAML or JSON file, then run it with the `faucet` binary —
//! no Rust code required.
//!
//! The library half of this crate exposes the same building blocks the binary
//! uses (config parsing, env interpolation, the connector registry) so that
//! integrations and tests can reuse them.

pub mod cli;
pub mod commands;
pub mod config;
pub mod env_config;
pub mod env_loader;
pub mod error;
pub mod interpolate;
pub mod registry;
pub mod state;
pub mod transforms;

pub use error::{CliError, CliResult};
