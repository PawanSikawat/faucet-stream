//! # Custom `faucet` binary with a third-party connector
//!
//! This is a worked example of connector plugin loading (issue #60). It builds
//! as a Cargo example of the `faucet-cli` crate, so from the repo root:
//!
//! ```console
//! $ cargo run --example custom-cli -- list
//! $ cargo run --example custom-cli -- run pipeline.yaml
//! ```
//!
//! In your own project you would instead depend on `faucet-cli` and your
//! connector crates, then write a `main.rs` exactly like the one below:
//!
//! ```toml
//! # Cargo.toml
//! [dependencies]
//! faucet-cli = "1"
//! faucet-core = "1"
//! faucet-source-lorem = "1"   # your third-party connector crate
//! ```
//!
//! The custom connector is usable from `faucet.yaml` exactly like a built-in —
//! `source: { type: lorem }` — across every command (`run`, `validate`,
//! `schema`, `list`, `preview`, `serve`, …).

use faucet_cli::registry::PluginRegistry;
use faucet_core::{async_trait, schema_for, FaucetError, JsonSchema, Source};
use faucet_core::serde_json::{json, Value};
use serde::Deserialize;
use std::collections::HashMap;

/// Config for the toy `lorem` source. In a real connector this lives in the
/// connector crate and derives `Serialize + Deserialize + JsonSchema`.
#[derive(Debug, Deserialize, JsonSchema)]
struct LoremConfig {
    /// How many placeholder records to emit.
    #[serde(default = "default_count")]
    count: usize,
}

fn default_count() -> usize {
    3
}

/// A toy source that emits `count` placeholder records.
struct LoremSource {
    count: usize,
}

impl LoremSource {
    fn from_value(cfg: Value) -> Result<Self, FaucetError> {
        let cfg: LoremConfig = faucet_core::serde_json::from_value(cfg)
            .map_err(|e| FaucetError::Config(format!("invalid lorem config: {e}")))?;
        Ok(Self { count: cfg.count })
    }
}

#[async_trait]
impl Source for LoremSource {
    async fn fetch_with_context(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok((0..self.count)
            .map(|i| json!({ "n": i, "text": "lorem ipsum" }))
            .collect())
    }

    fn config_schema(&self) -> Value {
        faucet_core::serde_json::to_value(schema_for!(LoremConfig)).unwrap_or(Value::Null)
    }

    fn connector_name(&self) -> &'static str {
        "lorem"
    }
}

fn main() -> std::process::ExitCode {
    let registry = PluginRegistry::with_builtins().register_source_with(
        "lorem",
        |cfg| Ok(Box::new(LoremSource::from_value(cfg)?)),
        || faucet_core::serde_json::to_value(schema_for!(LoremConfig)).unwrap_or(Value::Null),
        "Toy placeholder-record source (custom-cli example)",
    );
    faucet_cli::run_main(registry)
}
