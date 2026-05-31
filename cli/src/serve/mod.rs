//! `faucet serve` — HTTP control plane (#127). Runs pipeline configs submitted
//! over HTTP, reusing `executor::run_expanded`. Feature-gated on `serve`;
//! structured like `cli/src/schedule/`. See
//! `docs/superpowers/specs/2026-05-30-faucet-serve-design.md`.

pub mod auth;
pub mod config;
pub mod error;
pub mod handlers;
pub mod history;
pub mod idempotency;
pub mod load;
pub mod metrics;
pub mod observability;
pub mod registry;
pub mod server;
pub mod state;

pub use config::ServeConfig;

use crate::error::CliResult;

/// Boot the HTTP control plane and serve until SIGTERM/SIGINT.
pub async fn run_server(config: ServeConfig) -> CliResult<()> {
    server::serve(config).await
}
