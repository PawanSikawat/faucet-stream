//! axum router assembly and the bind / graceful-shutdown serve loop.

use crate::error::{CliError, CliResult};
use crate::serve::config::{HistoryBackendSpec, ServeConfig};
use crate::serve::handlers::{health, runs};
use crate::serve::history::RunHistory;
use crate::serve::history::memory::MemoryHistory;
use crate::serve::state::ServerState;
use crate::serve::{auth, metrics};
use axum::Router;
use axum::routing::{get, post};
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;

/// Build the full router: unauthenticated probes + the bearer-guarded `/v1` API.
pub fn build_router(state: ServerState, config: &ServeConfig) -> Router {
    let public = Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .route("/metrics", get(health::metrics));

    // `/v1` routes guarded by the bearer middleware via `route_layer` (only runs
    // for matched routes; OPTIONS preflight is allowed through inside the layer).
    let api = Router::new()
        .route("/v1/runs", post(runs::submit_run).get(runs::list_runs))
        .route("/v1/runs/{id}", get(runs::get_run).delete(runs::delete_run))
        .route("/v1/runs/{id}/cancel", post(runs::cancel_run))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_auth,
        ));

    let cors = if config.cors_origins.is_empty() {
        CorsLayer::new()
    } else {
        let origins: Vec<axum::http::HeaderValue> = config
            .cors_origins
            .iter()
            .filter_map(|o| match o.parse() {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!(origin = %o, error = %e, "ignoring invalid --cors-origin");
                    None
                }
            })
            .collect();
        CorsLayer::new().allow_origin(AllowOrigin::list(origins))
    };

    public
        .merge(api)
        .layer(RequestBodyLimitLayer::new(config.body_limit_bytes))
        .layer(axum::middleware::from_fn(metrics::track_metrics))
        .layer(cors)
        .with_state(state)
}

/// Construct the run-history backend. Phase 2/3 supports only memory; the
/// Postgres/SQLite specs return a typed "not yet implemented" error (Phase 5).
async fn build_history(config: &ServeConfig) -> CliResult<Arc<dyn RunHistory>> {
    match &config.history {
        HistoryBackendSpec::Memory => {
            Ok(Arc::new(MemoryHistory::new(config.idempotency_retention)) as Arc<dyn RunHistory>)
        }
        HistoryBackendSpec::Postgres(_) | HistoryBackendSpec::Sqlite(_) => Err(CliError::Serve(
            "persistent run history (postgres/sqlite) is not yet available — \
             omit --history to use the in-memory backend (tracking: #127 Phase 5)"
                .into(),
        )),
    }
}

/// Load the optional `--default-config` once at startup, fully resolved, as a
/// merge base `Value`.
async fn load_default_base(config: &ServeConfig) -> CliResult<Option<Value>> {
    match &config.default_config_path {
        None => Ok(None),
        Some(path) => {
            let cfg = crate::config::PipelineConfig::from_path_async(path).await?;
            Ok(Some(serde_json::to_value(&cfg).map_err(|e| {
                CliError::Serve(format!("serializing --default-config: {e}"))
            })?))
        }
    }
}

/// Boot the server: install observability, build state + router, bind, serve
/// until SIGTERM/SIGINT, then drain in-flight runs up to the grace window.
pub async fn serve(config: ServeConfig) -> CliResult<()> {
    let prom = crate::serve::observability::install(&config.log_level);

    let history = build_history(&config).await?;
    history
        .recover_orphans()
        .await
        .map_err(|e| CliError::Serve(format!("history recovery: {e}")))?;
    let default_base = load_default_base(&config).await?;

    let shutdown = CancellationToken::new();
    let state = ServerState::new(&config, prom, shutdown.clone(), history, default_base);
    let app = build_router(state.clone(), &config);

    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|e| CliError::Serve(format!("failed to bind {}: {e}", config.listen)))?;
    let local = listener
        .local_addr()
        .map_err(|e| CliError::Serve(e.to_string()))?;
    tracing::info!(listen = %local, "faucet serve listening");

    // The HTTP graceful-shutdown future resolves on signal and stops accepting
    // new connections / drains in-flight HTTP — it does NOT cancel run tasks.
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            wait_for_signal().await;
            tracing::info!("shutdown signal received; draining in-flight runs");
        })
        .await
        .map_err(|e| CliError::Serve(format!("server error: {e}")))?;

    // Now drain run tasks: wait up to the grace window, then cancel the rest.
    let drained = tokio::time::timeout(config.shutdown_grace, state.registry().wait_drained()).await;
    if drained.is_err() {
        let remaining = state.registry().in_flight();
        tracing::warn!(remaining, "grace window expired; cancelling in-flight runs");
        shutdown.cancel();
        // Give cancelled tasks a brief moment to write their terminal status.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), state.registry().wait_drained())
            .await;
    }
    tracing::info!("faucet serve stopped");
    Ok(())
}

/// Resolve on SIGTERM (Unix) or Ctrl-C (any platform).
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
