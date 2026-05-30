//! axum router assembly and the bind / graceful-shutdown serve loop.

use crate::error::{CliError, CliResult};
use crate::serve::config::ServeConfig;
use crate::serve::handlers::health;
use crate::serve::state::ServerState;
use crate::serve::{auth, metrics};
use axum::routing::get;
use axum::Router;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;

/// Build the full router. `/v1/*` routes are added in a later phase; here the
/// API sub-router is empty but carries the auth layer, and health/metrics are
/// public. The middleware stack (body-limit, request metrics, CORS) is wired
/// from the start so its ordering is correct.
///
/// Note: axum 0.8 rejects `route_layer` on a `Router` with no routes, so the
/// auth middleware is attached via `.layer(...)` on the empty api sub-router.
/// TODO(phase 2): switch /v1 routes to `route_layer` once routes land.
pub fn build_router(state: ServerState, config: &ServeConfig) -> Router {
    // Unauthenticated probe/scrape endpoints.
    let public = Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .route("/metrics", get(health::metrics));

    // Authenticated API surface (routes land in a later phase).
    // Using `.layer(...)` rather than `route_layer` because axum 0.8 rejects
    // `route_layer` on a Router with no routes.
    let api = Router::new().layer(axum::middleware::from_fn_with_state(
        state.clone(),
        auth::require_auth,
    ));

    // No origins configured → `CorsLayer::new()` emits no `Access-Control-*`
    // headers, so browsers block cross-origin requests (CORS effectively off).
    // The layer is a cheap pass-through for non-browser clients.
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

/// Boot the server: install observability, build the router, bind the listener,
/// and serve until SIGTERM / SIGINT, then drain.
pub async fn serve(config: ServeConfig) -> CliResult<()> {
    let prom = crate::serve::observability::install(&config.log_level);

    let shutdown = CancellationToken::new();
    let state = ServerState::new(&config, prom, shutdown.clone());
    let app = build_router(state, &config);

    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|e| CliError::Serve(format!("failed to bind {}: {e}", config.listen)))?;
    let local = listener
        .local_addr()
        .map_err(|e| CliError::Serve(e.to_string()))?;
    tracing::info!(listen = %local, "faucet serve listening");

    let shutdown_for_axum = shutdown.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            wait_for_signal().await;
            tracing::info!("shutdown signal received; draining");
            shutdown_for_axum.cancel();
        })
        .await
        .map_err(|e| CliError::Serve(format!("server error: {e}")))?;

    // A later phase awaits in-flight run tasks up to config.shutdown_grace, then cancels.
    Ok(())
}

/// Resolve on SIGTERM (Unix) or Ctrl-C (any platform).
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
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
