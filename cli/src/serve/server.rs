//! axum router assembly. The bind / graceful-shutdown serve loop is added in
//! the next task; this file currently only builds the router.

use crate::serve::config::ServeConfig;
use crate::serve::handlers::health;
use crate::serve::state::ServerState;
use crate::serve::{auth, metrics};
use axum::routing::get;
use axum::Router;
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
            .filter_map(|o| o.parse().ok())
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
