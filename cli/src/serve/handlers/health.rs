//! Liveness (`/healthz`), readiness (`/readyz`), and Prometheus (`/metrics`)
//! endpoints. All three are unauthenticated (probes / scrapers). Phase 1
//! `/readyz` is always-ready; it gains history-degraded / queue-full checks in
//! later phases.

use crate::serve::state::ServerState;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

/// Liveness: a responding handler means the process is alive.
pub async fn healthz() -> impl IntoResponse {
    StatusCode::OK
}

/// Readiness: ready to accept work. Phase 1 is unconditionally ready.
pub async fn readyz() -> impl IntoResponse {
    StatusCode::OK
}

/// Prometheus exposition rendered from serve's own recorder handle.
pub async fn metrics(State(state): State<ServerState>) -> Response {
    match state.render_metrics() {
        Some(body) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response(),
        // 503 (not 200) so a Prometheus scraper marks the target down rather
        // than silently recording a successful scrape with zero metrics. This
        // only fires if the recorder failed to install (e.g. a second server in
        // one process); in normal operation `render_metrics()` is `Some`.
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            "# metrics recorder not installed in this process\n",
        )
            .into_response(),
    }
}
