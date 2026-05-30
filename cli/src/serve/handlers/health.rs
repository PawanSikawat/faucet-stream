//! Liveness (`/healthz`), readiness (`/readyz`), and Prometheus (`/metrics`)
//! endpoints. All three are unauthenticated (probes / scrapers). Phase 1
//! `/readyz` is always-ready; it gains history-degraded / queue-full checks in
//! later phases.

use crate::serve::state::ServerState;
use axum::extract::State;
use axum::http::{header, StatusCode};
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
        Some(body) => (
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            body,
        )
            .into_response(),
        None => (
            StatusCode::OK,
            "# metrics recorder not installed in this process\n",
        )
            .into_response(),
    }
}
