//! `faucet_serve_*` request metrics. The `path` label is the *matched route
//! template* (`/v1/runs/{id}`), never the raw path — cardinality safety.

use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::Response;
use crate::serve::history::RunStatus;
use crate::serve::state::ServerState;
use std::time::Instant;

/// The matched route template for a request, or `<unmatched>` for a 404.
pub fn matched_path_label(req: &Request) -> String {
    req.extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_owned())
        .unwrap_or_else(|| "<unmatched>".to_string())
}

/// Middleware: count every request + record its duration, labelled by method,
/// matched-path template, and response status.
pub async fn track_metrics(req: Request, next: Next) -> Response {
    let method = req.method().as_str().to_owned();
    let path = matched_path_label(&req);
    let start = Instant::now();
    let resp = next.run(req).await;
    let status = resp.status().as_u16().to_string();

    metrics::counter!(
        "faucet_serve_requests_total",
        "method" => method.clone(), "path" => path.clone(), "status" => status
    )
    .increment(1);
    // `status` is intentionally omitted from the histogram to cap series count
    // (a duration histogram per status code multiplies time-series); join with
    // the counter when a status breakdown is needed.
    metrics::histogram!(
        "faucet_serve_request_duration_seconds",
        "method" => method, "path" => path
    )
    .record(start.elapsed().as_secs_f64());

    resp
}

/// Refresh the queue-depth + in-flight gauges from the registry. Called on every
/// queue transition (submit, queued→running, finish).
pub fn set_run_gauges(state: &ServerState) {
    metrics::gauge!("faucet_serve_runs_queued").set(state.registry().queued() as f64);
    metrics::gauge!("faucet_serve_runs_in_flight").set(state.registry().in_flight() as f64);
}

/// Count a run reaching a terminal state, labelled by status + a finer reason.
pub fn record_run_finished(status: RunStatus, reason: &'static str) {
    metrics::counter!(
        "faucet_serve_runs_total",
        "status" => status.as_str(), "reason" => reason
    )
    .increment(1);
}

/// Count an idempotency-key replay hit.
pub fn record_idempotency_hit() {
    metrics::counter!("faucet_serve_idempotency_hits_total").increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Request;

    #[test]
    fn unmatched_path_falls_back_to_sentinel() {
        let req = Request::builder()
            .uri("/whatever")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(matched_path_label(&req), "<unmatched>");
    }

    #[test]
    fn run_finished_label_strings_are_stable() {
        // Guards against accidental relabeling (which would split Prometheus series).
        use crate::serve::history::RunStatus;
        assert_eq!(RunStatus::Completed.as_str(), "completed");
        assert_eq!(RunStatus::Cancelled.as_str(), "cancelled");
    }
}
