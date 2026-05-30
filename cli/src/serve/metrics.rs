//! `faucet_serve_*` request metrics. The `path` label is the *matched route
//! template* (`/v1/runs/{id}`), never the raw path — cardinality safety.

use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::Response;
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
    metrics::histogram!(
        "faucet_serve_request_duration_seconds",
        "method" => method, "path" => path
    )
    .record(start.elapsed().as_secs_f64());

    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Request;

    #[test]
    fn unmatched_path_falls_back_to_sentinel() {
        let req = Request::builder().uri("/whatever").body(axum::body::Body::empty()).unwrap();
        assert_eq!(matched_path_label(&req), "<unmatched>");
    }
}
