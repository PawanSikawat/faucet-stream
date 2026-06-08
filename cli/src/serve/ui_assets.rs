//! Embedded web-console assets (serve-ui feature). The static shell is PUBLIC;
//! all data stays behind the bearer-gated `/v1` API. Assets are embedded at
//! compile time from `src/serve/ui/` via `rust-embed`.

use crate::serve::error::ServeError;
use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};

#[derive(rust_embed::RustEmbed)]
#[folder = "src/serve/ui/"]
struct UiAssets;

/// Send embedded asset bytes with the right content-type and `no-cache`
/// (assets are tiny + embedded; correctness over caching — see spec §11).
fn serve_asset(path: &str) -> Response {
    match UiAssets::get(path) {
        Some(file) => {
            let mime = file.metadata.mimetype();
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, mime.to_string()),
                    (header::CACHE_CONTROL, "no-cache".to_string()),
                ],
                file.data.into_owned(),
            )
                .into_response()
        }
        None => ServeError::NotFound.into_response(),
    }
}

/// `GET /` → the SPA shell.
pub async fn index() -> Response {
    serve_asset("index.html")
}

/// `GET /assets/{*path}` → an embedded asset by relative path.
pub async fn asset(Path(path): Path<String>) -> Response {
    serve_asset(&path)
}

/// True when the request prefers an HTML document (so a deep-link / refresh of a
/// client-side route should receive the SPA shell rather than a JSON 404).
pub(crate) fn wants_html(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|a| a.contains("text/html"))
        .unwrap_or(false)
}

/// Router fallback: an HTML-accepting GET to an unmatched path returns the SPA
/// shell (enables hash-route deep links); everything else gets the standard JSON
/// 404 so the API's 404 shape is preserved.
pub async fn spa_fallback(headers: HeaderMap) -> Response {
    if wants_html(&headers) {
        index().await
    } else {
        ServeError::NotFound.into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn wants_html_true_for_browser_accept() {
        let mut h = HeaderMap::new();
        h.insert(
            header::ACCEPT,
            HeaderValue::from_static("text/html,application/xhtml+xml"),
        );
        assert!(wants_html(&h));
    }

    #[test]
    fn wants_html_false_for_json_or_missing() {
        let mut h = HeaderMap::new();
        h.insert(header::ACCEPT, HeaderValue::from_static("application/json"));
        assert!(!wants_html(&h));
        assert!(!wants_html(&HeaderMap::new()));
    }

    #[test]
    fn index_asset_is_embedded_and_html() {
        let resp = serve_asset("index.html");
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get(header::CONTENT_TYPE).unwrap();
        assert!(ct.to_str().unwrap().contains("text/html"));
    }

    #[test]
    fn missing_asset_is_not_found() {
        let resp = serve_asset("does-not-exist.xyz");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
