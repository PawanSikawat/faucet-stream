//! Bearer-token authentication for `/v1/*`. Constant-time comparison via
//! `subtle`; the `Authorization` header is the only accepted credential.

use crate::serve::error::ServeError;
use crate::serve::state::ServerState;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use subtle::ConstantTimeEq;

/// Timing-safe byte-slice equality (length-aware: differing lengths never match,
/// and the comparison does not early-exit on the first mismatching byte).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

/// Validate a raw `Authorization` header value against the expected token.
pub fn authorize_header(header: Option<&str>, expected: &str) -> Result<(), ServeError> {
    let value = header.ok_or(ServeError::Unauthorized)?;
    let token = value.strip_prefix("Bearer ").ok_or(ServeError::Unauthorized)?;
    if constant_time_eq(token.as_bytes(), expected.as_bytes()) {
        Ok(())
    } else {
        Err(ServeError::Unauthorized)
    }
}

/// Axum middleware enforcing bearer auth on `/v1/*`. A no-op when the server was
/// started with `--no-auth`. CORS preflight (`OPTIONS`) is allowed through so
/// browsers (which omit `Authorization` on preflight) work behind a CORS policy.
pub async fn require_auth(
    State(state): State<ServerState>,
    req: Request,
    next: Next,
) -> Result<Response, ServeError> {
    if req.method() == axum::http::Method::OPTIONS {
        return Ok(next.run(req).await);
    }
    if let Some(expected) = state.auth_token() {
        let header = req
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        authorize_header(header, expected)?;
    }
    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_matches_only_identical() {
        assert!(constant_time_eq(b"abc123", b"abc123"));
        assert!(!constant_time_eq(b"abc123", b"abc124"));
        assert!(!constant_time_eq(b"abc", b"abc123")); // length differs
    }

    #[test]
    fn authorize_accepts_correct_bearer() {
        assert!(authorize_header(Some("Bearer s3cret"), "s3cret").is_ok());
    }

    #[test]
    fn authorize_rejects_wrong_or_missing() {
        assert!(authorize_header(Some("Bearer nope"), "s3cret").is_err());
        assert!(authorize_header(None, "s3cret").is_err());
        assert!(authorize_header(Some("s3cret"), "s3cret").is_err()); // no "Bearer " prefix
        assert!(authorize_header(Some("Basic s3cret"), "s3cret").is_err());
    }
}
