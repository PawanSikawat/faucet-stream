//! HTTP-facing error type. Every fallible serve handler returns `ServeError`,
//! which renders to a JSON `ApiError` body with the right status code.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

/// JSON error envelope: `{ "error": { "code": "...", "message": "..." } }`.
#[derive(Debug, Serialize)]
pub struct ApiError {
    pub error: ApiErrorBody,
}

#[derive(Debug, Serialize)]
pub struct ApiErrorBody {
    pub code: String,
    pub message: String,
}

/// All error outcomes a serve handler can produce. (More variants — 413, 429,
/// 409, 422 — arrive with the endpoints that raise them in later phases.)
#[derive(Debug)]
pub enum ServeError {
    Unauthorized,
    NotFound,
    BadConfig(String),
    Internal(String),
}

impl ServeError {
    pub fn status(&self) -> StatusCode {
        match self {
            ServeError::Unauthorized => StatusCode::UNAUTHORIZED,
            ServeError::NotFound => StatusCode::NOT_FOUND,
            ServeError::BadConfig(_) => StatusCode::BAD_REQUEST,
            ServeError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            ServeError::Unauthorized => "unauthorized",
            ServeError::NotFound => "not_found",
            ServeError::BadConfig(_) => "bad_config",
            ServeError::Internal(_) => "internal",
        }
    }

    fn message(&self) -> String {
        match self {
            ServeError::Unauthorized => "missing or invalid bearer token".into(),
            ServeError::NotFound => "not found".into(),
            ServeError::BadConfig(m) => m.clone(),
            ServeError::Internal(m) => m.clone(),
        }
    }

    pub fn api_error(&self) -> ApiError {
        // Scrub any resolved secret that reached the message.
        let message = crate::secrets::registry::redact(&self.message()).into_owned();
        ApiError {
            error: ApiErrorBody {
                code: self.code().to_string(),
                message,
            },
        }
    }
}

impl IntoResponse for ServeError {
    fn into_response(self) -> Response {
        (self.status(), Json(self.api_error())).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn maps_variants_to_status_codes() {
        assert_eq!(ServeError::Unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(ServeError::NotFound.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            ServeError::BadConfig("nope".into()).status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ServeError::Internal("boom".into()).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn body_carries_code_and_message() {
        let body = ServeError::NotFound.api_error();
        assert_eq!(body.error.code, "not_found");
        assert!(!body.error.message.is_empty());

        // Fixed-message variant carries the expected static text.
        let body = ServeError::Unauthorized.api_error();
        assert_eq!(body.error.code, "unauthorized");
        assert_eq!(body.error.message, "missing or invalid bearer token");
    }

    #[test]
    fn dynamic_message_variants_round_trip_to_body() {
        // BadConfig / Internal carry caller-supplied text — confirm it reaches
        // the body intact (the redaction pass leaves non-secret text unchanged).
        let body = ServeError::BadConfig("bad thing".into()).api_error();
        assert_eq!(body.error.code, "bad_config");
        assert_eq!(body.error.message, "bad thing");

        let body = ServeError::Internal("boom".into()).api_error();
        assert_eq!(body.error.code, "internal");
        assert_eq!(body.error.message, "boom");
    }
}
