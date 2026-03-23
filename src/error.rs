//! Error types for faucet-stream.

use thiserror::Error;

/// All possible errors returned by faucet-stream.
#[derive(Debug, Error)]
pub enum FaucetError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("JSONPath error: {0}")]
    JsonPath(String),

    #[error("Auth error: {0}")]
    Auth(String),

    #[error("Max pages reached: {0}")]
    MaxPages(usize),
}
