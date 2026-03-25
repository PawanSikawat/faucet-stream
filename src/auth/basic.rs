//! HTTP Basic authentication.

use crate::error::FaucetError;
use base64::Engine;
use reqwest::header::{HeaderMap, HeaderValue};

pub fn apply(headers: &mut HeaderMap, username: &str, password: &str) -> Result<(), FaucetError> {
    let encoded =
        base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
    let val = HeaderValue::from_str(&format!("Basic {encoded}"))
        .map_err(|e| FaucetError::Auth(format!("invalid basic auth value: {e}")))?;
    headers.insert("Authorization", val);
    Ok(())
}
