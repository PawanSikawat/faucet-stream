//! Bearer token authentication.

use faucet_core::FaucetError;
use reqwest::header::{HeaderMap, HeaderValue};

pub fn apply(headers: &mut HeaderMap, token: &str) -> Result<(), FaucetError> {
    let mut val = HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|e| FaucetError::Auth(format!("invalid bearer token value: {e}")))?;
    // Mark the token sensitive so reqwest/hyper redact it from trace/debug
    // logging (#78 LOW).
    val.set_sensitive(true);
    headers.insert("Authorization", val);
    Ok(())
}
