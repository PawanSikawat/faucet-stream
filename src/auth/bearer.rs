//! Bearer token authentication.

use crate::error::FaucetError;
use reqwest::header::{HeaderMap, HeaderValue};

pub fn apply(headers: &mut HeaderMap, token: &str) -> Result<(), FaucetError> {
    let val = HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|e| FaucetError::Auth(format!("invalid bearer token value: {e}")))?;
    headers.insert("Authorization", val);
    Ok(())
}
