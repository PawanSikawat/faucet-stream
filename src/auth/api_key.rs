//! API key header authentication.

use crate::error::FaucetError;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

pub fn apply(headers: &mut HeaderMap, header: &str, value: &str) -> Result<(), FaucetError> {
    let name = HeaderName::from_bytes(header.as_bytes())
        .map_err(|e| FaucetError::Auth(format!("invalid header name '{header}': {e}")))?;
    let val = HeaderValue::from_str(value)
        .map_err(|e| FaucetError::Auth(format!("invalid header value: {e}")))?;
    headers.insert(name, val);
    Ok(())
}
