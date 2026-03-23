//! Bearer token authentication.

use reqwest::header::{HeaderMap, HeaderValue};

pub fn apply(headers: &mut HeaderMap, token: &str) {
    headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token}")).expect("invalid bearer token"),
    );
}
