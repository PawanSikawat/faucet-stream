//! HTTP Basic authentication.

use base64::Engine;
use reqwest::header::{HeaderMap, HeaderValue};

pub fn apply(headers: &mut HeaderMap, username: &str, password: &str) {
    let encoded =
        base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
    headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Basic {encoded}")).expect("invalid basic auth"),
    );
}
