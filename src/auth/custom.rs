//! Custom header-based authentication.

use reqwest::header::HeaderMap;

pub fn apply(headers: &mut HeaderMap, custom: &HeaderMap) {
    headers.extend(custom.clone());
}
