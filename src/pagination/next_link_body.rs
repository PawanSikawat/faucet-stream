//! Next-link-in-body pagination.
//!
//! Handles APIs that embed the full URL of the next page in the response body
//! (e.g. `{"results": [...], "next_link": "https://api.example.com/workers?page=2"}`).
//! The extracted URL is used directly as the next request URL, identical to how
//! [`LinkHeader`](super::PaginationStyle::LinkHeader) uses the `Link` response header.

use crate::error::FaucetError;
use jsonpath_rust::JsonPath;
use serde_json::Value;

/// Extract the next-page URL from the response body using `next_link_path`.
///
/// Returns `true` and writes the URL into `next_link` when a non-null, non-empty
/// string is found at the path.  Returns `false` (and clears `next_link`) when
/// the path is absent, null, or an empty string — signalling the last page.
pub fn advance(
    body: &Value,
    next_link_path: &str,
    next_link: &mut Option<String>,
) -> Result<bool, FaucetError> {
    let results = body
        .query(next_link_path)
        .map_err(|e| FaucetError::JsonPath(format!("{e}")))?;
    match results.first() {
        Some(v) if !v.is_null() => {
            let url = v.as_str().unwrap_or(&v.to_string()).to_string();
            if url.is_empty() {
                *next_link = None;
                Ok(false)
            } else {
                *next_link = Some(url);
                Ok(true)
            }
        }
        _ => {
            *next_link = None;
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_next_link_url() {
        let body = json!({"results": [], "next_link": "https://api.example.com/workers?page=2"});
        let mut next_link = None;
        let has_next = advance(&body, "$.next_link", &mut next_link).unwrap();
        assert!(has_next);
        assert_eq!(
            next_link,
            Some("https://api.example.com/workers?page=2".into())
        );
    }

    #[test]
    fn stops_on_null() {
        let body = json!({"results": [], "next_link": null});
        let mut next_link = Some("stale".into());
        let has_next = advance(&body, "$.next_link", &mut next_link).unwrap();
        assert!(!has_next);
        assert!(next_link.is_none());
    }

    #[test]
    fn stops_when_field_absent() {
        let body = json!({"results": []});
        let mut next_link = None;
        let has_next = advance(&body, "$.next_link", &mut next_link).unwrap();
        assert!(!has_next);
        assert!(next_link.is_none());
    }

    #[test]
    fn stops_on_empty_string() {
        let body = json!({"results": [], "next_link": ""});
        let mut next_link = None;
        let has_next = advance(&body, "$.next_link", &mut next_link).unwrap();
        assert!(!has_next);
        assert!(next_link.is_none());
    }
}
