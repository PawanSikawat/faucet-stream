//! Next-link-in-body pagination.
//!
//! Handles APIs that embed the full URL of the next page in the response body
//! (e.g. `{"results": [...], "next_link": "https://api.example.com/workers?page=2"}`).
//! The extracted URL is used directly as the next request URL, identical to how
//! [`LinkHeader`](super::PaginationStyle::LinkHeader) uses the `Link` response header.

use faucet_core::FaucetError;
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
        // Absent or null → last page.
        None | Some(Value::Null) => {
            *next_link = None;
            Ok(false)
        }
        Some(Value::String(s)) => {
            if s.is_empty() {
                *next_link = None;
                Ok(false)
            } else {
                *next_link = Some(s.clone());
                Ok(true)
            }
        }
        // A next-page link must be a URL string; a number/bool/array/object at
        // the path means a misconfigured `next_link_path`. Fail loudly rather
        // than send a stringified JSON value as the next request URL (#78 LOW).
        Some(_) => Err(FaucetError::JsonPath(format!(
            "next-link path '{next_link_path}' resolved to a non-string value; the \
             next-page link must be a URL string"
        ))),
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

    #[test]
    fn non_string_next_link_errors() {
        // A numeric / object next_link is a misconfiguration — must error, not
        // stringify and send as the next URL (#78 LOW).
        let mut next_link = None;
        assert!(advance(&json!({"next_link": 5}), "$.next_link", &mut next_link).is_err());
        assert!(
            advance(
                &json!({"next_link": {"u": "x"}}),
                "$.next_link",
                &mut next_link
            )
            .is_err()
        );
    }
}
