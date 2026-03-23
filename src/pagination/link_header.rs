//! Link header pagination (RFC 8288).

use reqwest::header::HeaderMap;

/// Extract the rel="next" URL from a Link header.
pub fn extract_next_link(headers: &HeaderMap) -> Option<String> {
    let link = headers.get("link")?.to_str().ok()?;
    for part in link.split(',') {
        let part = part.trim();
        if part.contains("rel=\"next\"") {
            let start = part.find('<')? + 1;
            let end = part.find('>')?;
            return Some(part[start..end].to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn test_extract_next_link() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static(
                r#"<https://api.example.com/items?page=2>; rel="next", <https://api.example.com/items?page=5>; rel="last""#
            ),
        );
        assert_eq!(
            extract_next_link(&headers),
            Some("https://api.example.com/items?page=2".to_string()),
        );
    }

    #[test]
    fn test_no_next_link() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static(r#"<https://api.example.com/items?page=1>; rel="prev""#),
        );
        assert_eq!(extract_next_link(&headers), None);
    }

    #[test]
    fn test_empty_headers() {
        let headers = HeaderMap::new();
        assert_eq!(extract_next_link(&headers), None);
    }
}
