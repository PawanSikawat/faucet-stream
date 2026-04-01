//! Custom header-based authentication.

use reqwest::header::HeaderMap;

pub fn apply(headers: &mut HeaderMap, custom: &HeaderMap) {
    headers.extend(custom.clone());
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderName, HeaderValue};

    #[test]
    fn apply_merges_custom_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("existing", HeaderValue::from_static("keep"));

        let mut custom = HeaderMap::new();
        custom.insert(
            HeaderName::from_static("x-custom"),
            HeaderValue::from_static("val1"),
        );
        custom.insert(
            HeaderName::from_static("x-another"),
            HeaderValue::from_static("val2"),
        );

        apply(&mut headers, &custom);

        assert_eq!(headers.get("existing").unwrap(), "keep");
        assert_eq!(headers.get("x-custom").unwrap(), "val1");
        assert_eq!(headers.get("x-another").unwrap(), "val2");
    }

    #[test]
    fn apply_overwrites_conflicting_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-key", HeaderValue::from_static("old"));

        let mut custom = HeaderMap::new();
        custom.insert(
            HeaderName::from_static("x-key"),
            HeaderValue::from_static("new"),
        );

        apply(&mut headers, &custom);
        assert_eq!(headers.get("x-key").unwrap(), "new");
    }

    #[test]
    fn apply_empty_custom_is_noop() {
        let mut headers = HeaderMap::new();
        headers.insert("existing", HeaderValue::from_static("value"));

        apply(&mut headers, &HeaderMap::new());
        assert_eq!(headers.len(), 1);
        assert_eq!(headers.get("existing").unwrap(), "value");
    }
}
