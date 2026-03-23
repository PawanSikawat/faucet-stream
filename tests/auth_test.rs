use faucet_stream::Auth;
use reqwest::header::HeaderMap;

#[test]
fn bearer_auth_sets_header() {
    let mut headers = HeaderMap::new();
    Auth::Bearer("test-token".into())
        .apply(&mut headers)
        .unwrap();
    assert_eq!(headers.get("authorization").unwrap(), "Bearer test-token");
}

#[test]
fn basic_auth_sets_header() {
    let mut headers = HeaderMap::new();
    Auth::Basic {
        username: "user".into(),
        password: "pass".into(),
    }
    .apply(&mut headers)
    .unwrap();
    let value = headers.get("authorization").unwrap().to_str().unwrap();
    assert!(value.starts_with("Basic "));
}

#[test]
fn api_key_sets_custom_header() {
    let mut headers = HeaderMap::new();
    Auth::ApiKey {
        header: "X-Api-Key".into(),
        value: "secret-123".into(),
    }
    .apply(&mut headers)
    .unwrap();
    assert_eq!(headers.get("x-api-key").unwrap(), "secret-123");
}

#[test]
fn no_auth_leaves_headers_empty() {
    let mut headers = HeaderMap::new();
    Auth::None.apply(&mut headers).unwrap();
    assert!(headers.is_empty());
}
