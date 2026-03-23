//! Stream configuration and builder.

use crate::auth::Auth;
use crate::pagination::PaginationStyle;
use reqwest::{
    Method,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for a RestStream.
#[derive(Debug, Clone)]
pub struct RestStreamConfig {
    pub base_url: String,
    pub path: String,
    pub method: Method,
    pub auth: Auth,
    pub headers: HeaderMap,
    pub query_params: HashMap<String, String>,
    pub body: Option<Value>,
    pub pagination: PaginationStyle,
    pub records_path: Option<String>,
    pub max_pages: Option<usize>,
    pub request_delay: Option<Duration>,
    pub timeout: Option<Duration>,
    pub max_retries: u32,
    pub retry_backoff: Duration,
}

impl Default for RestStreamConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            path: String::new(),
            method: Method::GET,
            auth: Auth::None,
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: None,
            pagination: PaginationStyle::None,
            records_path: None,
            max_pages: Some(100),
            request_delay: None,
            timeout: Some(Duration::from_secs(30)),
            max_retries: 3,
            retry_backoff: Duration::from_secs(1),
        }
    }
}

impl RestStreamConfig {
    pub fn new(base_url: &str, path: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            path: path.to_string(),
            ..Default::default()
        }
    }

    pub fn method(mut self, m: Method) -> Self {
        self.method = m;
        self
    }
    pub fn auth(mut self, a: Auth) -> Self {
        self.auth = a;
        self
    }

    pub fn header(mut self, k: &str, v: &str) -> Self {
        self.headers.insert(
            HeaderName::from_bytes(k.as_bytes()).expect("invalid header name"),
            HeaderValue::from_str(v).expect("invalid header value"),
        );
        self
    }

    pub fn query(mut self, k: &str, v: &str) -> Self {
        self.query_params.insert(k.into(), v.into());
        self
    }

    pub fn body(mut self, b: Value) -> Self {
        self.body = Some(b);
        self
    }
    pub fn pagination(mut self, p: PaginationStyle) -> Self {
        self.pagination = p;
        self
    }
    pub fn records_path(mut self, p: &str) -> Self {
        self.records_path = Some(p.into());
        self
    }
    pub fn max_pages(mut self, n: usize) -> Self {
        self.max_pages = Some(n);
        self
    }
    pub fn request_delay(mut self, d: Duration) -> Self {
        self.request_delay = Some(d);
        self
    }
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }
    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    pub fn retry_backoff(mut self, d: Duration) -> Self {
        self.retry_backoff = d;
        self
    }
}
