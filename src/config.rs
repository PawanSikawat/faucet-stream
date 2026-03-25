//! Stream configuration and builder.

use crate::auth::Auth;
use crate::pagination::PaginationStyle;
use crate::replication::ReplicationMethod;
use crate::transform::RecordTransform;
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
    // ── Core request ──────────────────────────────────────────────────────────
    pub base_url: String,
    /// URL path, relative to `base_url`. May contain `{key}` placeholders that
    /// are substituted per-partition (e.g. `"/orgs/{org_id}/users"`).
    pub path: String,
    pub method: Method,
    pub auth: Auth,
    pub headers: HeaderMap,
    pub query_params: HashMap<String, String>,
    pub body: Option<Value>,

    // ── Pagination ────────────────────────────────────────────────────────────
    pub pagination: PaginationStyle,
    pub records_path: Option<String>,
    pub max_pages: Option<usize>,
    pub request_delay: Option<Duration>,

    // ── Reliability ───────────────────────────────────────────────────────────
    pub timeout: Option<Duration>,
    pub max_retries: u32,
    pub retry_backoff: Duration,
    /// HTTP status codes that should **not** cause an error. Responses with
    /// these codes are treated as empty pages (no records, no further pages).
    pub tolerated_http_errors: Vec<u16>,

    // ── Replication ───────────────────────────────────────────────────────────
    pub replication_method: ReplicationMethod,
    /// Field name (not a JSONPath) used for incremental replication bookmarking.
    pub replication_key: Option<String>,
    /// Bookmark value: records where `record[replication_key] <= start_replication_value`
    /// are filtered out when `replication_method` is `Incremental`.
    pub start_replication_value: Option<Value>,

    // ── Singer / Meltano metadata ─────────────────────────────────────────────
    /// Human-readable stream name (used in logging and Singer SCHEMA messages).
    pub name: Option<String>,
    /// Field names that uniquely identify a record (Singer `key_properties`).
    pub primary_keys: Vec<String>,
    /// JSON Schema describing the structure of each record.
    pub schema: Option<Value>,
    /// Maximum number of records to sample when inferring the schema via
    /// [`crate::stream::RestStream::infer_schema`].  `0` means sample all
    /// available records (up to `max_pages`).  Defaults to `100`.
    pub schema_sample_size: usize,

    // ── Partitions ────────────────────────────────────────────────────────────
    /// Each entry is a context map whose values are substituted into `path`
    /// placeholders. The stream is executed once per partition and results are
    /// concatenated.  Empty means run once with no substitution.
    pub partitions: Vec<HashMap<String, Value>>,

    // ── Record transforms ─────────────────────────────────────────────────────
    /// Transformations applied to every record in order.
    /// See [`RecordTransform`] for available options.
    pub transforms: Vec<RecordTransform>,
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
            tolerated_http_errors: Vec::new(),
            replication_method: ReplicationMethod::FullTable,
            replication_key: None,
            start_replication_value: None,
            name: None,
            primary_keys: Vec::new(),
            schema: None,
            schema_sample_size: 100,
            partitions: Vec::new(),
            transforms: Vec::new(),
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

    // ── Core request ──────────────────────────────────────────────────────────

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

    // ── Pagination ────────────────────────────────────────────────────────────

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

    // ── Reliability ───────────────────────────────────────────────────────────

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

    /// HTTP status codes that should be silently ignored (treated as empty pages).
    pub fn tolerate_http_error(mut self, status: u16) -> Self {
        self.tolerated_http_errors.push(status);
        self
    }

    // ── Replication ───────────────────────────────────────────────────────────

    pub fn replication_method(mut self, m: ReplicationMethod) -> Self {
        self.replication_method = m;
        self
    }

    /// Field name (not JSONPath) used as the incremental replication bookmark.
    pub fn replication_key(mut self, key: &str) -> Self {
        self.replication_key = Some(key.into());
        self
    }

    /// Bookmark start value: records at or before this value are filtered out
    /// when using `ReplicationMethod::Incremental`.
    pub fn start_replication_value(mut self, v: Value) -> Self {
        self.start_replication_value = Some(v);
        self
    }

    // ── Singer / Meltano metadata ─────────────────────────────────────────────

    /// Human-readable stream name.
    pub fn name(mut self, n: &str) -> Self {
        self.name = Some(n.into());
        self
    }

    /// Field names that uniquely identify a record (Singer `key_properties`).
    pub fn primary_keys(mut self, keys: Vec<String>) -> Self {
        self.primary_keys = keys;
        self
    }

    /// JSON Schema for the stream's records.
    pub fn schema(mut self, s: Value) -> Self {
        self.schema = Some(s);
        self
    }

    /// Maximum records to sample for schema inference (`0` = unlimited).
    pub fn schema_sample_size(mut self, n: usize) -> Self {
        self.schema_sample_size = n;
        self
    }

    // ── Partitions ────────────────────────────────────────────────────────────

    /// Add a partition context. The stream will execute once for each partition,
    /// substituting `{key}` placeholders in `path` with values from the context.
    pub fn add_partition(mut self, ctx: HashMap<String, Value>) -> Self {
        self.partitions.push(ctx);
        self
    }

    // ── Record transforms ─────────────────────────────────────────────────────

    /// Append a [`RecordTransform`] to the pipeline.
    ///
    /// Transforms are applied in the order they are added.
    pub fn add_transform(mut self, t: RecordTransform) -> Self {
        self.transforms.push(t);
        self
    }
}
