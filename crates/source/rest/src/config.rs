//! Stream configuration and builder.

use crate::auth::Auth;
use crate::pagination::PaginationStyle;
use faucet_core::AuthSpec;
use faucet_core::{ReplicationBind, ReplicationMethod};
use reqwest::{
    Method,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

/// How to parse the response body into records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum ResponseFormat {
    /// JSON — extract records via `records_path` (JSONPath). The default.
    #[default]
    Json,
    /// CSV — parse a tabular file body (each row → a JSON object). For
    /// authenticated file endpoints (e.g. an export URL). Always available.
    Csv,
    /// Excel (`.xlsx`/`.xls`) — parse a workbook body. Requires the crate's
    /// `excel` feature.
    Excel,
}

fn default_csv_delimiter() -> u8 {
    b','
}
fn default_csv_has_headers() -> bool {
    true
}

/// Configuration for a RestStream.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RestStreamConfig {
    // ── Core request ──────────────────────────────────────────────────────────
    pub base_url: String,
    /// URL path, relative to `base_url`. May contain `{key}` placeholders that
    /// are substituted per-partition (e.g. `"/orgs/{org_id}/users"`).
    pub path: String,
    #[serde(with = "crate::serde_helpers::http_method")]
    #[schemars(with = "String")]
    pub method: Method,
    /// Authentication: either inline (`{ type, config }`) or a `{ ref: <name> }`
    /// pointer to a shared provider in the CLI's top-level `auth:` catalog.
    pub auth: AuthSpec<Auth>,
    /// Static request headers sent on **every** request (data pages, async-job
    /// submit/poll/fetch requests, and OData `$metadata` discovery probes).
    /// Applied *before* the auth provider's header placements, so an auth
    /// header of the same name always wins on a clash. Values honor
    /// `${env:}` / `${param.*}` load-time interpolation and pass through the
    /// secrets/redaction boundary like other config strings. Invalid header
    /// names/values are rejected at config load
    /// ([`FaucetError::Config`](faucet_core::FaucetError::Config)), never a
    /// mid-run panic.
    ///
    /// ```yaml
    /// headers:
    ///   Prefer: transient
    ///   Accept: application/json
    /// ```
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[schemars(with = "std::collections::HashMap<String, String>")]
    pub headers: HashMap<String, String>,
    pub query_params: HashMap<String, String>,
    /// Repeated / array-valued query params (#536), rendered as repeated keys —
    /// e.g. `{ "group_by[]": ["api_key_id", "model"] }` → `?group_by[]=api_key_id&group_by[]=model`.
    /// Applied alongside (in addition to) [`query_params`](Self::query_params);
    /// use this for APIs that need a key to appear more than once (`group_by[]`,
    /// repeated `expand`/`fields`). Values honor `{placeholder}` context
    /// substitution for child sources, like `query_params`. Empty by default.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub query_params_multi: HashMap<String, Vec<String>>,
    pub body: Option<Value>,

    // ── Pagination ────────────────────────────────────────────────────────────
    pub pagination: PaginationStyle,
    pub records_path: Option<String>,
    pub max_pages: Option<usize>,
    #[serde(with = "faucet_core::config::duration_secs_option", default)]
    #[schemars(with = "Option<u64>")]
    pub request_delay: Option<Duration>,

    // ── Reliability ───────────────────────────────────────────────────────────
    #[serde(with = "faucet_core::config::duration_secs_option", default)]
    #[schemars(with = "Option<u64>")]
    pub timeout: Option<Duration>,
    /// Number of retries (after the first attempt) for transient request
    /// failures. Default `3`.
    ///
    /// **Precedence note:** the REST source predates the unified pipeline
    /// `resilience:` policy. When this field (or [`retry_backoff`](Self::retry_backoff))
    /// is left at its default, an injected `RetryPolicy` (e.g. from a
    /// pipeline-level `resilience:` block, via
    /// [`RestStream::with_retry_policy`](crate::RestStream::with_retry_policy))
    /// governs the retry budget. Setting this field away from its default makes
    /// it win — an explicit per-connector value is never silently overridden by
    /// a pipeline-wide default.
    pub max_retries: u32,
    /// Base exponential-backoff delay between retries. Default `1s`. Shares the
    /// legacy-field precedence rule documented on [`max_retries`](Self::max_retries).
    #[serde(with = "faucet_core::config::duration_secs")]
    #[schemars(with = "u64")]
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
    /// Opt-in identifier used by [`Pipeline::with_state_store`](faucet_core::Pipeline::with_state_store)
    /// to persist this stream's bookmark across runs. When set, the pipeline
    /// will load any previously-stored bookmark before fetching and write the
    /// new bookmark only after the sink confirms the batch.
    ///
    /// Keys must satisfy [`faucet_core::state::validate_state_key`].
    pub state_key: Option<String>,

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
    /// Maximum number of partitions to fetch concurrently.
    /// `None` means sequential processing (backward compatible default).
    pub partition_concurrency: Option<usize>,

    // ── Mutual TLS ─────────────────────────────────────────────────────────────
    /// Optional client-certificate (mutual TLS) config. When set, the source
    /// presents a client certificate on **every** request — data requests and
    /// any inline auth token request (both go through the same HTTP client).
    /// Requires the crate's `mtls` feature; a `tls` block on a build without it
    /// is a load-time error rather than being silently ignored.
    #[serde(default)]
    pub tls: Option<TlsClientConfig>,

    // ── Response format (#497) ─────────────────────────────────────────────────
    /// How to parse the response body. `json` (default) uses JSONPath
    /// extraction (`records_path`); `csv` / `excel` parse a tabular **file**
    /// body into records — for authenticated file endpoints such as a Microsoft
    /// Graph / OneDrive / SharePoint `…/content` download or any signed export
    /// URL. In file mode a single response is fetched (pagination must be
    /// `none`) and `records_path` does not apply. `excel` requires the crate's
    /// `excel` feature.
    #[serde(default)]
    pub response_format: ResponseFormat,
    /// CSV field delimiter byte (default `,`). Used only when
    /// `response_format: csv`.
    #[serde(default = "default_csv_delimiter")]
    pub csv_delimiter: u8,
    /// Whether the first CSV row is a header row supplying field names
    /// (default `true`). When `false`, fields are named `column_0`, `column_1`, …
    #[serde(default = "default_csv_has_headers")]
    pub csv_has_headers: bool,
    /// Excel worksheet to read: a sheet name, or a 0-based index as a string.
    /// When omitted, the first worksheet is used. `response_format: excel` only.
    #[serde(default)]
    pub excel_sheet: Option<String>,
    /// 0-based index of the Excel header row (default `0`). Rows above it are
    /// skipped; the header row supplies field names. `response_format: excel` only.
    #[serde(default)]
    pub excel_header_row: usize,

    // ── Server-side incremental push-down (#513) ────────────────────────────────
    /// Bind the stored bookmark into the outgoing request (query param / header /
    /// body field / path) so the server returns only new rows. Composes with the
    /// existing `replication_key` client-side filter, which stays active as a
    /// safety net. Requires `replication_method: incremental` + `replication_key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication_bind: Option<ReplicationBind>,

    // ── OData (#512) ────────────────────────────────────────────────────────────
    /// Speak the OData protocol: `@odata.nextLink` paging, the `$.value`
    /// envelope, `$select`/`$filter`/`$expand`/`$orderby` sugar, and
    /// `$metadata` (EDMX) → schema discovery. When set, it derives the
    /// pagination, `records_path`, query params, and `Prefer` header at load
    /// time (explicit values still win). See [`ODataConfig`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub odata: Option<ODataConfig>,

    // ── Response-decode pipeline (#515) ─────────────────────────────────────────
    /// Decode the response body before record extraction: a chain of
    /// `extract` (JSONPath) / `base64` / `gunzip` / `unzip` / `parse`
    /// (json|csv|xlsx|xml) steps. Lets a source consume base64/compressed/file
    /// payloads (e.g. a base64 XLSX inside a SOAP body, or a gzipped-CSV export).
    /// When set, it replaces the `response_format` body parsing, and pagination
    /// must be `none`. See [`DecodeStep`](crate::decode::DecodeStep).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub decode: Vec<crate::decode::DecodeStep>,

    // ── Async-job pattern (#514) ────────────────────────────────────────────────
    /// Run a submit→poll→fetch job lifecycle instead of a single GET, for
    /// bulk/export/report-run APIs (Salesforce Bulk, Stripe Reporting, …). The
    /// fetched result flows through `decode:` / `response_format`. When set,
    /// pagination must be `none`. See [`AsyncJobConfig`](crate::async_job::AsyncJobConfig).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub async_job: Option<crate::async_job::AsyncJobConfig>,

    // ── In-run datetime window slicing (#527) ───────────────────────────────────
    /// Bound each request to a rolling `[start, end)` window between the stored
    /// bookmark and `now`, iterating the windows within one run (each `step`
    /// wide) with per-window bookmark durability. For APIs that require — or cap —
    /// a bounded date range (analytics/ads/reporting feeds). Parity with Airbyte's
    /// `DatetimeBasedCursor`. Requires `replication_method: incremental` +
    /// `replication_key`, and a start bookmark (from state, or
    /// `start_replication_value`). See [`WindowSpec`](faucet_core::WindowSpec).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<faucet_core::WindowSpec>,
}

/// OData protocol version, which selects the paging-link key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "lowercase")]
pub enum ODataVersion {
    /// OData v2 — JSON-light next link `odata.nextLink`.
    V2,
    /// OData v4 (default) — next link `@odata.nextLink`.
    #[default]
    V4,
}

impl ODataVersion {
    /// JSONPath to the next-page link for this version.
    pub fn next_link_path(self) -> &'static str {
        match self {
            // Bracketed single-quoted keys — `@`/`.` aren't bare-identifier
            // chars, and jsonpath-rust wants `$['key']`, not `$."key"`.
            ODataVersion::V2 => "$['odata.nextLink']",
            ODataVersion::V4 => "$['@odata.nextLink']",
        }
    }
}

/// OData protocol options for the REST source (#512).
///
/// A minimal block — `{ entity: Orders }` — is enough; it derives paging,
/// the `$.value` envelope, and (for `faucet discover`) `$metadata` parsing.
/// The query-option fields render into the standard `$select`/`$filter`/
/// `$expand`/`$orderby` params.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct ODataConfig {
    /// Protocol version (default `v4`).
    #[serde(default)]
    pub version: ODataVersion,
    /// Entity set to read (appended to `base_url` as the path, e.g. `Orders`).
    /// Optional when the path already names the entity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
    /// `$select` — columns to return.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub select: Vec<String>,
    /// `$expand` — related entities to inline (one level).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expand: Vec<String>,
    /// `$filter` — server-side filter expression (verbatim).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    /// `$orderby` — server-side ordering (verbatim).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orderby: Option<String>,
    /// Server page size, sent as `Prefer: odata.maxpagesize=<n>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<usize>,
}

pub use faucet_core::TlsClientConfig;

/// Build a validated [`HeaderMap`] from the static `headers` string map.
///
/// Invalid header names/values become a typed
/// [`FaucetError::Config`](faucet_core::FaucetError::Config) so a malformed
/// header fails at config load rather than panicking mid-run. Used both by
/// [`RestStreamConfig::validate`] (to fail loudly at load) and by the request
/// path (which reuses the already-validated map).
pub(crate) fn build_header_map(
    headers: &HashMap<String, String>,
) -> Result<HeaderMap, faucet_core::FaucetError> {
    let mut map = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        let hn = HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
            faucet_core::FaucetError::Config(format!("rest: invalid header name '{name}': {e}"))
        })?;
        let hv = HeaderValue::from_str(value).map_err(|e| {
            faucet_core::FaucetError::Config(format!(
                "rest: invalid value for header '{name}': {e}"
            ))
        })?;
        map.insert(hn, hv);
    }
    Ok(map)
}

impl Default for RestStreamConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            path: String::new(),
            method: Method::GET,
            auth: AuthSpec::Inline(Auth::None),
            headers: HashMap::new(),
            query_params: HashMap::new(),
            query_params_multi: HashMap::new(),
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
            state_key: None,
            name: None,
            primary_keys: Vec::new(),
            schema: None,
            schema_sample_size: 100,
            partitions: Vec::new(),
            partition_concurrency: None,
            tls: None,
            response_format: ResponseFormat::Json,
            csv_delimiter: b',',
            csv_has_headers: true,
            excel_sheet: None,
            excel_header_row: 0,
            replication_bind: None,
            odata: None,
            decode: Vec::new(),
            async_job: None,
            window: None,
        }
    }
}

impl RestStreamConfig {
    /// Validate cross-field invariants that serde alone can't express.
    ///
    /// File response formats (`csv` / `excel`) fetch a single response and
    /// parse the whole body, so paginated / JSONPath-extracted requests are
    /// rejected rather than silently ignored.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        // Static custom headers: reject an invalid header name/value at load
        // time rather than panicking on the first request (#539).
        build_header_map(&self.headers)?;
        if !matches!(self.response_format, ResponseFormat::Json) {
            if !matches!(self.pagination, PaginationStyle::None) {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `response_format: csv|excel` fetches a single file body and does not \
                     paginate — set `pagination: none`"
                        .into(),
                ));
            }
            if self.records_path.is_some() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `records_path` (JSONPath) does not apply to `response_format: csv|excel` \
                     — the whole file body becomes the record set"
                        .into(),
                ));
            }
        }
        if let Some(bind) = &self.replication_bind {
            bind.validate()?;
            if !matches!(self.replication_method, ReplicationMethod::Incremental) {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `replication_bind` requires `replication_method: incremental`".into(),
                ));
            }
            if self.replication_key.is_none() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `replication_bind` requires `replication_key` (the field whose \
                     bookmark is pushed down)"
                        .into(),
                ));
            }
        }
        if self.odata.is_some() && !matches!(self.response_format, ResponseFormat::Json) {
            return Err(faucet_core::FaucetError::Config(
                "rest: `odata` speaks JSON — remove `response_format: csv|excel`".into(),
            ));
        }
        if !self.decode.is_empty() {
            if !matches!(self.pagination, PaginationStyle::None) {
                return Err(faucet_core::FaucetError::Config(
                    "rest: a `decode:` pipeline consumes a single response body — set \
                     `pagination: none`"
                        .into(),
                ));
            }
            if !matches!(self.response_format, ResponseFormat::Json) {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `decode:` replaces `response_format` body parsing — remove \
                     `response_format: csv|excel`"
                        .into(),
                ));
            }
        }
        if let Some(job) = &self.async_job {
            job.validate()?;
            if !matches!(self.pagination, PaginationStyle::None) {
                return Err(faucet_core::FaucetError::Config(
                    "rest: an `async_job:` lifecycle fetches a single result — set \
                     `pagination: none`"
                        .into(),
                ));
            }
        }
        if let Some(window) = &self.window {
            window.validate()?;
            if !matches!(self.replication_method, ReplicationMethod::Incremental) {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `window` slicing requires `replication_method: incremental`".into(),
                ));
            }
            if self.replication_key.is_none() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `window` slicing requires `replication_key` (the datetime cursor field)"
                        .into(),
                ));
            }
            if self.async_job.is_some() {
                return Err(faucet_core::FaucetError::Config(
                    "rest: `window` slicing and `async_job` are mutually exclusive — the async-job \
                     lifecycle fetches a single result and does not slice by window"
                        .into(),
                ));
            }
        }
        Ok(())
    }

    /// Derive request defaults from the `odata:` block (paging, `$.value`
    /// envelope, `$select`/`$filter`/`$expand`/`$orderby` params, and the
    /// `Prefer` page-size header). Explicit config always wins — a field the
    /// user already set is never overwritten. Idempotent.
    pub fn apply_odata_defaults(&mut self) {
        let Some(odata) = self.odata.clone() else {
            return;
        };
        // Entity → path when the path doesn't already name one.
        if self.path.trim_matches('/').is_empty()
            && let Some(entity) = &odata.entity
        {
            self.path = entity.clone();
        }
        // OData records live under `$.value`.
        if self.records_path.is_none() {
            self.records_path = Some("$.value[*]".to_owned());
        }
        // Follow `@odata.nextLink` (v4) / `odata.nextLink` (v2).
        if matches!(self.pagination, PaginationStyle::None) {
            self.pagination = PaginationStyle::NextLinkInBody {
                next_link_path: odata.version.next_link_path().to_owned(),
            };
        }
        // Query-option sugar → standard params (don't clobber explicit ones).
        let mut set_param = |k: &str, v: String| {
            self.query_params.entry(k.to_owned()).or_insert(v);
        };
        if !odata.select.is_empty() {
            set_param("$select", odata.select.join(","));
        }
        if !odata.expand.is_empty() {
            set_param("$expand", odata.expand.join(","));
        }
        if let Some(filter) = &odata.filter {
            set_param("$filter", filter.clone());
        }
        if let Some(orderby) = &odata.orderby {
            set_param("$orderby", orderby.clone());
        }
        // Server page size via the `Prefer` header (case-insensitive check so a
        // user-set `Prefer:` is not double-inserted).
        if let Some(n) = odata.page_size
            && !self
                .headers
                .keys()
                .any(|k| k.eq_ignore_ascii_case("prefer"))
        {
            self.headers
                .insert("prefer".to_owned(), format!("odata.maxpagesize={n}"));
        }
    }

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
        self.auth = AuthSpec::Inline(a);
        self
    }

    /// Add a static request header. Validation is deferred to
    /// [`RestStream::new`](crate::RestStream::new) (via [`validate`](Self::validate)),
    /// so an invalid name/value surfaces as a typed
    /// [`FaucetError::Config`](faucet_core::FaucetError::Config) rather than
    /// panicking here.
    pub fn header(mut self, k: &str, v: &str) -> Self {
        self.headers.insert(k.to_string(), v.to_string());
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

    /// Attach a mutual-TLS client identity (requires the `mtls` feature at build
    /// time; otherwise [`RestStream::new`](crate::RestStream::new) errors).
    pub fn tls(mut self, tls: TlsClientConfig) -> Self {
        self.tls = Some(tls);
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

    /// Opt the stream into resumable runs by giving it a stable state key.
    /// When this is set and the [`Pipeline`](faucet_core::Pipeline) is
    /// configured with a state store, the previously persisted bookmark is
    /// applied to the stream before fetching.
    pub fn state_key(mut self, key: &str) -> Self {
        self.state_key = Some(key.into());
        self
    }

    /// Bind the stored bookmark into the outgoing request (#513).
    pub fn replication_bind(mut self, bind: ReplicationBind) -> Self {
        self.replication_bind = Some(bind);
        self
    }

    /// Slice the run into rolling `[start, end)` datetime windows (#527).
    pub fn window(mut self, window: faucet_core::WindowSpec) -> Self {
        self.window = Some(window);
        self
    }

    /// Speak OData: derive paging, the `$.value` envelope, the query-option
    /// sugar, and `$metadata` discovery from the block (#512).
    pub fn odata(mut self, odata: ODataConfig) -> Self {
        self.odata = Some(odata);
        self
    }

    /// Set the response-decode pipeline (#515).
    pub fn decode(mut self, steps: Vec<crate::decode::DecodeStep>) -> Self {
        self.decode = steps;
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

    /// Add a repeated / array-valued query parameter (#536): `key` is emitted
    /// once per value (`?key=v0&key=v1`). Chainable.
    pub fn add_query_param_multi(mut self, key: &str, values: Vec<String>) -> Self {
        self.query_params_multi.insert(key.to_string(), values);
        self
    }

    /// Set the maximum number of partitions to fetch concurrently.
    /// `None` (default) means sequential processing.
    pub fn partition_concurrency(mut self, concurrency: Option<usize>) -> Self {
        self.partition_concurrency = concurrency;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{BindFormat, BindTarget, ReplicationBind};

    fn bind() -> ReplicationBind {
        ReplicationBind {
            into: BindTarget::Query,
            name: "since".to_owned(),
            template: "${bookmark}".to_owned(),
            format: BindFormat::Raw,
            advance_from: None,
        }
    }

    #[test]
    fn replication_bind_requires_incremental_and_key() {
        // Bind without incremental method → error.
        let mut c = RestStreamConfig::new("https://x", "/y");
        c.replication_bind = Some(bind());
        assert!(c.validate().is_err());

        // Incremental but no replication_key → error.
        c.replication_method = ReplicationMethod::Incremental;
        assert!(c.validate().is_err());

        // Incremental + key → ok.
        c.replication_key = Some("updated_at".to_owned());
        assert!(c.validate().is_ok());

        // An invalid bind (empty name) is rejected too.
        let mut bad = c.clone();
        bad.replication_bind = Some(ReplicationBind {
            name: String::new(),
            ..bind()
        });
        assert!(bad.validate().is_err());
    }

    #[test]
    fn odata_rejects_non_json_response_format() {
        let mut c = RestStreamConfig::new("https://x", "");
        c.odata = Some(ODataConfig {
            entity: Some("Orders".to_owned()),
            ..Default::default()
        });
        c.response_format = ResponseFormat::Csv;
        assert!(c.validate().is_err());
    }

    #[test]
    fn apply_odata_defaults_renders_all_options_and_v2_link() {
        let mut c = RestStreamConfig::new("https://host/odata", "");
        c.odata = Some(ODataConfig {
            version: ODataVersion::V2,
            entity: Some("Orders".to_owned()),
            select: vec!["A".to_owned(), "B".to_owned()],
            expand: vec!["Lines".to_owned()],
            filter: Some("A gt 1".to_owned()),
            orderby: Some("A desc".to_owned()),
            page_size: Some(250),
        });
        c.apply_odata_defaults();

        assert_eq!(c.path, "Orders");
        assert_eq!(c.records_path.as_deref(), Some("$.value[*]"));
        assert_eq!(c.query_params.get("$select").unwrap(), "A,B");
        assert_eq!(c.query_params.get("$expand").unwrap(), "Lines");
        assert_eq!(c.query_params.get("$filter").unwrap(), "A gt 1");
        assert_eq!(c.query_params.get("$orderby").unwrap(), "A desc");
        assert_eq!(
            c.headers.get("prefer").map(String::as_str),
            Some("odata.maxpagesize=250")
        );
        // v2 uses the un-prefixed next-link key.
        assert!(matches!(
            c.pagination,
            crate::pagination::PaginationStyle::NextLinkInBody { ref next_link_path }
                if next_link_path == "$['odata.nextLink']"
        ));
        // Idempotent: a second application doesn't clobber explicit values.
        c.apply_odata_defaults();
        assert_eq!(c.query_params.get("$select").unwrap(), "A,B");
    }

    #[test]
    fn headers_serde_round_trip_as_string_map() {
        let mut c = RestStreamConfig::new("https://x", "/y");
        c.headers
            .insert("Prefer".to_owned(), "transient".to_owned());
        c.headers
            .insert("Accept".to_owned(), "application/json".to_owned());
        // Serializes as a plain JSON string map (schema-visible field).
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["headers"]["Prefer"], "transient");
        assert_eq!(v["headers"]["Accept"], "application/json");
        // And round-trips back into the string map.
        let back: RestStreamConfig = serde_json::from_value(v).unwrap();
        assert_eq!(
            back.headers.get("Prefer").map(String::as_str),
            Some("transient")
        );
        assert!(back.validate().is_ok());
    }

    #[test]
    fn validate_rejects_invalid_header_name() {
        let mut c = RestStreamConfig::new("https://x", "/y");
        c.headers
            .insert("Invalid Header".to_owned(), "v".to_owned());
        let err = c.validate().unwrap_err();
        assert!(
            matches!(err, faucet_core::FaucetError::Config(_)),
            "expected Config error, got {err:?}"
        );
        assert!(err.to_string().contains("invalid header name"), "{err}");
    }

    #[test]
    fn validate_rejects_invalid_header_value() {
        let mut c = RestStreamConfig::new("https://x", "/y");
        // A newline is not a legal header value byte.
        c.headers
            .insert("X-Bad".to_owned(), "line\nbreak".to_owned());
        let err = c.validate().unwrap_err();
        assert!(
            matches!(err, faucet_core::FaucetError::Config(_)),
            "{err:?}"
        );
    }

    #[test]
    fn odata_version_next_link_paths() {
        assert_eq!(ODataVersion::V4.next_link_path(), "$['@odata.nextLink']");
        assert_eq!(ODataVersion::V2.next_link_path(), "$['odata.nextLink']");
    }
}
