//! GraphQL stream executor.

use crate::config::{GraphqlAuth, GraphqlStreamConfig};
use async_trait::async_trait;
use base64::Engine as _;
use faucet_core::util::{self, DEFAULT_ERROR_BODY_MAX_LEN};
use faucet_core::{AuthSpec, Credential, FaucetError, SharedAuthProvider, Stream, StreamPage};
use jsonpath_rust::JsonPath;
use reqwest::Client;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

/// Retries on transient (5xx / connection) failures before giving up.
const RETRY_MAX_ATTEMPTS: u32 = 3;
/// Base exponential-backoff delay between retries.
const RETRY_BASE_BACKOFF: Duration = Duration::from_millis(500);

/// A configured GraphQL source that handles pagination and extraction.
pub struct GraphqlStream {
    config: GraphqlStreamConfig,
    client: Client,
    /// Optional shared auth provider. When set, it takes precedence over inline
    /// auth. Used by the CLI to resolve `auth: { ref }`, and by library callers
    /// who construct one provider and inject it into many sources.
    auth_provider: Option<SharedAuthProvider>,
    /// Retry policy for transient request failures. Defaulted in `new()` to
    /// reproduce the legacy `RETRY_MAX_ATTEMPTS` / `RETRY_BASE_BACKOFF`
    /// constants; overridable via [`with_retry_policy`](Self::with_retry_policy).
    retry_policy: faucet_core::RetryPolicy,
}

/// Map a [`Credential`] from a shared provider onto the GraphQL [`GraphqlAuth`]
/// representation so the existing header-application path can be reused.
fn credential_to_auth(cred: Credential) -> GraphqlAuth {
    match cred {
        Credential::Bearer(token) => GraphqlAuth::Bearer { token },
        Credential::Token(token) => GraphqlAuth::Custom {
            headers: HashMap::from([("Authorization".into(), token)]),
        },
        Credential::Header { name, value } => GraphqlAuth::Custom {
            headers: HashMap::from([(name, value)]),
        },
        Credential::Basic { username, password } => GraphqlAuth::Custom {
            headers: HashMap::from([(
                "Authorization".into(),
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD
                        .encode(format!("{username}:{password}"))
                ),
            )]),
        },
    }
}

impl GraphqlStream {
    /// Create a new GraphQL stream from the given configuration.
    pub fn new(config: GraphqlStreamConfig) -> Self {
        Self {
            config,
            client: Client::new(),
            auth_provider: None,
            // Reproduce the legacy `execute_with_retry(RETRY_MAX_ATTEMPTS,
            // RETRY_BASE_BACKOFF, …)` behavior exactly: `max_retries` is
            // retries-after-first, so `max_attempts = RETRY_MAX_ATTEMPTS + 1`.
            retry_policy: faucet_core::RetryPolicy {
                max_attempts: RETRY_MAX_ATTEMPTS + 1,
                backoff: faucet_core::BackoffKind::Exponential,
                base: RETRY_BASE_BACKOFF,
                max: Duration::from_secs(60),
                jitter: true,
                retry_on: faucet_core::RetryClassSet::default(),
            },
        }
    }

    /// Attach a custom [`RetryPolicy`](faucet_core::RetryPolicy) for transient
    /// request failures, replacing the default derived from
    /// `RETRY_MAX_ATTEMPTS` / `RETRY_BASE_BACKOFF`. Used by the CLI to inject a
    /// pipeline-level `resilience:` policy into the source.
    pub fn with_retry_policy(mut self, policy: faucet_core::RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set, the
    /// provider supplies the credential for every request (taking precedence
    /// over inline auth), so several sources can share one token with
    /// single-flight refresh. Used by the CLI to resolve `auth: { ref }`, and by
    /// library callers who construct one provider and inject it into many sources.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Fetch all records across all pages.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        self.fetch_all_with_context(&std::collections::HashMap::new())
            .await
    }

    /// Fetch all records, merging parent context values into GraphQL variables.
    async fn fetch_all_with_context(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut all_records = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages_fetched = 0usize;

        loop {
            if let Some(max) = self.config.max_pages
                && pages_fetched >= max
            {
                tracing::warn!("max pages ({max}) reached");
                break;
            }

            let body = self.execute_query(&cursor, context).await?;
            let records = self.extract_records(&body)?;
            all_records.extend(records);
            pages_fetched += 1;

            // Check pagination.
            match &self.config.pagination {
                Some(pag) => {
                    let has_next = extract_bool(&body, &pag.has_next_page_path).unwrap_or(false);
                    if !has_next {
                        break;
                    }
                    let next_cursor = extract_string(&body, &pag.cursor_path);
                    if next_cursor.is_none() {
                        break;
                    }
                    // Loop detection: if the server returns the same cursor we
                    // just used, advancing would re-fetch the identical page —
                    // stop now (compare against the just-used cursor, not a
                    // lagged one, so no extra duplicate page is fetched first;
                    // #78 LOW).
                    if next_cursor == cursor {
                        tracing::warn!("cursor loop detected, stopping pagination");
                        break;
                    }
                    cursor = next_cursor;
                }
                None => break,
            }
        }

        tracing::info!(
            records = all_records.len(),
            pages = pages_fetched,
            "GraphQL fetch complete"
        );
        Ok(all_records)
    }

    /// Execute a single GraphQL query, merging parent context into variables.
    async fn execute_query(
        &self,
        cursor: &Option<String>,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<Value, FaucetError> {
        let mut variables = self.config.variables.clone();

        // Merge parent context values into GraphQL variables.
        if !context.is_empty()
            && let Value::Object(ref mut map) = variables
        {
            for (key, value) in context {
                map.insert(key.clone(), value.clone());
            }
        }

        // Inject cursor and page size into variables.
        if let (Some(pag), Some(cursor_val)) = (&self.config.pagination, cursor)
            && let Value::Object(ref mut map) = variables
        {
            map.insert(pag.cursor_variable.clone(), json!(cursor_val));
        }
        // Inject `first:` (or whatever `page_size_variable` is named) from
        // `batch_size`. `batch_size = 0` is the "use upstream default"
        // sentinel — we omit the variable entirely in that case.
        if let Some(pag) = &self.config.pagination
            && self.config.batch_size != 0
            && let Value::Object(map) = &mut variables
        {
            map.insert(
                pag.page_size_variable.clone(),
                json!(self.config.batch_size),
            );
        }

        let payload = json!({
            "query": self.config.query,
            "variables": variables,
        });

        let mut req = self
            .client
            .post(&self.config.endpoint)
            .headers(self.config.headers.clone())
            .json(&payload);

        // Resolve credentials to concrete auth. A shared auth provider (from
        // `auth: { ref }` or injected by a library caller) takes precedence;
        // otherwise the inline auth config is used directly.
        let effective_auth: GraphqlAuth = if let Some(provider) = &self.auth_provider {
            credential_to_auth(provider.credential().await?)
        } else {
            match &self.config.auth {
                AuthSpec::Inline(a) => a.clone(),
                AuthSpec::Reference(r) => {
                    return Err(FaucetError::Auth(format!(
                        "auth references provider '{}' but no provider was supplied; \
                         set one via the CLI `auth:` catalog or `with_auth_provider`",
                        r.name
                    )));
                }
            }
        };

        // Apply resolved auth to the request.
        match effective_auth {
            GraphqlAuth::None => {}
            GraphqlAuth::Bearer { token } => {
                req = req.bearer_auth(token);
            }
            GraphqlAuth::Custom { headers } => {
                let mut hm = reqwest::header::HeaderMap::new();
                for (name, value) in &headers {
                    let n =
                        reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
                            FaucetError::Auth(format!("invalid custom header name {name:?}: {e}"))
                        })?;
                    let v = reqwest::header::HeaderValue::from_str(value).map_err(|e| {
                        FaucetError::Auth(format!("invalid custom header value for {name:?}: {e}"))
                    })?;
                    hm.insert(n, v);
                }
                req = req.headers(hm);
            }
        }

        // Retry transient failures (5xx / connection resets) with jittered
        // backoff, matching the REST source's reliability layer (#78/#16).
        // GraphQL-level `errors` in a 200 body are application errors and are
        // handled below — they are not retried here.
        let body: Value =
            faucet_core::execute_with_policy(&self.retry_policy, None, || {
                let attempt = req.try_clone();
                async move {
                    let req = attempt.ok_or_else(|| {
                        FaucetError::Source("graphql: request is not cloneable for retry".into())
                    })?;
                    let resp = req.send().await.map_err(FaucetError::Http)?;
                    let resp = util::check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
                    resp.json().await.map_err(FaucetError::Http)
                }
            })
            .await?;

        // Check for GraphQL-level errors.
        if let Some(errors) = body.get("errors")
            && let Some(arr) = errors.as_array()
            && !arr.is_empty()
        {
            let msg = arr
                .iter()
                .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
                .collect::<Vec<_>>()
                .join("; ");
            // Surface "first: must be non-null" / similar variable validation
            // errors as `FaucetError::Config` so callers can react to the
            // `batch_size = 0` sentinel hitting a schema that requires a
            // non-null page-size argument. Detect by message substring —
            // GraphQL servers don't standardise an error-code field.
            let lower = msg.to_lowercase();
            if self.config.batch_size == 0
                && let Some(pag) = &self.config.pagination
            {
                let var_name = pag.page_size_variable.to_lowercase();
                if lower.contains(&var_name)
                    && (lower.contains("non-null")
                        || lower.contains("non null")
                        || lower.contains("must not be null")
                        || lower.contains("cannot be null")
                        || lower.contains("required"))
                {
                    return Err(FaucetError::Config(format!(
                        "batch_size = 0 requires the upstream to accept a null {}: argument \
                         (GraphQL errors: {msg})",
                        pag.page_size_variable
                    )));
                }
            }
            return Err(FaucetError::HttpStatus {
                status: 200,
                url: self.config.endpoint.clone(),
                body: format!("GraphQL errors: {msg}"),
            });
        }

        Ok(body)
    }

    /// Extract records from a GraphQL response using the configured JSONPath.
    fn extract_records(&self, body: &Value) -> Result<Vec<Value>, FaucetError> {
        match &self.config.records_path {
            Some(path) => util::extract_records(body, Some(path)),
            None => {
                // GraphQL-specific: return the `data` field as a single
                // record. A `data` that is JSON null (or absent entirely)
                // means there is nothing to extract — emit an empty page
                // rather than forwarding a bogus null record to the sink
                // (#146 LOW).
                match body.get("data") {
                    Some(Value::Null) | None => Ok(Vec::new()),
                    Some(data) => Ok(vec![data.clone()]),
                }
            }
        }
    }

    /// Core pagination loop yielded as a [`StreamPage`] stream.
    ///
    /// Each upstream GraphQL response → one [`StreamPage`]. The page size
    /// variable in the request comes from [`GraphqlStreamConfig::batch_size`];
    /// `batch_size = 0` omits it so the upstream uses its own default page
    /// size and emits a single page.
    ///
    /// Bookmarks are always `None` — the GraphQL source has no
    /// incremental-replication mode today. The
    /// [`bookmark_emitted`-style trailing-checkpoint](https://github.com/PawanSikawat/faucet-stream/commit/e6fdca5)
    /// guard from the REST source is preserved structurally so any future
    /// incremental mode picks it up without re-deriving the pattern.
    fn stream_pages_inner(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + '_>> {
        // Own the context so it can live inside the async-stream generator.
        let owned_context: std::collections::HashMap<String, Value> = context.clone();

        Box::pin(async_stream::try_stream! {
            let mut cursor: Option<String> = None;
            let mut pages_fetched = 0usize;
            // No incremental replication today — `running_max` stays `None`.
            // The structure mirrors the REST source so a future replication
            // mode can plug into the same scaffolding without reworking the
            // bookmark guard.
            let running_max: Option<Value> = None;
            let mut bookmark_emitted = false;

            loop {
                if let Some(max) = self.config.max_pages
                    && pages_fetched >= max
                {
                    tracing::warn!("max pages ({max}) reached");
                    break;
                }

                let body = self.execute_query(&cursor, &owned_context).await?;
                let records = self.extract_records(&body)?;
                pages_fetched += 1;

                // Advance pagination state BEFORE yielding the current page,
                // so the bookmark is only attached on the final page.
                let has_next = match &self.config.pagination {
                    Some(pag) => {
                        let next = extract_bool(&body, &pag.has_next_page_path).unwrap_or(false);
                        if next {
                            let next_cursor = extract_string(&body, &pag.cursor_path);
                            match next_cursor {
                                None => false,
                                Some(next_cursor) => {
                                    // Loop detection: if the server returns the
                                    // same cursor we just used, advancing would
                                    // re-fetch the identical page — stop now
                                    // (comparing against the just-used cursor,
                                    // not a lagged one, so we don't fetch an
                                    // extra duplicate page first; #78 LOW).
                                    if Some(&next_cursor) == cursor.as_ref() {
                                        tracing::warn!("cursor loop detected, stopping pagination");
                                        false
                                    } else {
                                        cursor = Some(next_cursor);
                                        true
                                    }
                                }
                            }
                        } else {
                            false
                        }
                    }
                    None => false,
                };

                if has_next {
                    // Intermediate page — bookmark stays `None`.
                    yield StreamPage { records, bookmark: None };
                } else {
                    // Final page — attach the consolidated bookmark (always
                    // `None` until incremental mode lands).
                    bookmark_emitted = running_max.is_some();
                    yield StreamPage {
                        records,
                        bookmark: running_max.clone(),
                    };
                    break;
                }
            }

            // Trailing checkpoint: if the loop exited (e.g. via `max_pages`
            // truncation) without carrying the bookmark on a real page, emit
            // one empty page carrying it so the pipeline persists progress.
            // No-op today because `running_max` is always `None`, but kept so
            // a future incremental mode inherits the guard from the REST
            // source's regression fix (commit e6fdca5).
            if !bookmark_emitted && running_max.is_some() {
                yield StreamPage {
                    records: Vec::new(),
                    bookmark: running_max,
                };
            }

            tracing::info!(
                pages = pages_fetched,
                batch_size = self.config.batch_size,
                "GraphQL source stream complete",
            );
        })
    }
}

#[async_trait]
impl faucet_core::Source for GraphqlStream {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        self.fetch_all_with_context(context).await
    }

    /// Stream GraphQL responses page-by-page without buffering the full
    /// result set. The trait-level `batch_size` argument is ignored in
    /// favour of [`GraphqlStreamConfig::batch_size`] — the config field is
    /// the user-facing knob the README documents, and routing the
    /// pipeline-supplied hint through it would silently override an
    /// explicit config value.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        self.stream_pages_inner(context)
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(GraphqlStreamConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        faucet_core::redact_uri_credentials(&self.config.endpoint)
    }
}

fn extract_string(body: &Value, path: &str) -> Option<String> {
    let results = body.query(path).ok()?;
    match results.first()? {
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn extract_bool(body: &Value, path: &str) -> Option<bool> {
    let results = body.query(path).ok()?;
    results.first()?.as_bool()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_string_from_json() {
        let body = json!({"data": {"users": {"pageInfo": {"endCursor": "abc123"}}}});
        assert_eq!(
            extract_string(&body, "$.data.users.pageInfo.endCursor"),
            Some("abc123".into())
        );
    }

    #[test]
    fn extract_bool_from_json() {
        let body = json!({"data": {"users": {"pageInfo": {"hasNextPage": true}}}});
        assert_eq!(
            extract_bool(&body, "$.data.users.pageInfo.hasNextPage"),
            Some(true)
        );
    }

    #[test]
    fn extract_records_with_path() {
        let config =
            GraphqlStreamConfig::new("https://api.example.com/graphql", "query { users { id } }")
                .records_path("$.data.users[*]");
        let stream = GraphqlStream::new(config);
        let body = json!({"data": {"users": [{"id": 1}, {"id": 2}]}});
        let records = stream.extract_records(&body).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], 1);
    }

    #[test]
    fn extract_records_without_path_returns_data() {
        let config =
            GraphqlStreamConfig::new("https://api.example.com/graphql", "query { user { id } }");
        let stream = GraphqlStream::new(config);
        let body = json!({"data": {"user": {"id": 1}}});
        let records = stream.extract_records(&body).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["user"]["id"], 1);
    }

    #[test]
    fn extract_records_without_path_null_data_yields_empty() {
        // A response of `{"data": null}` must NOT emit a bogus null record:
        // `data` being JSON null means there is nothing to extract, so the
        // page is empty (#146 LOW).
        let config =
            GraphqlStreamConfig::new("https://api.example.com/graphql", "query { user { id } }");
        let stream = GraphqlStream::new(config);
        let body = json!({ "data": null });
        let records = stream.extract_records(&body).unwrap();
        assert!(
            records.is_empty(),
            "expected empty Vec for null `data`, got {records:?}"
        );
    }

    #[test]
    fn extract_records_without_path_absent_data_yields_empty() {
        // No `data` field at all → nothing to extract → empty page (matches
        // the null-data case rather than forwarding the whole body).
        let config =
            GraphqlStreamConfig::new("https://api.example.com/graphql", "query { user { id } }");
        let stream = GraphqlStream::new(config);
        let body = json!({ "extensions": { "foo": 1 } });
        let records = stream.extract_records(&body).unwrap();
        assert!(
            records.is_empty(),
            "expected empty Vec when `data` is absent, got {records:?}"
        );
    }

    #[test]
    fn dataset_uri_returns_endpoint() {
        use faucet_core::Source;
        let stream = GraphqlStream::new(GraphqlStreamConfig::new(
            "https://api.example.com/graphql",
            "query { id }",
        ));
        assert_eq!(stream.dataset_uri(), "https://api.example.com/graphql");
    }

    #[test]
    fn dataset_uri_redacts_credentials() {
        use faucet_core::Source;
        let stream = GraphqlStream::new(GraphqlStreamConfig::new(
            "https://user:pw@api.example.com/graphql",
            "query { id }",
        ));
        assert_eq!(stream.dataset_uri(), "https://api.example.com/graphql");
    }

    #[test]
    fn default_retry_policy_reproduces_legacy_constants() {
        let stream = GraphqlStream::new(GraphqlStreamConfig::new(
            "https://api.example.com/graphql",
            "query { id }",
        ));
        assert_eq!(stream.retry_policy.max_attempts, RETRY_MAX_ATTEMPTS + 1);
        assert_eq!(stream.retry_policy.base, RETRY_BASE_BACKOFF);
    }

    #[test]
    fn with_retry_policy_overrides_the_default() {
        let policy = faucet_core::RetryPolicy {
            max_attempts: 9,
            base: Duration::from_secs(7),
            ..faucet_core::RetryPolicy::default()
        };
        let stream = GraphqlStream::new(GraphqlStreamConfig::new(
            "https://api.example.com/graphql",
            "query { id }",
        ))
        .with_retry_policy(policy);
        assert_eq!(stream.retry_policy.max_attempts, 9);
        assert_eq!(stream.retry_policy.base, Duration::from_secs(7));
    }
}
