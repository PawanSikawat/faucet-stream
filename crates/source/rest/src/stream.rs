//! The main REST stream executor.

use crate::auth::Auth;
use crate::auth::oauth2::TokenCache;
use crate::auth::token_endpoint::TokenEndpointCache;
use crate::config::RestStreamConfig;
use crate::extract;
use crate::pagination::{PaginationState, PaginationStyle};
use crate::retry;
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_core::replication::{
    ReplicationMethod, filter_incremental, max_replication_value, max_value,
};
use faucet_core::schema;
use faucet_core::transform::{self, CompiledTransform};
use futures_core::Stream;
use reqwest::Client;
use reqwest::header::HeaderMap;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

/// A configured REST API stream that handles pagination, auth, and extraction.
pub struct RestStream {
    config: RestStreamConfig,
    client: Client,
    /// Pre-compiled transforms (regex patterns compiled once at construction time).
    compiled_transforms: Vec<CompiledTransform>,
    /// Shared OAuth2 token cache (only used when `config.auth` is `Auth::OAuth2`).
    token_cache: TokenCache,
    /// Shared token endpoint cache (only used when `config.auth` is `Auth::TokenEndpoint`).
    token_endpoint_cache: TokenEndpointCache,
    /// Bookmark applied at runtime via
    /// [`Source::apply_start_bookmark`](faucet_core::Source::apply_start_bookmark).
    /// Takes precedence over `config.start_replication_value` when set.
    runtime_start: Arc<AsyncMutex<Option<Value>>>,
}

impl RestStream {
    /// Create a new stream from the given configuration.
    ///
    /// Returns [`FaucetError::Transform`] immediately if any `RenameKeys`
    /// transform contains an invalid regex pattern — fail-fast before any
    /// HTTP requests are made.
    pub fn new(config: RestStreamConfig) -> Result<Self, FaucetError> {
        // Validate expiry_ratio at construction time.
        let expiry_ratio_to_validate = match &config.auth {
            Auth::OAuth2 { expiry_ratio, .. } | Auth::TokenEndpoint { expiry_ratio, .. } => {
                Some(*expiry_ratio)
            }
            _ => None,
        };
        if let Some(ratio) = expiry_ratio_to_validate
            && (ratio <= 0.0 || ratio > 1.0)
        {
            return Err(FaucetError::Auth(format!(
                "expiry_ratio must be in (0.0, 1.0], got {ratio}"
            )));
        }

        let compiled_transforms = config
            .transforms
            .iter()
            .map(transform::compile)
            .collect::<Result<Vec<_>, _>>()?;

        let mut builder = Client::builder();
        if let Some(t) = config.timeout {
            builder = builder.timeout(t);
        }
        Ok(Self {
            config,
            client: builder.build()?,
            compiled_transforms,
            token_cache: TokenCache::new(),
            token_endpoint_cache: TokenEndpointCache::new(),
            runtime_start: Arc::new(AsyncMutex::new(None)),
        })
    }

    /// Fetch all records across all pages as raw JSON values.
    ///
    /// When `partitions` are configured, the stream is executed once per
    /// partition and all results are concatenated.
    ///
    /// When `replication_method` is `Incremental` and `replication_key` +
    /// `start_replication_value` are both set, records at or before the
    /// bookmark are filtered out.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        if self.config.partitions.is_empty() {
            self.fetch_partition(None, None).await
        } else if let Some(concurrency) = self.config.partition_concurrency {
            // Process partitions concurrently using a semaphore to limit parallelism.
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
            let mut handles = Vec::with_capacity(self.config.partitions.len());

            for ctx in &self.config.partitions {
                let permit =
                    semaphore.clone().acquire_owned().await.map_err(|e| {
                        FaucetError::Config(format!("semaphore acquire failed: {e}"))
                    })?;
                let fut = self.fetch_partition(Some(ctx), None);
                handles.push(async move {
                    let result = fut.await;
                    drop(permit);
                    result
                });
            }

            let results = futures::future::try_join_all(handles).await?;
            Ok(results.into_iter().flatten().collect())
        } else {
            let mut all_records = Vec::new();
            for ctx in &self.config.partitions {
                let records = self.fetch_partition(Some(ctx), None).await?;
                all_records.extend(records);
            }
            Ok(all_records)
        }
    }

    /// Fetch all records and deserialize into typed structs.
    pub async fn fetch_all_as<T: for<'de> Deserialize<'de>>(&self) -> Result<Vec<T>, FaucetError> {
        let values = self.fetch_all().await?;
        values
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(FaucetError::Json))
            .collect()
    }

    /// Infer a JSON Schema for this stream's records.
    ///
    /// If a `schema` is already set on the config, it is returned immediately
    /// without making any HTTP requests.
    ///
    /// Otherwise the stream fetches up to `schema_sample_size` records
    /// (respecting `max_pages`) and derives a JSON Schema from them.  Fields
    /// that are absent in some records, or that carry a `null` value, are
    /// marked as nullable (`["<type>", "null"]`).
    ///
    /// Set `schema_sample_size` to `0` to sample all available records.
    pub async fn infer_schema(&self) -> Result<Value, FaucetError> {
        if let Some(ref s) = self.config.schema {
            return Ok(s.clone());
        }
        let limit = match self.config.schema_sample_size {
            0 => None,
            n => Some(n),
        };
        let records = self.fetch_partition(None, limit).await?;
        Ok(schema::infer_schema(&records))
    }

    /// Fetch all records in incremental mode, returning the records along with
    /// the maximum value of `replication_key` observed across those records.
    ///
    /// The returned bookmark should be persisted by the caller and passed back
    /// as `start_replication_value` on the next run.
    ///
    /// If no `replication_key` is configured, this behaves identically to
    /// [`fetch_all`](Self::fetch_all) and the bookmark is `None`.
    pub async fn fetch_all_incremental(&self) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let records = self.fetch_all().await?;
        let bookmark = self
            .config
            .replication_key
            .as_deref()
            .and_then(|key| max_replication_value(&records, key))
            .cloned();
        Ok((records, bookmark))
    }

    /// Stream API pages without buffering the full result set.
    ///
    /// This is a thin convenience wrapper around the
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages) trait
    /// method — it discards bookmarks and yields one `Vec<Value>` per
    /// upstream API page. Use the trait method directly if you need
    /// per-page bookmarks for incremental replication.
    ///
    /// Note: partitions are not supported by `stream_pages`. Use `fetch_all`
    /// for multi-partition streams.
    ///
    /// ```rust,no_run
    /// use faucet_source_rest::{RestStream, RestStreamConfig};
    /// use futures::StreamExt;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let stream = RestStream::new(RestStreamConfig::new("https://api.example.com", "/items"))?;
    /// let mut pages = stream.stream_pages();
    /// while let Some(page) = pages.next().await {
    ///     let records = page?;
    ///     println!("got {} records", records.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn stream_pages(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<Value>, FaucetError>> + Send + '_>> {
        let mut inner = self.stream_pages_inner(None);
        Box::pin(async_stream::try_stream! {
            loop {
                let page = std::future::poll_fn(|cx| inner.as_mut().poll_next(cx)).await;
                match page {
                    Some(Ok(p)) => yield p.records,
                    Some(Err(e)) => Err(e)?,
                    None => break,
                }
            }
        })
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Core pagination loop shared by [`Source::stream_pages`] and
    /// [`fetch_partition`](Self::fetch_partition).
    ///
    /// Yields one [`faucet_core::StreamPage`] per page. The final page carries
    /// the consolidated replication bookmark (`Some(value)`); all intermediate
    /// pages carry `None`. When `context` is `Some`, path placeholders are
    /// substituted for partition support.
    fn stream_pages_inner(
        &self,
        context: Option<&HashMap<String, Value>>,
    ) -> Pin<Box<dyn Stream<Item = Result<faucet_core::StreamPage, FaucetError>> + Send + '_>> {
        // Clone the context into an owned map so it can live inside the
        // `async_stream` generator without borrowing from the caller.
        let owned_context: Option<HashMap<String, Value>> = context.cloned();

        Box::pin(async_stream::try_stream! {
            // Resolve the effective start-bookmark once at the top of the stream.
            // A runtime override (applied via `Source::apply_start_bookmark` —
            // typically by the pipeline reading from a `StateStore`) takes
            // precedence over the static config value.
            let effective_start: Option<Value> = {
                let guard = self.runtime_start.lock().await;
                guard
                    .clone()
                    .or_else(|| self.config.start_replication_value.clone())
            };

            let mut state = PaginationState::default();
            let mut pages_fetched = 0usize;
            let mut running_max: Option<Value> = effective_start.clone();
            let mut bookmark_emitted = false;

            loop {
                if let Some(max) = self.config.max_pages
                    && pages_fetched >= max
                {
                    tracing::warn!("max pages ({max}) reached");
                    break;
                }

                let mut params = self.config.query_params.clone();
                self.config.pagination.apply_params(&mut params, &state);

                let url_override = match &self.config.pagination {
                    PaginationStyle::LinkHeader | PaginationStyle::NextLinkInBody { .. } => {
                        state.next_link.clone()
                    }
                    _ => None,
                };

                let params_clone = params.clone();
                let ctx_ref = owned_context.as_ref();
                let (body, resp_headers) = retry::execute_with_retry(
                    self.config.max_retries,
                    self.config.retry_backoff,
                    || self.execute_request(&params_clone, url_override.as_deref(), ctx_ref),
                )
                .await?;

                let raw_records =
                    extract::extract_records(&body, self.config.records_path.as_deref())?;
                let raw_count = raw_records.len();

                let records =
                    if self.config.replication_method == ReplicationMethod::Incremental {
                        if let (Some(key), Some(start)) =
                            (&self.config.replication_key, effective_start.as_ref())
                        {
                            filter_incremental(raw_records, key, start)
                        } else {
                            raw_records
                        }
                    } else {
                        raw_records
                    };

                let records: Vec<Value> = records
                    .into_iter()
                    .map(|rec| transform::apply_all(rec, &self.compiled_transforms))
                    .collect();

                // Track the running max replication value across pages so the
                // final page can carry the consolidated bookmark.
                if self.config.replication_method == ReplicationMethod::Incremental
                    && let Some(key) = self.config.replication_key.as_deref()
                        && let Some(page_max) = max_replication_value(&records, key) {
                            let page_max = page_max.clone();
                            running_max = Some(match running_max.take() {
                                Some(prev) => max_value(prev, page_max),
                                None => page_max,
                            });
                        }

                // Advance pagination state to learn whether there is a next
                // page BEFORE yielding the current one. This way the bookmark
                // is only attached to pages where `has_next == false`, and we
                // never pre-fetch the next page just to classify the current
                // one as "final" (which would prevent early exit in callers
                // such as `fetch_partition` with `max_records`).
                let has_next = self
                    .config
                    .pagination
                    .advance(&body, &resp_headers, &mut state, raw_count)?;
                pages_fetched += 1;

                if has_next {
                    // Intermediate page — yield without bookmark so the
                    // pipeline does not persist a partial checkpoint.
                    yield faucet_core::StreamPage { records, bookmark: None };
                } else {
                    // Final page — attach the consolidated bookmark.
                    bookmark_emitted = running_max.is_some();
                    yield faucet_core::StreamPage {
                        records,
                        bookmark: running_max.clone(),
                    };
                    break;
                }

                if let Some(delay) = self.config.request_delay {
                    tokio::time::sleep(delay).await;
                }
            }

            // Trailing checkpoint: if the loop exited without carrying the
            // bookmark on a real page (e.g. via max_pages truncation, or with
            // zero pages fetched and a seeded start bookmark), emit one empty
            // page carrying the consolidated bookmark so the pipeline still
            // persists incremental progress and the next run resumes from here.
            if !bookmark_emitted && running_max.is_some() {
                yield faucet_core::StreamPage {
                    records: Vec::new(),
                    bookmark: running_max,
                };
            }
        })
    }

    /// Run the full pagination loop for a single partition context.
    ///
    /// `max_records`: when `Some(n)`, stop collecting after `n` records
    /// (used for schema sampling).
    async fn fetch_partition(
        &self,
        context: Option<&HashMap<String, Value>>,
        max_records: Option<usize>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut all_records = Vec::new();
        let mut pages_fetched = 0usize;
        let mut pages = self.stream_pages_inner(context);

        // Poll the stream without requiring StreamExt (avoids extra dependency).
        loop {
            let page = std::future::poll_fn(|cx: &mut std::task::Context<'_>| {
                pages.as_mut().poll_next(cx)
            })
            .await;

            match page {
                Some(Ok(page)) => {
                    pages_fetched += 1;
                    let records = page.records;
                    match max_records {
                        Some(limit) => {
                            let remaining = limit.saturating_sub(all_records.len());
                            all_records.extend(records.into_iter().take(remaining));
                            if all_records.len() >= limit {
                                break;
                            }
                        }
                        None => all_records.extend(records),
                    }
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }

        tracing::info!(
            stream = self.config.name.as_deref().unwrap_or("(unnamed)"),
            records = all_records.len(),
            pages = pages_fetched,
            "fetch complete"
        );
        Ok(all_records)
    }

    /// Execute a single HTTP request and return the response body and headers.
    ///
    /// - When `url_override` is `Some`, that full URL is used and query params
    ///   are **not** appended (Link header pagination encodes them in the URL).
    /// - When `path_context` is `Some`, `{key}` placeholders in `config.path`
    ///   are substituted with values from the context map (partition support).
    async fn execute_request(
        &self,
        params: &HashMap<String, String>,
        url_override: Option<&str>,
        path_context: Option<&HashMap<String, Value>>,
    ) -> Result<(Value, HeaderMap), FaucetError> {
        let use_override = url_override.is_some();
        let url = match url_override {
            Some(u) => u.to_string(),
            None => {
                let path = match path_context {
                    Some(ctx) => faucet_core::util::substitute_context(&self.config.path, ctx),
                    None => self.config.path.clone(),
                };
                format!("{}/{}", self.config.base_url, path.trim_start_matches('/'))
            }
        };

        // Resolve OAuth2 / TokenEndpoint credentials to a Bearer token before
        // applying auth headers. Tokens are cached and reused until they expire,
        // avoiding a token fetch on every HTTP request.
        let resolved_auth = match &self.config.auth {
            Auth::OAuth2 {
                token_url,
                client_id,
                client_secret,
                scopes,
                expiry_ratio,
            } => {
                let token = self
                    .token_cache
                    .get_or_refresh(
                        &self.client,
                        token_url,
                        client_id,
                        client_secret,
                        scopes,
                        *expiry_ratio,
                    )
                    .await?;
                Auth::Bearer { token }
            }
            Auth::TokenEndpoint {
                url: token_url,
                method: token_method,
                headers: token_headers,
                body: token_body,
                token_path,
                expiry_path,
                expiry_ratio,
                response_validator,
            } => {
                let token = self
                    .token_endpoint_cache
                    .get_or_refresh(
                        &self.client,
                        token_url,
                        token_method,
                        token_headers,
                        token_body.as_ref(),
                        token_path,
                        expiry_path.as_deref(),
                        *expiry_ratio,
                        response_validator.as_ref(),
                    )
                    .await?;
                Auth::Bearer { token }
            }
            other => other.clone(),
        };

        let mut headers = self.config.headers.clone();
        resolved_auth.apply(&mut headers)?;

        let mut req = self
            .client
            .request(self.config.method.clone(), &url)
            .headers(headers);

        if !use_override {
            // When parent context is available, substitute {placeholders} in
            // query param values so child sources can be parameterised.
            if let Some(ctx) = path_context {
                let substituted: HashMap<String, String> = params
                    .iter()
                    .map(|(k, v)| (k.clone(), faucet_core::util::substitute_context(v, ctx)))
                    .collect();
                req = req.query(&substituted.iter().collect::<Vec<_>>());
            } else {
                req = req.query(params);
            }
        }

        // ApiKeyQuery: inject the API key as a query parameter.
        if let Auth::ApiKeyQuery { param, value } = &self.config.auth {
            req = req.query(&[(param.as_str(), value.as_str())]);
        }

        if let Some(body) = &self.config.body {
            // Substitute context into body string values when available.
            if let Some(ctx) = path_context {
                let body_str = body.to_string();
                let substituted = faucet_core::util::substitute_context(&body_str, ctx);
                let substituted_value: Value =
                    serde_json::from_str(&substituted).unwrap_or(Value::String(substituted));
                req = req.json(&substituted_value);
            } else {
                req = req.json(body);
            }
        }

        let resp = req.send().await?;
        let status = resp.status();

        // 429 Too Many Requests: honour Retry-After before retrying.
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let wait = parse_retry_after(resp.headers());
            return Err(FaucetError::RateLimited(wait));
        }

        // Tolerated errors: treat as empty page.
        if self.config.tolerated_http_errors.contains(&status.as_u16()) {
            tracing::debug!(
                status = status.as_u16(),
                "tolerated HTTP error; treating as empty page"
            );
            return Ok((Value::Array(vec![]), HeaderMap::new()));
        }

        // For non-success responses, capture the body for debugging before
        // returning the error. This gives callers (and logs) the server's
        // error message rather than just a status code.
        if !status.is_success() {
            let resp_url = resp.url().to_string();
            let body_text = resp.text().await.unwrap_or_default();
            // Truncate very long error bodies to avoid bloating logs/errors.
            let truncated = if body_text.len() > 1024 {
                // Find a safe UTF-8 boundary at or before 1024 bytes.
                let end = body_text.floor_char_boundary(1024);
                format!("{}...(truncated)", &body_text[..end])
            } else {
                body_text
            };
            return Err(FaucetError::HttpStatus {
                status: status.as_u16(),
                url: resp_url,
                body: truncated,
            });
        }

        let resp_headers = resp.headers().clone();
        let body: Value = resp.json().await?;
        Ok((body, resp_headers))
    }
}

/// Parse the `Retry-After` header as a number of seconds.
/// Falls back to 60 s if the header is absent or unparseable.
fn parse_retry_after(headers: &HeaderMap) -> Duration {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(60))
}

#[async_trait]
impl faucet_core::Source for RestStream {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        if context.is_empty() {
            // No parent context — use normal fetch_all with partitions
            RestStream::fetch_all(self).await
        } else if self.config.partitions.is_empty() {
            // Parent context, no partitions — use context directly as partition context
            self.fetch_partition(Some(context), None).await
        } else {
            // Both parent context and partitions — merge context into each partition
            let mut all_records = Vec::new();
            for partition in &self.config.partitions {
                let mut merged = context.clone();
                merged.extend(partition.iter().map(|(k, v)| (k.clone(), v.clone())));
                all_records.extend(self.fetch_partition(Some(&merged), None).await?);
            }
            Ok(all_records)
        }
    }

    async fn fetch_with_context_incremental(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let records = self.fetch_with_context(context).await?;
        let bookmark = self
            .config
            .replication_key
            .as_deref()
            .and_then(|key| faucet_core::replication::max_replication_value(&records, key))
            .cloned();
        Ok((records, bookmark))
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(RestStreamConfig))
            .expect("schema serialization")
    }

    fn state_key(&self) -> Option<String> {
        self.config.state_key.clone()
    }

    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<faucet_core::StreamPage, FaucetError>> + Send + 'a>> {
        // RestStream chunks by upstream-API page boundaries, not by an
        // in-memory `batch_size` knob. The arg is accepted for trait
        // conformance and reserved for a future `page_size` mapping.
        self.stream_pages_inner(Some(context))
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self.runtime_start.lock().await = Some(bookmark);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_substitute_context_substitutes_placeholders() {
        let mut ctx = HashMap::new();
        ctx.insert("org_id".to_string(), json!("acme"));
        ctx.insert("repo".to_string(), json!("myrepo"));
        let result =
            faucet_core::util::substitute_context("/orgs/{org_id}/repos/{repo}/issues", &ctx);
        assert_eq!(result, "/orgs/acme/repos/myrepo/issues");
    }

    #[test]
    fn test_substitute_context_no_placeholders() {
        let ctx = HashMap::new();
        let result = faucet_core::util::substitute_context("/api/users", &ctx);
        assert_eq!(result, "/api/users");
    }

    #[test]
    fn test_substitute_context_numeric_value() {
        let mut ctx = HashMap::new();
        ctx.insert("id".to_string(), json!(42));
        let result = faucet_core::util::substitute_context("/items/{id}", &ctx);
        assert_eq!(result, "/items/42");
    }

    #[test]
    fn test_parse_retry_after_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_static("30"),
        );
        assert_eq!(parse_retry_after(&headers), Duration::from_secs(30));
    }

    #[test]
    fn test_parse_retry_after_missing_defaults_to_60() {
        assert_eq!(
            parse_retry_after(&HeaderMap::new()),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn test_parse_retry_after_non_numeric_defaults_to_60() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_static("not-a-number"),
        );
        assert_eq!(parse_retry_after(&headers), Duration::from_secs(60));
    }

    #[test]
    fn test_new_rejects_invalid_expiry_ratio_zero() {
        let config = RestStreamConfig::new("https://example.com", "/data").auth(Auth::OAuth2 {
            token_url: "https://auth.example.com/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scopes: vec![],
            expiry_ratio: 0.0,
        });
        let result = RestStream::new(config);
        assert!(result.is_err());
        assert!(matches!(result, Err(FaucetError::Auth(_))));
    }

    #[test]
    fn test_new_rejects_invalid_expiry_ratio_negative() {
        let config = RestStreamConfig::new("https://example.com", "/data").auth(Auth::OAuth2 {
            token_url: "https://auth.example.com/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scopes: vec![],
            expiry_ratio: -0.5,
        });
        assert!(RestStream::new(config).is_err());
    }

    #[test]
    fn test_new_rejects_invalid_expiry_ratio_above_one() {
        let config = RestStreamConfig::new("https://example.com", "/data").auth(Auth::OAuth2 {
            token_url: "https://auth.example.com/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scopes: vec![],
            expiry_ratio: 1.5,
        });
        assert!(RestStream::new(config).is_err());
    }

    #[test]
    fn test_new_accepts_valid_expiry_ratio() {
        let config = RestStreamConfig::new("https://example.com", "/data").auth(Auth::OAuth2 {
            token_url: "https://auth.example.com/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scopes: vec![],
            expiry_ratio: 1.0,
        });
        assert!(RestStream::new(config).is_ok());
    }

    #[test]
    fn test_new_rejects_invalid_transform_regex() {
        let config = RestStreamConfig::new("https://example.com", "/data").add_transform(
            faucet_core::RecordTransform::RenameKeys {
                pattern: "[invalid".into(),
                replacement: "".into(),
            },
        );
        let result = RestStream::new(config);
        assert!(result.is_err());
        assert!(matches!(result, Err(FaucetError::Transform(_))));
    }

    #[test]
    fn test_new_with_no_auth_succeeds() {
        let config = RestStreamConfig::new("https://example.com", "/data");
        assert!(RestStream::new(config).is_ok());
    }

    #[test]
    fn test_new_with_timeout() {
        let config =
            RestStreamConfig::new("https://example.com", "/data").timeout(Duration::from_secs(10));
        assert!(RestStream::new(config).is_ok());
    }

    #[test]
    fn test_substitute_context_missing_placeholder_unchanged() {
        let mut ctx = HashMap::new();
        ctx.insert("org".to_string(), json!("acme"));
        let result = faucet_core::util::substitute_context("/items/{missing}", &ctx);
        assert_eq!(result, "/items/{missing}");
    }

    #[test]
    fn test_substitute_context_boolean_value() {
        let mut ctx = HashMap::new();
        ctx.insert("flag".to_string(), json!(true));
        let result = faucet_core::util::substitute_context("/items/{flag}", &ctx);
        assert_eq!(result, "/items/true");
    }
}
