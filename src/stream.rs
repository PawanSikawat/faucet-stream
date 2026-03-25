//! The main REST stream executor.

use crate::auth::Auth;
use crate::auth::oauth2::TokenCache;
use crate::config::RestStreamConfig;
use crate::error::FaucetError;
use crate::extract;
use crate::pagination::{PaginationState, PaginationStyle};
use crate::replication::{ReplicationMethod, filter_incremental, max_replication_value};
use crate::retry;
use crate::schema;
use crate::transform::{self, CompiledTransform};
use futures_core::Stream;
use reqwest::Client;
use reqwest::header::HeaderMap;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

/// A configured REST API stream that handles pagination, auth, and extraction.
pub struct RestStream {
    config: RestStreamConfig,
    client: Client,
    /// Pre-compiled transforms (regex patterns compiled once at construction time).
    compiled_transforms: Vec<CompiledTransform>,
    /// Shared OAuth2 token cache (only used when `config.auth` is `Auth::OAuth2`).
    token_cache: TokenCache,
}

impl RestStream {
    /// Create a new stream from the given configuration.
    ///
    /// Returns [`FaucetError::Transform`] immediately if any `RenameKeys`
    /// transform contains an invalid regex pattern — fail-fast before any
    /// HTTP requests are made.
    pub fn new(config: RestStreamConfig) -> Result<Self, FaucetError> {
        // Validate OAuth2 expiry_ratio at construction time.
        if let Auth::OAuth2 { expiry_ratio, .. } = &config.auth
            && (*expiry_ratio <= 0.0 || *expiry_ratio > 1.0)
        {
            return Err(FaucetError::Auth(format!(
                "expiry_ratio must be in (0.0, 1.0], got {expiry_ratio}"
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

    /// Stream records page-by-page, yielding one `Vec<Value>` per page as it arrives.
    ///
    /// Unlike [`fetch_all`](Self::fetch_all), this does not wait for all pages to be fetched
    /// before returning — callers can process each page immediately.
    ///
    /// Note: partitions are not supported by `stream_pages`. Use `fetch_all` for
    /// multi-partition streams.
    ///
    /// ```rust,no_run
    /// use faucet_stream::{RestStream, RestStreamConfig};
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
        self.stream_pages_inner(None)
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Core pagination loop shared by [`stream_pages`](Self::stream_pages) and
    /// [`fetch_partition`](Self::fetch_partition).
    ///
    /// Yields one `Vec<Value>` per page.  When `context` is `Some`, path
    /// placeholders are substituted for partition support.
    fn stream_pages_inner(
        &self,
        context: Option<&HashMap<String, Value>>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<Value>, FaucetError>> + Send + '_>> {
        // Clone the context into an owned map so it can live inside the
        // `async_stream` generator without borrowing from the caller.
        let owned_context: Option<HashMap<String, Value>> = context.cloned();

        Box::pin(async_stream::try_stream! {
            let mut state = PaginationState::default();
            let mut pages_fetched = 0usize;

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
                        if let (Some(key), Some(start)) = (
                            &self.config.replication_key,
                            &self.config.start_replication_value,
                        ) {
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

                yield records;

                let has_next = self
                    .config
                    .pagination
                    .advance(&body, &resp_headers, &mut state, raw_count)?;
                pages_fetched += 1;
                if !has_next {
                    break;
                }

                if let Some(delay) = self.config.request_delay {
                    tokio::time::sleep(delay).await;
                }
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
                Some(Ok(records)) => {
                    pages_fetched += 1;
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
                    Some(ctx) => resolve_path(&self.config.path, ctx),
                    None => self.config.path.clone(),
                };
                format!("{}/{}", self.config.base_url, path.trim_start_matches('/'))
            }
        };

        // Resolve OAuth2 credentials to a Bearer token before applying auth headers.
        // The token is cached and reused until it expires, avoiding a token
        // fetch on every HTTP request.
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
                Auth::Bearer(token)
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
            req = req.query(params);
        }

        // ApiKeyQuery: inject the API key as a query parameter.
        if let Auth::ApiKeyQuery { param, value } = &self.config.auth {
            req = req.query(&[(param.as_str(), value.as_str())]);
        }

        if let Some(body) = &self.config.body {
            req = req.json(body);
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

/// Substitute `{key}` placeholders in `path` with values from `context`.
fn resolve_path(path: &str, context: &HashMap<String, Value>) -> String {
    let mut result = path.to_string();
    for (key, value) in context {
        let placeholder = format!("{{{key}}}");
        let replacement = match value {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        result = result.replace(&placeholder, &replacement);
    }
    result
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_resolve_path_substitutes_placeholders() {
        let mut ctx = HashMap::new();
        ctx.insert("org_id".to_string(), json!("acme"));
        ctx.insert("repo".to_string(), json!("myrepo"));
        let result = resolve_path("/orgs/{org_id}/repos/{repo}/issues", &ctx);
        assert_eq!(result, "/orgs/acme/repos/myrepo/issues");
    }

    #[test]
    fn test_resolve_path_no_placeholders() {
        let ctx = HashMap::new();
        let result = resolve_path("/api/users", &ctx);
        assert_eq!(result, "/api/users");
    }

    #[test]
    fn test_resolve_path_numeric_value() {
        let mut ctx = HashMap::new();
        ctx.insert("id".to_string(), json!(42));
        let result = resolve_path("/items/{id}", &ctx);
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
}
