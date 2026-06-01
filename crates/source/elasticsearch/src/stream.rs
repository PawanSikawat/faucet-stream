//! Elasticsearch scroll-based search source.

use crate::config::{ElasticsearchAuth, ElasticsearchSourceConfig};
use async_trait::async_trait;
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use faucet_core::{AuthSpec, FaucetError, SharedAuthProvider, Stream, StreamPage};
use reqwest::Client;
use serde_json::{Value, json};
use std::pin::Pin;

/// `size` used by the [`Source::stream_pages`] non-scroll fallback (when
/// `batch_size = 0`). Mirrors Elasticsearch's default `index.max_result_window`
/// so the request stays within ES's out-of-the-box cap.
pub(crate) const NO_BATCHING_SEARCH_SIZE: usize = 10_000;

/// A source that reads documents from an Elasticsearch index using the scroll API.
pub struct ElasticsearchSource {
    config: ElasticsearchSourceConfig,
    client: Client,
    /// Optional shared auth provider. When set it takes precedence over inline
    /// auth. Injected by the CLI (to resolve `auth: { ref }`) or directly by
    /// library callers who want to share one token across multiple sources.
    auth_provider: Option<SharedAuthProvider>,
}

impl ElasticsearchSource {
    /// Create a new Elasticsearch source from the given configuration.
    /// Construction does no I/O; it fails only on an invalid config (an
    /// out-of-range `batch_size`).
    pub fn new(config: ElasticsearchSourceConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        Ok(Self {
            config,
            client: Client::new(),
            auth_provider: None,
        })
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider supplies the credential for every request (taking precedence
    /// over inline auth), so several sources can share one token with
    /// single-flight refresh. Used by the CLI to resolve `auth: { ref }`, and
    /// by library callers who inject one provider into many sources.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Resolve the effective [`ElasticsearchAuth`] for the current request.
    ///
    /// Resolution order:
    /// 1. If a shared provider is attached, call it and map the credential.
    /// 2. Otherwise use the inline auth from config.
    /// 3. If the config is a `Reference` with no provider, return an error.
    async fn resolve_auth(&self) -> Result<ElasticsearchAuth, FaucetError> {
        if let Some(p) = &self.auth_provider {
            return faucet_elasticsearch_common::credential_to_auth(p.credential().await?);
        }
        match &self.config.auth {
            AuthSpec::Inline(a) => Ok(a.clone()),
            AuthSpec::Reference(r) => Err(FaucetError::Auth(format!(
                "auth references provider '{}' but no provider was supplied",
                r.name
            ))),
        }
    }

    /// Apply an [`ElasticsearchAuth`] to a request builder.
    fn apply_auth_value(
        req: reqwest::RequestBuilder,
        auth: &ElasticsearchAuth,
    ) -> reqwest::RequestBuilder {
        match auth {
            ElasticsearchAuth::None => req,
            ElasticsearchAuth::Basic { username, password } => {
                req.basic_auth(username, Some(password))
            }
            ElasticsearchAuth::Bearer { token } => req.bearer_auth(token),
            ElasticsearchAuth::ApiKey { key } => {
                req.header("Authorization", format!("ApiKey {key}"))
            }
        }
    }

    /// Extract `hits.hits[*]._source` from an Elasticsearch search response.
    fn extract_hits(body: &Value) -> Vec<Value> {
        body.get("hits")
            .and_then(|h| h.get("hits"))
            .and_then(|h| h.as_array())
            .map(|hits| {
                hits.iter()
                    .filter_map(|hit| hit.get("_source").cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Extract the `_scroll_id` from an Elasticsearch response.
    fn extract_scroll_id(body: &Value) -> Option<String> {
        body.get("_scroll_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    /// Clear a scroll context. Best-effort: errors are logged but not propagated.
    async fn clear_scroll(&self, scroll_id: &str) {
        let url = format!("{}/_search/scroll", self.config.base_url);
        let req = self
            .client
            .delete(&url)
            .json(&json!({"scroll_id": scroll_id}));
        let auth = match self.resolve_auth().await {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(error = %e, "failed to resolve auth for scroll cleanup");
                return;
            }
        };
        let req = Self::apply_auth_value(req, &auth);

        if let Err(e) = req.send().await {
            tracing::warn!(error = %e, "failed to clear Elasticsearch scroll context");
        }
    }

    /// Resolve the index and query under the supplied parent context. Returns
    /// `(index, query)`.
    fn resolve_index_and_query(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<(String, Value), FaucetError> {
        let index = if context.is_empty() {
            self.config.index.clone()
        } else {
            faucet_core::util::substitute_context(&self.config.index, context)
        };
        let query = if context.is_empty() {
            self.config.query.clone()
        } else {
            let s = serde_json::to_string(&self.config.query)
                .map_err(|e| FaucetError::Config(format!("failed to serialize query: {e}")))?;
            let s = faucet_core::util::substitute_context_json(&s, context);
            serde_json::from_str(&s).map_err(|e| {
                FaucetError::Config(format!("failed to parse substituted query: {e}"))
            })?
        };
        Ok((index, query))
    }
}

#[async_trait]
impl faucet_core::Source for ElasticsearchSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (index, query) = self.resolve_index_and_query(context)?;
        // Resolve auth once; reuse the same credential across all scroll pages.
        let auth = self.resolve_auth().await?;

        let mut all_records = Vec::new();

        // `batch_size = 0` is the "no batching" sentinel. Interpolating it
        // directly as `size=0` would make Elasticsearch return zero hits, so
        // map it to the same large page size the streaming path uses (#78/#33).
        let page_size = if self.config.batch_size == 0 {
            NO_BATCHING_SEARCH_SIZE
        } else {
            self.config.batch_size
        };

        // Initial search request with scroll.
        let url = format!(
            "{}/{}/_search?scroll={}&size={}",
            self.config.base_url, index, self.config.scroll_timeout, page_size
        );
        let req = self.client.post(&url).json(&json!({"query": query}));
        let req = Self::apply_auth_value(req, &auth);

        let resp = req.send().await?;
        let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        let body: Value = resp.json().await?;

        let mut records = Self::extract_hits(&body);
        let mut scroll_id = Self::extract_scroll_id(&body);
        let mut pages_fetched: usize = 1;

        tracing::debug!(
            records = records.len(),
            page = pages_fetched,
            "Elasticsearch initial search"
        );

        all_records.append(&mut records);

        // Scroll loop.
        while let Some(ref sid) = scroll_id {
            // Check max_pages limit.
            if let Some(max) = self.config.max_pages
                && pages_fetched >= max
            {
                tracing::debug!(max_pages = max, "max_pages reached, stopping scroll");
                break;
            }

            let scroll_url = format!("{}/_search/scroll", self.config.base_url);
            let req = self.client.post(&scroll_url).json(&json!({
                "scroll": self.config.scroll_timeout,
                "scroll_id": sid,
            }));
            let req = Self::apply_auth_value(req, &auth);

            let resp = req.send().await?;
            let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
            let body: Value = resp.json().await?;

            let mut page_records = Self::extract_hits(&body);
            pages_fetched += 1;

            tracing::debug!(
                records = page_records.len(),
                page = pages_fetched,
                "Elasticsearch scroll page"
            );

            // Stop when no more hits are returned.
            if page_records.is_empty() {
                break;
            }

            // Update scroll_id for the next iteration.
            scroll_id = Self::extract_scroll_id(&body);
            all_records.append(&mut page_records);
        }

        // Clear the scroll context (best-effort).
        if let Some(ref sid) = scroll_id {
            self.clear_scroll(sid).await;
        }

        tracing::debug!(
            total_records = all_records.len(),
            pages = pages_fetched,
            "Elasticsearch fetch complete"
        );

        Ok(all_records)
    }

    /// Stream documents from Elasticsearch as scroll pages, one
    /// [`StreamPage`] per scroll response. Bounds client-side memory at
    /// O(batch_size) regardless of the index's total document count.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of
    /// [`ElasticsearchSourceConfig::batch_size`] — the config is the
    /// user-facing knob the README documents, and routing the
    /// pipeline-supplied hint through it would silently override an explicit
    /// config value.
    ///
    /// When `batch_size = 0` the source issues a single non-scroll
    /// `_search?size=10_000` and emits exactly one page. The scroll API is
    /// not used and no scroll context needs to be cleared.
    ///
    /// The Elasticsearch search source has no incremental-replication mode
    /// today, so every emitted page carries `bookmark: None`.
    ///
    /// **Scroll-context cleanup is mandatory.** On every exit path — clean
    /// drain, `max_pages` truncation, mid-stream HTTP error, or consumer
    /// dropping the stream — the open `_scroll_id` is sent to
    /// `DELETE _search/scroll` so the cluster does not leak server-side
    /// state. Cleanup runs inside a guard whose `Drop` impl spawns the
    /// delete request, so even cancellation at any `.await` point still
    /// releases the context.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let (index, query) = self.resolve_index_and_query(context)?;
            // Resolve auth once; reuse across all scroll pages and cleanup.
            let auth = self.resolve_auth().await?;

            // batch_size == 0: single non-scroll _search with size = max_result_window default.
            if batch_size == 0 {
                let url = format!(
                    "{}/{}/_search?size={}",
                    self.config.base_url, index, NO_BATCHING_SEARCH_SIZE
                );
                let req = self.client.post(&url).json(&json!({"query": query}));
                let req = Self::apply_auth_value(req, &auth);
                let resp = req.send().await?;
                let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
                let body: Value = resp.json().await?;
                let records = Self::extract_hits(&body);
                tracing::info!(
                    docs = records.len(),
                    batch_size = 0,
                    "Elasticsearch source stream complete (no-batching path)",
                );
                yield StreamPage { records, bookmark: None };
                return;
            }

            // Scroll path. Wire up a guard so the scroll context is always
            // cleared, even on early-return / error / drop.
            // Pass the already-resolved auth so the guard's spawned cleanup
            // tasks never need to call async auth resolution.
            let mut guard = ScrollGuard::new(
                self.config.base_url.clone(),
                self.client.clone(),
                auth.clone(),
            );

            let url = format!(
                "{}/{}/_search?scroll={}&size={}",
                self.config.base_url, index, self.config.scroll_timeout, batch_size
            );
            let req = self.client.post(&url).json(&json!({"query": query}));
            let req = Self::apply_auth_value(req, &auth);
            let resp = req.send().await?;
            let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
            let body: Value = resp.json().await?;

            let records = Self::extract_hits(&body);
            guard.update(Self::extract_scroll_id(&body));
            let mut pages_emitted: usize = 0;
            let mut total = records.len();

            // The initial search always counts as page 1, even when it
            // returns zero hits — emit it and move on.
            pages_emitted += 1;
            let is_final = records.is_empty()
                || guard.scroll_id().is_none()
                || matches!(self.config.max_pages, Some(max) if pages_emitted >= max);
            yield StreamPage { records, bookmark: None };
            if is_final {
                guard.disarm_if_done();
                tracing::info!(
                    docs = total,
                    pages = pages_emitted,
                    batch_size,
                    "Elasticsearch source stream complete",
                );
                return;
            }

            // Scroll loop.
            while let Some(sid) = guard.scroll_id().map(|s| s.to_string()) {
                let scroll_url = format!("{}/_search/scroll", self.config.base_url);
                let req = self.client.post(&scroll_url).json(&json!({
                    "scroll": self.config.scroll_timeout,
                    "scroll_id": sid,
                }));
                let req = Self::apply_auth_value(req, &auth);
                let resp = req.send().await?;
                let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
                let body: Value = resp.json().await?;

                let records = Self::extract_hits(&body);
                guard.update(Self::extract_scroll_id(&body));
                pages_emitted += 1;
                total += records.len();

                let is_empty = records.is_empty();
                let hit_cap = matches!(self.config.max_pages, Some(max) if pages_emitted >= max);

                if is_empty {
                    // Final empty page — ES uses an empty hits array as the
                    // end-of-scroll sentinel. Drop it; nothing to emit.
                    break;
                }

                yield StreamPage { records, bookmark: None };

                if hit_cap {
                    tracing::debug!(
                        max_pages = self.config.max_pages.unwrap_or(0),
                        "max_pages reached, stopping scroll"
                    );
                    break;
                }
            }

            tracing::info!(
                docs = total,
                pages = pages_emitted,
                batch_size,
                "Elasticsearch source stream complete",
            );

            // Successful drain — let the guard clean up the scroll id (if any).
            guard.disarm_if_done();
        })
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(ElasticsearchSourceConfig))
            .expect("schema serialization")
    }
}

/// RAII guard that owns the active scroll id and clears it on drop.
///
/// Holds a pre-resolved [`ElasticsearchAuth`] (not `AuthSpec`) so the drop-path
/// spawned cleanup tasks never need to perform async auth resolution.
struct ScrollGuard {
    base_url: String,
    client: Client,
    auth: ElasticsearchAuth,
    scroll_id: Option<String>,
}

impl ScrollGuard {
    fn new(base_url: String, client: Client, auth: ElasticsearchAuth) -> Self {
        Self {
            base_url,
            client,
            auth,
            scroll_id: None,
        }
    }

    fn scroll_id(&self) -> Option<&str> {
        self.scroll_id.as_deref()
    }

    fn update(&mut self, new_id: Option<String>) {
        if let Some(id) = new_id {
            self.scroll_id = Some(id);
        }
    }

    /// Called when the stream drained cleanly. Spawns cleanup as a detached
    /// task and disarms the drop fallback.
    fn disarm_if_done(&mut self) {
        if let Some(sid) = self.scroll_id.take() {
            let base_url = self.base_url.clone();
            let auth = self.auth.clone();
            let client = self.client.clone();
            tokio::spawn(async move {
                let url = format!("{base_url}/_search/scroll");
                let req = client.delete(&url).json(&json!({"scroll_id": sid}));
                let req = apply_auth_to(req, &auth);
                if let Err(e) = req.send().await {
                    tracing::warn!(error = %e, "failed to clear Elasticsearch scroll context");
                }
            });
        }
    }
}

impl Drop for ScrollGuard {
    fn drop(&mut self) {
        if let Some(sid) = self.scroll_id.take() {
            // Error / cancellation path. Spawn so cleanup survives the
            // stream future being dropped mid-await.
            let base_url = self.base_url.clone();
            let auth = self.auth.clone();
            let client = self.client.clone();
            tokio::spawn(async move {
                let url = format!("{base_url}/_search/scroll");
                let req = client.delete(&url).json(&json!({"scroll_id": sid}));
                let req = apply_auth_to(req, &auth);
                if let Err(e) = req.send().await {
                    tracing::warn!(
                        error = %e,
                        "failed to clear Elasticsearch scroll context (drop path)",
                    );
                }
            });
        }
    }
}

/// Apply an [`ElasticsearchAuth`] to a request builder. Standalone so
/// spawned cleanup tasks can use it without holding a source reference.
fn apply_auth_to(
    req: reqwest::RequestBuilder,
    auth: &ElasticsearchAuth,
) -> reqwest::RequestBuilder {
    match auth {
        ElasticsearchAuth::None => req,
        ElasticsearchAuth::Basic { username, password } => req.basic_auth(username, Some(password)),
        ElasticsearchAuth::Bearer { token } => req.bearer_auth(token),
        ElasticsearchAuth::ApiKey { key } => req.header("Authorization", format!("ApiKey {key}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_rejects_out_of_range_batch_size() {
        let mut config = ElasticsearchSourceConfig::new("http://localhost:9200", "idx");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match ElasticsearchSource::new(config) {
            Err(FaucetError::Config(m)) => assert!(m.contains("batch_size"), "got: {m}"),
            _ => panic!("expected a batch_size Config error"),
        }
    }
}
