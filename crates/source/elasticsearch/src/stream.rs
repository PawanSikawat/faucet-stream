//! Elasticsearch scroll-based search source.

use crate::config::{ElasticsearchAuth, ElasticsearchSourceConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use reqwest::Client;
use serde_json::{Value, json};

/// A source that reads documents from an Elasticsearch index using the scroll API.
pub struct ElasticsearchSource {
    config: ElasticsearchSourceConfig,
    client: Client,
}

impl ElasticsearchSource {
    /// Create a new Elasticsearch source from the given configuration.
    pub fn new(config: ElasticsearchSourceConfig) -> Self {
        Self {
            config,
            client: Client::new(),
        }
    }

    /// Apply the configured authentication to a request builder.
    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.config.auth {
            ElasticsearchAuth::None => req,
            ElasticsearchAuth::Basic { username, password } => {
                req.basic_auth(username, Some(password))
            }
            ElasticsearchAuth::Bearer(token) => req.bearer_auth(token),
            ElasticsearchAuth::ApiKey(key) => req.header("Authorization", format!("ApiKey {key}")),
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
        let req = self.apply_auth(req);

        if let Err(e) = req.send().await {
            tracing::warn!(error = %e, "failed to clear Elasticsearch scroll context");
        }
    }
}

#[async_trait]
impl faucet_core::Source for ElasticsearchSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        // Resolve index and query with context substitution.
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

        let mut all_records = Vec::new();

        // Initial search request with scroll.
        let url = format!(
            "{}/{}/_search?scroll={}&size={}",
            self.config.base_url, index, self.config.scroll_timeout, self.config.scroll_size
        );
        let req = self.client.post(&url).json(&json!({"query": query}));
        let req = self.apply_auth(req);

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
            let req = self.apply_auth(req);

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

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(ElasticsearchSourceConfig))
            .expect("schema serialization")
    }
}
