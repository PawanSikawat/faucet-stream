//! GraphQL source configuration.

use faucet_core::{
    AuthSpec, DEFAULT_BATCH_SIZE, FaucetError, TlsClientConfig, validate_batch_size,
};
use reqwest::header::HeaderMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Authentication for GraphQL endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum GraphqlAuth {
    /// No authentication.
    None,
    /// Bearer token in the Authorization header.
    Bearer { token: String },
    /// Custom headers (e.g. API keys, cookies).
    Custom { headers: HashMap<String, String> },
}

/// Cursor-based pagination configuration for GraphQL.
///
/// Most GraphQL APIs use the Relay cursor specification with
/// `pageInfo { hasNextPage, endCursor }`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GraphqlPagination {
    /// JSONPath to the `hasNextPage` boolean in the response.
    pub has_next_page_path: String,
    /// JSONPath to the `endCursor` string in the response.
    pub cursor_path: String,
    /// Name of the cursor variable in the GraphQL query (default: `"after"`).
    pub cursor_variable: String,
    /// Name of the page size variable (default: `"first"`).
    ///
    /// The per-page record count itself comes from
    /// [`GraphqlStreamConfig::batch_size`] — the variable named here is the
    /// GraphQL variable that the `batch_size` value is injected into on each
    /// request. The plain `batch_size = 0` sentinel omits the variable so the
    /// upstream uses its own default page size.
    pub page_size_variable: String,
}

impl Default for GraphqlPagination {
    fn default() -> Self {
        Self {
            has_next_page_path: "$.data.*.pageInfo.hasNextPage".into(),
            cursor_path: "$.data.*.pageInfo.endCursor".into(),
            cursor_variable: "after".into(),
            page_size_variable: "first".into(),
        }
    }
}

/// Configuration for the GraphQL source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GraphqlStreamConfig {
    /// GraphQL endpoint URL.
    pub endpoint: String,
    /// The GraphQL query string.
    pub query: String,
    /// Variables to pass with the query.
    pub variables: Value,
    /// Authentication: either inline (`{ type, config }`) or a `{ ref: <name> }`
    /// pointer to a shared provider in the CLI's top-level `auth:` catalog.
    pub auth: AuthSpec<GraphqlAuth>,
    /// Additional request headers.
    #[serde(skip, default)]
    pub headers: HeaderMap,
    /// JSONPath expression to extract records from the response.
    pub records_path: Option<String>,
    /// Pagination configuration. `None` for single-page queries.
    pub pagination: Option<GraphqlPagination>,
    /// Maximum number of pages to fetch.
    pub max_pages: Option<usize>,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage), and the
    /// value injected as the GraphQL `first:` cursor argument (or whatever
    /// variable name [`GraphqlPagination::page_size_variable`] specifies).
    /// Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the page-size variable
    /// is omitted from the request so the upstream uses its own default page
    /// size, and the entire result set is emitted as a single page. If the
    /// upstream schema requires a non-null `first:` argument this will
    /// surface as `FaucetError::Config` at stream-time.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Optional client-certificate (mutual TLS) config. When set, the source
    /// presents a client certificate on every request (data + inline auth token
    /// request). Requires the crate's `mtls` feature.
    #[serde(default)]
    pub tls: Option<TlsClientConfig>,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl GraphqlStreamConfig {
    /// Create a new config with an endpoint and query.
    pub fn new(endpoint: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            query: query.into(),
            variables: Value::Object(Default::default()),
            auth: AuthSpec::Inline(GraphqlAuth::None),
            headers: HeaderMap::new(),
            records_path: None,
            pagination: None,
            max_pages: None,
            batch_size: DEFAULT_BATCH_SIZE,
            tls: None,
        }
    }

    /// Attach a mutual-TLS client identity (requires the `mtls` feature at build
    /// time; otherwise [`GraphqlStream::try_new`](crate::GraphqlStream::try_new)
    /// errors).
    pub fn tls(mut self, tls: TlsClientConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Set the GraphQL variables.
    pub fn variables(mut self, vars: Value) -> Self {
        self.variables = vars;
        self
    }

    /// Set the authentication method.
    pub fn auth(mut self, auth: GraphqlAuth) -> Self {
        self.auth = AuthSpec::Inline(auth);
        self
    }

    /// Set additional headers.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Set the JSONPath expression for record extraction.
    pub fn records_path(mut self, path: impl Into<String>) -> Self {
        self.records_path = Some(path.into());
        self
    }

    /// Enable cursor-based pagination with the given configuration.
    pub fn pagination(mut self, pagination: GraphqlPagination) -> Self {
        self.pagination = Some(pagination);
        self
    }

    /// Set the maximum number of pages to fetch.
    pub fn max_pages(mut self, max: usize) -> Self {
        self.max_pages = Some(max);
        self
    }

    /// Set the per-page record count for [`Source::stream_pages`](faucet_core::Source::stream_pages)
    /// and the GraphQL `first:` cursor argument.
    ///
    /// Pass `0` to opt out of batching — the page-size variable is omitted
    /// from the request so the upstream uses its own default page size, and
    /// the response is emitted as a single [`StreamPage`](faucet_core::StreamPage).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Validate the config at load time so a bad config fails fast with a typed
    /// [`FaucetError::Config`] instead of surfacing deep in a run: rejects an
    /// out-of-range `batch_size` (`> MAX_BATCH_SIZE`) and an empty `endpoint` or
    /// `query`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.endpoint.trim().is_empty() {
            return Err(FaucetError::Config(
                "GraphQL source requires a non-empty `endpoint`".into(),
            ));
        }
        if self.query.trim().is_empty() {
            return Err(FaucetError::Config(
                "GraphQL source requires a non-empty `query`".into(),
            ));
        }
        validate_batch_size(self.batch_size)?;
        if let Some(tls) = &self.tls {
            tls.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_config() {
        let config = GraphqlStreamConfig::new(
            "https://api.example.com/graphql",
            "query { users { id name } }",
        );
        assert_eq!(config.endpoint, "https://api.example.com/graphql");
        assert!(config.records_path.is_none());
        assert!(config.pagination.is_none());
        assert!(config.max_pages.is_none());
    }

    #[test]
    fn builder_methods() {
        let config =
            GraphqlStreamConfig::new("https://api.example.com/graphql", "query { users { id } }")
                .variables(json!({"org": "acme"}))
                .records_path("$.data.users.edges[*].node")
                .max_pages(10)
                .auth(GraphqlAuth::Bearer {
                    token: "token".into(),
                });
        assert_eq!(config.variables["org"], "acme");
        assert_eq!(config.records_path.unwrap(), "$.data.users.edges[*].node");
        assert_eq!(config.max_pages, Some(10));
    }

    #[test]
    fn default_pagination() {
        let pag = GraphqlPagination::default();
        assert_eq!(pag.cursor_variable, "after");
        assert_eq!(pag.page_size_variable, "first");
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }")
            .with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }")
            .with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "endpoint": "https://api.example.com/graphql",
            "query": "query { x }",
            "variables": {},
            "auth": {"type": "none"},
            "records_path": null,
            "pagination": null,
            "max_pages": null,
            "batch_size": 500
        }"#;
        let config: GraphqlStreamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn validate_accepts_valid_config() {
        assert!(
            GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }")
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn validate_rejects_oversized_batch_size() {
        let config = GraphqlStreamConfig::new("https://api.example.com/graphql", "query { x }")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(matches!(config.validate(), Err(FaucetError::Config(_))));
    }

    #[test]
    fn validate_rejects_empty_endpoint() {
        assert!(matches!(
            GraphqlStreamConfig::new("  ", "query { x }").validate(),
            Err(FaucetError::Config(_))
        ));
    }

    #[test]
    fn validate_rejects_empty_query() {
        assert!(matches!(
            GraphqlStreamConfig::new("https://api.example.com/graphql", "").validate(),
            Err(FaucetError::Config(_))
        ));
    }
}
