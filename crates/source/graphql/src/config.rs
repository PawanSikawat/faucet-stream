//! GraphQL source configuration.

use reqwest::header::HeaderMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Authentication for GraphQL endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
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
    /// Optional page size. Added to variables as `first` (or `page_size_variable`).
    pub page_size: Option<usize>,
    /// Name of the page size variable (default: `"first"`).
    pub page_size_variable: String,
}

impl Default for GraphqlPagination {
    fn default() -> Self {
        Self {
            has_next_page_path: "$.data.*.pageInfo.hasNextPage".into(),
            cursor_path: "$.data.*.pageInfo.endCursor".into(),
            cursor_variable: "after".into(),
            page_size: None,
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
    /// Authentication method.
    pub auth: GraphqlAuth,
    /// Additional request headers.
    #[serde(skip, default)]
    pub headers: HeaderMap,
    /// JSONPath expression to extract records from the response.
    pub records_path: Option<String>,
    /// Pagination configuration. `None` for single-page queries.
    pub pagination: Option<GraphqlPagination>,
    /// Maximum number of pages to fetch.
    pub max_pages: Option<usize>,
}

impl GraphqlStreamConfig {
    /// Create a new config with an endpoint and query.
    pub fn new(endpoint: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            query: query.into(),
            variables: Value::Object(Default::default()),
            auth: GraphqlAuth::None,
            headers: HeaderMap::new(),
            records_path: None,
            pagination: None,
            max_pages: None,
        }
    }

    /// Set the GraphQL variables.
    pub fn variables(mut self, vars: Value) -> Self {
        self.variables = vars;
        self
    }

    /// Set the authentication method.
    pub fn auth(mut self, auth: GraphqlAuth) -> Self {
        self.auth = auth;
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
        assert!(pag.page_size.is_none());
    }
}
