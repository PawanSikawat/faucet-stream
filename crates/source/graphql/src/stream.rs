//! GraphQL stream executor.

use crate::config::{GraphqlAuth, GraphqlStreamConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use jsonpath_rust::JsonPath;
use reqwest::Client;
use serde_json::{Value, json};

/// A configured GraphQL source that handles pagination and extraction.
pub struct GraphqlStream {
    config: GraphqlStreamConfig,
    client: Client,
}

impl GraphqlStream {
    /// Create a new GraphQL stream from the given configuration.
    pub fn new(config: GraphqlStreamConfig) -> Self {
        Self {
            config,
            client: Client::new(),
        }
    }

    /// Fetch all records across all pages.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        let mut all_records = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages_fetched = 0usize;
        let mut prev_cursor: Option<String> = None;

        loop {
            if let Some(max) = self.config.max_pages
                && pages_fetched >= max
            {
                tracing::warn!("max pages ({max}) reached");
                break;
            }

            let body = self.execute_query(&cursor).await?;
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
                    // Loop detection: stop if cursor hasn't changed.
                    if next_cursor == prev_cursor {
                        tracing::warn!("cursor loop detected, stopping pagination");
                        break;
                    }
                    prev_cursor = cursor;
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

    /// Execute a single GraphQL query.
    async fn execute_query(&self, cursor: &Option<String>) -> Result<Value, FaucetError> {
        let mut variables = self.config.variables.clone();

        // Inject cursor and page size into variables.
        if let (Some(pag), Some(cursor_val)) = (&self.config.pagination, cursor)
            && let Value::Object(ref mut map) = variables
        {
            map.insert(pag.cursor_variable.clone(), json!(cursor_val));
        }
        if let Some(pag) = &self.config.pagination
            && let (Some(size), Value::Object(map)) = (pag.page_size, &mut variables)
        {
            map.insert(pag.page_size_variable.clone(), json!(size));
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

        // Apply auth.
        match &self.config.auth {
            GraphqlAuth::None => {}
            GraphqlAuth::Bearer(token) => {
                req = req.bearer_auth(token);
            }
            GraphqlAuth::Custom(headers) => {
                req = req.headers(headers.clone());
            }
        }

        let resp = req.send().await.map_err(FaucetError::Http)?;

        let status = resp.status();
        if !status.is_success() {
            let url = resp.url().to_string();
            let body_text = resp.text().await.unwrap_or_default();
            return Err(FaucetError::HttpStatus {
                status: status.as_u16(),
                url,
                body: body_text,
            });
        }

        let body: Value = resp.json().await.map_err(FaucetError::Http)?;

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
            Some(path) => {
                let results = body
                    .query(path)
                    .map_err(|e| FaucetError::JsonPath(format!("JSONPath error: {e}")))?;
                Ok(results.into_iter().cloned().collect())
            }
            None => {
                // Return the data field as a single record.
                match body.get("data") {
                    Some(data) => Ok(vec![data.clone()]),
                    None => Ok(vec![body.clone()]),
                }
            }
        }
    }
}

#[async_trait]
impl faucet_core::Source for GraphqlStream {
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        GraphqlStream::fetch_all(self).await
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
}
