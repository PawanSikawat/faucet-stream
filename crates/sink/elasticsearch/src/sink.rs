//! Elasticsearch bulk index sink.

use crate::config::{ElasticsearchSinkAuth, ElasticsearchSinkConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use reqwest::Client;
use serde_json::Value;

/// A sink that writes JSON records to an Elasticsearch index using the bulk API.
pub struct ElasticsearchSink {
    config: ElasticsearchSinkConfig,
    client: Client,
}

impl ElasticsearchSink {
    /// Create a new Elasticsearch sink from the given configuration.
    pub fn new(config: ElasticsearchSinkConfig) -> Self {
        Self {
            config,
            client: Client::new(),
        }
    }

    /// Apply the configured authentication to a request builder.
    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.config.auth {
            ElasticsearchSinkAuth::None => req,
            ElasticsearchSinkAuth::Basic { username, password } => {
                req.basic_auth(username, Some(password))
            }
            ElasticsearchSinkAuth::Bearer(token) => req.bearer_auth(token),
            ElasticsearchSinkAuth::ApiKey(key) => {
                req.header("Authorization", format!("ApiKey {key}"))
            }
        }
    }

    /// Build the NDJSON bulk request body for a slice of records.
    ///
    /// Each record is preceded by an `{"index": {...}}` action line.
    /// If `id_field` is configured, the corresponding value from each record
    /// is used as the document `_id`.
    fn build_bulk_body(&self, records: &[Value]) -> Result<String, FaucetError> {
        let mut body = String::new();

        for record in records {
            // Build the action metadata.
            let mut action_meta = serde_json::Map::new();
            action_meta.insert(
                "_index".to_string(),
                Value::String(self.config.index.clone()),
            );

            if let Some(ref id_field) = self.config.id_field
                && let Some(id_val) = record.get(id_field)
            {
                let id_str = match id_val {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                action_meta.insert("_id".to_string(), Value::String(id_str));
            }

            let action_line = serde_json::to_string(&serde_json::json!({"index": action_meta}))
                .map_err(|e| FaucetError::Sink(format!("failed to serialize bulk action: {e}")))?;
            body.push_str(&action_line);
            body.push('\n');

            let record_line = serde_json::to_string(record)
                .map_err(|e| FaucetError::Sink(format!("failed to serialize record: {e}")))?;
            body.push_str(&record_line);
            body.push('\n');
        }

        Ok(body)
    }
}

#[async_trait]
impl faucet_core::Sink for ElasticsearchSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut total_written = 0;

        for chunk in records.chunks(self.config.batch_size) {
            let body = self.build_bulk_body(chunk)?;

            let url = format!("{}/_bulk", self.config.base_url);
            let req = self
                .client
                .post(&url)
                .header("Content-Type", "application/x-ndjson")
                .body(body);
            let req = self.apply_auth(req);

            let resp = req.send().await?;
            let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
            let resp_body: Value = resp.json().await?;

            // Check for item-level errors in the bulk response.
            if resp_body
                .get("errors")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                // Collect individual error messages for logging.
                let error_items: Vec<String> = resp_body
                    .get("items")
                    .and_then(|v| v.as_array())
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|item| {
                                item.get("index")
                                    .and_then(|idx| idx.get("error"))
                                    .map(|e| e.to_string())
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                if !error_items.is_empty() {
                    return Err(FaucetError::Sink(format!(
                        "Elasticsearch bulk request had {} errors: {}",
                        error_items.len(),
                        error_items.first().unwrap_or(&String::new())
                    )));
                }
            }

            total_written += chunk.len();
            tracing::debug!(records = chunk.len(), "Elasticsearch bulk batch written");
        }

        Ok(total_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bulk_body_without_id_field() {
        let config = ElasticsearchSinkConfig::new("http://localhost:9200", "test_idx");
        let sink = ElasticsearchSink::new(config);

        let records = vec![
            json!({"name": "Alice", "age": 30}),
            json!({"name": "Bob", "age": 25}),
        ];

        let body = sink.build_bulk_body(&records).unwrap();
        let lines: Vec<&str> = body.trim().split('\n').collect();

        // 2 records = 4 lines (action + data for each).
        assert_eq!(lines.len(), 4);

        // Verify action lines contain the index.
        let action: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(action["index"]["_index"], "test_idx");
        assert!(action["index"].get("_id").is_none());

        // Verify data lines.
        let data: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(data["name"], "Alice");
    }

    #[test]
    fn bulk_body_with_id_field() {
        let config =
            ElasticsearchSinkConfig::new("http://localhost:9200", "test_idx").id_field("doc_id");
        let sink = ElasticsearchSink::new(config);

        let records = vec![
            json!({"doc_id": "abc-123", "name": "Alice"}),
            json!({"doc_id": 42, "name": "Bob"}),
            json!({"name": "Charlie"}), // missing id field
        ];

        let body = sink.build_bulk_body(&records).unwrap();
        let lines: Vec<&str> = body.trim().split('\n').collect();
        assert_eq!(lines.len(), 6);

        // First record: string id.
        let action0: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(action0["index"]["_id"], "abc-123");

        // Second record: numeric id serialized as string.
        let action1: Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(action1["index"]["_id"], "42");

        // Third record: no id field, so no _id in action.
        let action2: Value = serde_json::from_str(lines[4]).unwrap();
        assert!(action2["index"].get("_id").is_none());
    }
}
