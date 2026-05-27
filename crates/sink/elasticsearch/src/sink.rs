//! Elasticsearch bulk index sink.

use crate::config::{ElasticsearchAuth, ElasticsearchSinkConfig};
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
    ///
    /// Returns [`FaucetError::Config`] if `batch_size` exceeds
    /// `MAX_BATCH_SIZE` (#78/#44).
    pub fn new(config: ElasticsearchSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        Ok(Self {
            config,
            client: Client::new(),
        })
    }

    /// Apply the configured authentication to a request builder.
    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.config.auth {
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

    /// Send a `POST /_bulk` request for a slice of records and return the raw
    /// response body as a [`Value`].
    ///
    /// All HTTP-level errors (non-2xx status, network failures, JSON parse
    /// errors) surface as `Err(FaucetError::…)`. Item-level errors inside the
    /// response body are left to the caller to inspect.
    async fn send_bulk_raw(&self, chunk: &[Value]) -> Result<Value, FaucetError> {
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
        Ok(resp_body)
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

/// Extract per-item error messages from a `_bulk` response body.
///
/// Each `items` entry is `{ "<action>": { ..., "error": {...} } }` where
/// `<action>` is `index` / `create` / `update` / `delete`. We read the error
/// from whichever action key is present, so all bulk operation types are
/// handled (not just `index`).
fn extract_bulk_error_messages(resp_body: &Value) -> Vec<String> {
    resp_body
        .get("items")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    item.as_object()
                        .and_then(|m| m.values().next())
                        .and_then(|action| action.get("error"))
                        .map(|e| e.to_string())
                })
                .collect()
        })
        .unwrap_or_default()
}

#[async_trait]
impl faucet_core::Sink for ElasticsearchSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(ElasticsearchSinkConfig))
            .expect("schema serialization")
    }

    /// Write records to Elasticsearch using the `_bulk` API.
    ///
    /// When `config.batch_size > 0` and the input slice is larger than
    /// `batch_size`, the slice is split into chunks of `batch_size`
    /// documents and each chunk is sent as a separate `POST /_bulk` HTTP
    /// call. When `config.batch_size == 0`, the entire upstream
    /// [`StreamPage`](faucet_core::StreamPage) is sent in a single bulk
    /// request — useful when the source already sizes pages for
    /// Elasticsearch's `_bulk` sweet spot (5–15 MB NDJSON per call).
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut total_written = 0;

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            // Sentinel: forward the entire upstream page as a single
            // `_bulk` POST. Caller is responsible for staying under
            // Elasticsearch's per-request limits.
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        for chunk in chunks {
            let resp_body = self.send_bulk_raw(chunk).await?;

            // Check for item-level errors in the bulk response.
            if resp_body
                .get("errors")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                let error_items = extract_bulk_error_messages(&resp_body);
                if let Some(first) = error_items.first() {
                    return Err(FaucetError::Sink(format!(
                        "Elasticsearch bulk request had {} errors: {first}",
                        error_items.len(),
                    )));
                }
                // `errors: true` but no per-item error could be extracted (an
                // items shape the parser doesn't recognise). Treat as a hard
                // failure rather than counting the chunk as written — otherwise
                // failed rows are silently dropped (#78/#32).
                return Err(FaucetError::Sink(
                    "Elasticsearch bulk request reported errors:true but no per-item error \
                     could be extracted from the response — treating as a hard failure to \
                     avoid silently dropping records"
                        .into(),
                ));
            }

            total_written += chunk.len();
            tracing::debug!(records = chunk.len(), "Elasticsearch bulk batch written");
        }

        Ok(total_written)
    }

    /// Write records using the `_bulk` API, returning a per-row outcome.
    ///
    /// Unlike [`write_batch`](faucet_core::Sink::write_batch), this method never collapses
    /// item-level Elasticsearch errors into a single outer `Err`. Each
    /// document maps to exactly one [`faucet_core::RowOutcome`]:
    ///
    /// - `Ok(())` — the item was accepted (no `"error"` key in the response
    ///   action object).
    /// - `Err(FaucetError::Sink(_))` — Elasticsearch rejected the document
    ///   (the `"error"` object from the response is included in the message).
    ///
    /// HTTP-level failures (non-2xx status, network errors) still surface as
    /// an outer `Err`, because the entire chunk could not be sent.
    ///
    /// When the server returns fewer items than records sent (a malformed
    /// response), the missing tail positions are padded with
    /// `Err(FaucetError::Sink("… truncated …"))` so the caller always
    /// receives exactly `records.len()` outcomes.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut outcomes: Vec<faucet_core::RowOutcome> = Vec::with_capacity(records.len());

        for chunk in chunks {
            let resp_body = self.send_bulk_raw(chunk).await?;

            let items = resp_body
                .get("items")
                .and_then(|v| v.as_array())
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            let mut chunk_outcomes: Vec<faucet_core::RowOutcome> = Vec::with_capacity(chunk.len());

            for item in items.iter().take(chunk.len()) {
                let action = item.get("index").or_else(|| item.get("create"));
                let error = action.and_then(|a| a.get("error"));
                if let Some(err) = error {
                    chunk_outcomes.push(Err(FaucetError::Sink(format!(
                        "Elasticsearch item rejected: {err}"
                    ))));
                } else {
                    chunk_outcomes.push(Ok(()));
                }
            }

            // Pad any missing tail positions defensively.
            while chunk_outcomes.len() < chunk.len() {
                chunk_outcomes.push(Err(FaucetError::Sink(
                    "Elasticsearch bulk response truncated — row outcome missing".into(),
                )));
            }

            outcomes.extend(chunk_outcomes);
        }

        Ok(outcomes)
    }

    fn connector_name(&self) -> &'static str {
        "elasticsearch"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_bulk_errors_reads_any_action_type() {
        // index + create actions, one with an error each.
        let body = json!({
            "errors": true,
            "items": [
                {"index": {"status": 201}},
                {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}},
                {"create": {"status": 409, "error": {"type": "version_conflict"}}},
            ]
        });
        let errs = extract_bulk_error_messages(&body);
        assert_eq!(errs.len(), 2, "{errs:?}");
        assert!(errs.iter().any(|e| e.contains("mapper_parsing_exception")));
        assert!(errs.iter().any(|e| e.contains("version_conflict")));
    }

    #[test]
    fn extract_bulk_errors_empty_when_no_item_errors() {
        // errors:true but the items shape carries no extractable error — the
        // caller treats this empty result as a hard failure (#78/#32).
        let body = json!({"errors": true, "items": [{"weird": {"status": 500}}]});
        assert!(extract_bulk_error_messages(&body).is_empty());
    }

    #[test]
    fn new_rejects_oversized_batch_size() {
        // Regression for #78/#44.
        let config = ElasticsearchSinkConfig::new("http://localhost:9200", "idx")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(ElasticsearchSink::new(config).is_err());
    }

    #[test]
    fn bulk_body_without_id_field() {
        let config = ElasticsearchSinkConfig::new("http://localhost:9200", "test_idx");
        let sink = ElasticsearchSink::new(config).unwrap();

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
        let sink = ElasticsearchSink::new(config).unwrap();

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
