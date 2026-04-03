//! S3 source stream executor.

use crate::config::{S3FileFormat, S3SourceConfig};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use faucet_core::FaucetError;
use futures::stream::{self, StreamExt, TryStreamExt};
use serde_json::Value;

/// An S3 source that lists and reads objects from a bucket.
pub struct S3Source {
    config: S3SourceConfig,
    client: Client,
}

impl S3Source {
    /// Create a new S3 source from the given configuration.
    ///
    /// Builds the S3 client eagerly so it is reused across calls.
    pub async fn new(config: S3SourceConfig) -> Result<Self, FaucetError> {
        let client = Self::build_client(&config).await?;
        Ok(Self { config, client })
    }

    /// Build an S3 client from the configuration.
    async fn build_client(config: &S3SourceConfig) -> Result<Client, FaucetError> {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

        if let Some(ref region) = config.region {
            config_loader = config_loader.region(aws_config::Region::new(region.clone()));
        }

        if let Some(ref endpoint) = config.endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint);
        }

        let sdk_config = config_loader.load().await;
        let client = Client::new(&sdk_config);
        Ok(client)
    }

    /// List object keys matching the configured bucket and prefix.
    async fn list_object_keys(&self) -> Result<Vec<String>, FaucetError> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self.client.list_objects_v2().bucket(&self.config.bucket);

            if let Some(ref prefix) = self.config.prefix {
                req = req.prefix(prefix);
            }

            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }

            let response = req.send().await.map_err(|e| {
                FaucetError::Config(format!(
                    "S3 list objects error for bucket '{}': {e}",
                    self.config.bucket
                ))
            })?;

            for object in response.contents() {
                let key: &str = object.key().unwrap_or_default();
                if key.is_empty() {
                    continue;
                }
                keys.push(key.to_string());

                if let Some(max) = self.config.max_objects
                    && keys.len() >= max
                {
                    return Ok(keys);
                }
            }

            if response.is_truncated() == Some(true) {
                continuation_token = response.next_continuation_token().map(String::from);
            } else {
                break;
            }
        }

        Ok(keys)
    }

    /// Read and parse a single S3 object into records.
    async fn read_object(&self, key: &str) -> Result<Vec<Value>, FaucetError> {
        let response = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                FaucetError::Config(format!("S3 get object error for key '{key}': {e}"))
            })?;

        let body =
            response.body.collect().await.map_err(|e| {
                FaucetError::Config(format!("S3 read body error for key '{key}': {e}"))
            })?;

        let text = String::from_utf8(body.into_bytes().to_vec()).map_err(|e| {
            FaucetError::Config(format!("S3 UTF-8 decode error for key '{key}': {e}"))
        })?;

        self.parse_content(key, &text)
    }

    /// Parse file content into records based on the configured file format.
    fn parse_content(&self, key: &str, text: &str) -> Result<Vec<Value>, FaucetError> {
        match self.config.file_format {
            S3FileFormat::JsonLines => {
                let mut records = Vec::new();
                for (line_num, line) in text.lines().enumerate() {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let value: Value = serde_json::from_str(trimmed).map_err(|e| {
                        FaucetError::Config(format!(
                            "S3 JSON parse error in '{key}' at line {}: {e}",
                            line_num + 1
                        ))
                    })?;
                    records.push(value);
                }
                Ok(records)
            }
            S3FileFormat::JsonArray => {
                let value: Value = serde_json::from_str(text).map_err(|e| {
                    FaucetError::Config(format!("S3 JSON parse error in '{key}': {e}"))
                })?;
                match value {
                    Value::Array(arr) => Ok(arr),
                    _ => Err(FaucetError::Config(format!(
                        "S3 expected JSON array in '{key}', got {}",
                        value_type_name(&value)
                    ))),
                }
            }
            S3FileFormat::RawText => {
                let record = serde_json::json!({
                    "key": key,
                    "content": text,
                });
                Ok(vec![record])
            }
        }
    }
}

#[async_trait]
impl faucet_core::Source for S3Source {
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        let keys = self.list_object_keys().await?;

        tracing::info!(
            bucket = %self.config.bucket,
            objects = keys.len(),
            "Listed S3 objects"
        );

        let concurrency = self.config.concurrency.max(1);

        let results: Vec<Vec<Value>> = stream::iter(keys)
            .map(|key| async move {
                let records = self.read_object(&key).await?;
                tracing::debug!(key = %key, records = records.len(), "Read S3 object");
                Ok::<Vec<Value>, FaucetError>(records)
            })
            .buffer_unordered(concurrency)
            .try_collect()
            .await?;

        let all_records: Vec<Value> = results.into_iter().flatten().collect();

        tracing::info!(total_records = all_records.len(), "S3 fetch complete");
        Ok(all_records)
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(S3SourceConfig))
            .expect("schema serialization")
    }
}

/// Return a human-readable name for a JSON value type.
fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::S3SourceConfig;
    use serde_json::json;

    /// Helper to build an S3Source synchronously for parse-only tests.
    /// We construct it directly to avoid needing an async runtime for unit tests
    /// that only exercise `parse_content`.
    fn test_source(config: S3SourceConfig) -> S3Source {
        // Build a dummy client — these tests never make network calls.
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build();
        let client = Client::new(&sdk_config);
        S3Source { config, client }
    }

    #[test]
    fn parse_json_lines() {
        let source = test_source(S3SourceConfig::new("test"));
        let text = r#"{"id":1,"name":"Alice"}
{"id":2,"name":"Bob"}
"#;
        let records = source.parse_content("test.jsonl", text).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], 1);
        assert_eq!(records[1]["name"], "Bob");
    }

    #[test]
    fn parse_json_lines_skips_empty() {
        let source = test_source(S3SourceConfig::new("test"));
        let text = r#"{"id":1}

{"id":2}

"#;
        let records = source.parse_content("test.jsonl", text).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn parse_json_lines_invalid() {
        let source = test_source(S3SourceConfig::new("test"));
        let text = "not json\n";
        let result = source.parse_content("test.jsonl", text);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("JSON parse error"));
        assert!(err.contains("line 1"));
    }

    #[test]
    fn parse_json_array() {
        let source = test_source(S3SourceConfig::new("test").file_format(S3FileFormat::JsonArray));
        let text = r#"[{"id":1},{"id":2}]"#;
        let records = source.parse_content("test.json", text).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], 1);
    }

    #[test]
    fn parse_json_array_not_array() {
        let source = test_source(S3SourceConfig::new("test").file_format(S3FileFormat::JsonArray));
        let text = r#"{"id":1}"#;
        let result = source.parse_content("test.json", text);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("expected JSON array"));
    }

    #[test]
    fn parse_raw_text() {
        let source = test_source(S3SourceConfig::new("test").file_format(S3FileFormat::RawText));
        let text = "hello world\nline two";
        let records = source.parse_content("data/file.txt", text).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0],
            json!({"key": "data/file.txt", "content": "hello world\nline two"})
        );
    }
}
