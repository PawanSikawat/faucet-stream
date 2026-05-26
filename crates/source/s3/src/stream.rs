//! S3 source stream executor.

use crate::config::{S3FileFormat, S3SourceConfig};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::stream::{self, StreamExt, TryStreamExt};
use serde_json::Value;
use std::pin::Pin;
use tokio::io::AsyncBufReadExt;

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
    ///
    /// When `prefix_override` is `Some`, it is used instead of `self.config.prefix`
    /// (used for parent-context substitution).
    async fn list_object_keys(
        &self,
        prefix_override: Option<&str>,
    ) -> Result<Vec<String>, FaucetError> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        let effective_prefix = prefix_override.or(self.config.prefix.as_deref());

        loop {
            let mut req = self.client.list_objects_v2().bucket(&self.config.bucket);

            if let Some(prefix) = effective_prefix {
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
        let text = self.read_object_text(key).await?;
        self.parse_content(key, &text)
    }

    /// Read the full body of a single S3 object into a UTF-8 `String`.
    async fn read_object_text(&self, key: &str) -> Result<String, FaucetError> {
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
        let bytes = body.into_bytes();

        #[cfg(feature = "compression")]
        let bytes = {
            let codec = self.config.compression.resolve(key);
            faucet_core::compression::warn_mismatch(key, codec);
            if codec == faucet_core::Compression::None {
                bytes.to_vec()
            } else {
                use std::io::Read;
                let mut r = faucet_core::compression::wrap_sync_reader(
                    std::io::Cursor::new(bytes.to_vec()),
                    codec,
                );
                let mut out = Vec::new();
                r.read_to_end(&mut out).map_err(|e| {
                    FaucetError::Source(format!("decompression failed for key '{key}': {e}"))
                })?;
                out
            }
        };
        #[cfg(not(feature = "compression"))]
        let bytes = bytes.to_vec();

        String::from_utf8(bytes)
            .map_err(|e| FaucetError::Config(format!("S3 UTF-8 decode error for key '{key}': {e}")))
    }

    /// Open an S3 object as an [`AsyncBufRead`](tokio::io::AsyncBufRead) over
    /// its body. Used by [`Source::stream_pages`](faucet_core::Source::stream_pages)
    /// to decode `JsonLines` objects line-by-line without buffering the
    /// whole file.
    async fn open_object_reader(
        &self,
        key: &str,
    ) -> Result<
        std::pin::Pin<Box<dyn tokio::io::AsyncBufRead + Send + Unpin>>,
        FaucetError,
    > {
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

        // `ByteStream::into_async_read` returns `impl AsyncRead`; wrap in a
        // `BufReader` so `.lines()` is usable and ownership is `Unpin`.
        let buffered = tokio::io::BufReader::new(response.body.into_async_read());
        #[cfg(feature = "compression")]
        {
            let codec = self.config.compression.resolve(key);
            faucet_core::compression::warn_mismatch(key, codec);
            Ok(faucet_core::compression::wrap_async_reader(buffered, codec))
        }
        #[cfg(not(feature = "compression"))]
        {
            Ok(Box::pin(buffered))
        }
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
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        // Substitute context into prefix when parent context is provided.
        let substituted_prefix: Option<String> = if !context.is_empty() {
            self.config
                .prefix
                .as_ref()
                .map(|p| faucet_core::util::substitute_context(p, context))
        } else {
            None
        };

        let keys = self.list_object_keys(substituted_prefix.as_deref()).await?;

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

    /// Stream records from listed S3 objects without buffering the full
    /// scan. Each emitted [`StreamPage`] holds up to
    /// [`S3SourceConfig::batch_size`] records.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// Behaviour by format:
    ///
    /// - `JsonLines` / `RawText`: the object body is decoded line-by-line
    ///   via [`tokio::io::AsyncBufReadExt::lines`] so client-side memory is
    ///   bounded at `O(batch_size)` per object. Multi-object scans are
    ///   flattened — a single page may carry lines drawn from any object.
    /// - `JsonArray`: each object is buffered fully (the JSON value can
    ///   only be parsed once the array is complete) and then its records
    ///   are chunked into pages of `batch_size`. See the README "Streaming
    ///   and batching" section for the caveat.
    ///
    /// `batch_size = 0` is the "no batching" sentinel: one [`StreamPage`]
    /// is emitted per S3 object (no within-object chunking and no
    /// cross-object accumulation). The S3 source has no
    /// incremental-replication mode today, so every emitted page carries
    /// `bookmark: None`.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            // Substitute context into prefix when parent context is provided.
            let substituted_prefix: Option<String> = if !context.is_empty() {
                self.config
                    .prefix
                    .as_ref()
                    .map(|p| faucet_core::util::substitute_context(p, context))
            } else {
                None
            };

            let keys = self.list_object_keys(substituted_prefix.as_deref()).await?;
            tracing::info!(
                bucket = %self.config.bucket,
                objects = keys.len(),
                "Listed S3 objects (stream)",
            );

            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            for key in &keys {
                match self.config.file_format {
                    S3FileFormat::JsonLines => {
                        let reader = self.open_object_reader(key).await?;
                        let mut lines = reader.lines();
                        let mut line_num: usize = 0;
                        while let Some(line) = lines
                            .next_line()
                            .await
                            .map_err(|e| FaucetError::Config(format!(
                                "S3 read body error for key '{key}': {e}"
                            )))?
                        {
                            line_num += 1;
                            let trimmed = line.trim();
                            if trimmed.is_empty() {
                                continue;
                            }
                            let value: Value =
                                serde_json::from_str(trimmed).map_err(|e| {
                                    FaucetError::Config(format!(
                                        "S3 JSON parse error in '{key}' at line {line_num}: {e}",
                                    ))
                                })?;
                            buffer.push(value);
                            if batch_size != 0 && buffer.len() >= chunk {
                                let page = std::mem::replace(
                                    &mut buffer,
                                    Vec::with_capacity(initial_capacity),
                                );
                                total += page.len();
                                yield StreamPage { records: page, bookmark: None };
                            }
                        }
                        if batch_size == 0 && !buffer.is_empty() {
                            let page = std::mem::take(&mut buffer);
                            total += page.len();
                            yield StreamPage { records: page, bookmark: None };
                        }
                    }
                    S3FileFormat::RawText => {
                        // RawText emits a single record per object; the
                        // `key` + `content` shape is unchanged so we
                        // continue to buffer the body fully. This still
                        // streams *across* objects.
                        let text = self.read_object_text(key).await?;
                        let record = serde_json::json!({
                            "key": key,
                            "content": text,
                        });
                        buffer.push(record);
                        if batch_size == 0 {
                            let page = std::mem::take(&mut buffer);
                            total += page.len();
                            yield StreamPage { records: page, bookmark: None };
                        } else if buffer.len() >= chunk {
                            let page = std::mem::replace(
                                &mut buffer,
                                Vec::with_capacity(initial_capacity),
                            );
                            total += page.len();
                            yield StreamPage { records: page, bookmark: None };
                        }
                    }
                    S3FileFormat::JsonArray => {
                        // JSON-array files cannot be parsed incrementally
                        // (the closing `]` is required to validate the
                        // structure), so each object is buffered fully and
                        // then chunked. The caveat is documented in the
                        // crate README.
                        let text = self.read_object_text(key).await?;
                        let value: Value = serde_json::from_str(&text).map_err(|e| {
                            FaucetError::Config(format!("S3 JSON parse error in '{key}': {e}"))
                        })?;
                        let array = match value {
                            Value::Array(arr) => arr,
                            other => {
                                Err(FaucetError::Config(format!(
                                    "S3 expected JSON array in '{key}', got {}",
                                    value_type_name(&other)
                                )))?;
                                unreachable!()
                            }
                        };
                        if batch_size == 0 {
                            // Flush any cross-object buffer first (none
                            // here because each iteration completes its
                            // own object — but keep symmetric with the
                            // line-shaped branches).
                            if !buffer.is_empty() {
                                let page = std::mem::take(&mut buffer);
                                total += page.len();
                                yield StreamPage { records: page, bookmark: None };
                            }
                            total += array.len();
                            yield StreamPage { records: array, bookmark: None };
                        } else {
                            for record in array {
                                buffer.push(record);
                                if buffer.len() >= chunk {
                                    let page = std::mem::replace(
                                        &mut buffer,
                                        Vec::with_capacity(initial_capacity),
                                    );
                                    total += page.len();
                                    yield StreamPage { records: page, bookmark: None };
                                }
                            }
                        }
                    }
                }
            }

            if !buffer.is_empty() {
                let page = std::mem::take(&mut buffer);
                total += page.len();
                yield StreamPage { records: page, bookmark: None };
            }

            tracing::info!(
                total_records = total,
                batch_size,
                objects = keys.len(),
                "S3 source stream complete",
            );
        })
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

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = S3SourceConfig::new("bucket");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }
}
