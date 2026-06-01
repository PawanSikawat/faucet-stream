//! GCS source stream executor.

use crate::config::{GcsFileFormat, GcsSourceConfig};
use async_trait::async_trait;
use faucet_core::{FaucetError, Stream, StreamPage};
use faucet_common_gcs::{build_storage, build_storage_control};
use futures::stream::{self, StreamExt, TryStreamExt};
use google_cloud_gax::paginator::ItemPaginator;
use google_cloud_storage::client::{Storage, StorageControl};
use serde_json::Value;
use std::pin::Pin;
use tokio::io::AsyncBufReadExt;

/// A GCS source that lists and reads objects from a bucket.
pub struct GcsSource {
    config: GcsSourceConfig,
    storage: Storage,
    control: StorageControl,
}

impl GcsSource {
    /// Construct the source. Builds both clients eagerly so they are
    /// reused across calls.
    pub async fn new(config: GcsSourceConfig) -> Result<Self, FaucetError> {
        let storage = build_storage(&config.auth, config.storage_host.as_deref()).await?;
        let control = build_storage_control(&config.auth, config.storage_host.as_deref()).await?;
        Ok(Self {
            config,
            storage,
            control,
        })
    }

    /// Bucket as a GCS resource path: `projects/_/buckets/{bucket}`.
    fn bucket_path(&self) -> String {
        format!("projects/_/buckets/{}", self.config.bucket)
    }

    /// List object names under the configured (or override) prefix,
    /// capped at `max_objects` if set.
    async fn list_object_names(
        &self,
        prefix_override: Option<&str>,
    ) -> Result<Vec<String>, FaucetError> {
        if let Some(ref keys) = self.config.object_keys {
            return Ok(cap_keys(keys.clone(), self.config.max_objects));
        }

        let effective_prefix = prefix_override.or(self.config.prefix.as_deref());
        let mut req = self.control.list_objects().set_parent(self.bucket_path());
        if let Some(p) = effective_prefix {
            req = req.set_prefix(p.to_string());
        }
        req = req.set_page_size(1000_i32);

        let mut paginator = req.by_item();
        let mut names: Vec<String> = Vec::new();
        while let Some(item) = paginator.next().await {
            let object = item.map_err(|e| {
                FaucetError::Source(format!(
                    "GCS list error for bucket '{}': {e}",
                    self.config.bucket
                ))
            })?;
            if object.name.is_empty() {
                continue;
            }
            names.push(object.name);
            if let Some(max) = self.config.max_objects
                && names.len() >= max
            {
                break;
            }
        }
        Ok(names)
    }

    /// Read the full body of a single GCS object into a UTF-8 `String`.
    async fn read_object_text(&self, key: &str) -> Result<String, FaucetError> {
        // Stream the (optionally decompressed) body straight into one String
        // via the same reader the line-streaming path uses, instead of holding
        // the raw bytes AND the decompressed bytes AND the String at once
        // (#78/#25). For JsonArray / RawText the whole object is still one
        // unit, but peak memory is now ~1× the decoded size rather than ~3×.
        use tokio::io::AsyncReadExt as _;
        let mut reader = self.open_object_reader(key).await?;
        let mut text = String::new();
        reader.read_to_string(&mut text).await.map_err(|e| {
            FaucetError::Source(format!(
                "GCS read/decode error for key '{key}' (not valid UTF-8?): {e}"
            ))
        })?;
        Ok(text)
    }

    /// Open a GCS object as an `AsyncBufRead` over its body so callers can
    /// decode line-by-line without buffering the entire object.
    ///
    /// Requires the `unstable-stream` feature on `google-cloud-storage`.
    async fn open_object_reader(
        &self,
        key: &str,
    ) -> Result<std::pin::Pin<Box<dyn tokio::io::AsyncBufRead + Send + Unpin>>, FaucetError> {
        let resp = self
            .storage
            .read_object(self.bucket_path(), key.to_string())
            .send()
            .await
            .map_err(|e| {
                FaucetError::Source(format!(
                    "GCS get error for bucket '{}' key '{key}': {e}",
                    self.config.bucket
                ))
            })?;
        let bytes_stream = resp
            .into_stream()
            .map_err(|e| std::io::Error::other(e.to_string()));
        let buffered = tokio::io::BufReader::new(tokio_util::io::StreamReader::new(bytes_stream));
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
            GcsFileFormat::JsonLines => {
                let mut records = Vec::new();
                for (line_num, line) in text.lines().enumerate() {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let value: Value = serde_json::from_str(trimmed).map_err(|e| {
                        FaucetError::Source(format!(
                            "GCS JSON parse error in '{key}' at line {}: {e}",
                            line_num + 1
                        ))
                    })?;
                    records.push(value);
                }
                Ok(records)
            }
            GcsFileFormat::JsonArray => {
                let value: Value = serde_json::from_str(text).map_err(|e| {
                    FaucetError::Source(format!("GCS JSON parse error in '{key}': {e}"))
                })?;
                match value {
                    Value::Array(arr) => Ok(arr),
                    other => Err(FaucetError::Source(format!(
                        "GCS expected JSON array in '{key}', got {}",
                        value_type_name(&other)
                    ))),
                }
            }
            GcsFileFormat::RawText => Ok(vec![serde_json::json!({
                "key": key,
                "content": text,
            })]),
        }
    }
}

#[async_trait]
impl faucet_core::Source for GcsSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let substituted_prefix: Option<String> = if !context.is_empty() {
            self.config
                .prefix
                .as_ref()
                .map(|p| faucet_core::util::substitute_context(p, context))
        } else {
            None
        };

        let keys = self
            .list_object_names(substituted_prefix.as_deref())
            .await?;
        tracing::info!(
            bucket = %self.config.bucket,
            objects = keys.len(),
            "Listed GCS objects",
        );

        let concurrency = self.config.concurrency.max(1);
        let results: Vec<Vec<Value>> = stream::iter(keys)
            .map(|key| async move {
                let text = self.read_object_text(&key).await?;
                let records = self.parse_content(&key, &text)?;
                tracing::debug!(key = %key, records = records.len(), "Read GCS object");
                Ok::<Vec<Value>, FaucetError>(records)
            })
            .buffer_unordered(concurrency)
            .try_collect()
            .await?;

        let all_records: Vec<Value> = results.into_iter().flatten().collect();
        tracing::info!(total_records = all_records.len(), "GCS fetch complete");
        Ok(all_records)
    }

    /// Stream records from listed GCS objects without buffering the full
    /// scan. Mirrors `S3Source::stream_pages` — see that implementation
    /// for the per-format reasoning.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let substituted_prefix: Option<String> = if !context.is_empty() {
                self.config
                    .prefix
                    .as_ref()
                    .map(|p| faucet_core::util::substitute_context(p, context))
            } else {
                None
            };

            let keys = self.list_object_names(substituted_prefix.as_deref()).await?;
            tracing::info!(
                bucket = %self.config.bucket,
                objects = keys.len(),
                "Listed GCS objects (stream)",
            );

            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            for key in &keys {
                match self.config.file_format {
                    GcsFileFormat::JsonLines => {
                        let reader = self.open_object_reader(key).await?;
                        let mut lines = reader.lines();
                        let mut line_num: usize = 0;
                        while let Some(line) = lines
                            .next_line()
                            .await
                            .map_err(|e| FaucetError::Source(format!(
                                "GCS read body error for key '{key}': {e}"
                            )))?
                        {
                            line_num += 1;
                            let trimmed = line.trim();
                            if trimmed.is_empty() { continue; }
                            let value: Value = serde_json::from_str(trimmed).map_err(|e| {
                                FaucetError::Source(format!(
                                    "GCS JSON parse error in '{key}' at line {line_num}: {e}",
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
                    GcsFileFormat::RawText => {
                        let text = self.read_object_text(key).await?;
                        let record = serde_json::json!({ "key": key, "content": text });
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
                    GcsFileFormat::JsonArray => {
                        let text = self.read_object_text(key).await?;
                        let value: Value = serde_json::from_str(&text).map_err(|e| {
                            FaucetError::Source(format!("GCS JSON parse error in '{key}': {e}"))
                        })?;
                        let array = match value {
                            Value::Array(arr) => arr,
                            other => Err(FaucetError::Source(format!(
                                "GCS expected JSON array in '{key}', got {}",
                                value_type_name(&other)
                            )))?,
                        };
                        if batch_size == 0 {
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
                "GCS source stream complete",
            );
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(GcsSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "gcs"
    }
}

/// Truncate an explicit object-key list to the `max_objects` cap.
///
/// `None` leaves the list untouched; `Some(n)` keeps at most the first `n`
/// keys. This mirrors the cap the listing path applies while paginating, so
/// `max_objects` is honoured whether keys come from `object_keys` or a live
/// `list_objects` scan.
fn cap_keys(mut keys: Vec<String>, max: Option<usize>) -> Vec<String> {
    if let Some(n) = max {
        keys.truncate(n);
    }
    keys
}

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
    use serde_json::json;

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = GcsSourceConfig::new("bucket");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }

    /// Construct a parse-only test config — used by tests that don't touch
    /// the network. We can't easily construct a real `GcsSource` without
    /// a runtime, so we test `parse_content` via a free helper that
    /// inlines the same logic with an explicit `format` argument.
    fn parse(format: GcsFileFormat, key: &str, text: &str) -> Result<Vec<Value>, FaucetError> {
        // Mirror the implementation in `parse_content`. Kept in sync with
        // production code via the integration test suite.
        match format {
            GcsFileFormat::JsonLines => {
                let mut records = Vec::new();
                for (line_num, line) in text.lines().enumerate() {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let value: Value = serde_json::from_str(trimmed).map_err(|e| {
                        FaucetError::Source(format!(
                            "GCS JSON parse error in '{key}' at line {}: {e}",
                            line_num + 1
                        ))
                    })?;
                    records.push(value);
                }
                Ok(records)
            }
            GcsFileFormat::JsonArray => {
                let value: Value = serde_json::from_str(text).map_err(|e| {
                    FaucetError::Source(format!("GCS JSON parse error in '{key}': {e}"))
                })?;
                match value {
                    Value::Array(arr) => Ok(arr),
                    other => Err(FaucetError::Source(format!(
                        "GCS expected JSON array in '{key}', got {}",
                        value_type_name(&other)
                    ))),
                }
            }
            GcsFileFormat::RawText => Ok(vec![json!({
                "key": key,
                "content": text,
            })]),
        }
    }

    #[test]
    fn parse_json_lines() {
        let r = parse(GcsFileFormat::JsonLines, "t", "{\"id\":1}\n{\"id\":2}\n").unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0]["id"], 1);
    }

    #[test]
    fn parse_json_lines_skips_blanks() {
        let r = parse(
            GcsFileFormat::JsonLines,
            "t",
            "{\"id\":1}\n\n{\"id\":2}\n\n",
        )
        .unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn parse_json_lines_reports_line_number() {
        let err = parse(GcsFileFormat::JsonLines, "t", "{\"id\":1}\nbad-line\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("line 2"), "unexpected: {msg}");
    }

    #[test]
    fn parse_json_array() {
        let r = parse(
            GcsFileFormat::JsonArray,
            "t.json",
            "[{\"id\":1},{\"id\":2}]",
        )
        .unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn parse_json_array_rejects_non_array() {
        let err = parse(GcsFileFormat::JsonArray, "t.json", "{\"id\":1}").unwrap_err();
        assert!(err.to_string().contains("expected JSON array"));
    }

    #[test]
    fn parse_raw_text_yields_single_record() {
        let r = parse(GcsFileFormat::RawText, "p/f.txt", "hello").unwrap();
        assert_eq!(r, vec![json!({"key": "p/f.txt", "content": "hello"})]);
    }

    #[test]
    fn cap_keys_truncates_explicit_list_to_max_objects() {
        let keys = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let capped = cap_keys(keys, Some(2));
        assert_eq!(capped, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn cap_keys_passes_through_when_no_max() {
        let keys = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let capped = cap_keys(keys.clone(), None);
        assert_eq!(capped, keys);
    }

    #[test]
    fn cap_keys_noop_when_max_exceeds_len() {
        let keys = vec!["a".to_string(), "b".to_string()];
        let capped = cap_keys(keys.clone(), Some(10));
        assert_eq!(capped, keys);
    }
}
