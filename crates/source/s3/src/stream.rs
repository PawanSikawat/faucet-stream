//! S3 source stream executor.

use crate::config::{S3FileFormat, S3SourceConfig};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use faucet_core::shard::ShardSpec;
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::stream::{self, StreamExt, TryStreamExt};
use serde_json::Value;
use std::pin::Pin;
use std::sync::Mutex;
use tokio::io::AsyncBufReadExt;

/// An S3 source that lists and reads objects from a bucket.
pub struct S3Source {
    config: S3SourceConfig,
    client: Client,
    /// Shard applied by the cluster coordinator (Mode B): `(shards, index)`.
    /// `None` (or `shards <= 1`) reads every listed object. Stored behind a
    /// `Mutex` so `apply_shard(&self, …)` can record it before streaming.
    applied_shard: Mutex<Option<(usize, usize)>>,
}

/// Stable FNV-1a hash of an object key, used to assign keys to shards.
///
/// Deterministic across processes and platforms (all cluster workers run the
/// identical binary and this fixed algorithm), so every worker maps a given key
/// to the same shard index — the partition is disjoint and complete.
fn shard_hash(key: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

impl S3Source {
    /// Create a new S3 source from the given configuration.
    ///
    /// Builds the S3 client eagerly so it is reused across calls.
    pub async fn new(config: S3SourceConfig) -> Result<Self, FaucetError> {
        let client = Self::build_client(&config).await?;
        Ok(Self {
            config,
            client,
            applied_shard: Mutex::new(None),
        })
    }

    /// Retain only the keys belonging to the applied shard (hash-of-key modulo
    /// `shards`). A no-op when no shard is applied or `shards <= 1`.
    fn shard_filter(&self, keys: Vec<String>) -> Vec<String> {
        match *self.applied_shard.lock().expect("shard mutex poisoned") {
            Some((shards, index)) if shards > 1 => keys
                .into_iter()
                .filter(|k| (shard_hash(k) % shards as u64) == index as u64)
                .collect(),
            _ => keys,
        }
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
                FaucetError::Source(format!(
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
                    return Ok(self.shard_filter(keys));
                }
            }

            if response.is_truncated() == Some(true) {
                continuation_token = response.next_continuation_token().map(String::from);
            } else {
                break;
            }
        }

        Ok(self.shard_filter(keys))
    }

    /// Read and parse a single S3 object into records.
    async fn read_object(&self, key: &str) -> Result<Vec<Value>, FaucetError> {
        let text = self.read_object_text(key).await?;
        self.parse_content(key, &text)
    }

    /// Read the full body of a single S3 object into a UTF-8 `String`.
    ///
    /// Streams the (optionally decompressed) body straight into one `String`
    /// via [`open_object_reader`](Self::open_object_reader) rather than
    /// buffering the raw bytes AND the decompressed bytes AND the `String`
    /// at once (#78/#25). The whole object is still one unit for
    /// `JsonArray` / `RawText`, but peak memory is now ~1× the decoded size.
    async fn read_object_text(&self, key: &str) -> Result<String, FaucetError> {
        use tokio::io::AsyncReadExt as _;
        let mut reader = self.open_object_reader(key).await?;
        let mut text = String::new();
        reader.read_to_string(&mut text).await.map_err(|e| {
            FaucetError::Source(format!(
                "S3 read/decode error for key '{key}' (not valid UTF-8?): {e}"
            ))
        })?;
        Ok(text)
    }

    /// Open an S3 object as an [`AsyncBufRead`](tokio::io::AsyncBufRead) over
    /// its body. Used by [`Source::stream_pages`](faucet_core::Source::stream_pages)
    /// to decode `JsonLines` objects line-by-line without buffering the
    /// whole file.
    async fn open_object_reader(
        &self,
        key: &str,
    ) -> Result<std::pin::Pin<Box<dyn tokio::io::AsyncBufRead + Send + Unpin>>, FaucetError> {
        let mut request = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key);
        // Ask S3 to return its stored checksum so we can verify the body (#161).
        if self.config.verify_checksum {
            request = request.checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled);
        }
        let response = request.send().await.map_err(|e| {
            FaucetError::Source(format!("S3 get object error for key '{key}': {e}"))
        })?;

        // Read all metadata BEFORE consuming `body` (which partially moves
        // `response`), so a cleanly-truncated/corrupted transfer is rejected
        // rather than silently parsed as a complete object (#161).
        let mut checks: Vec<Box<dyn faucet_core::IntegrityCheck>> = Vec::new();
        match crate::verify::length_check(response.content_length(), self.config.verify_length) {
            Some(check) => checks.push(check),
            None if self.config.verify_length => tracing::debug!(
                key = %key,
                "S3 object reports no Content-Length; length verification skipped"
            ),
            None => {}
        }
        if self.config.verify_checksum {
            let advertised = crate::verify::S3Checksums {
                crc32: response.checksum_crc32().map(str::to_string),
                crc32c: response.checksum_crc32_c().map(str::to_string),
                crc64nvme: response.checksum_crc64_nvme().map(str::to_string),
                sha256: response.checksum_sha256().map(str::to_string),
                etag: response.e_tag().map(str::to_string),
            };
            match crate::verify::checksum_check(&advertised) {
                Some(check) => checks.push(check),
                None => tracing::warn!(
                    key = %key,
                    "verify_checksum is enabled but S3 advertised no verifiable checksum for \
                     this object; relying on the length check only"
                ),
            }
        }

        // `ByteStream::into_async_read` returns `impl AsyncRead`. Wrap the RAW
        // body in the verifier first so length/checksum cover the stored bytes
        // (below any decompression), then `BufReader` so `.lines()` is usable
        // and ownership is `Unpin`.
        let verified = faucet_core::VerifyingReader::new(response.body.into_async_read(), checks);
        let buffered = tokio::io::BufReader::new(verified);
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
                        FaucetError::Source(format!(
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
                    FaucetError::Source(format!("S3 JSON parse error in '{key}': {e}"))
                })?;
                match value {
                    Value::Array(arr) => Ok(arr),
                    _ => Err(FaucetError::Source(format!(
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
                            .map_err(|e| FaucetError::Source(format!(
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
                                    FaucetError::Source(format!(
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
                            FaucetError::Source(format!("S3 JSON parse error in '{key}': {e}"))
                        })?;
                        let array = match value {
                            Value::Array(arr) => arr,
                            other => Err(FaucetError::Source(format!(
                                "S3 expected JSON array in '{key}', got {}",
                                value_type_name(&other)
                            )))?,
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

    fn dataset_uri(&self) -> String {
        match &self.config.prefix {
            Some(p) => format!("s3://{}/{}", self.config.bucket, p),
            None => format!("s3://{}", self.config.bucket),
        }
    }

    /// The S3 source is always shardable: any object set can be split by
    /// hash-of-key. Sharding only takes effect when the cluster coordinator
    /// calls `apply_shard`; a plain `faucet run` reads
    /// every object.
    fn is_shardable(&self) -> bool {
        true
    }

    /// Enumerate `target` hash-modulo shards. Each shard `i` will read the
    /// objects whose key hashes to `i (mod target)`. No I/O: the partition is
    /// defined by the hash function, so enumeration is cheap and stable as new
    /// objects appear. `target <= 1` yields a single whole-dataset shard.
    async fn enumerate_shards(&self, target: usize) -> Result<Vec<ShardSpec>, FaucetError> {
        if target <= 1 {
            return Ok(vec![ShardSpec::whole()]);
        }
        let shards = (0..target)
            .map(|i| {
                ShardSpec::new(
                    i.to_string(),
                    serde_json::json!({ "shards": target, "index": i }),
                )
            })
            .collect();
        Ok(shards)
    }

    /// Narrow this source to one hash-modulo shard. The whole-dataset shard
    /// clears any filter (reads every object).
    async fn apply_shard(&self, shard: &ShardSpec) -> Result<(), FaucetError> {
        let parsed = if shard.is_whole() {
            None
        } else {
            let shards = shard
                .descriptor
                .get("shards")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    FaucetError::Source(format!(
                        "s3: invalid shard descriptor (missing 'shards'): {}",
                        shard.descriptor
                    ))
                })?;
            let index = shard
                .descriptor
                .get("index")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    FaucetError::Source(format!(
                        "s3: invalid shard descriptor (missing 'index'): {}",
                        shard.descriptor
                    ))
                })?;
            Some((shards as usize, index as usize))
        };
        *self.applied_shard.lock().expect("shard mutex poisoned") = parsed;
        Ok(())
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
    use faucet_core::Source;
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
        S3Source {
            config,
            client,
            applied_shard: Mutex::new(None),
        }
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

    // ── Hash-modulo sharding ────────────────────────────────────────────────

    #[test]
    fn shard_hash_is_deterministic() {
        assert_eq!(
            shard_hash("data/part-001.jsonl"),
            shard_hash("data/part-001.jsonl")
        );
        assert_ne!(shard_hash("a"), shard_hash("b"));
    }

    #[tokio::test]
    async fn enumerate_shards_returns_target_disjoint_shards() {
        let source = test_source(S3SourceConfig::new("b"));
        assert!(source.is_shardable());
        let shards = source.enumerate_shards(3).await.unwrap();
        assert_eq!(shards.len(), 3);
        for (i, s) in shards.iter().enumerate() {
            assert_eq!(s.descriptor["shards"], 3);
            assert_eq!(s.descriptor["index"], i);
        }
    }

    #[tokio::test]
    async fn enumerate_shards_target_one_is_whole() {
        let source = test_source(S3SourceConfig::new("b"));
        let shards = source.enumerate_shards(1).await.unwrap();
        assert_eq!(shards.len(), 1);
        assert!(shards[0].is_whole());
    }

    // The union of every shard's filtered key set equals the full set, with no
    // key in two shards — the core no-dup / no-loss guarantee.
    #[tokio::test]
    async fn shard_filter_partitions_keys_disjointly_and_completely() {
        let keys: Vec<String> = (0..200).map(|i| format!("data/obj-{i}.jsonl")).collect();
        let n = 4;
        let mut union: Vec<String> = Vec::new();
        for index in 0..n {
            let source = test_source(S3SourceConfig::new("b"));
            source
                .apply_shard(&ShardSpec::new(
                    index.to_string(),
                    serde_json::json!({ "shards": n, "index": index }),
                ))
                .await
                .unwrap();
            let got = source.shard_filter(keys.clone());
            union.extend(got);
        }
        union.sort();
        let mut expected = keys.clone();
        expected.sort();
        assert_eq!(
            union, expected,
            "shards must union to the full key set, disjointly"
        );
    }

    #[tokio::test]
    async fn apply_whole_shard_reads_everything() {
        let keys: Vec<String> = (0..20).map(|i| format!("k{i}")).collect();
        let source = test_source(S3SourceConfig::new("b"));
        source.apply_shard(&ShardSpec::whole()).await.unwrap();
        assert_eq!(source.shard_filter(keys.clone()).len(), keys.len());
    }

    #[tokio::test]
    async fn apply_shard_rejects_malformed_descriptor() {
        let source = test_source(S3SourceConfig::new("b"));
        let err = source
            .apply_shard(&ShardSpec::new("0", serde_json::json!({ "index": 0 })))
            .await
            .unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
    }

    #[test]
    fn dataset_uri_no_prefix() {
        let source = test_source(S3SourceConfig::new("my-bucket"));
        assert_eq!(source.dataset_uri(), "s3://my-bucket");
    }

    #[test]
    fn dataset_uri_with_prefix() {
        let source = test_source(S3SourceConfig::new("my-bucket").prefix("data/2026/"));
        assert_eq!(source.dataset_uri(), "s3://my-bucket/data/2026/");
    }
}
