//! Parquet sink executor.

use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use faucet_core::FaucetError;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::{AsyncFileWriter, ParquetObjectWriter};
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config::{ParquetDestination, ParquetSinkConfig, SchemaSource};
use crate::schema::infer_schema;

/// A sink that writes JSON records as Apache Parquet files.
///
/// Lazily opens the first writer on the initial `write_batch` call so the
/// schema can be inferred from real records. Closing the sink — and writing
/// the Parquet footer — only happens on `flush()`; callers that skip it will
/// produce unreadable files.
pub struct ParquetSink {
    config: ParquetSinkConfig,
    store: Arc<dyn ObjectStore>,
    /// The directory portion of a `LocalPath` destination (created on `new`),
    /// or `None` for S3.
    local_root: Option<PathBuf>,
    state: Mutex<WriterState>,
}

/// Bookkeeping that mutates as we write.
struct WriterState {
    /// `None` until the first batch arrives.
    schema: Option<SchemaRef>,
    /// `None` between rollovers and at construction.
    writer: Option<AsyncArrowWriter<Box<dyn AsyncFileWriter>>>,
    /// Rows accepted by the current writer.
    rows_in_current_file: usize,
    /// Total files closed successfully (for diagnostics).
    files_written: usize,
}

impl WriterState {
    fn new() -> Self {
        Self {
            schema: None,
            writer: None,
            rows_in_current_file: 0,
            files_written: 0,
        }
    }
}

impl ParquetSink {
    /// Create a new Parquet sink. The underlying object store is built eagerly
    /// so that bad configuration fails fast rather than on first write.
    pub async fn new(config: ParquetSinkConfig) -> Result<Self, FaucetError> {
        config
            .validate()
            .map_err(|e| FaucetError::Config(format!("invalid parquet sink config: {e}")))?;

        let (store, local_root) = build_store(&config.destination).await?;

        Ok(Self {
            config,
            store,
            local_root,
            state: Mutex::new(WriterState::new()),
        })
    }

    /// Whether the configured destination writes one file per "key" prefix +
    /// uuid (S3 or directory-style local) or to a single fixed file.
    fn single_file_mode(&self) -> bool {
        match &self.config.destination {
            ParquetDestination::LocalPath { path } => {
                let p = FsPath::new(path);
                p.extension().and_then(|s| s.to_str()) == Some("parquet")
                    && self.config.max_rows_per_file.is_none()
                    && self.config.max_bytes_per_file.is_none()
            }
            ParquetDestination::S3(_) => false,
        }
    }

    /// Build the object_store `Path` for the next file. Each new file gets a
    /// unique UUID-suffixed name unless we're in single-file mode.
    ///
    /// Uses `Path::from_absolute_path` (not `from_filesystem_path`) because
    /// the target file does not exist yet — canonicalize would fail.
    fn next_object_path(&self) -> Result<ObjPath, FaucetError> {
        match &self.config.destination {
            ParquetDestination::LocalPath { path } => {
                let pb = if self.single_file_mode() {
                    PathBuf::from(path)
                } else {
                    let root = self
                        .local_root
                        .as_ref()
                        .expect("local_root set on local dest");
                    root.join(format!("{}.parquet", Uuid::new_v4()))
                };
                let absolute = if pb.is_absolute() {
                    pb.clone()
                } else {
                    std::env::current_dir()
                        .map_err(|e| FaucetError::Sink(format!("could not read cwd: {e}")))?
                        .join(&pb)
                };
                ObjPath::from_absolute_path(&absolute).map_err(|e| {
                    FaucetError::Sink(format!(
                        "could not encode local path {}: {e}",
                        absolute.display()
                    ))
                })
            }
            ParquetDestination::S3(s3) => {
                let key = format!("{}{}.parquet", s3.prefix, Uuid::new_v4());
                ObjPath::parse(&key)
                    .map_err(|e| FaucetError::Sink(format!("invalid s3 prefix/key '{key}': {e}")))
            }
        }
    }

    fn writer_properties(&self) -> WriterProperties {
        WriterProperties::builder()
            .set_compression(self.config.compression.as_parquet())
            .set_max_row_group_row_count(Some(self.config.row_group_size))
            .build()
    }

    async fn open_writer(
        &self,
        schema: SchemaRef,
    ) -> Result<AsyncArrowWriter<Box<dyn AsyncFileWriter>>, FaucetError> {
        let obj_path = self.next_object_path()?;
        let writer = ParquetObjectWriter::new(self.store.clone(), obj_path);
        let boxed: Box<dyn AsyncFileWriter> = Box::new(writer);
        AsyncArrowWriter::try_new(boxed, schema, Some(self.writer_properties()))
            .map_err(|e| FaucetError::Sink(format!("could not open parquet writer: {e}")))
    }

    /// Build a `RecordBatch` for `records` against the locked-in schema.
    ///
    /// Unknown fields (present in records but not in schema) are silently
    /// dropped by `arrow_json::Decoder` because we leave `strict_mode` at its
    /// default `false`; we still emit a `tracing::warn!` for visibility.
    ///
    /// Type drift (a field whose JSON type disagrees with the schema's
    /// `DataType`) is surfaced as a `FaucetError::Sink` naming the field and
    /// both sides of the mismatch.
    fn encode_batch(
        &self,
        schema: SchemaRef,
        records: &[Value],
    ) -> Result<RecordBatch, FaucetError> {
        warn_on_unknown_fields(&schema, records);

        let mut decoder = arrow_json::ReaderBuilder::new(schema.clone())
            .build_decoder()
            .map_err(|e| FaucetError::Sink(format!("could not build json decoder: {e}")))?;

        decoder
            .serialize(records)
            .map_err(|e| classify_decoder_error(&schema, records, e))?;

        let batch = decoder
            .flush()
            .map_err(|e| classify_decoder_error(&schema, records, e))?
            .ok_or_else(|| {
                FaucetError::Sink("json decoder produced no record batch".to_string())
            })?;
        Ok(batch)
    }

    /// Encode + write a single chunk of records, applying row/byte rollover
    /// after the write. Skips entirely on an empty chunk so callers do not
    /// have to guard.
    ///
    /// Splitting this out of `write_batch` lets the public entry point chunk
    /// large pages by `config.batch_size` while keeping the schema /
    /// writer-init / rollover invariants in one place.
    async fn write_chunk(
        &self,
        state: &mut WriterState,
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        if state.schema.is_none() {
            let schema = match &self.config.schema {
                Some(SchemaSource::Explicit {}) => {
                    return Err(FaucetError::Config(
                        "explicit parquet schemas are not supported yet; use inferred".to_string(),
                    ));
                }
                _ => infer_schema(records, self.config.effective_sample_size())?,
            };
            state.schema = Some(schema);
        }
        let schema = state.schema.clone().expect("schema set above");

        if state.writer.is_none() {
            state.writer = Some(self.open_writer(schema.clone()).await?);
        }

        let batch = self.encode_batch(schema.clone(), records)?;
        let batch_rows = batch.num_rows();

        let estimated_size = {
            let writer = state.writer.as_mut().expect("writer set above");
            writer
                .write(&batch)
                .await
                .map_err(|e| FaucetError::Sink(format!("parquet write failed: {e}")))?;
            // `bytes_written` only counts data already flushed to the
            // underlying sink; `in_progress_size` counts what is still
            // buffered in the active row group. We must sum both to know when
            // a file has reached the user's byte cap, otherwise large
            // partially-buffered row groups can hide indefinitely.
            writer.bytes_written() + writer.in_progress_size()
        };
        state.rows_in_current_file += batch_rows;

        if should_rollover(&self.config, state, estimated_size) {
            tracing::debug!(
                rows = state.rows_in_current_file,
                bytes = estimated_size,
                "Rolling over parquet file"
            );
            self.close_current(state).await?;
        }

        Ok(batch_rows)
    }

    /// Close the current writer (writing the parquet footer) and clear state.
    async fn close_current(&self, state: &mut WriterState) -> Result<(), FaucetError> {
        if let Some(writer) = state.writer.take() {
            writer
                .close()
                .await
                .map_err(|e| FaucetError::Sink(format!("could not close parquet writer: {e}")))?;
            state.files_written += 1;
            state.rows_in_current_file = 0;
        }
        Ok(())
    }
}

#[async_trait]
impl faucet_core::Sink for ParquetSink {
    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(ParquetSinkConfig))
            .expect("schema serialization")
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut state = self.state.lock().await;

        // Re-chunk the incoming page when the config asks for it. `batch_size
        // = 0` is the "no batching" sentinel: pass the page straight through.
        // Otherwise we slice into `batch_size`-sized windows and run the
        // existing write path once per chunk. Row/byte rollover logic is
        // applied per chunk and remains independent of `batch_size`.
        let chunk_size = self.config.batch_size;
        let mut total_rows = 0;
        if chunk_size == 0 || records.len() <= chunk_size {
            total_rows += self.write_chunk(&mut state, records).await?;
        } else {
            for chunk in records.chunks(chunk_size) {
                total_rows += self.write_chunk(&mut state, chunk).await?;
            }
        }

        Ok(total_rows)
    }

    /// Closes the in-flight Parquet writer so the file footer is flushed to
    /// disk / S3. Files left without `flush()` are unreadable.
    async fn flush(&self) -> Result<(), FaucetError> {
        let mut state = self.state.lock().await;
        self.close_current(&mut state).await?;
        tracing::debug!(files = state.files_written, "Parquet sink flushed");
        Ok(())
    }
}

fn should_rollover(cfg: &ParquetSinkConfig, state: &WriterState, bytes_written: usize) -> bool {
    if let Some(max_rows) = cfg.max_rows_per_file
        && state.rows_in_current_file >= max_rows
    {
        return true;
    }
    if let Some(max_bytes) = cfg.max_bytes_per_file
        && bytes_written >= max_bytes
    {
        return true;
    }
    false
}

fn warn_on_unknown_fields(schema: &SchemaRef, records: &[Value]) {
    if records.is_empty() {
        return;
    }
    let known: std::collections::HashSet<&str> =
        schema.fields().iter().map(|f| f.name().as_str()).collect();
    let mut already_warned: std::collections::HashSet<String> = std::collections::HashSet::new();
    for r in records {
        if let Value::Object(map) = r {
            for k in map.keys() {
                if !known.contains(k.as_str()) && already_warned.insert(k.clone()) {
                    tracing::warn!(field = %k, "dropping unknown field from parquet output");
                }
            }
        }
    }
}

/// Map a decoder error to `FaucetError::Sink`. When the message looks like
/// arrow-json's type-conflict report, we re-shape it to name both sides
/// of the drift so the user can diagnose it without reading arrow internals.
fn classify_decoder_error(
    schema: &SchemaRef,
    records: &[Value],
    err: arrow::error::ArrowError,
) -> FaucetError {
    let msg = err.to_string();
    if (msg.contains("whilst decoding field") || msg.contains("type"))
        && let Some(field) = guess_drifting_field(schema, records)
    {
        let schema_type = schema
            .field_with_name(&field)
            .map(|f| f.data_type().to_string())
            .unwrap_or_else(|_| "<unknown>".to_string());
        let record_type = sample_field_type(records, &field).unwrap_or("<absent>".to_string());
        return FaucetError::Sink(format!(
            "parquet type drift for field '{field}': schema declares {schema_type}, batch contains {record_type} (raw: {msg})"
        ));
    }
    FaucetError::Sink(format!("parquet encode failed: {msg}"))
}

fn guess_drifting_field(schema: &SchemaRef, records: &[Value]) -> Option<String> {
    for r in records {
        if let Value::Object(map) = r {
            for (k, v) in map {
                let Ok(field) = schema.field_with_name(k) else {
                    continue;
                };
                if !matches_data_type(field.data_type(), v) && !matches!(v, Value::Null) {
                    return Some(k.clone());
                }
            }
        }
    }
    None
}

fn matches_data_type(dt: &arrow::datatypes::DataType, value: &Value) -> bool {
    use arrow::datatypes::DataType as DT;
    match (dt, value) {
        (_, Value::Null) => true,
        (DT::Boolean, Value::Bool(_)) => true,
        (DT::Int64 | DT::Int32 | DT::Int16 | DT::Int8, Value::Number(n)) => n.is_i64(),
        (DT::UInt64 | DT::UInt32 | DT::UInt16 | DT::UInt8, Value::Number(n)) => n.is_u64(),
        (DT::Float64 | DT::Float32, Value::Number(_)) => true,
        (DT::Utf8 | DT::LargeUtf8, Value::String(_)) => true,
        (DT::List(_) | DT::LargeList(_), Value::Array(_)) => true,
        (DT::Struct(_), Value::Object(_)) => true,
        _ => false,
    }
}

fn sample_field_type(records: &[Value], field: &str) -> Option<String> {
    for r in records {
        if let Value::Object(map) = r
            && let Some(v) = map.get(field)
            && !v.is_null()
        {
            return Some(json_value_type_name(v).to_string());
        }
    }
    None
}

fn json_value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() => "integer",
        Value::Number(n) if n.is_u64() => "unsigned integer",
        Value::Number(_) => "float",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

async fn build_store(
    destination: &ParquetDestination,
) -> Result<(Arc<dyn ObjectStore>, Option<PathBuf>), FaucetError> {
    match destination {
        ParquetDestination::LocalPath { path } => {
            let target = FsPath::new(path);
            let parent = if target.extension().and_then(|e| e.to_str()) == Some("parquet") {
                target.parent().map(|p| p.to_path_buf())
            } else {
                Some(target.to_path_buf())
            }
            .unwrap_or_else(|| PathBuf::from("."));

            tokio::fs::create_dir_all(&parent).await.map_err(|e| {
                FaucetError::Sink(format!(
                    "could not create directory {}: {e}",
                    parent.display()
                ))
            })?;

            let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
            Ok((store, Some(parent)))
        }
        ParquetDestination::S3(s3) => {
            let mut builder = AmazonS3Builder::from_env().with_bucket_name(&s3.bucket);
            if let Some(region) = &s3.region {
                builder = builder.with_region(region);
            }
            if let Some(endpoint) = &s3.endpoint_url {
                builder = builder.with_endpoint(endpoint);
            }
            if s3.allow_http {
                builder = builder.with_allow_http(true);
            }
            let store = builder
                .build()
                .map_err(|e| FaucetError::Config(format!("could not build S3 client: {e}")))?;
            Ok((Arc::new(store), None))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ParquetCompression;
    use faucet_core::Sink;
    use serde_json::json;

    fn cfg(path: &std::path::Path) -> ParquetSinkConfig {
        ParquetSinkConfig::local(path.to_string_lossy().to_string())
    }

    #[tokio::test]
    async fn new_validates_config() {
        let cfg = ParquetSinkConfig::local("");
        match ParquetSink::new(cfg).await {
            Ok(_) => panic!("expected Config error"),
            Err(err) => assert!(matches!(err, FaucetError::Config(_))),
        }
    }

    #[tokio::test]
    async fn empty_batch_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
        let count = sink.write_batch(&[]).await.unwrap();
        assert_eq!(count, 0);
        sink.flush().await.unwrap();
    }

    #[tokio::test]
    async fn flush_without_write_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
        assert!(sink.flush().await.is_ok());
    }

    #[tokio::test]
    async fn type_drift_returns_sink_error() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
        sink.write_batch(&[json!({"x": 1})]).await.unwrap();
        let err = sink
            .write_batch(&[json!({"x": "not an int"})])
            .await
            .unwrap_err();
        match err {
            FaucetError::Sink(msg) => {
                assert!(
                    msg.contains("'x'") || msg.contains("x"),
                    "error must name the drifting field: {msg}"
                );
            }
            other => panic!("expected Sink error, got {other:?}"),
        }
        sink.flush().await.unwrap();
    }

    #[tokio::test]
    async fn unknown_fields_are_silently_dropped() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
        sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        let count = sink
            .write_batch(&[json!({"id": 2, "ghost": "value"})])
            .await
            .unwrap();
        assert_eq!(count, 1);
        sink.flush().await.unwrap();
    }

    #[tokio::test]
    async fn writer_properties_apply_compression() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = cfg(tmp.path()).compression(ParquetCompression::Zstd);
        let sink = ParquetSink::new(cfg).await.unwrap();
        let props = sink.writer_properties();
        assert!(matches!(
            props.compression(&parquet::schema::types::ColumnPath::new(vec![
                "any".to_string(),
            ])),
            parquet::basic::Compression::ZSTD(_)
        ));
    }

    #[test]
    fn should_rollover_rows_threshold() {
        let cfg = ParquetSinkConfig::local("/tmp/x").max_rows_per_file(10);
        let mut state = WriterState::new();
        state.rows_in_current_file = 9;
        assert!(!should_rollover(&cfg, &state, 0));
        state.rows_in_current_file = 10;
        assert!(should_rollover(&cfg, &state, 0));
        state.rows_in_current_file = 11;
        assert!(should_rollover(&cfg, &state, 0));
    }

    #[test]
    fn should_rollover_bytes_threshold() {
        let cfg = ParquetSinkConfig::local("/tmp/x").max_bytes_per_file(1024);
        let state = WriterState::new();
        assert!(!should_rollover(&cfg, &state, 1023));
        assert!(should_rollover(&cfg, &state, 1024));
        assert!(should_rollover(&cfg, &state, 4096));
    }

    #[test]
    fn should_rollover_no_thresholds_means_never() {
        let cfg = ParquetSinkConfig::local("/tmp/x");
        let mut state = WriterState::new();
        state.rows_in_current_file = 1_000_000;
        assert!(!should_rollover(&cfg, &state, usize::MAX / 2));
    }

    #[test]
    fn matches_data_type_for_primitives() {
        use arrow::datatypes::DataType as DT;
        assert!(matches_data_type(&DT::Boolean, &json!(true)));
        assert!(matches_data_type(&DT::Int64, &json!(1)));
        assert!(!matches_data_type(&DT::Int64, &json!(1.5)));
        assert!(matches_data_type(&DT::Float64, &json!(1.5)));
        assert!(matches_data_type(&DT::Float64, &json!(1)));
        assert!(matches_data_type(&DT::Utf8, &json!("hi")));
        assert!(!matches_data_type(&DT::Utf8, &json!(1)));
        assert!(matches_data_type(&DT::Boolean, &Value::Null));
    }

    #[test]
    fn json_value_type_name_covers_variants() {
        assert_eq!(json_value_type_name(&json!(null)), "null");
        assert_eq!(json_value_type_name(&json!(true)), "boolean");
        assert_eq!(json_value_type_name(&json!(1)), "integer");
        assert_eq!(json_value_type_name(&json!(1.5)), "float");
        assert_eq!(json_value_type_name(&json!("s")), "string");
        assert_eq!(json_value_type_name(&json!([1])), "array");
        assert_eq!(json_value_type_name(&json!({"a": 1})), "object");
    }
}
