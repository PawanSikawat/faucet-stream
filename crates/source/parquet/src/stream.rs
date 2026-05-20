//! Parquet source stream executor.
//!
//! Reads one or more Parquet files (local file, local glob, or S3 object /
//! prefix) and yields each row as a `serde_json::Value::Object`. RecordBatches
//! are streamed and converted incrementally — no whole-file buffering.

use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::{StreamExt, TryStreamExt, stream};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use serde_json::Value;

use crate::config::{ParquetLocation, ParquetS3Config, ParquetSourceConfig};
use crate::convert::record_batch_to_json;

/// A source that reads Parquet files into JSON records.
pub struct ParquetSource {
    config: ParquetSourceConfig,
    /// Eagerly-constructed object store used for S3 sources. `None` for
    /// local file / glob sources.
    s3_store: Option<Arc<dyn ObjectStore>>,
}

impl ParquetSource {
    /// Build a new Parquet source from `config`.
    ///
    /// Performs eager validation (concurrency > 0, mutually exclusive S3
    /// `key`/`prefix`) and pre-builds the S3 client when applicable so it can
    /// be reused across concurrent file reads.
    pub async fn new(config: ParquetSourceConfig) -> Result<Self, FaucetError> {
        // `batch_size == 0` is the "no batching" sentinel — accepted, and
        // means "let the file's native row-group size drive page cadence".
        // See `ParquetSourceConfig::batch_size` for the full contract.
        if config.concurrency == 0 {
            return Err(FaucetError::Config(
                "parquet source: concurrency must be > 0".into(),
            ));
        }

        let s3_store = match &config.source {
            ParquetLocation::S3(s3) => Some(build_s3_store(s3)?),
            _ => None,
        };

        Ok(Self { config, s3_store })
    }

    /// Resolve the configured `source` into the concrete list of files to read.
    ///
    /// For S3 prefix mode this issues a list-objects call. For glob mode this
    /// expands the pattern. The result is sorted for deterministic ordering.
    async fn resolve_files(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<FileTarget>, FaucetError> {
        match &self.config.source {
            ParquetLocation::LocalPath { path } => {
                let resolved = substitute(path, context);
                Ok(vec![FileTarget::Local(PathBuf::from(resolved))])
            }
            ParquetLocation::Glob { pattern } => {
                let resolved = substitute(pattern, context);
                expand_glob(&resolved)
            }
            ParquetLocation::S3(s3) => self.resolve_s3_files(s3, context).await,
        }
    }

    async fn resolve_s3_files(
        &self,
        s3: &ParquetS3Config,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<FileTarget>, FaucetError> {
        match (&s3.key, &s3.prefix) {
            (Some(_), Some(_)) => Err(FaucetError::Config(
                "parquet source: S3 config cannot set both `key` and `prefix`".into(),
            )),
            (None, None) => Err(FaucetError::Config(
                "parquet source: S3 config requires one of `key` or `prefix`".into(),
            )),
            (Some(key), None) => {
                let key = substitute(key, context);
                Ok(vec![FileTarget::S3(ObjectPath::from(key))])
            }
            (None, Some(prefix)) => {
                let prefix = substitute(prefix, context);
                let store = self.s3_store.as_ref().ok_or_else(|| {
                    FaucetError::Source("parquet source: S3 store not initialised".into())
                })?;
                list_s3_prefix(store.as_ref(), &prefix).await
            }
        }
    }

    /// Read a single resolved file, returning the rows it yields plus the
    /// Arrow schema used to decode it (so the caller can detect divergence
    /// across multiple files).
    async fn read_file(&self, target: &FileTarget) -> Result<FileOutput, FaucetError> {
        let display = target.display();
        match target {
            FileTarget::Local(path) => {
                let file = tokio::fs::File::open(path).await.map_err(|e| {
                    FaucetError::Source(format!("failed to open parquet file '{display}': {e}"))
                })?;
                self.decode(file, &display).await
            }
            FileTarget::S3(path) => {
                let store = self.s3_store.as_ref().ok_or_else(|| {
                    FaucetError::Source("parquet source: S3 store not initialised".into())
                })?;
                let reader = ParquetObjectReader::new(store.clone(), path.clone());
                self.decode(reader, &display).await
            }
        }
    }

    async fn decode<R>(&self, reader: R, display: &str) -> Result<FileOutput, FaucetError>
    where
        R: parquet::arrow::async_reader::AsyncFileReader + Send + Unpin + 'static,
    {
        let (mut batches, arrow_schema) = self.build_batch_stream(reader, display).await?;

        let mut rows: Vec<Value> = Vec::new();
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(|e| {
                FaucetError::Source(format!("parquet decode error in '{display}': {e}"))
            })?;
            let batch_rows = record_batch_to_json(&batch)?;
            rows.extend(batch_rows);
        }

        Ok(FileOutput {
            path: display.to_string(),
            rows,
            arrow_schema,
        })
    }

    /// Build a per-file Arrow `RecordBatch` stream from a low-level
    /// `AsyncFileReader`. Applies the configured projection and `batch_size`
    /// hint (skipped when `batch_size == 0`, so the file's native row-group
    /// size governs page cadence).
    ///
    /// Used by both [`decode`](Self::decode) (which materialises all rows
    /// into a `FileOutput`) and [`stream_pages`](
    /// faucet_core::Source::stream_pages) (which yields one `StreamPage`
    /// per `RecordBatch`).
    async fn build_batch_stream<R>(
        &self,
        reader: R,
        display: &str,
    ) -> Result<(BatchStream, arrow::datatypes::SchemaRef), FaucetError>
    where
        R: parquet::arrow::async_reader::AsyncFileReader + Send + Unpin + 'static,
    {
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| {
                FaucetError::Source(format!(
                    "failed to read parquet metadata for '{display}': {e}"
                ))
            })?;

        // `batch_size == 0` is the sentinel meaning "use the file's native
        // row-group size as the batch cadence" — i.e. don't override the
        // Arrow reader's default, which already yields one batch per
        // row-group.
        if self.config.batch_size > 0 {
            builder = builder.with_batch_size(self.config.batch_size);
        }

        if let Some(cols) = self.config.columns.as_deref() {
            let parquet_schema = builder.parquet_schema();
            validate_projection(cols, parquet_schema, display)?;
            let mask = ProjectionMask::columns(parquet_schema, cols.iter().map(String::as_str));
            builder = builder.with_projection(mask);
        }

        let arrow_schema = builder.schema().clone();

        let stream = builder.build().map_err(|e| {
            FaucetError::Source(format!(
                "failed to build parquet stream for '{display}': {e}"
            ))
        })?;

        Ok((Box::pin(stream), arrow_schema))
    }

    /// Open a per-file Arrow `RecordBatch` stream for a resolved target
    /// (local or S3), returning the boxed stream, the Arrow schema, and a
    /// display string for error messages.
    async fn open_target_stream(
        &self,
        target: &FileTarget,
    ) -> Result<(BatchStream, arrow::datatypes::SchemaRef, String), FaucetError> {
        let display = target.display();
        match target {
            FileTarget::Local(path) => {
                let file = tokio::fs::File::open(path).await.map_err(|e| {
                    FaucetError::Source(format!("failed to open parquet file '{display}': {e}"))
                })?;
                let (stream, schema) = self.build_batch_stream(file, &display).await?;
                Ok((stream, schema, display))
            }
            FileTarget::S3(path) => {
                let store = self.s3_store.as_ref().ok_or_else(|| {
                    FaucetError::Source("parquet source: S3 store not initialised".into())
                })?;
                let reader = ParquetObjectReader::new(store.clone(), path.clone());
                let (stream, schema) = self.build_batch_stream(reader, &display).await?;
                Ok((stream, schema, display))
            }
        }
    }
}

/// Boxed Arrow `RecordBatch` stream returned by
/// [`ParquetSource::build_batch_stream`].
type BatchStream =
    Pin<Box<dyn futures::Stream<Item = parquet::errors::Result<arrow::array::RecordBatch>> + Send>>;

#[async_trait]
impl faucet_core::Source for ParquetSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let targets = self.resolve_files(context).await?;

        tracing::info!(files = targets.len(), "Parquet source resolved files");

        if targets.is_empty() {
            return Ok(Vec::new());
        }

        let concurrency = self.config.concurrency.max(1);

        let outputs: Vec<FileOutput> = stream::iter(targets)
            .map(|target| async move {
                let out = self.read_file(&target).await?;
                tracing::debug!(file = %out.path, rows = out.rows.len(), "Parquet file decoded");
                Ok::<FileOutput, FaucetError>(out)
            })
            .buffer_unordered(concurrency)
            .try_collect()
            .await?;

        if outputs.len() > 1 {
            let first = &outputs[0];
            for other in &outputs[1..] {
                if first.arrow_schema != other.arrow_schema {
                    return Err(FaucetError::Source(schema_mismatch_message(first, other)));
                }
            }
        }

        let total: usize = outputs.iter().map(|o| o.rows.len()).sum();
        let mut all = Vec::with_capacity(total);
        for out in outputs {
            all.extend(out.rows);
        }

        tracing::info!(total_records = all.len(), "Parquet source fetch complete");
        Ok(all)
    }

    /// Stream RecordBatches from each resolved file, yielding one
    /// [`StreamPage`] per Arrow `RecordBatch` so client-side memory is
    /// bounded at `O(batch_size * row_width)` regardless of total file size.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of
    /// [`ParquetSourceConfig::batch_size`] — the config is the user-facing
    /// knob the README documents, and routing the pipeline-supplied hint
    /// through it would silently override an explicit config value.
    ///
    /// **Cadence:**
    /// - `batch_size > 0` — passed to
    ///   [`ParquetRecordBatchStreamBuilder::with_batch_size`]. Arrow may
    ///   emit a *smaller* batch at row-group boundaries, so an emitted page
    ///   can be smaller than `batch_size`.
    /// - `batch_size == 0` — the sentinel skips `with_batch_size`, so the
    ///   file's native row-group size drives the page cadence (one page per
    ///   row-group).
    ///
    /// **Multi-file scans** (glob / S3 prefix) iterate sequentially in
    /// sorted order. The first file's Arrow schema is the reference; any
    /// subsequent file with a different schema surfaces as
    /// [`FaucetError::Source`] naming both paths and the first diverging
    /// field — matching the eager `fetch_with_context` behaviour.
    ///
    /// Every page carries `bookmark: None` — the Parquet source has no
    /// incremental-replication mode.
    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            let targets = self.resolve_files(context).await?;
            tracing::info!(files = targets.len(), "Parquet source resolved files");

            if targets.is_empty() {
                return;
            }

            let mut total_records = 0usize;
            let mut total_pages = 0usize;
            // Reference schema captured from the first opened file. Used to
            // detect cross-file divergence in glob / S3-prefix scans —
            // preserves the eager `fetch_with_context` failure mode.
            let mut reference: Option<(String, arrow::datatypes::SchemaRef)> = None;

            for target in &targets {
                let (mut batches, arrow_schema, display) =
                    self.open_target_stream(target).await?;

                if let Some((ref first_path, ref first_schema)) = reference {
                    if first_schema != &arrow_schema {
                        Err(FaucetError::Source(schema_mismatch_message_pair(
                            first_path,
                            first_schema,
                            &display,
                            &arrow_schema,
                        )))?;
                    }
                } else {
                    reference = Some((display.clone(), arrow_schema));
                }

                while let Some(batch) = batches.next().await {
                    let batch = batch.map_err(|e| {
                        FaucetError::Source(format!(
                            "parquet decode error in '{display}': {e}"
                        ))
                    })?;
                    let rows = record_batch_to_json(&batch)?;
                    if rows.is_empty() {
                        continue;
                    }
                    total_records += rows.len();
                    total_pages += 1;
                    yield StreamPage { records: rows, bookmark: None };
                }
            }

            tracing::info!(
                pages = total_pages,
                total_records,
                batch_size = self.config.batch_size,
                "Parquet source stream complete",
            );
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(ParquetSourceConfig))
            .expect("schema serialization")
    }
}

/// Per-file decode output, kept around long enough to validate cross-file
/// schema consistency.
struct FileOutput {
    path: String,
    rows: Vec<Value>,
    arrow_schema: arrow::datatypes::SchemaRef,
}

/// Resolved file location ready for reading.
#[derive(Debug, Clone)]
enum FileTarget {
    Local(PathBuf),
    S3(ObjectPath),
}

impl FileTarget {
    fn display(&self) -> String {
        match self {
            FileTarget::Local(p) => p.display().to_string(),
            FileTarget::S3(p) => format!("s3://{p}"),
        }
    }
}

/// Apply context substitution only when there is something to substitute.
fn substitute(template: &str, context: &HashMap<String, Value>) -> String {
    if context.is_empty() {
        template.to_string()
    } else {
        faucet_core::util::substitute_context(template, context)
    }
}

/// Expand a glob pattern into a sorted list of local file paths.
fn expand_glob(pattern: &str) -> Result<Vec<FileTarget>, FaucetError> {
    let entries = glob::glob(pattern)
        .map_err(|e| FaucetError::Config(format!("invalid glob '{pattern}': {e}")))?;

    let mut paths = Vec::new();
    for entry in entries {
        let p = entry
            .map_err(|e| FaucetError::Source(format!("glob entry error for '{pattern}': {e}")))?;
        if p.is_file() {
            paths.push(p);
        }
    }
    paths.sort();
    Ok(paths.into_iter().map(FileTarget::Local).collect())
}

/// List S3 objects under `prefix` and turn them into `FileTarget::S3` entries.
async fn list_s3_prefix(
    store: &dyn ObjectStore,
    prefix: &str,
) -> Result<Vec<FileTarget>, FaucetError> {
    let prefix_path = if prefix.is_empty() {
        None
    } else {
        Some(ObjectPath::from(prefix))
    };

    let mut listing = store.list(prefix_path.as_ref());
    let mut keys = Vec::new();
    while let Some(item) = listing.next().await {
        let meta = item.map_err(|e| {
            FaucetError::Source(format!("S3 list error for prefix '{prefix}': {e}"))
        })?;
        keys.push(meta.location);
    }
    keys.sort();
    Ok(keys.into_iter().map(FileTarget::S3).collect())
}

/// Build an `AmazonS3` `object_store` client from a `ParquetS3Config`.
fn build_s3_store(s3: &ParquetS3Config) -> Result<Arc<dyn ObjectStore>, FaucetError> {
    if s3.bucket.trim().is_empty() {
        return Err(FaucetError::Config(
            "parquet source: S3 bucket must not be empty".into(),
        ));
    }

    let mut builder = AmazonS3Builder::from_env().with_bucket_name(&s3.bucket);
    if let Some(region) = &s3.region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = &s3.endpoint_url {
        builder = builder.with_endpoint(endpoint);
        if endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
    }

    let store = builder
        .build()
        .map_err(|e| FaucetError::Config(format!("failed to build S3 client: {e}")))?;
    Ok(Arc::new(store))
}

/// Verify every requested column exists in the file's Parquet schema. The
/// `ProjectionMask::columns` API silently ignores unknown names, so we
/// pre-validate here to surface a clear error to the caller.
fn validate_projection(
    requested: &[String],
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    display: &str,
) -> Result<(), FaucetError> {
    let root = parquet_schema.root_schema();
    let parquet::schema::types::Type::GroupType { fields, .. } = root else {
        return Err(FaucetError::Source(format!(
            "parquet root schema for '{display}' is not a group"
        )));
    };

    let known: std::collections::HashSet<&str> = fields.iter().map(|f| f.name()).collect();

    for name in requested {
        if !known.contains(name.as_str()) {
            return Err(FaucetError::Source(format!(
                "parquet source: projected column '{name}' not found in file '{display}' \
                 (available: {})",
                known.iter().copied().collect::<Vec<_>>().join(", ")
            )));
        }
    }

    Ok(())
}

/// Compose a descriptive cross-file schema mismatch error.
fn schema_mismatch_message(first: &FileOutput, other: &FileOutput) -> String {
    schema_mismatch_message_pair(
        &first.path,
        &first.arrow_schema,
        &other.path,
        &other.arrow_schema,
    )
}

/// Same as [`schema_mismatch_message`] but works on raw `(path, schema)`
/// pairs so it can be called from the streaming path where no `FileOutput`
/// exists.
fn schema_mismatch_message_pair(
    first_path: &str,
    first_schema: &arrow::datatypes::SchemaRef,
    other_path: &str,
    other_schema: &arrow::datatypes::SchemaRef,
) -> String {
    let first_fields: Vec<String> = first_schema
        .fields()
        .iter()
        .map(|f| format!("{}:{}", f.name(), f.data_type()))
        .collect();
    let other_fields: Vec<String> = other_schema
        .fields()
        .iter()
        .map(|f| format!("{}:{}", f.name(), f.data_type()))
        .collect();

    // Identify the first diverging field for a focused hint.
    let max_len = first_fields.len().max(other_fields.len());
    let mut first_diff = None;
    for i in 0..max_len {
        let a = first_fields
            .get(i)
            .map(String::as_str)
            .unwrap_or("<missing>");
        let b = other_fields
            .get(i)
            .map(String::as_str)
            .unwrap_or("<missing>");
        if a != b {
            first_diff = Some((i, a.to_string(), b.to_string()));
            break;
        }
    }

    let detail = match first_diff {
        Some((i, a, b)) => format!(" (field #{i}: '{a}' vs '{b}')"),
        None => String::new(),
    };

    format!("parquet source: schema mismatch between '{first_path}' and '{other_path}'{detail}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ParquetSourceConfig;

    #[test]
    fn substitute_passes_through_when_context_empty() {
        let ctx = HashMap::new();
        assert_eq!(substitute("/tmp/{x}.parquet", &ctx), "/tmp/{x}.parquet");
    }

    #[test]
    fn substitute_replaces_placeholders() {
        let mut ctx = HashMap::new();
        ctx.insert("region".to_string(), Value::String("us".into()));
        assert_eq!(
            substitute("data/{region}/x.parquet", &ctx),
            "data/us/x.parquet"
        );
    }

    #[tokio::test]
    async fn accepts_zero_batch_size_as_sentinel() {
        // `batch_size = 0` is the "no batching" sentinel — page cadence
        // falls back to the file's native row-group size. The source
        // constructor must accept it.
        let cfg = ParquetSourceConfig::local("/tmp/x.parquet").batch_size(0);
        let source = ParquetSource::new(cfg)
            .await
            .expect("batch_size=0 must be accepted as the no-batching sentinel");
        assert_eq!(source.config.batch_size, 0);
    }

    #[tokio::test]
    async fn rejects_zero_concurrency() {
        let cfg = ParquetSourceConfig::local("/tmp/x.parquet").concurrency(0);
        match ParquetSource::new(cfg).await {
            Err(FaucetError::Config(msg)) => assert!(msg.contains("concurrency")),
            other => panic!("expected Config error, got {:?}", other.err()),
        }
    }

    #[tokio::test]
    async fn rejects_s3_with_both_key_and_prefix() {
        let mut s3 = ParquetS3Config::object("b", "k.parquet");
        s3.prefix = Some("p/".into());
        let cfg = ParquetSourceConfig::s3(s3);
        let source = ParquetSource::new(cfg).await.unwrap();
        let err = source.resolve_files(&HashMap::new()).await.unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[tokio::test]
    async fn rejects_s3_with_neither_key_nor_prefix() {
        let s3 = ParquetS3Config {
            bucket: "b".into(),
            key: None,
            prefix: None,
            region: None,
            endpoint_url: None,
        };
        let cfg = ParquetSourceConfig::s3(s3);
        let source = ParquetSource::new(cfg).await.unwrap();
        let err = source.resolve_files(&HashMap::new()).await.unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn empty_bucket_rejected() {
        let s3 = ParquetS3Config::object("", "k.parquet");
        let err = build_s3_store(&s3).unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }
}
