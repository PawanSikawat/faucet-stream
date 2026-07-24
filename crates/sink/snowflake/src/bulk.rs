//! Arrow columnar bulk-load helpers (#381) — Parquet encode, external-stage
//! upload, and the `COPY INTO` statement builder.
//!
//! The Snowflake SQL REST API cannot run the `PUT` driver command, so internal
//! named stages are out of reach; instead we upload Parquet to the **external**
//! stage's backing cloud storage (via `object_store`) and then `COPY INTO … FROM
//! @stage/<file> FILE_FORMAT=(TYPE=PARQUET)`. Everything here is either pure
//! (the SQL builder, unit-tested) or thin I/O over shared crates.

use crate::config::SnowflakeStageConfig;
use arrow::array::RecordBatch;
use faucet_core::FaucetError;
use faucet_core::util::quote_ident;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use url::Url;

/// A resolved upload target: the object store for the stage's `url` plus the
/// base path within it. Built once (lazily) and reused across writes.
pub struct BulkStore {
    pub store: Arc<dyn ObjectStore>,
    pub base: ObjPath,
}

/// Resolve the `object_store` client + base path for a stage's backing `url`,
/// applying the user's `storage_options` verbatim.
pub fn resolve_store(cfg: &SnowflakeStageConfig) -> Result<BulkStore, FaucetError> {
    let url = Url::parse(&cfg.url).map_err(|e| {
        FaucetError::Config(format!(
            "snowflake bulk_load.url '{}' is not a valid URL: {e}",
            cfg.url
        ))
    })?;
    let (store, base) = object_store::parse_url_opts(&url, &cfg.storage_options).map_err(|e| {
        FaucetError::Sink(format!(
            "snowflake bulk_load: cannot open object store for '{}': {e}",
            cfg.url
        ))
    })?;
    Ok(BulkStore {
        store: Arc::from(store),
        base,
    })
}

/// Upload one Parquet file (`bytes`) into the stage's backing store under
/// `file`, returning the relative name used in `COPY INTO @stage/<file>`.
pub async fn upload(bs: &BulkStore, file: &str, bytes: Vec<u8>) -> Result<(), FaucetError> {
    let mut full = bs.base.to_string();
    if !full.is_empty() {
        full.push('/');
    }
    full.push_str(file);
    let path = ObjPath::from(full);
    bs.store
        .put(&path, object_store::PutPayload::from(bytes))
        .await
        .map_err(|e| FaucetError::Sink(format!("snowflake stage upload failed for {file}: {e}")))?;
    Ok(())
}

/// Encode one Arrow `RecordBatch` as a self-contained ZSTD-compressed Parquet
/// file in memory. Mirrors the S3/GCS sinks' `encode_parquet`.
pub fn encode_parquet(batch: &RecordBatch) -> Result<Vec<u8>, FaucetError> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .map_err(|e| FaucetError::Sink(format!("parquet writer init failed: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| FaucetError::Sink(format!("parquet write failed: {e}")))?;
        writer
            .close()
            .map_err(|e| FaucetError::Sink(format!("parquet finalize failed: {e}")))?;
    }
    Ok(buf)
}

/// Build the `COPY INTO` statement that loads a single staged Parquet `file`
/// (relative to `@stage`) into the fully-qualified target table.
///
/// Pure — the table name is escaped via [`quote_ident`]; `stage` and `file`
/// are the operator-provided stage name and a sink-generated UUID filename.
pub fn build_copy_into(
    database: &str,
    schema: &str,
    table: &str,
    stage: &SnowflakeStageConfig,
    file: &str,
) -> String {
    let target = format!(
        "{}.{}.{}",
        quote_ident(database),
        quote_ident(schema),
        quote_ident(table)
    );
    // `@stage/file` — strip any leading `@` the operator may have included so
    // we don't emit `@@stage`.
    let stage_name = stage.stage.trim_start_matches('@');
    let mut sql = format!(
        "COPY INTO {target} FROM @{stage_name}/{file} \
         FILE_FORMAT = (TYPE = PARQUET) \
         MATCH_BY_COLUMN_NAME = {}",
        stage.match_by_column_name
    );
    if stage.purge {
        sql.push_str(" PURGE = TRUE");
    }
    sql
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn stage() -> SnowflakeStageConfig {
        SnowflakeStageConfig {
            stage: "MY_STAGE".into(),
            url: "s3://bucket/prefix/".into(),
            storage_options: HashMap::new(),
            match_by_column_name: "CASE_INSENSITIVE".into(),
            purge: false,
        }
    }

    #[test]
    fn copy_into_quotes_target_and_references_stage_file() {
        let sql = build_copy_into("db", "sc", "events", &stage(), "faucet-abc.parquet");
        assert!(sql.contains(r#""db"."sc"."events""#), "{sql}");
        assert!(sql.contains("@MY_STAGE/faucet-abc.parquet"), "{sql}");
        assert!(sql.contains("TYPE = PARQUET"), "{sql}");
        assert!(
            sql.contains("MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"),
            "{sql}"
        );
        assert!(!sql.contains("PURGE"), "{sql}");
    }

    #[test]
    fn copy_into_adds_purge_when_requested() {
        let mut s = stage();
        s.purge = true;
        let sql = build_copy_into("db", "sc", "t", &s, "f.parquet");
        assert!(sql.contains("PURGE = TRUE"), "{sql}");
    }

    #[test]
    fn copy_into_strips_leading_at_from_stage() {
        let mut s = stage();
        s.stage = "@FQ.SC.STG".into();
        let sql = build_copy_into("db", "sc", "t", &s, "f.parquet");
        assert!(sql.contains("@FQ.SC.STG/f.parquet"), "{sql}");
        assert!(!sql.contains("@@"), "{sql}");
    }

    #[test]
    fn copy_into_escapes_injection_in_table_name() {
        let sql = build_copy_into("db", "sc", r#"ev"il"#, &stage(), "f.parquet");
        // The embedded quote is doubled by quote_ident, not left to break out.
        assert!(sql.contains(r#""ev""il""#), "{sql}");
    }

    #[test]
    fn encode_parquet_roundtrips_a_batch() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        let bytes = encode_parquet(&batch).unwrap();
        // Parquet magic header/footer.
        assert_eq!(&bytes[..4], b"PAR1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
    }
}
