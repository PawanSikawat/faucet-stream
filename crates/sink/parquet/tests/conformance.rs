//! `faucet-conformance` battery against the real Parquet sink.
//!
//! Parquet is an append-only file sink — it advertises no idempotency
//! mechanism, so the battery exercises the **honest branch**:
//! - check 1 `assert_config_schema_valid_value` (value form, for sinks);
//! - check 5 `assert_capabilities_truthful` — Append works, and the sink does
//!   *not* claim idempotent/keyed dedup (so the pipeline correctly refuses
//!   `delivery: exactly_once` for it).
use arrow::record_batch::RecordBatch;
use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink;
use faucet_sink_parquet::{ParquetSink, ParquetSinkConfig};
use futures::TryStreamExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

/// Count durable rows across every `.parquet` file the sink has written into
/// `dir`. Reads the files back via the raw `parquet` + `arrow` async reader
/// (the same reader `roundtrip.rs` uses) so the count reflects real durable
/// writes. A directory with no parquet files (before the first flush) is zero.
async fn count_rows(dir: &std::path::Path) -> usize {
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("parquet"))
                .collect()
        })
        .unwrap_or_default();
    files.sort();

    let mut total = 0usize;
    for f in files {
        let file = tokio::fs::File::open(&f).await.unwrap();
        let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
        let stream = builder.build().unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        total += batches.iter().map(|b| b.num_rows()).sum::<usize>();
    }
    total
}

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(
        faucet_sink_parquet::ParquetSinkConfig
    ))
    .unwrap();
    assert_config_schema_valid_value(&schema, "parquet");
}

#[tokio::test]
async fn conformance_capabilities_truthful() {
    let dir = tempfile::tempdir().unwrap();
    let sink = ParquetSink::new(ParquetSinkConfig::local(
        dir.path().to_string_lossy().to_string(),
    ))
    .await
    .unwrap();
    let sink_ref = &sink;
    let dir_path = dir.path().to_path_buf();

    faucet_conformance::assert_capabilities_truthful(&sink, || {
        let dir_path = dir_path.clone();
        async move {
            // Parquet only becomes readable once the footer is written on
            // flush(); flush so the durable row count reflects writes.
            sink_ref.flush().await.expect("flush");
            count_rows(&dir_path).await
        }
    })
    .await;

    // The honest branch must have left the append-only sink non-idempotent.
    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
}
