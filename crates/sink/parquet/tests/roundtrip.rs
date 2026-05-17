//! End-to-end round-trip tests for the Parquet sink.
//!
//! Each test writes JSON records to a temp directory (or in-memory object
//! store), then re-reads the resulting Parquet file via the raw `parquet` +
//! `arrow` APIs and asserts on the decoded `RecordBatch`. We deliberately do
//! NOT depend on `faucet-source-parquet` here; that crate is being built in
//! parallel and round-trips against it are out of scope until both ship.

use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use faucet_core::Sink;
use faucet_sink_parquet::{
    ParquetCompression, ParquetDestination, ParquetS3Destination, ParquetSink, ParquetSinkConfig,
};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjPath};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use serde_json::{Value, json};
use tempfile::TempDir;

fn cfg_dir(dir: &std::path::Path) -> ParquetSinkConfig {
    ParquetSinkConfig::local(dir.to_string_lossy().to_string())
}

async fn read_all_local(dir: &std::path::Path) -> Vec<RecordBatch> {
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("parquet"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no parquet files in {dir:?}");

    let mut all = Vec::new();
    for f in files {
        let file = tokio::fs::File::open(&f).await.unwrap();
        let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
        let stream = builder.build().unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert!(
            !batches.is_empty(),
            "parquet file {f:?} held no record batches"
        );
        all.extend(batches);
    }
    all
}

fn rows_in(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn primitives_round_trip() {
    let tmp = TempDir::new().unwrap();
    let sink = ParquetSink::new(cfg_dir(tmp.path())).await.unwrap();

    let records = vec![
        json!({"id": 1, "name": "alice", "active": true, "score": 1.5, "tag": null}),
        json!({"id": 2, "name": "bob", "active": false, "score": 2.25, "tag": "vip"}),
        json!({"id": 3, "name": "carol", "active": true, "score": 3.75, "tag": null}),
    ];
    let n = sink.write_batch(&records).await.unwrap();
    sink.flush().await.unwrap();
    assert_eq!(n, 3);

    let batches = read_all_local(tmp.path()).await;
    assert_eq!(rows_in(&batches), 3);

    let batch = &batches[0];
    assert_eq!(
        batch.column_by_name("id").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        batch.column_by_name("active").unwrap().data_type(),
        &DataType::Boolean
    );
    assert_eq!(
        batch.column_by_name("score").unwrap().data_type(),
        &DataType::Float64
    );
    assert_eq!(
        batch.column_by_name("name").unwrap().data_type(),
        &DataType::Utf8
    );

    let tag_col = batch.column_by_name("tag").unwrap();
    assert!(tag_col.is_null(0));
    assert!(!tag_col.is_null(1));
    assert!(tag_col.is_null(2));
}

#[tokio::test]
async fn missing_field_becomes_null() {
    let tmp = TempDir::new().unwrap();
    let sink = ParquetSink::new(cfg_dir(tmp.path())).await.unwrap();
    sink.write_batch(&[json!({"id": 1, "extra": "first"}), json!({"id": 2})])
        .await
        .unwrap();
    sink.flush().await.unwrap();

    let batches = read_all_local(tmp.path()).await;
    assert_eq!(rows_in(&batches), 2);
    let extra = batches[0].column_by_name("extra").unwrap();
    assert!(!extra.is_null(0));
    assert!(extra.is_null(1));
}

#[tokio::test]
async fn unknown_fields_dropped_after_schema_locked() {
    let tmp = TempDir::new().unwrap();
    let sink = ParquetSink::new(cfg_dir(tmp.path())).await.unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.write_batch(&[json!({"id": 2, "ghost": "value"})])
        .await
        .unwrap();
    sink.flush().await.unwrap();

    let batches = read_all_local(tmp.path()).await;
    assert_eq!(rows_in(&batches), 2);
    assert!(batches[0].schema().field_with_name("ghost").is_err());
}

#[tokio::test]
async fn nested_struct_and_list_round_trip() {
    let tmp = TempDir::new().unwrap();
    let sink = ParquetSink::new(cfg_dir(tmp.path())).await.unwrap();
    sink.write_batch(&[
        json!({"id": 1, "meta": {"city": "NYC", "zip": 10001}, "tags": ["a", "b"]}),
        json!({"id": 2, "meta": {"city": "SF", "zip": 94110}, "tags": ["c"]}),
    ])
    .await
    .unwrap();
    sink.flush().await.unwrap();

    let batches = read_all_local(tmp.path()).await;
    assert_eq!(rows_in(&batches), 2);
    let batch = &batches[0];

    let meta = batch.column_by_name("meta").unwrap();
    assert!(matches!(meta.data_type(), DataType::Struct(_)));
    let tags = batch.column_by_name("tags").unwrap();
    assert!(matches!(tags.data_type(), DataType::List(_)));
}

#[tokio::test]
async fn compression_variants_all_readable() {
    for compression in [
        ParquetCompression::Uncompressed,
        ParquetCompression::Snappy,
        ParquetCompression::Gzip,
        ParquetCompression::Zstd,
        ParquetCompression::Lz4,
    ] {
        let tmp = TempDir::new().unwrap();
        let cfg = cfg_dir(tmp.path()).compression(compression);
        let sink = ParquetSink::new(cfg).await.unwrap();
        sink.write_batch(&[json!({"x": 1, "y": "z"})])
            .await
            .unwrap();
        sink.flush().await.unwrap();
        let batches = read_all_local(tmp.path()).await;
        assert_eq!(rows_in(&batches), 1, "compression {compression:?} failed");
    }
}

#[tokio::test]
async fn rollover_by_row_count_creates_multiple_files() {
    let tmp = TempDir::new().unwrap();
    let cfg = cfg_dir(tmp.path()).max_rows_per_file(300);
    let sink = ParquetSink::new(cfg).await.unwrap();

    let total = 1000;
    let mut written = 0;
    while written < total {
        let batch: Vec<Value> = (written..(written + 100).min(total))
            .map(|i| json!({"i": i as i64}))
            .collect();
        let n = sink.write_batch(&batch).await.unwrap();
        written += n;
    }
    sink.flush().await.unwrap();

    let mut entries: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("parquet"))
        .collect();
    entries.sort();
    assert_eq!(
        entries.len(),
        4,
        "1000 rows with max_rows=300 should produce 4 files, got {entries:?}"
    );

    let batches = read_all_local(tmp.path()).await;
    assert_eq!(rows_in(&batches), 1000);
}

#[tokio::test]
async fn rollover_by_bytes_creates_multiple_files() {
    // Build records whose strings are unique so the parquet writer can't
    // compress or dictionary-encode the column down to a tiny footprint —
    // otherwise the byte threshold may never trigger.
    let tmp = TempDir::new().unwrap();
    let cfg = cfg_dir(tmp.path())
        .max_bytes_per_file(4_096)
        .row_group_size(5)
        .compression(ParquetCompression::Uncompressed);
    let sink = ParquetSink::new(cfg).await.unwrap();

    for i in 0..50 {
        let payload: String = (0..256)
            .map(|j| char::from(b'A' + (((i * 31 + j) % 26) as u8)))
            .collect();
        sink.write_batch(&[json!({"i": i as i64, "payload": payload})])
            .await
            .unwrap();
    }
    sink.flush().await.unwrap();

    let files: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("parquet"))
        .collect();
    assert!(
        files.len() > 1,
        "byte-based rollover should produce >1 file, got {}",
        files.len()
    );
    let batches = read_all_local(tmp.path()).await;
    assert_eq!(rows_in(&batches), 50);
}

#[tokio::test]
async fn type_drift_across_batches_errors() {
    let tmp = TempDir::new().unwrap();
    let sink = ParquetSink::new(cfg_dir(tmp.path())).await.unwrap();
    sink.write_batch(&[json!({"x": 1})]).await.unwrap();
    let err = sink
        .write_batch(&[json!({"x": "definitely not an int"})])
        .await
        .expect_err("type drift must error");
    let msg = format!("{err}");
    assert!(msg.contains("'x'") || msg.contains("x"), "msg: {msg}");
    sink.flush().await.unwrap();
}

#[tokio::test]
async fn dropping_without_flush_produces_no_visible_file() {
    // Parquet writers stream into the object_store as a multipart upload;
    // dropping the writer aborts the in-progress upload so no orphan/half-
    // written `.parquet` file is left behind. This is the strongest possible
    // guarantee: either flush() succeeded and the file is readable, or the
    // file never existed. Callers must therefore always call flush() at end
    // of pipeline — otherwise their data simply isn't persisted.
    let tmp = TempDir::new().unwrap();
    let sink = ParquetSink::new(cfg_dir(tmp.path())).await.unwrap();
    sink.write_batch(&[json!({"id": 1, "name": "alice"})])
        .await
        .unwrap();
    drop(sink);

    let entries: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("parquet"))
        .collect();
    assert!(
        entries.is_empty(),
        "dropping without flush should not leave any visible file, found {entries:?}"
    );
}

#[tokio::test]
async fn flush_makes_file_readable() {
    let tmp = TempDir::new().unwrap();
    let sink = ParquetSink::new(cfg_dir(tmp.path())).await.unwrap();
    sink.write_batch(&[json!({"id": 1, "name": "alice"})])
        .await
        .unwrap();
    sink.flush().await.unwrap();
    let batches = read_all_local(tmp.path()).await;
    assert_eq!(rows_in(&batches), 1);
}

#[tokio::test]
async fn in_memory_object_store_path_round_trips() {
    // We can't easily run end-to-end through `ParquetSink::new` with an
    // arbitrary `Arc<dyn ObjectStore>` (it builds its own from config), but we
    // CAN exercise the full encode + write path against the same
    // ParquetObjectWriter that the sink uses, which is what protects the S3
    // code path. This test would be the natural place to wire LocalStack in
    // future via `endpoint_url`.
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use parquet::arrow::AsyncArrowWriter;
    use parquet::arrow::async_writer::ParquetObjectWriter;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = ObjPath::parse("data/test.parquet").unwrap();
    let writer = ParquetObjectWriter::new(store.clone(), path.clone());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    let mut aw = AsyncArrowWriter::try_new(writer, schema.clone(), None).unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    aw.write(&batch).await.unwrap();
    aw.close().await.unwrap();

    // Read back via ParquetObjectReader
    let meta = store.head(&path).await.unwrap();
    let reader = ParquetObjectReader::new(store, meta.location).with_file_size(meta.size);
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(rows_in(&batches), 2);
}

#[tokio::test]
async fn s3_destination_builds_without_credentials_for_endpoint_url() {
    // Quick smoke test that the S3 config branch doesn't blow up when given
    // a non-AWS endpoint; we don't actually hit the network because there's
    // no write. Documents that `endpoint_url` is the path for LocalStack and
    // MinIO without changing the sink contract.
    let cfg = ParquetSinkConfig::new(ParquetDestination::S3(ParquetS3Destination {
        bucket: "test-bucket".to_string(),
        prefix: "data/".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint_url: Some("http://localhost:4566".to_string()),
        allow_http: true,
    }));
    // SAFETY: no other test in this binary reads AWS_* env vars, and the
    // values are constants — setting them twice from parallel tests would
    // still result in the same final state, so the data race is benign.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    }
    let result = ParquetSink::new(cfg).await;
    assert!(
        result.is_ok(),
        "S3 config should build cleanly: {:?}",
        result.err()
    );
}
