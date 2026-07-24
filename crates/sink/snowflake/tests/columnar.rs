//! Arrow columnar bulk-load path (#381) end-to-end against a local `file://`
//! stage + a wiremock'd `COPY INTO`. Exercises `write_batch_columnar` →
//! `bulk::{resolve_store, encode_parquet, upload, build_copy_into}` →
//! `execute_sql`, and asserts a Parquet object lands in the stage dir and the
//! COPY statement reaches the SQL REST endpoint.
#![cfg(feature = "arrow")]

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use faucet_core::Sink;
use faucet_sink_snowflake::{
    SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig, SnowflakeStageConfig,
};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn columnar_write_stages_parquet_and_issues_copy() {
    let server = MockServer::start().await;
    // The COPY INTO statement is the only REST call on this path.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "message": "Statement executed successfully."
        })))
        .expect(1)
        .mount(&server)
        .await;

    // A local filesystem "external stage" — object_store's `file://` backend
    // stands in for S3/GCS/Azure so the upload path runs in CI.
    let dir = tempfile::tempdir().unwrap();
    let stage_dir = dir.path().join("stage");
    std::fs::create_dir_all(&stage_dir).unwrap();
    let url = format!("file://{}/", stage_dir.to_str().unwrap());

    let config = SnowflakeSinkConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        "EVENTS",
        SnowflakeAuth::OAuth {
            token: "tok".into(),
        },
    )
    .with_bulk_load(SnowflakeStageConfig {
        stage: "DB.PUBLIC.EVENTS_STAGE".into(),
        url,
        storage_options: Default::default(),
        match_by_column_name: "CASE_INSENSITIVE".into(),
        purge: false,
    });

    let sink = SnowflakeSink::new(config)
        .unwrap()
        .with_endpoint(format!("{}/api/v2/statements", server.uri()));

    assert!(sink.supports_columnar());

    let written = sink
        .write_batch_columnar(&batch())
        .await
        .expect("columnar write");
    assert_eq!(written, 3);

    // A single Parquet object was staged to the local backing store.
    let staged: Vec<_> = std::fs::read_dir(&stage_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
        .collect();
    assert_eq!(staged.len(), 1, "exactly one staged parquet file");

    // An empty batch is a no-op (no upload, no COPY).
    let empty = RecordBatch::new_empty(batch().schema());
    assert_eq!(sink.write_batch_columnar(&empty).await.unwrap(), 0);
}

#[tokio::test]
async fn columnar_write_without_bulk_load_errors() {
    // A sink with no `bulk_load` does not advertise columnar; calling the
    // method directly must fail loudly rather than silently no-op.
    let sink = SnowflakeSink::new(SnowflakeSinkConfig::new(
        "acct",
        "WH",
        "DB",
        "PUBLIC",
        "EVENTS",
        SnowflakeAuth::OAuth { token: "t".into() },
    ))
    .unwrap();
    assert!(!sink.supports_columnar());
    let err = sink.write_batch_columnar(&batch()).await.unwrap_err();
    assert!(format!("{err}").contains("bulk_load"), "{err}");
}
