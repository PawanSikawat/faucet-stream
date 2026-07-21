//! Acceptance test for the SQL transform on the columnar fast path (#375).
//!
//! The parquet source/sink columnar impls are covered in their own crates; here
//! we prove the **pipeline** runs a SQL transform end-to-end on Arrow batches:
//! a columnar source wrapped in `TransformingSource([sql])` feeds a columnar
//! sink's `write_batch_columnar` — never the `Value` `write_batch` — so a
//! `parquet → sql → parquet`-shaped chain never materializes `serde_json::Value`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;

use arrow::array::RecordBatch;
use faucet_core::async_trait;
use faucet_core::columnar::{
    ColumnarPage, record_batch_to_values, values_to_record_batch_inferred,
};
use faucet_core::observability::Labels;
use faucet_core::{FaucetError, Pipeline, Sink, Source, Stream, TransformingSource};
use faucet_transform_sql::{SqlTransform, SqlTransformConfig};
use serde_json::{Value, json};

/// A columnar-capable source that emits its records as a single Arrow batch.
struct ColumnarSource(Vec<Value>);

#[async_trait]
impl Source for ColumnarSource {
    async fn fetch_with_context(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.0.clone())
    }
    fn supports_columnar(&self) -> bool {
        true
    }
    fn stream_batches<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        _bs: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<ColumnarPage, FaucetError>> + Send + 'a>> {
        let batch = values_to_record_batch_inferred(&self.0).unwrap();
        Box::pin(futures::stream::once(async move {
            Ok(ColumnarPage {
                batch,
                bookmark: None,
            })
        }))
    }
}

/// A sink that records which path was used: columnar batches vs `Value` rows.
#[derive(Default)]
struct PathRecordingSink {
    columnar_batches: Mutex<Vec<RecordBatch>>,
    value_rows: Mutex<usize>,
}

#[async_trait]
impl Sink for PathRecordingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        *self.value_rows.lock().unwrap() += records.len();
        Ok(records.len())
    }
    fn supports_columnar(&self) -> bool {
        true
    }
    async fn write_batch_columnar(&self, batch: &RecordBatch) -> Result<usize, FaucetError> {
        let n = batch.num_rows();
        self.columnar_batches.lock().unwrap().push(batch.clone());
        Ok(n)
    }
    async fn flush(&self) -> Result<(), FaucetError> {
        Ok(())
    }
}

#[tokio::test]
async fn sql_transform_runs_on_the_columnar_path_end_to_end() {
    let source = ColumnarSource(vec![
        json!({"id": 1, "v": 10}),
        json!({"id": 2, "v": 20}),
        json!({"id": 3, "v": 30}),
    ]);
    let sql = SqlTransform::compile(&SqlTransformConfig {
        query: "SELECT id, v * 2 AS doubled FROM batch WHERE id <= 2 ORDER BY id".into(),
        relations: vec![],
        memory_limit: None,
        threads: Some(1),
    })
    .unwrap();
    let wrapped = TransformingSource::new(
        Box::new(source),
        vec![sql.into_page_stage()],
        Labels::for_named("sql"),
    )
    .unwrap();
    // The chain (columnar source + sql stage) must advertise columnar support.
    assert!(wrapped.supports_columnar());

    let sink = PathRecordingSink::default();
    let result = Pipeline::new(&wrapped, &sink).run().await.unwrap();
    assert_eq!(result.records_written, 2, "WHERE id <= 2 keeps two rows");

    // The columnar path was taken: batches went through write_batch_columnar,
    // and the Value write_batch was never called.
    assert_eq!(
        *sink.value_rows.lock().unwrap(),
        0,
        "Value path must not run — no serde_json::Value materialization"
    );
    let batches = sink.columnar_batches.lock().unwrap();
    assert_eq!(
        batches.len(),
        1,
        "one transformed columnar page reached the sink"
    );
    let rows = record_batch_to_values(&batches[0]).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], json!(1));
    assert_eq!(rows[0]["doubled"], json!(20));
    assert_eq!(rows[1]["doubled"], json!(40));
}
