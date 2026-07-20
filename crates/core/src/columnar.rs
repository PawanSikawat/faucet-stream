//! Opt-in Apache Arrow columnar record path (feature `arrow`, RFC 0002 / #375).
//!
//! This is the **escape hatch** described in #324: a columnar representation a
//! connector may produce or consume at the page boundary, *additive* to and
//! coexisting with the default `serde_json::Value` row model. It touches neither
//! [`StreamPage`](crate::StreamPage) nor any existing connector — an Arrow-native
//! source overrides [`Source::stream_batches`](crate::Source::stream_batches) and
//! an Arrow-native sink overrides
//! [`Sink::write_batch_columnar`](crate::Sink::write_batch_columnar); the pipeline
//! uses the columnar path only when **both** sides advertise support (and no
//! `Value`-shaped stage needs to observe the records), so a
//! `parquet → parquet` chain never materializes `Value`.
//!
//! The `RecordBatch ↔ Value` conversions here are the single source of truth for
//! the shim (they match `faucet-transform-sql`'s `shovel` byte-for-byte, incl.
//! `with_explicit_nulls(true)` so an explicit-null field round-trips as
//! `"key": null` rather than being silently dropped — audit #321 H6).

use crate::FaucetError;
use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use serde_json::Value;
use std::sync::Arc;

/// A page of records in **columnar** (Arrow) form, the columnar analogue of
/// [`StreamPage`](crate::StreamPage).
///
/// `bookmark` carries the exact same checkpoint semantics as `StreamPage`:
/// `Some` triggers flush + bookmark-persist after the batch is durably written;
/// most sources emit `Some` only on the final batch, CDC-style sources per
/// committed transaction.
#[derive(Debug, Clone)]
pub struct ColumnarPage {
    /// The record batch to write to the sink for this page.
    pub batch: RecordBatch,
    /// Optional bookmark to checkpoint after this batch is durably written.
    pub bookmark: Option<Value>,
}

impl ColumnarPage {
    /// Construct a columnar page from a batch and optional bookmark.
    pub fn new(batch: RecordBatch, bookmark: Option<Value>) -> Self {
        Self { batch, bookmark }
    }

    /// Number of rows in the batch.
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
}

/// Map any display-able error into a [`FaucetError::Transform`] with context.
fn te<E: std::fmt::Display>(ctx: &str, e: E) -> FaucetError {
    FaucetError::Transform(format!("columnar shim: {ctx}: {e}"))
}

/// Infer an Arrow [`Schema`] from a slice of JSON records (each a JSON object).
///
/// Returns the inferred schema in an [`Arc`]. An empty slice yields a schema
/// with no fields.
pub fn infer_arrow_schema(records: &[Value]) -> Result<SchemaRef, FaucetError> {
    let iter = records
        .iter()
        .map(|v| Ok::<_, arrow::error::ArrowError>(v.clone()));
    let schema = arrow_json::reader::infer_json_schema_from_iterator(iter)
        .map_err(|e| te("schema inference", e))?;
    Ok(Arc::new(schema))
}

/// Encode a slice of JSON records into a single [`RecordBatch`] against `schema`.
///
/// Returns an empty batch if `records` is empty.
pub fn values_to_record_batch(
    records: &[Value],
    schema: SchemaRef,
) -> Result<RecordBatch, FaucetError> {
    let mut decoder = arrow_json::ReaderBuilder::new(schema.clone())
        .build_decoder()
        .map_err(|e| te("decoder build", e))?;
    decoder.serialize(records).map_err(|e| te("encode", e))?;
    let mut batches = Vec::new();
    while let Some(b) = decoder.flush().map_err(|e| te("flush", e))? {
        batches.push(b);
    }
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    if batches.len() == 1 {
        return Ok(batches.pop().unwrap());
    }
    arrow::compute::concat_batches(&schema, &batches).map_err(|e| te("concat", e))
}

/// Convenience: infer the schema from `records` and encode them into a batch.
pub fn values_to_record_batch_inferred(records: &[Value]) -> Result<RecordBatch, FaucetError> {
    let schema = infer_arrow_schema(records)?;
    values_to_record_batch(records, schema)
}

/// Decode a [`RecordBatch`] into JSON objects (one per row).
///
/// Uses `arrow-json`'s array writer with **explicit nulls enabled**, so a
/// null-valued column is emitted as `"key": null` rather than omitted — without
/// this a `SELECT *`-style identity would silently delete every explicit-null
/// field (audit #321 H6). An empty batch returns an empty `Vec`.
pub fn record_batch_to_values(batch: &RecordBatch) -> Result<Vec<Value>, FaucetError> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow_json::writer::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, arrow_json::writer::JsonArray>(&mut buf);
        writer.write(batch).map_err(|e| te("json write", e))?;
        writer.finish().map_err(|e| te("json finish", e))?;
    }
    serde_json::from_slice(&buf).map_err(|e| te("json parse", e))
}

/// Compare two schemas for field-level equality (name + data-type + nullability).
pub fn schema_eq(a: &Schema, b: &Schema) -> bool {
    a.fields() == b.fields()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trip_scalars_nulls_nested() {
        let recs = vec![
            json!({"id": 1, "name": "a", "score": 1.5, "ok": true, "tags": ["x", "y"]}),
            json!({"id": 2, "name": null, "score": null, "ok": false, "tags": []}),
        ];
        let batch = values_to_record_batch_inferred(&recs).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let back = record_batch_to_values(&batch).unwrap();
        assert_eq!(back[0]["id"], json!(1));
        assert_eq!(back[0]["tags"], json!(["x", "y"]));
        // #321 H6: an explicit-null field survives the round-trip.
        assert!(back[1].as_object().unwrap().contains_key("name"));
        assert_eq!(back[1]["name"], json!(null));
    }

    #[test]
    fn empty_records_yield_empty_batch_and_back() {
        let schema = infer_arrow_schema(&[json!({"a": 1})]).unwrap();
        let batch = values_to_record_batch(&[], schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert!(record_batch_to_values(&batch).unwrap().is_empty());
    }

    #[test]
    fn columnar_page_reports_rows() {
        let batch = values_to_record_batch_inferred(&[json!({"a": 1}), json!({"a": 2})]).unwrap();
        let page = ColumnarPage::new(batch, Some(json!({"lsn": 42})));
        assert_eq!(page.num_rows(), 2);
        assert_eq!(page.bookmark, Some(json!({"lsn": 42})));
    }
}
