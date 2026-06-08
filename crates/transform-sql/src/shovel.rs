//! JSON ↔ Arrow conversion. `arrow-json` for both directions; `arrow` for
//! concatenation. All errors surface as `FaucetError::Transform` (or
//! `FaucetError::Config` for arity mismatches in the inline `values` relation).

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use faucet_core::FaucetError;
use serde_json::{Map, Value};
use std::sync::Arc;

/// Map any display-able error into a `FaucetError::Transform` with context.
fn te<E: std::fmt::Display>(ctx: &str, e: E) -> FaucetError {
    FaucetError::Transform(format!("sql transform: {ctx}: {e}"))
}

/// Infer an Arrow [`Schema`] from a slice of JSON records.
///
/// Each element must be a JSON object. Returns the inferred schema wrapped in
/// an [`Arc`]. On an empty slice the result is a schema with no fields.
pub fn infer_schema(records: &[Value]) -> Result<SchemaRef, FaucetError> {
    let iter = records
        .iter()
        .map(|v| Ok::<_, arrow::error::ArrowError>(v.clone()));
    let schema = arrow_json::reader::infer_json_schema_from_iterator(iter)
        .map_err(|e| te("schema inference", e))?;
    Ok(Arc::new(schema))
}

/// Encode a slice of JSON records into a single [`RecordBatch`] against `schema`.
///
/// Uses the `arrow-json` decoder (`ReaderBuilder → build_decoder → serialize →
/// flush`). Returns an empty batch if `records` is empty.
pub fn json_to_record_batch(
    records: &[Value],
    schema: SchemaRef,
) -> Result<RecordBatch, FaucetError> {
    let mut decoder = arrow_json::ReaderBuilder::new(schema.clone())
        .build_decoder()
        .map_err(|e| te("decoder build", e))?;
    decoder
        .serialize(records)
        .map_err(|e| te("encode", e))?;
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

/// Decode one or more [`RecordBatch`]es into JSON objects (one per row).
///
/// Uses `arrow-json`'s [`ArrayWriter`][arrow_json::ArrayWriter], which produces
/// a JSON array. An empty input returns an empty `Vec`.
pub fn record_batches_to_json(batches: &[RecordBatch]) -> Result<Vec<Value>, FaucetError> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow_json::ArrayWriter::new(&mut buf);
        for b in batches {
            writer.write(b).map_err(|e| te("json write", e))?;
        }
        writer.finish().map_err(|e| te("json finish", e))?;
    }
    let rows: Vec<Value> = serde_json::from_slice(&buf).map_err(|e| te("json parse", e))?;
    Ok(rows)
}

/// Build a [`RecordBatch`] from inline `columns` + `rows` (the `values` relation).
///
/// Each row is zipped with `columns` to produce a JSON object, and the resulting
/// objects are passed through [`infer_schema`] + [`json_to_record_batch`]. A row
/// whose length differs from `columns` is rejected with [`FaucetError::Config`].
pub fn values_to_record_batch(
    columns: &[String],
    rows: &[Vec<Value>],
) -> Result<RecordBatch, FaucetError> {
    let mut objs = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        if row.len() != columns.len() {
            return Err(FaucetError::Config(format!(
                "sql transform: values relation row {i} has {} cells, expected {}",
                row.len(),
                columns.len()
            )));
        }
        let mut m = Map::new();
        for (c, v) in columns.iter().zip(row.iter()) {
            m.insert(c.clone(), v.clone());
        }
        objs.push(Value::Object(m));
    }
    let schema = infer_schema(&objs)?;
    json_to_record_batch(&objs, schema)
}

/// Compare two schemas for field-level equality (name + data-type + nullability).
///
/// Used by the per-page schema cache in the runtime to detect schema drift
/// between pages.
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
        let schema = infer_schema(&recs).unwrap();
        let batch = json_to_record_batch(&recs, schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let back = record_batches_to_json(&[batch]).unwrap();
        assert_eq!(back[0]["id"], json!(1));
        assert_eq!(back[0]["tags"], json!(["x", "y"]));
        assert_eq!(back[1]["name"], json!(null));
    }

    #[test]
    fn values_to_batch_builds_named_columns() {
        let batch = values_to_record_batch(
            &["id".to_string(), "label".to_string()],
            &[vec![json!(1), json!("NA")], vec![json!(2), json!("EU")]],
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
        let back = record_batches_to_json(&[batch]).unwrap();
        assert_eq!(back[1]["label"], json!("EU"));
    }

    #[test]
    fn schema_drift_changes_inferred_schema() {
        let a = infer_schema(&[json!({"a": 1})]).unwrap();
        let b = infer_schema(&[json!({"a": 1, "b": "x"})]).unwrap();
        assert_ne!(a.fields().len(), b.fields().len());
    }
}
